use super::table::BlockHandle;
use crate::Options;
use crate::db::InternalIterator;
use crate::db::InternalKey;
use crate::{DBError, util};

// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

/*

1. 数据区（Entries）
Block 内部存储一系列有序的 key-value entry。
每个 entry 采用前缀压缩格式，即只存储与前一个 key 不同的部分（delta encoding）。
每个 entry 的格式为：
shared：与前一个 key 共享的前缀长度（Varint32 或 1 字节优化）
non_shared：本 entry 独有的 key 长度
value_length：value 的长度
key_delta：本 entry 独有的 key 内容
value：value 内容
2. Restart 点数组（Restart Array）
为了加速查找，Block 每隔一定数量的 entry（如 16 个）就设置一个 restart 点。
restart 点处的 entry 的 key 不做前缀压缩，完整存储。
Block 末尾存储了一个 restart 数组，记录每个 restart entry 在数据区的偏移量（uint32_t 数组）。
3. Restart 数组长度
Block 最后 4 字节存储 restart 数组的长度（即 restart 点的数量，uint32_t）。
4. 内存布局示意
| entry1 | entry2 | ... | entryN | restart_array | num_restarts
entry：前缀压缩的 key-value 对
restart_array：每个 restart entry 的偏移量数组
num_restarts：restart 点数量（4 字节）
5. 读取流程
查找时，先用二分法在 restart 数组中定位到合适的 restart 点，然后线性扫描到目标 key。

*/

use crate::util::encode_varint32;
use std::cmp;

pub struct BlockBuilder {
    buffer: Vec<u8>,
    restarts: Vec<u32>,
    counter: usize,
    last_key: Vec<u8>,
    options: crate::Options,
}

impl BlockBuilder {
    pub fn new(options: Options) -> Self {
        let restarts = vec![0];
        BlockBuilder {
            buffer: Vec::new(),
            restarts,
            counter: 0,
            last_key: Vec::new(),
            options,
        }
    }

    pub fn add(&mut self, key: &InternalKey) {
        let mut key_bytes = Vec::new();
        key.encoding_key(&mut key_bytes);
        let value = key.value();
        let mut shared = 0;
        if self.counter < self.options.block_restart_interval {
            let min_len = cmp::min(self.last_key.len(), key_bytes.len());
            while shared < min_len && self.last_key[shared] == key_bytes[shared] {
                shared += 1;
            }
        } else {
            // 到达 restart interval，重启前缀压缩
            self.restarts.push(self.buffer.len() as u32);
            self.counter = 0;
            shared = 0;
        }
        let non_shared = key_bytes.len() - shared;
        encode_varint32(&mut self.buffer, shared as u32);
        encode_varint32(&mut self.buffer, non_shared as u32);
        encode_varint32(&mut self.buffer, value.len() as u32);
        self.buffer.extend_from_slice(&key_bytes[shared..]);
        self.buffer.extend_from_slice(value);
        self.last_key.clear();
        self.last_key.extend_from_slice(&key_bytes);
        self.counter += 1;
    }

    /// 用于 index_block
    pub fn add_handle(&mut self, key: &[u8], handle: &BlockHandle) {
        let mut value = Vec::new();
        handle.encode_to(&mut value);
        // sequence 和 value_type 在 index block 中不需要
        let ikey = InternalKey::new(key, 0, crate::db::dbformat::ValueType::TypeValue, &value);
        self.add(&ikey);
    }

    pub fn finish(mut self) -> Vec<u8> {
        // 写入 restart array
        for &offset in &self.restarts {
            util::encode_fixed32(&mut self.buffer, offset);
            //self.buffer.extend_from_slice(&offset.to_le_bytes());
        }
        // 写入 restart 数组长度
        util::encode_fixed32(&mut self.buffer, self.restarts.len() as u32);
        self.buffer
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn current_size_estimate(&self) -> usize {
        // buffer + restart array + restart count
        self.buffer.len() + self.restarts.len() * 4 + 4
    }
}

#[derive(Clone)]
pub struct Block {
    data: Vec<u8>,
    restarts: Vec<u8>,
    num_restarts: usize,
}

impl Block {
    pub fn new(contents: Vec<u8>) -> Self {
        let len = contents.len();
        let num_restarts = util::decode_fixed32(&contents[len - 4..]).unwrap() as usize;
        //let num_restarts = u32::from_le_bytes(data[len - 4..].try_into().unwrap()) as usize;
        let restarts_start = len - 4 - num_restarts * 4;
        //let restarts = &data[restarts_start..len - 4];
        let mut data = contents;
        let mut restarts = data.split_off(restarts_start);
        restarts.truncate(len - 4);
        Block {
            data,
            restarts,
            num_restarts,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

pub struct BlockIter {
    block: Block,
    next_offset: usize,
    key: Option<InternalKey>,
    valid: bool,
    key_bytes: Option<Vec<u8>>,
    current_offset: usize,
}

// value_end, key, (value_start, value_end)
type BlockEntry = (usize, InternalKey);
type BlockEntryResult = Result<Option<BlockEntry>, DBError>;

impl BlockIter {
    pub fn new(block: Block) -> Self {
        BlockIter {
            block,
            next_offset: 0,
            key: None,
            valid: false,
            key_bytes: None,
            current_offset: 0,
        }
    }

    fn parse_next_entry(&mut self, offset: usize) -> BlockEntryResult {
        let data = &self.block.data;
        if offset >= data.len() {
            return Ok(None);
        }
        // 解析 shared, non_shared, value_len
        let (shared, n1) = util::decode_varint32(&data[offset..]).ok_or(DBError::Corruption)?;
        let (non_shared, n2) =
            util::decode_varint32(&data[offset + n1..]).ok_or(DBError::Corruption)?;
        let (value_len, n3) =
            util::decode_varint32(&data[offset + n1 + n2..]).ok_or(DBError::Corruption)?;
        let key_start = offset + n1 + n2 + n3;
        let key_end = key_start + non_shared as usize;
        let value_start = key_end;
        let value_end = value_start + value_len as usize;
        let mut key_bytes = match &self.key_bytes {
            Some(last) => {
                let mut v = last.clone();
                v.truncate(shared as usize);
                v
            }
            None => Vec::new(),
        };
        key_bytes.extend_from_slice(&data[key_start..key_end]);
        // 组装 InternalKey 的完整 entry 字节（key + value）
        let mut entry_bytes = key_bytes.clone();
        // 追加 value 的 varint32 长度和内容
        let mut value_len_buf = Vec::new();
        util::encode_varint32(&mut value_len_buf, value_len);
        entry_bytes.extend_from_slice(&value_len_buf);
        entry_bytes.extend_from_slice(&data[value_start..value_end]);
        let key = InternalKey::decode(&entry_bytes).ok_or(DBError::Corruption)?;
        self.key_bytes = Some(key_bytes);
        Ok(Some((value_end, key)))
    }

    fn entry_offset_by_restart(&self, restart_idx: usize) -> usize {
        debug_assert!(
            restart_idx < self.block.num_restarts,
            "restart_idx out of bounds"
        );
        let off = &self.block.restarts[restart_idx * 4..restart_idx * 4 + 4];
        u32::from_le_bytes(off.try_into().unwrap()) as usize
    }

    fn corruption(&mut self) -> Result<(), DBError> {
        self.valid = false;
        self.key = None;
        self.next_offset = 0;
        self.key_bytes = None;
        Err(DBError::Corruption)
    }
}

impl InternalIterator for BlockIter {
    fn seek(&mut self, target: &InternalKey) -> Result<(), DBError> {
        // 二分查找 restart 点
        let mut left = 0;
        let mut right = self.block.num_restarts - 1;
        while left < right {
            let mid = (left + right).div_ceil(2);
            let off = self.entry_offset_by_restart(mid);
            let entry = self.parse_next_entry(off)?;
            if let Some((_, key)) = entry {
                if key.user_key() < target.user_key() {
                    left = mid;
                } else {
                    right = mid - 1;
                }
            } else {
                return Err(DBError::Corruption);
            }
        }
        // 线性扫描
        let mut offset = if left == self.block.num_restarts {
            self.entry_offset_by_restart(self.block.num_restarts - 1)
        } else {
            self.entry_offset_by_restart(left)
        };
        self.key_bytes = None;
        loop {
            let entry = self.parse_next_entry(offset)?;
            if let Some((next_offset, key)) = entry {
                if key.user_key() >= target.user_key() {
                    // 如果当前 key 已经大于等于目标 key，直接返回
                    self.key = Some(key);
                    self.current_offset = offset;
                    self.next_offset = next_offset;
                    self.valid = true;

                    dbg!(
                        "seek",
                        self.current_offset,
                        self.key.as_ref().map(|k| k.user_key()),
                    );

                    return Ok(());
                } else {
                    offset = next_offset;
                }
            } else {
                self.valid = false;
                return Ok(());
            }
        }
    }

    fn seek_to_first(&mut self) -> Result<(), DBError> {
        self.current_offset = 0;
        self.key_bytes = None;
        let entry = self.parse_next_entry(self.current_offset)?;
        if let Some((next_offset, key)) = entry {
            self.key = Some(key);
            self.next_offset = next_offset;
            self.valid = true;
        } else {
            self.valid = false;
        }
        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<(), DBError> {
        if self.block.num_restarts == 0 {
            self.valid = false;
            return Err(DBError::Corruption);
        }
        // 定位到最后一个 restart 点
        let restart_idx = self.block.num_restarts - 1;
        let mut offset = self.entry_offset_by_restart(restart_idx);
        self.key_bytes = None;
        loop {
            let entry = match self.parse_next_entry(offset) {
                Ok(e) => e,
                Err(_) => return self.corruption(),
            };
            if let Some((next_offset, key)) = entry {
                self.key = Some(key);
                self.current_offset = offset;
                self.next_offset = next_offset;
                offset = next_offset;
                self.valid = true;
            } else {
                break;
            }
        }
        Ok(())
    }

    fn valid(&self) -> bool {
        self.valid
    }
    fn key(&self) -> Option<&InternalKey> {
        self.key.as_ref()
    }

    fn next(&mut self) -> Result<(), DBError> {
        assert!(self.valid, "Iterator is not valid");
        let offset = self.next_offset;
        let entry = match self.parse_next_entry(offset) {
            Ok(e) => e,
            Err(_) => return self.corruption(),
        };
        if let Some((next_offset, key)) = entry {
            self.key = Some(key);
            self.next_offset = next_offset;
            self.current_offset = offset;
            self.valid = true;

            dbg!(
                "next: ",
                self.current_offset,
                self.key.as_ref().map(|k| k.user_key()),
            );
        } else {
            self.valid = false;
        }
        Ok(())
    }

    fn prev(&mut self) -> Result<(), DBError> {
        assert!(self.valid, "Iterator is not valid");

        if self.current_offset == 0 {
            // 如果已经是第一个 entry，直接无效化
            self.valid = false;
            return Ok(());
        }

        // 找到当前 entry 属于哪个 restart 区间
        let mut entry_offset = 0;
        for i in 0..self.block.num_restarts {
            let off = self.entry_offset_by_restart(i);
            if off >= self.current_offset {
                break;
            }
            entry_offset = off;
        }

        // 线性扫描找到前一个 entry
        //let mut last_valid = None;
        self.key_bytes = None;
        let mut offset = entry_offset;
        let original_offset = self.current_offset;
        while offset < original_offset {
            let entry = match self.parse_next_entry(offset) {
                Ok(e) => e,
                Err(_) => return self.corruption(),
            };
            if let Some((next_offset, key)) = entry {
                self.key = Some(key);
                self.current_offset = offset;
                self.next_offset = next_offset;
                self.valid = true;
                offset = next_offset;
            } else {
                panic!(
                    "should not happen: no entry found before offset {} ?",
                    self.next_offset
                );
            }
        }

        dbg!(
            "prev: ",
            &self.current_offset,
            &self.key.as_ref().map(|k| k.user_key())
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Options;
    use crate::db::InternalKey;
    use crate::db::dbformat::MAX_SEQUENCE_NUMBER;
    use crate::db::dbformat::ValueType;

    fn make_internal_key(user_key: &[u8], seq: u64, value: &[u8]) -> InternalKey {
        InternalKey::new(user_key, seq, ValueType::TypeValue, value)
    }

    #[test]
    fn test_blockiter_seek_prev_next_combo() {
        let mut opts = Options::default();
        opts.block_restart_interval = 2;
        let mut builder = BlockBuilder::new(opts.clone());
        let keys: Vec<&[u8]> = vec![b"a", b"c", b"e", b"g"];
        let values: Vec<&[u8]> = vec![b"1", b"2", b"3", b"4"];
        for (i, (k, v)) in keys.iter().zip(values.iter()).enumerate() {
            let ikey = InternalKey::new(k, i as u64, ValueType::TypeValue, v);
            builder.add(&ikey);
        }
        let block_data = builder.finish();
        let block = Block::new(block_data);
        let mut iter = BlockIter::new(block);

        // seek 到 "c"
        let target = InternalKey::new(b"c", MAX_SEQUENCE_NUMBER, ValueType::TypeValue, b"");
        iter.seek(&target).unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"c");

        // next 应该到 "e"
        iter.next().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"e");

        // prev 应该回到 "c"
        iter.prev().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"c");

        // 再 prev 回到 "a"
        iter.prev().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"a");

        // prev 应该无效
        iter.prev().unwrap();
        assert!(!iter.valid());

        // seek 到 "f"（不存在），应该到 "g"
        let target = InternalKey::new(b"f", MAX_SEQUENCE_NUMBER, ValueType::TypeValue, b"");
        iter.seek(&target).unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"g");

        // next 应该无效
        iter.next().unwrap();
        assert!(!iter.valid());
    }

    /// 正常 block 构建、正序/逆序遍历、seek、边界 seek 行为的回归测试
    #[test]
    fn test_blockbuilder_and_block_roundtrip() {
        let mut opts = Options::default();
        opts.block_restart_interval = 2;
        let mut builder = BlockBuilder::new(opts.clone());
        let keys: Vec<&[u8]> = vec![b"a" as &[u8], b"ab", b"abc", b"b", b"c"];
        let values: Vec<&[u8]> = vec![b"1", b"2", b"3", b"4", b"5"];
        let mut internal_keys = vec![];
        for (i, (k, v)) in keys.iter().zip(values.iter()).enumerate() {
            let ikey = make_internal_key(k, i as u64, v);
            builder.add(&ikey);
            internal_keys.push(ikey);
        }
        let block_data = builder.finish();
        let block = Block::new(block_data);
        let mut iter = BlockIter::new(block);
        // 正序遍历
        iter.seek_to_first().unwrap();
        let mut got = vec![];
        while iter.valid() {
            let k = iter.key().unwrap();
            got.push((k.user_key().to_vec(), k.value().to_vec()));
            iter.next().unwrap();
        }
        let expected: Vec<(Vec<u8>, Vec<u8>)> = keys
            .iter()
            .zip(values.iter())
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        assert_eq!(got, expected);
        // 逆序遍历
        iter.seek_to_last().unwrap();
        let mut got_rev = vec![];
        while iter.valid() {
            let k = iter.key().unwrap();
            got_rev.push((k.user_key().to_vec(), k.value().to_vec()));
            iter.prev().unwrap();
        }
        let mut expected_rev = expected.clone();
        expected_rev.reverse();
        assert_eq!(got_rev, expected_rev);

        // seek 测试
        for (i, k) in keys.iter().enumerate() {
            let target = make_internal_key(k, i as u64, b""); // value 字段可为空
            iter.seek(&target).unwrap();
            assert!(iter.valid());
            let got_key = iter.key().unwrap();
            assert_eq!(got_key.user_key(), *k);
            assert_eq!(got_key.value(), values[i]);
            // next 检查
            iter.next().unwrap();
            if i + 1 < keys.len() {
                assert!(iter.valid());
                let next_key = iter.key().unwrap();
                assert_eq!(next_key.user_key(), keys[i + 1]);
                assert_eq!(next_key.value(), values[i + 1]);
            } else {
                assert!(!iter.valid());
            }
            // 再 seek 回来
            iter.seek(&target).unwrap();
            assert!(iter.valid());
            // prev 检查
            iter.prev().unwrap();
            if i == 0 {
                assert!(!iter.valid());
            } else {
                assert!(iter.valid());
                let prev_key = iter.key().unwrap();
                assert_eq!(prev_key.user_key(), keys[i - 1]);
                assert_eq!(prev_key.value(), values[i - 1]);
                iter.next().unwrap(); // 恢复到下一个
                assert!(iter.valid());
                assert_eq!(iter.key().unwrap().user_key(), keys[i]);
                assert_eq!(iter.key().unwrap().value(), values[i]);
            }
        }
        // seek 不存在的 key，应该定位到大于等于该 key 的 entry 或无效
        let not_exist = make_internal_key(b"ba", 0, b"");
        iter.seek(&not_exist).unwrap();
        assert!(iter.valid());
        let got_key = iter.key().unwrap();
        // "ba" 介于 "b" 和 "c" 之间，应该定位到 "c"
        assert_eq!(got_key.user_key(), b"c");
        assert_eq!(got_key.value(), b"5");

        // seek 到比最小 key 还小的 key，应返回第一个 entry
        let min_key = make_internal_key(b"", 0, b"");
        iter.seek(&min_key).unwrap();
        assert!(iter.valid());
        let got_key = iter.key().unwrap();
        assert_eq!(got_key.user_key(), b"a");
        assert_eq!(got_key.value(), b"1");

        // seek 到比最大 key 还大的 key，应 invalid
        let max_key = make_internal_key(b"z", 0, b"");
        iter.seek(&max_key).unwrap();
        // "z" > "c"，应无效
        assert!(!iter.valid());

        // seek 到 block 里不存在但靠近 restart 点的 key
        let near_restart1 = make_internal_key(b"aa", 0, b"");
        iter.seek(&near_restart1).unwrap();
        assert!(iter.valid());
        let got_key = iter.key().unwrap();
        // "aa" 介于 "a" 和 "ab" 之间，应返回 "ab"
        assert_eq!(got_key.user_key(), b"ab");
        assert_eq!(got_key.value(), b"2");

        let near_restart2 = make_internal_key(b"bb", 0, b"");
        iter.seek(&near_restart2).unwrap();
        assert!(iter.valid());
        let got_key = iter.key().unwrap();
        // "bb" 介于 "b" 和 "c" 之间，应返回 "c"
        assert_eq!(got_key.user_key(), b"c");
        assert_eq!(got_key.value(), b"5");
    }

    /// 空 block 的行为，iter 应 invalid
    #[test]
    fn test_blockbuilder_empty() {
        let opts = Options::default();
        let builder = BlockBuilder::new(opts);
        assert!(builder.is_empty());
        let data = builder.finish();
        let block = Block::new(data);
        let iter = BlockIter::new(block);
        assert!(!iter.valid());
    }

    /// 只有一个 entry 且为 restart 点，测试 next/prev/seek 边界
    #[test]
    fn test_blockbuilder_single_entry_restart() {
        let opts = Options {
            block_restart_interval: 16,
            ..Default::default()
        };
        let mut builder = BlockBuilder::new(opts.clone());
        let key = make_internal_key(b"x", 1, b"v");
        builder.add(&key);
        let block_data = builder.finish();
        let block = Block::new(block_data);
        let mut iter = BlockIter::new(block);
        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"x");
        assert_eq!(iter.key().unwrap().value(), b"v");
        iter.next().unwrap();
        assert!(!iter.valid());
        iter.seek_to_last().unwrap();
        assert!(iter.valid());
        iter.prev().unwrap();
        assert!(!iter.valid());
    }

    /// 所有 entry 都是 restart 点（block_restart_interval=1），测试遍历
    #[test]
    fn test_blockbuilder_all_restart_points() {
        let opts = Options {
            block_restart_interval: 1,
            ..Default::default()
        };
        let mut builder = BlockBuilder::new(opts.clone());
        let keys = [b"a", b"b", b"c"];
        let values = [b"1", b"2", b"3"];
        for (i, (k, v)) in keys.iter().zip(values.iter()).enumerate() {
            let ikey = make_internal_key(*k, i as u64, *v);
            builder.add(&ikey);
        }
        let block_data = builder.finish();
        let block = Block::new(block_data);
        let mut iter = BlockIter::new(block);
        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"a");
        iter.next().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"b");
        iter.next().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"c");
        iter.next().unwrap();
        assert!(!iter.valid());
    }

    /// key 为空、value 为空的 entry，测试遍历和内容
    #[test]
    fn test_blockbuilder_empty_key_and_value() {
        let opts = Options {
            block_restart_interval: 2,
            ..Default::default()
        };
        let mut builder = BlockBuilder::new(opts.clone());
        let k1 = make_internal_key(b"", 1, b"v1");
        let k2 = make_internal_key(b"k2", 2, b"");
        builder.add(&k1);
        builder.add(&k2);
        let block_data = builder.finish();
        let block = Block::new(block_data);
        let mut iter = BlockIter::new(block);
        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"");
        assert_eq!(iter.key().unwrap().value(), b"v1");
        iter.next().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"k2");
        assert_eq!(iter.key().unwrap().value(), b"");
    }

    /// 极端长 key/value，测试大数据 entry 的正确性
    #[test]
    fn test_blockbuilder_long_key_value() {
        let opts = Options {
            block_restart_interval: 2,
            ..Default::default()
        };
        let mut builder = BlockBuilder::new(opts.clone());
        let long_key = vec![b'x'; 1024];
        let long_val = vec![b'y'; 2048];
        let k1 = make_internal_key(&long_key, 1, &long_val);
        builder.add(&k1);
        let block_data = builder.finish();
        let block = Block::new(block_data);
        let mut iter = BlockIter::new(block);
        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), &long_key[..]);
        assert_eq!(iter.key().unwrap().value(), &long_val[..]);
    }

    /// 重复 user_key 但 seq 不同，测试 entry 顺序和内容
    #[test]
    fn test_blockbuilder_duplicate_user_key_diff_seq() {
        let opts = Options {
            block_restart_interval: 2,
            ..Default::default()
        };
        let mut builder = BlockBuilder::new(opts.clone());
        let k1 = make_internal_key(b"dup", 1, b"v1");
        let k2 = make_internal_key(b"dup", 2, b"v2");
        builder.add(&k1);
        builder.add(&k2);
        let block_data = builder.finish();
        let block = Block::new(block_data);
        let mut iter = BlockIter::new(block);
        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"dup");
        assert_eq!(iter.key().unwrap().value(), b"v1");
        iter.next().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"dup");
        assert_eq!(iter.key().unwrap().value(), b"v2");
    }

    /// seek 到正好等于 restart 点的 key，测试定位准确性
    #[test]
    fn test_blockbuilder_seek_restart_point() {
        let opts = Options {
            block_restart_interval: 2,
            ..Default::default()
        };
        let mut builder = BlockBuilder::new(opts.clone());
        let keys = [b"a", b"b", b"c", b"d"];
        let values = [b"1", b"2", b"3", b"4"];
        for (i, (k, v)) in keys.iter().zip(values.iter()).enumerate() {
            let ikey = make_internal_key(*k, i as u64, *v);
            builder.add(&ikey);
        }
        let block_data = builder.finish();
        let block = Block::new(block_data);
        let mut iter = BlockIter::new(block);
        // seek 到 restart 点 "a"、"c"
        let t1 = make_internal_key(b"a", 0, b"");
        iter.seek(&t1).unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"a");
        let t2 = make_internal_key(b"c", 0, b"");
        iter.seek(&t2).unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"c");
    }

    /// block_restart_interval 恰好等于 entry 数，测试首尾遍历
    #[test]
    fn test_blockbuilder_restart_interval_eq_entry_count() {
        let opts = Options {
            block_restart_interval: 4,
            ..Default::default()
        };
        let mut builder = BlockBuilder::new(opts.clone());
        let keys = [b"a", b"b", b"c", b"d"];
        let values = [b"1", b"2", b"3", b"4"];
        for (i, (k, v)) in keys.iter().zip(values.iter()).enumerate() {
            let ikey = make_internal_key(*k, i as u64, *v);
            builder.add(&ikey);
        }
        let block_data = builder.finish();
        let block = Block::new(block_data);
        let mut iter = BlockIter::new(block);
        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"a");
        iter.seek_to_last().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"d");
    }
}
