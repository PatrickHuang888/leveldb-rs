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

use crate::util::{decode_varint32, encode_varint32};
use core::num;
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
        let mut restarts = Vec::new();
        restarts.push(0); // 第一个 restart 点
        BlockBuilder {
            buffer: Vec::new(),
            restarts,
            counter: 0,
            last_key: Vec::new(),
            options,
        }
    }

    pub fn add(&mut self, key: &InternalKey) {
        let key_bytes = key.encode();
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

pub struct Block<'a> {
    data: &'a [u8],
    restarts: &'a [u8],
    num_restarts: usize,
}

impl<'a> Block<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        let len = data.len();
        let num_restarts = util::decode_fixed32(&data[len - 4..]).unwrap() as usize;
        //let num_restarts = u32::from_le_bytes(data[len - 4..].try_into().unwrap()) as usize;
        let restarts_start = len - 4 - num_restarts * 4;
        let restarts = &data[restarts_start..len - 4];
        Block {
            data: &data[..restarts_start],
            restarts,
            num_restarts,
        }
    }
}
pub struct BlockIter<'a> {
    block: &'a Block<'a>,
    offset: usize,
    key: Option<InternalKey>,
    value_range: Option<(usize, usize)>,
    valid: bool,
    last_key: Option<InternalKey>,
}

impl<'a> BlockIter<'a> {
    pub fn new(block: &'a Block<'a>) -> Self {
        let mut iter = BlockIter {
            block,
            offset: 0,
            key: None,
            value_range: None,
            valid: false,
            last_key: None,
        };
        iter.seek_to_first();
        iter
    }

    fn parse_entry(&mut self, offset: usize) -> Option<(usize, InternalKey, (usize, usize))> {
        let data = &self.block.data;
        if offset >= data.len() {
            return None;
        }
        let (shared, n1) = util::decode_varint32(&data[offset..])?;
        let (non_shared, n2) = util::decode_varint32(&data[offset + n1..])?;
        let (value_len, n3) = util::decode_varint32(&data[offset + n1 + n2..])?;
        let key_start = offset + n1 + n2 + n3;
        let key_end = key_start + non_shared as usize;
        let value_start = key_end;
        let value_end = value_start + value_len as usize;
        let mut key_bytes = match &self.last_key {
            Some(last) => {
                let mut v = last.encode();
                v.truncate(shared as usize);
                v
            }
            None => Vec::new(),
        };
        key_bytes.extend_from_slice(&data[key_start..key_end]);
        let key = InternalKey::decode(&key_bytes)?;
        Some((value_end, key, (value_start, value_end)))
    }

    fn entry_offset_by_restart(&self, restart_idx: usize) -> usize {
        let off = &self.block.restarts[restart_idx * 4..restart_idx * 4 + 4];
        u32::from_le_bytes(off.try_into().unwrap()) as usize
    }
}

impl<'a> InternalIterator for BlockIter<'a> {
    fn seek(&mut self, target: &InternalKey) {
        // 二分查找 restart 点
        let mut left = 0;
        let mut right = self.block.num_restarts;
        while left < right {
            let mid = (left + right) / 2;
            let off = self.entry_offset_by_restart(mid);
            let tmp_last_key: Option<InternalKey> = None;
            let (shared, n1) = util::decode_varint32(&self.block.data[off..]).unwrap();
            let (non_shared, n2) = util::decode_varint32(&self.block.data[off + n1..]).unwrap();
            let (_value_len, n3) =
                util::decode_varint32(&self.block.data[off + n1 + n2..]).unwrap();
            let key_start = off + n1 + n2 + n3;
            let key_end = key_start + non_shared as usize;
            let mut key_bytes = match &tmp_last_key {
                Some(last) => {
                    let mut v = last.encode();
                    v.truncate(shared as usize);
                    v
                }
                None => Vec::new(),
            };
            key_bytes.extend_from_slice(&self.block.data[key_start..key_end]);
            let key = InternalKey::decode(&key_bytes).unwrap();
            match key.cmp(target) {
                std::cmp::Ordering::Less => left = mid + 1,
                _ => right = mid,
            }
        }
        // 线性扫描
        let mut offset = if left == self.block.num_restarts {
            self.entry_offset_by_restart(self.block.num_restarts - 1)
        } else {
            self.entry_offset_by_restart(left)
        };
        self.last_key = None;
        loop {
            if let Some((next_offset, key, value_range)) = self.parse_entry(offset) {
                match key.cmp(target) {
                    std::cmp::Ordering::Less => {
                        offset = next_offset;
                        self.last_key = Some(key);
                    }
                    _ => {
                        self.key = Some(key.clone());
                        self.value_range = Some(value_range);
                        self.offset = next_offset;
                        self.valid = true;
                        self.last_key = Some(key);
                        return;
                    }
                }
            } else {
                self.valid = false;
                return;
            }
        }
    }

    fn seek_to_first(&mut self) {
        self.offset = 0;
        self.last_key = None;
        if let Some((next_offset, key, value_range)) = self.parse_entry(self.offset) {
            self.key = Some(key.clone());
            self.value_range = Some(value_range);
            self.offset = next_offset;
            self.valid = true;
            self.last_key = Some(key);
        } else {
            self.valid = false;
        }
    }
    fn seek_to_last(&mut self) {
        // 定位到最后一个 restart 点
        if self.block.num_restarts == 0 {
            self.valid = false;
            return;
        }
        let mut restart_idx = self.block.num_restarts - 1;
        let mut offset = self.entry_offset_by_restart(restart_idx);
        self.last_key = None;
        let mut last_valid = None;
        while let Some((next_offset, key, value_range)) = self.parse_entry(offset) {
            last_valid = Some((offset, key.clone(), value_range));
            offset = next_offset;
            self.last_key = Some(key);
        }
        if let Some((off, key, value_range)) = last_valid {
            self.offset = off;
            self.key = Some(key);
            self.value_range = Some(value_range);
            self.valid = true;
        } else {
            self.valid = false;
        }
    }
    fn valid(&self) -> bool {
        self.valid
    }
    fn key(&self) -> Option<&InternalKey> {
        self.key.as_ref()
    }
    fn next(&mut self) {
        if !self.valid {
            return;
        }
        let offset = self.offset;
        if let Some((next_offset, key, value_range)) = self.parse_entry(offset) {
            self.key = Some(key.clone());
            self.value_range = Some(value_range);
            self.offset = next_offset;
            self.valid = true;
            self.last_key = Some(key);
        } else {
            self.valid = false;
        }
    }

    fn prev(&mut self) {
        // 只能从 restart 点往前线性扫描
        if !self.valid {
            return;
        }
        // 找到当前 entry 属于哪个 restart 区间
        let mut restart_idx = 0;
        let mut entry_offset = 0;
        for i in 0..self.block.num_restarts {
            let off = self.entry_offset_by_restart(i);
            if off >= self.offset {
                break;
            }
            restart_idx = i;
            entry_offset = off;
        }
        let mut last_valid = None;
        self.last_key = None;
        while entry_offset < self.offset {
            if let Some((next_offset, key, value_range)) = self.parse_entry(entry_offset) {
                if next_offset == self.offset {
                    break;
                }
                last_valid = Some((entry_offset, key.clone(), value_range));
                entry_offset = next_offset;
                self.last_key = Some(key);
            } else {
                break;
            }
        }
        if let Some((off, key, value_range)) = last_valid {
            self.offset = off;
            self.key = Some(key);
            self.value_range = Some(value_range);
            self.valid = true;
        } else {
            self.valid = false;
        }
    }
}
