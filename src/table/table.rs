use rand::seq::index;

/*
Table组织形式如下

1. Data Blocks（数据块）
Table 文件主体由多个 Data Block 组成，每个 Data Block 存储一段有序的 key-value 对。
Data Block 内部采用前缀压缩和 restart 点机制（见 block 结构）。
2. Filter Block（可选，布隆过滤器）
如果启用 filter policy（如 Bloom filter），会有一个 Filter Block，用于快速判断某个 key 是否可能存在于 Table 中，减少磁盘读取。
3. Metaindex Block（元索引块）
存储所有元数据块（如 Filter Block）的索引信息。
例如，记录 filter block 的名字和在文件中的位置。
4. Index Block（索引块）
存储每个 Data Block 的索引信息。
每条索引记录包含：分割 key（经过压缩优化）和对应 Data Block 的偏移量与长度（BlockHandle）。
查找时先在 Index Block 二分查找，定位到对应 Data Block，再在 Data Block 内查找具体 key。
5. Footer（文件尾部）
固定长度（48字节），包含：
Metaindex Block 的 BlockHandle（偏移和长度）
Index Block 的 BlockHandle
魔数（magic number）用于校验文件类型
6. 物理布局示意
| Data Block 1 | Data Block 2 | ... | Filter Block | Metaindex Block | Index Block | Footer |
7. 写入流程
写满一个 Data Block 就写入文件，并记录其 BlockHandle。
Index Block 记录每个 Data Block 的分割 key 和 BlockHandle。
最后写入 Filter Block、Metaindex Block、Index Block 和 Footer。

8. BlockHandle 结构
BlockHandle 在 SSTable 文件中以**变长整数（Varint64）**格式存储
[offset (varint64)][size (varint64)]


index_block 里的 key 是“data block 的分割 key”，即大于等于该 data block 所有 user_key，
且小于下一个 data block 第一个 uesr_key的最短 key，
并且尽量短。value 是对应 data block 的 BlockHandle（即 offset+size）。
*/
use crate::DBError;
use crate::db::dbformat::ValueType;
use crate::db::{InternalIterator, InternalKey};
use crate::table::block::BlockIter;
use crate::table::block::{Block, BlockBuilder};
use crate::util::{decode_varint64, encode_varint64};
use std::io::{Read, Seek};
use std::io::{SeekFrom, Write};

/// BlockHandle 结构
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    pub fn encode_to(&self, dst: &mut Vec<u8>) {
        encode_varint64(dst, self.offset);
        encode_varint64(dst, self.size);
    }
    pub fn decode_from(src: &[u8]) -> Option<(BlockHandle, usize)> {
        let (offset, n1) = decode_varint64(src)?;
        let (size, n2) = decode_varint64(&src[n1..])?;
        Some((BlockHandle { offset, size }, n1 + n2))
    }
}

/// Footer 结构
pub struct Footer {
    pub metaindex_handle: BlockHandle,
    pub index_handle: BlockHandle,
    pub magic: u64,
}

impl Footer {
    pub const SIZE: usize = 48;
    pub const MAGIC: u64 = 0xdb4775248b80fb57;

    pub fn encode_to(&self, dst: &mut Vec<u8>) {
        self.metaindex_handle.encode_to(dst);
        self.index_handle.encode_to(dst);
        dst.resize(Footer::SIZE - 8, 0);
        let mut magic_bytes = [0u8; 8];
        magic_bytes.copy_from_slice(&self.magic.to_le_bytes());
        dst.extend_from_slice(&magic_bytes);
    }
    pub fn decode_from(src: &[u8]) -> Option<Footer> {
        if src.len() < 8 {
            return None;
        }
        let (metaindex_handle, n1) = BlockHandle::decode_from(src)?;
        let (index_handle, _) = BlockHandle::decode_from(&src[n1..])?;
        let magic = u64::from_le_bytes(src[Footer::SIZE - 8..].try_into().ok()?);
        Some(Footer {
            metaindex_handle,
            index_handle,
            magic,
        })
    }
}

pub struct TableBuilder<W: Write + Seek> {
    writer: W,
    data_block_handles: Vec<BlockHandle>,
    index_block: BlockBuilder,
    filter_block: Option<BlockBuilder>,
    offset: u64,
    cur_block: BlockBuilder,
    cur_block_first_key: Option<Vec<u8>>,
    cur_block_size: usize,
    block_size_limit: usize,
    last_block_max_key: Option<Vec<u8>>,
    pending_index_entry: Option<(Vec<u8>, BlockHandle)>,
}

impl<W: Write + Seek> TableBuilder<W> {
    pub fn new(writer: W) -> Self {
        TableBuilder {
            writer,
            data_block_handles: vec![],
            index_block: BlockBuilder::new(Default::default()),
            filter_block: None,
            offset: 0,
            cur_block: BlockBuilder::new(Default::default()),
            cur_block_first_key: None,
            cur_block_size: 0,
            block_size_limit: 4096, // 4KB
            last_block_max_key: None,
            pending_index_entry: None,
        }
    }

    /// 添加 InternalKey 到内存 block，超限自动 flush
    pub fn add(&mut self, key: &InternalKey) -> Result<(), DBError> {
        if self.cur_block_first_key.is_none() {
            self.cur_block_first_key = Some(key.user_key().to_vec());
        }
        // 记录当前 block 的最大 key
        self.last_block_max_key = Some(key.user_key().to_vec());
        self.cur_block.add(key);
        self.cur_block_size += key.encode().len();
        if self.cur_block_size >= self.block_size_limit {
            self.flush_block()?;
        }
        Ok(())
    }

    pub fn flush_block(&mut self) -> Result<(), DBError> {
        if self.cur_block.is_empty() {
            return Ok(());
        }
        let first_key = self.cur_block_first_key.take().unwrap_or_default();
        let last_key = self.last_block_max_key.take().unwrap_or_default();
        let block = std::mem::replace(&mut self.cur_block, BlockBuilder::new(Default::default()));
        self.cur_block_size = 0;
        let block_handle = self.add_data_block(block)?;
        // 写 index block entry（如果有上一个 block）
        if let Some((prev_max_key, prev_handle)) = self.pending_index_entry.take() {
            let sep = shortest_separator(&prev_max_key, &first_key);
            self.index_block.add_handle(&sep, &prev_handle);
        }
        self.pending_index_entry = Some((last_key, block_handle));
        Ok(())
    }

    fn add_data_block(&mut self, block: BlockBuilder) -> Result<BlockHandle, DBError> {
        let block_data = block.finish();
        let offset = self.offset;
        self.writer.write_all(&block_data)?;
        let size = block_data.len() as u64;
        self.offset += size;
        let handle = BlockHandle { offset, size };
        self.data_block_handles.push(handle.clone());
        Ok(handle)
    }

    /// 写入所有元数据和 Footer, 返回 table 的总长度
    pub fn finish(mut self) -> Result<u64, DBError> {
        self.flush_block()?;
        // 最后一个 block 的 index entry
        if let Some((last_max_key, last_handle)) = self.pending_index_entry.take() {
            let sep = shortest_successor(&last_max_key);
            self.index_block.add_handle(&sep, &last_handle);
        }
        let index_block_data = self.index_block.finish();
        let index_offset = self.offset;
        self.writer.write_all(&index_block_data)?;
        let index_handle = BlockHandle {
            offset: index_offset,
            size: index_block_data.len() as u64,
        };
        self.offset += index_block_data.len() as u64;
        let footer = Footer {
            metaindex_handle: BlockHandle { offset: 0, size: 0 },
            index_handle,
            magic: Footer::MAGIC,
        };
        let mut footer_bytes = Vec::new();
        footer.encode_to(&mut footer_bytes);
        self.writer.write_all(&footer_bytes)?;
        self.offset += footer_bytes.len() as u64;
        Ok(self.offset)
    }
}

/// Table 用于读取 Table 文件
pub struct Table<R: Read + Seek> {
    reader: R,
    metaindex_handle: BlockHandle,

    index_block: Block,
}

impl<R: Read + Seek> Table<R> {
    pub fn open(mut reader: R) -> Result<Self, DBError> {
        // 读取 Footer
        reader.seek(SeekFrom::End(-(Footer::SIZE as i64)))?;
        let mut footer_bytes = vec![0u8; Footer::SIZE];
        reader.read_exact(&mut footer_bytes)?;
        let footer = Footer::decode_from(&footer_bytes)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad footer"))?;
        let index_block = Table::read_block(&mut reader, &footer.index_handle)?;
        Ok(Table {
            reader,
            metaindex_handle: footer.metaindex_handle,
            index_block,
        })
    }

    /// 读取指定 Block
    pub fn read_block(reader: &mut R, handle: &BlockHandle) -> Result<Block, DBError> {
        reader.seek(SeekFrom::Start(handle.offset))?;
        let mut buf = vec![0u8; handle.size as usize];
        reader.read_exact(&mut buf)?;
        Ok(Block::new(buf))
    }

    pub fn get(&mut self, key: &InternalKey) -> Result<Option<InternalKey>, DBError> {
        // 使用 index_block 查找对应的 data block
        let mut index_iter = BlockIter::new(self.index_block.clone());
        index_iter.seek(key)?;
        if !index_iter.valid() {
            return Ok(None);
        }
        let handle_bytes = index_iter
            .key()
            .as_ref()
            .ok_or(DBError::Corruption)?
            .value();
        let mut handle_slice = handle_bytes;
        let (handle, _) = BlockHandle::decode_from(&mut handle_slice).ok_or(DBError::Corruption)?;
        // 读取对应的 data block
        let data_block = Table::read_block(&mut self.reader, &handle)?;
        // 在 data block 中查找 key
        let mut data_iter = BlockIter::new(data_block);
        data_iter.seek(key)?;
        if data_iter.valid() {
            let ikey = data_iter.key().unwrap();
            if ikey.user_key() == key.user_key() {
                return Ok(Some(ikey.clone()));
            }
        }
        Ok(None)
    }
    // 可扩展：查找 key、读取 filter block、metaindex block 等
}

// 工具函数：shortest_separator/shortest_successor
fn shortest_separator(a: &[u8], b: &[u8]) -> Vec<u8> {
    let min_len = a.len().min(b.len());
    for i in 0..min_len {
        if a[i] != b[i] {
            let mut sep = a[..=i].to_vec();
            if a[i] < 0xff && a[i] + 1 < b[i] {
                sep[i] += 1;
                sep.truncate(i + 1);
                return sep;
            }
            break;
        }
    }
    a.to_vec()
}
fn shortest_successor(key: &[u8]) -> Vec<u8> {
    let mut res = key.to_vec();
    for i in 0..res.len() {
        if res[i] != 0xff {
            res[i] += 1;
            res.truncate(i + 1);
            return res;
        }
    }
    res
}

pub(crate) struct TableIterator<R: Read + Seek> {
    t: Table<R>,
    index_iter: BlockIter,
    data_iter: Option<BlockIter>,
    valid: bool,
    data_block_handle: Option<BlockHandle>,
}
impl<R: Read + Seek> TableIterator<R> {
    pub(crate) fn new(t: Table<R>) -> Self {
        let index_iter = BlockIter::new(t.index_block.clone());
        TableIterator {
            t,
            index_iter,
            data_iter: None,
            valid: false,
            data_block_handle: None,
        }
    }

    fn init_data_block(&mut self) -> Result<(), DBError> {
        if !self.index_iter.valid() {
            self.valid = false;
            self.data_iter = None;
            return Ok(());
        }

        if let Some((handle, _)) = BlockHandle::decode_from(self.index_iter.key().unwrap().value())
        {
            let need_reload = !matches!(self.data_block_handle.as_ref(), Some(h) if self.data_iter.is_some() && h == &handle);
            if need_reload {
                let data_block = Table::read_block(&mut self.t.reader, &handle)
                    .inspect_err(|_| self.valid = false)?;
                self.data_iter = Some(BlockIter::new(data_block));
                self.data_block_handle = Some(handle);
            }
            Ok(())
        } else {
            self.valid = false;
            Err(DBError::Corruption)
        }
    }

    fn skip_data_block_backward(&mut self) -> Result<(), DBError> {
        while self.data_iter.is_some() && (!self.data_iter.as_ref().unwrap().valid()) {
            // 用在 prev 里到头了？
            if !self.index_iter.valid() {
                self.valid = false;
                return Ok(());
            }
            self.index_iter.prev()?;
            self.init_data_block()?;
            if let Some(data_iter) = self.data_iter.as_mut() {
                data_iter.seek_to_last()?;
                self.valid = data_iter.valid();
            }
        }
        Ok(())
    }

    fn skip_data_block_forward(&mut self) -> Result<(), DBError> {
        while self.data_iter.is_some() && (!self.data_iter.as_ref().unwrap().valid()) {
            // 用在 next 里到尾了？
            if !self.index_iter.valid() {
                self.valid = false;
                return Ok(());
            }
            self.index_iter.next()?;
            self.init_data_block()?;
            if let Some(data_iter) = self.data_iter.as_mut() {
                data_iter.seek_to_first()?;
                self.valid = data_iter.valid();
            }
        }
        Ok(())
    }
}

impl<R: Read + Seek> InternalIterator for TableIterator<R> {
    fn seek(&mut self, key: &InternalKey) -> Result<(), DBError> {
        self.index_iter.seek(key)?;
        self.init_data_block()?;
        if let Some(data_iter) = self.data_iter.as_mut() {
            data_iter.seek(key)?;
            self.valid = data_iter.valid();
        }
        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<(), DBError> {
        self.index_iter.seek_to_first()?;
        self.init_data_block()?;
        if let Some(data_iter) = self.data_iter.as_mut() {
            data_iter.seek_to_first()?;
            self.valid = data_iter.valid();
        }
        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<(), DBError> {
        self.index_iter.seek_to_last()?;
        self.init_data_block()?;
        if let Some(data_iter) = self.data_iter.as_mut() {
            data_iter.seek_to_last()?;
            self.valid = data_iter.valid();
        }
        Ok(())
    }

    fn valid(&self) -> bool {
        self.valid
    }

    fn key(&self) -> Option<&InternalKey> {
        self.data_iter.as_ref().and_then(|it| it.key())
    }

    fn next(&mut self) -> Result<(), DBError> {
        if let Some(data_iter) = self.data_iter.as_mut() {
            data_iter.next()?;
            if data_iter.valid() {
                self.valid = true;
                return Ok(());
            } else {
                // 如果 data_iter 无效，尝试跳过到下一个 index entry
                self.skip_data_block_forward()?;
            }
        } else {
            self.valid = false;
        }
        Ok(())
    }

    fn prev(&mut self) -> Result<(), DBError> {
        assert!(self.valid, "prev called on invalid iterator");
        if let Some(data_iter) = self.data_iter.as_mut() {
            data_iter.prev()?;
            if data_iter.valid() {
                self.valid = true;
                return Ok(());
            } else {
                // 如果 data_iter 无效，尝试回退到上一个 index entry
                self.skip_data_block_backward()?;
            }
        } else {
            self.valid = false;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::InternalIterator;
    use crate::db::InternalKey;
    use crate::db::dbformat::ValueType;
    use std::io::Cursor;

    fn make_internal_key(user_key: &[u8], seq: u64, value: &[u8]) -> InternalKey {
        InternalKey::new(user_key, seq, ValueType::TypeValue, value)
    }

    #[test]
    fn test_table_builder_and_reader() {
        let mut buf = Vec::new();
        {
            let mut builder = TableBuilder::new(Cursor::new(&mut buf));
            let keys = [b"a", b"b", b"c", b"d"];
            let values = [b"1", b"2", b"3", b"4"];
            for (i, (k, v)) in keys.iter().zip(values.iter()).enumerate() {
                let ikey = make_internal_key(*k, i as u64, *v);
                builder.add(&ikey).unwrap();
            }
            builder.finish().unwrap();
        }
        // 读回 Table
        let mut table = Table::open(Cursor::new(&buf)).unwrap();
        // 直接使用 table.index_block
        let index_block = table.index_block.clone();
        assert!(!index_block.is_empty());
        // 读取第一个 data block
        let mut iter = crate::table::block::BlockIter::new(index_block);
        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        let handle_bytes = iter.key().unwrap().value();
        let mut handle_slice = handle_bytes;
        let (handle, _) = BlockHandle::decode_from(&mut handle_slice).unwrap();
        let data_block = Table::read_block(&mut table.reader, &handle).unwrap();
        // 校验 data block 内容
        let mut iter = crate::table::block::BlockIter::new(data_block);
        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        let k = iter.key().unwrap();
        assert_eq!(k.user_key(), b"a");
        assert_eq!(k.value(), b"1");
    }

    #[test]
    fn test_table_iterator_empty() {
        // 空表
        let mut buf = Vec::new();
        {
            let builder = TableBuilder::new(Cursor::new(&mut buf));
            builder.finish().unwrap();
        }
        let table = Table::open(Cursor::new(&buf)).unwrap();
        let mut iter = TableIterator::new(table);
        // 空表 valid 应为 false
        assert!(!iter.valid());
        assert!(iter.seek_to_first().is_ok());
        assert!(!iter.valid());
        assert!(iter.seek_to_last().is_ok());
        assert!(!iter.valid());
    }

    #[test]
    fn test_table_iterator_forward_and_backward() {
        let mut buf = Vec::new();
        let keys = [b"a", b"b", b"c", b"d"];
        let values = [b"1", b"2", b"3", b"4"];
        {
            let mut builder = TableBuilder::new(Cursor::new(&mut buf));
            for (i, (k, v)) in keys.iter().zip(values.iter()).enumerate() {
                let ikey = make_internal_key(*k, i as u64, *v);
                builder.add(&ikey).unwrap();
            }
            builder.finish().unwrap();
        }
        let table = Table::open(Cursor::new(&buf)).unwrap();
        let mut iter = TableIterator::new(table);
        // 正序遍历
        iter.seek_to_first().unwrap();
        let mut result = Vec::new();
        while iter.valid() {
            let k = iter.key().unwrap();
            result.push((k.user_key().to_vec(), k.value().to_vec()));
            iter.next().unwrap();
        }
        assert_eq!(
            result,
            keys.iter()
                .zip(values.iter())
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect::<Vec<_>>()
        );
        // 逆序遍历
        iter.seek_to_last().unwrap();
        let mut result = Vec::new();
        while iter.valid() {
            let k = iter.key().unwrap();
            result.push((k.user_key().to_vec(), k.value().to_vec()));
            iter.prev().unwrap();
        }
        let mut expect = keys
            .iter()
            .zip(values.iter())
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect::<Vec<_>>();
        expect.reverse();
        assert_eq!(result, expect);
    }

    #[test]
    fn test_table_iterator_seek_with_next_prev() {
        let mut buf = Vec::new();
        let keys = [b"a", b"c", b"e", b"g"];
        let values = [b"1", b"2", b"3", b"4"];
        {
            let mut builder = TableBuilder::new(Cursor::new(&mut buf));
            for (i, (k, v)) in keys.iter().zip(values.iter()).enumerate() {
                let ikey = make_internal_key(*k, i as u64, *v);
                builder.add(&ikey).unwrap();
            }
            builder.finish().unwrap();
        }
        let table = Table::open(Cursor::new(&buf)).unwrap();
        let mut iter = TableIterator::new(table);

        // seek 到 "c"，next 应该到 "e"，prev 应该到 "a"
        let ikey = make_internal_key(b"c", 0, b"");
        iter.seek(&ikey).unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"c");

        iter.next().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"e");

        iter.prev().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"c");

        iter.prev().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"a");

        // seek 到 "b"，应该到 "c"，next 到 "e"，prev 到 "a"
        let ikey = make_internal_key(b"b", 0, b"");
        iter.seek(&ikey).unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"c");

        iter.next().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"e");

        iter.prev().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"c");

        iter.prev().unwrap();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap().user_key(), b"a");

        // seek 到 "z"，应该 invalid
        let ikey = make_internal_key(b"z", 0, b"");
        iter.seek(&ikey).unwrap();
        assert!(!iter.valid());
    }

    #[test]
    fn test_table_iterator() {
        let mut buf = Vec::new();
        {
            let mut builder = TableBuilder::new(Cursor::new(&mut buf));
            let keys = [b"a", b"b", b"c", b"d", b"e"];
            let values = [b"1", b"2", b"3", b"4", b"5"];
            for (i, (k, v)) in keys.iter().zip(values.iter()).enumerate() {
                let ikey = make_internal_key(*k, i as u64, *v);
                builder.add(&ikey).unwrap();
            }
            builder.finish().unwrap();
        }
        let table = Table::open(Cursor::new(&buf)).unwrap();
        let mut iter = TableIterator::new(table);

        // 正序遍历
        iter.seek_to_first().unwrap();
        let mut results = Vec::new();
        while iter.valid() {
            let k = iter.key().unwrap();
            results.push((k.user_key().to_vec(), k.value().to_vec()));
            iter.next().unwrap();
        }
        assert_eq!(
            results,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
                (b"d".to_vec(), b"4".to_vec()),
                (b"e".to_vec(), b"5".to_vec()),
            ]
        );

        // 逆序遍历
        iter.seek_to_last().unwrap();
        let mut rev_results = Vec::new();
        while iter.valid() {
            let k = iter.key().unwrap();
            rev_results.push((k.user_key().to_vec(), k.value().to_vec()));
            iter.prev().unwrap();
        }
        assert_eq!(
            rev_results,
            vec![
                (b"e".to_vec(), b"5".to_vec()),
                (b"d".to_vec(), b"4".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"a".to_vec(), b"1".to_vec()),
            ]
        );

        // seek 精确命中
        iter.seek(&make_internal_key(b"c", 2, b"3")).unwrap();
        assert!(iter.valid());
        let k = iter.key().unwrap();
        assert_eq!(k.user_key(), b"c");
        assert_eq!(k.value(), b"3");

        // seek 到不存在 key，应该到下一个更大 key
        iter.seek(&make_internal_key(b"bb", 0, b"")).unwrap();
        assert!(iter.valid());
        let k = iter.key().unwrap();
        assert_eq!(k.user_key(), b"c");
        assert_eq!(k.value(), b"3");

        // seek 超过最大 key
        iter.seek(&make_internal_key(b"z", 0, b"")).unwrap();
        assert!(!iter.valid());
    }
}
