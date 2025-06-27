use crate::db::InternalKey;
use crate::table::BL_MAX_ENCODED_LENGTH;
use crate::table::BlockHandle;
use crate::table::block::BlockBuilder;
use crate::util;
use crate::util::file::WriteableFile;
use crate::{CompressionType, DBError, Options};

const BLOCK_TRAILER_SIZE: usize = 5; // 块尾部大小，包含压缩类型和CRC校验

pub(super) struct TableBuilder {
    data_block: BlockBuilder,  // 用于存储数据块
    index_block: BlockBuilder, // 用于存储索引块
    num_entries: usize,        // 记录添加的键值对数量
    last_key: InternalKey,     // 用于存储上一个键

    options: crate::Options, // 配置选项
    index_block_options: crate::Options,

    // We do not emit the index entry for a block until we have seen the
    // first key for the next data block.  This allows us to use shorter
    // keys in the index block.  For example, consider a block boundary
    // between the keys "the quick brown fox" and "the who".  We can use
    // "the r" as the key for the index block entry since it is >= all
    // entries in the first block and < all entries in subsequent
    // blocks.
    //
    // Invariant: r->pending_index_entry is true only if data_block is empty.
    pending_index_entry: bool,
    pending_handle: BlockHandle, // Handle to add to index block

    file: WriteableFile,
    offset: usize,              // 记录当前偏移量
    compressed_output: Vec<u8>, // 用于存储压缩后的输出
}

impl TableBuilder {
    pub fn new(options: Options, f: WriteableFile) -> Self {
        let mut index_block_options = options.clone();
        index_block_options.block_restart_interval = 1; // 索引块的重启间隔设置为1
        TableBuilder {
            data_block: BlockBuilder::new(options.clone()),
            index_block: BlockBuilder::new(index_block_options.clone()),
            num_entries: 0,
            last_key: InternalKey::default(),
            options,
            index_block_options, // 索引块的选项
            pending_index_entry: false,
            pending_handle: BlockHandle::new(), // 初始化待处理的块句柄
            file: f,                            // 初始化可写文件
            offset: 0,                          // 初始化偏移量
            compressed_output: Vec::new(),      // 初始化压缩输出
        }
    }

    pub fn add(&mut self, key: &InternalKey) -> Result<(), DBError> {
        if self.num_entries > 0 {
            assert!(key > &self.last_key, "Keys must be added in sorted order");
        }

        if self.pending_index_entry {
            assert!(
                self.data_block.is_empty(),
                "Data block must be empty when pending index entry is true"
            );
            self.last_key = key.find_shortest_seperator(&self.last_key);
            let mut handle_encoding = Vec::new();
            self.pending_handle.encode_to(&mut handle_encoding);
            self.last_key.set_value(handle_encoding.as_ref());
            self.index_block.add(&self.last_key);
            self.pending_index_entry = false; // 重置标志
        }

        self.last_key = key.clone();
        self.num_entries += 1;
        self.data_block.add(key);

        let estimated_block_size = self.data_block.current_size_estimate();
        if estimated_block_size > self.options.block_size {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), DBError> {
        if self.data_block.is_empty() {
            return Ok(());
        }
        assert!(
            !self.pending_index_entry,
            "Pending index entry must be false before flushing"
        );
        // 取出 data_block 的所有权
        let old_block = std::mem::replace(
            &mut self.data_block,
            BlockBuilder::new(self.options.clone()),
        );
        let contents = old_block.finish();
        self.write_data_block(&contents)?;
        Ok(())
    }

    fn write_data_block(&mut self, raw: &[u8]) -> Result<(), DBError> {
        let mut handle = BlockHandle::new();
        self.write_block(raw, &mut handle)?;
        self.pending_handle = handle; // 更新待处理的块句柄
        self.pending_index_entry = true; // 设置待处理索引条目标志
        Ok(())
    }

    fn write_index_block(
        &mut self,
        contents: &[u8],
        handle: &mut BlockHandle,
    ) -> Result<(), DBError> {
        self.write_block(contents, handle)?;
        Ok(())
    }

    fn write_block(&mut self, raw: &[u8], handle: &mut BlockHandle) -> Result<(), DBError> {
        let mut block_data = Vec::with_capacity(raw.len() + BLOCK_TRAILER_SIZE);
        let mut c_type = self.options.compression;
        match self.options.compression {
            crate::CompressionType::SNAPPY_COMPRESSION => {
                let compressed_data = snappy::compress(raw);
                if compressed_data.len() < raw.len() - raw.len() / 8 {
                    block_data.extend_from_slice(compressed_data.as_ref());
                } else {
                    block_data.extend_from_slice(raw);
                    c_type = crate::CompressionType::NO_COMPRESSION;
                }
            }
            crate::CompressionType::NO_COMPRESSION => {
                block_data.extend_from_slice(raw);
            }
            crate::CompressionType::ZSTD_COMPRESSION => {
                let compressed_data = zstd::encode_all(raw, 0)?;
                if compressed_data.len() < raw.len() - raw.len() / 8 {
                    block_data.extend_from_slice(compressed_data.as_ref());
                } else {
                    block_data.extend_from_slice(raw);
                    c_type = crate::CompressionType::NO_COMPRESSION;
                }
            }
            _ => {
                return Err(DBError::NotSupported);
            }
        }
        self.write_raw_block(&block_data, c_type, handle)?;
        Ok(())
    }

    fn write_raw_block(
        &mut self,
        block: &[u8],
        c_type: CompressionType,
        handle: &mut BlockHandle,
    ) -> Result<(), DBError> {
        handle.set_offset(self.offset);
        handle.set_size(block.len());

        self.file.append(block)?;
        let mut trailer = [0u8; BLOCK_TRAILER_SIZE];
        trailer[0] = c_type as u8; // 这里可以设置块类型
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&block);
        hasher.update(&trailer[..1]);
        // TODO: crc mask
        let crc = hasher.finalize();
        util::encode_fixed32_slice(&mut trailer[1..], crc);
        self.file.append(&trailer)?;
        self.offset += block.len() + BLOCK_TRAILER_SIZE;
        Ok(())
    }

    fn finish(mut self) -> Result<(), DBError> {
        self.flush()?; // 确保最后的数据块被刷新

        let mut metaindex_handle = BlockHandle::new();
        let mut index_handle = BlockHandle::new();

        let meta_index_block = BlockBuilder::new(self.options.clone());
        let contents = meta_index_block.finish();
        self.write_index_block(&contents, &mut metaindex_handle)?;

        // 取出 index_block 的所有权
        let mut index_block = std::mem::replace(
            &mut self.index_block,
            BlockBuilder::new(self.index_block_options.clone()),
        );

        if self.pending_index_entry {
            let mut handle_encoding = Vec::new();
            self.pending_handle.encode_to(&mut handle_encoding);
            self.last_key.set_value(handle_encoding.as_ref());
            index_block.add(&self.last_key);
        }
        let index_contents = index_block.finish();
        self.write_index_block(&index_contents, &mut index_handle)?;

        // write footer
        let footer = Footer {
            metaindex_handle,
            index_handle,
        };
        let mut footer_buf = Vec::with_capacity(FOOTER_SIZE);
        footer.encode_to(&mut footer_buf);
        self.file.append(&footer_buf)?;
        self.offset += footer_buf.len();
        Ok(())
    }
}

struct Footer {
    metaindex_handle: BlockHandle, // 元数据索引块的句柄
    index_handle: BlockHandle,     // 索引块的句柄
}

impl Footer {
    fn new() -> Self {
        Footer {
            metaindex_handle: BlockHandle::new(),
            index_handle: BlockHandle::new(),
        }
    }

    fn decode_from(buf: &[u8]) -> Result<Self, DBError> {
        if buf.len() < FOOTER_SIZE {
            return Err(DBError::Corruption);
        }
        let (metaindex_handle, offset_size) = BlockHandle::decode_from(buf)?;
        let (index_handle, _) = BlockHandle::decode_from(&buf[offset_size..])?;
        if util::decode_fixed32(&buf[offset_size + offset_size..]) != Some(0x0f1e2d3c) {
            return Err(DBError::Corruption);
        }
        Ok(Footer {
            metaindex_handle,
            index_handle,
        })
    }

    fn encode_to(&self, buf: &mut Vec<u8>) {
        self.metaindex_handle.encode_to(buf);
        self.index_handle.encode_to(buf);
        buf.resize(2 * BL_MAX_ENCODED_LENGTH, 0);
        util::encode_fixed64(buf, 0xdb4775248b80fb57); // Magic number for footer        
        assert!(buf.len() == FOOTER_SIZE, "Footer size mismatch");
    }
}

// Encoded length of a Footer.  Note that the serialization of a
// Footer will always occupy exactly this many bytes.  It consists
// of two block handles and a magic number.
const FOOTER_SIZE: usize = BL_MAX_ENCODED_LENGTH * 2 + 8;
