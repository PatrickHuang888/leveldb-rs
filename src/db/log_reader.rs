use super::log_writer::{BLOCK_SIZE, HEADER_SIZE, RecordType};
use crc32fast::Hasher;
use std::io::{Read, Result as IoResult, Seek, SeekFrom};

pub trait Reporter {
    fn corruption(&mut self, bytes: u64, reason: &str);
}

/// Reader for reading log files in LevelDB.
/// 1. 正确读取根据 writer写在 block 里的记录的fragment，fragment 可以是完整的记录，也可以是被分割的记录的firest、middle、last部分，根据片段组装 record.
/// 2. 要求跳过损坏的记录，记录有错误（比如长度/crc）则跳过真个 block,继续下一个 block。
/// 如果是类型问题组装不起来就继续下一个 fragment,找到可以组装的为止。
/// 3. Reader 要求错误报告机制
/// 区分错误和正常 EOF
/// 截断的尾部记录 → 忽略（不报错）
/// 数据损坏 → 报告并跳过
///
/// 4. Reader 要求可以从指定位置开始读，指定位置可能在 block 开始，中间或者结尾。如果不是在 block 的开始，则要求跳过当前记录，直到找到下一个完整的记录读取。
///
/// 以为 AI 很快能搞定，结果还是手搓一个。TODO:后续还要补充测试
pub struct LogReader<R: Read + Seek> {
    reader: R,
    reporter: Option<Box<dyn Reporter>>,
    buffer: [u8; BLOCK_SIZE as usize],
    buffer_len: u64,
    offset_in_block: u64,
    current_offset: u64, // 当前读取位置
    initial_offset: u64,
    last_record_offset: u64, // 上次读取的偏移量
}

impl<R: Read + Seek> LogReader<R> {
    pub fn new(
        mut reader: R,
        reporter: Option<Box<dyn Reporter>>,
        initial_offset: u64,
    ) -> IoResult<Self> {
        let block_start = initial_offset / BLOCK_SIZE * BLOCK_SIZE;
        reader.seek(SeekFrom::Start(block_start))?;

        Ok(Self {
            reader,
            reporter,
            buffer: [0; BLOCK_SIZE as usize],
            buffer_len: 0,
            offset_in_block: 0,
            current_offset: 0,
            initial_offset,
            last_record_offset: 0,
        })
    }

    pub fn read_record(&mut self) -> IoResult<Option<Vec<u8>>> {
        let mut scratch = Vec::new();
        let mut in_fragmented_record = None;

        loop {
            if let Some((record_type, fragment)) = self.read_physical_record()? {
                let mut corruption = None;
                match record_type {
                    RecordType::Full => {
                        if in_fragmented_record.is_some() {
                            corruption = Some((
                                self.buffer_len - self.offset_in_block,
                                "partial record without end",
                            ));
                            in_fragmented_record = None;
                        } else {
                            return Ok(Some(fragment.to_vec()));
                        }
                    }

                    RecordType::First => {
                        if in_fragmented_record.is_some() {
                            corruption = Some((
                                self.buffer_len - self.offset_in_block,
                                "partial record without end",
                            ));
                        } else {
                            scratch.extend_from_slice(fragment);
                            in_fragmented_record = Some(RecordType::First);
                        }
                    }
                    RecordType::Middle => {
                        if !matches!(
                            in_fragmented_record,
                            Some(RecordType::First) | Some(RecordType::Middle)
                        ) {
                            corruption = Some((
                                self.buffer_len - self.offset_in_block,
                                "missing start of fragmented record",
                            ));
                        } else {
                            in_fragmented_record = Some(RecordType::Middle);
                            scratch.extend_from_slice(fragment);
                        }
                    }
                    RecordType::Last => {
                        if !matches!(
                            in_fragmented_record,
                            Some(RecordType::First) | Some(RecordType::Middle)
                        ) {
                            corruption = Some((
                                self.buffer_len - self.offset_in_block,
                                "missing start of fragmented record",
                            ));
                        } else {
                            scratch.extend_from_slice(fragment);
                            return Ok(Some(scratch.clone()));
                        }
                    }
                    RecordType::BadRecord => {
                        if in_fragmented_record.is_some() {
                            scratch.clear();
                            in_fragmented_record = None;
                            continue; // 跳过损坏的block, 已经 report 过了
                        }
                    }
                }

                if let Some((bytes, reason)) = corruption {
                    self.report_corruption(bytes, reason);
                    // 继续
                }
            } else {
                return Ok(None); // EOF
            }
        }
    }

    /// None 就到结尾了
    fn read_physical_record(&mut self) -> IoResult<Option<(RecordType, &[u8])>> {
        loop {
            // 需要读取新的block
            if self.offset_in_block >= self.buffer_len {
                let bytes_read = self.reader.read(&mut self.buffer[..])?;
                if bytes_read == 0 {
                    return Ok(None); // EOF
                }
                self.buffer_len = bytes_read as u64;
                self.offset_in_block = 0;
                self.last_record_offset = self.current_offset;
                self.current_offset += bytes_read as u64;
            }

            // 检查是否有足够空间读取header
            if self.offset_in_block + HEADER_SIZE > self.buffer_len {
                // 剩余空间不足，跳过填充部分到下一个block
                self.skip_to_next_block()?;
                continue;
            }

            let record_start = self.last_record_offset;
            let header = &self.buffer[self.offset_in_block as usize
                ..self.offset_in_block as usize + HEADER_SIZE as usize];
            let crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
            let length = (header[4] as u16) | ((header[5] as u16) << 8);
            let record_type_u8 = header[6];

            if let Some(record_type) = RecordType::from_u8(record_type_u8) {
                if self.offset_in_block + HEADER_SIZE + length as u64 > self.buffer_len {
                    self.report_corruption(
                        self.buffer_len - self.offset_in_block,
                        "bad record length",
                    );
                    self.skip_to_next_block()?;
                    continue;
                }

                // 校验CRC
                let mut hasher = Hasher::new();
                hasher.update(&[record_type as u8]);
                hasher.update(
                    &self.buffer[(self.offset_in_block + HEADER_SIZE) as usize
                        ..(self.offset_in_block + HEADER_SIZE + length as u64) as usize],
                );
                if hasher.finalize() != crc {
                    self.report_corruption(
                        self.buffer_len - self.offset_in_block + HEADER_SIZE,
                        "checksum mismatch",
                    );
                    self.skip_to_next_block()?;
                    continue;
                }

                let data_start = self.offset_in_block + HEADER_SIZE;
                let data_end = self.offset_in_block + HEADER_SIZE + length as u64;
                self.offset_in_block = data_end;
                self.last_record_offset = record_start + HEADER_SIZE + length as u64;

                if record_start < self.initial_offset {
                    continue; // 如果当前 block 的偏移量没到初始偏移量，继续
                }
                return Ok(Some((
                    record_type,
                    &self.buffer[data_start as usize..data_end as usize],
                )));
            } else {
                self.report_corruption(self.buffer_len - self.offset_in_block, "bad record type");
                self.skip_to_next_block()?;
                continue;
            }
        }
    }

    fn skip_to_next_block(&mut self) -> IoResult<()> {
        // 计算需要跳过的字节数以到达下一个block边界
        let offset_in_block = self.current_offset % BLOCK_SIZE;
        if offset_in_block != 0 {
            let remaining = BLOCK_SIZE - offset_in_block;
            self.reader.seek(SeekFrom::Current(remaining as i64))?;
            self.current_offset += remaining;
        }
        self.buffer_len = 0;
        self.offset_in_block = 0;
        Ok(())
    }

    fn report_corruption(&mut self, bytes: u64, reason: &str) {
        if let Some(reporter) = self.reporter.as_mut() {
            reporter.corruption(bytes, reason);
        }
    }
}
