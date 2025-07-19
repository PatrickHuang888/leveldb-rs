use crc32fast::Hasher;
use std::io::{Result as IoResult, Write};

/// WAL用，文件按固定大小的 block（通常 32KB）组织，如果记录太大无法放入当前 block，会自动分片处理。
/// 1.每个片段都有对应的类型标记：
/// kFullType：完整记录（不需要分片）
/// kFirstType：第一个片段
/// kMiddleType：中间片段
/// kLastType：最后一个片段
/// 2. header
/// char buf[kHeaderSize];  // kHeaderSize = 7
/// buf[0-3]: CRC32 校验码（4字节）
/// buf[4]:   长度低8位（1字节）
/// buf[5]:   长度高8位（1字节）  
/// buf[6]:   记录类型（1字节）
/// 3. Block 边界处理
/// 如果当前 block 剩余空间不足以放下 header（7字节），就切换到新 block
/// 用零填充当前 block 的剩余空间
///
///
pub(crate) const BLOCK_SIZE: u64 = 32768; // 32KB
pub(crate) const HEADER_SIZE: u64 = 7;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecordType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,

    BadRecord = 5, // 用于标记损坏的记录
}

impl RecordType {
    pub fn from_u8(n: u8) -> Option<Self> {
        match n {
            1 => Some(RecordType::Full),
            2 => Some(RecordType::First),
            3 => Some(RecordType::Middle),
            4 => Some(RecordType::Last),
            5 => Some(RecordType::BadRecord),
            _ => None,
        }
    }
}

pub struct LogWriter<W: Write> {
    writer: W,
    block_offset: u64, // 当前 block 内的偏移量
}

impl<W: Write> LogWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            block_offset: 0,
        }
    }

    pub fn add_record(&mut self, data: &[u8]) -> IoResult<()> {
        let mut ptr = data;
        let mut begin = true;

        // Handle empty record case
        if ptr.is_empty() {
            let leftover = BLOCK_SIZE - self.block_offset;
            if leftover < HEADER_SIZE {
                self.fill_with_zeros(leftover as usize)?;
                self.block_offset = 0;
            }
            self.emit_physical_record(RecordType::Full, &[])?;
            return Ok(());
        }

        while !ptr.is_empty() {
            let leftover = BLOCK_SIZE - self.block_offset;

            // 如果当前 block 剩余空间不足以放下 header，切换到新 block
            if leftover < HEADER_SIZE {
                self.fill_with_zeros(leftover as usize)?;
                self.block_offset = 0;
            }

            let available = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let fragment_length = std::cmp::min(ptr.len(), available as usize);

            let record_type = if begin && fragment_length == ptr.len() {
                RecordType::Full
            } else if begin {
                RecordType::First
            } else if fragment_length == ptr.len() {
                RecordType::Last
            } else {
                RecordType::Middle
            };

            self.emit_physical_record(record_type, &ptr[..fragment_length])?;
            ptr = &ptr[fragment_length..];
            begin = false;
        }

        Ok(())
    }

    fn emit_physical_record(&mut self, record_type: RecordType, data: &[u8]) -> IoResult<()> {
        let length = data.len();
        let mut header = [0u8; HEADER_SIZE as usize];

        // 计算 CRC32
        let mut hasher = Hasher::new();
        hasher.update(&[record_type as u8]);
        hasher.update(data);
        let crc = hasher.finalize();

        // 填充 header
        header[0..4].copy_from_slice(&crc.to_le_bytes());
        header[4] = (length & 0xff) as u8;
        header[5] = (length >> 8) as u8;
        header[6] = record_type as u8;

        // 写入 header 和 data
        self.writer.write_all(&header)?;
        self.writer.write_all(data)?;
        self.writer.flush()?;

        self.block_offset += HEADER_SIZE + length as u64;
        Ok(())
    }

    fn fill_with_zeros(&mut self, count: usize) -> IoResult<()> {
        let zeros = vec![0u8; count];
        self.writer.write_all(&zeros)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::log_reader::{LogReader, Reporter};
    use std::io::{Cursor, Seek, SeekFrom};

    struct TestReporter {}
    impl Reporter for TestReporter {
        fn corruption(&mut self, bytes: u64, reason: &str) {
            println!("Corruption reported: {}, skip {} bytes", reason, bytes);
        }
    }

    fn roundtrip(records: Vec<&[u8]>) -> Vec<Vec<u8>> {
        let mut buf = Cursor::new(Vec::new());
        {
            let mut writer = LogWriter::new(&mut buf);
            for rec in &records {
                writer.add_record(rec).unwrap();
            }
        }
        buf.seek(SeekFrom::Start(0)).unwrap();
        let reporter = Box::new(TestReporter {});
        let mut reader = LogReader::new(&mut buf, Some(reporter), 0).unwrap();
        let mut result = Vec::new();
        while let Some(rec) = reader.read_record().unwrap() {
            result.push(rec);
        }
        result
    }

    #[test]
    fn test_single_record() {
        let recs = roundtrip(vec![b"hello"]);
        assert_eq!(recs, vec![b"hello".to_vec()]);
    }

    #[test]
    fn test_many_records() {
        const NUM_RECORDS: usize = 100_000;

        // 准备测试数据：100k条记录，每条16字节
        let records: Vec<Vec<u8>> = (0..NUM_RECORDS)
            .map(|i| format!("record_{:08}", i).into_bytes())
            .collect();

        // 写入所有记录
        let mut buf = Cursor::new(Vec::new());
        {
            let mut writer = LogWriter::new(&mut buf);
            for record in &records {
                writer.add_record(record).unwrap();
            }
        }

        // 读取并验证所有记录
        buf.seek(SeekFrom::Start(0)).unwrap();
        let reporter = Box::new(TestReporter {});
        let mut reader = LogReader::new(&mut buf, Some(reporter), 0).unwrap();

        let mut count = 0;
        while let Some(record) = reader.read_record().unwrap() {
            assert_eq!(record, records[count]);
            count += 1;
        }

        assert_eq!(count, NUM_RECORDS, "应该读取到100k条记录");
    }

    #[test]
    fn test_empty_record() {
        let recs = roundtrip(vec![b""]);
        assert_eq!(recs, vec![b"".to_vec()]);
    }

    #[test]
    fn test_large_record_fragmentation() {
        let big = vec![42u8; (BLOCK_SIZE * 2 + 100) as usize];
        let recs = roundtrip(vec![&big]);
        assert_eq!(recs, vec![big]);
    }

    #[test]
    fn test_mixed_records() {
        // 小记录: 10字节
        let small = b"small_data";
        // 中记录: 50KB
        let medium = vec![b'M'; 50 * 1024]; // 50KB
        // 大记录: 100KB
        let large = vec![b'L'; 100 * 1024]; // 100KB

        let recs = roundtrip(vec![small, &medium, &large, small]);

        // 验证所有记录
        assert_eq!(recs[0], small.to_vec(), "first small record");
        assert_eq!(recs[1], medium, "medium record (50KB)");
        assert_eq!(recs[2], large, "large record (100KB)");
        assert_eq!(recs[3], small.to_vec(), "second small record");
    }

    #[test]
    fn test_initial_offset_reads_next_record() {
        // 写入三条记录
        let rec1 = b"first record";
        let rec2 = b"second record";
        let rec3 = b"third record";
        let mut buf = Cursor::new(Vec::new());
        {
            let mut writer = LogWriter::new(&mut buf);
            writer.add_record(rec1).unwrap();
            writer.add_record(rec2).unwrap();
            writer.add_record(rec3).unwrap();
        }

        // offset 设置在第一条记录之后的某个位置
        let offset = HEADER_SIZE + rec1.len() as u64 + 3; // 第一条记录之后偏移3字节

        let reporter = Box::new(TestReporter {});
        let mut reader = LogReader::new(&mut buf, Some(reporter), offset).unwrap();
        // 应该直接读到第三条记录
        let rec = reader.read_record().unwrap();
        assert_eq!(rec, Some(rec3.to_vec()));
        // 再次读取应为 None
        assert_eq!(reader.read_record().unwrap(), None);
    }

    #[test]
    fn test_initial_offset_with_spanning_record() {
        // 第一条记录小
        let rec1 = b"first record";
        // 第二条记录跨 block
        let rec2 = vec![b'2'; (BLOCK_SIZE + 100) as usize];
        // 第三条记录小
        let rec3 = b"third record";

        let mut buf = Cursor::new(Vec::new());
        {
            let mut writer = LogWriter::new(&mut buf);
            writer.add_record(rec1).unwrap();
            writer.add_record(&rec2).unwrap();
            writer.add_record(rec3).unwrap();
        }

        // offset 设置在第一条记录之后的某个位置
        let offset = HEADER_SIZE + rec1.len() as u64 + 4; // 包含 CRC32

        let reporter = Box::new(TestReporter {});
        let mut reader = LogReader::new(&mut buf, Some(reporter), offset).unwrap();

        // 应该能正确读到第三条记录
        let rec = reader.read_record().unwrap();
        assert_eq!(rec, Some(rec3.to_vec()));
        // 再次读取应为 None
        assert_eq!(reader.read_record().unwrap(), None);
    }
}
