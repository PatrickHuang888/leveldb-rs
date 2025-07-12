use crate::DBError;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};

const WRITABLE_FILE_BUFFER_SIZE: usize = 65536; // 64KB

pub struct WriteableFile {
    filename: String,
    file: File,
    buf: Vec<u8>,
    is_manifested: bool,
}

impl WriteableFile {
    pub fn new(filename: &str) -> Result<Self, DBError> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(filename)?;
        Ok(WriteableFile {
            filename: filename.to_string(),
            file,
            buf: Vec::with_capacity(WRITABLE_FILE_BUFFER_SIZE),
            is_manifested: is_manifested(filename),
        })
    }

    pub fn append(&mut self, data: &[u8]) -> Result<(), DBError> {
        // TODO:限制内存使用
        self.buf.extend_from_slice(data);
        if self.buf.len() >= WRITABLE_FILE_BUFFER_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), DBError> {
        if !self.buf.is_empty() {
            self.file.write_all(&self.buf)?;
            self.buf.clear();
        }
        self.file.flush()?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), DBError> {
        self.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn close(&mut self) -> Result<(), DBError> {
        self.flush()?;
        self.sync()?;
        // 文件关闭由 Drop 自动完成
        Ok(())
    }
}

fn is_manifested(filename: &str) -> bool {
    filename.starts_with("MANIFEST")
}

pub(crate) fn descriptor_file_name(dbname: &str, file_number: u64) -> String {
    format!("{}/MANIFEST-{:06}", dbname, file_number)
}

pub(crate) fn db_file_name(dbname: &str, file_number: u64) -> String {
    format!("{}/{:06}.ldb", dbname, file_number)
}
