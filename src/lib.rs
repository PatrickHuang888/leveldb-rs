pub mod db;
pub mod table;
pub(crate) mod util;

#[derive(Clone)]
pub struct Options {
    // Number of keys between restart points for delta encoding of keys.
    // This parameter can be changed dynamically.  Most clients should
    // leave this parameter alone.
    pub block_restart_interval: usize,
    // Approximate size of user data packed per block.  Note that the
    // block size specified here corresponds to uncompressed data.  The
    // actual size of the unit read from disk may be smaller if
    // compression is enabled.  This parameter can be changed dynamically.
    pub block_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            block_restart_interval: 16,
            block_size: 4096,
        }
    }
}

#[derive(Debug)]
pub enum DBError {
    NotFound,
    NotSupported,
    Corruption,
    IoError(std::io::Error),
}

impl From<std::io::Error> for DBError {
    fn from(e: std::io::Error) -> Self {
        DBError::IoError(e)
    }
}

impl std::fmt::Display for DBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBError::NotFound => write!(f, "Not found"),
            DBError::NotSupported => write!(f, "Not supported"),
            DBError::Corruption => write!(f, "Corruption"),
            DBError::IoError(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl Clone for DBError {
    fn clone(&self) -> Self {
        match self {
            DBError::NotFound => DBError::NotFound,
            DBError::NotSupported => DBError::NotSupported,
            DBError::Corruption => DBError::Corruption,
            DBError::IoError(e) => DBError::IoError(std::io::Error::new(e.kind(), e.to_string())),
        }
    }
}
