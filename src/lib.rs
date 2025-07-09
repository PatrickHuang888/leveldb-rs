pub mod db;
pub mod table;
pub(crate) mod util;

// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompressionType {
    // NOTE: do not change the values of existing entries, as these are
    // part of the persistent format on disk.
    NO_COMPRESSION = 0x0,
    SNAPPY_COMPRESSION = 0x1,
    ZSTD_COMPRESSION = 0x2,
}

#[derive(Clone)]
pub struct DBConfig {
    // Compress blocks using the specified compression algorithm.  This
    // parameter can be changed dynamically.
    //
    // Default: kSnappyCompression, which gives lightweight but fast
    // compression.
    //
    // Typical speeds of kSnappyCompression on an Intel(R) Core(TM)2 2.4GHz:
    //    ~200-500MB/s compression
    //    ~400-800MB/s decompression
    // Note that these speeds are significantly faster than most
    // persistent storage speeds, and therefore it is typically never
    // worth switching to kNoCompression.  Even if the input data is
    // incompressible, the kSnappyCompression implementation will
    // efficiently detect that and will switch to uncompressed mode.
    pub compression: CompressionType,

    // Number of keys between restart points for delta encoding of keys.
    // This parameter can be changed dynamically.  Most clients should
    // leave this parameter alone.
    pub block_restart_interval: usize,
    // Approximate size of user data packed per block.  Note that the
    // block size specified here corresponds to uncompressed data.  The
    // actual size of the unit read from disk may be smaller if
    // compression is enabled.  This parameter can be changed dynamically.
    pub block_size: usize,

    pub test_disable_compaction: bool,
}

impl Default for DBConfig {
    fn default() -> Self {
        DBConfig {
            block_restart_interval: 16,
            block_size: 4096,
            compression: CompressionType::SNAPPY_COMPRESSION,
            test_disable_compaction: false,
        }
    }
}

#[derive(Debug)]
pub enum DBError {
    NotFound,
    NotSupported,
    Corruption,
    IOError(std::io::Error),
}

impl From<std::io::Error> for DBError {
    fn from(e: std::io::Error) -> Self {
        DBError::IOError(e)
    }
}

impl std::fmt::Display for DBError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBError::NotFound => write!(f, "Not found"),
            DBError::NotSupported => write!(f, "Not supported"),
            DBError::Corruption => write!(f, "Corruption"),
            DBError::IOError(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl Clone for DBError {
    fn clone(&self) -> Self {
        match self {
            DBError::NotFound => DBError::NotFound,
            DBError::NotSupported => DBError::NotSupported,
            DBError::Corruption => DBError::Corruption,
            DBError::IOError(e) => DBError::IOError(std::io::Error::new(e.kind(), e.to_string())),
        }
    }
}
