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
