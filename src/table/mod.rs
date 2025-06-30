use crate::DBError;
use crate::util;

mod block;
mod table;

const BL_MAX_ENCODED_LENGTH: usize = 10 + 10;
pub(crate) struct BlockHandle {
    offset: u64, // 块的偏移量
    size: u64,   // 块的大小
}
impl BlockHandle {
    fn new() -> Self {
        BlockHandle { offset: 0, size: 0 }
    }
    fn set_offset(&mut self, offset: usize) {
        // 设置块的偏移量
    }
    fn set_size(&mut self, size: usize) {
        // 设置块的大小
    }
    fn encode_to(&self, buf: &mut Vec<u8>) {
        assert!(self.offset != 0);
        assert!(self.size != 0);
        util::encode_varint64(buf, self.offset);
        util::encode_varint64(buf, self.size);
    }

    fn decode_from(buf: &[u8]) -> Result<(Self, usize), DBError> {
        let (offset, offset_size) = util::decode_varint64(buf).ok_or(DBError::Corruption)?;
        let (size, size_size) =
            util::decode_varint64(&buf[offset_size..]).ok_or(DBError::Corruption)?;
        Ok((BlockHandle { offset, size }, offset_size + size_size))
    }
}
