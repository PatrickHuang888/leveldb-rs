pub(super) fn decode_varint32(buf: &[u8]) -> Option<(u32, usize)> {
    let mut result = 0u32;
    let mut shift = 0;
    for (i, &b) in buf.iter().enumerate() {
        result |= ((b & 0x7F) as u32) << shift;
        if b < 0x80 {
            return Some((result, i + 1));
        }
        shift += 7;
        if shift > 28 {
            return None; // varint32 太长
        }
    }
    None
}

pub(super) fn encode_varint32(buf: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}
