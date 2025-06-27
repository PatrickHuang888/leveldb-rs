pub(super) mod file;

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

pub(super) fn decode_varint64(buf: &[u8]) -> Option<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0;
    for (i, &b) in buf.iter().enumerate() {
        result |= ((b & 0x7F) as u64) << shift;
        if b < 0x80 {
            return Some((result, i + 1));
        }
        shift += 7;
        if shift > 63 {
            return None; // varint64 太长
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

pub(super) fn encode_varint64(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

pub(super) fn encode_fixed32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub(super) fn encode_fixed32_slice(buf: &mut [u8], v: u32) {
    if buf.len() < 4 {
        panic!("Buffer too small for fixed32 encoding");
    }
    buf[0..4].copy_from_slice(&v.to_le_bytes());
}

pub(super) fn encode_fixed64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}
pub(super) fn decode_fixed32(buf: &[u8]) -> Option<u32> {
    if buf.len() < 4 {
        return None; // 缓冲区不足
    }
    let value = u32::from_le_bytes(buf[0..4].try_into().ok()?);
    Some(value)
}

pub(super) fn decode_fixed64(buf: &[u8]) -> Option<u64> {
    if buf.len() < 8 {
        return None; // 缓冲区不足
    }
    let value = u64::from_le_bytes(buf[0..8].try_into().ok()?);
    Some(value)
}
