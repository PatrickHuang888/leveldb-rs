use crate::util;

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Debug)]
pub enum ValueType {
    TypeDeletion = 0x0,
    TypeValue = 0x1,
    TypeNotSet = 0x10,
}
pub const VALUE_TYPE_FOR_SEEK: ValueType = ValueType::TypeValue;

pub struct LookupKey {
    user_key: Vec<u8>,
    sequence: SequenceNumber,
    value_type: ValueType,
}

pub(super) type SequenceNumber = u64;
// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
pub(super) const MAX_SEQUENCE_NUMBER: SequenceNumber = (1u64 << 56) - 1;

impl LookupKey {
    pub fn new(user_key: &Vec<u8>, s: SequenceNumber, t: ValueType) -> Self {
        LookupKey {
            user_key: user_key.clone(),
            sequence: s,
            value_type: t,
        }
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        let (key_len, offset) = util::decode_varint32(buf)?;
        let user_key = buf[offset..offset + key_len as usize].to_vec();
        let tag_offset = offset + key_len as usize;
        let tag_bytes = &buf[tag_offset..tag_offset + 8];
        let packed = u64::from_le_bytes(tag_bytes.try_into().ok()?);
        let sequence = packed >> 8;
        let value_type = match packed & 0xFF {
            0x1 => ValueType::TypeValue,
            0x2 => ValueType::TypeDeletion,
            _ => return None, // 未知的值类型
        };

        Some(LookupKey {
            user_key,
            sequence,
            value_type,
        })
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.clear();
        let key_len = self.user_key.len() + 8;
        util::encode_varint32(buf, key_len as u32);
        buf.extend_from_slice(&self.user_key);
        let packed = (self.sequence << 8) | (self.value_type as u64);
        buf.extend_from_slice(&packed.to_le_bytes());
    }
    pub fn get_user_key(&self) -> &Vec<u8> {
        &self.user_key
    }
    pub fn get_sequence_number(&self) -> SequenceNumber {
        self.sequence
    }
    pub fn get_value_type(&self) -> ValueType {
        self.value_type
    }
}

pub(crate) struct TableKey {
    // entry format is:
    //    klength  varint32      --> key_start
    //    userkey  char[klength]
    //    tag      uint64        --> value_start
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    lookup_key: LookupKey,
    value: Vec<u8>,
}

impl TableKey {
    pub fn new(lookup_key: LookupKey, value: Vec<u8>) -> Self {
        TableKey { lookup_key, value }
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        let (key_len, offset) = util::decode_varint32(buf)?;
        let key_start = offset;
        let value_start = key_start + key_len as usize + 8; // 8 bytes for tag
        let (value_len, offset) = util::decode_varint32(&buf[value_start..])?;
        let value_offset = value_start + offset;
        let value = buf[value_offset..value_offset + value_len as usize].to_vec();

        Some(TableKey {
            lookup_key: match LookupKey::decode(&buf[..value_start]) {
                Some(lk) => lk,
                None => return None,
            },
            value,
        })
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.clear();
        let key = self.lookup_key.get_user_key();
        util::encode_varint32(buf, key.len() as u32);
        buf.extend_from_slice(key);
        let tag = (self.lookup_key.get_sequence_number() << 8)
            | (self.lookup_key.get_value_type() as u64);
        buf.extend_from_slice(&tag.to_le_bytes());
        util::encode_varint32(buf, self.value.len() as u32);
        buf.extend_from_slice(&self.value);
    }

    pub(super) fn get_lookup_key(&self) -> &LookupKey {
        &self.lookup_key
    }
    pub(super) fn get_value(&self) -> &Vec<u8> {
        &self.value
    }
}
