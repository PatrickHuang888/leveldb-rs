use crate::DBError;
use crate::util;
use dbformat::MAX_SEQUENCE_NUMBER;
use dbformat::SequenceNumber;
use dbformat::VALUE_TYPE_FOR_SEEK;
use dbformat::ValueType;
use skiplist::Key;
use skiplist::Size;

mod db_impl;
pub(crate) mod dbformat;
pub(crate) mod memtable;
mod skiplist;

pub(super) trait InternalIterator {
    fn seek(&mut self, key: &InternalKey) -> Result<(), DBError>;
    fn seek_to_first(&mut self) -> Result<(), DBError>;
    fn seek_to_last(&mut self) -> Result<(), DBError>;
    fn valid(&self) -> bool;
    fn key(&self) -> Option<&InternalKey>;
    fn next(&mut self) -> Result<(), DBError>;
    fn prev(&mut self) -> Result<(), DBError>;
}

pub(crate) struct InternalKey {
    user_key: Vec<u8>,
    sequence: SequenceNumber,
    value_type: ValueType,
    value: Vec<u8>,
}

impl InternalKey {
    pub fn new(
        user_key: &[u8],
        sequence: SequenceNumber,
        value_type: ValueType,
        value: &[u8],
    ) -> Self {
        InternalKey {
            user_key: user_key.to_vec(),
            sequence,
            value_type,
            value: value.to_vec(),
        }
    }

    pub fn user_key(&self) -> &[u8] {
        &self.user_key
    }

    pub fn user_key_resize(&mut self, size: usize) {
        if size < self.user_key.len() {
            self.user_key.truncate(size);
        } else {
            self.user_key.resize(size, 0);
        }
    }

    pub fn user_key_append(&mut self, key: &[u8]) {
        self.user_key.extend_from_slice(key);
    }

    pub fn sequence(&self) -> SequenceNumber {
        self.sequence
    }

    pub fn value_type(&self) -> ValueType {
        self.value_type
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn set_value(&mut self, value: &[u8]) {
        self.value.clear();
        self.value.extend_from_slice(value);
    }

    pub fn find_shortest_seperator(&self, other: &InternalKey) -> InternalKey {
        // Attempt to shorten the user portion of the key
        let mut tmp_key = self.clone();
        tmp_key.user_key = find_bitwise_shortest_separator(&self.user_key, &other.user_key);
        if (tmp_key.user_key.len() < self.user_key.len()) && (tmp_key.user_key > self.user_key) {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            tmp_key.sequence = MAX_SEQUENCE_NUMBER;
            tmp_key.value_type = VALUE_TYPE_FOR_SEEK;
            assert!(*self < tmp_key);
            assert!(tmp_key < *other);
        }
        tmp_key
    }

    pub fn find_shot_successor(&self) -> InternalKey {
        // Attempt to find the next key that is larger than self
        let mut tmp_key = self.clone();
        tmp_key.user_key = find_bitwise_short_successor(&self.user_key);
        if (tmp_key.user_key.len() < self.user_key.len()) && (self.user_key < tmp_key.user_key) {
            // User key has become shorter physically, but larger logically.
            // Tack on the earliest possible number to the shortened user key.
            tmp_key.sequence = MAX_SEQUENCE_NUMBER;
            tmp_key.value_type = VALUE_TYPE_FOR_SEEK;
            assert!(self < &tmp_key);
            return tmp_key;
        }
        self.clone()
    }

    pub fn reset(&mut self) {
        self.user_key.clear();
        self.sequence = 0;
        self.value_type = ValueType::TypeNotSet;
        self.value.clear();
    }

    pub fn encoding_key(&self, buf: &mut Vec<u8>) {
        // 编码 InternalKey 为字节数组
        let key_len = self.user_key.len() + 8; // 8 bytes for sequence and value_type
        util::encode_varint32(buf, key_len as u32);
        buf.extend_from_slice(&self.user_key);
        let packed = (self.sequence << 8) | (self.value_type as u64);
        buf.extend_from_slice(&packed.to_le_bytes());
    }

    pub fn encoding_value(&self, buf: &mut Vec<u8>) {
        util::encode_varint32(buf, self.value.len() as u32);
        buf.extend_from_slice(&self.value);
    }

    pub fn encode(&self) -> Vec<u8> {
        // 返回编码后的字节数组
        let mut buf = Vec::new();
        self.encoding_key(&mut buf);
        self.encoding_value(&mut buf);
        buf
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        // 解析 user_key + 8字节 tag + value
        if buf.len() < 8 {
            return None;
        }
        // 先解析 user_key + tag
        let mut pos = 0;
        // 解析 user_key 长度
        let (key_len, n1) = util::decode_varint32(&buf[pos..])?;
        pos += n1;
        if buf.len() < pos + key_len as usize {
            return None;
        }
        let user_key = buf[pos..pos + key_len as usize - 8].to_vec();
        let tag_bytes = &buf[pos + key_len as usize - 8..pos + key_len as usize];
        let packed = u64::from_le_bytes(tag_bytes.try_into().ok()?);
        let sequence = packed >> 8;
        let value_type = match (packed & 0xFF) as u8 {
            0x0 => ValueType::TypeDeletion,
            0x1 => ValueType::TypeValue,
            0x10 => ValueType::TypeNotSet,
            _ => return None,
        };
        pos += key_len as usize;
        // 解析 value 长度
        let (value_len, n2) = util::decode_varint32(&buf[pos..])?;
        pos += n2;
        if buf.len() < pos + value_len as usize {
            return None;
        }
        let value = buf[pos..pos + value_len as usize].to_vec();
        Some(InternalKey {
            user_key,
            sequence,
            value_type,
            value,
        })
    }
}

fn find_bitwise_shortest_separator(key1: &[u8], key2: &[u8]) -> Vec<u8> {
    // 找到两个字节数组之间的最短分隔符
    let mut separator = Vec::new();
    let min_len = key1.len().min(key2.len());
    let mut diff_index = 0;
    while diff_index < min_len && key1[diff_index] == key2[diff_index] {
        separator.push(key1[diff_index]);
        diff_index += 1;
    }
    if diff_index < min_len {
        let diff_byte = key1[diff_index];
        if (diff_byte < 0xff) && diff_byte + 1 < key2[diff_index] {
            // 如果 key1 的字节小于 0xff 且加 1 后小于 key2 的对应字节
            separator.push(diff_byte + 1);
        }
    } else {
        // Do not shorten if one string is a prefix of the other
    }
    separator
}

fn find_bitwise_short_successor(key1: &[u8]) -> Vec<u8> {
    // Find first character that can be incremented
    let mut successor = Vec::new();
    for i in 0..key1.len() {
        let byte = key1[i];
        if byte != 0xff {
            successor.push(byte + 1);
            return successor;
        } else {
            successor.push(byte);
        }
    }
    successor
}

impl Clone for InternalKey {
    fn clone(&self) -> Self {
        InternalKey {
            user_key: self.user_key.clone(),
            sequence: self.sequence,
            value_type: self.value_type,
            value: self.value.clone(),
        }
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        //    increasing user key (according to user-supplied comparator)
        //    decreasing sequence number
        //    decreasing type (though sequence# should be enough to disambiguate)
        match self.user_key.cmp(&other.user_key) {
            std::cmp::Ordering::Equal => match other.sequence.cmp(&self.sequence) {
                std::cmp::Ordering::Equal => other.value_type.cmp(&self.value_type),
                ord => ord,
            },
            ord => ord,
        }
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for InternalKey {
    fn eq(&self, other: &Self) -> bool {
        self.user_key == other.user_key
            && self.sequence == other.sequence
            && self.value_type == other.value_type
    }
}

impl Eq for InternalKey {}

impl Default for InternalKey {
    fn default() -> Self {
        InternalKey {
            user_key: vec![],
            sequence: 0,
            value_type: ValueType::TypeValue,
            value: vec![],
        }
    }
}

impl Key for InternalKey {}
impl Size for InternalKey {
    fn size(&self) -> usize {
        // user_key + sequence + value_type + value
        self.user_key.len() + 8 + 1 + self.value.len()
    }
}
