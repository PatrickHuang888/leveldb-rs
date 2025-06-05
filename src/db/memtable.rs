use super::dbformat::VALUE_TYPE_FOR_SEEK;
use super::dbformat::ValueType;
use super::dbformat::decode_varint32;
use super::dbformat::encode_varint32;
use super::skiplist::Key;
use super::skiplist::SkipList;
use super::skiplist::SkipListIter;
use crate::db::dbformat::LookupKey;
use crate::db::dbformat::SequenceNumber;
use crate::db::dbformat::TableKey;

#[derive(Debug)]
pub enum MemTableError {
    NotFound,
    NotSupported,
    Corruption,
    IoError(std::io::Error),
}

impl From<std::io::Error> for MemTableError {
    fn from(e: std::io::Error) -> Self {
        MemTableError::IoError(e)
    }
}

impl std::fmt::Display for MemTableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemTableError::NotFound => write!(f, "Not found"),
            MemTableError::NotSupported => write!(f, "Not supported"),
            MemTableError::Corruption => write!(f, "Corruption"),
            MemTableError::IoError(e) => write!(f, "IO error: {}", e),
        }
    }
}

struct MemtableKey {
    user_key: Vec<u8>,
    sequence: SequenceNumber,
    value_type: ValueType,
    value: Vec<u8>,
}

impl MemtableKey {
    pub fn new(
        user_key: &[u8],
        sequence: SequenceNumber,
        value_type: ValueType,
        value: &[u8],
    ) -> Self {
        MemtableKey {
            user_key: user_key.to_vec(),
            sequence,
            value_type,
            value: value.to_vec(),
        }
    }

    pub fn user_key(&self) -> &[u8] {
        &self.user_key
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
}

impl Clone for MemtableKey {
    fn clone(&self) -> Self {
        MemtableKey {
            user_key: self.user_key.clone(),
            sequence: self.sequence,
            value_type: self.value_type,
            value: self.value.clone(),
        }
    }
}

impl Ord for MemtableKey {
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

impl PartialOrd for MemtableKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for MemtableKey {
    fn eq(&self, other: &Self) -> bool {
        self.user_key == other.user_key
            && self.sequence == other.sequence
            && self.value_type == other.value_type
    }
}

impl Eq for MemtableKey {}

impl Default for MemtableKey {
    fn default() -> Self {
        MemtableKey {
            user_key: vec![],
            sequence: 0,
            value_type: ValueType::TypeValue,
            value: vec![],
        }
    }
}

impl Key for MemtableKey {}

type Table = SkipList<MemtableKey>;
pub struct MemTable {
    table: Table,
}

impl std::error::Error for MemTableError {}

impl MemTable {
    fn new() -> Self {
        MemTable {
            table: SkipList::new(),
        }
    }

    fn get(&self, user_key: &[u8], s: SequenceNumber) -> Result<Option<Vec<u8>>, MemTableError> {
        let key = MemtableKey::new(user_key, s, VALUE_TYPE_FOR_SEEK, &[]);
        let mut iter = self.table.iter();
        iter.seek(&key);
        if iter.valid() {
            let entry = iter.key().unwrap();
            if entry.user_key() == user_key {
                match entry.value_type() {
                    ValueType::TypeValue => Ok(Some(entry.value().to_vec())),
                    ValueType::TypeDeletion => Err(MemTableError::NotFound),
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn add(&mut self, s: SequenceNumber, t: ValueType, user_key: &[u8], value: &[u8]) {
        let key = MemtableKey::new(user_key, s, t, value);
        self.table.insert(&key);
    }

    pub fn iter(&self) -> MemTableIter<'_> {
        MemTableIter::new(self)
    }
}

struct MemTableIter<'a> {
    inner: SkipListIter<'a, MemtableKey>,
}

impl<'a> MemTableIter<'a> {
    pub fn new(memtable: &'a MemTable) -> Self {
        MemTableIter {
            inner: memtable.table.iter(),
        }
    }
    pub fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
    }
    pub fn seek_to_last(&mut self) {
        self.inner.seek_to_last();
    }
    pub fn seek(&mut self, user_key: &[u8]) {
        // 查找 user_key 的最大 sequence
        let lookup_key = MemtableKey::new(user_key, u64::MAX, ValueType::TypeValue, &[]);
        self.inner.seek(&lookup_key);
    }
    pub fn valid(&self) -> bool {
        self.inner.valid()
    }
    pub fn key(&self) -> Option<&'a MemtableKey> {
        self.inner.key()
    }

    pub fn next(&mut self) {
        self.inner.next();
    }
    pub fn prev(&mut self) {
        self.inner.prev();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_insert_and_iter() {
        let mut mem = MemTable::new();
        // 乱序且长度不同的 key/value
        let test_data: Vec<(&[u8], &[u8])> = vec![
            (b"dog", b"v1"),
            (b"a", b"v2"),
            (b"zebra", b"v3"),
            (b"cat", b"v4"),
            (b"apple", b"v5"),
            (b"banana", b"v6"),
        ];
        // 插入乱序
        for (i, (k, v)) in test_data.iter().enumerate() {
            mem.add(i as u64, ValueType::TypeValue, k, v);
        }
        // 期望正序遍历结果
        let mut expected: Vec<(Vec<u8>, Vec<u8>)> = test_data
            .iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        // 正序遍历
        let mut iter = mem.iter();
        iter.seek_to_first();
        let mut results = vec![];
        while iter.valid() {
            let key = iter.key().unwrap();
            results.push((key.user_key().to_vec(), key.value().to_vec()));
            iter.next();
        }
        // 打印字符串形式
        let to_str = |v: &Vec<(Vec<u8>, Vec<u8>)>| {
            v.iter()
                .map(|(k, val)| {
                    format!(
                        "({:?}, {:?})",
                        String::from_utf8_lossy(k),
                        String::from_utf8_lossy(val)
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        println!("results: [{}]", to_str(&results));
        assert_eq!(results, expected);
        // 校验 value
        for (i, (k, v)) in results.iter().enumerate() {
            assert_eq!(
                v,
                &expected[i].1,
                "value mismatch for key: {:?}",
                String::from_utf8_lossy(k)
            );
        }

        // 逆序遍历测试
        let mut iter = mem.iter();
        iter.seek_to_last();
        let mut rev_results = vec![];
        while iter.valid() {
            let key = iter.key().unwrap();
            rev_results.push((key.user_key().to_vec(), key.value().to_vec()));
            iter.prev();
        }
        let mut expected_rev = expected.clone();
        expected_rev.reverse();
        println!("rev_results: [{}]", to_str(&rev_results));
        assert_eq!(rev_results, expected_rev);
        // 校验逆序 value
        for (i, (k, v)) in rev_results.iter().enumerate() {
            assert_eq!(
                v,
                &expected_rev[i].1,
                "reverse value mismatch for key: {:?}",
                String::from_utf8_lossy(k)
            );
        }

        // 随机 seek 测试（包含边界和不存在的 key）
        let mut iter = mem.iter();
        let seek_cases: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"cat" as &[u8], Some(b"cat" as &[u8])), // 精确命中
            (b"banana", Some(b"banana")),             // 精确命中
            (b"zebra", Some(b"zebra")),               // 精确命中
            (b"a", Some(b"a")),                       // 精确命中
            (b"apple", Some(b"apple")),               // 精确命中
            (b"dog", Some(b"dog")),                   // 精确命中
            (b"b", Some(b"banana")),                  // 小于 banana，lower_bound
            (b"c", Some(b"cat")),                     // 小于 cat，lower_bound
            (b"zzz", None),                           // 大于所有 key，不存在
            (b"0", Some(b"a")),                       // 小于所有 key，lower_bound
        ];
        for (seek_key, expect_key) in seek_cases.iter() {
            iter.seek(seek_key);
            if let Some(expect) = expect_key {
                assert!(
                    iter.valid(),
                    "seek to {:?} should be valid",
                    String::from_utf8_lossy(seek_key)
                );
                let key = iter.key().unwrap();
                println!(
                    "seek to {:?}, got key: {:?}, value: {:?}",
                    String::from_utf8_lossy(seek_key),
                    String::from_utf8_lossy(key.user_key()),
                    String::from_utf8_lossy(key.value())
                );
                assert_eq!(key.user_key(), *expect, "seek result user_key mismatch");
            } else {
                println!(
                    "seek to {:?} should be invalid (None)",
                    String::from_utf8_lossy(seek_key)
                );
                assert!(
                    !iter.valid(),
                    "seek to {:?} should be invalid",
                    String::from_utf8_lossy(seek_key)
                );
                assert!(
                    iter.key().is_none(),
                    "seek to {:?} key() should be None",
                    String::from_utf8_lossy(seek_key)
                );
            }
        }
    }
}
