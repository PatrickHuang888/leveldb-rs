use super::InternalIterator;
use super::InternalKey;
use super::dbformat::VALUE_TYPE_FOR_SEEK;
use super::dbformat::ValueType;
use super::skiplist::Key;
use super::skiplist::SkipList;
use super::skiplist::SkipListIter;
use crate::DBError;
use crate::db::dbformat::MAX_SEQUENCE_NUMBER;
use crate::db::dbformat::SequenceNumber;
use crate::db::skiplist::Size;
use crate::util;
use std::char::MAX;
use std::sync::Arc;

type Table = SkipList<InternalKey>;
type TableIter = SkipListIter<InternalKey>;

pub(super) struct MemTable {
    table: Arc<Table>,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            table: Arc::new(SkipList::new()),
        }
    }

    // 返回 None 表示没找到，NotFound 错误表示删除了
    pub fn get(&self, user_key: &[u8], s: SequenceNumber) -> Result<Option<Vec<u8>>, DBError> {
        let key = InternalKey::new(user_key, s, VALUE_TYPE_FOR_SEEK, &[]);
        let mut iter = MemTableIter::new(self);
        iter.seek(&key);
        if iter.valid() {
            let entry = iter.key().unwrap();
            if entry.user_key() == user_key {
                match entry.value_type() {
                    ValueType::TypeValue => Ok(Some(entry.value().to_vec())),
                    ValueType::TypeDeletion => Err(DBError::NotFound),
                    _ => {
                        panic!(
                            "Unexpected value type: {:?} for key: {:?}",
                            entry.value_type(),
                            String::from_utf8_lossy(entry.user_key())
                        );
                    }
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub fn add(&mut self, s: SequenceNumber, t: ValueType, user_key: &[u8], value: &[u8]) {
        let key = InternalKey::new(user_key, s, t, value);
        let table = Arc::as_ptr(&self.table.clone()) as *mut Table;
        unsafe {
            (*table).insert(&key);
        }
    }

    pub fn aproximate_size(&self) -> usize {
        self.table.approximate_size()
    }
}

pub(super) struct MemTableIter {
    iter: TableIter,
}

impl MemTableIter {
    pub fn new(mem: &MemTable) -> Self {
        MemTableIter {
            iter: TableIter::new(mem.table.clone()),
        }
    }
}

impl InternalIterator for MemTableIter {
    fn seek(&mut self, key: &InternalKey) -> Result<(), DBError> {
        self.iter.seek(key);
        Ok(())
    }

    fn seek_to_first(&mut self) -> Result<(), DBError> {
        self.iter.seek_to_first();
        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<(), DBError> {
        self.iter.seek_to_last();
        Ok(())
    }

    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn key(&self) -> Option<&InternalKey> {
        self.iter.key()
    }

    fn next(&mut self) -> Result<(), DBError> {
        self.iter.next();
        Ok(())
    }

    fn prev(&mut self) -> Result<(), DBError> {
        self.iter.prev();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::db::dbformat::MAX_SEQUENCE_NUMBER;

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
        let mut iter = MemTableIter::new(&mem);
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
        let mut iter = MemTableIter::new(&mem);
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
        let mut iter = MemTableIter::new(&mem);
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
            let key = InternalKey::new(&seek_key, MAX_SEQUENCE_NUMBER, ValueType::TypeValue, &[]);
            iter.seek(&key);
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
