use crate::db::{
    dbformat::{DBError, SequenceNumber, VALUE_TYPE_FOR_SEEK, ValueType},
    memtable::{MemTable, MemTableIter},
};
use std::{sync::Arc, sync::Mutex};
use tokio::sync::{mpsc, oneshot};

struct WriteOptions {
    sync: bool,
}

const CHANNEL_MAX_SIZE: usize = 10_000;
const BATCH_COMBINE_THRESHOLD: usize = 100;

struct WriteBatch {
    writes: Vec<(Vec<u8>, Vec<u8>, ValueType)>, // (key, value, value_type)
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch { writes: Vec::new() }
    }
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.writes
            .push((key.to_vec(), value.to_vec(), ValueType::TypeValue));
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.writes
            .push((key.to_vec(), vec![], ValueType::TypeDeletion));
    }
}

struct WriteMsg {
    batch: WriteBatch,
    resp: oneshot::Sender<Result<(), DBError>>,
}

struct DB {
    mem: Arc<MemTable>,
    tx: mpsc::Sender<WriteMsg>,
    last_sequence: Arc<Mutex<SequenceNumber>>,
}

impl DB {
    fn new() -> Result<Self, DBError> {
        let rt = tokio::runtime::Runtime::new()?;
        let mem = Arc::new(MemTable::new());
        let (tx, mut rx) = mpsc::channel::<WriteMsg>(CHANNEL_MAX_SIZE);
        let mem_clone = mem.clone();

        let last_sequence = Arc::new(Mutex::new(0u64));
        let snapshot = Arc::clone(&last_sequence);

        std::thread::spawn(move || {
            rt.block_on(async move {
                let mut queue = Vec::new();
                let mem_ptr = Arc::as_ptr(&mem_clone) as *mut MemTable;
                loop {
                    tokio::select! {
                        biased;
                        msg = rx.recv() => {
                            if let Some(msg) = msg {
                                queue.push(msg);
                                // 批量收集到阈值或队列空闲
                                while queue.len() < BATCH_COMBINE_THRESHOLD {
                                    match rx.try_recv() {
                                        Ok(next) => queue.push(next),
                                        Err(_) => break,
                                    }
                                }
                                // 一次性处理 queue 里的所有 WriteBatch
                                for msg in queue.drain(..) {
                                    for (key, value, value_type) in msg.batch.writes {
                                        let seq = {
                                            let mut last_seq = snapshot.lock().unwrap();
                                            *last_seq +=1;
                                            *last_seq
                                        };
                                        unsafe {(*mem_ptr).add(seq, value_type, &key, &value);}
                                    }
                                    let _ = msg.resp.send(Ok(()));
                                }
                            } else {
                                // sender 关闭，安全退出
                                // TODO: 善后工作
                                break;
                            }
                        }
                    }
                }
            });
        });
        Ok(DB {
            mem,
            tx,
            last_sequence,
        })
    }

    pub async fn write(
        &self,
        options: WriteOptions,
        updates: Option<WriteBatch>,
    ) -> Result<(), DBError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let batch = match updates {
            Some(b) => b,
            None => {
                // todo:这里可以返回错误或执行其他逻辑
                return Err(DBError::NotSupported);
            }
        };
        let msg = WriteMsg {
            batch,
            resp: resp_tx,
        };
        self.tx.send(msg).await.map_err(|_| DBError::NotSupported)?;
        resp_rx.await.unwrap_or(Err(DBError::NotSupported))
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), DBError> {
        let mut batch = WriteBatch::new();
        batch.put(key, value);
        self.write(WriteOptions { sync: false }, Some(batch)).await
    }

    pub async fn delete(&self, key: &[u8]) -> Result<(), DBError> {
        let mut batch = WriteBatch::new();
        batch.delete(key);
        self.write(WriteOptions { sync: false }, Some(batch)).await
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DBError> {
        let snapshot = {
            let last_seq = self.last_sequence.lock().unwrap();
            *last_seq
        };

        self.mem.get(key, snapshot)
    }

    pub fn iter(&self) -> DBIterator {
        let snapshot = {
            let last_seq = self.last_sequence.lock().unwrap();
            *last_seq
        };
        let iter = self.mem.iter();
        DBIterator {
            snapshot,
            iter,
            valid: false,
            saved_user_key: Vec::new(),
            saved_value: Vec::new(),
            direction: Direction::Forward,
        }
    }
}

pub struct DBIterator<'a> {
    snapshot: SequenceNumber,
    iter: MemTableIter<'a>,
    valid: bool,
    saved_user_key: Vec<u8>,
    saved_value: Vec<u8>,
    direction: Direction,
}

#[derive(PartialEq)]
enum Direction {
    Forward,
    Reverse,
}

impl DBIterator<'_> {
    // Helper method to find the next valid key visible to the user
    fn find_next_valid_key(&mut self, mut skipping: bool) {
        //assert!(self.valid());
        assert!(self.direction == Direction::Forward);
        loop {
            if let Some(key) = self.iter.key() {
                if key.sequence() <= self.snapshot {
                    match key.value_type() {
                        ValueType::TypeDeletion => {
                            skipping = true;
                            self.saved_user_key.clear();
                            self.saved_user_key.extend_from_slice(key.user_key());
                        }
                        ValueType::TypeValue => {
                            if skipping && key.user_key() <= &self.saved_user_key[..] {
                                // hidden by deletion
                                // 小于这个比较奇怪
                                // 按 AI的解释是经过多个 memtable iterator 的合并有可能user_key 并不是一直向前，
                                // 所以很奇怪，理论上经过 iter->next()user_key 应该不会小于 ??
                            } else {
                                self.valid = true;
                                self.saved_user_key.clear();
                                return;
                            }
                        }
                        _ => panic!("invalid value type"),
                    }
                }
            }
            self.next();
            if !self.valid() {
                break;
            }
        }
        self.valid = false;
        self.saved_user_key.clear();
    }

    // Helper method to find the previous valid key visible to the user
    fn find_prev_valid_key(&mut self) {
        assert!(self.direction == Direction::Reverse);

        let mut value_type = ValueType::TypeDeletion;
        while self.iter.valid() {
            if let Some(key) = self.iter.key() {
                if key.sequence() <= self.snapshot {
                    if value_type != ValueType::TypeDeletion
                        && key.user_key() < &self.saved_user_key[..]
                    {
                        // We encountered a non-deleted value in entries for previous keys,
                        break;
                    }
                    value_type = key.value_type();
                    match value_type {
                        ValueType::TypeDeletion => {
                            self.saved_user_key.clear();
                            self.saved_value.clear();
                        }
                        ValueType::TypeValue => {
                            self.saved_user_key.clear();
                            self.saved_user_key.extend_from_slice(key.user_key());
                            self.saved_value.clear();
                            self.saved_value.extend_from_slice(key.value());
                        }
                        _ => panic!("invalid value type"),
                    }
                }
            }
            self.iter.prev();
        }

        if value_type == ValueType::TypeDeletion {
            // If we reach here, it means we didn't find any valid key
            self.valid = false;
            self.saved_user_key.clear();
            self.saved_value.clear();
            self.direction = Direction::Forward;
        } else {
            self.valid = true;
        }
    }

    pub fn seek_to_first(&mut self) {
        self.direction = Direction::Forward;
        self.saved_value.clear();
        self.iter.seek_to_first();
        if self.iter.valid() {
            self.find_next_valid_key(false);
        } else {
            self.valid = false;
            self.saved_user_key.clear();
        }
    }

    pub fn seek_to_last(&mut self) {
        self.direction = Direction::Reverse;
        self.saved_value.clear();
        self.iter.seek_to_last();
        self.find_prev_valid_key();
    }

    pub fn seek(&mut self, user_key: &[u8]) {
        self.direction = Direction::Forward;
        self.saved_value.clear();
        self.saved_user_key.clear();
        self.saved_user_key.extend_from_slice(user_key);
        self.iter
            .seek(&self.saved_user_key, self.snapshot, VALUE_TYPE_FOR_SEEK);
        if self.iter.valid() {
            self.find_next_valid_key(false);
        } else {
            self.valid = false;
            self.saved_user_key.clear();
        }
    }

    pub fn next(&mut self) {
        assert!(self.valid);

        if self.direction == Direction::Reverse {
            // If we were going backwards, switch to forward direction
            self.direction = Direction::Forward;

            if self.iter.valid() {
                self.iter.next();
            } else {
                self.iter.seek_to_first();
            }

            if !self.iter.valid() {
                self.saved_user_key.clear();
                self.valid = false;
                return;
            }
        } else {
            // Save current key before moving forward
            if let Some(key) = self.iter.key() {
                self.saved_user_key.extend_from_slice(key.user_key());
            }

            // Move to next key
            self.iter.next();

            // If iterator is no longer valid, clear saved key and set valid to false
            if !self.iter.valid() {
                self.saved_user_key.clear();
                self.valid = false;
                return;
            }
        }

        self.find_next_valid_key(true);
    }

    pub fn prev(&mut self) {
        assert!(self.valid);

        if self.direction == Direction::Forward {
            // iter_ is pointing at the current entry.  Scan backwards until
            // the key changes so we can use the normal reverse scanning code.
            assert!(self.iter.valid());
            if let Some(key) = self.iter.key() {
                self.saved_user_key.clear();
                self.saved_user_key.extend_from_slice(key.user_key());
            }
            // Move backwards until we find a key less than saved_key
            loop {
                self.iter.prev();

                if !self.iter.valid() {
                    self.valid = false;
                    self.saved_user_key.clear();
                    self.saved_value.clear();
                    return;
                }

                // Both saved_key and current key must exist at this point
                if let Some(key) = self.iter.key() {
                    if key.user_key() < &self.saved_user_key[..] {
                        break;
                    }
                }
            }
            self.direction = Direction::Reverse;
        }

        self.find_prev_valid_key();
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub fn key(&self) -> Option<&[u8]> {
        if self.direction == Direction::Forward {
            self.iter.key().map(|k| k.user_key())
        } else {
            Some(&self.saved_user_key[..])
        }
    }

    pub fn value(&self) -> Option<&[u8]> {
        if self.direction == Direction::Forward {
            self.iter.key().map(|k| k.value())
        } else {
            Some(&self.saved_value[..])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn test_put_get_delete() {
        let rt = Runtime::new().unwrap();
        let db = DB::new().unwrap();

        // put
        rt.block_on(async {
            db.put(b"foo", b"bar").await.unwrap();
        });

        // get
        let val = db.get(b"foo").unwrap();
        assert_eq!(val, Some(b"bar".to_vec()));

        // delete
        rt.block_on(async {
            db.delete(b"foo").await.unwrap();
        });

        // get after delete
        let val = db.get(b"foo").unwrap_err();
        assert!(matches!(val, DBError::NotFound));

        // 批量写入
        let mut batch = WriteBatch::new();
        batch.put(b"k1", b"v1");
        batch.put(b"k2", b"v2");
        batch.put(b"k3", b"v3");
        rt.block_on(async {
            db.write(WriteOptions { sync: false }, Some(batch))
                .await
                .unwrap();
        });
        assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(db.get(b"k3").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn test_dbiterator() {
        let rt = Runtime::new().unwrap();
        let db = DB::new().unwrap();

        // 批量插入多组数据
        let mut batch = WriteBatch::new();
        batch.put(b"a", b"1");
        batch.put(b"b", b"2");
        batch.put(b"c", b"3");
        rt.block_on(async {
            db.write(WriteOptions { sync: false }, Some(batch))
                .await
                .unwrap();
        });

        // 正向遍历
        let mut iter = db.iter();
        iter.seek_to_first();
        let mut keys = Vec::new();
        let mut values = Vec::new();
        while iter.valid() {
            keys.push(iter.key().unwrap().to_vec());
            values.push(iter.value().unwrap().to_vec());
            iter.next();
        }
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(values, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);

        // 反向遍历
        let mut iter = db.iter();
        iter.seek_to_last();
        let mut rev_keys = Vec::new();
        let mut rev_values = Vec::new();
        while iter.valid() {
            rev_keys.push(iter.key().unwrap().to_vec());
            rev_values.push(iter.value().unwrap().to_vec());
            iter.prev();
        }
        assert_eq!(
            rev_values,
            vec![b"3".to_vec(), b"2".to_vec(), b"1".to_vec()]
        );

        // seek 到 b
        let mut iter = db.iter();
        iter.seek(b"b");
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"b");
        assert_eq!(iter.value().unwrap(), b"2");
        // next
        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"c");
        // prev
        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key().unwrap(), b"b");
    }
}
