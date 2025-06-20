use crate::db::{
    dbformat::{DBError, SequenceNumber, ValueType},
    memtable::{InternalIterator, InternalKey, MemTable, MemTableIter},
};
use std::{cell::RefCell, rc::Rc, sync::Arc, sync::Mutex};
use tokio::sync::{Notify, mpsc, oneshot};

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
    inner: Arc<Mutex<DBInner>>,
    tx: mpsc::Sender<WriteMsg>,
    last_sequence: Arc<Mutex<SequenceNumber>>,
    compacting_notify: Arc<Notify>,
}

struct DBInner {
    mem: Arc<MemTable>,
    _imm: Option<Arc<MemTable>>,
}

impl DB {
    fn new() -> Result<Self, DBError> {
        let mem = Arc::new(MemTable::new());
        let (tx, mut rx) = mpsc::channel::<WriteMsg>(CHANNEL_MAX_SIZE);

        let last_sequence = Arc::new(Mutex::new(0u64));
        let snapshot = Arc::clone(&last_sequence);

        let inner = Arc::new(Mutex::new(DBInner { mem, _imm: None }));
        let inner_clone = Arc::clone(&inner);

        let db = DB {
            inner,
            tx,
            last_sequence: Arc::clone(&last_sequence),
            compacting_notify: Arc::new(Notify::new()),
        };

        let notify_notifier = Arc::clone(&db.compacting_notify);
        let notify_waiting = Arc::clone(&db.compacting_notify);

        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                loop {
                    notify_waiting.notified().await;
                    // 这里可以添加 compaction 或 flush 的逻辑
                    // 例如，检查 _imm 是否存在，如果存在则进行合并或持久化
                }
            });
        });

        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let mut queue = Vec::new();

                let mem_clone = {
                    let mut guard = inner_clone.lock().unwrap();

                    // make room for write
                    if guard.mem.aproximate_size() > 100_000 {
                        if guard._imm.is_none() {
                            guard._imm = Some(Arc::clone(&guard.mem));
                            guard.mem = Arc::new(MemTable::new());
                        }
                        // notify compaction or flush
                        // 也许 compaction 正忙?仍然发消息?
                        notify_notifier.notify_one();
                    }
                    Arc::clone(&guard.mem)
                };

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
        Ok(db)
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

        let (mem, _imm) = {
            let inner = self.inner.lock().unwrap();
            (Arc::clone(&inner.mem), inner._imm.clone())
        };
        let try_get = |table: &Arc<MemTable>| table.get(key, snapshot);

        match try_get(&mem) {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => {
                if let Some(imm) = _imm {
                    match try_get(&imm) {
                        Ok(Some(value)) => Ok(Some(value)),
                        Ok(None) => Ok(None), // TODO: 查找 SSTable
                        Err(e) => Err(e),
                    }
                } else {
                    Ok(None) // TODO: 查找 SSTable
                }
            }
            Err(e) => Err(e),
        }
    }
}

pub struct DBIterator {
    snapshot: SequenceNumber,
    iter: MergeIterator,
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

impl DBIterator {
    pub fn new(
        iterators: Vec<Rc<RefCell<dyn InternalIterator>>>,
        snapshot: SequenceNumber,
    ) -> Self {
        let iter = MergeIterator::new(iterators);
        DBIterator {
            snapshot,
            iter,
            valid: false,
            saved_user_key: Vec::new(),
            saved_value: Vec::new(),
            direction: Direction::Forward,
        }
    }

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
        let ikey = InternalKey::new(user_key, self.snapshot, ValueType::TypeValue, &[]);
        self.iter.seek(&ikey);
        if self.iter.valid() {
            self.find_next_valid_key(false);
        } else {
            self.valid = false;
            self.saved_user_key.clear();
        }
    }

    pub fn next(&mut self) {
        //assert!(self.valid);

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

    pub fn key(&self) -> Option<Vec<u8>> {
        if self.direction == Direction::Forward {
            self.iter.key().map(|k| k.user_key().to_vec())
        } else {
            Some(self.saved_user_key.clone())
        }
    }

    pub fn value(&self) -> Option<Vec<u8>> {
        if self.direction == Direction::Forward {
            self.iter.key().map(|k| k.value().to_vec())
        } else {
            Some(self.saved_value.clone())
        }
    }
}

struct MergeIterator {
    iterators: Vec<Rc<RefCell<dyn InternalIterator>>>,
    current: Option<Rc<RefCell<dyn InternalIterator>>>,
    direction: Direction,
}

impl MergeIterator {
    pub fn new(iterators: Vec<Rc<RefCell<dyn InternalIterator>>>) -> Self {
        MergeIterator {
            iterators,
            current: None,
            direction: Direction::Forward,
        }
    }

    pub fn valid(&self) -> bool {
        self.current.is_some()
    }

    pub fn seek_to_first(&mut self) {
        for iter in &self.iterators {
            iter.borrow_mut().seek_to_first();
        }
        self.find_smallest();
        self.direction = Direction::Forward;
    }

    fn find_smallest(&mut self) {
        let mut smallest: Option<Rc<RefCell<dyn InternalIterator>>> = None;
        for iter in &self.iterators {
            let iter_borrow = iter.borrow();
            if let Some(key) = iter_borrow.key() {
                let is_smaller = match &smallest {
                    None => true,
                    Some(smallest_iter) => {
                        let smallest_borrow = smallest_iter.borrow();
                        if let Some(smallest_key) = smallest_borrow.key() {
                            key < smallest_key
                        } else {
                            false
                        }
                    }
                };
                if is_smaller {
                    smallest = Some(iter.clone());
                }
            }
        }
        self.current = smallest;
    }

    pub fn seek_to_last(&mut self) {
        for iter in &self.iterators {
            iter.borrow_mut().seek_to_last();
        }
        self.find_largest();
        self.direction = Direction::Reverse;
    }

    fn find_largest(&mut self) {
        let mut largest: Option<Rc<RefCell<dyn InternalIterator>>> = None;
        for iter in &self.iterators {
            let iter_borrow = iter.borrow();
            if let Some(key) = iter_borrow.key() {
                let is_larger = match &largest {
                    None => true,
                    Some(largest_iter) => {
                        let largest_borrow = largest_iter.borrow();
                        if let Some(largest_key) = largest_borrow.key() {
                            key > largest_key
                        } else {
                            false
                        }
                    }
                };
                if is_larger {
                    largest = Some(iter.clone());
                }
            }
        }
        self.current = largest;
    }

    pub fn seek(&mut self, key: &InternalKey) {
        self.direction = Direction::Forward;
        for iter in &mut self.iterators {
            iter.borrow_mut().seek(key);
        }
        self.find_smallest();
    }

    pub fn key(&self) -> Option<InternalKey> {
        self.current.as_ref().and_then(|iter| {
            let iter_borrow = iter.borrow();
            iter_borrow.key().cloned()
        })
    }

    pub fn next(&mut self) {
        if self.direction != Direction::Forward {
            // Ensure that all children are positioned after key()
            if let Some(current) = &self.current {
                let key = self.key();
                for iter in &mut self.iterators {
                    if !Rc::ptr_eq(iter, current) {
                        if let Some(ref k) = key {
                            iter.borrow_mut().seek(k);
                        }
                        if iter.borrow().valid() && (key == iter.borrow().key().cloned()) {
                            iter.borrow_mut().next();
                        }
                    }
                }
            }
            self.direction = Direction::Forward;
        }

        if let Some(current) = &self.current {
            current.borrow_mut().next();
        }
        self.find_smallest();
    }

    pub fn prev(&mut self) {
        if self.direction != Direction::Reverse {
            // Ensure that all children are positioned before key()
            if let Some(current) = &self.current {
                let key = self.key();
                for iter in &mut self.iterators {
                    if !Rc::ptr_eq(iter, current) {
                        if let Some(ref k) = key {
                            iter.borrow_mut().seek(k);
                        }
                        if iter.borrow().valid() {
                            iter.borrow_mut().prev();
                        } else {
                            // has no entries >= key().  Position at last entry.
                            iter.borrow_mut().seek_to_last();
                        }
                    }
                }
            }
            self.direction = Direction::Reverse;
        }

        if let Some(current) = &self.current {
            current.borrow_mut().prev();
        }
        self.find_largest();
    }
}

#[cfg(test)]
mod tests {
    use crate::db::dbformat::MAX_SEQUENCE_NUMBER;

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
        let mem = Arc::clone(&db.inner.lock().unwrap().mem);
        let mut iter = DBIterator::new(
            vec![Rc::new(RefCell::new(MemTableIter::new(&mem)))],
            MAX_SEQUENCE_NUMBER, // 使用当前的 sequence number
        );
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
        let mut iter = DBIterator::new(
            vec![Rc::new(RefCell::new(MemTableIter::new(&mem)))],
            MAX_SEQUENCE_NUMBER, // 使用当前的 sequence number
        );
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
        let mut iter = DBIterator::new(
            vec![Rc::new(RefCell::new(MemTableIter::new(&mem)))],
            MAX_SEQUENCE_NUMBER, // 使用当前的 sequence number
        );
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

    #[test]
    fn test_merge_iterator_multiple_memtables() {
        // 构造两个 memtable，key 有重叠
        let mut mem1 = MemTable::new();
        let mut mem2 = MemTable::new();
        // mem1: a->1, b->2, c->3
        mem1.add(1, ValueType::TypeValue, b"a", b"1");
        mem1.add(2, ValueType::TypeValue, b"b", b"2");
        mem1.add(3, ValueType::TypeValue, b"c", b"3");
        // mem2: b->20, c->30, d->40
        mem2.add(4, ValueType::TypeValue, b"b", b"20");
        mem2.add(5, ValueType::TypeValue, b"c", b"30");
        mem2.add(6, ValueType::TypeValue, b"d", b"40");
        // 包裹 Arc
        let mem1 = Arc::new(mem1);
        let mem2 = Arc::new(mem2);
        // 构造 MergeIterator
        let iter1 = Rc::new(RefCell::new(MemTableIter::new(&mem1)));
        let iter2 = Rc::new(RefCell::new(MemTableIter::new(&mem2)));
        let mut merge_iter = MergeIterator::new(vec![iter1, iter2]);
        // 正向遍历
        merge_iter.seek_to_first();
        let mut keys = Vec::new();
        let mut values = Vec::new();
        while merge_iter.valid() {
            if let Some(key) = merge_iter.key() {
                keys.push(key.user_key().to_vec());
                values.push(key.value().to_vec());
            }
            merge_iter.next();
        }
        // 打印 values 作为字符串
        let value_strs: Vec<String> = values
            .iter()
            .map(|v| String::from_utf8_lossy(v).to_string())
            .collect();
        println!("values as string = {:?}", value_strs);

        // 不会去重
        assert_eq!(
            keys,
            vec![
                b"a".to_vec(),
                b"b".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"c".to_vec(),
                b"d".to_vec()
            ]
        );
        // 版本逆序
        assert_eq!(
            values,
            vec![
                b"1".to_vec(),
                b"20".to_vec(),
                b"2".to_vec(),
                b"30".to_vec(),
                b"3".to_vec(),
                b"40".to_vec()
            ]
        );
    }
}
