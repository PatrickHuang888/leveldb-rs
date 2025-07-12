use super::InternalIterator;
use super::InternalKey;
use super::version::VersionEdit;
use crate::DBConfig;
use crate::DBError;
use crate::db::dbformat::VALUE_TYPE_FOR_SEEK;
use crate::db::version::FileMetaData;
use crate::db::version::Version;
use crate::db::version::VersionSet;
use crate::db::{
    dbformat::{SequenceNumber, ValueType},
    memtable::{MemTable, MemTableIter},
};
use crate::table::table::TableBuilder;
use std::fs::OpenOptions;
use std::fs::remove_file;
use std::sync::atomic::AtomicBool;
use std::{cell::RefCell, rc::Rc, sync::Arc, sync::Mutex};
use tokio::sync::{Notify, mpsc, oneshot};

struct WriteOptions {
    sync: bool,
}

const CHANNEL_MAX_SIZE: usize = 10_000;
const BATCH_COMBINE_THRESHOLD: usize = 100;
const COMPACTION_MEMORY_THRESHOLD: usize = 100_000;

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

pub struct DB {
    inner: Arc<Mutex<DBInner>>,
    tx: mpsc::Sender<WriteMsg>,
    switch_notify: Arc<Notify>,
    dbname: String,
    has_imm: Arc<AtomicBool>, // 是否有 _imm 存在
    config: DBConfig,
}

struct DBInner {
    mem: Arc<MemTable>,
    imm: Option<Arc<MemTable>>,
    versions: VersionSet,
    last_sequence: SequenceNumber,
}

impl DB {
    fn new() -> Result<Self, DBError> {
        Self::new_with_name("default_db")
    }

    fn new_with_name(dbname: &str) -> Result<Self, DBError> {
        Self::new_with_config(dbname, DBConfig::default())
    }

    fn new_with_config(dbname: &str, config: DBConfig) -> Result<Self, DBError> {
        let mem = Arc::new(MemTable::new());
        let (tx, rx) = mpsc::channel::<WriteMsg>(CHANNEL_MAX_SIZE);

        let inner = Arc::new(Mutex::new(DBInner {
            mem,
            imm: None,
            versions: VersionSet::new(),
            last_sequence: 0,
        }));

        let db = DB {
            inner: Arc::clone(&inner),
            tx,
            switch_notify: Arc::new(Notify::new()),
            dbname: dbname.to_string(),
            has_imm: Arc::new(AtomicBool::new(false)),
            config,
        };

        // Start background threads
        Self::spawn_compaction_thread(
            dbname.to_string(),
            Arc::clone(&inner),
            Arc::clone(&db.switch_notify),
            Arc::clone(&db.has_imm),
            db.config.test_disable_compaction,
        );

        Self::spawn_write_thread(
            rx,
            Arc::clone(&inner),
            Arc::clone(&db.switch_notify),
            Arc::clone(&db.has_imm),
        );

        Ok(db)
    }

    fn spawn_compaction_thread(
        dbname: String,
        inner: Arc<Mutex<DBInner>>,
        notify: Arc<Notify>,
        has_imm: Arc<AtomicBool>,
        test_disable_compaction: bool,
    ) {
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new()
                .expect("Failed to create tokio runtime for compaction thread");

            rt.block_on(async move {
                loop {
                    notify.notified().await;
                    println!("Switching memtable signal received");

                    if test_disable_compaction {
                        println!("Compaction disabled, skipping...");
                        continue;
                    }

                    if has_imm.load(std::sync::atomic::Ordering::Relaxed) {
                        println!("Compacting memtable...");
                        match Self::compact_memtable(&dbname, Arc::clone(&inner)) {
                            Ok(()) => {
                                has_imm.store(false, std::sync::atomic::Ordering::Release);
                                println!("Compaction completed successfully.");
                                // TODO: remove obsolete files
                            }
                            Err(e) => {
                                eprintln!("Compaction failed: {:?}", e);
                            }
                        }
                    } else {
                        println!("No _imm to compact.");
                    }
                }
            });
        });
    }

    fn spawn_write_thread(
        mut rx: mpsc::Receiver<WriteMsg>,
        inner: Arc<Mutex<DBInner>>,
        switch_notify: Arc<Notify>,
        has_imm: Arc<AtomicBool>,
    ) {
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new()
                .expect("Failed to create tokio runtime for write thread");

            rt.block_on(async move {
                let mut queue = Vec::with_capacity(BATCH_COMBINE_THRESHOLD);

                loop {
                    {
                        let mut guard = inner.lock().unwrap();
                        if Self::should_trigger_imm(&guard) {
                            Self::switch_mem(&mut guard, &switch_notify, &has_imm);
                        }
                    }

                    // Process write requests
                    tokio::select! {
                        biased;
                        msg = rx.recv() => {
                            match msg {
                                Some(msg) => {
                                    Self::process_write_batch(
                                        msg,
                                        &mut queue,
                                        &mut rx,
                                        &inner,
                                    ).await;
                                }
                                None => {
                                    // Channel closed, perform cleanup and exit
                                    println!("Write channel closed, shutting down write thread");
                                    break;
                                }
                            }
                        }
                    }
                }
            });
        });
    }

    fn should_trigger_imm(guard: &DBInner) -> bool {
        guard.mem.aproximate_size() > COMPACTION_MEMORY_THRESHOLD && guard.imm.is_none()
    }

    fn switch_mem(guard: &mut DBInner, notify: &Arc<Notify>, has_imm: &Arc<AtomicBool>) {
        dbg!("Moving memtable to _imm");
        guard.imm = Some(Arc::clone(&guard.mem));
        guard.mem = Arc::new(MemTable::new());
        has_imm.store(true, std::sync::atomic::Ordering::Release);
        notify.notify_one();
    }

    async fn process_write_batch(
        msg: WriteMsg,
        queue: &mut Vec<WriteMsg>,
        rx: &mut mpsc::Receiver<WriteMsg>,
        inner: &Arc<Mutex<DBInner>>,
    ) {
        queue.push(msg);

        // Collect additional messages up to threshold
        while queue.len() < BATCH_COMBINE_THRESHOLD {
            match rx.try_recv() {
                Ok(next_msg) => queue.push(next_msg),
                Err(_) => break,
            }
        }

        // Process all batched writes
        dbg!(queue.len());
        let (mem, mut sequence) = {
            let guard = inner.lock().unwrap();
            (guard.mem.clone(), guard.last_sequence)
        };
        let mem_ptr = Arc::as_ptr(&mem) as *mut MemTable;
        for msg in queue.drain(..) {
            for (key, value, value_type) in msg.batch.writes {
                sequence += 1;
                unsafe {
                    (*mem_ptr).add(sequence, value_type, &key, &value);
                }
            }
            let _ = msg.resp.send(Ok(()));
        }
        {
            let mut guard = inner.lock().unwrap();
            guard.last_sequence = sequence;
        }
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
        let (mem, imm, current, snapshot) = {
            let inner = self.inner.lock().unwrap();
            (
                inner.mem.clone(),
                inner.imm.clone(),
                inner.versions.current(),
                inner.last_sequence,
            )
        };

        // Try memtable first
        if let Some(value) = mem.get(key, snapshot)? {
            return Ok(Some(value));
        }

        // Try immutable memtable if it exists
        if let Some(imm) = imm {
            if let Some(value) = imm.get(key, snapshot)? {
                return Ok(Some(value));
            }
        }

        // Try SSTable files if current version exists
        if let Some(current) = current {
            let internal_key = InternalKey::new(key, snapshot, VALUE_TYPE_FOR_SEEK, &[]);
            return current.get(&self.dbname, &internal_key);
        }

        Ok(None)
    }

    /// Compacts the immutable memtable to an SSTable file
    fn compact_memtable(dbname: &str, inner: Arc<Mutex<DBInner>>) -> Result<(), DBError> {
        use super::version::VersionBuilder;

        let file_path;
        let mut meta;
        let iter;
        let mut new_version: Option<Version> = None;

        // Phase 1: Prepare compaction metadata and iterator
        {
            let mut inner_guard = inner.lock().unwrap();
            assert!(inner_guard.imm.is_some());

            println!("Compacting memtable...");
            meta = FileMetaData {
                file_number: inner_guard.versions.next_file_number(),
                file_size: 0,
                smallest_key: None,
                largest_key: None,
            };
            file_path = format!("{}-{}.ldb", dbname, meta.file_number);
            iter = MemTableIter::new(inner_guard.imm.as_ref().unwrap());
        }

        // Phase 2: Build the SSTable file
        Self::build_table(&file_path, iter, &mut meta).inspect_err(|_e| {
            let _ = remove_file(&file_path);
        })?;

        println!(
            "Compacted memtable to file: {} (size: {} bytes)",
            meta.file_number, meta.file_size
        );

        // Phase 3: Update version information
        {
            let mut inner_guard = inner.lock().unwrap();
            inner_guard.imm = None; // Clear _imm

            let mut edit = VersionEdit::new();
            if meta.file_size > 0 {
                let level = inner_guard
                    .versions
                    .pick_level_for_memtable_output(&meta.smallest_key, &meta.largest_key);
                edit.add_file(level, meta);
            }

            // Apply version edit
            let mut builder;
            match inner_guard.versions.current() {
                Some(current) => {
                    builder = VersionBuilder::new((*current).clone());
                }
                None => {
                    // If no current version, create a new one
                    builder = VersionBuilder::new(Version::default());
                }
            }
            builder.apply(&edit);
            new_version = Some(builder.build());

            // TODO: Write manifest
        }

        // TODO: Write log
        // TODO: Set current manifest

        // Phase 4: Install new version
        {
            let mut inner_guard = inner.lock().unwrap();
            if let Some(v) = new_version {
                inner_guard.versions.append(v);
            }
            // TODO: Remove obsolete files
        }

        Ok(())
    }

    /// Builds an SSTable file from an iterator
    /// On error, the caller should delete the file
    fn build_table(
        table_file_name: &str,
        mut iter: impl InternalIterator,
        meta: &mut FileMetaData,
    ) -> Result<(), DBError> {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(table_file_name)
            .map_err(DBError::from)?;

        iter.seek_to_first()?;

        if !iter.valid() {
            // Empty iterator, nothing to build
            meta.file_size = 0;
            return Ok(());
        }

        meta.smallest_key = iter.key().cloned();
        let mut builder = TableBuilder::new(&mut file);
        let mut largest_key = None;

        while iter.valid() {
            let key = iter.key().ok_or(DBError::Corruption)?;
            largest_key = Some(key.clone());
            builder.add(key)?;
            iter.next()?;
        }

        meta.largest_key = largest_key;
        meta.file_size = builder.finish()?;

        Ok(())
    }
}

pub struct DBIterator {
    snapshot: SequenceNumber,
    iter: MergingIterator,
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
    pub fn new(db: &DB) -> Result<Self, DBError> {
        let inner = db.inner.lock().unwrap();
        let snapshot = inner.last_sequence;
        let mut iterators: Vec<Box<dyn InternalIterator>> =
            vec![Box::new(MemTableIter::new(&inner.mem))];
        if let Some(imm) = &inner.imm {
            iterators.push(Box::new(MemTableIter::new(imm)));
        }
        let current = inner.versions.current();
        if let Some(current) = current {
            current.new_files_iterators(&db.dbname)?;
        }
        Ok(DBIterator::internal_new(iterators, snapshot))
    }

    fn internal_new(iterators: Vec<Box<dyn InternalIterator>>, snapshot: SequenceNumber) -> Self {
        let iter = MergingIterator::new(iterators);
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
    fn find_next_valid_key(&mut self, mut skipping: bool) -> Result<(), DBError> {
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
                                return Ok(());
                            }
                        }
                        _ => panic!("invalid value type"),
                    }
                }
            }
            self.next().inspect_err(|_e| {
                self.valid = false;
            })?;
            if !self.valid() {
                break;
            }
        }
        self.valid = false;
        Ok(())
    }

    // Helper method to find the previous valid key visible to the user
    fn find_prev_valid_key(&mut self) -> Result<(), DBError> {
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
            self.iter.prev().inspect_err(|_e| {
                self.valid = false;
            })?;
        }

        if value_type == ValueType::TypeDeletion {
            // If we reach here, it means we didn't find any valid key
            self.valid = false;
        } else {
            self.valid = true;
        }
        Ok(())
    }

    pub fn seek_to_first(&mut self) -> Result<(), DBError> {
        self.direction = Direction::Forward;
        self.saved_value.clear();
        self.iter
            .seek_to_first()
            .inspect_err(|_e| self.valid = false)?;
        if self.iter.valid() {
            self.find_next_valid_key(false).inspect_err(|_e| {
                self.valid = false;
            })?;
        } else {
            self.valid = false;
        }
        Ok(())
    }

    pub fn seek_to_last(&mut self) -> Result<(), DBError> {
        self.direction = Direction::Reverse;
        self.saved_value.clear();
        self.iter.seek_to_last().inspect_err(|_e| {
            self.valid = false;
        })?;
        self.find_prev_valid_key().inspect_err(|_e| {
            self.valid = false;
        })?;
        Ok(())
    }

    pub fn seek(&mut self, user_key: &[u8]) -> Result<(), DBError> {
        self.direction = Direction::Forward;
        self.saved_value.clear();
        self.saved_user_key.clear();
        self.saved_user_key.extend_from_slice(user_key);
        let ikey = InternalKey::new(user_key, self.snapshot, ValueType::TypeValue, &[]);
        self.iter.seek(&ikey).inspect_err(|_e| {
            self.valid = false;
        })?;
        if self.iter.valid() {
            self.find_next_valid_key(false).inspect_err(|_e| {
                self.valid = false;
            })?;
        } else {
            self.valid = false;
        }
        Ok(())
    }

    pub fn next(&mut self) -> Result<(), DBError> {
        assert!(self.valid);

        if self.direction == Direction::Reverse {
            // If we were going backwards, switch to forward direction
            self.direction = Direction::Forward;

            if self.iter.valid() {
                self.iter.next().inspect_err(|_e| {
                    self.valid = false;
                })?;
            } else {
                self.iter.seek_to_first().inspect_err(|_e| {
                    self.valid = false;
                })?;
            }

            if !self.iter.valid() {
                self.valid = false;
                return Ok(());
            }
        } else {
            // Save current key before moving forward
            if let Some(key) = self.iter.key() {
                self.saved_user_key.extend_from_slice(key.user_key());
            }

            // Move to next key
            self.iter.next().inspect_err(|_e| {
                self.valid = false;
            })?;

            // If iterator is no longer valid, clear saved key and set valid to false
            if !self.iter.valid() {
                self.valid = false;
                return Ok(());
            }
        }

        self.find_next_valid_key(true).inspect_err(|_e| {
            self.valid = false;
        })?;
        Ok(())
    }

    pub fn prev(&mut self) -> Result<(), DBError> {
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
                self.iter.prev().inspect_err(|_e| {
                    self.valid = false;
                })?;

                if !self.iter.valid() {
                    self.valid = false;
                    return Ok(());
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

        self.find_prev_valid_key().inspect_err(|_e| {
            self.valid = false;
        })?;
        Ok(())
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    /// 使用前确定 iterator valid
    pub fn key(&self) -> Vec<u8> {
        if self.direction == Direction::Forward {
            match self.iter.key() {
                Some(key) => key.user_key().to_vec(),
                None => vec![],
            }
        } else {
            self.saved_user_key.clone()
        }
    }

    /// 使用前确定 iterator valid
    pub fn value(&self) -> Vec<u8> {
        if self.direction == Direction::Forward {
            match self.iter.key() {
                Some(key) => key.value().to_vec(),
                None => vec![],
            }
        } else {
            self.saved_value.clone()
        }
    }
}

struct MergingIterator {
    iterators: Vec<Box<dyn InternalIterator>>,
    current_index: Option<usize>,
    direction: Direction,
}

impl MergingIterator {
    pub fn new(iterators: Vec<Box<dyn InternalIterator>>) -> Self {
        MergingIterator {
            iterators,
            current_index: None,
            direction: Direction::Forward,
        }
    }

    pub fn valid(&self) -> bool {
        self.current_index.is_some()
    }

    pub fn seek_to_first(&mut self) -> Result<(), DBError> {
        for iter in &mut self.iterators {
            iter.seek_to_first()?;
        }
        self.find_smallest();
        self.direction = Direction::Forward;
        Ok(())
    }

    fn find_smallest(&mut self) {
        let mut smallest_index: Option<usize> = None;
        let mut smallest_key: Option<InternalKey> = None;

        for (index, iter) in self.iterators.iter().enumerate() {
            if let Some(key) = iter.key() {
                match &smallest_key {
                    None => {
                        smallest_index = Some(index);
                        smallest_key = Some(key.clone());
                    }
                    Some(current_smallest) => {
                        if key < current_smallest {
                            smallest_index = Some(index);
                            smallest_key = Some(key.clone());
                        }
                    }
                }
            }
        }
        self.current_index = smallest_index;
    }

    pub fn seek_to_last(&mut self) -> Result<(), DBError> {
        for iter in &mut self.iterators {
            iter.seek_to_last()?;
        }
        self.find_largest();
        self.direction = Direction::Reverse;
        Ok(())
    }

    fn find_largest(&mut self) {
        let mut largest_index: Option<usize> = None;
        let mut largest_key: Option<InternalKey> = None;

        for (index, iter) in self.iterators.iter().enumerate() {
            if let Some(key) = iter.key() {
                match &largest_key {
                    None => {
                        largest_index = Some(index);
                        largest_key = Some(key.clone());
                    }
                    Some(current_largest) => {
                        if key > current_largest {
                            largest_index = Some(index);
                            largest_key = Some(key.clone());
                        }
                    }
                }
            }
        }
        self.current_index = largest_index;
    }

    pub fn seek(&mut self, key: &InternalKey) -> Result<(), DBError> {
        self.direction = Direction::Forward;
        for iter in &mut self.iterators {
            iter.seek(key)?;
        }
        self.find_smallest();
        Ok(())
    }

    pub fn key(&self) -> Option<InternalKey> {
        self.current_index
            .and_then(|index| self.iterators[index].key().cloned())
    }

    pub fn next(&mut self) -> Result<(), DBError> {
        if self.direction != Direction::Forward {
            // Ensure that all children are positioned after key()
            if let Some(current_index) = self.current_index {
                let key = self.key();
                for (index, iter) in self.iterators.iter_mut().enumerate() {
                    if index != current_index {
                        if let Some(ref k) = key {
                            iter.seek(k)?; // Direct method call - no borrow checking
                        }
                        let should_advance = iter.valid() && (key == iter.key().cloned());
                        if should_advance {
                            iter.next()?; // Direct method call - no borrow checking
                        }
                    }
                }
            }
            self.direction = Direction::Forward;
        }

        if let Some(current_index) = self.current_index {
            self.iterators[current_index].next()?; // Direct method call - safe
        }
        self.find_smallest();
        Ok(())
    }

    pub fn prev(&mut self) -> Result<(), DBError> {
        if self.direction != Direction::Reverse {
            // Ensure that all children are positioned before key()
            if let Some(current_index) = self.current_index {
                let key = self.key();
                for (index, iter) in self.iterators.iter_mut().enumerate() {
                    if index != current_index {
                        if let Some(ref k) = key {
                            iter.seek(k)?;
                        }
                        let is_valid = iter.valid();
                        if is_valid {
                            iter.prev()?;
                        } else {
                            // has no entries >= key().  Position at last entry.
                            iter.seek_to_last()?;
                        }
                    }
                }
            }
            self.direction = Direction::Reverse;
        }

        if let Some(current_index) = self.current_index {
            self.iterators[current_index].prev()?;
        }
        self.find_largest();
        Ok(())
    }
}

#[cfg(test)]
#[allow(warnings)]
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

        let mut iter = DBIterator::new(&db).unwrap();
        // 正向遍历
        iter.seek_to_first();
        let mut keys = Vec::new();
        let mut values = Vec::new();
        while iter.valid() {
            keys.push(iter.key().to_vec());
            values.push(iter.value().to_vec());
            iter.next();
        }
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(values, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);

        // 反向遍历
        iter.seek_to_last();
        let mut rev_keys = Vec::new();
        let mut rev_values = Vec::new();
        while iter.valid() {
            rev_keys.push(iter.key().to_vec());
            rev_values.push(iter.value().to_vec());
            iter.prev();
        }
        assert_eq!(
            rev_values,
            vec![b"3".to_vec(), b"2".to_vec(), b"1".to_vec()]
        );

        // seek 到 b
        iter.seek(b"b");
        assert!(iter.valid());
        assert_eq!(iter.key(), b"b");
        assert_eq!(iter.value(), b"2");
        // next
        iter.next();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"c");
        // prev
        iter.prev();
        assert!(iter.valid());
        assert_eq!(iter.key(), b"b");
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
        let iter1 = Box::new(MemTableIter::new(&mem1));
        let iter2 = Box::new(MemTableIter::new(&mem2));
        let mut merge_iter = MergingIterator::new(vec![iter1, iter2]);
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

    #[test]
    fn test_db_write_batch_threshold_concurrent() {
        use futures::future::join_all;
        use std::thread;
        use std::time::Duration;
        use tokio::runtime::Runtime;

        let rt = Runtime::new().unwrap();
        let db = Arc::new(DB::new().unwrap());

        let total = BATCH_COMBINE_THRESHOLD * 3 + 7;
        let concurrency = 150;

        rt.block_on(async {
            let mut tasks = Vec::new();
            for t in 0..concurrency {
                let db = db.clone();
                let start = t * (total / concurrency);
                let end = if t == concurrency - 1 {
                    total
                } else {
                    (t + 1) * (total / concurrency)
                };
                tasks.push(tokio::spawn(async move {
                    for i in start..end {
                        let mut batch = WriteBatch::new();
                        batch.put(format!("k{}", i).as_bytes(), format!("v{}", i).as_bytes());
                        db.write(WriteOptions { sync: false }, Some(batch))
                            .await
                            .unwrap();
                    }
                }));
            }
            join_all(tasks).await;
        });

        // 等待后台线程处理
        thread::sleep(Duration::from_millis(200));

        // 检查部分 key 是否写入成功
        assert_eq!(db.get(b"k0").unwrap(), Some(b"v0".to_vec()));
        assert_eq!(
            db.get(format!("k{}", total - 1).as_bytes()).unwrap(),
            Some(format!("v{}", total - 1).as_bytes().to_vec())
        );

        // 你可以在 queue.drain(..) 处加 println!("drain batch, len={}", batch_len);
        // 运行测试时应能看到多次批量处理（合并）。
    }

    #[test]
    fn test_mem_to_imm_switch_only() {
        use std::thread;
        use std::time::Duration;
        use tokio::runtime::Runtime;

        let rt = Runtime::new().unwrap();
        // 创建禁用 compaction 的数据库
        let mut config = DBConfig::default();
        config.test_disable_compaction = true;
        let db = DB::new_with_config("test_db", config).unwrap();

        // 写入大量数据，超过 COMPACTION_THRESHOLD，触发 memtable -> imm
        let value = vec![b'x'; 1024]; // 1KB value
        let key_prefix = b"key_";
        let batch_size = COMPACTION_MEMORY_THRESHOLD / 1024 + 10; // 保证超过阈值

        // 检查初始状态
        {
            let inner = db.inner.lock().unwrap();
            assert!(inner.imm.is_none(), "imm should be None initially");
            assert!(inner.mem.aproximate_size() < COMPACTION_MEMORY_THRESHOLD);
            assert!(
                !db.has_imm.load(std::sync::atomic::Ordering::Relaxed),
                "has_imm should be false initially"
            );
        }

        rt.block_on(async {
            let mut batch = WriteBatch::new();
            for i in 0..batch_size {
                let mut key = key_prefix.to_vec();
                key.extend_from_slice(i.to_string().as_bytes());
                batch.put(&key, &value);
            }

            db.write(WriteOptions { sync: false }, Some(batch))
                .await
                .unwrap();
        });

        // 短暂等待，让写入线程处理完毕
        thread::sleep(Duration::from_millis(50));

        {
            let inner = db.inner.lock().unwrap();
            // 验证 mem 已经切换到 imm
            assert!(
                inner.imm.is_some(),
                "imm should be Some after memtable switch"
            );
            // 验证新的 mem 是空的或者很小
            assert!(
                inner.mem.aproximate_size() < COMPACTION_MEMORY_THRESHOLD,
                "new mem should be small after switch"
            );
            // 验证 has_imm 标志
            assert!(
                db.has_imm.load(std::sync::atomic::Ordering::Relaxed),
                "has_imm flag should be true"
            );
        }

        // 再写入一条数据，应该写到新的 memtable
        rt.block_on(async {
            let mut batch = WriteBatch::new();
            batch.put(b"new_key", b"new_value");
            db.write(WriteOptions { sync: false }, Some(batch))
                .await
                .unwrap();
        });

        // 短暂等待处理
        thread::sleep(Duration::from_millis(10));

        // 验证 imm 仍然存在（因为 compaction 被禁用）
        {
            let inner = db.inner.lock().unwrap();
            assert!(
                inner.imm.is_some(),
                "imm should still exist when compaction is disabled"
            );
            assert!(
                db.has_imm.load(std::sync::atomic::Ordering::Relaxed),
                "has_imm flag should remain true when compaction is disabled"
            );
        }

        // 验证数据能够正确读取（从 mem 和 imm）
        assert_eq!(db.get(b"new_key").unwrap(), Some(b"new_value".to_vec()));

        // 验证至少能找到一个旧的 key（在 imm 中）
        let mut found_old = false;
        for i in 0..batch_size.min(5) {
            let mut key = key_prefix.to_vec();
            key.extend_from_slice(i.to_string().as_bytes());
            if db.get(&key).unwrap().is_some() {
                found_old = true;
                break;
            }
        }
        assert!(found_old, "Should find at least one old key in imm");

        // 打印状态信息
        let inner = db.inner.lock().unwrap();
        if let Some(ref imm) = inner.imm {
            println!("imm approximate_size = {}", imm.aproximate_size());
        }
        println!("new mem approximate_size = {}", inner.mem.aproximate_size());
    }

    #[test]
    fn test_imm_compaction_to_sst_and_read() {
        use std::fs;
        use std::thread;
        use std::time::Duration;
        use tokio::runtime::Runtime;

        let rt = Runtime::new().unwrap();
        let dbname = "test_compaction_db";
        let db = DB::new_with_name(dbname).unwrap();

        // Write enough data to trigger memtable -> imm switch
        let value: Vec<u8> = vec![b'x'; 1024]; // 1KB value
        let key_prefix = b"compact_key_";
        let batch_size = COMPACTION_MEMORY_THRESHOLD / 1024 + 10; // Ensure threshold is exceeded

        // Phase 1: Write data to trigger imm switch
        rt.block_on(async {
            let mut batch = WriteBatch::new();
            for i in 0..batch_size {
                let mut key = key_prefix.to_vec();
                key.extend_from_slice(i.to_string().as_bytes());
                batch.put(&key, &value);
            }

            db.write(WriteOptions { sync: false }, Some(batch))
                .await
                .unwrap();
        });

        // Wait for write thread to process and switch memtable (shorter wait)
        thread::sleep(Duration::from_millis(20));

        // Check if imm exists (it should after memtable switch)
        let imm_exists = {
            let inner = db.inner.lock().unwrap();
            inner.imm.is_some()
        };

        if imm_exists {
            println!("imm exists after write, waiting for compaction to complete");
            // Phase 2: Wait for compaction to complete
            let mut compaction_completed = false;
            for i in 0..50 {
                // Wait up to 5 seconds
                thread::sleep(Duration::from_millis(100));
                if !db.has_imm.load(std::sync::atomic::Ordering::Relaxed) {
                    compaction_completed = true;
                    println!("Compaction completed after {}ms", (i + 1) * 100);
                    break;
                }
            }
            assert!(
                compaction_completed,
                "Compaction should complete within 5 seconds"
            );
        } else {
            println!("imm already compacted, waiting a bit more to ensure SST file is created");
            // If imm is already compacted, wait a bit more for file creation
            thread::sleep(Duration::from_millis(50));
        }

        // Verify imm is cleared after compaction
        {
            let inner = db.inner.lock().unwrap();
            assert!(
                inner.imm.is_none(),
                "imm should be cleared after compaction"
            );
        }

        // Phase 3: Verify SST file is created
        let sst_files: Vec<_> = fs::read_dir(".")
            .unwrap()
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                let filename = path.file_name()?.to_str()?;
                if filename.starts_with(dbname) && filename.ends_with(".ldb") {
                    Some(filename.to_string())
                } else {
                    None
                }
            })
            .collect();

        assert!(
            !sst_files.is_empty(),
            "At least one SST file should be created"
        );
        println!("Created SST files: {:?}", sst_files);

        // Phase 4: Verify data can be read from SST files
        // The data should now be in SST files, not in memory
        let mut found_keys = 0;
        for i in 0..batch_size.min(10) {
            // Test first 10 keys
            let mut key = key_prefix.to_vec();
            key.extend_from_slice(i.to_string().as_bytes());

            match db.get(&key) {
                Ok(Some(retrieved_value)) => {
                    assert_eq!(
                        retrieved_value, value,
                        "Retrieved value should match original"
                    );
                    found_keys += 1;
                }
                Ok(None) => {
                    // Key not found, this might be expected in some cases
                }
                Err(e) => {
                    panic!("Error reading key {}: {:?}", i, e);
                }
            }
        }

        assert!(
            found_keys > 0,
            "Should find at least some keys in SST files"
        );
        println!("Successfully read {} keys from SST files", found_keys);

        // Phase 5: Test that new writes go to new memtable and can be read
        rt.block_on(async {
            db.put(b"new_after_compaction", b"new_value").await.unwrap();
        });

        // This should be readable from the new memtable
        assert_eq!(
            db.get(b"new_after_compaction").unwrap(),
            Some(b"new_value".to_vec()),
            "New data should be readable from new memtable"
        );

        // Phase 6: Verify that both old data (from SST) and new data (from mem) are accessible
        let test_key = format!("{}0", String::from_utf8_lossy(key_prefix));
        assert_eq!(
            db.get(test_key.as_bytes()).unwrap(),
            Some(value.clone()),
            "Old data should still be accessible from SST"
        );
        /*if let Ok(Some(_)) = db.get(test_key.as_bytes()) {
            println!("Successfully read old data from SST file");
        }*/

        // Clean up SST files
        for sst_file in sst_files {
            let _ = fs::remove_file(sst_file);
        }
    }

    #[test]
    fn test_multiple_memtable_compactions() {
        use std::fs;
        use std::thread;
        use std::time::Duration;
        use tokio::runtime::Runtime;

        let rt = Runtime::new().unwrap();
        let dbname = "test_multi_compaction_db";
        let db = DB::new_with_name(dbname).unwrap();

        // Trigger multiple compactions
        let value = vec![b'y'; 1024];
        let batch_size = COMPACTION_MEMORY_THRESHOLD / 1024 + 5;

        for round in 0..3 {
            rt.block_on(async {
                let mut batch = WriteBatch::new();
                for i in 0..batch_size {
                    let key = format!("round_{}_key_{}", round, i);
                    batch.put(key.as_bytes(), &value);
                }

                db.write(WriteOptions { sync: false }, Some(batch))
                    .await
                    .unwrap();
            });

            // Wait for compaction to complete
            thread::sleep(Duration::from_millis(200));

            // Wait for has_imm to be cleared
            for _ in 0..30 {
                if !db.has_imm.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                thread::sleep(Duration::from_millis(50));
            }
        }

        // Verify we can read data from all rounds
        let mut total_found = 0;
        for round in 0..3 {
            let mut round_found = 0;
            for i in 0..batch_size.min(3) {
                let key = format!("round_{}_key_{}", round, i);
                if let Ok(Some(retrieved_value)) = db.get(key.as_bytes()) {
                    assert_eq!(retrieved_value, value);
                    round_found += 1;
                    total_found += 1;
                }
            }
            println!("Found {} keys from round {}", round_found, round);
        }

        assert!(
            total_found > 0,
            "Should find keys from multiple compaction rounds"
        );
        println!("Total keys found across all rounds: {}", total_found);

        // Clean up
        let sst_files: Vec<_> = fs::read_dir(".")
            .unwrap()
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                let filename = path.file_name()?.to_str()?;
                if filename.starts_with(dbname) && filename.ends_with(".ldb") {
                    Some(filename.to_string())
                } else {
                    None
                }
            })
            .collect();

        for sst_file in sst_files {
            let _ = fs::remove_file(sst_file);
        }
    }
}
