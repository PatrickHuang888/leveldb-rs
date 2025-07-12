use crate::DBError;
use crate::db::InternalIterator;
use crate::db::InternalKey;
use crate::db::dbformat::ValueType;
use crate::table::table::Table;
use crate::table::table::TableIterator;
use crate::util::file::db_file_name;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::sync::Arc;

const NUM_LEVELS: u64 = 7; // 假设有7个层级

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FileMetaData {
    pub file_number: u64,
    pub file_size: u64,
    pub smallest_key: Option<InternalKey>,
    pub largest_key: Option<InternalKey>,
}

impl Ord for FileMetaData {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        assert!(
            self.smallest_key.is_some() && other.smallest_key.is_some(),
            "Keys must be present"
        );
        if self.smallest_key.as_ref().unwrap() < other.smallest_key.as_ref().unwrap() {
            std::cmp::Ordering::Less
        } else {
            self.file_number.cmp(&other.file_number)
        }
    }
}

impl PartialOrd for FileMetaData {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
pub(super) struct VersionEdit {
    pub deleted_files: Vec<(u64, u64)>,      // (level, file_number)
    pub new_files: Vec<(u64, FileMetaData)>, // (level, FileMetaData)
}

impl VersionEdit {
    pub fn new() -> Self {
        VersionEdit {
            deleted_files: Vec::new(),
            new_files: Vec::new(),
        }
    }

    pub fn add_file(&mut self, level: u64, file: FileMetaData) {
        self.new_files.push((level, file));
    }

    pub fn delete_file(&mut self, level: u64, file_number: u64) {
        self.deleted_files.push((level, file_number));
    }
}

pub(super) struct VersionSet {
    file_number: u64,              // 下一个文件编号
    current: Option<Arc<Version>>, // 当前版本
    versions: Vec<Arc<Version>>,
}

impl VersionSet {
    pub fn new() -> Self {
        VersionSet {
            file_number: 1, // 从1开始
            current: None,
            versions: Vec::new(),
        }
    }

    pub fn next_file_number(&mut self) -> u64 {
        self.file_number += 1;
        self.file_number
    }

    pub fn append(&mut self, v: Version) {
        let current = Arc::new(v);
        self.current = Some(current.clone());
        self.versions.push(current);
    }

    pub fn pick_level_for_memtable_output(
        &self,
        smallest: &Option<InternalKey>,
        largest: &Option<InternalKey>,
    ) -> u64 {
        // 简单的实现：总是返回0级
        // 实际应用中需要根据键的范围和现有文件来决定
        // TODO:
        0
    }

    pub fn current(&self) -> Option<Arc<Version>> {
        let c = self.current.as_ref().map(|v| v.clone());
        c
    }
}

struct LevelState {
    deleted_files: Vec<u64>,
    added_files: Vec<FileMetaData>,
}
pub(super) struct VersionBuilder {
    base: Version,
    levels: [LevelState; NUM_LEVELS as usize],
}

impl VersionBuilder {
    pub fn new(base: Version) -> Self {
        VersionBuilder {
            base,
            levels: std::array::from_fn(|_| LevelState {
                deleted_files: Vec::new(),
                added_files: Vec::new(),
            }),
        }
    }

    pub fn apply(&mut self, edit: &VersionEdit) {
        for (level, file_meta) in &edit.new_files {
            self.levels[*level as usize]
                .added_files
                .push(file_meta.clone());
            self.levels[*level as usize]
                .deleted_files
                .retain(|&f| f != file_meta.file_number);
        }
        for (level, file_number) in &edit.deleted_files {
            self.levels[*level as usize]
                .deleted_files
                .push(*file_number);
        }
    }

    pub fn build(mut self) -> Version {
        use std::collections::HashSet;
        for level in 0..NUM_LEVELS {
            let state = &self.levels[level as usize];
            let files = &mut self.base.files[level as usize];

            if state.deleted_files.is_empty() && state.added_files.is_empty() {
                continue;
            }

            let deleted: &Vec<u64> = &state.deleted_files;
            let added: &Vec<FileMetaData> = &state.added_files;
            let added_file_numbers: HashSet<u64> = added.iter().map(|f| f.file_number).collect();

            // 只保留未被删除且不被新增覆盖的文件
            files.retain(|f| {
                !deleted.contains(&f.file_number) && !added_file_numbers.contains(&f.file_number)
            });

            // 只添加未被删除的新增文件
            files.extend(
                added
                    .iter()
                    .filter(|f| !deleted.contains(&f.file_number))
                    .cloned(),
            );

            // 排序
            files.sort();

            // Debug 检查：level > 0 不允许区间重叠
            #[cfg(debug_assertions)]
            if level > 0 {
                for w in files.windows(2) {
                    let a = &w[0];
                    let b = &w[1];
                    if let (Some(a_largest), Some(b_smallest)) = (&a.largest_key, &b.smallest_key) {
                        assert!(
                            a_largest < b_smallest,
                            "Level {} 文件区间重叠: [{:?}, {:?}] vs [{:?}, {:?}]",
                            level,
                            a.smallest_key,
                            a_largest,
                            b_smallest,
                            b.largest_key
                        );
                    }
                }
            }
        }
        self.base
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Version {
    files: [Vec<FileMetaData>; NUM_LEVELS as usize],
}

impl Version {
    pub fn get(&self, dbname: &str, key: &InternalKey) -> Result<Option<Vec<u8>>, DBError> {
        // Search through all levels, starting from level 0
        for level in 0..NUM_LEVELS {
            let level_files = &self.files[level as usize];

            if level == 0 {
                // Level 0 files may have overlapping ranges, so we need to check all files
                // Sort by newest first (highest file number first) to get the most recent value
                let mut candidates: Vec<&FileMetaData> = level_files
                    .iter()
                    .filter(|file| self.file_may_contain_key(file, key))
                    .collect();

                // Sort by file number in descending order (newest first)
                candidates.sort_by(|a, b| b.file_number.cmp(&a.file_number));

                for file in candidates {
                    let file_path = db_file_name(dbname, file.file_number);
                    match self.search_in_file(&file_path, key)? {
                        Some(value) => return Ok(Some(value)),
                        None => continue,
                    }
                }
            } else {
                // Level > 0: files are sorted and non-overlapping
                // Use binary search to find the file that might contain the key
                if let Some(file) = self.find_file_in_level(level_files, key) {
                    let file_path = db_file_name(dbname, file.file_number);
                    if let Some(value) = self.search_in_file(&file_path, key)? {
                        return Ok(Some(value));
                    }
                }
            }
        }

        Ok(None)
    }

    fn file_may_contain_key(&self, file: &FileMetaData, key: &InternalKey) -> bool {
        match (&file.smallest_key, &file.largest_key) {
            (Some(smallest), Some(largest)) => {
                key.user_key() >= smallest.user_key() && key.user_key() <= largest.user_key()
            }
            _ => false, // File without key range should not be searched
        }
    }

    fn find_file_in_level<'a>(
        &self,
        files: &'a [FileMetaData],
        key: &InternalKey,
    ) -> Option<&'a FileMetaData> {
        // Binary search for the file that might contain the key
        // Find the first file whose largest_key >= key
        let mut left = 0;
        let mut right = files.len();

        while left < right {
            let mid = (left + right) / 2;
            let file = &files[mid];

            match &file.largest_key {
                Some(largest) if largest.user_key() < key.user_key() => {
                    left = mid + 1;
                }
                _ => {
                    right = mid;
                }
            }
        }

        // Check if the found file actually contains the key range
        if left < files.len() {
            let file = &files[left];
            if self.file_may_contain_key(file, key) {
                Some(file)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn search_in_file(&self, file: &str, key: &InternalKey) -> Result<Option<Vec<u8>>, DBError> {
        // TODO: This should use a file cache to avoid repeatedly opening files
        // For now, we'll open the file each time

        match File::open(file) {
            Ok(file_handle) => {
                let mut table = Table::open(file_handle)?;
                if let Some(v) = table.get(key)? {
                    match v.value_type() {
                        ValueType::TypeDeletion => {
                            // Key found but marked as deletion
                            Err(DBError::NotFound)
                        }
                        ValueType::TypeValue => Ok(Some(v.value().to_vec())),
                        ValueType::TypeNotSet => Err(DBError::Corruption),
                    }
                } else {
                    // Key not found in this file
                    Ok(None)
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) fn new_files_iterators(
        &self,
        dbname: &str,
    ) -> Result<Vec<Box<dyn InternalIterator>>, DBError> {
        let mut iterators: Vec<Box<dyn InternalIterator>> = Vec::new();

        // level 0 文件打开，因为overlap，TODO:TableCache 需要处理
        for file in &self.files[0] {
            let f = File::open(db_file_name(dbname, file.file_number))?;
            let t = Table::open(f)?;
            let iter = TableIterator::new(t);
            iterators.push(Box::new(iter));
        }

        // For levels 1 and above, create ConcatenatingIterator for each level
        for level_files in &self.files[1..] {
            if !level_files.is_empty() {
                let iter = ConcatenatingIterator::new(level_files.clone(), dbname.to_string());
                iterators.push(Box::new(iter));
            }
        }
        Ok(iterators)
    }
}

struct ConcatenatingIterator {
    index: LevelFileNumIterator,
    current_table_iter: Option<TableIterator<File>>,
    dbname: String,
}

impl ConcatenatingIterator {
    pub fn new(files: Vec<FileMetaData>, dbname: String) -> Self {
        ConcatenatingIterator {
            index: LevelFileNumIterator::new(files),
            current_table_iter: None,
            dbname,
        }
    }

    fn open_current_file(&mut self) -> Result<(), DBError> {
        assert!(
            self.index.valid(),
            "Index must be valid to open current file"
        );
        let file_number = self.index.key().unwrap();
        let file_path = db_file_name(&self.dbname, file_number);
        match File::open(file_path) {
            Ok(file_handle) => {
                let table = Table::open(file_handle)?;
                self.current_table_iter = Some(TableIterator::new(table));
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    fn skip_empty_files_forward(&mut self) -> Result<(), DBError> {
        while self.current_table_iter.is_none()
            || !self.current_table_iter.as_ref().unwrap().valid()
        {
            if !self.index.valid() {
                self.current_table_iter = None;
                return Ok(());
            }
            self.index.next();
            self.open_current_file()?;
            if let Some(ref mut iter) = self.current_table_iter {
                iter.seek_to_first()?;
            }
        }
        Ok(())
    }

    fn skip_empty_files_backward(&mut self) -> Result<(), DBError> {
        while self.current_table_iter.is_none()
            || !self.current_table_iter.as_ref().unwrap().valid()
        {
            if !self.index.valid() {
                self.current_table_iter = None;
                return Ok(());
            }
            self.index.prev();
            self.open_current_file()?;
            if let Some(ref mut iter) = self.current_table_iter {
                iter.seek_to_last()?;
            }
        }
        Ok(())
    }
}

impl InternalIterator for ConcatenatingIterator {
    fn valid(&self) -> bool {
        self.current_table_iter
            .as_ref()
            .is_some_and(|iter| iter.valid())
    }

    fn seek_to_first(&mut self) -> Result<(), DBError> {
        self.index.seek_to_first();
        if !self.index.valid() {
            self.current_table_iter = None;
            return Ok(());
        }
        self.open_current_file()?;
        if let Some(ref mut iter) = self.current_table_iter {
            iter.seek_to_first()?;
        }
        self.skip_empty_files_forward()?;
        Ok(())
    }

    fn seek_to_last(&mut self) -> Result<(), DBError> {
        self.index.seek_to_last();
        if !self.index.valid() {
            self.current_table_iter = None;
            return Ok(());
        }
        self.open_current_file()?;
        if let Some(ref mut iter) = self.current_table_iter {
            iter.seek_to_last()?;
        }
        self.skip_empty_files_backward()?;
        Ok(())
    }

    fn seek(&mut self, key: &InternalKey) -> Result<(), DBError> {
        self.index.seek(key);
        self.open_current_file()?;
        if let Some(ref mut iter) = self.current_table_iter {
            iter.seek(key)?;
        }
        self.skip_empty_files_forward()?;
        Ok(())
    }

    fn next(&mut self) -> Result<(), DBError> {
        assert!(self.valid(), "Cannot call next on an invalid iterator");
        self.current_table_iter.as_mut().unwrap().next()?;
        self.skip_empty_files_forward()?;
        Ok(())
    }

    fn prev(&mut self) -> Result<(), DBError> {
        assert!(self.valid(), "Cannot call prev on an invalid iterator");
        self.current_table_iter.as_mut().unwrap().prev()?;
        self.skip_empty_files_backward()?;
        Ok(())
    }

    fn key(&self) -> Option<&InternalKey> {
        self.current_table_iter.as_ref()?.key()
    }
}

struct LevelFileNumIterator {
    files: Vec<FileMetaData>,
    index: usize,
}
impl LevelFileNumIterator {
    fn new(files: Vec<FileMetaData>) -> Self {
        LevelFileNumIterator { files, index: 0 }
    }

    fn next(&mut self) {
        self.index += 1;
    }

    fn prev(&mut self) {
        if self.index > 0 {
            self.index -= 1;
        } else {
            self.index = self.files.len(); // 设置为无效状态
        }
    }

    fn valid(&self) -> bool {
        self.index < self.files.len()
    }

    // 返回当前文件的 file_number
    fn key(&self) -> Option<u64> {
        if self.valid() {
            Some(self.files[self.index].file_number)
        } else {
            None
        }
    }

    fn seek(&mut self, key: &InternalKey) {
        // 二分查找第一个 largest_key >= key 的文件
        let mut left = 0;
        let mut right = self.files.len();

        while left < right {
            let mid = (left + right) / 2;
            if let Some(largest) = &self.files[mid].largest_key {
                if largest < key {
                    // Key at "mid.largest" is < "target".  Therefore all
                    // files at or before "mid" are uninteresting.
                    left = mid + 1;
                } else {
                    // Key at "mid.largest" is >= "target".  Therefore all files
                    // after "mid" are uninteresting.
                    right = mid;
                }
            } else {
                // error
                self.index = self.files.len();
                return;
            }
        }
        self.index = right;
    }

    fn seek_to_first(&mut self) {
        self.index = 0;
    }

    fn seek_to_last(&mut self) {
        if !self.files.is_empty() {
            self.index = self.files.len() - 1;
        } else {
            self.index = 0;
        }
    }
}
