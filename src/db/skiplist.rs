// 这一段来自 leveldb,目的是做到相同的线程语义
// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

use std::{
    sync::atomic::{AtomicU8, AtomicUsize},
    u8, vec,
};

pub trait Size {
    fn size(&self) -> usize;
}

pub trait Key: Ord + Size + Clone + Default {}

const MAX_HEIGHT: u8 = 12;
const HEAD: usize = 1;
const TAIL: usize = 0;

pub struct SkipList<K>
where
    K: Key,
{
    // Modified only by Insert().  Read racily by readers, but stale
    // values are ok.
    max_height: AtomicU8,
    // index 0表示尾，1表示头
    arena: Vec<Node<K>>,
}

impl<K> SkipList<K>
where
    K: Key,
{
    pub(super) fn new() -> Self {
        let head = Node::new(K::default(), MAX_HEIGHT);
        let end = Node::new(K::default(), MAX_HEIGHT);
        let arena = vec![head, end];
        SkipList {
            max_height: AtomicU8::new(1),
            arena: arena,
        }
    }

    fn new_node(&mut self, key: &K, height: u8) -> usize {
        let node = Node::new(key.clone(), height);
        self.arena.push(node);
        self.arena.len() - 1
    }

    fn find_great_or_equal(&self, key: &K, prev: &mut Vec<usize>) -> usize {
        let mut x = HEAD;
        let mut level = self.get_max_height() - 1;
        loop {
            let next = self.arena[x].next(level as usize);
            if self.key_is_after_node(key, next) {
                // keep searching
                x = next;
            } else {
                if prev.len() != 0 {
                    prev[level as usize] = x;
                }
                if level == 0 {
                    return next;
                } else {
                    // go down a level
                    level -= 1;
                }
            }
        }
    }

    fn find_less_than(&self, key: &K) -> usize {
        let mut x = HEAD;
        let mut level = self.get_max_height() - 1;
        loop {
            assert!(x == HEAD || self.arena[x].get_key() < key);
            let next = self.arena[x].next(level as usize);
            if (next == TAIL) || (self.arena[next].get_key() >= key) {
                if level == 0 {
                    return x;
                } else {
                    // go down a level
                    level -= 1;
                }
            } else {
                x = next;
            }
        }
    }

    fn find_last(&self) -> usize {
        let mut x = HEAD;
        let mut level = self.get_max_height() - 1;
        loop {
            let next = self.arena[x].next(level as usize);
            if next == TAIL {
                if level == 0 {
                    return x;
                } else {
                    // go down a level
                    level -= 1;
                }
            } else {
                x = next;
            }
        }
    }

    fn get_max_height(&self) -> u8 {
        self.max_height.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn key_is_after_node(&self, key: &K, n: usize) -> bool {
        (n != TAIL) && (self.arena[n].get_key() < key)
    }

    fn reandom_height(&self) -> u8 {
        const BRANCH: u8 = 4;
        let mut height = 1;
        while height < MAX_HEIGHT && (rand::random::<u8>() % BRANCH == 0) {
            height += 1;
        }
        assert!(height <= MAX_HEIGHT);
        height
    }

    pub fn contains(&self, key: &K) -> bool {
        let mut prev = vec![];
        let x = self.find_great_or_equal(key, &mut prev);
        if x == TAIL {
            return false;
        }
        self.arena[x].get_key() == key
    }

    pub fn insert(&mut self, key: &K) {
        let mut prev = vec![0; MAX_HEIGHT as usize];
        let x = self.find_great_or_equal(&key, &mut prev);
        // Our data structure does not allow duplicate insertion
        assert!((x == TAIL) || (self.arena[x].get_key() != key));

        let height = self.reandom_height();
        if height > self.get_max_height() {
            for i in self.get_max_height()..height {
                prev[i as usize] = HEAD;
            }
            // It is ok to mutate max_height_ without any synchronization
            // with concurrent readers.  A concurrent reader that observes
            // the new value of max_height_ will see either the old value of
            // new level pointers from head_ (nullptr), or a new value set in
            // the loop below.  In the former case the reader will
            // immediately drop to the next level since nullptr sorts after all
            // keys.  In the latter case the reader will use the new node.
            self.max_height
                .store(height, std::sync::atomic::Ordering::Relaxed);
        }

        let new = self.new_node(key, height);
        for i in 0..height {
            let prev_i = &self.arena[prev[i as usize]];
            // NoBarrier_SetNext() suffices since we will add a barrier when
            // we publish a pointer to "x" in prev[i].
            self.arena[new].no_barrier_set_next(i as usize, prev_i.no_barrier_next(i as usize));
            prev_i.set_next(i as usize, new);
        }
    }

    pub fn iter(&self) -> SkipListIter<K> {
        SkipListIter::new(self)
    }

    pub fn approximate_size(&self) -> usize {
        self.arena
            .iter()
            .map(|node| {
                std::mem::size_of_val(node)
                    + node.key.size()
                    + node.indexes.capacity() * std::mem::size_of::<usize>()
            })
            .sum()
    }
}

struct Node<Key> {
    key: Key,
    indexes: Vec<AtomicUsize>,
}

impl<Key> Node<Key> {
    fn new(key: Key, height: u8) -> Self {
        Node {
            key: key,
            indexes: (0..height).map(|_| AtomicUsize::new(0)).collect(),
        }
    }
    fn next(&self, n: usize) -> usize {
        self.indexes[n].load(std::sync::atomic::Ordering::Acquire)
    }
    fn set_next(&self, n: usize, index: usize) {
        self.indexes[n].store(index, std::sync::atomic::Ordering::Release);
    }

    fn no_barrier_next(&self, n: usize) -> usize {
        self.indexes[n].load(std::sync::atomic::Ordering::Relaxed)
    }

    fn no_barrier_set_next(&self, n: usize, index: usize) {
        self.indexes[n].store(index, std::sync::atomic::Ordering::Relaxed);
    }
    fn get_key(&self) -> &Key {
        &self.key
    }
}

pub struct SkipListIter<'a, K>
where
    K: Key,
{
    list: &'a SkipList<K>,
    current: usize,
}

impl<'a, K> SkipListIter<'a, K>
where
    K: Key,
{
    pub fn new(list: &'a SkipList<K>) -> Self {
        SkipListIter {
            list,
            current: TAIL,
        }
    }

    pub fn seek_to_first(&mut self) {
        self.current = self.list.arena[HEAD].next(0);
    }

    pub fn seek_to_last(&mut self) {
        self.current = self.list.find_last();
        if self.current == HEAD {
            self.current = TAIL;
        }
    }

    pub fn seek(&mut self, key: &K) {
        self.current = self.list.find_great_or_equal(key, &mut vec![]);
    }

    pub fn valid(&self) -> bool {
        self.current != TAIL
    }

    pub fn key(&self) -> Option<&'a K> {
        if self.valid() {
            Some(self.list.arena[self.current].get_key())
        } else {
            None
        }
    }

    pub fn next(&mut self) {
        if self.valid() {
            self.current = self.list.arena[self.current].next(0);
        }
    }

    pub fn prev(&mut self) {
        if self.valid() {
            self.current = self
                .list
                .find_less_than(self.list.arena[self.current].get_key());
            if self.current == HEAD {
                self.current = TAIL;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use rand::RngCore;

    use super::*;

    #[derive(Clone, Default, Ord, PartialOrd, Eq, PartialEq, Debug)]
    struct IntKey(i32);

    impl Key for IntKey {}
    impl Size for IntKey {
        fn size(&self) -> usize {
            std::mem::size_of::<i32>()
        }
    }

    #[test]
    fn test_insert_and_contains_random() {
        use rand::Rng;
        let mut skiplist = SkipList::new();
        let mut rng = rand::thread_rng();
        let mut nums = Vec::new();
        for _ in 0..2000 {
            let n = rng.gen_range(0..5000);
            if !skiplist.contains(&IntKey(n)) {
                skiplist.insert(&IntKey(n));
                nums.push(n);
            }
        }
        // 检查插入的数都能查到
        for n in nums.iter() {
            assert!(skiplist.contains(&IntKey(*n)), "Should contain {}", n);
        }
        // 正序遍历 skiplist，收集所有 key
        let mut iter = skiplist.iter();
        iter.seek_to_first();
        let mut result = vec![];
        while iter.valid() {
            result.push(iter.key().unwrap().0);
            iter.next();
        }
        // 检查遍历结果是升序且包含所有插入的 key
        let mut nums_sorted = nums.clone();
        nums_sorted.sort();
        assert_eq!(result, nums_sorted);
        // 逆序遍历 skiplist，收集所有 key
        if !result.is_empty() {
            let mut iter = skiplist.iter();
            iter.seek_to_last();
            let mut rev_result = vec![];
            while iter.valid() {
                rev_result.push(iter.key().unwrap().0);
                iter.prev();
            }
            let mut nums_sorted_rev = nums_sorted.clone();
            nums_sorted_rev.reverse();
            assert_eq!(rev_result, nums_sorted_rev);
        }

        // seek 测试
        let mut iter = skiplist.iter();
        // seek 到已存在的 key
        if !nums_sorted.is_empty() {
            let mid = nums_sorted[nums_sorted.len() / 2];
            iter.seek(&IntKey(mid));
            assert!(iter.valid());
            assert_eq!(iter.key().unwrap().0, mid);

            // seek 到比最小 key 小的 key，应指向最小 key
            let min = *nums_sorted.first().unwrap();
            iter.seek(&IntKey(min - 1));
            assert!(iter.valid());
            assert_eq!(iter.key().unwrap().0, min);

            // seek 到比最大 key 大的 key，应 invalid
            let max = *nums_sorted.last().unwrap();
            iter.seek(&IntKey(max + 1));
            assert!(!iter.valid());

            // 随机 seek 测试
            let mut rng = rand::thread_rng();
            for _ in 0..20 {
                let idx = rng.gen_range(0..nums_sorted.len());
                let target = nums_sorted[idx];
                iter.seek(&IntKey(target));
                assert!(iter.valid());
                assert_eq!(iter.key().unwrap().0, target);

                // 随机 seek 到不存在的 key，应该指向下一个更大的 key或invalid
                let fake = target - 1;
                iter.seek(&IntKey(fake));
                if let Some(&next) = nums_sorted.iter().find(|&&x| x >= fake) {
                    assert!(iter.valid());
                    assert_eq!(iter.key().unwrap().0, next);
                } else {
                    assert!(!iter.valid());
                }
            }
        }
    }

    #[test]
    fn test_concurrent_read_visibility_loop() {
        for round in 0..10 {
            println!("[test_concurrent_read_visibility] round {}", round + 1);
            run_concurrent_read_visibility();
            println!("[test_concurrent_read_visibility] round {} done", round + 1);
        }
    }

    #[test]
    fn run_concurrent_read_visibility() {
        use rand::Rng;
        use std::sync::{
            Arc, Barrier,
            atomic::{AtomicUsize, Ordering},
        };
        use std::thread;
        use std::time::Duration;

        const N_KEYS: usize = 32;
        const N_READERS: usize = 4;
        const N_WRITES: usize = 2000;

        // Key: (id, generation, hash)
        #[derive(Clone, Default, Eq, PartialEq, Debug)]
        struct TestKey {
            id: usize,
            generation: usize,
            hash: u64,
        }
        impl Ord for TestKey {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                (self.id, self.generation, self.hash).cmp(&(other.id, other.generation, other.hash))
            }
        }
        impl PartialOrd for TestKey {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Key for TestKey {}
        impl Size for TestKey {
            fn size(&self) -> usize {
                std::mem::size_of::<usize>() * 2 + std::mem::size_of::<u64>()
            }
        }

        fn make_key(id: usize, generation: usize) -> TestKey {
            let hash = (id as u64)
                .wrapping_mul(6364136223846793005)
                .wrapping_add(generation as u64);
            TestKey {
                id,
                generation,
                hash,
            }
        }

        fn random_target(rng: &mut impl RngCore) -> TestKey {
            match rng.next_u32() % 10 {
                0 => make_key(0, 0),
                1 => make_key(N_KEYS, 0),
                _ => make_key(rng.next_u32() as usize % N_KEYS, 0),
            }
        }

        let skiplist = Arc::new(SkipList::new());
        let latest_gen = Arc::new((0..N_KEYS).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let barrier = Arc::new(Barrier::new(N_READERS + 1));
        let quit = Arc::new(AtomicBool::new(false));

        // 写线程
        let skiplist_writer = skiplist.clone();
        let latest_gen_writer = latest_gen.clone();
        let barrier_writer = barrier.clone();
        let quit_writer = quit.clone();
        let writer = thread::spawn(move || {
            barrier_writer.wait();
            let mut rng = rand::thread_rng();
            let skiplist_mut = Arc::as_ptr(&skiplist_writer) as *mut SkipList<_>;
            for _ in 0..N_WRITES {
                let id = rng.gen_range(0..N_KEYS);
                let generation = latest_gen_writer[id].load(Ordering::Acquire) + 1;
                let key = make_key(id, generation);
                unsafe {
                    (*skiplist_mut).insert(&key);
                }
                latest_gen_writer[id].store(generation, Ordering::Release);
                if generation % 10 == 0 {
                    thread::sleep(Duration::from_millis(1));
                }
            }
            quit_writer.store(true, Ordering::Release);
        });

        // 读线程
        let mut readers = vec![];
        for _ in 0..N_READERS {
            let skiplist_reader = skiplist.clone();
            let latest_gen_reader = latest_gen.clone();
            let barrier_reader = barrier.clone();
            let quit_reader = quit.clone();

            readers.push(thread::spawn(move || {
                let mut rng = rand::thread_rng();
                barrier_reader.wait();

                while !quit_reader.load(Ordering::Acquire) {
                    // Remember the initial committed state of the skiplist.
                    let snapshot: Vec<usize> = (0..N_KEYS)
                        .map(|i| latest_gen_reader[i].load(Ordering::Acquire))
                        .collect();

                    let start_key = random_target(&mut rng);
                    let list = unsafe { &*Arc::as_ptr(&skiplist_reader) };
                    let mut iter = list.iter();
                    iter.seek(&start_key);

                    let mut pos = start_key.clone();
                    loop {
                        let current = match iter.key() {
                            None => make_key(N_KEYS, 0),
                            Some(k) => k.clone(),
                        };
                        assert!(start_key <= current, "Iterator should not go backwards");

                        // Verify that everything in [pos,current) was not present in
                        // initial_state.
                        while pos < current {
                            assert!(pos.id < N_KEYS, "Key ID out of bounds");

                            assert!(
                                pos.generation == 0 || pos.generation > snapshot[pos.id],
                                "Key {:?}, initgen {:?} should not be present in initial state",
                                pos,
                                snapshot[pos.id]
                            );
                            // Advance to next key in the valid key space
                            if pos.id < current.id {
                                pos = make_key(pos.id + 1, 0);
                            } else {
                                pos = make_key(pos.id, pos.generation + 1);
                            }
                        }

                        if !iter.valid() {
                            break;
                        }

                        if rng.next_u32() % 2 == 0 {
                            iter.next();
                            pos = make_key(pos.id, pos.generation + 1)
                        } else {
                            let new_target = random_target(&mut rng);
                            if new_target > pos {
                                pos = new_target;
                            }
                            iter.seek(&pos);
                        }

                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }));
        }
        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }
    }
}
