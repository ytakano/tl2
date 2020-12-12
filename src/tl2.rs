use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{fence, AtomicU64, Ordering};

const STRIPE_SIZE: usize = 8; // u64, 8B
const MEM_SIZE: usize = 128;

#[macro_export]
macro_rules! load {
    ($t:ident, $a:expr) => {
        if let Some(v) = ($t).load($a) {
            v
        } else {
            return tl2::TMResult::Retry;
        }
    };
}

#[macro_export]
macro_rules! store {
    ($t:ident, $a:expr, $v:expr) => {
        $t.store($a, $v)
    };
}

pub struct Memory {
    mem: Vec<u8>,
    lock_ver: Vec<AtomicU64>, // write-locks
    global_clock: AtomicU64,
    shift_size: usize,
}

pub enum TMResult<T> {
    Ok(T),
    Retry,
}

impl Memory {
    pub fn new() -> Memory {
        let mem = [0].repeat(MEM_SIZE);
        let mut lock_ver = Vec::new();

        let mut shift = 0;
        loop {
            if STRIPE_SIZE & (1 << shift) > 0 {
                break;
            }
            shift += 1;
        }

        for _ in 0..MEM_SIZE >> shift {
            lock_ver.push(AtomicU64::new(0));
        }

        Memory {
            mem: mem,
            lock_ver: lock_ver,
            global_clock: AtomicU64::new(0),
            shift_size: shift,
        }
    }

    fn test_not_modify(&self, addr: usize, rv: u64) -> bool {
        let n = self.lock_ver[addr >> self.shift_size].load(Ordering::Relaxed);
        n <= rv
    }

    fn get_addr_ver(&self, addr: usize) -> u64 {
        let n = self.lock_ver[addr >> self.shift_size].load(Ordering::Relaxed);
        n & !(1 << 63)
    }

    fn lock_addr(&mut self, addr: usize) -> bool {
        match self.lock_ver[addr >> self.shift_size].fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |val| {
                let n = val & (1 << 63);
                if n == 0 {
                    Some(val | (1 << 63))
                } else {
                    None
                }
            },
        ) {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    fn unlock_addr(&mut self, addr: usize) {
        self.lock_ver[addr >> self.shift_size].fetch_and(!(1 << 63), Ordering::Relaxed);
    }
}

pub struct WriteTrans<'a> {
    read_ver: u64,
    read_set: HashSet<usize>,
    write_set: HashMap<usize, [u8; STRIPE_SIZE]>,
    locked: Vec<usize>,
    is_abort: bool,
    mem: &'a mut Memory,
}

impl<'a> WriteTrans<'a> {
    fn new(mem: &mut Memory) -> WriteTrans {
        WriteTrans {
            read_set: HashSet::new(),
            write_set: HashMap::new(),
            locked: Vec::new(),
            is_abort: false,

            // 1. Sample global version-clock
            read_ver: mem.global_clock.load(Ordering::Acquire),

            mem: mem,
        }
    }

    pub fn load(&mut self, addr: usize) -> Option<[u8; STRIPE_SIZE]> {
        if self.is_abort {
            return None;
        }

        self.read_set.insert(addr);

        // read from write-set
        if let Some(m) = self.write_set.get(&addr) {
            return Some(*m);
        }

        // pre validation
        if !self.mem.test_not_modify(addr, self.read_ver) {
            self.is_abort = true;
            return None;
        }

        fence(Ordering::Acquire);

        // read from memory
        let mut mem = [0; STRIPE_SIZE];
        for (dst, src) in mem
            .iter_mut()
            .zip(self.mem.mem[addr..addr + STRIPE_SIZE].iter())
        {
            *dst = *src;
        }

        fence(Ordering::SeqCst);

        // post validation
        if !self.mem.test_not_modify(addr, self.read_ver) {
            self.is_abort = true;
            return None;
        }

        Some(mem)
    }

    pub fn store(&mut self, addr: usize, val: [u8; STRIPE_SIZE]) {
        self.write_set.insert(addr, val);
    }

    fn transaction<F, R>(&mut self, f: F) -> TMResult<R>
    where
        F: Fn(&mut WriteTrans) -> TMResult<R>,
    {
        // 2. Run through a speculative execution
        let result;
        match f(self) {
            TMResult::Retry => {
                return TMResult::Retry;
            }
            TMResult::Ok(val) => {
                if self.is_abort {
                    return TMResult::Retry;
                }
                result = val;
            }
        }

        // 3. Lock the write-set
        if !self.lock_write_set() {
            return TMResult::Retry;
        }

        // 4. Increment global version-clock
        let ver = 1 + self.inc_global_clock();

        // 5. Validate the read-set
        if !self.validate_read_set() {
            return TMResult::Retry;
        }

        // 6. Commit and release the locks
        self.commit(ver);

        return TMResult::Ok(result);
    }

    fn lock_write_set(&mut self) -> bool {
        for (addr, _) in self.write_set.iter() {
            if self.mem.lock_addr(*addr) {
                self.locked.push(*addr);
            } else {
                return false;
            }
        }
        true
    }

    fn validate_read_set(&self) -> bool {
        for addr in self.read_set.iter() {
            if self.write_set.contains_key(addr) {
                let ver = self.mem.get_addr_ver(*addr);
                if ver > self.read_ver {
                    return false;
                }
            } else {
                if !self.mem.test_not_modify(*addr, self.read_ver) {
                    return false;
                }
            }
        }
        true
    }

    fn inc_global_clock(&mut self) -> u64 {
        self.mem.global_clock.fetch_add(1, Ordering::AcqRel)
    }

    fn commit(&mut self, ver: u64) {
        for (addr, val) in self.write_set.iter() {
            let addr = *addr as usize;
            for (dst, src) in self.mem.mem[addr..addr + STRIPE_SIZE].iter_mut().zip(val) {
                *dst = *src;
            }
            let idx = addr >> self.mem.shift_size;
            self.mem.lock_ver[idx].store(ver, Ordering::Release);
        }

        self.locked.clear();
    }
}

impl<'a> Drop for WriteTrans<'a> {
    fn drop(&mut self) {
        for addr in self.locked.iter() {
            self.mem.unlock_addr(*addr);
        }
    }
}

pub struct ReadTrans<'a> {
    read_ver: u64,
    is_abort: bool,
    mem: &'a Memory,
}

impl<'a> ReadTrans<'a> {
    fn new(mem: &Memory) -> ReadTrans {
        ReadTrans {
            is_abort: false,

            // 1. Sample global version-clock
            read_ver: mem.global_clock.load(Ordering::Acquire),

            mem: mem,
        }
    }

    pub fn load(&mut self, addr: usize) -> Option<[u8; STRIPE_SIZE]> {
        if self.is_abort {
            return None;
        }

        // pre validation
        if !self.mem.test_not_modify(addr, self.read_ver) {
            self.is_abort = true;
            return None;
        }

        fence(Ordering::Acquire);

        // read from memory
        let mut mem = [0; STRIPE_SIZE];
        for (dst, src) in mem
            .iter_mut()
            .zip(self.mem.mem[addr..addr + STRIPE_SIZE].iter())
        {
            *dst = *src;
        }

        fence(Ordering::SeqCst);

        // post validation
        if !self.mem.test_not_modify(addr, self.read_ver) {
            self.is_abort = true;
            return None;
        }

        Some(mem)
    }
}

pub struct STM {
    mem: UnsafeCell<Memory>,
}

unsafe impl Sync for STM {}

impl STM {
    pub fn new() -> STM {
        STM {
            mem: UnsafeCell::new(Memory::new()),
        }
    }

    pub fn write_transaction<F, R>(&self, f: F) -> R
    where
        F: Fn(&mut WriteTrans) -> TMResult<R>,
    {
        loop {
            let mut tr = WriteTrans::new(unsafe { &mut *self.mem.get() });
            match tr.transaction(&f) {
                TMResult::Retry => (),
                TMResult::Ok(val) => return val,
            }
        }
    }

    pub fn read_transaction<F, R>(&self, f: F) -> R
    where
        F: Fn(&mut ReadTrans) -> TMResult<R>,
    {
        loop {
            let mut tr = ReadTrans::new(unsafe { &*self.mem.get() });
            match f(&mut tr) {
                TMResult::Retry => (),
                TMResult::Ok(val) => {
                    if tr.is_abort == true {
                        continue;
                    } else {
                        return val;
                    }
                }
            }
        }
    }
}
