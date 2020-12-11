use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicU64;

const STRIPE_SIZE: usize = 8; // u64, 8B
const MEM_SIZE: usize = 128;

pub struct Memory {
    mem: Vec<u8>,
    lock_ver: Vec<AtomicU64>, // write-locks
    global_clock: AtomicU64,
    shift_size: usize,
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

    pub fn write_transaction<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut WriteTrans) -> R,
    {
        loop {
            let mut trans = WriteTrans::new(self);
            let result = f(&mut trans);
            if !trans.is_abort {
                return result;
            }
        }
    }

    fn test_not_modify(&mut self, addr: usize, rv: u64) -> bool {
        let n = *self.lock_ver[addr >> self.shift_size].get_mut();
        n & (1 << 63) == 0 && n <= rv
    }
}

pub struct WriteTrans<'a> {
    read_ver: u64,
    read_set: HashSet<usize>,
    write_set: HashMap<usize, [u8; STRIPE_SIZE]>,
    is_abort: bool,
    mem: &'a mut Memory,
}

impl<'a> WriteTrans<'a> {
    fn new(mem: &mut Memory) -> WriteTrans {
        WriteTrans {
            read_ver: *mem.global_clock.get_mut(),
            read_set: HashSet::new(),
            write_set: HashMap::new(),
            is_abort: false,
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

        // read from memory
        let mut mem = [0; STRIPE_SIZE];
        for (dst, src) in mem
            .iter_mut()
            .zip(self.mem.mem[addr..addr + STRIPE_SIZE].iter())
        {
            *dst = *src;
        }

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
}
