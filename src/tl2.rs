use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ptr::copy_nonoverlapping;
use std::sync::atomic::AtomicU64;

const STRIPE_SIZE: usize = 128;
const MEM_SIZE: usize = 1024 * 1024;

pub struct Memory {
    mem: *mut u8,
    lock_ver: Vec<AtomicU64>,
}

thread_local!(
    static READ_VERSION: RefCell<usize> = RefCell::new(0);
    static READ_SET: RefCell<HashSet<usize>> = RefCell::new(HashSet::new());
    static WRITE_SET: RefCell<HashMap<usize, [u8; STRIPE_SIZE]>> = RefCell::new(HashMap::new());
);

impl Memory {
    // data load for write transaction
    pub fn wtr_load(&mut self, addr: usize) -> [u8; STRIPE_SIZE] {
        let mut mem = [0; STRIPE_SIZE];
        if let Some(m) = WRITE_SET.with(move |s| {
            if let Some(m) = s.borrow_mut().get(&addr) {
                unsafe { copy_nonoverlapping(m.as_ptr(), mem.as_mut_ptr(), STRIPE_SIZE) };
            }
            Some(mem)
        }) {
            return m;
        }

        let mut mem = [0; STRIPE_SIZE];
        let src = (self.mem as usize + addr) as *const u8;
        unsafe { copy_nonoverlapping(src, mem.as_mut_ptr(), STRIPE_SIZE) };
        mem
    }

    // data store for write transaction
    pub fn wtr_store(&mut self, dst: usize, src: [u8; STRIPE_SIZE]) {
        WRITE_SET.with(|s| {
            s.borrow_mut().insert(dst, src);
        });
    }
}
