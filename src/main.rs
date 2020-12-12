use std::sync::Arc;
use std::{thread, time};

mod tl2;

const NUM_PHILOSOPHERS: usize = 2;

fn philosopher(stm: Arc<tl2::STM>, n: usize) {
    let left = 8 * n;
    let right = 8 * ((n + 1) % NUM_PHILOSOPHERS);

    for _ in 0..1000000 {
        // pickup chopsticks
        while !stm.write_transaction(|tr| {
            let mut f1 = load!(tr, left);
            let mut f2 = load!(tr, right);
            if f1[0] == 0 && f2[0] == 0 {
                f1[0] = 1;
                f2[0] = 1;
                store!(tr, left, f1);
                store!(tr, right, f2);
                tl2::TMResult::Ok(true)
            } else {
                tl2::TMResult::Ok(false)
            }
        }) {
            println!("#{} failed to pickup", n);
            let ten_millis = time::Duration::from_micros(1);
            thread::sleep(ten_millis);
        }

        println!("#{} is eating", n);

        // release chopsticks
        stm.write_transaction(|tr| {
            let mut f1 = load!(tr, left);
            let mut f2 = load!(tr, right);
            f1[0] = 0;
            f2[0] = 0;
            store!(tr, left, f1);
            store!(tr, right, f2);
            tl2::TMResult::Ok(())
        });
    }
}

fn main() {
    let stm = Arc::new(tl2::STM::new());
    let mut v = Vec::new();

    for i in 0..NUM_PHILOSOPHERS {
        let s = stm.clone();
        let th = std::thread::spawn(move || philosopher(s, i));
        v.push(th);
    }

    for th in v {
        th.join().unwrap();
    }
}
