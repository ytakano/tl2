mod tl2;

fn main() {
    let mut mem = tl2::Memory::new();
    while !mem.write_transaction(|t| {
        let mut f1 = t.load(0);
        let mut f2 = t.load(8);
        if f1[0] == 0 && f2[0] == 0 {
            f1[0] = 1;
            f2[0] = 1;
            t.store(0, f1);
            t.store(8, f2);
            true
        } else {
            false
        }
    }) {}

    println!("Hello, world!");
}
