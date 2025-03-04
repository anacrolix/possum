fn main() {
    // We have a custom "loom" cfg that Rust warns about since 1.80.
    println!("cargo::rustc-check-cfg=cfg(loom)");
}
