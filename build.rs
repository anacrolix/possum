use std::env;

fn main() {
    // We have a custom "loom" cfg that Rust warns about since 1.80.
    println!("cargo::rustc-check-cfg=cfg(loom)");

    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::generate(crate_dir)
        .expect("Unable to generate bindings")
        // I expect there's a better place to put this, then copy it into the Go
        // directory so it is packaged in the Go module.
        .write_to_file("go/cpossum/possum.h");
}
