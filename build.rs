use std::env;

fn main() {
    // We have a custom "loom" cfg that Rust warns about since 1.80.
    println!("cargo::rustc-check-cfg=cfg(loom)");

    // Link Windows system libraries when building for Windows
    if cfg!(target_os = "windows") {
        println!("cargo:rustc-link-lib=kernel32");
        println!("cargo:rustc-link-lib=ws2_32");
        println!("cargo:rustc-link-lib=bcrypt");
        println!("cargo:rustc-link-lib=ole32");
        println!("cargo:rustc-link-lib=oleaut32");
        println!("cargo:rustc-link-lib=userenv");
        println!("cargo:rustc-link-lib=ntdll");
        println!("cargo:rustc-link-lib=synchronization");
    }

    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::generate(crate_dir)
        .expect("Unable to generate bindings")
        // I expect there's a better place to put this, then copy it into the Go
        // directory so it is packaged in the Go module.
        .write_to_file("go/cpossum/possum.h");
}
