mod upstream;
pub use upstream::*;

/// Engines to run, from the comma separated `BENCH_ENGINES` environment variable, or all of them.
pub fn engine_enabled(name: &str) -> bool {
    match std::env::var("BENCH_ENGINES") {
        Ok(engines) => engines.split(',').any(|engine| engine.trim() == name),
        Err(_) => true,
    }
}
