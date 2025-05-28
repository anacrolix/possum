#[macro_export]
macro_rules! log_time {
    ($fmt:literal, $expr:expr) => {{
        let start = std::time::Instant::now();
        let ret = $expr;
        let duration = start.elapsed();
        tracing::debug!(target: "possum::timing", duration = ?duration, $fmt);
        ret
    }};
    ({ $($fields:tt)* }, $($exprs:expr),+; $($stmts:stmt);*) => {
        let start = std::time::Instant::now();
        $($stmts);+
        let duration = start.elapsed();
        tracing::debug!(target: "possum::timing", { duration = ?duration, $($fields)* }, $($exprs),+);
    };
    ($($args:expr),+; $($stmt:stmt);+) => {
        let start = std::time::Instant::now();
        $($stmt);+
        let duration = start.elapsed();
        tracing::debug!(
            target: "possum::timing",
            duration = ?duration,
            $($args),+
        );
    };
}
