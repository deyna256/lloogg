fn main() {
    let n: usize = std::env::var("LLOOGG_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    assert!(n > 0 && n <= 3854, "LLOOGG_N must be in [1, 3854]");

    let out = std::env::var("OUT_DIR").unwrap();
    std::fs::write(
        format!("{out}/generated_constants.rs"),
        format!("pub const COMPILED_N: usize = {n};\n"),
    )
    .unwrap();

    println!("cargo:rerun-if-env-changed=LLOOGG_N");
}
