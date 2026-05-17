# lloogg — event logging server

# ── Build ─────────────────────────────────────────────
# Release build (`just build` or `just build LLOOGG_N=500`)
build LLOOGG_N="100":
    mkdir -p data
    LLOOGG_N={{ LLOOGG_N }} cargo build --release

# Debug build
build-dev LLOOGG_N="100":
    mkdir -p data
    LLOOGG_N={{ LLOOGG_N }} cargo build

# ── Run ───────────────────────────────────────────────
# Start server (release)
run: build
    RUST_LOG=info ./target/release/lloogg lloogg.toml

# Start server (debug)
run-dev: build-dev
    RUST_LOG=debug ./target/debug/lloogg lloogg.dev.toml

# ── Test ──────────────────────────────────────────────
# Unit tests
test:
    cargo test --lib

# Unit tests only
test-unit:
    cargo test --lib

# Integration tests only
test-integration:
    cargo test --test integration

# ── Lint / Check ──────────────────────────────────────
# Check compilation
check:
    cargo check

# Lint with clippy (deny warnings)
clippy:
    cargo clippy --all-targets

# ── Housekeeping ──────────────────────────────────────
# Clean build artifacts
clean:
    cargo clean
