# lloogg — event logging server

# ── Build ─────────────────────────────────────────────
# Release build (`just build` or `just build N=500`)
build N="100":
    mkdir -p data
    N={{ N }} cargo build --release

# Debug build
build-dev N="100":
    mkdir -p data
    N={{ N }} cargo build

# ── Run ───────────────────────────────────────────────
# Start server (release)
run N="100":
    N={{ N }} cargo run --release -- lloogg.toml .

# ── Test ──────────────────────────────────────────────
test:
    cargo test

# Unit tests only
test-unit:
    cargo test --lib

# Integration tests only
test-integration:
    cargo test --test integration

# ── Bench ─────────────────────────────────────────────
# Run benchmark against local server
bench:
    cargo run --bin bench --release -- 127.0.0.1:7379

# Run interactive demo TUI (requires running server)
demo HOST="127.0.0.1:7379":
    cargo run --bin demo --release -- {{HOST}}

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
