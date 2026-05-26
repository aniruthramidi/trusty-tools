//! Per-palace BM25 lexical-search subprocess binary (issue #156).
//!
//! Why: trusty-memory's recall path lacked a lexical lane — only vector
//! similarity. For short, identifier-heavy queries ("cargo test",
//! "PalaceHandle") BM25 routinely wins; hybrid recall via Reciprocal Rank
//! Fusion needs both lanes. Running BM25 in-process blocks the hot path on
//! disk I/O and contends with redb/usearch locks; a subprocess per palace
//! gives each palace its own writer (the subprocess IS the lock) and mirrors
//! the `trusty-embed-daemon` architecture (PR #157).
//!
//! What: this binary is now a thin shell — it parses CLI flags with `clap`,
//! initialises tracing on stderr, builds a [`trusty_bm25_daemon::DaemonConfig`],
//! and hands control to [`trusty_bm25_daemon::run`]. All startup logic
//! (snapshot load, batch-queue worker, UDS bind, accept loop, signal-driven
//! shutdown) lives in the library half of this crate so embedders can reuse
//! it without spawning a subprocess.
//!
//! Test: per-module unit tests live in the library half (`src/{batch_queue,
//! index, protocol, server, socket}.rs`); end-to-end coverage in
//! `tests/bm25_daemon.rs`.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

use trusty_bm25_daemon::batch_queue::{DEFAULT_MAX_BATCH_SIZE, DEFAULT_WRITE_WINDOW_MS};
use trusty_bm25_daemon::{run, DaemonConfig};

/// CLI flags for the BM25 daemon.
///
/// Why: operators (and parent processes like trusty-memory's subprocess
/// spawner) configure the daemon by passing flags. Keeping the surface small
/// matches the daemon's single responsibility. The struct exists solely to
/// translate CLI args into a [`DaemonConfig`] — the library entry-point is
/// the canonical configuration shape.
/// What: palace name (determines the default socket path), data directory
/// (where the snapshot lives), optional socket override, batch-tuning knobs,
/// and verbosity. All have documented defaults from the batch_queue / socket
/// constants.
/// Test: covered indirectly by the integration test which constructs custom
/// palace / data-dir / socket arguments via the library entry-point.
#[derive(Debug, Parser)]
#[command(
    name = "trusty-bm25-daemon",
    version,
    about = "Per-palace BM25 lexical-index subprocess for the trusty-* ecosystem"
)]
struct Cli {
    /// Palace name — used to derive the default socket path
    /// (`$TMPDIR/trusty-bm25-<palace>.sock`) and to identify this instance
    /// in log messages.
    #[arg(long)]
    palace: String,

    /// Directory where the BM25 snapshot (`bm25_index.json`) is stored.
    /// Created automatically if it does not exist.
    #[arg(long)]
    data_dir: PathBuf,

    /// Override the Unix domain socket path. Defaults to
    /// `$TMPDIR/trusty-bm25-<palace>.sock`.
    #[arg(long, env = "TRUSTY_BM25_SOCKET")]
    socket: Option<PathBuf>,

    /// Write-coalescing window in milliseconds.
    #[arg(long, default_value_t = DEFAULT_WRITE_WINDOW_MS, env = "TRUSTY_BM25_WRITE_WINDOW_MS")]
    write_window_ms: u64,

    /// Maximum number of write ops in one batch before forcing a flush.
    #[arg(long, default_value_t = DEFAULT_MAX_BATCH_SIZE, env = "TRUSTY_BM25_MAX_BATCH_SIZE")]
    max_batch_size: usize,

    /// Increase verbosity (-v info, -vv debug, -vvv trace).
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    trusty_common::init_tracing(cli.verbose);

    let config = DaemonConfig {
        palace: cli.palace,
        data_dir: cli.data_dir,
        socket: cli.socket,
        write_window_ms: cli.write_window_ms,
        max_batch_size: cli.max_batch_size,
    };

    run(config).await
}
