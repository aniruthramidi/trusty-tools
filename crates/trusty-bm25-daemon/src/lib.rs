//! Library entry-point for the per-palace BM25 lexical-search daemon (issue #156).
//!
//! Why: this crate began life as a binary that `trusty-memory` spawns as a
//! subprocess. Hosts that want to embed the daemon in-process (integration
//! tests, future single-process deployments, or callers that already manage
//! their own Tokio runtime) need a stable `pub async fn run(...)` entry-point
//! that does not depend on the binary's CLI parsing. Exposing the modules
//! also lets tests reuse the dispatch helpers instead of re-implementing
//! them, keeping the wire contract honest.
//!
//! What: re-exports the internal modules (`batch_queue`, `index`, `protocol`,
//! `server`, `socket`), defines [`DaemonConfig`] holding every runtime
//! parameter the binary previously parsed from `clap`, and provides
//! [`run`] — an async function that performs the same startup sequence as
//! the binary (load index → spawn batch worker → bind UDS → accept loop →
//! SIGTERM/SIGINT shutdown → cleanup).
//!
//! Test: covered by the binary's integration test (`tests/bm25_daemon.rs`)
//! and the per-module unit tests under `src/`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::signal::unix::{signal, SignalKind};

pub mod batch_queue;
pub mod index;
pub mod protocol;
pub mod server;
pub mod socket;

use batch_queue::{BatchConfig, BatchQueue, DEFAULT_MAX_BATCH_SIZE, DEFAULT_WRITE_WINDOW_MS};
use index::PalaceBm25Index;

/// Runtime configuration for the BM25 daemon.
///
/// Why: the binary used to fold CLI parsing and startup logic together in
/// `main`. Splitting them apart means every embedder (binary, tests,
/// in-process hosts) constructs the same value type, so a future field
/// addition only changes one struct. The defaults mirror the CLI flag
/// defaults from `batch_queue` and `socket` so a caller that wants the
/// stock daemon needs only `palace` and `data_dir`.
/// What: a plain POD with the five knobs the daemon honours. `socket =
/// None` means "use `socket::default_socket_path(&palace)`".
/// `write_window_ms` and `max_batch_size` flow straight into a
/// [`BatchConfig`] inside [`run`].
/// Test: indirectly exercised by every `run` callsite — the integration
/// test in `tests/bm25_daemon.rs` and the binary itself.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    /// Palace name — used to derive the default socket path
    /// (`$TMPDIR/trusty-bm25-<palace>.sock`) and to identify this instance
    /// in log messages.
    pub palace: String,

    /// Directory where the BM25 snapshot (`bm25_index.json`) is stored.
    /// Created automatically if it does not exist.
    pub data_dir: PathBuf,

    /// Override the Unix domain socket path. `None` means use
    /// [`socket::default_socket_path`] on `palace`.
    pub socket: Option<PathBuf>,

    /// Write-coalescing window in milliseconds.
    pub write_window_ms: u64,

    /// Maximum number of write ops in one batch before forcing a flush.
    pub max_batch_size: usize,
}

impl DaemonConfig {
    /// Construct a config with the documented defaults for the batching
    /// knobs and an auto-derived socket path.
    ///
    /// Why: most callers (the binary, integration tests, embedders) want
    /// the stock daemon and should not have to repeat the defaults at
    /// every callsite. Centralising the defaults here keeps them in lock-
    /// step with `batch_queue::DEFAULT_*` constants.
    /// What: returns a `DaemonConfig` with `socket = None`,
    /// `write_window_ms = DEFAULT_WRITE_WINDOW_MS`, and
    /// `max_batch_size = DEFAULT_MAX_BATCH_SIZE`.
    /// Test: indirectly — the binary uses identical defaults from clap.
    pub fn new(palace: impl Into<String>, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            palace: palace.into(),
            data_dir: data_dir.into(),
            socket: None,
            write_window_ms: DEFAULT_WRITE_WINDOW_MS,
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
        }
    }
}

/// Run the BM25 daemon to completion (until SIGTERM / SIGINT or the accept
/// loop exits).
///
/// Why: the startup sequence — load snapshot, spawn worker, bind UDS, run
/// accept loop with signal-driven shutdown, clean up — is identical
/// whether the daemon is launched from `main` or embedded in another
/// host. Pulling it into a library function lets callers compose the
/// daemon with their own runtime and signal-handling strategy without
/// shelling out to a subprocess.
/// What: validates the data dir is writable by loading the snapshot,
/// spawns the batch-queue worker, ensures the socket's parent directory
/// exists, cleans up any stale socket file, binds the listener, then
/// races the accept loop against SIGTERM/SIGINT. Returns `Ok(())` on
/// clean shutdown. The socket file is removed on exit so the next
/// run does not see `EADDRINUSE`.
/// Test: end-to-end via the binary's integration test
/// (`tests/bm25_daemon.rs`) and the per-module unit tests for every
/// component touched.
pub async fn run(config: DaemonConfig) -> Result<()> {
    let DaemonConfig {
        palace,
        data_dir,
        socket: socket_override,
        write_window_ms,
        max_batch_size,
    } = config;

    let socket_path = socket_override.unwrap_or_else(|| socket::default_socket_path(&palace));

    let batch_config = BatchConfig {
        max_batch_size: max_batch_size.max(1),
        write_window: Duration::from_millis(write_window_ms),
    };

    tracing::info!(
        palace = %palace,
        data_dir = %data_dir.display(),
        socket = %socket_path.display(),
        max_batch_size = batch_config.max_batch_size,
        write_window_ms = batch_config.write_window.as_millis(),
        "trusty-bm25-daemon starting"
    );

    // Step 1: load (or create) the palace BM25 snapshot. This validates the
    // data-dir exists and is writable before we bind the socket.
    let palace_index = PalaceBm25Index::load_or_create(&data_dir)
        .with_context(|| format!("load BM25 palace index from {}", data_dir.display()))?;

    // Step 2: spawn the batch-queue worker. The worker takes ownership of
    // the index and is the sole writer for the rest of the daemon's lifetime.
    let queue = Arc::new(BatchQueue::new(palace_index, batch_config));

    // Step 3: ensure the socket's parent directory exists, then clean up any
    // leftover socket file from a prior crash.
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create socket directory {}", parent.display()))?;
    }
    socket::cleanup_stale_socket(&socket_path);

    // Step 4: bind the UDS listener.
    let listener = server::bind_listener(&socket_path)
        .with_context(|| format!("bind bm25 daemon socket at {}", socket_path.display()))?;
    tracing::info!(
        palace = %palace,
        socket = %socket_path.display(),
        "trusty-bm25-daemon ready"
    );

    // Step 5: run the accept loop alongside a signal-driven shutdown.
    let accept = tokio::spawn(server::run_accept_loop(listener, queue));

    let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;

    tokio::select! {
        _ = sigterm.recv() => {
            tracing::info!("received SIGTERM — shutting down");
        }
        _ = sigint.recv() => {
            tracing::info!("received SIGINT — shutting down");
        }
        _ = accept => {
            // The accept loop never returns in normal operation.
            tracing::warn!("accept loop exited unexpectedly");
        }
    }

    // Step 6: remove the socket file on clean exit so the next run does not
    // see EADDRINUSE.
    socket::cleanup_stale_socket(&socket_path);
    Ok(())
}
