//! sermuxd — serial port multiplexer daemon.
//!
//! Milestone 1: sole owner of the serial port; every byte read from the port
//! is broadcast verbatim to all connected raw-TCP clients, and bytes from any
//! client are written verbatim to the port (no TX arbitration yet — everyone
//! may write, like socat).
//! Milestone 2: local PTY client — minicom attaches to a stable symlink and
//! behaves like any other client.
//! Replay buffer (pulled forward from milestone 4): a global rolling buffer
//! of recent serial output; every newly connected client gets a copy before
//! joining the live stream.

use std::collections::VecDeque;
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio_serial::SerialPortBuilderExt;
use tracing::{error, info, warn};

/// Size of read chunks moving through the hub.
const CHUNK_SIZE: usize = 4096;
/// Queue depth for client -> serial writes.
const TX_QUEUE_CHUNKS: usize = 64;

/// Serial port multiplexer daemon.
#[derive(Parser, Debug)]
#[command(name = "sermuxd", version, about, long_about = None)]
struct Args {
    /// Serial device to own exclusively.
    #[arg(long, default_value = "/dev/ttyUSB0")]
    port: String,

    /// Baud rate.
    #[arg(long, default_value_t = 115200)]
    baud: u32,

    /// TCP port for raw data clients.
    #[arg(long, default_value_t = 8000)]
    data_port: u16,

    /// Address to bind the data listener on.
    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    /// Per-client broadcast queue depth, in chunks (~4 KiB each).
    // Rationale for the drop policy: see PLAN.md, "Buffering".
    #[arg(long, default_value_t = 64)]
    queue_chunks: usize,

    /// Symlink path for the local PTY client (point minicom at it).
    #[arg(long, default_value = "/tmp/ttyMUX0")]
    pty_link: PathBuf,

    /// Rolling replay buffer size, in KiB, sent to each new client (0 disables).
    #[arg(long, default_value_t = 256)]
    replay_kb: usize,
}

/// One serial RX chunk, tagged with a sequence number so a client that took
/// a replay-buffer snapshot can skip live chunks it already has.
#[derive(Clone)]
struct Chunk {
    seq: u64,
    bytes: Bytes,
}

/// Global rolling replay buffer: a suffix of everything the serial port ever
/// emitted, capped in size. Snapshots are copies — reading never consumes.
struct ReplayBuffer {
    inner: Mutex<ReplayInner>,
}

struct ReplayInner {
    chunks: VecDeque<Chunk>,
    bytes: usize,
    cap: usize,
}

impl ReplayBuffer {
    fn new(cap: usize) -> Self {
        Self {
            inner: Mutex::new(ReplayInner {
                chunks: VecDeque::new(),
                bytes: 0,
                cap,
            }),
        }
    }

    fn push(&self, mut chunk: Chunk) {
        let mut inner = self.inner.lock().expect("replay mutex poisoned");
        if inner.cap == 0 {
            return;
        }
        // Roll over: drop oldest chunks until the new one fits.
        while !inner.chunks.is_empty() && inner.bytes + chunk.bytes.len() > inner.cap {
            let old = inner.chunks.pop_front().expect("non-empty");
            inner.bytes -= old.bytes.len();
        }
        // A single chunk larger than the cap keeps only its own tail, so the
        // buffer invariant (a stream suffix of at most `cap` bytes) holds.
        if chunk.bytes.len() > inner.cap {
            chunk.bytes = chunk.bytes.slice(chunk.bytes.len() - inner.cap..);
        }
        inner.bytes += chunk.bytes.len();
        inner.chunks.push_back(chunk);
    }

    /// Copy of the current buffer plus the highest sequence number in it, so
    /// the caller can skip live broadcast chunks already covered here.
    fn snapshot(&self) -> (u64, Vec<Bytes>) {
        let inner = self.inner.lock().expect("replay mutex poisoned");
        let max_seq = inner.chunks.back().map(|c| c.seq).unwrap_or(0);
        let chunks = inner.chunks.iter().map(|c| c.bytes.clone()).collect();
        (max_seq, chunks)
    }
}

/// Shared hub: serial RX fan-out (broadcast + replay) and client TX fan-in
/// (mpsc).
#[derive(Clone)]
struct Hub {
    /// Serial -> clients live stream. Every client subscribes; `Chunk` clones
    /// are cheap (`Bytes` is a refcount bump).
    rx: broadcast::Sender<Chunk>,
    /// Rolling replay buffer, snapshotted (copied) for each new client.
    replay: Arc<ReplayBuffer>,
    /// Clients -> serial. Single consumer is the serial TX task.
    tx: mpsc::Sender<Bytes>,
}

/// Own the serial port: fan reads out to the broadcast hub, fan hub writes
/// in to the port.
async fn run_serial(
    port: String,
    baud: u32,
    hub_rx: broadcast::Sender<Chunk>,
    replay: Arc<ReplayBuffer>,
    mut hub_tx: mpsc::Receiver<Bytes>,
) -> Result<()> {
    let serial = tokio_serial::new(&port, baud)
        .open_native_async()
        .with_context(|| format!("failed to open serial port {port}"))?;
    info!(port, baud, "serial port open");
    let (mut reader, mut writer) = tokio::io::split(serial);

    // Serial -> replay buffer + broadcast.
    let mut rx_task = tokio::spawn(async move {
        let mut buf = vec![0u8; CHUNK_SIZE];
        let mut seq: u64 = 0;
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => anyhow::bail!("serial port returned EOF"),
                Ok(n) => {
                    seq += 1;
                    let chunk = Chunk {
                        seq,
                        bytes: Bytes::copy_from_slice(&buf[..n]),
                    };
                    // Push before broadcasting: a client subscribing right now
                    // either misses the chunk twice (fine, it pre-dates its
                    // snapshot) or has it in both and dedups by seq.
                    replay.push(chunk.clone());
                    // No receivers is normal (zero clients); not an error.
                    let _ = hub_rx.send(chunk);
                }
                Err(e) => anyhow::bail!("serial read error: {e}"),
            }
        }
    });

    // Broadcast clients -> serial.
    let mut tx_task = tokio::spawn(async move {
        while let Some(chunk) = hub_tx.recv().await {
            writer
                .write_all(&chunk)
                .await
                .context("serial write error")?;
        }
        anyhow::bail!("serial TX channel closed")
    });

    // Whichever direction fails first kills the daemon: without the serial
    // port there is nothing to multiplex.
    tokio::select! {
        res = &mut rx_task => res.context("serial RX task panicked")?,
        res = &mut tx_task => res.context("serial TX task panicked")?,
    }
}

/// Remove the PTY symlink when the daemon (or the PTY task) shuts down.
struct LinkGuard(PathBuf);

impl Drop for LinkGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// The local PTY client endpoint (milestone 2).
struct Pty {
    /// Master side, non-blocking, ready for `AsyncFd`.
    master: std::fs::File,
    /// Kept open so the pts path (the symlink target) stays valid and master
    /// reads never see EIO when minicom closes the slave.
    _slave: OwnedFd,
    /// Kept so the symlink is removed when the PTY task ends.
    _link: LinkGuard,
}

/// Create the PTY pair, put the slave in raw mode, and publish the stable
/// symlink minicom will open.
fn setup_pty(link: &Path) -> Result<Pty> {
    use nix::fcntl::{FcntlArg, OFlag};
    use nix::sys::termios::{SetArg, cfmakeraw, tcgetattr, tcsetattr};

    let pty = nix::pty::openpty(None, None).context("openpty failed")?;

    // Raw mode on the slave: no line-discipline mangling (CRLF translation,
    // XON/XOFF, echo), so the stream stays byte-transparent even for clients
    // dumber than minicom. minicom reconfigures termios itself on open.
    let mut tio = tcgetattr(&pty.slave).context("tcgetattr on pty slave")?;
    cfmakeraw(&mut tio);
    tcsetattr(&pty.slave, SetArg::TCSANOW, &tio).context("set pty slave raw")?;

    let slave_path = std::fs::read_link(format!("/proc/self/fd/{}", pty.slave.as_raw_fd()))
        .context("resolve pts path")?;

    // Replace only stale symlinks; never delete someone's real file.
    if let Ok(md) = std::fs::symlink_metadata(link) {
        if md.file_type().is_symlink() {
            std::fs::remove_file(link)
                .with_context(|| format!("remove stale link {}", link.display()))?;
        } else {
            anyhow::bail!("{} exists and is not a symlink", link.display());
        }
    }
    std::os::unix::fs::symlink(&slave_path, link)
        .with_context(|| format!("create link {}", link.display()))?;
    info!(link = %link.display(), dev = %slave_path.display(), "PTY client up; point minicom at the link");

    // Non-blocking so the master can live on the tokio reactor via AsyncFd.
    // try_clone()d handles share the open file description, hence this flag.
    nix::fcntl::fcntl(&pty.master, FcntlArg::F_SETFL(OFlag::O_NONBLOCK))
        .context("set O_NONBLOCK on pty master")?;

    Ok(Pty {
        master: pty.master.into(),
        _slave: pty.slave,
        _link: LinkGuard(link.to_path_buf()),
    })
}

/// Write all of `bytes` to the PTY master, waiting for kernel buffer space
/// when the consumer (e.g. a paused minicom) is slow.
async fn pty_write_all(fd: &AsyncFd<std::fs::File>, mut bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    while !bytes.is_empty() {
        let mut guard = fd.writable().await?;
        match guard.try_io(|inner| inner.get_ref().write(bytes)) {
            Ok(Ok(n)) => bytes = &bytes[n..],
            Ok(Err(e)) => return Err(e),
            Err(_not_ready) => continue, // WouldBlock: readiness consumed, retry
        }
    }
    Ok(())
}

/// The PTY client: behaves exactly like a TCP client — receives the
/// broadcast, may write to the port (milestone 2: still no arbitration).
async fn run_pty(hub: Hub, pty: Pty) -> Result<()> {
    use std::io::Read;

    // The two halves share one open file description (dup), so O_NONBLOCK
    // applies to both. pty._slave / pty._link stay alive until we return.
    let reader_fd = AsyncFd::new(pty.master.try_clone().context("dup pty master")?)?;
    let writer_fd = AsyncFd::new(pty.master)?;

    // Subscribe before snapshotting so nothing is missed in between; chunks
    // present in both are deduplicated by sequence number.
    let mut rx = hub.rx.subscribe();
    let (replayed, snapshot) = hub.replay.snapshot();
    let tx = hub.tx.clone();

    // Replay snapshot, then broadcast -> PTY master.
    let writer_task = tokio::spawn(async move {
        for bytes in &snapshot {
            if pty_write_all(&writer_fd, bytes).await.is_err() {
                return;
            }
        }
        loop {
            let bytes = match rx.recv().await {
                Ok(chunk) => {
                    if chunk.seq <= replayed {
                        continue; // already in the snapshot
                    }
                    chunk.bytes
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(chunks = n, "slow PTY consumer dropped data");
                    Bytes::from(format!(
                        "\r\n*** sermuxd: dropped {n} chunk(s), slow reader ***\r\n"
                    ))
                }
                Err(broadcast::error::RecvError::Closed) => break,
            };
            if pty_write_all(&writer_fd, &bytes).await.is_err() {
                break;
            }
        }
    });

    // PTY master -> serial.
    let mut buf = vec![0u8; CHUNK_SIZE];
    let result: Result<()> = loop {
        let mut guard = match reader_fd.readable().await {
            Ok(guard) => guard,
            Err(e) => break Err(e).context("pty readable wait"),
        };
        match guard.try_io(|inner| inner.get_ref().read(&mut buf)) {
            Ok(Ok(0)) => break Err(anyhow::anyhow!("pty master EOF")),
            Ok(Ok(n)) => {
                if tx.send(Bytes::copy_from_slice(&buf[..n])).await.is_err() {
                    break Err(anyhow::anyhow!("serial TX channel closed"));
                }
            }
            Ok(Err(e)) => break Err(e).context("pty read"),
            Err(_not_ready) => continue, // WouldBlock: readiness consumed, retry
        }
    };

    writer_task.abort();
    result
}

/// Handle one raw-TCP data client until it disconnects.
async fn handle_client(stream: TcpStream, hub: Hub) -> Result<()> {
    let peer = stream.peer_addr().context("peer_addr")?;
    stream.set_nodelay(true).ok();
    info!(%peer, "client connected");

    let (mut sock_reader, mut sock_writer) = stream.into_split();
    // Subscribe before snapshotting so nothing is missed in between; chunks
    // present in both are deduplicated by sequence number.
    let mut rx = hub.rx.subscribe();
    let (replayed, snapshot) = hub.replay.snapshot();
    let tx = hub.tx.clone();

    // Replay snapshot, then broadcast -> client socket.
    let writer_task = tokio::spawn(async move {
        for bytes in &snapshot {
            if sock_writer.write_all(bytes).await.is_err() {
                return;
            }
        }
        loop {
            match rx.recv().await {
                Ok(chunk) => {
                    if chunk.seq <= replayed {
                        continue; // already in the snapshot
                    }
                    if sock_writer.write_all(&chunk.bytes).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(%peer, chunks = n, "slow client dropped data");
                    let notice =
                        format!("\r\n*** sermuxd: dropped {n} chunk(s), slow reader ***\r\n");
                    if sock_writer.write_all(notice.as_bytes()).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // Client socket -> serial (milestone 1: no arbitration, everyone may write).
    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        match sock_reader.read(&mut buf).await {
            Ok(0) => break, // clean disconnect
            Ok(n) => {
                if tx.send(Bytes::copy_from_slice(&buf[..n])).await.is_err() {
                    break; // serial side is gone
                }
            }
            Err(e) => {
                warn!(%peer, "client read error: {e}");
                break;
            }
        }
    }

    writer_task.abort();
    info!(%peer, "client disconnected");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    let (bcast_tx, _) = broadcast::channel::<Chunk>(args.queue_chunks);
    let (serial_tx, serial_rx) = mpsc::channel::<Bytes>(TX_QUEUE_CHUNKS);
    let replay = Arc::new(ReplayBuffer::new(args.replay_kb * 1024));
    let hub = Arc::new(Hub {
        rx: bcast_tx.clone(),
        replay: replay.clone(),
        tx: serial_tx,
    });

    // PTY client (milestone 2): local minicom attaches via the symlink.
    // Set up before the listeners so a bad --pty-link fails fast at startup.
    let pty = setup_pty(&args.pty_link)?;
    {
        let hub = hub.clone();
        tokio::spawn(async move {
            // A PTY failure mid-run must not take down the hub.
            if let Err(e) = run_pty(hub.as_ref().clone(), pty).await {
                error!("PTY client task failed: {e:#}");
            }
        });
    }

    // If the serial task ends (port error/unplug), the daemon has no purpose.
    let serial_task = tokio::spawn(run_serial(
        args.port.clone(),
        args.baud,
        bcast_tx,
        replay,
        serial_rx,
    ));

    let listener = TcpListener::bind((args.bind.as_str(), args.data_port))
        .await
        .with_context(|| format!("failed to bind {}:{}", args.bind, args.data_port))?;
    info!(bind = %args.bind, port = args.data_port, "data listener up");

    tokio::pin!(serial_task);
    loop {
        tokio::select! {
            res = &mut serial_task => {
                match res.context("serial task panicked")? {
                    Ok(()) => error!("serial task ended unexpectedly"),
                    Err(e) => error!("serial task failed: {e:#}"),
                }
                anyhow::bail!("serial port lost");
            }
            accept = listener.accept() => {
                let (stream, _) = accept.context("accept failed")?;
                let hub = hub.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, hub.as_ref().clone()).await {
                        warn!("client handler error: {e:#}");
                    }
                });
            }
        }
    }
}
