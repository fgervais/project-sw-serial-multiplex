//! sermuxd — serial port multiplexer daemon.
//!
//! Milestone 1: sole owner of the serial port; every byte read from the port
//! is broadcast verbatim to all connected clients.
//! Milestone 2: local PTY client — minicom attaches to a stable symlink and
//! behaves like any other client.
//! Milestone 3: TX arbitration — a token handed out by `CLAIM` on the
//! control port must be presented on the data port (`TOKEN <hex>\n` as the
//! first bytes); only the bound owner may write to the serial port. The PTY
//! client is the default owner; `RELEASE`/auto-release fall back to it.
//! Replay buffer (pulled forward from milestone 4): a global rolling buffer
//! of recent serial output; every newly connected client gets a copy before
//! joining the live stream.

use std::collections::{HashMap, HashSet, VecDeque};
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio_serial::SerialPortBuilderExt;
use tracing::{error, info, warn};

/// Size of read chunks moving through the hub.
const CHUNK_SIZE: usize = 4096;
/// Queue depth for client -> serial writes.
const TX_QUEUE_CHUNKS: usize = 64;
/// Reject-notice queue depth per client (dropped when full).
const NOTICE_QUEUE: usize = 8;
/// Marker token for the default-owner entry (the PTY client never claims).
const DEFAULT_TOKEN: u64 = 0;

type ClientId = u64;

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

    /// TCP port for the line-based control protocol (STATUS/CLAIM/RELEASE).
    #[arg(long, default_value_t = 8001)]
    control_port: u16,

    /// Address to bind the listeners on.
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

    /// Inject a one-line ASCII notice into a client's stream when its TX
    /// bytes are dropped because it does not hold the TX lock.
    #[arg(long)]
    reject_notice: bool,
}

/// One serial RX chunk, tagged with a sequence number so a client that took
/// a replay-buffer snapshot can skip live chunks it already has.
#[derive(Clone)]
struct Chunk {
    seq: u64,
    bytes: Bytes,
}

/// A client write, tagged with the sender's identity so the serial TX task
/// can arbitrate.
struct TxMsg {
    client: ClientId,
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

/// Client registry + TX-lock arbiter. All methods lock the mutex briefly and
/// never hold it across an `.await`.
struct State {
    reject_notice: bool,
    inner: Mutex<StateInner>,
}

struct ClientInfo {
    name: String,
    notice: Option<mpsc::Sender<Bytes>>,
}

struct StateInner {
    next_id: ClientId,
    clients: HashMap<ClientId, ClientInfo>,
    /// Current TX owner and the token it bound with; token `DEFAULT_TOKEN`
    /// marks the synthetic default-owner entry (the PTY client).
    owner: Option<(ClientId, u64)>,
    /// Tokens handed out by CLAIM but not yet bound to a data client.
    pending: HashSet<u64>,
    /// The default owner (the PTY client); RELEASE/auto-release fall back to
    /// it. If the PTY is gone, the lock simply stays free.
    default: Option<ClientId>,
}

impl State {
    fn new(reject_notice: bool) -> Self {
        Self {
            reject_notice,
            inner: Mutex::new(StateInner {
                next_id: 0,
                clients: HashMap::new(),
                owner: None,
                pending: HashSet::new(),
                default: None,
            }),
        }
    }

    fn register(&self, name: String, notice: Option<mpsc::Sender<Bytes>>) -> ClientId {
        let mut g = self.inner.lock().expect("state mutex poisoned");
        let id = g.next_id;
        g.next_id += 1;
        g.clients.insert(id, ClientInfo { name, notice });
        id
    }

    /// Mark a client (the PTY) as the fallback owner. If nothing owns TX
    /// right now, it becomes the owner immediately — without stealing a
    /// bound client's lock.
    fn set_default(&self, id: ClientId) {
        let mut g = self.inner.lock().expect("state mutex poisoned");
        g.default = Some(id);
        if g.owner.is_none() {
            g.owner = Some((id, DEFAULT_TOKEN));
        }
    }

    /// Remove a client; if it owned TX, ownership falls back to the default
    /// (auto-release on owner disconnect, so a crashed client can never
    /// wedge the port).
    fn unregister(&self, id: ClientId) {
        let mut g = self.inner.lock().expect("state mutex poisoned");
        g.clients.remove(&id);
        if let Some((owner_id, _)) = g.owner
            && owner_id == id
        {
            g.owner = fallback_owner(&g);
        }
        if g.default == Some(id) {
            g.default = None;
        }
    }

    /// CLAIM: hand out a token that a data client can bind with. Stealing
    /// from the *default* owner (PTY) does not need --force; stealing from a
    /// bound client does. Until the token binds, the previous owner's
    /// replacement is the default fallback, so minicom keeps working between
    /// CLAIM and TOKEN.
    fn claim(&self, force: bool) -> std::result::Result<u64, String> {
        let mut g = self.inner.lock().expect("state mutex poisoned");
        if let Some((owner_id, tok)) = g.owner {
            let stealable_default = tok == DEFAULT_TOKEN;
            if !force && !stealable_default {
                let name = g
                    .clients
                    .get(&owner_id)
                    .map(|c| c.name.clone())
                    .unwrap_or_else(|| "<gone>".into());
                return Err(name);
            }
            g.owner = fallback_owner(&g);
        }
        // Random, unique, nonzero token (zero marks the default owner).
        let tok = loop {
            let t: u64 = rand::random();
            if t != DEFAULT_TOKEN && !g.pending.contains(&t) {
                break t;
            }
        };
        g.pending.insert(tok);
        Ok(tok)
    }

    /// RELEASE: succeed if any of the caller's tokens is the bound owner's
    /// token (fall back to the default owner) or is still pending.
    fn release(&self, mine: &[u64]) -> bool {
        let mut g = self.inner.lock().expect("state mutex poisoned");
        if let Some((_, tok)) = g.owner
            && tok != DEFAULT_TOKEN
            && mine.contains(&tok)
        {
            g.owner = fallback_owner(&g);
            return true;
        }
        for tok in mine {
            if g.pending.remove(tok) {
                return true;
            }
        }
        false
    }

    /// Drop unbound tokens (control connection cleanup). If the claim died
    /// before anyone bound it, the default owner takes over again.
    fn discard_pending(&self, mine: &[u64]) {
        let mut g = self.inner.lock().expect("state mutex poisoned");
        for tok in mine {
            g.pending.remove(tok);
        }
        if g.owner.is_none() {
            g.owner = fallback_owner(&g);
        }
    }

    /// Bind a token handed out by CLAIM to this data client, making it the
    /// TX owner.
    fn bind(&self, id: ClientId, token: u64) -> bool {
        let mut g = self.inner.lock().expect("state mutex poisoned");
        if g.pending.remove(&token) {
            g.owner = Some((id, token));
            true
        } else {
            false
        }
    }

    fn owns(&self, id: ClientId) -> bool {
        let g = self.inner.lock().expect("state mutex poisoned");
        g.owner.map(|(owner_id, _)| owner_id == id).unwrap_or(false)
    }

    /// Best-effort reject notice; dropped when the client's notice queue is
    /// full. Uses try_send so the mutex never blocks on an async send.
    fn notify_reject(&self, id: ClientId, msg: Bytes) {
        let g = self.inner.lock().expect("state mutex poisoned");
        if let Some(client) = g.clients.get(&id)
            && let Some(sender) = &client.notice
        {
            let _ = sender.try_send(msg);
        }
    }

    fn status_line(&self) -> String {
        let g = self.inner.lock().expect("state mutex poisoned");
        let mut names: Vec<&str> = g.clients.values().map(|c| c.name.as_str()).collect();
        names.sort_unstable();
        let owner_name = g
            .owner
            .and_then(|(id, _)| g.clients.get(&id).map(|c| c.name.as_str()))
            .unwrap_or("none");
        format!(
            "clients: {} ({})  tx-owner: {}",
            names.len(),
            names.join(", "),
            owner_name
        )
    }
}

/// Owner fallback after a release: the default client, if still registered.
fn fallback_owner(g: &StateInner) -> Option<(ClientId, u64)> {
    g.default
        .filter(|d| g.clients.contains_key(d))
        .map(|d| (d, DEFAULT_TOKEN))
}

/// Shared hub: serial RX fan-out (broadcast + replay), client identity and
/// TX arbitration (state), client TX fan-in (mpsc).
#[derive(Clone)]
struct Hub {
    /// Serial -> clients live stream. Every client subscribes; `Chunk` clones
    /// are cheap (`Bytes` is a refcount bump).
    rx: broadcast::Sender<Chunk>,
    /// Rolling replay buffer, snapshotted (copied) for each new client.
    replay: Arc<ReplayBuffer>,
    /// Clients -> serial. Single consumer is the serial TX task, which drops
    /// writes from non-owners.
    tx: mpsc::Sender<TxMsg>,
    /// Client registry + TX-lock arbiter.
    state: Arc<State>,
}

/// Owning TX task: writes only from the TX owner reach the port.
async fn run_serial(
    hub: Hub,
    mut hub_tx: mpsc::Receiver<TxMsg>,
    port: String,
    baud: u32,
) -> Result<()> {
    let serial = tokio_serial::new(&port, baud)
        .open_native_async()
        .with_context(|| format!("failed to open serial port {port}"))?;
    info!(port, baud, "serial port open");
    let (mut reader, mut writer) = tokio::io::split(serial);

    let hub_rx = hub.rx.clone();
    let replay = hub.replay.clone();
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

    // Clients -> serial, gated by the arbiter.
    let state = hub.state.clone();
    let reject_notice = state.reject_notice;
    let mut tx_task = tokio::spawn(async move {
        while let Some(msg) = hub_tx.recv().await {
            if state.owns(msg.client) {
                writer
                    .write_all(&msg.bytes)
                    .await
                    .context("serial write error")?;
            } else if reject_notice {
                state.notify_reject(
                    msg.client,
                    Bytes::from("\r\n*** sermuxd: TX denied (not owner) ***\r\n"),
                );
            }
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

/// The PTY client: like a TCP client, but registered as "pty" and installed
/// as the default TX owner.
async fn run_pty(hub: Hub, pty: Pty) -> Result<()> {
    use std::io::Read;

    let (notice_tx, mut notice_rx) = mpsc::channel::<Bytes>(NOTICE_QUEUE);
    let notice = if hub.state.reject_notice {
        Some(notice_tx)
    } else {
        None
    };
    let id = hub.state.register("pty".to_string(), notice);
    hub.state.set_default(id);

    // The two halves share one open file description (dup), so O_NONBLOCK
    // applies to both. pty._slave / pty._link stay alive until we return.
    let reader_fd = AsyncFd::new(pty.master.try_clone().context("dup pty master")?)?;
    let writer_fd = AsyncFd::new(pty.master)?;

    // Subscribe before snapshotting so nothing is missed in between; chunks
    // present in both are deduplicated by sequence number.
    let mut rx = hub.rx.subscribe();
    let (replayed, snapshot) = hub.replay.snapshot();
    let tx = hub.tx.clone();

    // Replay snapshot, then broadcast + reject notices -> PTY master.
    let writer_task = tokio::spawn(async move {
        for bytes in &snapshot {
            if pty_write_all(&writer_fd, bytes).await.is_err() {
                return;
            }
        }
        loop {
            tokio::select! {
                item = rx.recv() => {
                    let bytes = match item {
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
                notice = notice_rx.recv() => {
                    let Some(bytes) = notice else { break };
                    if pty_write_all(&writer_fd, &bytes).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // PTY master -> serial, tagged with this client's identity.
    let mut buf = vec![0u8; CHUNK_SIZE];
    let result: Result<()> = loop {
        let mut guard = match reader_fd.readable().await {
            Ok(guard) => guard,
            Err(e) => break Err(e).context("pty readable wait"),
        };
        match guard.try_io(|inner| inner.get_ref().read(&mut buf)) {
            Ok(Ok(0)) => break Err(anyhow::anyhow!("pty master EOF")),
            Ok(Ok(n)) => {
                let msg = TxMsg {
                    client: id,
                    bytes: Bytes::copy_from_slice(&buf[..n]),
                };
                if tx.send(msg).await.is_err() {
                    break Err(anyhow::anyhow!("serial TX channel closed"));
                }
            }
            Ok(Err(e)) => break Err(e).context("pty read"),
            Err(_not_ready) => continue, // WouldBlock: readiness consumed, retry
        }
    };

    writer_task.abort();
    hub.state.unregister(id);
    result
}

/// If a chunk starts with a `TOKEN <hex>\n` handshake, try to bind it
/// (staking the client's TX claim) and return the remainder; otherwise
/// return the chunk unchanged. Checked on every read: a client may claim at
/// any point in its session, and a chunk is only ever consumed as a
/// handshake when it begins exactly with the documented `TOKEN ` prefix.
fn strip_token_preamble<'a>(chunk: &'a [u8], hub: &Hub, id: ClientId) -> &'a [u8] {
    const PREFIX: &[u8] = b"TOKEN ";
    if !chunk.starts_with(PREFIX) {
        return chunk;
    }
    let Some(pos) = chunk.iter().position(|&b| b == b'\n') else {
        return chunk; // prefix but no full line yet: treat as raw data
    };
    let tok_text = std::str::from_utf8(&chunk[PREFIX.len()..pos])
        .unwrap_or("")
        .trim();
    match u64::from_str_radix(tok_text, 16) {
        Ok(tok) if hub.state.bind(id, tok) => {
            info!(token = %tok_text, "client bound TX token, holds the lock");
        }
        _ => {
            warn!(token = %tok_text, "client sent an unknown token, dropped the preamble");
        }
    }
    &chunk[pos + 1..]
}

/// Handle one raw-TCP data client until it disconnects.
async fn handle_client(stream: TcpStream, hub: Hub) -> Result<()> {
    let peer = stream.peer_addr().context("peer_addr")?;
    stream.set_nodelay(true).ok();
    info!(%peer, "client connected");

    let (notice_tx, mut notice_rx) = mpsc::channel::<Bytes>(NOTICE_QUEUE);
    let notice = if hub.state.reject_notice {
        Some(notice_tx)
    } else {
        None
    };
    let id = hub.state.register(format!("tcp:{peer}"), notice);

    let (mut sock_reader, mut sock_writer) = stream.into_split();
    // Subscribe before snapshotting so nothing is missed in between; chunks
    // present in both are deduplicated by sequence number.
    let mut rx = hub.rx.subscribe();
    let (replayed, snapshot) = hub.replay.snapshot();
    let tx = hub.tx.clone();

    // Replay snapshot, then broadcast + reject notices -> client socket.
    let writer_task = tokio::spawn(async move {
        for bytes in &snapshot {
            if sock_writer.write_all(bytes).await.is_err() {
                return;
            }
        }
        loop {
            tokio::select! {
                item = rx.recv() => {
                    let bytes = match item {
                        Ok(chunk) => {
                            if chunk.seq <= replayed {
                                continue; // already in the snapshot
                            }
                            chunk.bytes
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!(%peer, chunks = n, "slow client dropped data");
                            Bytes::from(format!(
                                "\r\n*** sermuxd: dropped {n} chunk(s), slow reader ***\r\n"
                            ))
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    };
                    if sock_writer.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
                notice = notice_rx.recv() => {
                    let Some(bytes) = notice else { break };
                    if sock_writer.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // Client socket -> serial, tagged with this client's identity. A chunk
    // that starts with the TOKEN handshake is consumed as arbitration
    // protocol; everything else passes through raw.
    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        match sock_reader.read(&mut buf).await {
            Ok(0) => break, // clean disconnect
            Ok(n) => {
                let chunk = strip_token_preamble(&buf[..n], &hub, id);
                if chunk.is_empty() {
                    continue;
                }
                let msg = TxMsg {
                    client: id,
                    bytes: Bytes::copy_from_slice(chunk),
                };
                if tx.send(msg).await.is_err() {
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
    hub.state.unregister(id);
    info!(%peer, "client disconnected");
    Ok(())
}

/// One control command, one response line starting with `OK` or `ERR`.
/// Commands are case-insensitive (flags and hex tokens compare as-is).
fn control_command(hub: &Hub, line: &str, my_tokens: &mut Vec<u64>) -> String {
    const USAGE: &str = "ERR usage: STATUS | CLAIM [--force] | RELEASE [token] | HELP";
    let parts: Vec<&str> = line.split_whitespace().collect();
    let Some(verb) = parts.first() else {
        return format!("{USAGE}\n");
    };
    let args = &parts[1..];
    if verb.eq_ignore_ascii_case("STATUS") && args.is_empty() {
        return format!("OK {}\n", hub.state.status_line());
    }
    if verb.eq_ignore_ascii_case("HELP") && args.is_empty() {
        return format!("{USAGE}\n");
    }
    if verb.eq_ignore_ascii_case("CLAIM") {
        let force = match args {
            [] => false,
            [flag] if flag.eq_ignore_ascii_case("--force") => true,
            _ => return format!("{USAGE}\n"),
        };
        return match hub.state.claim(force) {
            Ok(tok) => {
                my_tokens.push(tok);
                format!("OK token: {tok:016x}\n")
            }
            Err(owner) => format!("ERR tx held by {owner}, use CLAIM --force\n"),
        };
    }
    if verb.eq_ignore_ascii_case("RELEASE") {
        return match args {
            // Bare RELEASE acts on every token this connection claimed.
            [] if hub.state.release(my_tokens) => {
                my_tokens.clear();
                "OK released\n".to_string()
            }
            [] => "ERR not owner\n".to_string(),
            [tok_text] => match u64::from_str_radix(tok_text, 16) {
                Ok(tok) if hub.state.release(&[tok]) => {
                    my_tokens.retain(|t| *t != tok);
                    "OK released\n".to_string()
                }
                _ => "ERR not owner\n".to_string(),
            },
            _ => format!("{USAGE}\n"),
        };
    }
    format!("{USAGE}\n")
}

/// Handle one control client: a line-based protocol that never touches the
/// serial data path.
async fn handle_control(stream: TcpStream, hub: Hub) -> Result<()> {
    let peer = stream.peer_addr().context("peer_addr")?;
    stream.set_nodelay(true).ok();
    info!(%peer, "control client connected");

    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader);
    let mut my_tokens: Vec<u64> = Vec::new();
    let mut line = String::new();
    loop {
        line.clear();
        match lines.read_line(&mut line).await {
            Ok(0) => break, // clean disconnect
            Ok(_) => {
                let response = control_command(&hub, &line, &mut my_tokens);
                if writer.write_all(response.as_bytes()).await.is_err() {
                    break;
                }
            }
            Err(e) => {
                warn!(%peer, "control read error: {e}");
                break;
            }
        }
    }

    // The claim dies with the control connection only if it was never bound
    // to a data client: an unbound pending token would leak otherwise.
    hub.state.discard_pending(&my_tokens);
    info!(%peer, "control client disconnected");
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
    let (serial_tx, serial_rx) = mpsc::channel::<TxMsg>(TX_QUEUE_CHUNKS);
    let replay = Arc::new(ReplayBuffer::new(args.replay_kb * 1024));
    let state = Arc::new(State::new(args.reject_notice));
    let hub = Arc::new(Hub {
        rx: bcast_tx.clone(),
        replay: replay.clone(),
        tx: serial_tx,
        state: state.clone(),
    });

    // PTY client (milestone 2): local minicom attaches via the symlink.
    // Set up before the listeners so a bad --pty-link fails fast at startup.
    // The PTY registers as the default TX owner (milestone 3).
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
        hub.as_ref().clone(),
        serial_rx,
        args.port.clone(),
        args.baud,
    ));

    let data_listener = TcpListener::bind((args.bind.as_str(), args.data_port))
        .await
        .with_context(|| format!("failed to bind {}:{}", args.bind, args.data_port))?;
    info!(bind = %args.bind, port = args.data_port, "data listener up");

    let control_listener = TcpListener::bind((args.bind.as_str(), args.control_port))
        .await
        .with_context(|| format!("failed to bind {}:{}", args.bind, args.control_port))?;
    info!(bind = %args.bind, port = args.control_port, "control listener up");

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
            accept = data_listener.accept() => {
                let (stream, _) = accept.context("accept failed")?;
                let hub = hub.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, hub.as_ref().clone()).await {
                        warn!("client handler error: {e:#}");
                    }
                });
            }
            accept = control_listener.accept() => {
                let (stream, _) = accept.context("accept failed")?;
                let hub = hub.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_control(stream, hub.as_ref().clone()).await {
                        warn!("control handler error: {e:#}");
                    }
                });
            }
        }
    }
}
