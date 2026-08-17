//! sermuxd — serial port multiplexer daemon.
//!
//! Milestone 1: sole owner of the serial port; every byte read from the port
//! is broadcast verbatim to all connected raw-TCP clients, and bytes from any
//! client are written verbatim to the port (no TX arbitration yet — everyone
//! may write, like socat).

use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
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
}

/// Shared hub: serial RX fan-out (broadcast) and client TX fan-in (mpsc).
#[derive(Clone)]
struct Hub {
    /// Serial -> clients. Every client subscribes; `Bytes` clones are cheap
    /// refcount bumps.
    rx: broadcast::Sender<Bytes>,
    /// Clients -> serial. Single consumer is the serial TX task.
    tx: mpsc::Sender<Bytes>,
}

/// Own the serial port: fan reads out to the broadcast hub, fan hub writes
/// in to the port.
async fn run_serial(
    port: String,
    baud: u32,
    hub_rx: broadcast::Sender<Bytes>,
    mut hub_tx: mpsc::Receiver<Bytes>,
) -> Result<()> {
    let serial = tokio_serial::new(&port, baud)
        .open_native_async()
        .with_context(|| format!("failed to open serial port {port}"))?;
    info!(port, baud, "serial port open");
    let (mut reader, mut writer) = tokio::io::split(serial);

    // Serial -> broadcast.
    let mut rx_task = tokio::spawn(async move {
        let mut buf = vec![0u8; CHUNK_SIZE];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => anyhow::bail!("serial port returned EOF"),
                Ok(n) => {
                    // No receivers is normal (zero clients); not an error.
                    let _ = hub_rx.send(Bytes::copy_from_slice(&buf[..n]));
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

/// Handle one raw-TCP data client until it disconnects.
async fn handle_client(stream: TcpStream, hub: Hub) -> Result<()> {
    let peer = stream.peer_addr().context("peer_addr")?;
    stream.set_nodelay(true).ok();
    info!(%peer, "client connected");

    let (mut sock_reader, mut sock_writer) = stream.into_split();
    let mut rx = hub.rx.subscribe();
    let tx = hub.tx.clone();

    // Broadcast -> client socket.
    let writer_task = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(chunk) => {
                    if sock_writer.write_all(&chunk).await.is_err() {
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
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    let (bcast_tx, _) = broadcast::channel::<Bytes>(args.queue_chunks);
    let (serial_tx, serial_rx) = mpsc::channel::<Bytes>(TX_QUEUE_CHUNKS);
    let hub = Arc::new(Hub {
        rx: bcast_tx.clone(),
        tx: serial_tx,
    });

    // If the serial task ends (port error/unplug), the daemon has no purpose.
    let serial_task = tokio::spawn(run_serial(args.port.clone(), args.baud, bcast_tx, serial_rx));

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
