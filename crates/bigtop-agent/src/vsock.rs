//! vsock log streaming: guests dial the host and push length-prefixed
//! log frames, which the agent forwards into its normal log pipeline.
//!
//! Transport reality check (see Firecracker's `docs/vsock.md`): there is
//! **no `AF_VSOCK` on the host side**. The agent configures the guest's
//! virtio-vsock device with `PUT /vsock` (`guest_cid` + `uds_path`), and
//! Firecracker bridges guest `AF_VSOCK` connections to `(CID 2, port)`
//! into the agent's `AF_UNIX` listener at `<uds_path>_<port>`. Only the
//! *guest* ever touches `AF_VSOCK`.
//!
//! Wire protocol (see `SPEC.md` for the guest contract):
//!
//! ```text
//! frame  := u32 BE length (stream byte + payload) | u8 stream | payload bytes
//! stream := 0 handshake (payload: UTF-8 task id, first frame on a connection)
//!         | 1 stdout    (payload: one log line)
//!         | 2 stderr    (payload: one log line)
//!         | 3 complete  (payload: empty; guest finished, host closes)
//! ```
//!
//! Real `AF_VSOCK` needs a KVM-capable host for the *guest* side, which CI
//! and this sandbox do not have. The frame codec and the accept/serve path are transport
//! agnostic, so every test below runs them over `tokio::io::duplex`
//! loopbacks and real `AF_UNIX` sockets — the exact sockets the agent
//! binds in production; only the guest side of the bridge is
//! vsock-specific.

use bigtop_core::TaskId;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{mpsc, oneshot, Mutex};

/// Host port guests dial for log streaming; the agent listens on
/// `<uds_path>_<VSOCK_LOG_PORT>`.
pub const VSOCK_LOG_PORT: u32 = 4668;
/// Guest-side destination CID: `VMADDR_CID_HOST`.
pub const VSOCK_HOST_CID: u32 = 2;

/// Largest accepted frame payload (1 MiB). Length prefixes beyond
/// `MAX_FRAME_PAYLOAD + 1` are rejected before any allocation.
pub const MAX_FRAME_PAYLOAD: usize = 1024 * 1024;

/// Log stream selector: the first payload byte of every frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LogStream {
    /// First frame on a connection; payload is the UTF-8 task id.
    Handshake = 0,
    /// Guest stdout line.
    Stdout = 1,
    /// Guest stderr line.
    Stderr = 2,
    /// Guest finished its command; the host may snapshot, then closes.
    Complete = 3,
}

impl LogStream {
    /// Decode a selector byte.
    const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Handshake),
            1 => Some(Self::Stdout),
            2 => Some(Self::Stderr),
            3 => Some(Self::Complete),
            _ => None,
        }
    }
}

/// One length-prefixed log frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFrame {
    /// Which stream this frame belongs to.
    pub stream: LogStream,
    /// Frame payload bytes.
    pub payload: Vec<u8>,
}

impl LogFrame {
    /// Serialize into the exact wire bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + self.payload.len());
        let total = self.payload.len().saturating_add(1);
        // Saturate: a payload past u32::MAX cannot exist in practice, and the
        // decoder rejects anything past MAX_FRAME_PAYLOAD anyway.
        let len: u32 = u32::try_from(total).unwrap_or(u32::MAX);
        out.extend_from_slice(&len.to_be_bytes());
        out.push(self.stream as u8);
        out.extend_from_slice(&self.payload);
        out
    }

    /// Decode one frame. Returns `Ok(None)` on clean EOF at a frame
    /// boundary, `Err` on truncation, oversize, or a bad stream selector.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the peer closes mid-frame, sends a length
    /// prefix outside `1..=MAX_FRAME_PAYLOAD + 1`, or uses an unknown
    /// stream selector.
    pub async fn decode<R>(reader: &mut R) -> io::Result<Option<Self>>
    where
        R: AsyncRead + Unpin,
    {
        let mut header = [0u8; 5];
        if !read_full_or_eof(reader, &mut header).await? {
            return Ok(None);
        }
        let len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let len = usize::try_from(len).unwrap_or(usize::MAX);
        if len == 0 || len > MAX_FRAME_PAYLOAD + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("vsock: bad frame length {len}"),
            ));
        }
        let stream = LogStream::from_u8(header[4]).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("vsock: bad stream selector {}", header[4]),
            )
        })?;
        let mut payload = vec![0u8; len - 1];
        reader.read_exact(&mut payload).await.map_err(|err| {
            if err.kind() == io::ErrorKind::UnexpectedEof {
                io::Error::new(io::ErrorKind::UnexpectedEof, "vsock: truncated frame")
            } else {
                err
            }
        })?;
        Ok(Some(Self { stream, payload }))
    }
}

/// Fill `buf` or report clean EOF (`false`) / truncation (`Err`).
async fn read_full_or_eof<R>(reader: &mut R, buf: &mut [u8]) -> io::Result<bool>
where
    R: AsyncRead + Unpin,
{
    let mut filled = 0;
    while filled < buf.len() {
        let n = reader.read(&mut buf[filled..]).await?;
        if n == 0 {
            if filled == 0 {
                return Ok(false);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "vsock: truncated frame header",
            ));
        }
        filled += n;
    }
    Ok(true)
}

/// Routes guest vsock connections to the log channel of their task, and
/// delivers the guest's completion signal to subscribers (used by the
/// `OnSuccess` snapshot policy).
#[derive(Debug, Clone, Default)]
pub struct VsockLogHub {
    inner: Arc<Mutex<HubInner>>,
}

#[derive(Debug, Default)]
struct HubInner {
    senders: HashMap<TaskId, mpsc::Sender<String>>,
    completions: HashMap<TaskId, Vec<oneshot::Sender<()>>>,
}

impl VsockLogHub {
    /// Create an empty hub.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a task's log channel. Later guest connections that
    /// handshake with this task id feed this channel.
    pub async fn register(&self, task_id: TaskId, tx: mpsc::Sender<String>) {
        self.inner.lock().await.senders.insert(task_id, tx);
    }

    /// Forget a task: drops its log channel and completion subscribers.
    pub async fn unregister(&self, task_id: &TaskId) {
        let mut inner = self.inner.lock().await;
        inner.senders.remove(task_id);
        inner.completions.remove(task_id);
    }

    /// Subscribe to the guest's completion signal for `task_id`. Fires once,
    /// on the first `Complete` frame or connection close after a handshake.
    pub async fn subscribe_completion(&self, task_id: &TaskId) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .lock()
            .await
            .completions
            .entry(task_id.clone())
            .or_default()
            .push(tx);
        rx
    }

    /// Look up a task's log channel.
    async fn sender_for(&self, task_id: &TaskId) -> Option<mpsc::Sender<String>> {
        self.inner.lock().await.senders.get(task_id).cloned()
    }

    /// Fire (and clear) all completion subscribers for `task_id`.
    async fn notify_completion(&self, task_id: &TaskId) {
        let subs = self.inner.lock().await.completions.remove(task_id);
        if let Some(subs) = subs {
            for tx in subs {
                let _ = tx.send(());
            }
        }
    }
}

/// Serve one guest connection: handshake, then forward frames until the
/// guest completes or goes away.
///
/// The stream is the agent side of Firecracker's vsock bridge: the guest
/// dials `(CID 2, VSOCK_LOG_PORT)` over `AF_VSOCK` and Firecracker pairs it
/// with the agent's `AF_UNIX` listener at `<uds_path>_<port>`, so by the
/// time this runs the transport is an ordinary byte stream.
pub async fn handle_connection<S>(hub: VsockLogHub, mut stream: S)
where
    S: AsyncRead + Unpin,
{
    let task_id = match LogFrame::decode(&mut stream).await {
        Ok(Some(frame)) if frame.stream == LogStream::Handshake => {
            TaskId::from(String::from_utf8_lossy(&frame.payload).into_owned())
        }
        _ => return,
    };
    let Some(tx) = hub.sender_for(&task_id).await else {
        return;
    };
    while let Ok(Some(frame)) = LogFrame::decode(&mut stream).await {
        match frame.stream {
            LogStream::Handshake => {}
            LogStream::Complete => {
                hub.notify_completion(&task_id).await;
                return;
            }
            LogStream::Stdout | LogStream::Stderr => {
                let tag = if frame.stream == LogStream::Stdout {
                    "stdout"
                } else {
                    "stderr"
                };
                let text = String::from_utf8_lossy(&frame.payload);
                let mut closed = false;
                for line in text.lines() {
                    if tx.send(format!("[vsock:{tag}] {line}")).await.is_err() {
                        closed = true;
                        break;
                    }
                }
                if closed {
                    return;
                }
            }
        }
    }
    hub.notify_completion(&task_id).await;
}

/// Accept guest log connections on `listener` forever, serving each with
/// [`handle_connection`].
///
/// The listener is the per-task `AF_UNIX` socket at `<uds_path>_<port>`
/// that Firecracker's vsock bridge pairs guest connections into. The
/// caller (the task's spawn) owns the socket path and stops this when the
/// task finishes.
///
/// # Errors
///
/// Returns the accept error if the listener fails.
pub async fn serve_vsock_logs(
    hub: VsockLogHub,
    listener: tokio::net::UnixListener,
) -> io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let hub = hub.clone();
        tokio::spawn(async move {
            handle_connection(hub, stream).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    fn handshake(task: &str) -> LogFrame {
        LogFrame {
            stream: LogStream::Handshake,
            payload: task.as_bytes().to_vec(),
        }
    }

    #[tokio::test]
    async fn frames_roundtrip_over_loopback() {
        let (mut guest, mut host) = tokio::io::duplex(64 * 1024);
        let frames = vec![
            handshake("task-1"),
            LogFrame {
                stream: LogStream::Stdout,
                payload: "héllo ✓".as_bytes().to_vec(),
            },
            LogFrame {
                stream: LogStream::Stderr,
                payload: Vec::new(),
            },
            LogFrame {
                stream: LogStream::Complete,
                payload: Vec::new(),
            },
        ];
        for frame in &frames {
            guest.write_all(&frame.encode()).await.expect("write frame");
        }
        drop(guest);
        for frame in &frames {
            let got = LogFrame::decode(&mut host)
                .await
                .expect("decode")
                .expect("frame");
            assert_eq!(got, *frame);
        }
        assert!(LogFrame::decode(&mut host).await.expect("eof").is_none());
    }

    #[tokio::test]
    async fn decode_rejects_truncated_frame() {
        let (mut guest, mut host) = tokio::io::duplex(1024);
        let mut bytes = handshake("task-1").encode();
        bytes.truncate(3);
        guest.write_all(&bytes).await.expect("write");
        drop(guest);
        let err = LogFrame::decode(&mut host).await.expect_err("truncated");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn decode_rejects_oversize_length() {
        let (mut guest, mut host) = tokio::io::duplex(1024);
        let mut header = u32::try_from(MAX_FRAME_PAYLOAD + 2)
            .expect("payload bound fits in u32")
            .to_be_bytes()
            .to_vec();
        header.push(LogStream::Stdout as u8);
        guest.write_all(&header).await.expect("write");
        let err = LogFrame::decode(&mut host).await.expect_err("oversize");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn decode_rejects_bad_stream_selector() {
        let (mut guest, mut host) = tokio::io::duplex(1024);
        guest
            .write_all(&[0, 0, 0, 2, 99, b'x'])
            .await
            .expect("write");
        let err = LogFrame::decode(&mut host).await.expect_err("bad selector");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn handler_forwards_lines_and_completion() {
        let hub = VsockLogHub::new();
        let task_id = TaskId::from("task-7".to_string());
        let (tx, mut rx) = mpsc::channel(16);
        hub.register(task_id.clone(), tx).await;
        let done = hub.subscribe_completion(&task_id).await;

        let (mut guest, host) = tokio::io::duplex(64 * 1024);
        let worker = tokio::spawn(async move { handle_connection(hub, host).await });
        for frame in [
            handshake("task-7"),
            LogFrame {
                stream: LogStream::Stdout,
                payload: b"one\ntwo".to_vec(),
            },
            LogFrame {
                stream: LogStream::Stderr,
                payload: b"oops".to_vec(),
            },
            LogFrame {
                stream: LogStream::Complete,
                payload: Vec::new(),
            },
        ] {
            guest.write_all(&frame.encode()).await.expect("write");
        }
        worker.await.expect("handler");
        drop(guest);

        assert_eq!(rx.recv().await.expect("line"), "[vsock:stdout] one");
        assert_eq!(rx.recv().await.expect("line"), "[vsock:stdout] two");
        assert_eq!(rx.recv().await.expect("line"), "[vsock:stderr] oops");
        done.await.expect("completion fired");
    }

    #[tokio::test]
    async fn handler_ignores_unknown_task() {
        let hub = VsockLogHub::new();
        let (mut guest, host) = tokio::io::duplex(64 * 1024);
        let worker = tokio::spawn(async move { handle_connection(hub, host).await });
        guest
            .write_all(&handshake("task-ghost").encode())
            .await
            .expect("write");
        worker.await.expect("handler");
    }

    #[tokio::test]
    async fn handler_ignores_non_handshake_first_frame() {
        let hub = VsockLogHub::new();
        let task_id = TaskId::from("task-8".to_string());
        let (tx, mut rx) = mpsc::channel(16);
        hub.register(task_id, tx).await;
        let (mut guest, host) = tokio::io::duplex(64 * 1024);
        let worker = tokio::spawn(async move { handle_connection(hub, host).await });
        guest
            .write_all(
                &LogFrame {
                    stream: LogStream::Stdout,
                    payload: b"sneaky".to_vec(),
                }
                .encode(),
            )
            .await
            .expect("write");
        drop(guest);
        worker.await.expect("handler");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn eof_after_handshake_still_notifies_completion() {
        let hub = VsockLogHub::new();
        let task_id = TaskId::from("task-9".to_string());
        let (tx, _rx) = mpsc::channel(16);
        hub.register(task_id.clone(), tx).await;
        let done = hub.subscribe_completion(&task_id).await;
        let (mut guest, host) = tokio::io::duplex(64 * 1024);
        let worker = tokio::spawn(async move { handle_connection(hub, host).await });
        guest
            .write_all(&handshake("task-9").encode())
            .await
            .expect("write");
        drop(guest);
        worker.await.expect("handler");
        done.await.expect("completion fired on EOF");
    }

    #[tokio::test]
    async fn serve_unix_socket_routes_guest_frames() {
        use tokio::net::{UnixListener, UnixStream};

        // The exact production shape: the agent binds <uds_path>_<port> and
        // Firecracker pairs the guest's (CID 2, port) connection into it.
        let dir = std::env::temp_dir().join(format!("bigtop-vsock-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.expect("test dir");
        let sock_path = dir.join(format!("v.sock_{VSOCK_LOG_PORT}"));
        let _ = tokio::fs::remove_file(&sock_path).await;
        let listener = UnixListener::bind(&sock_path).expect("bind unix listener");

        let hub = VsockLogHub::new();
        let task_id = TaskId::from("task-unix".to_string());
        let (tx, mut rx) = mpsc::channel(16);
        hub.register(task_id.clone(), tx).await;
        let done = hub.subscribe_completion(&task_id).await;

        let server = tokio::spawn(serve_vsock_logs(hub, listener));
        let mut guest = UnixStream::connect(&sock_path).await.expect("connect");
        for frame in [
            handshake("task-unix"),
            LogFrame {
                stream: LogStream::Stdout,
                payload: b"hello over unix".to_vec(),
            },
            LogFrame {
                stream: LogStream::Complete,
                payload: Vec::new(),
            },
        ] {
            guest.write_all(&frame.encode()).await.expect("write frame");
        }
        drop(guest);

        assert_eq!(
            rx.recv().await.expect("line"),
            "[vsock:stdout] hello over unix"
        );
        done.await.expect("completion fired");
        server.abort();
        tokio::fs::remove_dir_all(&dir).await.expect("cleanup");
    }
}
