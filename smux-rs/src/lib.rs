//! # smux-rs
//!
//! A stream multiplexer for kcptun. SMUX allows multiple logical streams over
//! a single underlying transport (typically a KCP connection). It is a Rust
//! port of the Go `smux` library by xtaci.
//!
//! ## Protocol
//!
//! SMUX frames have a header followed by optional payload:
//!
//! ```text
//! 0      4       8       12      16      20
//! +------+-------+-------+-------+-------+
//! | ver  | cmd   | length| stream_id    |
//! +------+-------+-------+-------+-------+
//! | data...                               |
//! +---------------------------------------+
//! ```
//!
//! ## Architecture
//!
//! - `Session` — multiplexer over a single transport
//! - `Stream` — individual logical channel (implements AsyncRead + AsyncWrite)
//! - `SmuxConn` / `SmuxConnBuilder` — high-level connect/serve builder (auto driver)
//! - Keepalive via periodic ping frames
//!
//! Standalone:
//! ```ignore
//! let conn = SmuxConn::connect(tcp).version(2).build().await?;
//! let stream = conn.open_stream()?;
//! ```
pub mod conn;
pub mod frame;
pub mod io;
pub mod session;
pub mod stream;

pub use conn::{SmuxConn, SmuxConnBuilder};
pub use frame::{Cmd, Frame, FrameCodec, FrameError, FRAME_HEADER_SIZE, MAX_FRAME_SIZE};
pub use io::SmuxIo;
pub use session::{Config, Session, SessionError, UpdFrame, DEFAULT_CONFIG};
pub use stream::Stream;
