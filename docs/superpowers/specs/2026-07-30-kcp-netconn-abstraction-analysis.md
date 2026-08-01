# KcpConn/KcpSession 重构设计文档

**日期**: 2026-07-31
**目标**: 将 kcp-rs 重构为 KcpConn（通用 KCP 可靠传输）+ KcpSession（kcptun 加密会话层），API 兼容 tokio TCP，与 Go kcp-go v5 全兼容

---

## 一、架构总览

### 分层归属

```
kcp-rs/                        通用 KCP ARQ 库
├── kcp.rs                     核心状态机（不变）
├── segment.rs                 段定义（不变）
├── fec.rs                     FEC 编解码原语（不变）
├── crypto_buf.rs              加密缓冲区（不变）
├── snmp.rs                    统计计数器（不变）
├── session.rs [DEPRECATED]    旧 UDPSession
└── conn.rs [NEW]              KcpConn + KcpListener

kcptun-common/                 kcptun 共享会话层
├── key.rs                     密钥派生（不变）
├── mode.rs                    KCP 模式（不变）
├── snappy_frame.rs            Snappy 帧（不变）
├── pipe.rs                    双向 copy（不变）
└── session.rs [NEW]           KcpSession 构造 + CryptoTransport

kcptun-client/main.rs          [REFACTOR] 使用 KcpSession
kcptun-server/main.rs          [REFACTOR] 使用 KcpSession
```

### 协议栈

```
      用户 → AsyncRead/AsyncWrite
                  │
          ┌───────┴───────┐
          │   KcpConn     │  kcp-rs/conn.rs
          │               │
          │  flush loop   │  KCP + FEC + transport
          │  input loop   │
          └───────┬───────┘
                  │ DatagramSocket trait
          ┌───────┴───────┐
          │  Transport    │  可插拔：UDP / raw TCP / 加密套壳
          └───────┬───────┘
                  │
     ┌────────────┴────────────┐
     │  UDP socket             │  kio-rs 已有
     │  TcpRawConn (Linux)     │  kio-rs 已有
     │  CryptoTransport        │  kcptun-common 新增
     └─────────────────────────┘
```

### 数据流全路径

```
KcpConn 无加密无 FEC:
  write → poll_write → write_buf
    → flush loop: KCP::send → KCP::flush → raw segs → transport.send_batch()
  read ← poll_read ← read_buf
    ← input loop: transport.recv() → KCP::input() → KCP::recv() → read_buf

KcpConn + FEC:
  write 同上，flush 后: raw segs → FEC encode → transport.send_batch()
  read: transport.recv() → FEC decode → KCP::input() × N → KCP::recv() → read_buf

KcpSession (KcpConn + CryptoTransport + FEC):
  write: flush → raw segs → FEC encode → [CryptoTransport.encrypt] → UDP.send_batch()
  read: UDP.recv() → [CryptoTransport.decrypt] → FEC decode → KCP::input × N → recv
```

---

## 二、kcp-rs/conn.rs — KcpConn

### KcpConn 结构体

```rust
/// KCP 可靠传输连接。
///
/// 通过 generic DatagramSocket 支持 UDP、raw TCP、加密套壳等传输层。
/// 提供与 tokio::net::TcpStream 一致的 AsyncRead + AsyncWrite 接口。
///
/// ── 可选 FEC ──
///
/// KcpConn 可选的 FEC 是 Reed-Solomon 前向纠错，用于抗丢包。
/// FEC 作用于 raw KCP segments 之上（先 FEC 后传输，先接收后 FEC 再 KCP）。
/// 与 Go kcp-go v5 完全兼容。
///
/// ── 无加密 ──
///
/// KcpConn 本身不含任何加密逻辑。加密通过 DatagramSocket 的包装层
/// （CryptoTransport）注入，对 KcpConn 完全透明。
pub struct KcpConn {
    // ── 传输层 ──
    /// 泛型传输层。默认 = UdpSocket，可替换为 TcpRawConn、CryptoTransport 等。
    transport: Box<dyn DatagramSocket>,

    // ── KCP 状态机 ──
    /// KCP ARQ 状态机，flush/input 任务共享。
    kcp: Arc<Mutex<KCP>>,

    // ── 可选 FEC ──
    fec_encoder: Option<Mutex<FecEncoder>>,
    fec_decoder: Option<Mutex<FecDecoder>>,

    // ── 用户数据缓冲 ──
    /// poll_write 积累的数据，flush loop 消费。
    write_buf: Arc<Mutex<BytesMut>>,
    /// KCP::recv 产出的数据，poll_read 消费。
    read_buf: Arc<Mutex<VecDeque<BytesMut>>>,

    // ── 异步协作原语 ──
    /// SMUX 写入后立即通知 flush loop 唤醒，减少延迟。
    flush_notify: Arc<Notify>,
    /// KCP 发送窗口有空余时通知 poll_write 恢复写。
    write_notify: Arc<Notify>,
    /// 当前 KCP 等待发送的包数，poll_write 判读背压。
    wait_send: Arc<AtomicUsize>,

    // ── 配置 ──
    acknodelay: bool,          // input() 时是否立即发送 ACK
    remote_addr: SocketAddr,

    // ── 生命周期 ──
    closed: Arc<AtomicBool>,

    // ── 后台任务 ──
    _handles: Vec<JoinHandle>,
}
```

### KcpConfig

```rust
/// KCP 配置。提供合理的默认值（Fast3 优化参数）。
#[derive(Clone)]
pub struct KcpConfig {
    // ── KCP 传输参数 ──
    pub mtu: u32,              // 1350
    pub sndwnd: u32,           // 128
    pub rcvwnd: u32,           // 128
    pub mode: KcpMode,         // KcpMode::Fast3

    // manual mode（mode=Manual 时使用）
    pub nodelay: u32,          // 1
    pub interval: u32,         // 10
    pub resend: u32,           // 2
    pub nc: u32,               // 1

    pub stream: bool,          // true
    pub acknodelay: bool,      // true

    // ── FEC ──
    pub datashard: u32,        // 0（禁用）
    pub parityshard: u32,      // 0（禁用）

    // ── SMUX 配置（仅 kcptun 用，KcpConn 忽略）──
    pub smuxver: u8,           // 1
    pub smuxbuf: usize,        // 4194304
    pub streambuf: usize,      // 65536
    pub framesize: usize,      // 65536
    pub keepalive: u64,        // 10s
    pub nocomp: bool,          // false
    pub ratelimit: u32,        // 0
}

#[derive(Clone, Default)]
pub enum KcpMode {
    #[default]
    Fast3,
    Normal,
    Fast,
    Fast2,
    Manual, // 手写 nodelay/interval/resend/nc
}
```

### Builder

```rust
pub struct KcpConnBuilder {
    addr: Option<String>,
    transport: Option<Box<dyn DatagramSocket>>,
    config: KcpConfig,
}

impl KcpConnBuilder {
    pub fn transport(mut self, t: impl DatagramSocket + 'static) -> Self {
        self.transport = Some(Box::new(t));
        self
    }

    pub fn mtu(mut self, v: u32) -> Self { self.config.mtu = v; self }
    pub fn sndwnd(mut self, v: u32) -> Self { self.config.sndwnd = v; self }
    pub fn rcvwnd(mut self, v: u32) -> Self { self.config.rcvwnd = v; self }
    pub fn mode(mut self, v: KcpMode) -> Self { self.config.mode = v; self }
    pub fn fec(mut self, d: u32, p: u32) -> Self {
        self.config.datashard = d; self.config.parityshard = p; self
    }
}

impl IntoFuture for KcpConnBuilder {
    type Output = Result<KcpConn>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;
    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let transport = self.transport.unwrap_or_else(|| {
                Box::new(UdpSocket::bind("0.0.0.0:0"))  // 默认 UDP
            });
            // ... 创建 KCP、FEC、spawn 任务
        })
    }
}
```

### KcpConn 方法

```rust
impl KcpConn {
    // ── 构造 ──
    /// "127.0.0.1:29900" → Builder
    ///
    /// let conn = KcpConn::connect("127.0.0.1:29900").await?;
    /// let conn = KcpConn::connect("127.0.0.1:29900").mtu(1400).fec(10,3).await?;
    pub fn connect(addr: impl ToSocketAddrs) -> KcpConnBuilder;

    /// 与 connect 同，但用户提供 transport（raw TCP / CryptoTransport 等）
    pub fn with_transport(transport: Box<dyn DatagramSocket>) -> KcpConnBuilder;

    // ── 配置（构造后动态调整）──
    pub fn set_nodelay(&self, nodelay: u32, interval: u32, resend: u32, nc: u32);
    pub fn set_window_size(&self, snd_wnd: u32, rcv_wnd: u32);
    pub fn set_mtu(&self, mtu: u32);
    pub fn set_stream_mode(&self, enable: bool);

    // ── 生命周期 ──
    pub fn close(&self);
    pub fn is_closed(&self) -> bool;
    pub fn remote_addr(&self) -> SocketAddr;
}
```

### KcpListener

```rust
/// KCP 服务端监听器。与 tokio::net::TcpListener 一致的接口。
///
/// ── 使用 ──
///
/// let listener = KcpListener::bind("0.0.0.0:29900").await?;
/// // 或自定义传输
/// let listener = KcpListener::bind("0.0.0.0:29900")
///     .transport(raw_tcp_listener)
///     .fec(10, 3)
///     .await?;
///
/// while let Some((conn, peer)) = listener.accept().await {
///     kio::spawn_task(async move { /* conn.read/write */ });
/// }
pub struct KcpListener {
    transport: Box<dyn DatagramSocket>,
    sessions: Arc<DashMap<SocketAddr, KcpConn>>,
    config: KcpConfig,
    closed: Arc<AtomicBool>,
}

impl KcpListener {
    pub fn bind(addr: impl ToSocketAddrs) -> KcpListenerBuilder;
    pub async fn accept(&self) -> Option<(KcpConn, SocketAddr)>;
    pub fn close(&self);
}
```

### DatagramSocket trait

```rust
/// 传输层抽象。KcpConn 通过此 trait 与具体传输解耦。
///
/// 已知实现:
/// - kio::UdpSocket              — 默认 UDP 传输
/// - kio::TcpRawConn (Linux)     — raw TCP 传输
/// - kcptun_common::CryptoTransport — 加密传输包装
pub trait DatagramSocket: Send + Sync {
    /// 读取一个数据报。返回实际读到的字节数。
    fn poll_recv(&self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>>;

    /// 批量发送数据报。
    fn poll_send_batch(&self, cx: &mut Context<'_>, pkts: &[Bytes]) -> Poll<io::Result<()>>;

    /// 高优发送（ACK 用）。默认实现 = poll_send_batch。
    fn poll_send_urgent(&self, cx: &mut Context<'_>, pkts: &[Bytes]) -> Poll<io::Result<()>> {
        self.poll_send_batch(cx, pkts)
    }

    fn local_addr(&self) -> io::Result<SocketAddr>;
}
```

注意：这里改成 `poll_*` 签名，因为 KcpConn 的内部任务需要非阻塞 polling。

---

## 三、内部后台任务

### Input Loop

```rust
// 在 KcpConn 构造时 spawn：
async fn input_loop(self: Arc<Self>) {
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        if self.closed.load(Acquire) { break; }

        let n = poll_fn(|cx| self.transport.poll_recv(cx, &mut buf)).await?;
        // ── FEC decode ──
        let inputs = if let Some(ref dec) = self.fec_decoder {
            let mut dec = dec.lock();
            let recovered = dec.decode(&buf[..n]);
            fec_to_kcp_inputs(&buf[..n], &recovered)  // 0~N 条
        } else {
            vec![Bytes::copy_from_slice(&buf[..n])]
        };

        // ── KCP input ──
        let mut kcp = self.kcp.lock();
        for input in &inputs {
            if kcp.input(input, self.acknodelay).is_err() { break; }
        }
        // 收集 ACK
        let acks = drain_output();
        drop(kcp);  // ← 释放 KCP 锁再发送

        // ── 发送 ACK（可能 FEC 编码后）──
        if !acks.is_empty() {
            let to_send = if let Some(ref enc) = self.fec_encoder {
                fec_expand_packets(&mut enc.lock(), acks, 500)
            } else { acks };
            poll_fn(|cx| self.transport.poll_send_urgent(cx, &to_send)).await?;
        }

        // ── 唤醒等待写 ──
        let ws = kcp.wait_send() as usize;
        self.wait_send.store(ws, Relaxed);
        if ws < self.config.sndwnd as usize {
            self.write_notify.notify_waiters();
        }
    }
}
```

### Flush Loop

```rust
async fn flush_loop(self: Arc<Self>) {
    let mut next_update = KCP_UPDATE_INTERVAL_MS;
    loop {
        if self.closed.load(Acquire) { break; }

        // 等待超时或 flush_notify
        let _ = timeout(Duration::from_millis(next_update), self.flush_notify.notified()).await;

        // ── KCP send + flush（紧凑持锁）──
        let raw = {
            let wb = self.write_buf.lock().split().freeze();
            let mut kcp = self.kcp.lock();
            if !wb.is_empty() { kcp.send(&wb).ok(); }
            next_update = kcp.flush() as u64;
            let ws = kcp.wait_send() as usize;
            let has_data = !wb.is_empty() || ws > 0;
            drop(kcp);
            let packets = drain_output();
            (packets, ws, has_data)
        };

        // ── FEC → 发送 ──
        let to_send = if let Some(ref enc) = self.fec_encoder {
            fec_expand_packets(&mut enc.lock(), raw.0, 500)
        } else { raw.0 };

        if !to_send.is_empty() {
            poll_fn(|cx| self.transport.poll_send_batch(cx, &to_send)).await?;
        }

        // ── 动态间隔 + 唤醒 writer ──
        next_update = if raw.2 || raw.1 > 0 {
            1  // 有数据待发 → 尽快再 flush
        } else {
            next_update.clamp(1, KCP_UPDATE_INTERVAL_MS)
        };
        self.wait_send.store(raw.1, Relaxed);
        self.write_notify.notify_waiters();
    }
}
```

---

## 四、AsyncRead + AsyncWrite

```rust
impl kio::AsyncRead for KcpConn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut kio::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut rb = self.read_buf.lock();
        if let Some(data) = rb.pop_front() {
            let n = data.len().min(buf.remaining());
            buf.put_slice(&data[..n]);
            if n < data.len() {
                rb.push_front(data.slice(n..));
            }
            return Poll::Ready(Ok(()));
        }
        // 无数据，注册 waker
        // （input loop 有新数据时会通过 channel 或 notify 唤醒）
        Poll::Pending
    }
}

impl kio::AsyncWrite for KcpConn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.is_closed() { return Poll::Ready(Err(io::Error::new( BrokenPipe ))); }

        // 背压
        if self.wait_send.load(Relaxed) >= self.config.sndwnd as usize {
            return Poll::Pending;  // flush loop 会 notify_waiters
        }

        let mut wb = self.write_buf.lock();
        wb.extend_from_slice(buf);
        let n = buf.len();
        self.flush_notify.notify_one();  // 立即唤醒 flush loop
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.close();
        Poll::Ready(Ok(()))
    }
}
```

---

## 五、kcptun-common/session.rs — KcpSession

### CryptoTransport

```rust
/// 加密传输层包装。
///
/// 透明地在 DatagramSocket 上叠加 CFB/AEAD 加解密：
/// ```
/// CryptoTransport(udp).send(data) = udp.send(encrypt(data))
/// CryptoTransport(udp).recv(buf)  = udp.recv() → decrypt(buf) → buf
/// ```
pub struct CryptoTransport {
    inner: Box<dyn DatagramSocket>,
    crypt: Arc<CryptEngine>,
    has_encryption: bool,
    data_crypto_buf: Mutex<CryptoBuf>,
    ack_crypto_buf: Mutex<CryptoBuf>,  // ACK 专用，防竞争
}

impl DatagramSocket for CryptoTransport {
    fn poll_recv(&self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        let n = ready!(self.inner.poll_recv(cx, buf)?);
        if !self.has_encryption && !self.crypt.is_aead() {
            return Poll::Ready(Ok(n));  // null: 直通
        }
        // AEAD
        if self.crypt.is_aead() {
            let aead = self.crypt.as_aead().unwrap();
            match aead.open(&buf[..n]) {
                Ok(plain) => { buf[..plain.len()].copy_from_slice(&plain); Ok(plain.len()) }
                Err(_) => Poll::Ready(Err(io::Error::new(InvalidData, "AEAD decrypt failed")))
            }
        } else {
            // CFB in-place
            match decrypt_cfb_in_place(&mut buf[..n], &self.crypt, false) {
                Ok(body) => {
                    let len = body.len();
                    buf[..len].copy_from_slice(body);
                    Poll::Ready(Ok(len))
                }
                Err(_) => Poll::Ready(Err(io::Error::new(InvalidData, "CFB checksum failed")))
            }
        }
    }

    fn poll_send_batch(&self, cx: &mut Context<'_>, pkts: &[Bytes]) -> Poll<io::Result<()>> {
        if !self.has_encryption && !self.crypt.is_aead() {
            return self.inner.poll_send_batch(cx, pkts);  // null: 直通
        }
        let encrypted = if self.crypt.is_aead() {
            let aead = self.crypt.as_aead().unwrap();
            let mut cb = self.data_crypto_buf.lock();
            pkts.iter().map(|d| cb.seal_aead(aead, d)).collect()
        } else {
            let mut cb = self.data_crypto_buf.lock();
            pkts.iter().map(|d| cb.encrypt_cfb(d, &self.crypt)).collect()
        };
        self.inner.poll_send_batch(cx, &encrypted)
    }

    fn poll_send_urgent(&self, cx: &mut Context<'_>, pkts: &[Bytes]) -> Poll<io::Result<()>> {
        if !self.has_encryption {
            return self.inner.poll_send_urgent(cx, pkts);
        }
        // ACK 用独立 buffer 加密
        let encrypted = if self.crypt.is_aead() {
            // ...
        } else {
            let mut cb = self.ack_crypto_buf.lock();
            pkts.iter().map(|d| cb.encrypt_cfb(d, &self.crypt)).collect()
        };
        self.inner.poll_send_batch(cx, &encrypted)
    }
}
```

### KcpSession 工厂函数

```rust
/// 创建加密的 KCP 会话。
/// 返回的 KcpConn 与普通 KcpConn 是同一类型，可无缝用于 SMUX 等上层。
///
/// 内部构造 CryptoTransport(udp) → KcpConn.with_transport(ct)
pub async fn kcp_session(
    addr: impl ToSocketAddrs,
    key: &[u8; 32],
    crypt: &str,
    config: KcpConfig,
) -> Result<KcpConn> {
    let udp = UdpSocket::bind("0.0.0.0:0").await?;
    let (engine, _) = CryptEngine::select(crypt, key);
    let ct = CryptoTransport {
        inner: Box::new(udp),
        crypt: Arc::new(engine),
        has_encryption: crypt != "null",
        data_crypto_buf: Mutex::new(CryptoBuf::new(rand())),
        ack_crypto_buf: Mutex::new(CryptoBuf::new(!rand())),
    };
    KcpConn::with_transport(Box::new(ct))
        .await  // ← 这里需要 addr，但 Builder 已经设好了
}
```

---

## 六、功能与 Go 兼容性对照

| 特性 | Go kcp-go | Rust 当前 | Rust 新架构 | 兼容性 |
|------|-----------|-----------|-------------|--------|
| KCP 24B header | kcp.go | kcp.rs | 不变 | ✅ |
| FEC 0x00f1/0x00f2 | fec.go | fec.rs | KcpConn 内 | ✅ |
| FEC recover | fec.go | fec.rs | KcpConn input loop | ✅ |
| CFB 20B header | xor.go etc. | crypto_buf.rs | CryptoTransport | ✅ |
| AES-GCM | aes.go | crypt.rs | CryptoTransport | ✅ |
| ACK nodelay | sess.go | 传给 input() | KcpConn input loop | ✅ |
| flush timing | update() + ticker | timeout + notify | KcpConn flush loop | ✅ |
| dead_link | kcp.go | KCP::is_dead() | KcpConn flush loop | ✅ |
| SNMP | snmp.go | snmp.rs | KcpConn + CryptoTransport | ✅ |
| 速率限制 | sema.go | RateLimiter | kcptun binary | ✅ |
| Snappy | snappy.go | SnappyFrame | kcptun binary | ✅ |
| SMUX | kcptun mux | smux-rs | kcptun binary | ✅ |
| QPP | N/A | qpp-rs | kcptun binary | ✅ |

**三项 Go kcp-go 特有的非 wire 特性（不在兼容范围内）**:
- `SetReadDeadline` / `SetWriteDeadline` — Go net.Conn 方法，不跨语言
- `Session.CloseWrite()` — Go 半关闭，不跨语言
- *ControlMessage* — Go-specific

---

## 七、性能优化保留清单

| 优化 | 当前位置 | 新架构位置 | 策略 |
|------|---------|------------|------|
| KCP 锁外加密 | flush loop | CryptoTransport.poll_send_batch | 加密在 transport 层，不在 KCP 锁内 |
| batch encrypt (thread::scope) | encrypt_batch | CryptoTransport（保留） | 重 cipher 大 batch 时 threads scope |
| cpu_block offload | flush loop | CryptoTransport（保留） | should_cpu_block_encrypt 计算 |
| ack_crypto_buf 分离 | KcpConn | CryptoTransport | send_urgent 走 ack_crypto_buf |
| send_batch (sendmmsg) | KcpConn | transport.poll_send_batch | trait 方法，底层实现不变 |
| flush_notify 立即唤醒 | KcpConn | KcpConn | poll_write 后 notify_one |
| write_notify 背压 | SmuxIo | KcpConn | poll_write 检查 wait_send |
| 动态 next_update | KcpConn | KcpConn flush_loop | 与当前完全一致 |
| FEC expand_packets | KcpConn | KcpConn flush_loop | 不变 |
| SegmentPool | KCP | KCP | 不变 |
| SNMP atomic counters | snmp.rs | snmp.rs | 不变 |

---

## 八、实施计划

### Phase 1: kcp-rs/conn.rs — KcpConn raw（~500 行）

文件: `kcp-rs/src/conn.rs`
Feature gate: `async`（需 kio-rs）

输出:
- KcpConfig + KcpMode
- DatagramSocket trait（含 poll_recv/poll_send_batch/poll_send_urgent）
- KcpConn（含 input_loop + flush_loop）
- KcpListener（bind + accept）
- KcpConnBuilder（IntoFuture）
- AsyncRead + AsyncWrite impl

**验证**: `cargo build -p kcp-rs --features async`，无加密 echo 测试

### Phase 2: KcpConn + FEC（~100 行）

将 fec_encoder/fec_decoder 集成到 KcpConn 的 input_loop + flush_loop。

**验证**: FEC e2e 测试，原始数据完整性

### Phase 3: kcptun-common/session.rs — KcpSession（~200 行）

文件: `kcptun-common/src/session.rs`

输出:
- CryptoTransport（DatagramSocket impl，加密/解密/ACK buffer 分离）
- kcp_session() 工厂函数
- 保留 cpu_block / batch encrypt / parallel 所有优化

**验证**: `make e2e`（全部 cipher × mode × FEC 组合）

### Phase 4: 客户端/服务端重构（~500 行）

- 替换现有 KcpConn → kcp_session()
- 替换 KcpServerSession → KcpListener + kcp_session()
- 删除 ~1200 行重复 flush/input/encrypt/FEC 代码
- Snappy + SMUX 保留在 binary

**验证**: `make stress` + `make e2e` + `make clippy`

### Phase 5: 扫尾（~50 行）

- 旧 session.rs 标记 `#[deprecated]`
- AGENTS.md 更新
- CHANGELOG.md

---

## 九、总代码量

| Phase | 新增 | 删除 | 净变化 |
|-------|------|------|--------|
| 1: KcpConn raw | ~500 | 0 | +500 |
| 2: KcpConn + FEC | ~100 | 0 | +100 |
| 3: CryptoTransport + KcpSession | ~200 | 0 | +200 |
| 4: 客户端/服务端重构 | ~500 | ~1200 | -700 |
| 5: 扫尾 | ~50 | 0 | +50 |
| **总计** | **~1350** | **~1200** | **+150** |

与当前代码量相比基本持平，但消除了重复逻辑，API 更干净，分层更合理。

---

## Implementation status (2026-07-31 Task 7)

| Phase | Status | Notes |
|-------|--------|-------|
| 1 KcpConn raw | **done** | `aa64678d` |
| 2 KcpConn + FEC | **done** | `3dbd4660` |
| 3 CryptoTransport + kcp_session | **done** | `5ecd06ab`; dial/accept helpers `2e71800a` |
| 4 client/server production rewrite | **deferred** | binaries still use legacy flush loops; library path ready |
| 5 docs / AGENTS | **done** (Task 7) | this file tracked; AGENTS synced |

**Out of scope until cut-over:** multi-peer `KcpListener`, full binary replacement of KCP loops.
