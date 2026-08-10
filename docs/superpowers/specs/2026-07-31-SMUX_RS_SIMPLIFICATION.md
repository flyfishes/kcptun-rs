# smux-rs Builder 风格 API 简化设计

**日期**: 2026-07-31  
**目标**: 去掉 SmuxIo，Stream 直接 AsyncRead/AsyncWrite；SmuxConn 用 Builder 构造（对齐 KcpConn），同时兼容独立使用与 kcptun 集成

---

## 一、核心思路

```
KcpConn 风格:
  KcpConn::connect("addr").mtu(1400).fec(10,3).await?
  → 返回 KcpConn: AsyncRead + AsyncWrite

SmuxConn 风格（对齐）:
  SmuxConn::connect(transport).version(2).keepalive(10).await?
  → 返回 SmuxConn
  → open_stream() / accept() 返回 Arc<Stream>: AsyncRead + AsyncWrite

不再需要:
  SmuxIo
  with_backpressure
  client()/server()/new()+run()/spawn() 四套入口
```

---

## 二、目标 API

### 独立使用（TCP / 任意 transport）

```rust
use smux_rs::{SmuxConn, Config};
use kio::{AsyncReadExt, AsyncWriteExt};

// ── Client ──
let tcp = kio::TcpStream::connect("127.0.0.1:8080").await?;
let smux = SmuxConn::connect(tcp)
    .version(2)
    .keepalive(10)
    .max_frame_size(16 * 1024)
    .await?;

let mut stream = smux.open_stream()?;   // Arc<Stream>
stream.write_all(b"hello").await?;
let mut buf = [0u8; 1024];
let n = stream.read(&mut buf).await?;

// ── Server ──
let (tcp, _) = listener.accept().await?;
let smux = SmuxConn::serve(tcp)
    .version(2)
    .keepalive(10)
    .await?;

loop {
    let mut stream = smux.accept().await?;  // Arc<Stream>
    kio::spawn_task(async move {
        // handle stream
    });
}

// ── 也可以一次塞完整 Config ──
let smux = SmuxConn::connect(tcp)
    .config(Config { version: 2, ..DEFAULT_CONFIG })
    .await?;
```

### 与 KcpConn 组合（kcptun 高层路径）

```rust
// KcpConn / KcpSession 已经是 AsyncRead+AsyncWrite
let kcp = KcpSession::connect("1.2.3.4:29900", key, "aes-128").await?;

let smux = SmuxConn::connect(kcp)   // 直接当 transport 传进去
    .version(2)
    .await?;

let stream = smux.open_stream()?;
// stream → AsyncRead/Write
// 背压自然发生在 KcpConn::poll_write，SMUX 不需要知道 KCP
```

### kcptun 低阶路径（自定义 flush，仍保留）

```rust
// 不走 SmuxConn 驱动时，仍可用 Session
let session = Session::new_client(&cfg)?;
let stream = session.open_stream()?;  // Arc<Stream>，已有 AsyncRead/Write

// 二进制自己:
//   process_data / prepare_outbound_into
//   stream 读写
// 不再 with_backpressure
```

---

## 三、Builder 设计

```rust
pub struct SmuxConnBuilder<T> {
    transport: T,
    is_client: bool,
    config: Config,
}

impl SmuxConn {
    /// 客户端：拥有 transport，自动启动驱动
    pub fn connect<T>(transport: T) -> SmuxConnBuilder<T>
    where
        T: kio::AsyncRead + kio::AsyncWrite + Send + Unpin + 'static;

    /// 服务端：拥有已 accept 的 transport，自动启动驱动
    pub fn serve<T>(transport: T) -> SmuxConnBuilder<T>
    where
        T: kio::AsyncRead + kio::AsyncWrite + Send + Unpin + 'static;
}

impl<T> SmuxConnBuilder<T>
where
    T: kio::AsyncRead + kio::AsyncWrite + Send + Unpin + 'static,
{
    pub fn config(mut self, cfg: Config) -> Self;
    pub fn version(mut self, v: u8) -> Self;
    pub fn keepalive(mut self, secs: u64) -> Self;
    pub fn keepalive_timeout(mut self, secs: u64) -> Self;
    pub fn max_frame_size(mut self, n: usize) -> Self;
    pub fn max_receive_buffer(mut self, n: usize) -> Self;
    pub fn max_stream_buffer(mut self, n: usize) -> Self;
}

impl<T> IntoFuture for SmuxConnBuilder<T>
where
    T: kio::AsyncRead + kio::AsyncWrite + Send + Unpin + 'static,
{
    type Output = Result<SmuxConn, SessionError>;
    // resolve:
    //   1. Session::new_client/server
    //   2. enable_accept if server
    //   3. split transport → spawn read task + flush task
    //   4. return SmuxConn { session, flush_notify, _handles }
}
```

### 与 KcpConn Builder 对照

| | KcpConn | SmuxConn |
|--|---------|----------|
| 入口 | `connect(addr)` / `with_transport(t)` | `connect(t)` / `serve(t)` |
| 配置链 | `.mtu().fec().mode()` | `.version().keepalive().config()` |
| 完成 | `.await?` → `KcpConn` | `.await?` → `SmuxConn` |
| 读写对象 | conn 本身 AsyncRead/Write | `open_stream()` / `accept()` → Stream |
| 传输 | 默认 UDP，可换 | 调用方传入任意 AsyncRead+Write |

命名差异是合理的：
- KCP 是“连地址”
- SMUX 是“在已有连接上多路复用”

---

## 四、结构体

### SmuxConn

```rust
pub struct SmuxConn {
    session: Arc<Session>,
    flush_notify: Arc<kio::Notify>,
    _handles: Vec<JoinHandle<()>>,  // read + flush 两个任务
}

impl SmuxConn {
    pub fn open_stream(&self) -> Result<Arc<Stream>, SessionError>;
    // open_stream:
    //   let s = session.open_stream()?;
    //   session.queue_syn(s.id());
    //   s.set_flush_notify(self.flush_notify.clone());
    //   Ok(s)

    pub async fn accept(&self) -> Result<Arc<Stream>, SessionError>;
    // accept:
    //   wait accept_notify / pop_accepted_stream
    //   set_flush_notify
    //   Ok(stream)

    pub fn close(&self);
    pub fn is_closed(&self) -> bool;
    pub fn session(&self) -> &Session;  // 低阶逃生舱
}
```

### Stream（合并原 SmuxIo）

```rust
pub struct Stream {
    // 现有字段不变 ...
    /// 写后通知 flush 循环（SmuxConn 路径设置；低阶 Session 可不设）
    flush_notify: parking_lot::Mutex<Option<Arc<kio::Notify>>>,
}

impl Stream {
    pub fn set_flush_notify(&self, n: Arc<kio::Notify>);
    // 现有 sync read/write/push/drain 不变
}

// 直接实现异步 IO —— 不再需要 SmuxIo
impl kio::AsyncRead for Stream { /* poll_read_into */ }
impl kio::AsyncWrite for Stream {
    fn poll_write(...) {
        match self.write(buf) {
            Ok(n) => {
                if let Some(nfy) = self.flush_notify.lock().as_ref() {
                    nfy.notify_one();
                }
                Poll::Ready(Ok(n))
            }
            Err(StreamError::Closed) => Poll::Ready(Err(...)),
            ...
        }
    }
    fn poll_shutdown(...) {
        self.mark_local_closed();
        // 也 notify flush，尽快发 FIN
        if let Some(nfy) = self.flush_notify.lock().as_ref() {
            nfy.notify_one();
        }
        Poll::Ready(Ok(()))
    }
}
```

**删除整个 `src/io.rs`。**

---

## 五、内部驱动（对用户不可见）

`Builder.await` 时统一启动双任务（不再提供 run/spawn 两套）：

```text
Read task:
  loop {
    n = transport.read(buf)
    session.process_data(&buf[..n])
  }

Flush task:
  loop {
    wait(flush_notify | 10ms)
    prepare_outbound_into(buf)
    keepalive / reap (节流 ~1s)
    transport.write_all(buf)
    mark_fins_sent
  }
```

- 独立使用 TCP：transport = TcpStream（内部 split）
- 与 KcpConn 组合：transport = KcpConn（内部 split 或读写共享）
- 不再需要 10ms `run()` 单任务超时 hack 作为主路径  
  （若某 runtime 难 split，可内部 fallback，但不暴露两套 API）

---

## 六、删除与保留

### 删除

| 项 | 原因 |
|----|------|
| `src/io.rs` / `SmuxIo` | Stream 自己就是 AsyncRead/Write |
| `with_backpressure` | 背压归 transport（KcpConn/TCP） |
| `SmuxConn::new()` 无 transport | 统一走 Builder |
| `client()` / `server()` 旧签名 | 被 `connect(t)` / `serve(t)` 取代 |
| 公开的 `run()` / `spawn()` | 驱动内化，用户不需要管 |

### 保留

| 项 | 原因 |
|----|------|
| `Session` 全部低阶 API | kcptun 自定义调度仍需要 |
| `Frame` / `FrameCodec` | 协议层 |
| `Stream` sync API | 低阶/测试 |
| keepalive / reap | 独立使用必须自动做 |
| `open_stream` / `accept` | 用户主入口 |

### 迁移对照

```rust
// 旧独立使用
SmuxConn::client(cfg, tcp)?
SmuxConn::server(cfg, tcp)?
let s: SmuxIo = conn.open_stream()?;

// 新独立使用
SmuxConn::connect(tcp).config(cfg).await?
SmuxConn::serve(tcp).config(cfg).await?
let s: Arc<Stream> = conn.open_stream()?;

// 旧 kcptun
SmuxIo::with_backpressure(stream, flush, wait_send, snd_wnd, write_notify)
SmuxIo::new(stream, flush)

// 新 kcptun
// 高层: SmuxConn::connect(kcp_conn).await?
// 低阶: session.open_stream()?  // Arc<Stream> 直接用
```

---

## 七、为什么这样仍兼容“单独使用”

1. **一句话上手**  
   `SmuxConn::connect(tcp).await?` — 比现在的 `client(cfg, tcp)` 更整齐，也对齐 KcpConn。

2. **不要求用户理解 Session/flush**  
   Builder 内部启动驱动；keepalive/reap 自动。

3. **返回值可直接 pipe**  
   `Arc<Stream>: AsyncRead + AsyncWrite`，可直接 `copy_bidirectional`。

4. **不依赖 KCP**  
   transport 是任意 AsyncRead+Write；TCP 独立场景完全成立。

5. **不强迫 split**  
   split 是实现细节，藏在 Builder 里。

---

## 八、与 KcpConn 文档的一致性

```text
用户心智模型（两层都一样）:

  Xxx::入口(资源)
      .链式配置()
      .await?
  → 得到可异步读写的对象（或可 open 出可读写对象）

KCP:
  资源 = 地址 / DatagramSocket
  结果 = KcpConn 本身可读写

SMUX:
  资源 = 已有字节流 transport
  结果 = SmuxConn，再 open/accept 出 Stream 读写
```

这是合理的语义差，不是 API 风格差。

---

## 九、实施步骤

### Phase 1: Stream 吸收 Async I/O（~120 行）
- Stream 增加 `flush_notify`
- 实现 `kio::AsyncRead` / `AsyncWrite`（从 io.rs 搬）
- 单测：Stream 直接 read/write

### Phase 2: SmuxConnBuilder（~200 行）
- `connect(t)` / `serve(t)` + 链式配置 + `IntoFuture`
- 内部 split + 双任务驱动
- `open_stream` / `accept` 返回 `Arc<Stream>`

### Phase 3: 删除旧 API（~ -500 行）
- 删 `io.rs`
- 删旧 `client/server/new/run/spawn` 公开面
- 更新 lib.rs re-exports
- 更新 conn 测试

### Phase 4: 适配
- kcptun-client: 去掉 `with_backpressure`
- kcptun-server: `SmuxIo::new` → 直接用 Stream
- AGENTS.md / 文档示例

### 验证
```bash
cargo test -p smux-rs
# 独立路径: connect(tcp) / serve(tcp) 读写多流
cargo build --workspace
make e2e
make stress
make clippy
```

---

## 十、工作量

| 项 | 量 |
|----|----|
| 新增 Builder + Stream async | ~300 行 |
| 删除 SmuxIo + 旧入口 | ~-500 行 |
| kcptun 适配 | ~50 行 |
| **净变化** | **约 -150 行，API 面显著变小** |

---

## 十一、结论

可以，而且应该这样做：

1. **不需要 SmuxIo** — Stream 实现异步 IO 即可  
2. **Builder 构造** — 与 KcpConn 同一心智模型  
3. **独立使用更简单** — `connect(tcp).await?` / `serve(tcp).await?`  
4. **kcptun 更干净** — 高层把 KcpConn 当 transport；低阶继续用 Session，不再塞 KCP 字段进 SMUX  

推荐公开入口只留：

```rust
SmuxConn::connect(transport).…await?
SmuxConn::serve(transport).…await?
smux.open_stream() / smux.accept()
Session::*   // 低阶保留
Stream       // AsyncRead + AsyncWrite
```

---

## Implementation status (2026-07-31 Task 7)

| Item | Status | Notes |
|------|--------|-------|
| Stream AsyncRead/Write + flush_notify | **done** | `28c3d7d8` |
| Drop SmuxIo KCP `with_backpressure` | **done** | SmuxIo thin wrapper only |
| SmuxConn Builder connect/serve | **done** | `de95341b`; client/server thin wrappers kept |
| Production binary rewrite onto SmuxConn | **deferred** | binaries still drive `Session` low-level |
| AGENTS / README | **done** (Task 7) | no stale backpressure claims |
