use std::io;

/// A persistent Snappy framing stream decoder that maintains state across
/// incremental data feeds. This is required because KCP stream mode can
/// split snappy frames across multiple recv() calls, and the snappy
/// framing format's stream header (0xff...) only appears once at the start.
pub struct SnappyStreamDecoder {
    buf: Vec<u8>,
    pos: usize,
    hdr_ok: bool,
    /// Reused across chunks — Decoder::new() is cheap but not free under
    /// high-throughput session-level compression.
    decoder: snap::raw::Decoder,
}

impl Default for SnappyStreamDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl SnappyStreamDecoder {
    pub fn new() -> Self {
        SnappyStreamDecoder {
            buf: Vec::new(),
            pos: 0,
            hdr_ok: false,
            decoder: snap::raw::Decoder::new(),
        }
    }
    pub fn feed(&mut self, data: &[u8]) -> io::Result<Vec<u8>> {
        self.buf.extend_from_slice(data);
        if self.pos > 65536 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        let mut out = Vec::new();
        loop {
            let avail = self.buf.len() - self.pos;
            // Skip stream identifier (0xFF 0x06 0x00 0x00 "sNaPpY")
            if !self.hdr_ok {
                if avail < 10 {
                    break;
                }
                if self.buf[self.pos] != 0xff || &self.buf[self.pos + 4..self.pos + 10] != b"sNaPpY"
                {
                    // Not a stream identifier — skip one byte and try to resync
                    self.pos += 1;
                    continue;
                }
                self.pos += 10;
                self.hdr_ok = true;
                continue;
            }
            // Read chunk header: [type 1B][length 3B LE]
            if avail < 4 {
                break;
            }
            let ct = self.buf[self.pos];
            let chunk_len = u32::from_le_bytes([
                self.buf[self.pos + 1],
                self.buf[self.pos + 2],
                self.buf[self.pos + 3],
                0,
            ]) as usize;
            if chunk_len > 16_777_216 {
                self.pos += 4 + chunk_len.min(avail - 4);
                continue;
            }
            if 4 + chunk_len > avail {
                break;
            }
            let chunk_data = &self.buf[self.pos + 4..self.pos + 4 + chunk_len];
            self.pos += 4 + chunk_len;
            match ct {
                0x00 => {
                    // Compressed chunk: [CRC32 4B][snappy block]
                    if chunk_data.len() < 4 {
                        continue;
                    }
                    let snappy_data = &chunk_data[4..];
                    match self.decoder.decompress_vec(snappy_data) {
                        Ok(d) => out.extend(d),
                        Err(_) => continue,
                    }
                }
                0x01 => {
                    // Uncompressed chunk: [CRC32 4B][raw data]
                    if chunk_data.len() >= 4 {
                        out.extend_from_slice(&chunk_data[4..]);
                    }
                }
                _ => {}
            }
        }
        Ok(out)
    }
}
