# bench/profiles

Generated pprof artifacts live here. **Do not commit large binary profiles.**
Only `README.md`, `HOTSPOTS.md`, and `.gitkeep` are tracked.

## Go-compatible pprof (protobuf)

CPU + heap/allocs profiles for `go tool pprof`.

```bash
make profiling-bins
make profile            # Rust → bench/profiles/rust-*.pb (+ -heap.pb, -allocs.pb)
make profile-go         # Go   → bench/profiles/go-*.pb.gz
```

Artifacts (gitignored — regenerate locally):

| File pattern             | Description                     |
|--------------------------|---------------------------------|
| `rust-*.pb`              | Rust CPU (Go pprof protobuf)    |
| `rust-*-heap.pb`         | Rust heap (inuse)               |
| `rust-*-allocs.pb`       | Rust cumulative allocs          |
| `go-*.pb.gz`             | Go kcptun CPU                   |

View:
```bash
go tool pprof -http=:0 bench/profiles/rust-server-aes-*.pb
go tool pprof -http=:0 bench/profiles/rust-server-aes-*-heap.pb
```

Interpretation notes (committed): `HOTSPOTS.md`.
