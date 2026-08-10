// P99 / P999 round-trip latency probe for the kcp-go v5 KCP layer.
//
// Measures end-to-end echo RTT between two kcp-go endpoints over localhost UDP,
// with no kcptun / SMUX / snappy / crypto layers — the bare `UDPSession`
// (kcp-go's session API, mirroring kcp-rs's `KcpConn`), no BlockCrypt, no FEC.
//
// Subcommands:
//
//	server --port P            raw KCP echo server (accepts one or more clients)
//	bench  --port P --samples N --size B
//	                           self pair (go↔go): echo RTT, prints a RESULT line
//
// Build (offline from module cache):
//
//	cd tests/kcp-go-latency && GOPROXY=off go mod tidy && GOPROXY=off go build -o kcp-go-latency .
package main

import (
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	kcpgo "github.com/xtaci/kcp-go/v5"
)

// KCP Fast3 profile (nodelay=1, interval=10, resend=2, nc=1) — matches kcp-rs
// KcpMode::Fast3, so cross-interop settings align.
const (
	mtu         = 1350
	sndwnd      = 512
	rcvwnd      = 512
	defRPS      = 500
	defWarmup   = 5  // seconds
	defDuration = 60 // seconds
	defSize     = 1024
)

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: kcp-go-latency server|bench|closed|client [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "server":
		runServer(os.Args[2:])
	case "bench":
		runBench(os.Args[2:])
	case "closed":
		runClosed(os.Args[2:])
	case "client":
		runClient(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		os.Exit(2)
	}
}

func tune(s *kcpgo.UDPSession) {
	s.SetNoDelay(1, 10, 2, 1)
	s.SetWindowSize(sndwnd, rcvwnd)
	s.SetMtu(mtu)
}

// Echo loop: read one complete KCP message, write it back verbatim.
func echoLoop(sess *kcpgo.UDPSession) {
	defer sess.Close()
	buf := make([]byte, 65536)
	for {
		n, err := sess.Read(buf)
		if err != nil {
			return
		}
		if n > 0 {
			if _, err := sess.Write(buf[:n]); err != nil {
				return
			}
		}
	}
}

func runServer(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	port := fs.Int("port", 0, "UDP listen port")
	_ = fs.Parse(args)

	ln, err := kcpgo.ListenWithOptions(fmt.Sprintf("127.0.0.1:%d", *port), nil, 0, 0)
	if err != nil {
		fatal(err)
	}
	fmt.Fprintf(os.Stderr, "kcp-go echo server listening on %s\n", ln.Addr())
	for {
		sess, err := ln.AcceptKCP()
		if err != nil {
			fatal(err)
		}
		tune(sess)
		go echoLoop(sess)
	}
}

// measureOpen runs the open-model fixed-rate latency test: sends `rps`
// requests/sec on a strict cadence (never waiting for a response), matches each
// echo to its send time in arrival order, drops the warm-up phase, and collects
// raw per-request latencies (µs) across the measurement phase. Returns
// (latencies_us, measure_sends, measure_ok).
func measureOpen(client *kcpgo.UDPSession, rps int, warmup, duration time.Duration, size int) ([]float64, int, int, int) {
	interval := time.Duration(float64(time.Second) / float64(rps))
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = 0x5A
	}
	rx := make([]byte, size)
	rxFilled := 0
	var us []float64
	warmupEnd := time.Now().Add(warmup)
	measureEnd := warmupEnd.Add(duration)
	sends, ok, skipped := 0, 0, 0

	// Lag past which the sender sheds its backlog instead of catching up.
	// Must exceed scheduler jitter so ordinary wake noise is absorbed, and stay
	// well under a stalled-write timescale so a real block still sheds.
	const maxLag = 50 * time.Millisecond

	// Sender runs in its own goroutine so a Write that blocks on a full send
	// window cannot stop the reader — in an in-process echo topology a combined
	// loop deadlocks outright, since the peer only drains once we read.
	var mu sync.Mutex
	var inFlight []time.Time
	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)
		nextSend := time.Now()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if now := time.Now(); now.Before(nextSend) {
				time.Sleep(nextSend.Sub(now))
				continue
			}
			if _, err := client.Write(payload); err != nil {
				return
			}
			sentAt := time.Now()
			mu.Lock()
			inFlight = append(inFlight, sentAt)
			if !sentAt.Before(warmupEnd) {
				sends++
			}
			nextSend = nextSend.Add(interval)
			// Resync only on a genuine backlog: a stalled write makes lag
			// compound without bound, whereas scheduler jitter does not, and
			// folding the two together would discard throughput to measure an
			// artifact. `skipped` keeps real drops visible.
			if lag := time.Since(nextSend); lag > maxLag {
				skipped++
				nextSend = time.Now().Add(interval)
			}
			mu.Unlock()
		}
	}()

	for {
		// Bounded poll so the loop still notices measureEnd while idle.
		_ = client.SetReadDeadline(time.Now().Add(100 * time.Microsecond))
		n, err := client.Read(rx[rxFilled:])
		if err == nil && n > 0 {
			rxFilled += n
			if rxFilled == size {
				rxFilled = 0
				mu.Lock()
				if len(inFlight) > 0 {
					t0 := inFlight[0]
					inFlight = inFlight[1:]
					if !t0.Before(warmupEnd) {
						us = append(us, float64(time.Since(t0).Microseconds()))
						ok++
					}
				}
				mu.Unlock()
			}
		}

		if !time.Now().Before(measureEnd) {
			break
		}
	}

	close(stop)
	// Unblock a sender parked in Write on a full window so it can observe stop.
	_ = client.SetWriteDeadline(time.Now())
	<-done

	mu.Lock()
	defer mu.Unlock()
	return us, sends, ok, skipped
}

// measureClosed runs the closed-loop concurrency model: maintain exactly
// `concurrency` in-flight requests. When a slot frees up (response received),
// immediately send the next. Both implementations run at their own max
// sustainable speed, so throughput and latency are directly comparable.
//
// KCP is an ordered stream — responses arrive in send order, so we match
// each echo to the oldest in-flight request (FIFO).
func measureClosed(client *kcpgo.UDPSession, concurrency int, warmup, duration time.Duration, size int) ([]float64, int, int) {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = 0x5A
	}
	rx := make([]byte, size)
	rxFilled := 0
	var inFlight []time.Time
	var us []float64
	warmupEnd := time.Now().Add(warmup)
	measureEnd := warmupEnd.Add(duration)
	sends, ok := 0, 0

	for {
		// Fill up to `concurrency` in-flight requests.
		for len(inFlight) < concurrency {
			if _, err := client.Write(payload); err != nil {
				return us, sends, ok
			}
			sentAt := time.Now()
			inFlight = append(inFlight, sentAt)
			if !sentAt.Before(warmupEnd) {
				sends++
			}
		}

		// Read one response chunk (10s deadline).
		_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
		n, err := client.Read(rx[rxFilled:])
		if err != nil {
			return us, sends, ok
		}
		if n == 0 {
			continue
		}
		rxFilled += n
		for rxFilled >= size {
			rxFilled -= size
			if len(inFlight) > 0 {
				t0 := inFlight[0]
				inFlight = inFlight[1:]
				if !t0.Before(warmupEnd) {
					us = append(us, float64(time.Since(t0).Microseconds()))
					ok++
				}
			}
		}

		now := time.Now()
		if !now.Before(measureEnd) {
			break
		}
	}
	return us, sends, ok
}

func runBench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	port := fs.Int("port", 0, "UDP listen port (server side)")
	rps := fs.Int("rps", defRPS, "fixed request rate (open model)")
	warmup := fs.Int("warmup", defWarmup, "warm-up seconds (excluded)")
	duration := fs.Int("duration", defDuration, "measurement seconds")
	size := fs.Int("size", defSize, "payload size in bytes")
	_ = fs.Parse(args)

	// Echo server on :port; the kcp-go listener auto-adopts the client conv.
	ln, err := kcpgo.ListenWithOptions(fmt.Sprintf("127.0.0.1:%d", *port), nil, 0, 0)
	if err != nil {
		fatal(err)
	}
	go func() {
		sess, err := ln.AcceptKCP()
		if err != nil {
			return
		}
		tune(sess)
		echoLoop(sess)
	}()

	client, err := kcpgo.DialWithOptions(fmt.Sprintf("127.0.0.1:%d", *port), nil, 0, 0)
	if err != nil {
		fatal(err)
	}
	tune(client)

	us, sends, ok, skipped := measureOpen(client, *rps, time.Duration(*warmup)*time.Second, time.Duration(*duration)*time.Second, *size)
	client.Close()

	fmt.Fprintf(os.Stderr, "[go-go] warmup=%ds duration=%ds rps=%d measure_sends=%d measure_ok=%d shed=%d\n", *warmup, *duration, *rps, sends, ok, skipped)
	report("go-go", sends, ok, *size, *rps, us, skipped)
}

// runClosed runs the closed-loop concurrency benchmark: N in-flight requests,
// each implementation at its own max sustainable speed.
func runClosed(args []string) {
	fs := flag.NewFlagSet("closed", flag.ExitOnError)
	port := fs.Int("port", 0, "UDP listen port (server side)")
	concurrency := fs.Int("concurrency", 1, "in-flight requests (closed-loop)")
	warmup := fs.Int("warmup", defWarmup, "warm-up seconds (excluded)")
	duration := fs.Int("duration", defDuration, "measurement seconds")
	size := fs.Int("size", defSize, "payload size in bytes")
	userMtu := fs.Int("mtu", mtu, "KCP MTU (larger = fewer fragments)")
	_ = fs.Parse(args)

	ln, err := kcpgo.ListenWithOptions(fmt.Sprintf("127.0.0.1:%d", *port), nil, 0, 0)
	if err != nil {
		fatal(err)
	}
	tuneMtu := func(s *kcpgo.UDPSession) {
		s.SetNoDelay(1, 10, 2, 1)
		s.SetWindowSize(sndwnd, rcvwnd)
		s.SetMtu(*userMtu)
	}
	go func() {
		sess, err := ln.AcceptKCP()
		if err != nil {
			return
		}
		tuneMtu(sess)
		echoLoop(sess)
	}()

	client, err := kcpgo.DialWithOptions(fmt.Sprintf("127.0.0.1:%d", *port), nil, 0, 0)
	if err != nil {
		fatal(err)
	}
	tuneMtu(client)

	us, sends, ok := measureClosed(client, *concurrency, time.Duration(*warmup)*time.Second, time.Duration(*duration)*time.Second, *size)
	client.Close()

	// Actual throughput = completed requests / measurement duration.
	actualRPS := 0
	if *duration > 0 {
		actualRPS = int(math.Round(float64(ok) / float64(*duration)))
	}
	fmt.Fprintf(os.Stderr, "[go-go] warmup=%ds duration=%ds concurrency=%d measure_sends=%d measure_ok=%d\n", *warmup, *duration, *concurrency, sends, ok)
	report("go-go", sends, ok, *size, actualRPS, us, 0)
}

// report computes percentiles over the microsecond samples and prints the
// machine-readable RESULT line plus a human table.
func report(combo string, samples, ok, size, rps int, us []float64, shed int) {
	if ok == 0 {
		fmt.Fprintln(os.Stderr, "no successful samples")
		os.Exit(1)
	}
	sort.Float64s(us)
	n := len(us)
	idx := func(q float64) int {
		i := int(math.Ceil(float64(n) * q))
		if i > n {
			i = n
		}
		if i < 1 {
			i = 1
		}
		return i - 1
	}
	p50, p90, p99, p999 := us[idx(0.50)], us[idx(0.90)], us[idx(0.99)], us[idx(0.999)]
	var sum float64
	for _, v := range us {
		sum += v
	}
	avg := sum / float64(n)

	fmt.Printf("RESULT combo=%s samples=%d ok=%d shed=%d size=%d rps=%d p50_us=%.1f p90_us=%.1f p99_us=%.1f p999_us=%.1f avg_us=%.1f min_us=%.1f max_us=%.1f\n",
		combo, samples, ok, shed, size, rps, p50, p90, p99, p999, avg, us[0], us[n-1])
	fmt.Printf("  samples=%d ok=%d shed=%d payload=%dB rps=%d  p50=%.2fms p90=%.2fms p99=%.2fms p999=%.2fms avg=%.2fms min=%.2fms max=%.2fms\n",
		samples, ok, shed, size, rps, p50/1000, p90/1000, p99/1000, p999/1000, avg/1000, us[0]/1000, us[n-1]/1000)
}

// runClient measures echo RTT against an external KCP echo server (e.g. the
// kcp-rs `server` mode) — the reverse of the `bench` direction. The conv must
// match the server's (kcp-rs KcpListener default 0x00C0_FFEE).
func runClient(args []string) {
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	addr := fs.String("addr", "", "server host:port")
	rps := fs.Int("rps", defRPS, "fixed request rate (open model)")
	warmup := fs.Int("warmup", defWarmup, "warm-up seconds (excluded)")
	duration := fs.Int("duration", defDuration, "measurement seconds")
	size := fs.Int("size", defSize, "payload size in bytes")
	conv := fs.Uint("conv", 0x00C0_FFEE, "conv (must match server)")
	_ = fs.Parse(args)
	if *addr == "" {
		fatal(fmt.Errorf("client mode requires --addr"))
	}
	raddr, err := net.ResolveUDPAddr("udp", *addr)
	if err != nil {
		fatal(err)
	}
	udp, err := net.ListenUDP("udp", nil)
	if err != nil {
		fatal(err)
	}
	client, err := kcpgo.NewConn3(uint32(*conv), raddr, nil, 0, 0, udp)
	if err != nil {
		fatal(err)
	}
	tune(client)

	us, sends, ok, skipped := measureOpen(client, *rps, time.Duration(*warmup)*time.Second, time.Duration(*duration)*time.Second, *size)
	client.Close()

	fmt.Fprintf(os.Stderr, "[go-rust] warmup=%ds duration=%ds rps=%d measure_sends=%d measure_ok=%d shed=%d\n", *warmup, *duration, *rps, sends, ok, skipped)
	report("go-rust", sends, ok, *size, *rps, us, skipped)
}
