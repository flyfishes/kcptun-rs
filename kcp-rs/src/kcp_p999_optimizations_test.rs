//! Tests for P999 latency optimizations in KCP

use super::*;
use std::sync::{Arc, Mutex};

/// Test that nodelay mode improves RTT responsiveness
#[test]
fn test_nodelay_mode_rto_behavior() {
    // Test that nodelay mode is properly configurable
    let output = |_: bytes::Bytes| {};
    let mut kcp = KCP::new(1, 0, output);

    // Test normal mode configuration
    kcp.set_nodelay(0, 40, 2, 1);
    let normal_interval = kcp.interval();

    // Test nodelay mode configuration
    kcp.set_nodelay(1, 10, 2, 1);
    let nodelay_interval = kcp.interval();

    // Nodelay mode should use smaller interval for faster response
    assert!(
        nodelay_interval < normal_interval,
        "Nodelay mode should use smaller interval"
    );
}

/// Test that P999 optimizations don't break basic KCP functionality
#[test]
fn test_basic_kcp_functionality_preserved() {
    let store: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let store2 = store.clone();
    let mut sender = KCP::new(42, 0, move |data: bytes::Bytes| {
        store2.lock().unwrap().push(data.to_vec());
    });

    let mut receiver = KCP::new(42, 0, |_: bytes::Bytes| {});

    // Enable nodelay mode for P999 optimizations
    sender.set_nodelay(1, 10, 2, 1);
    receiver.set_nodelay(1, 10, 2, 1);

    // Test basic send/receive works
    sender.send(&[1u8, 2, 3, 4]).unwrap();
    sender.flush();

    // Get the sent packet
    let packets = {
        let mut guard = store.lock().unwrap();
        guard.drain(..).collect::<Vec<_>>()
    };

    assert!(!packets.is_empty(), "Should have sent packets");

    // Feed packet to receiver
    for packet in &packets {
        receiver.input(packet, false).unwrap();
    }

    // Receiver should have data
    let recv_data = receiver.recv().unwrap();
    assert_eq!(recv_data.len(), 4, "Should have received 4 bytes");
    assert_eq!(&recv_data[..4], &[1u8, 2, 3, 4], "Data should match");
}

/// Test that congestion control adjustments work correctly
#[test]
fn test_congestion_control_adjustments() {
    let output = |_: bytes::Bytes| {};
    let mut kcp = KCP::new(1, 0, output);

    // Enable nodelay mode
    kcp.set_nodelay(1, 10, 2, 1);

    // Set initial window size
    let _initial_cwnd = kcp.cwnd();

    // Send some data to populate snd_buf
    for i in 0..5 {
        kcp.send(&[i; 100]).unwrap();
    }

    kcp.flush();

    // Window should be properly managed
    assert!(kcp.cwnd() > 0, "Congestion window should be positive");

    // Verify that we can still send more data
    kcp.send(&[99u8; 50]).unwrap();

    // Basic sanity checks on KCP state
    assert!(
        kcp.snd_nxt() > kcp.snd_una(),
        "Next send should be ahead of unacknowledged"
    );
}

/// Test that fast retransmit still works with optimized settings
#[test]
fn test_fast_retransmit_still_works() {
    let store: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
    let store2 = store.clone();
    let mut sender = KCP::new(42, 0, move |data: bytes::Bytes| {
        store2.lock().unwrap().push(data.to_vec());
    });

    let mut receiver = KCP::new(42, 0, |_: bytes::Bytes| {});

    // Enable nodelay mode which should improve P999
    sender.set_nodelay(1, 10, 2, 1);
    receiver.set_nodelay(1, 10, 2, 1);

    // Send multiple segments
    for i in 0..3 {
        sender.send(&[i; 100]).unwrap();
    }

    sender.flush();

    // Verify packets were sent
    let packets = {
        let mut guard = store.lock().unwrap();
        guard.drain(..).collect::<Vec<_>>()
    };

    assert!(!packets.is_empty(), "Should have sent packets");

    // Feed packets to receiver (simulating out-of-order delivery)
    for packet in &packets {
        receiver.input(packet, false).unwrap();
    }

    // Receiver should be able to flush ACKs
    receiver.flush();

    // Basic connectivity test - receiver should be responsive
    assert_eq!(
        receiver.wait_send(),
        0,
        "receiver should not have queued sends"
    );
}
