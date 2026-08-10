//! Periodic KCP SNMP CSV logger.

use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use log::error;

/// Expand strftime-style format specifiers in `path` using the current UTC time.
///
/// Supported specifiers: %Y (year), %m (month), %d (day), %H (hour),
/// %M (minute), %S (second). Matches Go kcptun's `time.Now().Format(logfile)`
/// behavior for SNMP log paths.
fn expand_time_format(path: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    // Convert to UTC broken-down time using the civil calendar algorithm.
    let (year, month, day, hour, minute, second) = civil_from_secs(secs);

    path.replace("%Y", &format!("{:04}", year))
        .replace("%m", &format!("{:02}", month))
        .replace("%d", &format!("{:02}", day))
        .replace("%H", &format!("{:02}", hour))
        .replace("%M", &format!("{:02}", minute))
        .replace("%S", &format!("{:02}", second))
}

/// Simple civil calendar conversion from Unix seconds to (year, month, day, hour, min, sec).
/// Based on the Howard Hinnant algorithm.
fn civil_from_secs(secs: u64) -> (i32, u32, u32, u32, u32, u32) {
    let days = (secs / 86400) as i64;
    let time_of_day = secs % 86400;
    let hour = (time_of_day / 3600) as u32;
    let minute = ((time_of_day % 3600) / 60) as u32;
    let second = (time_of_day % 60) as u32;

    // Shift epoch from 1970-01-01 to 0000-03-01 (civil calendar epoch).
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    (y as i32, m, d, hour, minute, second)
}

/// Rust-only SNMP columns (not in Go `ToSlice` / CSV header).
///
/// Written to a sidecar `<path>.rustobs` so H2 / offload investigations can
/// read `EncryptInline` / `EncryptOffload` without changing Go-compatible CSV.
const RUST_OBS_HEADER: &str =
    "timestamp,EmptyFlush,EncryptInline,EncryptOffload,DecryptOffloadSkipped,WriteInlineSends,WriteFlushSends,InputUrgentSends";

fn rust_obs_path(go_csv_path: &str) -> String {
    format!("{go_csv_path}.rustobs")
}

fn write_rust_obs_line(path: &str, ts: u64) {
    let snmp = &kcp_rs::DEFAULT_SNMP;
    let line = format!(
        "{},{},{},{},{},{},{},{}",
        ts,
        snmp.empty_flush.load(Ordering::Acquire),
        snmp.encrypt_inline.load(Ordering::Acquire),
        snmp.encrypt_offload.load(Ordering::Acquire),
        snmp.decrypt_offload_skipped.load(Ordering::Acquire),
        snmp.write_inline_sends.load(Ordering::Acquire),
        snmp.write_flush_sends.load(Ordering::Acquire),
        snmp.input_urgent_sends.load(Ordering::Acquire),
    );
    let mut f = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to open Rust SNMP obs file '{}': {}", path, e);
            return;
        }
    };
    if let Ok(meta) = f.metadata() {
        if meta.len() == 0 {
            if let Err(e) = writeln!(f, "{RUST_OBS_HEADER}") {
                error!("Rust SNMP obs write error: {}", e);
                return;
            }
        }
    }
    if let Err(e) = writeln!(f, "{line}") {
        error!("Rust SNMP obs write error: {}", e);
    }
}

/// Periodically log KCP SNMP statistics to a CSV file.
///
/// Also appends a Go-incompatible sidecar at `<path>.rustobs` with
/// EmptyFlush / EncryptInline / EncryptOffload / DecryptOffloadSkipped
/// (needed for encrypt offload ratio in H2 verification).
pub async fn snmp_logger(path: String, period: Duration, stop: Arc<AtomicBool>) {
    kio::sleep_ms(period.as_millis() as u64).await;

    // Expand time format specifiers in the path (matching Go's time.Now().Format).
    let expanded_path = expand_time_format(&path);

    let file = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&expanded_path)
    {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to open SNMP log file '{}': {}", expanded_path, e);
            return;
        }
    };
    let mut writer = std::io::BufWriter::new(file);

    let headers = kcp_rs::SNMP::header();
    if let Err(e) = writeln!(writer, "timestamp,{}", headers.join(",")) {
        error!("SNMP log write error: {}", e);
        return;
    }
    let _ = writer.flush();
    // Bootstrap rustobs header early so short runs still have a parseable file.
    {
        let obs = rust_obs_path(&expanded_path);
        let ts0 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        write_rust_obs_line(&obs, ts0);
    }

    while !stop.load(Ordering::Relaxed) {
        kio::sleep_ms(period.as_millis() as u64).await;
        if stop.load(Ordering::Relaxed) {
            break;
        }
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Re-open the file each time with expanded time format (matching Go's
        // per-tick time.Now().Format(logfile) behavior for log rotation).
        let expanded_path = expand_time_format(&path);
        let mut f = match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&expanded_path)
        {
            Ok(f) => f,
            Err(e) => {
                error!("Failed to open SNMP log file '{}': {}", expanded_path, e);
                return;
            }
        };

        // Read process-wide counters updated by KCP hot paths.
        let values = kcp_rs::DEFAULT_SNMP.to_slice();
        let headers = kcp_rs::SNMP::header();
        // Write header if file is empty or just created.
        if let Ok(meta) = f.metadata() {
            if meta.len() == 0 {
                if let Err(e) = writeln!(f, "timestamp,{}", headers.join(",")) {
                    error!("SNMP log write error: {}", e);
                }
            }
        }
        if let Err(e) = writeln!(f, "{},{}", ts, values.join(",")) {
            error!("SNMP log write error: {}", e);
        }

        write_rust_obs_line(&rust_obs_path(&expanded_path), ts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_civil_from_secs_epoch() {
        // Unix epoch: 1970-01-01 00:00:00 UTC
        let (y, m, d, h, min, s) = civil_from_secs(0);
        assert_eq!((y, m, d), (1970, 1, 1));
        assert_eq!((h, min, s), (0, 0, 0));
    }

    #[test]
    fn test_civil_from_secs_known() {
        // 2024-06-15 12:30:45 UTC = 1718454645
        let ts: u64 = 1718454645;
        let (y, m, d, h, min, s) = civil_from_secs(ts);
        assert_eq!((y, m, d), (2024, 6, 15));
        assert_eq!((h, min, s), (12, 30, 45));
    }

    #[test]
    fn test_civil_from_secs_leap_year() {
        // 2024-02-29 00:00:00 UTC (2024 is a leap year)
        // 2024-03-01 00:00:00 UTC = 1709251200
        // Days in Jan(31) + Feb(29) = 60 days from 1970-01-01
        // 1970 to 2024 = 54 years, with leap years: 1972,76,80,84,88,92,96,2000,04,08,12,16,20,24 = 14 leap days
        // 54*365 + 14 + 60 = 19710 + 14 + 60 = 19784 days
        // But let's just compute it:
        let feb29_2024: u64 = 1709164800; // 2024-02-29 00:00:00 UTC
        let (y, m, d, h, min, s) = civil_from_secs(feb29_2024);
        assert_eq!((y, m, d), (2024, 2, 29));
        assert_eq!((h, min, s), (0, 0, 0));
    }

    #[test]
    fn test_civil_from_secs_year_2000() {
        // 2000-01-01 00:00:00 UTC (2000 was a leap year)
        // 30 years * 365 + 7 leap days = 10957 days
        let y2k: u64 = 946684800;
        let (y, m, d, h, min, s) = civil_from_secs(y2k);
        assert_eq!((y, m, d), (2000, 1, 1));
        assert_eq!((h, min, s), (0, 0, 0));
    }

    #[test]
    fn test_expand_time_format_epoch() {
        let result = expand_time_format("snmp-%Y%m%d.log");
        // At epoch 0, the result should be "snmp-19700101.log"
        // But expand_time_format uses SystemTime::now(), so we can't test exact values.
        // Instead, check that format specifiers are replaced (no % left).
        assert!(!result.contains("%Y"));
        assert!(!result.contains("%m"));
        assert!(!result.contains("%d"));
        assert!(result.starts_with("snmp-"));
        assert!(result.ends_with(".log"));
        // The date part should be 8 digits
        let date_part = &result[5..13];
        assert!(date_part.chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn test_expand_time_format_mixed_literals() {
        let result = expand_time_format("/var/log/kcptun/%Y/%m/snmp-%d.log");
        assert!(!result.contains('%'), "all % specifiers must be replaced");
        assert!(result.starts_with("/var/log/kcptun/"));
        assert!(result.ends_with(".log"));
    }

    #[test]
    fn test_expand_time_format_hms() {
        let result = expand_time_format("dump-%H%M%S.csv");
        assert!(!result.contains('%'), "all % specifiers must be replaced");
        assert!(result.starts_with("dump-"));
        assert!(result.ends_with(".csv"));
        let time_part = &result[5..11];
        assert!(time_part.chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn test_civil_vs_expand_consistency() {
        // Verify that the same timestamp produces consistent results
        let secs: u64 = 1718454645; // 2024-06-15 12:30:45
        let (y, m, d, h, min, s) = civil_from_secs(secs);
        let formatted = format!("{:04}{:02}{:02}-{:02}{:02}{:02}", y, m, d, h, min, s);
        assert_eq!(formatted, "20240615-123045");
    }
}
