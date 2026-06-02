// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Pure formatting helpers used across the UI components.
//!
//! Nothing in here touches the `App`, ratatui types, or any I/O — these
//! functions exist so the rest of the UI code can pretend strings just
//! arrive pre-formatted.

/// Right-pad `s` with spaces to at least `width` columns.
pub(super) fn pad_to(s: &str, width: u16) -> String {
    format!("{:width$}", s, width = width as usize)
}

/// Format a millisecond timestamp as a local date/time with UTC offset.
pub(super) fn format_timestamp(ms: u64) -> String {
    use chrono::{Local, TimeZone};
    let secs = (ms / 1_000) as i64;
    let nsecs = ((ms % 1_000) * 1_000_000) as u32;
    match Local.timestamp_opt(secs, nsecs) {
        chrono::LocalResult::Single(dt) => dt.format("%Y-%m-%d %H:%M:%S %:z").to_string(),
        _ => format!("{ms}"),
    }
}

/// Format time until a scheduled job is due, or how overdue it is.
pub(super) fn format_due(ready_at: u64, now_ms: u64) -> String {
    if ready_at == 0 {
        return "-".to_string();
    }
    let diff = ready_at.abs_diff(now_ms);
    if diff < 1_000 {
        return "< 1s".to_string();
    }
    if ready_at >= now_ms {
        format_duration_ms(diff)
    } else {
        let elapsed = format_duration_ms(diff);
        format!("{elapsed} ago")
    }
}

/// Format elapsed time since a timestamp (e.g. "5s", "2m18s").
pub(super) fn format_elapsed(timestamp_ms: u64, now_ms: u64) -> String {
    if timestamp_ms == 0 {
        return "-".to_string();
    }
    let diff_ms = now_ms.saturating_sub(timestamp_ms);
    if diff_ms < 1_000 {
        return "< 1s".to_string();
    }
    format_duration_ms(diff_ms)
}

/// Format a duration in milliseconds as a human-readable compound string.
pub(super) fn format_duration_ms(ms: u64) -> String {
    if ms < 1_000 {
        format!("{ms}ms")
    } else if ms < 60_000 {
        let s = ms / 1_000;
        format!("{s}s")
    } else if ms < 3_600_000 {
        let m = ms / 60_000;
        let s = (ms % 60_000) / 1_000;
        if s > 0 {
            format!("{m}m{s}s")
        } else {
            format!("{m}m")
        }
    } else if ms < 86_400_000 {
        let h = ms / 3_600_000;
        let m = (ms % 3_600_000) / 60_000;
        if m > 0 {
            format!("{h}h{m}m")
        } else {
            format!("{h}h")
        }
    } else {
        let d = ms / 86_400_000;
        let h = (ms % 86_400_000) / 3_600_000;
        if h > 0 {
            format!("{d}d{h}h")
        } else {
            format!("{d}d")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_elapsed_examples() {
        let now = 10_000_000u64;
        assert_eq!(format_elapsed(now - 500, now), "< 1s");
        assert_eq!(format_elapsed(now - 5_200, now), "5s");
        assert_eq!(format_elapsed(now - 138_000, now), "2m18s");
        assert_eq!(format_elapsed(now - 3_960_000, now), "1h6m");
        assert_eq!(format_elapsed(0, now), "-");
    }

    #[test]
    fn format_duration_ms_examples() {
        assert_eq!(format_duration_ms(0), "0ms");
        assert_eq!(format_duration_ms(500), "500ms");
        assert_eq!(format_duration_ms(999), "999ms");
        assert_eq!(format_duration_ms(1_000), "1s");
        assert_eq!(format_duration_ms(5_200), "5s");
        assert_eq!(format_duration_ms(60_000), "1m");
        assert_eq!(format_duration_ms(138_000), "2m18s");
        assert_eq!(format_duration_ms(3_600_000), "1h");
        assert_eq!(format_duration_ms(3_960_000), "1h6m");
        assert_eq!(format_duration_ms(86_400_000), "1d");
        assert_eq!(format_duration_ms(190_800_000), "2d5h");
    }

    #[test]
    fn format_due_examples() {
        let now = 10_000_000u64;
        // Future: shows remaining time.
        assert_eq!(format_due(now + 232_000, now), "3m52s");
        assert_eq!(format_due(now + 5_000, now), "5s");
        // Overdue: shows elapsed with "ago" suffix.
        assert_eq!(format_due(now - 60_000, now), "1m ago");
        assert_eq!(format_due(now - 500, now), "< 1s");
        // Sub-second future rounds to "< 1s".
        assert_eq!(format_due(now + 500, now), "< 1s");
        // Zero timestamp.
        assert_eq!(format_due(0, now), "-");
    }
}
