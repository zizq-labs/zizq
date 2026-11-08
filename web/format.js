// Formatting for durations and times, matching `zizq top`.

// A clock duration, e.g. "04:12:09", or "3d 04:12:09" past a day.
export function formatClock(ms) {
  const total = Math.floor(ms / 1000);
  const days = Math.floor(total / 86400);
  const pad = (n) => String(n).padStart(2, "0");
  const hms = [
    pad(Math.floor((total % 86400) / 3600)),
    pad(Math.floor((total % 3600) / 60)),
    pad(total % 60),
  ].join(":");
  return days > 0 ? `${days}d ${hms}` : hms;
}

// A compact duration in its two largest units, e.g. "2m18s" or "1d4h".
export function formatDuration(ms) {
  const units = [
    [86_400_000, "d"],
    [3_600_000, "h"],
    [60_000, "m"],
    [1_000, "s"],
  ];
  if (ms < 1_000) {
    return `${ms}ms`;
  }
  const i = units.findIndex(([size]) => ms >= size);
  const [size, unit] = units[i];
  const whole = Math.floor(ms / size);
  if (i === units.length - 1) {
    return `${whole}${unit}`;
  }
  const [nextSize, nextUnit] = units[i + 1];
  const rest = Math.floor((ms % size) / nextSize);
  return rest > 0 ? `${whole}${unit}${rest}${nextUnit}` : `${whole}${unit}`;
}

// Time since `timestampMs`, e.g. "5s".
export function formatElapsed(timestampMs, nowMs) {
  if (!timestampMs) {
    return "-";
  }
  const diff = Math.max(0, nowMs - timestampMs);
  return diff < 1_000 ? "< 1s" : formatDuration(diff);
}

// Time until `readyAtMs`, or how long ago it passed, e.g. "5m" or "5m ago".
export function formatDue(readyAtMs, nowMs) {
  if (!readyAtMs) {
    return "-";
  }
  const diff = Math.abs(readyAtMs - nowMs);
  if (diff < 1_000) {
    return "< 1s";
  }
  return readyAtMs >= nowMs ? formatDuration(diff) : `${formatDuration(diff)} ago`;
}
