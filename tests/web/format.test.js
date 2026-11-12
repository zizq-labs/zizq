// Tests for web/format.js. The examples match `zizq top`'s, in
// src/commands/top/ui/format.rs, so both show durations the same way.

import assert from "node:assert/strict";
import { test } from "node:test";

import {
  formatClock,
  formatDue,
  formatDuration,
  formatElapsed,
  formatTimestamp,
} from "../../web/format.js";

test("formatDuration", () => {
  assert.equal(formatDuration(0), "0ms");
  assert.equal(formatDuration(500), "500ms");
  assert.equal(formatDuration(999), "999ms");
  assert.equal(formatDuration(1_000), "1s");
  assert.equal(formatDuration(5_200), "5s");
  assert.equal(formatDuration(60_000), "1m");
  assert.equal(formatDuration(138_000), "2m18s");
  assert.equal(formatDuration(3_600_000), "1h");
  assert.equal(formatDuration(3_960_000), "1h6m");
  assert.equal(formatDuration(86_400_000), "1d");
  assert.equal(formatDuration(190_800_000), "2d5h");
});

test("formatElapsed", () => {
  const now = 10_000_000;
  assert.equal(formatElapsed(now - 500, now), "< 1s");
  assert.equal(formatElapsed(now - 5_200, now), "5s");
  assert.equal(formatElapsed(now - 138_000, now), "2m18s");
  assert.equal(formatElapsed(now - 3_960_000, now), "1h6m");
  assert.equal(formatElapsed(0, now), "-");
});

test("formatDue", () => {
  const now = 10_000_000;
  assert.equal(formatDue(now + 232_000, now), "3m52s");
  assert.equal(formatDue(now + 5_000, now), "5s");
  assert.equal(formatDue(now - 60_000, now), "1m ago");
  assert.equal(formatDue(now - 500, now), "< 1s");
  assert.equal(formatDue(now + 500, now), "< 1s");
  assert.equal(formatDue(0, now), "-");
});

test("formatClock", () => {
  assert.equal(formatClock(0), "00:00:00");
  assert.equal(formatClock(3_723_000), "01:02:03");
  assert.equal(formatClock(90_061_000), "1d 01:01:01");
});

test("formatTimestamp", () => {
  const ms = new Date(2026, 10, 11, 14, 3, 22).getTime();
  assert.equal(formatTimestamp(ms), "2026-11-11 14:03:22");
  assert.equal(formatTimestamp(new Date(2026, 0, 5, 9, 7, 1).getTime()), "2026-01-05 09:07:01");
});
