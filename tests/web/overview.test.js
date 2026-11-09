// Tests for web/overview.js.

import assert from "node:assert/strict";
import { test } from "node:test";

import { marker, scrubTarget, topAtFraction } from "../../web/overview.js";

const close = (actual, expected, message) =>
  assert.ok(Math.abs(actual - expected) < 1e-9, `${message}: ${actual} != ${expected}`);

test("the marker covers the rows shown, as a share of the list", () => {
  const result = marker(1000, 250, 100);

  close(result.start, 0.25, "start");
  close(result.width, 0.1, "width");
});

test("the marker is clamped to the list when its count has shrunk", () => {
  const result = marker(100, 90, 50);

  close(result.start, 0.9, "start");
  close(result.width, 0.1, "width");
});

test("an empty list is marked whole", () => {
  assert.deepEqual(marker(0, 0, 0), { start: 0, width: 1 });
});

test("a list with no rows loaded yet is marked whole", () => {
  assert.deepEqual(marker(10, 0, 0), { start: 0, width: 1 });
});

const bar = [
  { name: "in_flight", left: 0, width: 10 },
  { name: "ready", left: 12, width: 100 },
  { name: "scheduled", left: 114, width: 20 },
];

test("scrubTarget finds the segment and the point along it", () => {
  assert.deepEqual(scrubTarget(5, bar), { name: "in_flight", fraction: 0.5 });
  assert.deepEqual(scrubTarget(37, bar), { name: "ready", fraction: 0.25 });
  assert.deepEqual(scrubTarget(134, bar), { name: "scheduled", fraction: 1 });
});

test("scrubTarget takes the nearest edge in a gap or past the ends", () => {
  assert.deepEqual(scrubTarget(10.5, bar), { name: "in_flight", fraction: 1 });
  assert.deepEqual(scrubTarget(11.5, bar), { name: "ready", fraction: 0 });
  assert.deepEqual(scrubTarget(-20, bar), { name: "in_flight", fraction: 0 });
  assert.deepEqual(scrubTarget(500, bar), { name: "scheduled", fraction: 1 });
});

test("topAtFraction centres the point on screen", () => {
  assert.equal(topAtFraction(0.5, 1000, 20), 490);
});

test("topAtFraction stays within the list", () => {
  assert.equal(topAtFraction(0, 1000, 20), 0);
  assert.equal(topAtFraction(1, 1000, 20), 980);
  assert.equal(topAtFraction(0.5, 10, 20), 0);
});
