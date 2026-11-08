// Tests for web/overview.js.

import assert from "node:assert/strict";
import { test } from "node:test";

import { marker } from "../../web/overview.js";

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
