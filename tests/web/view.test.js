// Tests for web/view.js.

import assert from "node:assert/strict";
import { test } from "node:test";

import {
  clampTop,
  needsFetch,
  topFromPointer,
  visibleRows,
  wantedWindow,
} from "../../web/view.js";

// A window of `length` jobs starting at `firstPosition`, with ids naming
// their positions.
function windowAt(firstPosition, length) {
  const items = Array.from({ length }, (_, i) => ({ id: `job-${firstPosition + i}` }));
  return { items, firstPosition };
}

test("clampTop keeps the last screen full", () => {
  assert.equal(clampTop(-5, 100, 10), 0);
  assert.equal(clampTop(50, 100, 10), 50);
  assert.equal(clampTop(95, 100, 10), 90);
});

test("clampTop is zero when the whole list fits", () => {
  assert.equal(clampTop(3, 5, 10), 0);
});

test("visibleRows takes rows from the window by position", () => {
  const rows = visibleRows(windowAt(100, 50), 120, 3, 1000);

  assert.deepEqual(
    rows.map((r) => [r.position, r.job.id]),
    [
      [120, "job-120"],
      [121, "job-121"],
      [122, "job-122"],
    ],
  );
});

test("visibleRows leaves rows outside the window empty", () => {
  const rows = visibleRows(windowAt(100, 50), 148, 4, 1000);

  assert.deepEqual(
    rows.map((r) => r.job?.id ?? null),
    ["job-148", "job-149", null, null],
  );
});

test("visibleRows stops at the end of the list", () => {
  assert.equal(visibleRows(windowAt(0, 5), 0, 10, 5).length, 5);
  assert.equal(visibleRows(windowAt(0, 0), 0, 10, 0).length, 0);
});

test("wantedWindow is three screens, starting a screen above", () => {
  assert.deepEqual(wantedWindow(500, 20), { offset: 480, limit: 60 });
  assert.deepEqual(wantedWindow(5, 20), { offset: 0, limit: 60 });
});

test("a window fetched for a position needs no further fetch", () => {
  const { offset, limit } = wantedWindow(500, 20);

  assert.equal(needsFetch(windowAt(offset, limit), 500, 20, 10_000), false);
});

test("rows shown outside the window need a fetch", () => {
  assert.equal(needsFetch(windowAt(0, 60), 1000, 20, 10_000), true);
  assert.equal(needsFetch(windowAt(1000, 60), 990, 20, 10_000), true);
});

test("nearing an edge with more beyond it needs a fetch", () => {
  assert.equal(needsFetch(windowAt(480, 60), 525, 20, 10_000), true, "near the end");
  assert.equal(needsFetch(windowAt(480, 60), 485, 20, 10_000), true, "near the start");
});

test("nearing an edge that is the end of the list needs no fetch", () => {
  assert.equal(needsFetch(windowAt(0, 60), 0, 20, 10_000), false, "start of the list");
  assert.equal(needsFetch(windowAt(940, 60), 980, 20, 1000), false, "end of the list");
});

test("a list shorter than a screen needs no fetch once loaded", () => {
  assert.equal(needsFetch(windowAt(0, 5), 0, 20, 5), false);
});

test("an empty list needs no fetch", () => {
  assert.equal(needsFetch(windowAt(0, 0), 0, 20, 0), false);
});

test("topFromPointer maps the thumb's travel onto the list", () => {
  const track = { trackY: 100, grab: 10, space: 200 };

  assert.equal(topFromPointer(110, track, 1000, 20), 0, "thumb at the top");
  assert.equal(topFromPointer(210, track, 1000, 20), 490, "halfway");
  assert.equal(topFromPointer(310, track, 1000, 20), 980, "thumb at the bottom");
});

test("topFromPointer stops at either end of the track", () => {
  const track = { trackY: 100, grab: 10, space: 200 };

  assert.equal(topFromPointer(0, track, 1000, 20), 0);
  assert.equal(topFromPointer(900, track, 1000, 20), 980);
});

test("topFromPointer is zero when the list fits or the thumb fills the track", () => {
  assert.equal(topFromPointer(150, { trackY: 100, grab: 0, space: 200 }, 10, 20), 0);
  assert.equal(topFromPointer(150, { trackY: 100, grab: 0, space: 0 }, 1000, 20), 0);
});
