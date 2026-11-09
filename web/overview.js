// Geometry for the queue overview: the bar showing all three lists in order
// (in flight, ready, scheduled), each sized by its share of every job, as
// `zizq top`'s depth bar does. The browser lays out the segments; this works
// out where the marker for the rows shown sits within the selected one, and
// where in the queue a pointer scrubbing along the bar points.

import { clampTop } from "./view.js";

// Where rows `first` to `first + length` of a list of `count` jobs sit
// within its segment, as fractions from 0 to 1. An empty list is marked
// whole, so the marker still shows which list is selected.
export function marker(count, first, length) {
  if (count === 0 || length === 0) {
    return { start: 0, width: 1 };
  }
  const from = Math.min(first, count) / count;
  const to = Math.min(first + length, count) / count;
  return { start: from, width: to - from };
}

// The list and the point along it (0 to 1) under a pointer at `x`, given
// each segment's `left` and `width` on screen. Between segments, or past
// either end of the bar, it is the nearest segment's edge.
export function scrubTarget(x, segments) {
  let best = null;
  for (const { name, left, width } of segments) {
    const distance = x < left ? left - x : x > left + width ? x - (left + width) : 0;
    if (!best || distance < best.distance) {
      const fraction = width > 0 ? Math.max(0, Math.min(1, (x - left) / width)) : 0;
      best = { name, fraction, distance };
    }
  }
  return best && { name: best.name, fraction: best.fraction };
}

// The position that puts the point `fraction` of the way along a list of
// `total` jobs in the middle of the `rows` shown.
export function topAtFraction(fraction, total, rows) {
  return clampTop(Math.round(fraction * total - rows / 2), total, rows);
}
