// Geometry for the queue overview: the bar showing all three lists in order
// (in flight, ready, scheduled), each sized by its share of every job, as
// `zizq top`'s depth bar does. The browser lays out the segments; this works
// out where the marker for the rows shown sits within the selected one.

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
