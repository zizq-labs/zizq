// The rows of a list shown on screen, and the window of rows fetched from
// the server to show them.
//
// A list can be far longer than could ever be loaded, so the page shows a
// fixed number of rows (`rows`, however many fit) starting at a position
// (`top`) in the whole list. The server sends a window of the list around
// that position: three screens, starting one screen above, as `zizq top`
// fetches. Moving within that window needs no fetch until the rows shown
// get within a screen of either edge.

// Keeps `top` within the list, so the last screen is full when it can be.
export function clampTop(top, total, rows) {
  return Math.max(0, Math.min(top, total - rows));
}

// The rows to show, from position `top`: each with its position in the list
// and its job, or `null` while that row has not been fetched yet.
export function visibleRows(list, top, rows, total) {
  const count = Math.max(0, Math.min(rows, total - top));
  return Array.from({ length: count }, (_, i) => {
    const position = top + i;
    return { position, job: list.items[position - list.firstPosition] ?? null };
  });
}

// The window to ask the server for so that rows `top` onwards are in its
// middle.
export function wantedWindow(top, rows) {
  return { offset: Math.max(0, top - rows), limit: rows * 3 };
}

// Whether the window held for `list` should be replaced: when any row shown
// is missing from it, or when the rows shown are within a screen of an edge
// that has more of the list beyond it.
export function needsFetch(list, top, rows, total) {
  const start = list.firstPosition;
  const end = start + list.items.length;
  const shownEnd = Math.min(top + rows, total);

  if (shownEnd > top && (top < start || shownEnd > end)) {
    return true;
  }
  const nearStart = top < start + rows && start > 0;
  const nearEnd = shownEnd + rows > end && end < total;
  return nearStart || nearEnd;
}

// The position to show when the scrollbar thumb is dragged so that the
// point grabbed (`grab` pixels below the thumb's top) is at `pointerY`. The
// thumb's top travels over `space` pixels from the track's top at `trackY`,
// covering positions 0 to `total - rows`.
export function topFromPointer(pointerY, { trackY, grab, space }, total, rows) {
  const last = Math.max(0, total - rows);
  if (space <= 0) {
    return 0;
  }
  const travel = Math.max(0, Math.min(1, (pointerY - trackY - grab) / space));
  return Math.round(travel * last);
}
