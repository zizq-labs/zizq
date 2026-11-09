// The selected job list: the queue overview, the rows that fit on screen,
// and a scrollbar for moving through the whole list. See view.js for how
// the rows shown relate to the window of rows fetched from the server.

import { formatDue, formatElapsed } from "./format.js";
import { marker, scrubTarget, topAtFraction } from "./overview.js";
import { html, useEffect, useRef, useState } from "./preact.js";
import { clampTop, needsFetch, topFromPointer, visibleRows, wantedWindow } from "./view.js";

// Every table row, headings included, is this tall, so the number of rows
// that fit can be worked out from the table's height.
const ROW_HEIGHT = 28;

// The least time between fetches for one list, and how long to wait for a
// reply before giving up on one.
const FETCH_INTERVAL_MS = 100;
const FETCH_TIMEOUT_MS = 2000;

// How far ←/→ scroll the table sideways.
const SIDEWAYS_STEP = 48;

const numbers = new Intl.NumberFormat();

// How each list is presented. `time` fills the one column that differs.
export const LISTS = {
  in_flight: {
    label: "in flight",
    total: "total_in_flight",
    timeHeading: "duration",
    time: (job, now) => formatElapsed(job.dequeued_at, now),
  },
  ready: {
    label: "ready",
    total: "total_ready",
    timeHeading: "delay",
    time: (job, now) => formatElapsed(job.ready_at, now),
  },
  scheduled: {
    label: "scheduled",
    total: "total_scheduled",
    timeHeading: "due",
    time: (job, now) => formatDue(job.ready_at, now),
  },
};

const LIST_ORDER = Object.keys(LISTS);

// The number of table rows that fit in the element `ref` points at, less
// one for the headings.
function useRowsThatFit(ref) {
  const [rows, setRows] = useState(0);
  useEffect(() => {
    const observer = new ResizeObserver(([entry]) => {
      setRows(Math.max(1, Math.floor(entry.contentRect.height / ROW_HEIGHT) - 1));
    });
    observer.observe(ref.current);
    return () => observer.disconnect();
  }, []);
  return rows;
}

// Asks the server for a new window of `name` when the rows shown need one.
function useWindowFetch(stream, name, list, top, rows, total) {
  useEffect(() => {
    if (!stream.server || rows === 0 || !needsFetch(list, top, rows, total)) {
      return;
    }
    const now = performance.now();
    const sent = stream.fetches[name];
    if (sent && (sent.waiting ? now - sent.at < FETCH_TIMEOUT_MS : now - sent.at < FETCH_INTERVAL_MS)) {
      return;
    }
    const { offset, limit } = wantedWindow(top, rows);
    stream.fetches[name] = { at: now, waiting: true };
    stream.send({ type: "subscribe", list: name, anchor: { kind: "offset", offset }, limit });
  });
}

// Keyboard shortcuts for the list, wherever focus is, other than in a text
// field. They follow `zizq top`, except that Shift+←/→ switch lists, since
// Tab moves focus in a browser.
function useListKeys({ rows, moveBy, moveTo, scrollSideways, selected, onSelect }) {
  const latest = useRef();
  latest.current = { rows, moveBy, moveTo, scrollSideways, selected, onSelect };

  useEffect(() => {
    const onKeyDown = (event) => {
      if (event.defaultPrevented || event.ctrlKey || event.metaKey || event.altKey) {
        return;
      }
      if (event.target.closest?.("input, textarea, select, [contenteditable]")) {
        return;
      }
      const k = latest.current;
      const switchBy = (step) => {
        const i = LIST_ORDER.indexOf(k.selected);
        k.onSelect(LIST_ORDER[(i + step + LIST_ORDER.length) % LIST_ORDER.length]);
      };
      const actions = event.shiftKey
        ? {
            ArrowLeft: () => switchBy(-1),
            ArrowRight: () => switchBy(1),
            G: () => k.moveTo(Infinity),
          }
        : {
            ArrowUp: () => k.moveBy(-1),
            k: () => k.moveBy(-1),
            ArrowDown: () => k.moveBy(1),
            j: () => k.moveBy(1),
            PageUp: () => k.moveBy(-k.rows),
            PageDown: () => k.moveBy(k.rows),
            Home: () => k.moveTo(0),
            g: () => k.moveTo(0),
            End: () => k.moveTo(Infinity),
            ArrowLeft: () => k.scrollSideways(-SIDEWAYS_STEP),
            ArrowRight: () => k.scrollSideways(SIDEWAYS_STEP),
            1: () => k.onSelect(LIST_ORDER[0]),
            2: () => k.onSelect(LIST_ORDER[1]),
            3: () => k.onSelect(LIST_ORDER[2]),
          };
      const action = actions[event.key];
      if (action) {
        event.preventDefault();
        action();
      }
    };
    addEventListener("keydown", onKeyDown);
    return () => removeEventListener("keydown", onKeyDown);
  }, []);
}

// Turns wheel movement into whole rows, carrying the remainder over so
// that slow trackpad scrolling still adds up.
function useWheelRows(moveBy, rows) {
  const carried = useRef(0);
  return (event) => {
    if (Math.abs(event.deltaY) < Math.abs(event.deltaX)) {
      return;
    }
    event.preventDefault();
    const perStep = [1 / ROW_HEIGHT, 1, rows][event.deltaMode] ?? 1;
    carried.current += event.deltaY * perStep;
    const whole = Math.trunc(carried.current);
    carried.current -= whole;
    if (whole !== 0) {
      moveBy(whole);
    }
  };
}

// Turns vertical touch swipes into whole rows, the way the wheel does, and
// keeps a flicked list moving until it slows to a stop. Sideways swipes are
// left to the browser (see `touch-action` in the stylesheet).
function useTouchRows(moveBy) {
  const latest = useRef(moveBy);
  latest.current = moveBy;
  const touch = useRef(null);
  const glide = useRef(0);

  const stopGliding = () => {
    cancelAnimationFrame(glide.current);
    glide.current = 0;
  };

  useEffect(() => stopGliding, []);

  // Moves by `pixels` of finger travel, carrying part-rows over.
  const travel = (pixels) => {
    const t = touch.current;
    t.carried += pixels / ROW_HEIGHT;
    const whole = Math.trunc(t.carried);
    t.carried -= whole;
    if (whole !== 0) {
      latest.current(whole);
    }
  };

  const onPointerDown = (event) => {
    if (event.pointerType !== "touch") {
      return;
    }
    stopGliding();
    touch.current = { id: event.pointerId, y: event.clientY, carried: 0, velocity: 0, at: event.timeStamp };
  };

  const onPointerMove = (event) => {
    const t = touch.current;
    if (t?.id !== event.pointerId) {
      return;
    }
    // Dragging up moves further down the list.
    const pixels = t.y - event.clientY;
    const elapsed = Math.max(1, event.timeStamp - t.at);
    t.velocity = 0.8 * (pixels / elapsed) + 0.2 * t.velocity;
    t.y = event.clientY;
    t.at = event.timeStamp;
    travel(pixels);
  };

  const onPointerEnd = (event) => {
    const t = touch.current;
    if (t?.id !== event.pointerId) {
      return;
    }
    // A finger held still before lifting is not a flick.
    if (event.timeStamp - t.at > 100 || Math.abs(t.velocity) < 0.1) {
      touch.current = null;
      return;
    }
    let last = performance.now();
    const step = (now) => {
      const elapsed = now - last;
      last = now;
      travel(t.velocity * elapsed);
      t.velocity *= Math.pow(0.95, elapsed / 16);
      glide.current = Math.abs(t.velocity) > 0.02 ? requestAnimationFrame(step) : 0;
    };
    glide.current = requestAnimationFrame(step);
  };

  return { onPointerDown, onPointerMove, onPointerUp: onPointerEnd, onPointerCancel: onPointerEnd };
}

// All three lists in queue order, sized by their share of every job, with a
// marker under the rows shown. Decorative: the tabs carry the same totals.
// Pressing on the bar, and dragging along it, scrubs through the whole
// queue: the list under the pointer is selected and shown at that point,
// switching lists as the pointer crosses into another segment. That is a
// pointer convenience, so the bar stays hidden from assistive technology,
// which has the tabs and the scrollbar.
function Overview({ server, selected, shown, onScrub }) {
  const barRef = useRef();
  const scrubbing = useRef(null);

  const scrubTo = (x) => {
    const segments = [...barRef.current.children].map((el) => {
      const box = el.getBoundingClientRect();
      return { name: el.dataset.list, left: box.left, width: box.width };
    });
    const target = scrubTarget(x, segments);
    if (target) {
      onScrub(target.name, target.fraction);
    }
  };

  const onPointerDown = (event) => {
    if (!server || event.button !== 0) {
      return;
    }
    event.preventDefault();
    scrubbing.current = event.pointerId;
    barRef.current.setPointerCapture(event.pointerId);
    scrubTo(event.clientX);
  };

  const onPointerMove = (event) => {
    if (scrubbing.current === event.pointerId) {
      scrubTo(event.clientX);
    }
  };

  const onPointerEnd = (event) => {
    if (scrubbing.current === event.pointerId) {
      scrubbing.current = null;
    }
  };

  return html`
    <div
      ref=${barRef}
      class="overview"
      aria-hidden="true"
      onPointerDown=${onPointerDown}
      onPointerMove=${onPointerMove}
      onPointerUp=${onPointerEnd}
      onPointerCancel=${onPointerEnd}
    >
      ${LIST_ORDER.map(
        (name) => html`
          <span
            key=${name}
            class="segment ${name}"
            data-list=${name}
            data-selected=${name === selected}
            style=${{ flexGrow: server ? server[LISTS[name].total] : 0 }}
          >
            ${name === selected &&
            shown &&
            html`
              <span
                class="marker"
                style=${{ left: `${shown.start * 100}%`, width: `${shown.width * 100}%` }}
              ></span>
            `}
          </span>
        `,
      )}
    </div>
  `;
}

function JobRows({ name, rows }) {
  const { timeHeading, time } = LISTS[name];
  const now = Date.now();

  return html`
    <table class="jobs">
      <thead>
        <tr>
          <th class="num">id</th>
          <th class="num">priority</th>
          <th>queue</th>
          <th class="num">${timeHeading}</th>
          <th class="num">attempts</th>
          <th>type</th>
        </tr>
      </thead>
      <tbody>
        ${rows.map(({ position, job }) =>
          job
            ? html`
                <tr key=${job.id} class=${retryClass(job.attempts)}>
                  <td class="num id">${job.id}</td>
                  <td class="num priority">${job.priority}</td>
                  <td>${job.queue}</td>
                  <td class="num">${time(job, now)}</td>
                  <td class="num attempts">${job.attempts}</td>
                  <td class="type">${job.job_type}</td>
                </tr>
              `
            : html`
                <tr key=${`pending-${position}`} class="pending">
                  <td colspan="6">…</td>
                </tr>
              `,
        )}
      </tbody>
    </table>
  `;
}

function retryClass(attempts) {
  return attempts === 0 ? "" : attempts === 1 ? "retried" : "retried-again";
}

// Shows where the rows on screen sit in the whole list. The thumb has a
// minimum height, so its position is a share of the track it can move in.
//
// Dragging the thumb moves through the list, keeping the point grabbed under
// the pointer. Pressing elsewhere on the track centres the thumb there, then
// drags from that point, as native scrollbars do.
function Scrollbar({ top, rows, total, onMove }) {
  const trackRef = useRef();
  const drag = useRef(null);

  const size = total > rows ? rows / total : 1;
  const travel = total > rows ? top / (total - rows) : 0;
  const thumbHeight = `max(24px, ${size * 100}%)`;
  const first = Math.min(top + 1, total);
  const last = Math.min(top + rows, total);

  const onPointerDown = (event) => {
    const thumb = trackRef.current.querySelector(".thumb");
    if (!thumb || event.button !== 0) {
      return;
    }
    // A touch here is for the scrollbar, not a swipe through the list.
    event.stopPropagation();
    event.preventDefault();

    const track = trackRef.current.getBoundingClientRect();
    const box = thumb.getBoundingClientRect();
    const onThumb = event.target === thumb;
    drag.current = {
      id: event.pointerId,
      trackY: track.top,
      grab: onThumb ? event.clientY - box.top : box.height / 2,
      space: track.height - box.height,
    };
    trackRef.current.setPointerCapture(event.pointerId);
    if (!onThumb) {
      onMove(topFromPointer(event.clientY, drag.current, total, rows));
    }
  };

  const onPointerMove = (event) => {
    if (drag.current?.id === event.pointerId) {
      onMove(topFromPointer(event.clientY, drag.current, total, rows));
    }
  };

  const onPointerEnd = (event) => {
    if (drag.current?.id === event.pointerId) {
      drag.current = null;
    }
  };

  return html`
    <div
      ref=${trackRef}
      class="scrollbar"
      role="scrollbar"
      aria-controls="list-rows"
      aria-orientation="vertical"
      aria-valuemin="0"
      aria-valuemax=${Math.max(0, total - rows)}
      aria-valuenow=${top}
      aria-valuetext="rows ${first} to ${last} of ${total}"
      onPointerDown=${onPointerDown}
      onPointerMove=${onPointerMove}
      onPointerUp=${onPointerEnd}
      onPointerCancel=${onPointerEnd}
    >
      ${total > rows &&
      html`
        <div
          class="thumb"
          style=${{ height: thumbHeight, top: `calc((100% - ${thumbHeight}) * ${travel})` }}
        ></div>
      `}
    </div>
  `;
}

export function ListPanel({ stream, selected, onSelect, tops, setTop }) {
  const { server, lists } = stream;
  const list = lists[selected];
  const total = server ? server[LISTS[selected].total] : 0;

  const bodyRef = useRef();
  const rows = useRowsThatFit(bodyRef);
  const top = clampTop(tops[selected], total, rows);

  useTick(1000);
  useWindowFetch(stream, selected, list, top, rows, total);

  const moveTo = (position) => setTop(selected, () => clampTop(position, total, rows));
  const moveBy = (step) =>
    setTop(selected, (current) => clampTop(clampTop(current, total, rows) + step, total, rows));
  const scrollSideways = (step) => bodyRef.current.scrollBy({ left: step });
  const scrub = (name, fraction) => {
    const listTotal = server[LISTS[name].total];
    setTop(name, () => topAtFraction(fraction, listTotal, rows));
    if (name !== selected) {
      onSelect(name);
    }
  };
  useListKeys({ rows, moveBy, moveTo, scrollSideways, selected, onSelect });
  const onWheel = useWheelRows(moveBy, rows);
  const touchHandlers = useTouchRows(moveBy);

  const shown = visibleRows(list, top, rows, total);
  const range = shown.length
    ? `${numbers.format(top + 1)}–${numbers.format(top + shown.length)}`
    : "";

  return html`
    <section
      id="list-panel"
      class="panel ${selected}"
      role="tabpanel"
      aria-labelledby="tab-${selected}"
      style=${{ "--row-height": `${ROW_HEIGHT}px` }}
    >
      <${Overview}
        server=${server}
        selected=${selected}
        shown=${server && marker(total, top, shown.length)}
        onScrub=${scrub}
      />
      <div class="list-body" onWheel=${onWheel} ...${touchHandlers}>
        <div class="table-scroll" id="list-rows" ref=${bodyRef}>
          ${total === 0
            ? html`<p class="empty">No jobs ${LISTS[selected].label}.</p>`
            : html`<${JobRows} name=${selected} rows=${shown} />`}
        </div>
        <${Scrollbar} top=${top} rows=${rows} total=${total} onMove=${moveTo} />
      </div>
      ${shown.length > 0 &&
      html`<p class="range">rows ${range} of ${numbers.format(total)}</p>`}
    </section>
  `;
}

// Re-renders the calling component every `ms` milliseconds.
export function useTick(ms) {
  const [, setTick] = useState(0);
  useEffect(() => {
    const timer = setInterval(() => setTick((n) => n + 1), ms);
    return () => clearInterval(timer);
  }, [ms]);
}
