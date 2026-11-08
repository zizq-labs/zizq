// Live view of a Zizq server, fed by the admin API's `/events` WebSocket.
//
// Every message on that socket carries the server's current status, and
// job messages keep local copies of the server's job lists up to date (see
// lists.js). Messages can arrive far faster than the screen refreshes on a
// busy server, so they are applied as they arrive but rendered at most once
// per animation frame.

import { formatClock, formatDue, formatElapsed } from "./format.js";
import { applyMessage, emptyLists } from "./lists.js";
import { html, render, useEffect, useRef, useState } from "./preact.js";

const MIN_RETRY_MS = 1000;
const MAX_RETRY_MS = 10000;

const numbers = new Intl.NumberFormat();

// How each list is presented. `time` fills the one column that differs.
const LISTS = {
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

// Connects to `/events`, reconnecting with backoff. Returns the connection
// state, the most recent `ServerStatus` and when it arrived (so uptime can
// keep counting between messages), and the job lists.
function useAdminStream() {
  const [, setVersion] = useState(0);
  const stream = useRef(null);
  stream.current ??= {
    conn: { state: "connecting" },
    server: null,
    receivedAt: 0,
    lists: emptyLists(),
  };

  useEffect(() => {
    const s = stream.current;
    let ws = null;
    let retryTimer = 0;
    let frame = 0;
    let retryMs = MIN_RETRY_MS;
    let stopped = false;

    const rerender = () => setVersion((v) => v + 1);
    const flush = () => {
      frame = 0;
      rerender();
    };

    const connect = () => {
      s.conn = { state: "connecting" };
      rerender();

      const scheme = location.protocol === "https:" ? "wss:" : "ws:";
      ws = new WebSocket(`${scheme}//${location.host}/events`);

      ws.onopen = () => {
        retryMs = MIN_RETRY_MS;
        s.conn = { state: "connected" };
        rerender();
      };

      ws.onmessage = (event) => {
        let message;
        try {
          message = JSON.parse(event.data);
        } catch {
          return;
        }
        if (message.server) {
          s.server = message.server;
          s.receivedAt = performance.now();
        }
        applyMessage(s.lists, message);
        frame ||= requestAnimationFrame(flush);
      };

      // Also follows a failed connection attempt, so this is the only
      // place that needs to retry. The next connection starts with a
      // fresh snapshot, so the lists are cleared rather than left stale.
      ws.onclose = () => {
        if (stopped) {
          return;
        }
        s.conn = { state: "disconnected", retryMs };
        s.lists = emptyLists();
        rerender();
        retryTimer = setTimeout(connect, retryMs);
        retryMs = Math.min(retryMs * 2, MAX_RETRY_MS);
      };
    };

    connect();

    return () => {
      stopped = true;
      clearTimeout(retryTimer);
      cancelAnimationFrame(frame);
      ws?.close();
    };
  }, []);

  return stream.current;
}

// Re-renders the calling component every `ms` milliseconds.
function useTick(ms) {
  const [, setTick] = useState(0);
  useEffect(() => {
    const timer = setInterval(() => setTick((n) => n + 1), ms);
    return () => clearInterval(timer);
  }, [ms]);
}

// The selected list, kept in the URL fragment so a reload or a shared link
// opens the same one.
function useSelectedList() {
  const fromHash = () => {
    const name = location.hash.slice(1);
    return name in LISTS ? name : "in_flight";
  };
  const [selected, setSelected] = useState(fromHash);

  useEffect(() => {
    const onHashChange = () => setSelected(fromHash());
    addEventListener("hashchange", onHashChange);
    return () => removeEventListener("hashchange", onHashChange);
  }, []);

  const select = (name) => {
    history.replaceState(null, "", `#${name}`);
    setSelected(name);
  };
  return [selected, select];
}

function Header({ conn, server, receivedAt }) {
  useTick(1000);

  const text =
    conn.state === "disconnected"
      ? `disconnected, retrying in ${conn.retryMs / 1000}s`
      : conn.state;

  // Uptime stops counting while disconnected, since the server may not be
  // up at all.
  const connected = conn.state === "connected";
  const uptime =
    server && formatClock(server.uptime_ms + (connected ? performance.now() - receivedAt : 0));

  return html`
    <header class="bar">
      <span class="brand">zizq</span>
      <span class="conn"><span class="dot"></span>${text}</span>
      <span class="host">${location.host}</span>
      ${server &&
      html`
        <span class="server">
          <span>v${server.version}</span>
          <span>${server.tier}</span>
          <span>up ${uptime}</span>
        </span>
      `}
    </header>
  `;
}

function ListTabs({ server, selected, onSelect }) {
  return html`
    <div class="tabs" role="tablist" aria-label="Job lists">
      ${Object.entries(LISTS).map(
        ([name, list]) => html`
          <button
            key=${name}
            class="tab ${name}"
            role="tab"
            aria-selected=${name === selected}
            onClick=${() => onSelect(name)}
          >
            <span class="tab-label">${list.label}</span>
            <span class="tab-total">${server ? numbers.format(server[list.total]) : "-"}</span>
          </button>
        `,
      )}
    </div>
  `;
}

function JobTable({ name, list, total }) {
  useTick(1000);
  const { timeHeading, time } = LISTS[name];
  const now = Date.now();
  const first = list.firstPosition + 1;
  const last = list.firstPosition + list.items.length;
  const range = `${numbers.format(first)}–${numbers.format(last)}`;

  return html`
    <div class="table-scroll ${name}" role="tabpanel">
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
          ${list.items.map(
            (job) => html`
              <tr key=${job.id} class=${retryClass(job.attempts)}>
                <td class="num id">${job.id}</td>
                <td class="num priority">${job.priority}</td>
                <td>${job.queue}</td>
                <td class="num">${time(job, now)}</td>
                <td class="num attempts">${job.attempts}</td>
                <td class="type">${job.job_type}</td>
              </tr>
            `,
          )}
        </tbody>
      </table>
      ${list.items.length === 0
        ? html`<p class="empty">No jobs ${LISTS[name].label}.</p>`
        : html`
            <p class="range">
              showing ${range} of ${numbers.format(Math.max(total, last))}
            </p>
          `}
    </div>
  `;
}

function retryClass(attempts) {
  return attempts === 0 ? "" : attempts === 1 ? "retried" : "retried-again";
}

function App() {
  const { conn, server, receivedAt, lists } = useAdminStream();
  const [selected, select] = useSelectedList();
  const total = server ? server[LISTS[selected].total] : 0;

  return html`
    <div class="app" data-conn=${conn.state}>
      <${Header} conn=${conn} server=${server} receivedAt=${receivedAt} />
      <main>
        <${ListTabs} server=${server} selected=${selected} onSelect=${select} />
        <${JobTable} name=${selected} list=${lists[selected]} total=${total} />
      </main>
    </div>
  `;
}

render(html`<${App} />`, document.getElementById("root"));
