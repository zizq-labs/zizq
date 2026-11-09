// Live view of a Zizq server, fed by the admin API's `/events` WebSocket.
//
// Every message on that socket carries the server's current status, and
// job messages keep local copies of the server's job lists up to date (see
// lists.js). Messages can arrive far faster than the screen refreshes on a
// busy server, so they are applied as they arrive but rendered at most once
// per animation frame.

import { formatClock } from "./format.js";
import { LISTS, ListPanel, useTick } from "./list_view.js";
import { applyMessage, emptyLists } from "./lists.js";
import { html, render, useEffect, useRef, useState } from "./preact.js";

const MIN_RETRY_MS = 1000;
const MAX_RETRY_MS = 10000;

const numbers = new Intl.NumberFormat();

// Connects to `/events`, reconnecting with backoff. Returns the connection
// state, the most recent `ServerStatus` and when it arrived (so uptime can
// keep counting between messages), the job lists, `send` for messages to the
// server, and `fetches`, the last window requested for each list.
function useAdminStream() {
  const [, setVersion] = useState(0);
  const stream = useRef(null);
  stream.current ??= {
    conn: { state: "connecting" },
    server: null,
    receivedAt: 0,
    lists: emptyLists(),
    fetches: {},
    send: () => {},
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
        s.fetches = {};
        s.send = (message) => ws.send(JSON.stringify(message));
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
        // The server answers a window request with a snapshot of every
        // list, so any snapshot ends the wait for one.
        if (message.event === "job_snapshot") {
          for (const fetch of Object.values(s.fetches)) {
            fetch.waiting = false;
          }
        }
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
        s.send = () => {};
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

  const text = {
    connecting: "Connecting",
    connected: "Connected",
    disconnected: `Disconnected, retrying in ${conn.retryMs / 1000}s`,
  }[conn.state];

  // Uptime stops counting while disconnected, since the server may not be
  // up at all.
  const connected = conn.state === "connected";
  const uptime =
    server && formatClock(server.uptime_ms + (connected ? performance.now() - receivedAt : 0));

  return html`
    <header class="bar">
      <img class="brand" src="logo.svg" alt="Zizq" />
      <span class="conn"><span class="dot"></span>${text}</span>
      <span class="host">${location.host}</span>
      ${server &&
      html`
        <span class="server">
          <span>v${server.version}</span>
          ${server.tier !== "free" && html`<span>${server.tier}</span>`}
          <span>Uptime ${uptime}</span>
        </span>
      `}
    </header>
  `;
}

function ListTabs({ server, selected, onSelect }) {
  const jobs = server
    ? Object.values(LISTS).reduce((sum, list) => sum + server[list.total], 0)
    : null;

  return html`
    <div class="tab-bar">
      <div class="tabs" role="tablist" aria-label="Job lists">
      ${Object.keys(LISTS).map(
        (name) => html`
          <button
            key=${name}
            id="tab-${name}"
            class="tab ${name}"
            role="tab"
            aria-selected=${name === selected}
            aria-controls="list-panel"
            onClick=${() => onSelect(name)}
          >
            <span class="tab-label">${LISTS[name].label}</span>
            <span class="tab-total">
              ${server ? numbers.format(server[LISTS[name].total]) : "-"}
            </span>
          </button>
        `,
      )}
      </div>
      ${jobs !== null &&
      html`<span class="jobs-total">${numbers.format(jobs)} ${jobs === 1 ? "job" : "jobs"}</span>`}
    </div>
  `;
}

function App() {
  const stream = useAdminStream();
  const { conn, server, receivedAt } = stream;
  const [selected, select] = useSelectedList();

  // Each list's position, kept while another list is selected. `setTop`
  // takes a function of the current position, since several moves (a burst
  // of wheel or touch events) can arrive before the next render.
  const [tops, setTops] = useState({ in_flight: 0, ready: 0, scheduled: 0 });
  const setTop = (name, update) =>
    setTops((current) => ({ ...current, [name]: update(current[name]) }));

  return html`
    <div class="app" data-conn=${conn.state}>
      <${Header} conn=${conn} server=${server} receivedAt=${receivedAt} />
      <main>
        <${ListTabs} server=${server} selected=${selected} onSelect=${select} />
        <${ListPanel}
          stream=${stream}
          selected=${selected}
          onSelect=${select}
          tops=${tops}
          setTop=${setTop}
        />
      </main>
    </div>
  `;
}

render(html`<${App} />`, document.getElementById("root"));
