// Live view of a Zizq server, fed by the admin API's `/events` WebSocket.
//
// Every message on that socket carries the server's current status, so the
// page only needs to keep the latest one and render it. Messages can arrive
// far faster than the screen refreshes on a busy server, so state updates
// are coalesced into animation frames.

import { html, render, useEffect, useState } from "./preact.js";

const MIN_RETRY_MS = 1000;
const MAX_RETRY_MS = 10000;

const numbers = new Intl.NumberFormat();

// Connects to `/events`, reconnecting with backoff, and returns the
// connection state plus the most recent `ServerStatus` and when it
// arrived, so uptime can keep counting between messages.
function useServerStatus() {
  const [view, setView] = useState({
    conn: { state: "connecting" },
    server: null,
    receivedAt: 0,
  });

  useEffect(() => {
    let ws = null;
    let retryTimer = 0;
    let frame = 0;
    let retryMs = MIN_RETRY_MS;
    let latest = null;
    let stopped = false;

    const setConn = (conn) => setView((v) => ({ ...v, conn }));

    const flush = () => {
      frame = 0;
      setView((v) => ({ ...v, server: latest.server, receivedAt: latest.at }));
    };

    const connect = () => {
      setConn({ state: "connecting" });

      const scheme = location.protocol === "https:" ? "wss:" : "ws:";
      ws = new WebSocket(`${scheme}//${location.host}/events`);

      ws.onopen = () => {
        retryMs = MIN_RETRY_MS;
        setConn({ state: "connected" });
      };

      ws.onmessage = (event) => {
        let message;
        try {
          message = JSON.parse(event.data);
        } catch {
          return;
        }
        if (message.server) {
          latest = { server: message.server, at: performance.now() };
          frame ||= requestAnimationFrame(flush);
        }
      };

      // Also follows a failed connection attempt, so this is the only
      // place that needs to retry.
      ws.onclose = () => {
        if (stopped) {
          return;
        }
        setConn({ state: "disconnected", retryMs });
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

  return view;
}

// Re-renders the calling component every `ms` milliseconds.
function useTick(ms) {
  const [, setTick] = useState(0);
  useEffect(() => {
    const timer = setInterval(() => setTick((n) => n + 1), ms);
    return () => clearInterval(timer);
  }, [ms]);
}

function Header({ conn }) {
  const text =
    conn.state === "disconnected"
      ? `disconnected, retrying in ${conn.retryMs / 1000}s`
      : conn.state;

  return html`
    <header class="bar">
      <span class="brand">zizq</span>
      <span class="conn"><span class="dot"></span>${text}</span>
      <span class="host">${location.host}</span>
    </header>
  `;
}

function Totals({ server }) {
  const total = (key) => (server ? numbers.format(server[key]) : "-");

  return html`
    <section aria-labelledby="jobs-heading">
      <h2 id="jobs-heading">Jobs</h2>
      <dl class="totals">
        <div class="in-flight">
          <dt>in flight</dt>
          <dd>${total("total_in_flight")}</dd>
        </div>
        <div class="ready">
          <dt>ready</dt>
          <dd>${total("total_ready")}</dd>
        </div>
        <div class="scheduled">
          <dt>scheduled</dt>
          <dd>${total("total_scheduled")}</dd>
        </div>
      </dl>
    </section>
  `;
}

function ServerInfo({ server, receivedAt, connected }) {
  useTick(1000);

  // Uptime stops counting while disconnected, since the server may not be
  // up at all.
  const uptime = server
    ? formatDuration(server.uptime_ms + (connected ? performance.now() - receivedAt : 0))
    : "-";

  return html`
    <section aria-labelledby="server-heading">
      <h2 id="server-heading">Server</h2>
      <dl class="props">
        <dt>version</dt>
        <dd>${server?.version ?? "-"}</dd>
        <dt>tier</dt>
        <dd>${server?.tier ?? "-"}</dd>
        <dt>uptime</dt>
        <dd>${uptime}</dd>
      </dl>
    </section>
  `;
}

function App() {
  const { conn, server, receivedAt } = useServerStatus();

  return html`
    <div class="app" data-conn=${conn.state}>
      <${Header} conn=${conn} />
      <main>
        <${Totals} server=${server} />
        <${ServerInfo}
          server=${server}
          receivedAt=${receivedAt}
          connected=${conn.state === "connected"}
        />
      </main>
    </div>
  `;
}

function formatDuration(ms) {
  const total = Math.floor(ms / 1000);
  const days = Math.floor(total / 86400);
  const pad = (n) => String(n).padStart(2, "0");
  const hms = [
    pad(Math.floor((total % 86400) / 3600)),
    pad(Math.floor((total % 3600) / 60)),
    pad(total % 60),
  ].join(":");
  return days > 0 ? `${days}d ${hms}` : hms;
}

render(html`<${App} />`, document.getElementById("root"));
