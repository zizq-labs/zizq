// Live view of a Zizq server, fed by the admin API's `/events` WebSocket.
//
// Every message on the WebSocket carries the server's current status, so the
// page only needs to keep the latest one and render it. Messages can arrive
// far faster than the screen refreshes on a busy server, so rendering is
// coalesced into animation frames.

"use strict";

(() => {
  const MIN_RETRY_MS = 1000;
  const MAX_RETRY_MS = 10000;

  const numbers = new Intl.NumberFormat();
  const $ = (id) => document.getElementById(id);

  // The most recent `ServerStatus`, and when it arrived, so uptime can keep
  // counting between messages.
  let server = null;
  let receivedAt = 0;
  let connected = false;
  let frameRequested = false;
  let retryMs = MIN_RETRY_MS;

  function connect() {
    setConnection("connecting", "connecting");

    const scheme = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${scheme}//${location.host}/events`);

    ws.onopen = () => {
      connected = true;
      retryMs = MIN_RETRY_MS;
      setConnection("connected", "connected");
    };

    ws.onmessage = (event) => {
      let message;
      try {
        message = JSON.parse(event.data);
      } catch {
        return;
      }
      if (message.server) {
        server = message.server;
        receivedAt = performance.now();
        requestRender();
      }
    };

    // Also follows a failed connection attempt, so this is the only place
    // that needs to retry.
    ws.onclose = () => {
      connected = false;
      setConnection("disconnected", `disconnected, retrying in ${retryMs / 1000}s`);
      setTimeout(connect, retryMs);
      retryMs = Math.min(retryMs * 2, MAX_RETRY_MS);
    };
  }

  function setConnection(state, text) {
    document.body.dataset.conn = state;
    $("conn-text").textContent = text;
  }

  function requestRender() {
    if (!frameRequested) {
      frameRequested = true;
      requestAnimationFrame(render);
    }
  }

  function render() {
    frameRequested = false;
    if (!server) {
      return;
    }

    $("total-ready").textContent = numbers.format(server.total_ready);
    $("total-in-flight").textContent = numbers.format(server.total_in_flight);
    $("total-scheduled").textContent = numbers.format(server.total_scheduled);
    $("version").textContent = server.version;
    $("tier").textContent = server.tier;

    // Uptime stops counting while disconnected, since the server may not be
    // up at all.
    const elapsed = connected ? performance.now() - receivedAt : 0;
    $("uptime").textContent = formatDuration(server.uptime_ms + elapsed);
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

  $("host").textContent = location.host;
  setInterval(requestRender, 1000);
  connect();
})();
