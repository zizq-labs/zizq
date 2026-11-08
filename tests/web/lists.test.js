// Tests for web/lists.js. Run with `node --test tests/web/`.

import assert from "node:assert/strict";
import { test } from "node:test";

import { applyMessage, emptyLists } from "../../web/lists.js";

function job(id, fields = {}) {
  return { id, queue: "q", job_type: "t", priority: 100, ready_at: 0, attempts: 0, ...fields };
}

function window(items, first_position = 0) {
  return { items, first_position, total: items.length };
}

function snapshot({ in_flight = [], ready = [], scheduled = [] } = {}) {
  return {
    event: "job_snapshot",
    in_flight: window(in_flight),
    ready: window(ready),
    scheduled: window(scheduled),
  };
}

function changed(status, id, job) {
  return { event: "job_changed", id, status, ...(job ? { job } : {}) };
}

const ids = (list) => list.items.map((j) => j.id);

test("a snapshot replaces every list", () => {
  const lists = emptyLists();
  applyMessage(lists, snapshot({ ready: [job("a")] }));

  const message = snapshot({ in_flight: [job("b")], scheduled: [job("c")] });
  message.ready.first_position = 40;
  assert.equal(applyMessage(lists, message), true);

  assert.deepEqual(ids(lists.in_flight), ["b"]);
  assert.deepEqual(ids(lists.ready), []);
  assert.equal(lists.ready.firstPosition, 40);
  assert.deepEqual(ids(lists.scheduled), ["c"]);
});

test("ready jobs are kept in priority order, then by id", () => {
  const lists = emptyLists();
  applyMessage(lists, changed("ready", "b", job("b", { priority: 5 })));
  applyMessage(lists, changed("ready", "a", job("a", { priority: 5 })));
  applyMessage(lists, changed("ready", "c", job("c", { priority: 1 })));

  assert.deepEqual(ids(lists.ready), ["c", "a", "b"]);
});

test("in-flight jobs are kept in dequeued order", () => {
  const lists = emptyLists();
  applyMessage(lists, changed("in_flight", "a", job("a", { dequeued_at: 30 })));
  applyMessage(lists, changed("in_flight", "b", job("b", { dequeued_at: 10 })));

  assert.deepEqual(ids(lists.in_flight), ["b", "a"]);
});

test("scheduled jobs are kept in due order, then by id", () => {
  const lists = emptyLists();
  applyMessage(lists, changed("scheduled", "b", job("b", { ready_at: 20 })));
  applyMessage(lists, changed("scheduled", "c", job("c", { ready_at: 10 })));
  applyMessage(lists, changed("scheduled", "a", job("a", { ready_at: 20 })));

  assert.deepEqual(ids(lists.scheduled), ["c", "a", "b"]);
});

test("a job already in the list is not inserted twice", () => {
  const lists = emptyLists();
  applyMessage(lists, changed("ready", "a", job("a")));
  applyMessage(lists, changed("ready", "a", job("a")));

  assert.deepEqual(ids(lists.ready), ["a"]);
});

test("a job going in flight leaves the ready list", () => {
  const lists = emptyLists();
  applyMessage(lists, snapshot({ ready: [job("a"), job("b")] }));
  applyMessage(lists, changed("in_flight", "a", job("a", { dequeued_at: 1 })));

  assert.deepEqual(ids(lists.ready), ["b"]);
  assert.deepEqual(ids(lists.in_flight), ["a"]);
});

test("removals take the job out of the named list only", () => {
  for (const [status, list] of [
    ["in_flight_removed", "in_flight"],
    ["ready_removed", "ready"],
    ["scheduled_removed", "scheduled"],
  ]) {
    const lists = emptyLists();
    applyMessage(lists, snapshot({ in_flight: [job("a")], ready: [job("a")], scheduled: [job("a")] }));
    applyMessage(lists, changed(status, "a"));
    for (const name of ["in_flight", "ready", "scheduled"]) {
      assert.deepEqual(ids(lists[name]), name === list ? [] : ["a"], `${status} on ${name}`);
    }
  }
});

test("completed and dead jobs leave every list", () => {
  for (const status of ["completed", "dead"]) {
    const lists = emptyLists();
    applyMessage(lists, snapshot({ in_flight: [job("a")], ready: [job("a")], scheduled: [job("a")] }));
    applyMessage(lists, changed(status, "a"));

    assert.deepEqual(ids(lists.in_flight), [], status);
    assert.deepEqual(ids(lists.ready), [], status);
    assert.deepEqual(ids(lists.scheduled), [], status);
  }
});

test("removing an unknown job changes nothing", () => {
  const lists = emptyLists();
  applyMessage(lists, snapshot({ ready: [job("a")] }));
  applyMessage(lists, changed("ready_removed", "missing"));

  assert.deepEqual(ids(lists.ready), ["a"]);
});

test("messages that are not about jobs are ignored", () => {
  const lists = emptyLists();
  assert.equal(applyMessage(lists, { event: "heartbeat" }), false);
});
