// Local copies of the job lists the server is showing this connection.
//
// For each list, the server keeps a window of jobs per connection. It sends
// the whole window in a `job_snapshot`, then a `job_changed` for every job
// that enters or leaves it. Applying those in order keeps these copies the
// same as the server's windows, in the same order.

export const LIST_NAMES = ["in_flight", "ready", "scheduled"];

const byId = (a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);

// The order each list is kept in on the server.
const ORDER = {
  in_flight: (a, b) => (a.dequeued_at ?? 0) - (b.dequeued_at ?? 0),
  ready: (a, b) => a.priority - b.priority || byId(a, b),
  scheduled: (a, b) => a.ready_at - b.ready_at || byId(a, b),
};

export function emptyLists() {
  return Object.fromEntries(
    LIST_NAMES.map((name) => [name, { items: [], firstPosition: 0 }]),
  );
}

// Applies one admin message to `lists`, in place. Returns whether any list
// changed.
export function applyMessage(lists, message) {
  switch (message.event) {
    case "job_snapshot":
      for (const name of LIST_NAMES) {
        const window = message[name];
        lists[name] = { items: window.items, firstPosition: window.first_position };
      }
      return true;
    case "job_changed":
      applyChange(lists, message);
      return true;
    default:
      return false;
  }
}

function applyChange(lists, { id, status, job }) {
  switch (status) {
    case "in_flight":
      remove(lists.ready, id);
      insert(lists.in_flight, job, ORDER.in_flight);
      break;
    case "ready":
      insert(lists.ready, job, ORDER.ready);
      break;
    case "scheduled":
      insert(lists.scheduled, job, ORDER.scheduled);
      break;
    case "in_flight_removed":
      remove(lists.in_flight, id);
      break;
    case "ready_removed":
      remove(lists.ready, id);
      break;
    case "scheduled_removed":
      remove(lists.scheduled, id);
      break;
    case "completed":
    case "dead":
      for (const name of LIST_NAMES) {
        remove(lists[name], id);
      }
      break;
  }
}

// Inserts `job` at its place in `list` by `order`, unless it is already
// there. A change without its job carries nothing to insert.
function insert(list, job, order) {
  if (!job) {
    return;
  }
  const items = list.items;
  let lo = 0;
  let hi = items.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (order(items[mid], job) < 0) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  if (items[lo]?.id !== job.id) {
    items.splice(lo, 0, job);
  }
}

function remove(list, id) {
  const index = list.items.findIndex((job) => job.id === id);
  if (index >= 0) {
    list.items.splice(index, 1);
  }
}
