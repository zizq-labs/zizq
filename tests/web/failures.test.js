// Tests for web/failures.js.

import assert from "node:assert/strict";
import { test } from "node:test";

import { combineFailures, oldestAttempt } from "../../web/failures.js";

const failure = (attempt, message = `failure ${attempt}`) => ({ attempt, message });
const attempts = (failures) => failures.map((f) => f.attempt);

test("combineFailures puts older pages after the newest, newest first", () => {
  const combined = combineFailures([failure(13), failure(12)], [failure(11), failure(10)]);

  assert.deepEqual(attempts(combined), [13, 12, 11, 10]);
});

test("combineFailures keeps each attempt once, preferring the newest page", () => {
  const combined = combineFailures(
    [failure(12, "fresh"), failure(11, "fresh")],
    [failure(11, "stale"), failure(10)],
  );

  assert.deepEqual(attempts(combined), [12, 11, 10]);
  assert.equal(combined[1].message, "fresh");
});

test("combineFailures with nothing older is just the newest page", () => {
  assert.deepEqual(attempts(combineFailures([failure(2), failure(1)], [])), [2, 1]);
  assert.deepEqual(combineFailures([], []), []);
});

test("oldestAttempt is the last of failures listed newest first", () => {
  assert.equal(oldestAttempt([failure(5), failure(3)]), 3);
  assert.equal(oldestAttempt([]), undefined);
});
