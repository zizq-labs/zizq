// Tests for web/search.js.

import assert from "node:assert/strict";
import { test } from "node:test";

import { searchTerms } from "../../web/search.js";

test("searchTerms splits on commas and spaces", () => {
  assert.deepEqual(searchTerms("emails, reports  sms"), ["emails", "reports", "sms"]);
});

test("searchTerms ignores empty values", () => {
  assert.deepEqual(searchTerms(" , ,"), []);
  assert.deepEqual(searchTerms(""), []);
});
