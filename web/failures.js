// A job's failures, as loaded: the newest page from its `job_detail`, and any
// older pages loaded with `get_errors` since.

// Every failure loaded, newest first, each attempt once. Where both hold an
// attempt, the newer copy in `newest` wins.
export function combineFailures(newest, older) {
  const byAttempt = new Map();
  for (const error of [...older, ...newest]) {
    byAttempt.set(error.attempt, error);
  }
  return [...byAttempt.values()].sort((a, b) => b.attempt - a.attempt);
}

// The attempt to ask for failures before, to continue past those loaded:
// the oldest of them.
export function oldestAttempt(failures) {
  return failures.length ? failures[failures.length - 1].attempt : undefined;
}
