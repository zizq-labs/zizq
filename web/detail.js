// The details of one job: its attributes, payload and failures, as the
// server reports them in answer to `get_job`.

import { formatDue, formatDuration, formatElapsed, formatTimestamp } from "./format.js";
import { html, useEffect, useState } from "./preact.js";

const STATUSES = {
  scheduled: "Scheduled",
  ready: "Ready",
  in_flight: "In-Flight",
  completed: "Completed",
  dead: "Dead",
};

// Waiting for the details shows the job's id and nothing else, so the
// panel doesn't flash an empty state before they arrive.
export function JobDetail({ id, detail, onClose }) {
  const job = detail?.job;

  return html`
    <aside class="detail" aria-label="Job details">
      <div class="detail-heading">
        <span class="detail-id">${id}</span>
        ${detail?.status &&
        html`<span class="status ${detail.status}">${STATUSES[detail.status]}</span>`}
        <button class="close" title="Close (Esc)" aria-label="Close" onClick=${onClose}>
          ✕
        </button>
      </div>
      <div class="detail-body">
        ${!detail
          ? null
          : !job
            ? html`<p class="gone">This job no longer exists.</p>`
            : html`
                <${Attributes} job=${job} status=${detail.status} />
                <h3>Payload</h3>
                <pre class="payload">${JSON.stringify(job.payload ?? null, null, 2)}</pre>
                <${Failures} id=${id} errors=${detail.errors} more=${detail.more_errors} />
              `}
      </div>
    </aside>
  `;
}

function Attributes({ job, status }) {
  const now = Date.now();
  const at = (ms, relative) => ms && `${formatTimestamp(ms)} (${relative(ms, now)})`;
  const rows = [
    ["Queue", job.queue],
    ["Type", job.job_type],
    ["Priority", job.priority],
    ["Attempts", job.retry_limit != null ? `${job.attempts} / ${job.retry_limit}` : job.attempts],
    [status === "scheduled" ? "Due" : "Ready at", at(job.ready_at, status === "scheduled" ? formatDue : ago)],
    ["Taken", at(job.dequeued_at, ago)],
    ["Failed", at(job.failed_at, ago)],
    ["Backoff", job.backoff && backoff(job.backoff)],
    ["Retention", job.retention && retention(job.retention)],
    ["Unique key", job.unique_key && `${job.unique_key} (while ${job.unique_while})`],
  ].filter(([, value]) => value != null && value !== "");

  return html`
    <dl class="attributes">
      ${rows.map(([label, value]) => html`<dt>${label}</dt><dd>${value}</dd>`)}
    </dl>
  `;
}

const ago = (ms, now) => `${formatElapsed(ms, now)} ago`;

function backoff({ base_ms, exponent, jitter_ms }) {
  return `${formatDuration(base_ms)} × attempts^${exponent} + up to ${formatDuration(jitter_ms)}`;
}

function retention({ completed_ms, dead_ms }) {
  return [
    completed_ms != null && `completed ${formatDuration(completed_ms)}`,
    dead_ms != null && `dead ${formatDuration(dead_ms)}`,
  ]
    .filter(Boolean)
    .join(", ");
}

// Newest first. The newest is open, since it's nearly always the one
// that matters, including when a new failure arrives while watching; the
// rest show one line each until opened.
function Failures({ id, errors, more }) {
  const [open, setOpen] = useState(() => new Set());
  const newest = errors[0]?.attempt;

  useEffect(() => setOpen(new Set()), [id]);
  useEffect(() => {
    if (newest !== undefined) {
      setOpen((current) => new Set([...current, newest]));
    }
  }, [id, newest]);

  if (errors.length === 0) {
    return html`<h3>Failures</h3><p class="none">None.</p>`;
  }

  const toggle = (attempt) =>
    setOpen((current) => {
      const next = new Set(current);
      next.has(attempt) ? next.delete(attempt) : next.add(attempt);
      return next;
    });

  return html`
    <h3>Failures</h3>
    <ol class="failures">
      ${errors.map((error) => {
        const isOpen = open.has(error.attempt);
        return html`
          <li key=${error.attempt} class=${isOpen ? "open" : ""}>
            <button class="failure-summary" aria-expanded=${isOpen} onClick=${() => toggle(error.attempt)}>
              <span class="attempt">#${error.attempt}</span>
              <span class="when">${ago(error.failed_at, Date.now())}</span>
              <span class="what">
                ${error.error_type && html`<b>${error.error_type}</b>: `}${firstLine(error.message)}
              </span>
            </button>
            ${isOpen &&
            html`
              <div class="failure-detail">
                <p>Failed ${formatTimestamp(error.failed_at)}, after ${formatDuration(Math.max(0, error.failed_at - error.dequeued_at))}</p>
                <pre>${error.message}</pre>
                ${error.backtrace && html`<pre class="backtrace">${error.backtrace}</pre>`}
              </div>
            `}
          </li>
        `;
      })}
    </ol>
    ${more && html`<p class="none">Older failures aren't shown yet.</p>`}
  `;
}

function firstLine(text) {
  return text.split("\n", 1)[0];
}
