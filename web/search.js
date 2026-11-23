// Searching the selected list by queue and type, as `zizq top`'s `/` does.
//
// The fields stay hidden behind a magnifying glass until it's clicked, or
// `/` is pressed, and close again when focus leaves them, unless something
// has been typed into them.
//
// The server walks the list from a job (or either end) and answers with the
// next match. A new search starts from the top of the list (or the bottom,
// searching backwards); searching again for the same thing carries on from
// the job whose details are open. The list panel then moves to the match
// and opens it (see `ListPanel`).

import { html, useEffect, useRef, useState } from "./preact.js";

// How long focus can be away from the search before it closes, so moving
// between its fields and buttons doesn't close it.
const CLOSE_DELAY_MS = 150;

// The values a search field lists, separated by commas or spaces. Within a
// field any value matches; across the fields, both must.
export function searchTerms(text) {
  return text.split(/[\s,]+/).filter(Boolean);
}

export function SearchBar({ stream, list, frozen }) {
  const [queues, setQueues] = useState("");
  const [types, setTypes] = useState("");
  const [open, setOpen] = useState(false);
  const searchRef = useRef();
  const queueRef = useRef();
  const lastSearch = useRef(null);

  const find = (direction) => {
    const query = { queues: searchTerms(queues), types: searchTerms(types) };
    if (frozen || (!query.queues.length && !query.types.length)) {
      return;
    }
    const key = JSON.stringify([list, query]);
    const again = key === lastSearch.current && stream.selected;
    lastSearch.current = key;
    const anchor = again
      ? { kind: "job_id", id: stream.selected }
      : { kind: direction === "forward" ? "start" : "end" };
    stream.find({ list, anchor, direction, query });
  };

  // Opens the fields, then focuses the first once they can take it.
  const openFields = () => {
    setOpen(true);
    requestAnimationFrame(() => queueRef.current?.focus());
  };

  // `/` jumps to the search, as in `zizq top`, and `n`/`N` find the next and
  // previous match of the last search from anywhere but a text field.
  const latest = useRef();
  latest.current = { find, openFields, empty: !queues && !types, hasSearched: lastSearch.current !== null };
  useEffect(() => {
    const onKeyDown = (event) => {
      if (event.defaultPrevented || event.ctrlKey || event.metaKey || event.altKey) {
        return;
      }
      if (event.target.closest?.("input, textarea, select, [contenteditable]")) {
        return;
      }
      if (event.key === "/") {
        event.preventDefault();
        latest.current.openFields();
      } else if ((event.key === "n" || event.key === "N") && latest.current.hasSearched) {
        event.preventDefault();
        latest.current.find(event.key === "n" ? "forward" : "backward");
      }
    };
    addEventListener("keydown", onKeyDown);
    return () => removeEventListener("keydown", onKeyDown);
  }, []);

  const onFieldKeyDown = (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      find(event.shiftKey ? "backward" : "forward");
    } else if (event.key === "Escape") {
      event.currentTarget.blur();
    }
  };

  // Closes once focus has gone elsewhere, if nothing was typed.
  const onFocusOut = () => {
    setTimeout(() => {
      if (latest.current.empty && !searchRef.current?.contains(document.activeElement)) {
        setOpen(false);
      }
    }, CLOSE_DELAY_MS);
  };

  const onToggle = () => {
    if (!open) {
      openFields();
    } else if (!queues && !types) {
      setOpen(false);
    } else {
      queueRef.current?.focus();
    }
  };

  const noMatch = stream.found && stream.found.position === null;
  const disabled = frozen;

  return html`
    <div
      ref=${searchRef}
      class="search"
      role="search"
      data-open=${open}
      title=${frozen ? "Resume to search" : ""}
      onFocusOut=${onFocusOut}
    >
      <button
        class="search-toggle"
        aria-label="Search (/)"
        aria-expanded=${open}
        title="Search (/)"
        onClick=${onToggle}
      >
        <span class="find-icon" aria-hidden="true"></span>
      </button>
      <div class="search-fields" inert=${!open}>
        <label>
          Queue
          <input
            ref=${queueRef}
            value=${queues}
            disabled=${disabled}
            spellcheck=${false}
            onInput=${(e) => setQueues(e.currentTarget.value)}
            onKeyDown=${onFieldKeyDown}
          />
        </label>
        <label>
          Type
          <input
            value=${types}
            disabled=${disabled}
            spellcheck=${false}
            onInput=${(e) => setTypes(e.currentTarget.value)}
            onKeyDown=${onFieldKeyDown}
          />
        </label>
        <button class="step" disabled=${disabled} title="Previous match (N)" onClick=${() => find("backward")}>
          ↑
        </button>
        <button class="step" disabled=${disabled} title="Next match (n)" onClick=${() => find("forward")}>
          ↓
        </button>
        ${noMatch && html`<span class="no-match">No more matches</span>`}
      </div>
    </div>
  `;
}
