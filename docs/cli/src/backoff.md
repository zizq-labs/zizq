# Backoff & Retry Policies

When jobs fail, often the errors that lead to failure are transient in nature
and the job may succeed when retried. For example, network connectivity with
an external service may have been interrupted, or storage on some server is at
capacity.

Zizq applies exponential backoff to jobs that fail. Errors are captured on the
job's error list and the job is scheduled to run again at a later time,
increasing with each successive failure until a configured retry limit is
reached. Once the retry limit is reached, the job is not rescheduled, but is
marked `dead`.

The logic applied to jobs when determining how to handle retries is known as
the _backoff policy_. Clients can explicitly specify their own policies on a
per-job basis, but the server otherwise applies its default policy, which can
be configured when starting the server.

## Backoff Policy Structure

There are two logical parts to the backoff policy:

1. The retry limit (maximum number of permitted retries).
2. The exponential backoff formula itself.

The retry limit is self-explanatory. If the limit is set to `3`, for example
the job may fail once, retry, twice, retry, three times, retry, but on the
fourth failure the job will not retry.

The formula for exponential backoff requires further explanation. The formula
is:

``` text
t = B + (a^E) + (a * rand(0 to J))
```

Where:

* `t` is the delay to apply before retrying
* `B` is the base delay applied to all retries
* `a` is the number of previous attempts
* `E` is the backoff exponent (optionally fractional)
* `J` is a random jitter used to spread retries

The variables `B`, `E` and `J` are configurable.

## Zizq Defaults

The default backoff policy uses a retry limit of `25` and uses the following
parameters for the backoff formula:

``` text
B = 15s
E = 4
J = 30s
```

This gives roughly 3 weeks of total retry time before the job is eventually
moved to the `dead` list.

## Adjusting the Backoff Curve

You can adjust the inputs in the chart below to see how changing these
parameters affects the backoff curve. The defaults are very reasonable. There
are two lines on the chart due to the presence of the random jitter, which is
designed to avoid clusters of failures all retrying at the same time. An actual
retry could occur anywhere within the band.

<style>
  #backoff-chart-container {
    max-width: 720px;
    margin: 1.5em auto;
  }

  .bc-inputs {
    display: flex;
    flex-wrap: wrap;
    gap: 1em;
    margin-bottom: 1em;
  }

  .bc-param {
    display: flex;
    align-items: center;
    gap: 0.4em;
    font-size: 0.9em;
  }

  .bc-param input[type=number] {
    width: 4.5em;
    padding: 2px 4px;
    font-size: 0.9em;
    background: var(--theme-popup-bg, #1e1e2e);
    color: var(--fg, #cdd6f4);
    border: 1px solid var(--sidebar-fg, #585b70);
    border-radius: 3px;
  }

  .bc-param input[type=range] {
    width: 100px;
  }

  .bc-chart {
    position: relative;
    width: 100%;
  }

  #bc-canvas {
    width: 100%;
    height: 340px;
    display: block;
    cursor: crosshair;
  }

  #bc-tooltip {
    display: none;
    position: absolute;
    pointer-events: none;
    background: var(--sidebar-bg, #1e1e2e);
    color :var(--fg, #cdd6f4);
    border: 1px solid var(--sidebar-fg, #585b70);
    border-radius: 4px;
    padding: 4px 8px;
    font-size: 0.82em;
    white-space: nowrap;
    z-index: 10;
  }
</style>

<div id="backoff-chart-container">
  <div class="bc-inputs">
    <label class="bc-param">Base (B)
      <input type="number" id="bc-base-num" min="0" value="15" step="0.1">s
      <input type="range" id="bc-base" min="0" max="300" value="15" step="0.1">
    </label>
    <label class="bc-param">Exponent (E)
      <input type="number" id="bc-exp-num" min="0.1" value="4" step="0.1">
      <input type="range" id="bc-exp" min="1" max="8" value="4" step="0.1">
    </label>
    <label class="bc-param">Jitter (J)
      <input type="number" id="bc-jitter-num" min="0" value="30" step="0.1">s
      <input type="range" id="bc-jitter" min="0" max="300" value="30" step="0.1">
    </label>
    <label class="bc-param">Retry Limit
      <input type="number" id="bc-retries-num" min="1" value="25" step="1">
      <input type="range" id="bc-retries" min="1" max="100" value="25" step="1">
    </label>
  </div>
  <div class="bc-chart">
    <canvas id="bc-canvas">
    </canvas>
    <div id="bc-tooltip"></div>
  </div>
</div>

## Configuration Options

The defaults can be configured by using the following command line arguments
and environment variables.

* `--default-retry-limit`, `ZIZQ_DEFAULT_RETRY_LIMIT`
* `--default-backoff-base`, `ZIZQ_DEFAULT_BACKOFF_BASE`
* `--default-backoff-exponent`, `ZIZQ_DEFAULT_BACKOFF_EXPONENT`
* `--default-backoff-jitter`, `ZIZQ_DEFAULT_BACKOFF_JITTER`

Values for `--default-backoff-base` and `--default-backoff-litter` are either
provided in raw milliseconds, or with an explicit unit, such as `12.5s`.

> [!NOTE]
> When any of `--default-backoff-base`, `--default-backoff-exponent` or
> `--default-backoff-jitter` are provided, all three must be provided as they
> form a single formula in unison.
