// The admin web UI, in real browsers, against a real server.

import { expect, test } from "./fixtures.js";

const job = (fields = {}) => ({ type: "send_email", queue: "emails", ...fields });
const jobs = (n, fields) => Array.from({ length: n }, () => job(fields));

const footer = (page) => page.locator(".range");
const rows = (page) => page.locator(".jobs tbody tr");

// Every test also checks the page raised no errors along the way.
test.beforeEach(async ({ page }) => {
  page.errors = [];
  page.on("pageerror", (error) => page.errors.push(error.message));
});

test.afterEach(async ({ page }) => {
  expect(page.errors, "errors on the page").toEqual([]);
});

test("connects and shows the server's status", async ({ page, zizq }) => {
  await page.goto(zizq.url);

  await expect(page.locator(".conn")).toHaveText("Connected");
  await expect(page.locator(".server")).toContainText(/v\d+\.\d+\.\d+/);
  await expect(page.locator(".server")).toContainText("Uptime");
});

test("shows each list's total and the total of every job", async ({ page, zizq }) => {
  await zizq.enqueue(jobs(3));
  await zizq.enqueue(jobs(2, { ready_at: Date.now() + 3_600_000 }));
  await zizq.take("emails", 1);

  await page.goto(zizq.url);

  await expect(page.locator("#tab-in_flight .tab-total")).toHaveText("1");
  await expect(page.locator("#tab-ready .tab-total")).toHaveText("2");
  await expect(page.locator("#tab-scheduled .tab-total")).toHaveText("2");
  await expect(page.locator(".jobs-total")).toHaveText("5 jobs");
});

test("lists ready jobs in priority order", async ({ page, zizq }) => {
  await zizq.enqueue([job({ priority: 500 }), job({ priority: 10 }), job({ priority: 100 })]);

  await page.goto(`${zizq.url}#ready`);

  await expect(rows(page).locator(".priority")).toHaveText(["10", "100", "500"]);
});

test("shows new jobs without a reload", async ({ page, zizq }) => {
  await page.goto(`${zizq.url}#ready`);
  await expect(page.locator(".empty")).toHaveText("No jobs ready.");

  await zizq.enqueue([job({ type: "arrived_later" })]);

  await expect(rows(page).locator(".type")).toHaveText(["arrived_later"]);
});

test("moves through a long list with the keyboard", async ({ page, zizq }) => {
  await zizq.enqueue(jobs(1000));
  await page.goto(`${zizq.url}#ready`);
  await expect(footer(page)).toContainText("of 1,000");

  const shown = await rows(page).count();
  await page.keyboard.press("PageDown");
  await expect(footer(page)).toContainText(`Rows ${shown + 1}–`);

  await page.keyboard.press("End");
  await expect(footer(page)).toContainText(`–1,000 of 1,000`);

  await page.keyboard.press("Home");
  await expect(footer(page)).toContainText("Rows 1–");
});

test("follows the end of a list as jobs arrive", async ({ page, zizq }) => {
  await zizq.enqueue(jobs(500));
  await page.goto(`${zizq.url}#ready`);
  await expect(footer(page)).toContainText("of 500");

  await page.keyboard.press("End");
  await expect(footer(page)).toContainText("following");

  await zizq.enqueue([job({ type: "arrived_later", priority: 60000 })]);

  await expect(footer(page)).toContainText("–501 of 501");
  await expect(rows(page).last().locator(".type")).toHaveText("arrived_later");

  await page.keyboard.press("ArrowUp");
  await expect(footer(page)).not.toContainText("following");
});

test("switches lists with the keyboard", async ({ page, zizq }) => {
  await page.goto(zizq.url);
  await expect(page.locator("#tab-in_flight")).toHaveAttribute("aria-selected", "true");

  await page.keyboard.press("Shift+ArrowRight");
  await expect(page.locator("#tab-ready")).toHaveAttribute("aria-selected", "true");

  await page.keyboard.press("3");
  await expect(page.locator("#tab-scheduled")).toHaveAttribute("aria-selected", "true");
  await expect(page).toHaveURL(/#scheduled$/);
});

test("switches lists from the queue overview", async ({ page, zizq }) => {
  await zizq.enqueue(jobs(100));
  await zizq.enqueue(jobs(100, { ready_at: Date.now() + 3_600_000 }));
  await page.goto(`${zizq.url}#ready`);
  await expect(footer(page)).toContainText("of 100");

  await page.locator(".segment.scheduled").click();

  await expect(page.locator("#tab-scheduled")).toHaveAttribute("aria-selected", "true");
  await expect(footer(page)).toContainText("of 100");
});

test("reconnects after the server restarts", async ({ page, zizq }) => {
  await zizq.enqueue(jobs(3));
  await page.goto(`${zizq.url}#ready`);
  await expect(page.locator(".conn")).toHaveText("Connected");

  await zizq.stop();
  await expect(page.locator(".conn")).toContainText("Disconnected");

  await zizq.start();
  await expect(page.locator(".conn")).toHaveText("Connected", { timeout: 20_000 });
  await expect(rows(page)).toHaveCount(3);
});

test("freezing holds the list still while the totals keep updating", async ({ page, zizq }) => {
  await zizq.enqueue(jobs(3, { priority: 500 }));
  await page.goto(`${zizq.url}#ready`);
  await expect(rows(page)).toHaveCount(3);

  await page.keyboard.press("p");
  await expect(page.locator(".freeze")).toHaveAttribute("aria-pressed", "true");
  await expect(page.locator(".list-footer .freeze")).toHaveText("Resume");

  await zizq.enqueue(jobs(2, { priority: 10, type: "arrived_frozen" }));
  await expect(page.locator("#tab-ready .tab-total")).toHaveText("5");
  await expect(rows(page)).toHaveCount(3);
  await expect(rows(page).locator(".type")).not.toContainText(["arrived_frozen"]);
  await expect(footer(page)).toContainText("of 3");

  await page.locator(".freeze").click();
  await expect(page.locator(".freeze")).toHaveAttribute("aria-pressed", "false");
  await expect(rows(page)).toHaveCount(5);
  await expect(rows(page).first().locator(".type")).toHaveText("arrived_frozen");
});

test("a frozen list can't be moved through", async ({ page, zizq }) => {
  await zizq.enqueue(jobs(1000));
  await page.goto(`${zizq.url}#ready`);
  await expect(footer(page)).toContainText("Rows 1–");

  await page.keyboard.press("p");
  await page.keyboard.press("PageDown");
  await page.keyboard.press("End");
  await page.mouse.move(400, 400);
  await page.mouse.wheel(0, 600);
  await page.waitForTimeout(500);
  await expect(footer(page)).toContainText("Rows 1–");

  await page.keyboard.press("p");
  await page.keyboard.press("PageDown");
  await expect(footer(page)).not.toContainText("Rows 1–");
});

test("clicking a job opens its details, with its payload and failures", async ({ page, zizq }) => {
  await zizq.enqueue([job({ type: "spawn", queue: "work", payload: { cmd: "convert" } })]);
  const [taken] = await zizq.take("work", 1);
  await zizq.fail(taken.id, {
    message: "fork failed: cannot allocate memory",
    error_type: "ForkError",
    backtrace: "worker.sh:12",
    retry_at: Date.now() + 3_600_000,
  });

  await page.goto(`${zizq.url}#scheduled`);
  await rows(page).first().click();

  const detail = page.locator(".detail");
  await expect(detail.locator(".detail-id")).toHaveText(taken.id);
  await expect(detail.locator(".status")).toHaveText("Scheduled");
  await expect(detail.locator(".payload")).toContainText('"cmd": "convert"');
  await expect(detail.locator(".failures li")).toHaveCount(1);
  await expect(detail.locator(".failures li").first()).toContainText("ForkError");
  await expect(detail.locator(".failure-detail")).toContainText("cannot allocate memory");
  await expect(detail.locator(".backtrace")).toHaveText("worker.sh:12");
  await expect(rows(page).first()).toHaveClass(/selected/);

  await page.keyboard.press("Escape");
  await expect(detail).toHaveCount(0);
});

test("a job's details follow it as it changes", async ({ page, zizq }) => {
  await zizq.enqueue([job({ queue: "work" })]);
  await page.goto(`${zizq.url}#ready`);
  await rows(page).first().click();
  await expect(page.locator(".detail .status")).toHaveText("Ready");

  await zizq.take("work", 1);

  await expect(page.locator(".detail .status")).toHaveText("In-Flight");
  await expect(page.locator(".detail .attributes")).toContainText("Taken");
});

test("clicking the open job again closes its details", async ({ page, zizq }) => {
  await zizq.enqueue(jobs(2));
  await page.goto(`${zizq.url}#ready`);

  await rows(page).first().click();
  await expect(page.locator(".detail")).toHaveCount(1);
  await rows(page).first().click();
  await expect(page.locator(".detail")).toHaveCount(0);
});
