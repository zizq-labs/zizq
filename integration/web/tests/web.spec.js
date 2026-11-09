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
