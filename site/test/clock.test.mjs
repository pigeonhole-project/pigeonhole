// The shim's clock (wasi.js Clock): the module's time stands while held.
import assert from "node:assert/strict";
import {test} from "node:test";
import {Clock} from "../src/assets/wasi.js";

const sleep = ms => new Promise(r => setTimeout(r, ms));

test("clock: not held, it is the wall's", () => {
  const c = new Clock();
  assert.ok(Math.abs(c.now() - performance.now()) < 1);
  assert.ok(Math.abs(c.wall() - Date.now()) < 2);
});

test("clock: held, no time passes; released, it goes on as far behind as it stood", async () => {
  const c = new Clock();
  c.hold();
  const at = c.now();
  await sleep(30);
  assert.equal(c.now(), at, "held: the same instant");
  c.hold(); // a second hold changes nothing
  c.release();
  assert.ok(c.now() - at < 5, "released where it stood");
  assert.ok(performance.now() - c.now() >= 29, "behind the wall by the hold");
  assert.ok(Date.now() - c.wall() >= 28, "its wall clock as far behind");
  await sleep(20);
  assert.ok(c.now() - at >= 19, "and goes on");
  c.release(); // not held: nothing
  assert.equal(c.held, false);
});

test("clock: advance moves it on at once, never back; held or running", async () => {
  const c = new Clock();
  const at = c.now();
  c.advance(at + 1000);
  assert.ok(c.now() - at >= 1000, "running: on");
  c.hold();
  const held = c.now();
  c.advance(held + 250);
  assert.equal(c.now(), held + 250);
  c.advance(held);
  assert.equal(c.now(), held + 250, "never back");
});

test("clock: at a quarter speed, a quarter of the wall's time passes; until() is in the wall's ms", async () => {
  const c = new Clock();
  c.setSpeed(0.25);
  const at = c.now();
  await sleep(80);
  const passed = c.now() - at;
  assert.ok(passed >= 19 && passed < 40, `${passed} ms`);
  assert.ok(Math.abs(c.until(c.now() + 25) - 100) < 2);
  c.hold();
  assert.equal(c.until(c.now() + 25), Infinity, "held: never");
});
