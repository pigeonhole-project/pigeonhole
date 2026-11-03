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
