// The demo's knobs (demo/settings.js): defaults, ranges, changes told.
import assert from "node:assert/strict";
import {test} from "node:test";
import {clamp, DEFAULTS, GROUPS, KNOBS, reset, set, settings, watch} from "../src/assets/demo/settings.js";

test("settings: every knob's default is within its range, in a known group, once", () => {
  assert.equal(new Set(KNOBS.map(k => k.key)).size, KNOBS.length);
  for (const k of KNOBS) {
    assert.ok(k.min <= k.def && k.def <= k.max, k.key);
    assert.ok(GROUPS.includes(k.group), k.key);
  }
  assert.deepEqual(settings, DEFAULTS, "no browser here: nothing kept");
});

test("settings: a value is clamped to its range; not a number is the default", () => {
  const k = KNOBS.find(x => x.key === "floors");
  assert.equal(clamp(k, 1000), k.max);
  assert.equal(clamp(k, -1), k.min);
  assert.equal(clamp(k, "12"), 12);
  assert.equal(clamp(k, "lots"), k.def);
});

test("settings: set and reset tell the watchers which knobs changed", () => {
  const told = [];
  const unwatch = watch(knobs => told.push(knobs.map(k => k.key)));
  set("floors", 12);
  set("spacing", 0);
  set("nonsense", 1);
  assert.equal(settings.floors, 12);
  reset();
  reset();
  unwatch();
  set("floors", 3);
  reset();
  assert.deepEqual(told, [["floors"], ["spacing"], ["spacing", "floors"]]);
  assert.deepEqual(settings, DEFAULTS);
});
