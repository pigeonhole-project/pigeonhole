// The node's log as the page keeps it (demo/log-tail.js): the last lines,
// a level filter, each line as text.
import assert from "node:assert/strict";
import {test} from "node:test";
import {format, jsonl, makeTail, passes} from "../src/assets/demo/log-tail.js";

const line = {
  timestamp: "2026-11-05T12:03:41.2071Z",
  level: "DEBUG",
  target: "pigeonhole_blob::metrics",
  fields: {message: "store call", ms: 12, error: "a b"},
  spans: [{name: "store", instance: "tg-1", op: "put"}],
};

test("tail: the last `max` lines, the oldest gone first; cleared", () => {
  let max = 3;
  const tail = makeTail(() => max);
  tail.push([1, 2]);
  tail.push([3, 4, 5]);
  assert.deepEqual(tail.lines(), [3, 4, 5]);
  max = 2;
  tail.push([6]);
  assert.deepEqual(tail.lines(), [5, 6], "a smaller max takes effect at the next push");
  tail.clear();
  assert.deepEqual(tail.lines(), []);
});

test("passes: a line at the level picked or above", () => {
  assert.equal(passes({level: "WARN"}, "INFO"), true);
  assert.equal(passes({level: "DEBUG"}, "INFO"), false);
  assert.equal(passes({level: "ERROR"}, "WARN"), true);
});

test("format: time, level, where it comes from, message, fields; plain or coloured", () => {
  assert.equal(format(line, false), '12:03:41.207 DEBUG pigeonhole_blob::metrics store{instance=tg-1 op=put}: store call ms=12 error="a b"');
  assert.ok(format(line).includes("\x1b[34mDEBUG\x1b[0m"), "a debug line's level in blue");
  assert.equal(format({level: "INFO", target: "x", fields: {message: "hi"}}, false), "--:--:--.--- INFO  x: hi");
  assert.equal(format({level: "INFO", target: "x", fields: {message: "m", text: "z".repeat(500)}}, false), `--:--:--.--- INFO  x: m text=${"z".repeat(119)}…`);
});

test("jsonl: a JSON object a line, as kept", () => {
  assert.equal(jsonl([{a: 1}, {b: 2}]), '{"a":1}\n{"b":2}\n');
  assert.equal(jsonl([]), "");
});
