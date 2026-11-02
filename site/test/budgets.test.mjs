import assert from "node:assert/strict";
import {test} from "node:test";
import {budgetsOf, sendLevel} from "../src/assets/demo/budgets.js";

const b = (tokens, capacity, extra = {}) => ({tokens, capacity, per_sec: 1, cooling_ms: 0, waiting: 0, ...extra});
const entry = (bot, chat, send, whole, service = "telegram") => ({service, bot, chat, classes: {send, read: b(5, 5)}, bot_total: whole});

test("budgetsOf: a channel's bots, by service and chat", () => {
  const all = [entry("fake-bot-1", "-100", b(1, 2), b(9, 10)), entry("fake-bot-1", "-200", b(2, 2), b(9, 10)), entry("7", "-100", b(1, 2), b(1, 2), "discord")];
  assert.equal(budgetsOf({kind: "telegram", scope: -100}, all).length, 1);
  assert.equal(budgetsOf({kind: "discord", scope: "-100"}, all)[0].bot, "7");
  assert.deepEqual(budgetsOf({kind: "telegram", scope: "-300"}, all), []);
});

test("sendLevel: the worst of the channel's bots, the bot's own budget too", () => {
  assert.equal(sendLevel([]), null);
  assert.deepEqual(sendLevel([entry("a", "-1", b(20, 20), b(25, 25))]), {frac: 1, state: "ok"});
  assert.deepEqual(sendLevel([entry("a", "-1", b(20, 20), b(5, 25))]), {frac: 0.2, state: "low"}, "the bot itself is low");
  const busy = sendLevel([entry("a", "-1", b(10, 20), b(25, 25)), entry("b", "-1", b(18, 20, {waiting: 2}), b(25, 25))]);
  assert.deepEqual(busy, {frac: 0.9, state: "busy"}, "calls waiting outrank a lower bar");
  assert.equal(sendLevel([entry("a", "-1", b(0, 3, {cooling_ms: 4000}), b(9, 10))]).state, "busy", "a rate limit cools it");
});
