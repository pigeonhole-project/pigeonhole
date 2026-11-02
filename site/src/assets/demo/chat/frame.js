// An outbox's post as the Raft frame it holds (m.frame, opened by the node
// with the cluster's key: openraft_chat::view): a line per envelope, and
// every field of each, for the post opened.
import {bytes, t} from "../../lib/i18n.js";

// {head, lines: [one per envelope], fields: [[[name, value]…]: the frame's,
// then each envelope's]}; head: why a post holds no frame of this cluster.
export function frameModel(f) {
  if (!f) return null;
  if (f.error) return {head: t("frame.unreadable", {error: f.error}), lines: [], fields: []};
  return {
    head: null,
    lines: f.envelopes.map(envelopeLine),
    fields: [Object.entries({from: f.from, seq: f.seq}), ...f.envelopes.map(inOrder)],
  };
}

// An envelope's fields as openraft_chat::view declares them (the node's
// JSON comes sorted by name).
const ORDER = [
  "to",
  "rpc",
  "kind",
  "reply",
  "result",
  "error",
  "term",
  "by",
  "committed",
  "prev",
  "entries",
  "commit",
  "last",
  "matched",
  "offset",
  "bytes",
  "done",
];
const rank = k => (ORDER.includes(k) ? ORDER.indexOf(k) : ORDER.length);
const inOrder = e => Object.entries(e).sort(([a], [b]) => rank(a) - rank(b));

const range = ([a, b]) => (a === b ? `${a}` : `${a}–${b}`);

// "→ 2: heartbeat, term 3, commit 41"; a response, what it answers.
function envelopeLine(e) {
  const parts = [];
  const answer = r => t(`frame.result_${r}`, {error: e.error ?? "", matched: e.matched ?? ""});
  if (e.reply) parts.push(`${t(`frame.reply_${e.kind}`)} ${answer(e.result)}`);
  else if (e.kind === "vote") parts.push(t("frame.vote", {by: e.by}));
  else if (e.kind === "snapshot") parts.push(t("frame.snapshot", {size: bytes(e.bytes), offset: e.offset}) + (e.done ? `, ${t("frame.snapshot_done")}` : ""));
  else parts.push(e.entries ? t("frame.append", {entries: range(e.entries)}) : t("frame.heartbeat"));
  if (e.term != null) parts.push(t("frame.term", {term: e.term}));
  if (e.commit != null) parts.push(t("frame.commit", {n: e.commit}));
  if (e.last != null && !e.reply) parts.push(t(e.kind === "snapshot" ? "frame.snapshot_last" : "frame.last", {n: e.last}));
  return `→ ${e.to}: ${parts.join(", ")}`;
}
