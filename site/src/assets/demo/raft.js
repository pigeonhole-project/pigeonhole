// A cluster's Raft as the page follows it: each node's own word (its role,
// its term: the `cluster` op asked of it), when each last heard a leader
// (the frames the nodes logged, events.js), and a chronicle of what
// happened, as say.* sentences ([key, vars]), newest last (the last
// `settings.chronicle` kept).
//
// A follower that hears nothing from a leader for an election's timeout
// stands for leader: its election clock (`clock`) runs down from 1 to 0
// between two heartbeats.
import {settings} from "./settings.js";

export function makeRaftWatch() {
  const nodes = new Map(); // id → {role, term, heard: ms of the last word from a leader}
  const chronicle = []; // [{at: ms, sentence}]
  const node = id => {
    if (!nodes.has(id)) nodes.set(id, {role: null, term: 0, heard: null});
    return nodes.get(id);
  };
  // A sentence into the chronicle; returned, to be said now.
  function tell(at, sentence) {
    chronicle.push({at, sentence});
    while (chronicle.length > settings.chronicle) chronicle.shift();
    return sentence;
  }

  return {
    // A frame a node logged (events.js "raft"): a leader's heartbeat or
    // entries reaching a follower wind its clock up again; an answered vote
    // is told. Returns the sentences to say.
    frame(e, now) {
      const reached = e.via === "chat" ? e.at === "read" : e.at === "sent";
      if ((e.kind === "heartbeat" || e.kind === "append") && reached) node(e.to).heard = now;
      if (e.kind === "voted") return [tell(now, [e.granted ? "raft.vote_for" : "raft.vote_against", {from: e.from, to: e.to}])];
      return [];
    },
    // Node `id`'s own view ({role, term}): standing for leader, or leading
    // in a newer term, is told. Returns the sentences to say.
    view(id, {role, term}, now) {
      const n = node(id),
        said = [];
      if (role === "candidate" && (n.role !== "candidate" || term > n.term)) said.push(tell(now, ["raft.candidate", {n: id, term}]));
      if (role === "leader" && n.role !== "leader" && n.role != null) said.push(tell(now, ["raft.leader", {n: id, term}]));
      if (role !== "leader" && n.role === "leader" && role != null) said.push(tell(now, ["raft.stepped_down", {n: id, term}]));
      n.role = role;
      n.term = term;
      return said;
    },
    // Node `id` cut off from the others, or back.
    cut(id, on, now) {
      return tell(now, [on ? "raft.cut" : "raft.back", {n: id}]);
    },
    // How much of node `id`'s election clock is left (1…0): `timeout` ms
    // since it last heard a leader is 0. None for a leader, or a node not
    // heard of yet.
    clock(id, now, timeout) {
      const n = nodes.get(id);
      if (!n || n.role === "leader" || n.heard == null || !timeout) return null;
      return Math.max(0, 1 - (now - n.heard) / timeout);
    },
    // {role, term} of node `id` as it last said.
    of: id => nodes.get(id) ?? null,
    // The chronicle, oldest first.
    chronicle: () => [...chronicle],
  };
}
