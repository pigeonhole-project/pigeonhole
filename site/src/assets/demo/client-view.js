// Your panel: in a cluster, which node your requests go to; uploads (files,
// or samples made here) and the bucket's objects, each with read, copy and
// delete. On a real node, a bucket picker.
//
// cluster: {nodes: [id], node: the one picked or null (the leader), leader, cut: Set}.
// on: {bucket(name), upload(name, ArrayBuffer), tooBig(), read(key, size),
// copy(key), remove(key), sendTo(id | null)}. Returns {nodes,
// setObjects([[key, size]]), setCluster(cluster)}: the cluster changed (a
// new leader, a node cut off), its picks anew.
import {button, h} from "../lib/dom.js";
import {bytes, t} from "../lib/i18n.js";
import {nodeLink} from "./entities.js";

const MAX_UPLOAD = 64 << 20; // it all lives in the tab's memory

export function clientView({buckets, bucket, objects, admin, cluster}, on) {
  const view = document.getElementById("tpl-client").content.cloneNode(true);
  const drop = view.querySelector("#drop"),
    rows = view.querySelector("#objects");
  let current = objects;

  // A cluster: the leader, whichever it is, or a node of your choosing (a
  // follower turns you away: only the leader serves).
  const sendTo = view.querySelector("#send-to"),
    picks = view.querySelector("#send-to-picks"),
    note = view.querySelector("#send-to-note");
  function setCluster(cluster) {
    sendTo.hidden = !cluster;
    if (!cluster) return;
    const pick = (label, id) => {
      const b = button("btn node-pick", label, () => on.sendTo(id));
      b.setAttribute("aria-pressed", String(cluster.node === id));
      return b;
    };
    picks.replaceChildren(
      pick(t("demo.send_leader"), null),
      ...cluster.nodes.map(id => pick(`${t("nodes.node", {n: id})}${id === cluster.leader ? " ★" : ""}${cluster.cut.has(id) ? " ✂" : ""}`, id)),
    );
    if (cluster.node == null && cluster.leader != null) note.replaceChildren(t("demo.send_now"), " ", nodeLink(cluster.leader));
    else note.textContent = cluster.node == null ? "" : t("demo.send_fixed");
  }
  setCluster(cluster);

  if (admin) {
    const pick = h("select", "bucket-pick");
    pick.setAttribute("aria-label", t("admin.bucket"));
    for (const b of buckets) {
      const opt = h("option", "", b);
      opt.value = b;
      opt.selected = b === bucket;
      pick.append(opt);
    }
    pick.addEventListener("change", () => on.bucket(pick.value));
    drop.before(pick);
  }

  async function upload(files) {
    for (const f of files) {
      if (f.size > MAX_UPLOAD) on.tooBig();
      else await on.upload(f.name, await f.arrayBuffer());
    }
  }
  const file = view.querySelector("#file");
  file.addEventListener("change", async () => {
    await upload([...file.files]);
    file.value = "";
  });
  drop.addEventListener("dragover", e => {
    e.preventDefault();
    drop.classList.add("over");
  });
  drop.addEventListener("dragleave", () => drop.classList.remove("over"));
  drop.addEventListener("drop", e => {
    e.preventDefault();
    drop.classList.remove("over");
    upload([...e.dataTransfer.files]);
  });
  for (const b of view.querySelectorAll("[data-sample]")) {
    b.addEventListener("click", () => {
      const [name, body] =
        b.dataset.sample === "photo" ? [fresh(current, "photo", ".jpg"), randomBytes(7 << 20)] : [fresh(current, "notes", ".md"), sampleText(3 << 20)];
      on.upload(name, body);
    });
  }

  function setObjects(objects) {
    current = objects;
    rows.replaceChildren();
    if (!objects.length) {
      const tr = h("tr");
      tr.append(h("td", "empty", t("try.no_objects")));
      rows.append(tr);
    }
    for (const [key, size] of objects) {
      const tr = h("tr"),
        acts = h("td", "acts");
      acts.append(
        button("mini", t("try.read"), () => on.read(key, size)),
        button("mini", t("try.copy"), () => on.copy(key)),
        button("mini", t("try.delete"), () => on.remove(key)),
      );
      tr.append(h("td", "key", key), h("td", "n", bytes(size)), acts);
      rows.append(tr);
    }
  }
  setObjects(objects);
  return {nodes: [...view.childNodes], setObjects, setCluster};
}

// A new object each time: photo-1.jpg, photo-2.jpg… (an upload under a
// name already there overwrites it, as S3 does).
function fresh(objects, base, ext) {
  let i = 1;
  while (objects.some(([k]) => k === `${base}-${i}${ext}`)) i++;
  return `${base}-${i}${ext}`;
}

// Bytes that do not compress (getRandomValues gives 64 KiB at a time).
function randomBytes(n) {
  const buf = new Uint8Array(n);
  for (let i = 0; i < n; i += 65536) crypto.getRandomValues(buf.subarray(i, i + 65536));
  return buf.buffer;
}

// Text that compresses well.
function sampleText(n) {
  const text = `${t("demo.sample_text")}\n`;
  return new TextEncoder().encode(text.repeat(Math.ceil(n / text.length))).buffer;
}
