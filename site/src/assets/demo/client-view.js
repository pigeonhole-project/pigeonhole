// Your panel: uploads (files, or samples made here) and the bucket's
// objects, each with read, copy and delete. On a real node, a bucket picker.
//
// on: {bucket(name), upload(name, ArrayBuffer), tooBig(), read(key, size),
// copy(key), remove(key)}. Returns {nodes, setObjects([[key, size]])}.
import {button, h} from "../lib/dom.js";
import {bytes, t} from "../lib/i18n.js";

export const MAX_UPLOAD = 64 << 20; // it all lives in the tab's memory

export function clientView({buckets, bucket, objects, admin}, on) {
  const view = document.getElementById("tpl-client").content.cloneNode(true);
  const drop = view.querySelector("#drop"),
    rows = view.querySelector("#objects");
  let current = objects;

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
  return {nodes: [...view.childNodes], setObjects};
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
