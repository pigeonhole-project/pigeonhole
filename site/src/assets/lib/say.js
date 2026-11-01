import {bytes, t} from "./i18n.js";

// What an S3 write did, as a say.* string and its values: `posts` are
// what one channel got for it ({kind, bytes}). Null for other calls.
export function s3Sentence(op, posts) {
  const key = op.key || op.bucket || "";
  const chunks = n => t("chunks", {n});
  if (op.op === "PutObject") {
    const size = bytes(+op.size);
    if (String(op.inline) === "true") return ["say.PutObjectInline", {key, size}];
    const stored = posts.reduce((a, p) => a + p.bytes, 0);
    const vars = {key, size, stored: bytes(stored), chunks: chunks(posts.length || +op.extents)};
    if (posts.length && posts.every(p => p.kind === "text")) return ["say.PutObjectText", vars];
    return [stored && stored < op.size * 0.8 ? "say.PutObjectZ" : "say.PutObject", vars];
  }
  if (op.op === "CopyObject") return ["say.CopyObject", {key, chunks: chunks(+op.extents)}];
  if (op.op === "DeleteObject") return ["say.DeleteObject", {key, n: +op.released}];
  return null;
}
