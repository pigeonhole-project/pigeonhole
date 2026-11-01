// The shared modules: strings, numbers, the op protocol, the S3 sentences.
import assert from "node:assert/strict";
import {beforeEach, test} from "node:test";
import {bytes, esc, t, tHtml} from "../src/assets/lib/i18n.js";
import {frame, unframe} from "../src/assets/lib/ops.js";
import {s3Sentence} from "../src/assets/lib/say.js";

beforeEach(() => {
  globalThis.LANG = "en";
  globalThis.I18N = {
    hi: "Hi, <b>{name}</b>",
    chunks: {one: "{n} chunk", other: "{n} chunks"},
  };
});

test("t fills placeholders and keeps unknown ones and keys", () => {
  assert.equal(t("hi", {name: "x"}), "Hi, <b>x</b>");
  assert.equal(t("hi"), "Hi, <b>{name}</b>");
  assert.equal(t("missing.key"), "missing.key");
});

test("t picks the plural form for n", () => {
  assert.equal(t("chunks", {n: 1}), "1 chunk");
  assert.equal(t("chunks", {n: 3}), "3 chunks");
});

test("tHtml escapes the values, not the string", () => {
  assert.equal(tHtml("hi", {name: '<img src="x">'}), "Hi, <b>&lt;img src=&quot;x&quot;&gt;</b>");
  assert.equal(esc("a&b"), "a&amp;b");
});

test("bytes in binary units, in the page's language", () => {
  assert.equal(bytes(512), "512 B");
  assert.equal(bytes(1536), "1.5 KiB");
  assert.equal(bytes(7 << 20), "7.0 MiB");
  assert.equal(bytes(300 << 20), "300 MiB");
  globalThis.LANG = "ru";
  assert.equal(bytes(1536), "1,5 КиБ");
});

test("frame: the request with its body's length, a newline, the body", () => {
  const body = new Uint8Array([1, 2, 3]);
  const out = frame({op: "put", key: "k"}, body.buffer);
  const nl = out.indexOf(10);
  assert.deepEqual(JSON.parse(new TextDecoder().decode(out.subarray(0, nl))), {op: "put", key: "k", len: 3});
  assert.deepEqual([...out.subarray(nl + 1)], [1, 2, 3]);
  assert.deepEqual(JSON.parse(new TextDecoder().decode(frame({op: "list"}))), {op: "list", len: 0});
});

test("unframe: the reply apart from the log; stray lines dropped", () => {
  const {reply, events} = unframe(['{"fields":{"message":"a"}}', "not json", '{"reply":{"ok":true}}']);
  assert.deepEqual(reply, {ok: true});
  assert.deepEqual(events, [{fields: {message: "a"}}]);
});

test("s3Sentence: a put as text, as files, compressed, inline", () => {
  const put = {op: "PutObject", key: "k", size: "3000000", extents: "2"};
  assert.equal(s3Sentence(put, [{kind: "text", bytes: 400}])[0], "say.PutObjectText");
  assert.equal(s3Sentence(put, [{kind: "document", bytes: 2_900_000}])[0], "say.PutObject");
  assert.equal(s3Sentence(put, [{kind: "document", bytes: 1000}])[0], "say.PutObjectZ");
  assert.equal(s3Sentence({...put, inline: "true"}, [])[0], "say.PutObjectInline");
  // No posts seen: the chunks the gateway counted.
  assert.equal(s3Sentence(put, [])[1].chunks, "2 chunks");
});

test("s3Sentence: copies and deletes; nothing for the rest", () => {
  assert.deepEqual(s3Sentence({op: "CopyObject", key: "c", extents: "1"}, []), ["say.CopyObject", {key: "c", chunks: "1 chunk"}]);
  assert.deepEqual(s3Sentence({op: "DeleteObject", key: "d", released: "2"}, []), ["say.DeleteObject", {key: "d", n: 2}]);
  assert.equal(s3Sentence({op: "GetObject", key: "g"}, []), null);
});
