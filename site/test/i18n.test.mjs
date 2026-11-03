// Every string the page scripts ask for by a literal key (t("…"), a
// sentence ["…", {…}]) is in both languages' files, as a "js." key.
import assert from "node:assert/strict";
import {readdirSync, readFileSync} from "node:fs";
import {test} from "node:test";

const site = new URL("../", import.meta.url);
const strings = lang => JSON.parse(readFileSync(new URL(`i18n/${lang}.json`, site), "utf8"));
const scripts = readdirSync(new URL("src/assets/", site), {recursive: true})
  .filter(f => f.endsWith(".js"))
  .map(f => [f, readFileSync(new URL(`src/assets/${f}`, site), "utf8")]);

// t("key"), tHtml("key", …), tHtmlWith("key", …); ["key", {…}] (a sentence, entities.js).
const USES = [/\bt(?:Html|HtmlWith)?\("([a-z][\w.]*)"/g, /\["([a-z]+\.[\w.]+)", \{/g];

test("i18n: both languages have the same keys", () => {
  const [en, ru] = [strings("en"), strings("ru")];
  assert.deepEqual(Object.keys(ru).sort(), Object.keys(en).sort());
});

test("i18n: every literal key the scripts use is there", () => {
  const missing = [];
  for (const lang of ["en", "ru"]) {
    const have = strings(lang);
    for (const [file, text] of scripts)
      for (const re of USES) for (const [, key] of text.matchAll(re)) if (!(`js.${key}` in have)) missing.push(`${lang}: ${key} (${file})`);
  }
  assert.deepEqual(missing, []);
});
