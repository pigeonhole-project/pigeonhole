// The page's strings (window.I18N, the i18n file's "js." keys) and numbers
// in its language (window.LANG). Read on each call: a test sets them late.
const lang = () => globalThis.LANG || "en";
let plurals = null;

// t("say.PutObject", {key: "x"}) → the string with {placeholders} filled.
// A plural string is an object of Intl.PluralRules categories; pass {n}.
export function t(key, vars = {}) {
  return fill(key, vars, v => v);
}

// The same for a string that holds HTML (<b>, <code>): the values filled
// in are escaped, so data never becomes markup.
export function tHtml(key, vars = {}) {
  return fill(key, vars, esc);
}

function fill(key, vars, value) {
  let s = globalThis.I18N?.[key];
  if (s == null) return key;
  if (typeof s === "object") {
    if (plurals?.resolvedOptions().locale !== lang()) plurals = new Intl.PluralRules(lang());
    s = s[plurals.select(vars.n)] ?? s.other;
  }
  return s.replace(/\{(\w+)\}/g, (_, k) => (vars[k] == null ? `{${k}}` : value(vars[k])));
}

export const esc = v => String(v).replace(/[&<>"]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"})[c]);

// 1536 → "1.5 KiB" ("1,5 КиБ" in Russian).
export function bytes(n) {
  const ru = lang() === "ru";
  const u = ru ? ["Б", "КиБ", "МиБ", "ГиБ"] : ["B", "KiB", "MiB", "GiB"];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) {
    n /= 1024;
    i++;
  }
  const v = i === 0 ? String(n) : n.toFixed(n < 10 ? 1 : 0);
  return `${ru ? v.replace(".", ",") : v} ${u[i]}`;
}
