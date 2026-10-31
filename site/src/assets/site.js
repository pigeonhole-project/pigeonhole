// Shared by every page: the theme toggle and the i18n helpers.
(function () {
  const root = document.documentElement;
  const saved = localStorage.getItem("ph-theme");
  if (saved) root.dataset.theme = saved;
  const btn = document.getElementById("theme");
  if (btn) btn.addEventListener("click", () => {
    const dark = root.dataset.theme ? root.dataset.theme === "dark" : matchMedia("(prefers-color-scheme: dark)").matches;
    root.dataset.theme = dark ? "light" : "dark";
    localStorage.setItem("ph-theme", root.dataset.theme);
  });
})();

const PR = new Intl.PluralRules(window.LANG || "en");

// t("say.PutObject", {key: "x"}) → the string with {placeholders} filled.
// A plural string is an object of Intl.PluralRules categories; pass {n}.
function t(key, vars = {}) {
  let s = (window.I18N || {})[key];
  if (s == null) return key;
  if (typeof s === "object") s = s[PR.select(vars.n)] ?? s.other;
  return s.replace(/\{(\w+)\}/g, (_, k) => (vars[k] ?? `{${k}}`));
}

function bytes(n) {
  const u = window.LANG === "ru" ? ["Б", "КиБ", "МиБ", "ГиБ"] : ["B", "KiB", "MiB", "GiB"];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  const v = i === 0 ? String(n) : n.toFixed(n < 10 ? 1 : 0);
  return `${window.LANG === "ru" ? v.replace(".", ",") : v} ${u[i]}`;
}

// Text-only element: values that come from data never go through innerHTML.
function h(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text != null) e.textContent = text;
  return e;
}
