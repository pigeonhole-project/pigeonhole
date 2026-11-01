// Every page: the theme toggle, remembered in this browser.
const root = document.documentElement;
const saved = localStorage.getItem("ph-theme");
if (saved) root.dataset.theme = saved;
document.getElementById("theme")?.addEventListener("click", () => {
  const dark = root.dataset.theme ? root.dataset.theme === "dark" : matchMedia("(prefers-color-scheme: dark)").matches;
  root.dataset.theme = dark ? "light" : "dark";
  localStorage.setItem("ph-theme", root.dataset.theme);
});
