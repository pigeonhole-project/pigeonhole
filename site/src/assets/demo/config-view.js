// The config the node (or every node of a cluster) runs: edited and
// restarted on, put back to the demo's, or everything this browser keeps
// erased. Read-only on a real node.
//
// on: {restart(text), erase()}. Returns {nodes, setError(text)}.
import {within} from "../lib/dom.js";

export function configView({config, demo, admin}, on) {
  const view = document.getElementById("tpl-config").content.cloneNode(true);
  const $ = within(view);
  const editor = $("config-text"),
    error = $("config-error");
  editor.value = config;
  editor.readOnly = admin;
  const restart = () => on.restart(editor.value);
  $("config-reset").addEventListener("click", () => {
    editor.value = demo;
    error.textContent = "";
  });
  $("config-apply").addEventListener("click", restart);
  editor.addEventListener("keydown", e => {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault();
      restart();
    }
  });
  $("erase").addEventListener("click", on.erase);
  return {
    nodes: [...view.childNodes],
    setError: text => {
      error.textContent = text;
    },
  };
}
