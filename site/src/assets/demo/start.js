// The demo's node, started first: demo.html loads this module before
// main.js, so the worker (and its wasm download, the page's heaviest part)
// starts while main.js's modules are still coming. main.js imports `node`.
import {connect} from "./node.js";

export const node = connect(new URL(`../web-worker.js${new URL(import.meta.url).search}`, import.meta.url));
