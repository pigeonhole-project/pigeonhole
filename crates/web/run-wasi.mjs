// Run a wasm32-wasip1 binary under Node's WASI: node run-wasi.mjs <file.wasm>
import { readFile } from "node:fs/promises";
import { WASI } from "node:wasi";
import { tmpdir } from "node:os";
// The temp dir as /tmp: the smoke keeps fake channels on disk there.
const wasi = new WASI({ version: "preview1", args: [], env: { TMPDIR: "/tmp" }, preopens: { "/tmp": tmpdir() }, returnOnExit: true });
const mod = await WebAssembly.compile(await readFile(process.argv[2]));
const inst = await WebAssembly.instantiate(mod, wasi.getImportObject());
process.exitCode = wasi.start(inst);
