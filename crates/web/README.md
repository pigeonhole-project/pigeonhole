# pigeonhole in the browser

`pigeonhole-web` builds the same stack the binary builds for `kind = "memory"`
stores — the metadata tablet on openraft, a replica group of two blob stores
(`telegram` and `discord`), the chunk store with zstd, the S3 handlers — in
one process, without HTTP, for `wasm32-wasip1`. The site runs it in a Web
Worker under a WASI shim.

```
rustup target add wasm32-wasip1
rustup component add llvm-tools   # llvm-ar: zstd's C objects for wasm
make web-smoke                    # build, then run src/main.rs under Node's WASI
```

What wasm needed from the rest of the workspace:

- tokio with only `io-util`, `macros`, `rt`, `sync`, `time` (the workspace
  default now; the binary and the ByteStream gateway add the rest);
- openraft without `rt-multi-thread` (vendor/openraft/VENDORED.md);
- compression through `pigeonhole_codec::blocking`, which runs inline where
  there are no threads.
