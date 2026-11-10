# Working in this repository

For coding agents (Claude Code sessions and others). Several sessions often
work at once, each on its own branch; these rules keep them out of each
other's way and keep the disk from filling up.

## A worktree per task

Never work in the main checkout. Make a worktree for the task with

    scripts/wt new <branch>          # ../pigeonhole-<branch>, a new branch off main

and, once the branch is in main, drop all of it (the directory, its
build, the branch) with

    scripts/wt done <branch>

`scripts/wt list` shows every worktree (in main or not, uncommitted work,
touched in the last hour, `target/` size); `scripts/wt clean` lists the
merged, clean, idle ones and `scripts/wt clean --yes` removes them. Don't
remove another session's worktree: ask it first.

## Tools

`mise install` installs the tools the tasks call, at the versions in
`mise.toml` (Rust comes from rustup). Sessions get them on `PATH` through
the SessionStart hook in `.claude/settings.json`; a terminal needs mise's
shims on `PATH` (`eval "$(mise activate zsh --shims)"`).

## Builds

`scripts/wt new` makes the worktree a Ferry agent when Ferry's warm base
exists (`mise run bootstrap` once; `ferry base --rev main --watch --gc` keeps
it on main): its target dir is a copy-on-write fork of main's build, kept in
Ferry's store, so the first build compiles only what the branch changed.
The hook puts Ferry's cargo shim first on `PATH`; it sends each cargo call
to the build of the worktree it runs in. Without a base `scripts/wt new`
makes a plain worktree with its own `target/`.

Don't set `CARGO_TARGET_DIR` to share builds between worktrees: cargo
compares mtimes, so a session can rebuild nothing and run another
session's code, and sessions building at once wait on cargo's lock.

`make lint`, `make test` and `make sim` take turns with the other sessions'
heavy commands (heavy: a few machine-wide slots). One may wait up to 5
minutes for a slot before it runs, so give it a long timeout or run it in
the background. Run other heavy commands the same way: `heavy -- cargo
build --release`.

## Before merging into main

Run what the branch touches:

- Rust: `make lint` (rustfmt check, clippy with `-D warnings`), `make test`
  (cargo-nextest: every test binary at once, ~30 s for the workspace), and
  `make sim` for meta, blob, storage or sim changes.
- Dependencies, shell scripts or text: `make gates` (the crate layers in
  easement.yml, cargo-deny, cargo-machete, ShellCheck, typos).
- The site's JavaScript: `make lint-js`, `make test-js`.
- The web node (crates/web) or the demo: `make web-smoke`.

## Merging: one session at a time

The others wait while one session merges, so keep that short: the slow
checks come before it.

1. Rebase on the latest local main and run the checks above in your
   worktree. Nobody waits on these.
2. Get the user's go to merge.
3. Tell the other sessions "merging <branch>" and wait if one of them is
   merging.
4. Rebase again. If main moved since step 1, run `make lint` and `make test`
   again (and `make sim` or `make web-smoke` only if the new commits in main
   touch what they check); if it didn't, nothing to run. `git merge --no-ff`.
5. Tell them "merged <branch> as <hash>", then `scripts/wt done <branch>`.

Nobody pushes without the user asking.
