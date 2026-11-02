# Working in this repository

For coding agents (Claude Code sessions and others). Several sessions often
work at once, each on its own branch; these rules keep them out of each
other's way and keep the disk from filling up.

## A worktree per task

Never work in the main checkout. Make a worktree for the task with

    scripts/wt new <branch>          # ../pigeonhole-<branch>, a new branch off main

and, once the branch is in main, drop all of it (the directory, its
`target/`, the branch) with

    scripts/wt done <branch>

`scripts/wt list` shows every worktree (in main or not, uncommitted work,
touched in the last hour, `target/` size); `scripts/wt clean` lists the
merged, clean, idle ones and `scripts/wt clean --yes` removes them. Don't
remove another session's worktree: ask it first.

## Builds

Each worktree has its own `target/`. Don't set `CARGO_TARGET_DIR` to share
builds between them: cargo locks the directory, so sessions building at
once wait on each other (and a compiler cache such as sccache keys on it).

## Before merging into main

Run what the branch touches:

- Rust: `make lint` (rustfmt check, clippy with `-D warnings`), `cargo test`
  for the changed crates (`--workspace` if shared ones changed),
  `./scripts/check-deps.sh --strict`, and `make sim` for meta, blob, storage
  or sim changes.
- The site's JavaScript: `make lint-js`, `make test-js`.
- The web node (crates/web) or the demo: `make web-smoke`.

## Merging: one session at a time

1. Get the user's go to merge.
2. Tell the other sessions "merging <branch>" and wait if one of them is
   merging.
3. Rebase on the latest local main, run the checks above, `git merge --no-ff`.
4. Tell them "merged <branch> as <hash>", then `scripts/wt done <branch>`.

Nobody pushes without the user asking.
