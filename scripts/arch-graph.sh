#!/usr/bin/env bash
# The components' graph (easement graph) as a Mermaid block on stdout.
#
#   scripts/arch-graph.sh                 the core: without the HIDE components
#   scripts/arch-graph.sh <component>     one component and its neighbours
#
# HIDE (space-separated) defaults to the composition roots and the tests,
# which depend on nearly everything and hide the layers behind their arrows.
set -euo pipefail

HIDE=${HIDE-"bin web testkit fake-channels fakes tests"}

if [[ $# -gt 0 ]]; then
  graph=$(easement graph --focus "$1")
else
  # easement names nodes c0, c1, …: drop the hidden ones and every arrow
  # that starts or ends at one.
  graph=$(easement graph | awk -v hide="$HIDE" '
    BEGIN { n = split(hide, h, " "); for (i = 1; i <= n; i++) hidden[h[i]] = 1 }
    match($0, /^ *c[0-9]+\["[^"]*"\]$/) {
      id = $1; sub(/\[.*/, "", id)
      name = $0; sub(/^[^"]*"/, "", name); sub(/".*/, "", name)
      if (name in hidden) { dropped[id] = 1; next }
    }
    $2 ~ /^-->/ && ($1 in dropped || $NF in dropped) { next }
    { print }')
fi
fence='```'
printf '%smermaid\n%s\n%s\n' "$fence" "$graph" "$fence"
