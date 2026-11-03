#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 3 ]]; then
  printf 'usage: clone-current-snapshot.sh SOURCE DESTINATION BRANCH\n' >&2
  exit 2
fi
source_repo=$1
destination=$2
branch=$3

# This helper owns the selected repositories, even inside another Git command.
unset GIT_DIR GIT_COMMON_DIR GIT_WORK_TREE GIT_INDEX_FILE
unset GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES

if [[ -e "$destination" || -L "$destination" ]]; then
  printf 'snapshot destination must not exist\n' >&2
  exit 1
fi
source_repo="$(cd -- "$source_repo" && pwd -P)"
git check-ref-format --branch "$branch" >/dev/null
expected_head="$(git -C "$source_repo" rev-parse --verify "refs/heads/$branch^{commit}")"

# Local clones otherwise ignore depth and can copy or hardlink the full history.
git -c core.hooksPath=/dev/null clone --quiet --no-local --no-hardlinks \
  --depth=1 --single-branch --no-tags --branch "$branch" -- "$source_repo" "$destination"
[[ "$(git -C "$destination" rev-parse HEAD)" == "$expected_head" ]]
[[ "$(git -C "$destination" rev-list --count HEAD)" == 1 ]]
printf '%s\n' "$expected_head"
