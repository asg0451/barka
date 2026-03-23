#!/usr/bin/env bash
# Fetches inline PR review comments (REST: pulls/{pr}/comments).
# Resolves owner/repo from `gh repo view` and PR from arg or current branch.
# Usage: ./scripts/gh-pr-review-comments.sh [--fmt] [PR_NUMBER]
# Requires: gh auth (same as other gh commands in this repo).

set -euo pipefail

FMT=false
for arg in "$@"; do
  case "$arg" in
    --fmt) FMT=true ;;
    *)     PR="${arg}" ;;
  esac
done

if [[ -z "${PR:-}" ]]; then
  PR="$(gh pr view --json number -q .number)"
fi

name_with_owner="$(gh repo view --json nameWithOwner -q .nameWithOwner)"
owner="${name_with_owner%%/*}"
repo="${name_with_owner#*/}"

if [[ "$FMT" == true ]]; then
  gh api \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "/repos/${owner}/${repo}/pulls/${PR}/comments" \
  | jq -r '.[] | "---\nFile: \(.path):\(.line // "?")\nresolved=\(.is_resolved // "?")\n\(.body[:500])"'
else
  exec gh api \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "/repos/${owner}/${repo}/pulls/${PR}/comments"
fi
