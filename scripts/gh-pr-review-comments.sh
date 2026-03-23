#!/usr/bin/env bash
# Fetches inline PR review comments (REST: pulls/{pr}/comments).
# Resolves owner/repo from `gh repo view` and PR from arg or current branch.
# Usage: ./scripts/gh-pr-review-comments.sh [PR_NUMBER]
# Requires: gh auth (same as other gh commands in this repo).

set -euo pipefail

PR="${1:-}"
if [[ -z "$PR" ]]; then
  PR="$(gh pr view --json number -q .number)"
fi

name_with_owner="$(gh repo view --json nameWithOwner -q .nameWithOwner)"
owner="${name_with_owner%%/*}"
repo="${name_with_owner#*/}"

exec gh api \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  "/repos/${owner}/${repo}/pulls/${PR}/comments"
