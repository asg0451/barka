#!/usr/bin/env bash
# Lists PR review threads (resolved/unresolved) via GraphQL — same shape as
# `.github/workflows/claude.yml` (Step 1: Manage Existing Review Threads).
# Usage: ./scripts/gh-pr-review-threads.sh [PR_NUMBER]
# Requires: gh auth.

set -euo pipefail

PR="${1:-}"
if [[ -z "$PR" ]]; then
  PR="$(gh pr view --json number -q .number)"
fi

name_with_owner="$(gh repo view --json nameWithOwner -q .nameWithOwner)"
owner="${name_with_owner%%/*}"
repo="${name_with_owner#*/}"

# Compact single-line query for portability (e.g. macOS bash).
QUERY='query($owner: String!, $name: String!, $number: Int!) { repository(owner: $owner, name: $name) { pullRequest(number: $number) { reviewThreads(first: 100) { nodes { id isResolved path line comments(first: 10) { nodes { id databaseId author { login } body } } } } } } } }'

exec gh api graphql \
  -f query="$QUERY" \
  -f owner="$owner" \
  -f name="$repo" \
  -F number="$PR"
