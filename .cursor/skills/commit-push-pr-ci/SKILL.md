---
name: commit-push-pr-ci
description: Commits changes, pushes a branch, opens or updates a GitHub pull request, and watches CI until it passes—fixing failures and re-pushing in a loop. Use when the user asks to ship work, open a PR, run CI to green, or follow commit → push → PR → CI.
---

# Commit, push, PR, and watch CI

## Goal

Land changes on GitHub with a PR and **do not stop until CI succeeds** (or a hard blocker requires human input). Prefer the GitHub CLI (`gh`) when available.

## Preconditions

- Clean intent: user wants this branch merged via PR with passing checks.
- `gh auth status` succeeds (run `gh auth login` if not).
- Remote is GitHub (`origin`).

## Workflow (execute in order)

### 1. Inspect and preflight

- `git status` and `git diff` (staged and unstaged). Understand what is being shipped.
- **Project checks before commit** (adapt to repo; do not skip if the project documents them):
  - This repo: read [`CLAUDE.md`](../../../CLAUDE.md) — e.g. `cargo clippy -- -D warnings`, `cargo test` where applicable; some tests need LocalStack per CLAUDE.
- Fix obvious issues locally before the first push when practical.

### 2. Commit

- Stage only what belongs in this PR (`git add` deliberately; avoid unrelated files).
- Message: clear, imperative subject; body if needed. Conventional style is fine if the repo uses it (`feat:`, `fix:`, etc.).
- `git commit` (or amend if the user wants a single commit: `git commit --amend`).

### 3. Push

- New branch: `git push -u origin HEAD`.
- Existing upstream: `git push`.

### 4. Open or reuse a PR

- If no PR for this branch:  
  `gh pr create --title "..." --body "..."`  
  (fill title/body from the actual change; link issues with `Fixes #n` if relevant.)
- If a PR already exists: note its number (`gh pr view` or `gh pr status`).

### 5. Watch CI

- From the repo root, on the PR branch:  
  `gh pr checks --watch`  
  (Waits for checks on the PR associated with the current branch.)
- Alternatives if needed:
  - Latest workflow run: `gh run list --branch "$(git branch --show-current)" --limit 1` then `gh run watch <RUN_ID>`.
  - Logs on failure: `gh run view <RUN_ID> --log-failed`.

### 6. If CI fails — iterate

1. Read failed step logs (`gh run view`, job URL from `gh pr checks`, or Actions web UI).
2. Reproduce locally when possible (same command as CI: see `.github/workflows/` if present).
3. Fix the root cause; avoid papering over with `#[allow]` or disabled tests unless the project already does that.
4. `git add` → `git commit` → `git push`.
5. Return to **Watch CI** until all required checks pass.

### 7. Done when

- `gh pr checks` shows success (or the repo’s required checks are green).
- Briefly summarize for the user: PR link, what was fixed in the loop if anything.

## Edge cases

- **No `gh`**: use `git push` and open the compare URL Git prints; poll CI in the browser or install `gh`.
- **Draft PR**: `gh pr create --draft` if the user wants draft first; still run `--watch` when aiming for green.
- **Force-push**: only if the user explicitly wants history rewritten (`git push --force-with-lease` after rebase/amend).
- **Merge conflicts with base**: rebase or merge `main`/`master` as the project prefers, then push and re-watch CI.

## Anti-patterns

- Declaring success after push without confirming checks.
- Commits that bundle unrelated changes just to “fix CI” without separate commits when the user cares about history (ask if unsure).
- Skipping local preflight when CLAUDE/agents document mandatory commands before push.
