---
name: commit-push-pr-ci-review-loop
description: Commits changes, pushes a branch, opens or updates a GitHub pull request, watches CI until green, requests a Claude code review, fixes findings, and repeats until both CI and the review are clean. Use when the user asks to ship work, open a PR, run CI to green, or follow commit → push → PR → CI.
---

# Commit, push, PR, watch CI, and Claude review

## Goal

Land changes on GitHub with a PR and **do not stop until CI succeeds and the Claude code review is clean** (or a hard blocker requires human input). Prefer the GitHub CLI (`gh`) when available.

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

### 7. Request Claude review

Once CI is green, trigger the Claude Code review action:

1. Get the PR number:
   `PR=$(gh pr view --json number -q .number)`
2. Post the trigger comment:
   `gh pr comment "$PR" --body "@claude pls review this pr"`

### 8. Watch the Claude review action

The comment triggers the **Claude Code** workflow (`.github/workflows/claude.yml`). Wait for it to finish:

1. Poll until the workflow run appears (it takes a few seconds after the comment):
   `gh run list --workflow=claude.yml --branch "$(git branch --show-current)" --limit 1 --json databaseId,status,conclusion`
2. Watch the run:
   `gh run watch <RUN_ID>`
   (or poll `gh run view <RUN_ID> --json status,conclusion` in a loop.)
3. If the run itself fails (infrastructure error, not a code review finding), check logs with `gh run view <RUN_ID> --log-failed` and decide whether to retry or escalate.

### 9. Read Claude's review findings

After the action completes, read what Claude posted:

1. **PR-level comments** (top-level review feedback):
   `gh pr view "$PR" --comments`
2. **Inline review comments** (code-specific findings):
   `gh api -H "Accept: application/vnd.github+json" repos/{owner}/{repo}/pulls/{PR}/comments`
3. **Review threads** (to see unresolved items):
   Use the GraphQL query from the workflow to list unresolved review threads, or simply read through the comments from steps 1-2.

Focus on **unresolved** findings from `github-actions[bot]` (that is the Claude action's identity).

### 10. Fix Claude's findings — iterate

For each actionable finding:

1. Understand the issue — read the referenced file and line.
2. Fix the root cause locally (same standards as CI fixes: no papering over).
3. `git add` → `git commit` → `git push`.
4. Return to **Watch CI** (step 5). Once CI is green again, return to **Request Claude review** (step 7) to get a fresh review that will also resolve previously-fixed threads.
5. Repeat until Claude's review produces **no new unresolved findings**.

If a finding is a false positive or a style disagreement you want to skip, note it for the user rather than silently ignoring it.

### 11. Done when

- `gh pr checks` shows success (all required CI checks are green).
- Claude's most recent review has **no unresolved findings** (or only items explicitly waved off by the user).
- Briefly summarize for the user: PR link, what CI and review issues were fixed in the loop.

## Edge cases

- **No `gh`**: use `git push` and open the compare URL Git prints; poll CI in the browser or install `gh`.
- **Draft PR**: `gh pr create --draft` if the user wants draft first; still run `--watch` when aiming for green.
- **Force-push**: only if the user explicitly wants history rewritten (`git push --force-with-lease` after rebase/amend).
- **Merge conflicts with base**: rebase or merge `main`/`master` as the project prefers, then push and re-watch CI.
- **Claude action not set up**: if `.github/workflows/claude.yml` does not exist or the comment does not trigger a run, skip the review steps and inform the user.
- **Claude review loops endlessly**: if the same finding keeps reappearing after a fix, stop and surface it to the user — don't loop more than 3 rounds without human input.

## Anti-patterns

- Declaring success after push without confirming checks.
- Declaring success after CI without waiting for the Claude review.
- Commits that bundle unrelated changes just to "fix CI" without separate commits when the user cares about history (ask if unsure).
- Skipping local preflight when CLAUDE/agents document mandatory commands before push.
- Silently ignoring Claude review findings without telling the user.
