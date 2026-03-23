# Lessons

## Read AGENTS.md and CLAUDE.md before coding

`AGENTS.md` points at `CLAUDE.md`. Don't rely on memory or grep alone for architecture, test setup, or workflow—open them at the start of a session or task so global Cursor rules and repo docs stay aligned.

## Don't skip LocalStack tests

The LocalStack-dependent tests in this repo are the core test suite, not optional integration tests. Don't filter them out with `--skip`. If LocalStack is expected to be running (and it is for this project), run the full suite.

## Update lessons.md immediately on correction

When the user corrects something (rejects a tool call, says "don't do X"), update this file right then — not at the end, not when prompted. The self-improvement loop only works if it's reflexive.

## Branch before working

Always create a feature branch before making any code changes. Never work directly on master. This should happen before the first edit, not after all the work is done.

## Follow the full workflow, not just the code

The task isn't just "write the code." It includes: branch, implement, test, commit, PR. Don't wait for the user to remind you about the non-coding steps.

## Think through concurrency invariants before implementing

When adding epoch/leadership gating, consider what happens to in-flight work when state changes. Don't just gate the entry point — ensure the invariant holds through the entire operation (accept → batch → flush → S3 write).

## Run clippy before pushing

CI runs `cargo clippy -- -D warnings`. Run it locally before committing to avoid a round-trip failure.

## Keep inline comments brief

One sentence explaining *why* is enough. Don't write multi-line paragraphs about the protocol, alternatives, or future work in code comments. If it needs more than a line or two, it belongs in the PR description or docs, not inline.

## Test isolation: unique S3 prefixes

Jepsen (and integration tests in general) must use a unique S3 prefix per run. Without it, consumers read stale segments from previous runs, causing spurious failures (unexpected values, duplicates). Always check that test runs can't pollute each other via shared state.

## Don't reuse field names with different semantics

When adding a field to a response struct that means something different from an existing field (e.g., `offset` for produce base offset vs. consume cursor), give it a distinct name (`next_offset`). Don't reuse-and-comment — rename.
