# Local Fork Review Instructions

This repository is a fork of upstream DeepSeek Harness.

Follow the upstream AGENTS.md first. These rules additionally apply to our local fork changes.

## Local patch policy

Local behavioral patches must remain:
- small;
- isolated;
- removable;
- justified by a concrete upstream gap.

Review local changes for:
- unnecessary coupling between DSH subsystems;
- divergence from upstream architecture;
- lifecycle/event-ordering bugs;
- stale state after cancellation, failure, or disposal;
- duplicated behavior already available upstream;
- changes that make future upstream rebases harder.

Do not suggest unrelated refactors of upstream code.

## Background-activity scheduler patch

The local scheduler patch exists to suppress empty goal rounds while a parent owns unresolved background work.

The goal-round-driver must depend only on the narrow backgroundActivity service.

It must not directly depend on subagents, jobs, little-coder, specific models, or Worker Profile engines.

Failed/cancelled background work must not leave stale activity that permanently suppresses the goal.

Final settlement should cause one coalesced goal re-drive, not polling or repeated wakeups.
