---
description: "Parent-scoped tracker of unresolved subagent runs and jobs that lets the goal driver suppress its automatic round reservation while the parent still owns live work."
kind: "package-reference"
---

# @deepseek-ai/dsh-background-activity

English | [中文](README.zh.md)

## Summary

Use `dsh-background-activity` to answer a simple question: does a parent agent still own any subagent run or job that started under its session id? Consumers subscribe to the moment the parent's last owned activity settles, without depending on which worker implementation produced the work. The scheduler uses this signal to suppress its automatic goal-round reservation while a live child or job holds the parent, and to wake the driver once at the final settlement.

## Table of Contents

- [Use this package](#use-this-package)
- [Understand the implementation](#understand-the-implementation)
- [Further Exploration](#further-exploration)
- [Model Experience](#model-experience)
- [Known Limitations and Deferred Work](#known-limitations-and-deferred-work)
- [Dev Note](#dev-note)

-----

<a id="use-this-package"></a>
## Use this package

Mount the plugin in any composition that should let the scheduler hold its goal driver while a parent agent owns unresolved background work. The plugin registers the `backgroundActivity` service on the host context and binds subagent lifecycle events from each live agent's scope and jobs service changes from the unscoped host context.

### Composition

```yaml
- name: '@deepseek-ai/dsh-session'
- name: '@deepseek-ai/dsh-subagent'
- name: '@deepseek-ai/dsh-jobs-local'
- name: '@deepseek-ai/dsh-background-activity'
- name: '@deepseek-ai/dsh-goal'
- name: '@deepseek-ai/dsh-goal-round-driver'
```

### What an entry means

The plugin is a service: `static inject = ['agents']` declares its only required dependency; loading is independent of which worker provider or jobs implementation is composed. The plugin publishes `ctx.backgroundActivity`, which the goal-round-driver reads through `ctx.get(name)` exactly because the service is optional.

### Query and subscribe

Two methods compose the public surface (`BackgroundActivityView`):

| Method | Behavior |
|---|---|
| `hasActive(parentId)` | Returns whether the given parent agent id still owns any running or stopping subagent run or job. The query is read-only and resolves synchronously. |
| `onSettled(parentId, callback)` | Subscribes to the moment the parent's last owned activity empties. The callback fires once per non-empty-to-empty transition; a parent that is already quiescent when subscribed never fires. The returned function unsubscribes the callback. |

Failed or missing subagent or jobs integration is not catastrophic: the plugin tracks whichever sources it can observe and reports `hasActive` against the union.

### What an entry does NOT do

The plugin does not own execution resources — subagents run in the subagent runtime, jobs live in the jobs service. Its only state is the per-parent activity set and the per-parent settled-listener set, and it never sends anything to the model.

-----

<a id="understand-the-implementation"></a>
## Understand the implementation

<details>
<summary>Implementation internals — click to expand</summary>

This section explains the tracker's data shape and lifecycle seams; the observable behavior is fully covered in [Use this package](#use-this-package).

### Design concept

Each owned activity occupies one slot in a per-parent set, identified by `'subagent:<runId>'` or `'job:<jobId>'`. Parallel starts and settlements cannot desynchronize a counter, because there is no counter — every slot is independent. The set-keyed approach handles the corner cases a counter cannot: a run that completes before its peer arrives, a job removed while subagents remain, or a settlement notification for a key the tracker already dropped.

### Source map

| File | Role |
|---|---|
| [`src/index.ts`](src/index.ts) | Service declaration, per-agent scope subscriptions, jobs reconciliation, settled-call dispatch. |

### Lifecycle seams

- **Per-agent scopes** — `createScope(ctx, agent)` mints a scope per agent and `subagent/start` and `subagent/end` listeners record activity transitions; the scope is disposed on `agent/disposed` and its state is purged.
- **Optional Jobs integration** — when `ctx.get('jobs')` returns a registry, the plugin subscribes to `onJobsChanged`, diffs the live owner-scoped set against the parent's stored jobs, and fires settled only when the parent's set empties; the disposer is released on plugin disposal so a reloaded tracker does not retain a stale listener.
- **Mounted-over-existing agents** — the constructor iterates `ctx.agents.list()` and seeds live work so the tracker observes agents created before the plugin loaded.
- **Disposal** — the effects-attached cleanup disposes every agent scope, releases the jobs subscription, and clears retained activity and settled state; calling `dispose()` twice is a no-op.

### Settled dispatch containment

`notifySettled` invokes each callback inside a per-callback try/catch and an asynchronous catch for rejections, so a single throwing or rejecting listener cannot trigger an unhandled rejection at this seam; the synchronous try/catch still logs a warn.

</details>

-----

<a id="further-exploration"></a>
## Further Exploration

Read these pages when the tracker's contract is not enough. They move from this seam to adjacent session and scheduler packages.

- [Session package map](../README.md) — adjacent session, projection, and persistence packages.
- [Goal subsystem](../../../docs/subsystems/goal.md) — the scheduler that consumes this tracker to suppress automatic goal rounds.
- [Jobs package](../../jobs/jobs/README.md) — the parallel-bucket lifecycle that this tracker reconciles into the parent's set.

-----

<a id="model-experience"></a>
## Model Experience

### Goal-round suppression signal

#### What the model sees

The model sees nothing from this package. The tracker is a passive observer that exposes a service handle to its consumer (the goal-round-driver); it publishes no schema, prompt, result text, or other model input, and the goal driver uses it only to gate its automatic scheduling decisions. A parent goal that would otherwise reserve a round while owning live work is held by the goal-round-driver until the tracker fires `onSettled`, at which point the driver reserves exactly one coalesced round and the existing pause/resume flow resumes.

#### Token effect

None. The package adds zero tokens to any request — every provider round is owned by the goal driver or the model-facing tools, not by the tracker. The tracker's settled signal gates automatic goal-round reservations, but the package itself never assembles or sends provider requests.

#### KV Cache effect

None. The package never participates in a model request, so it never influences the reconstructed content or cache key of any provider call. The tracker's settled signal affects only the goal-round cadence, not the contents of the rounds that do run.

## Known Limitations and Deferred Work

<a id="known-limitations-and-deferred-work"></a>

These limits define what the tracker describes and when it is absent. They are current package constraints.

- **Optional services mean optional inputs** — a composition without `ctx.jobs` still tracks subagent work but never reconciles jobs, and the goal driver cannot suppress rounds for jobs that ran without joining this scope.
- **Whole-set queries only** — `hasActive` returns a boolean for a parent id; a consumer that needs to enumerate the live activity ids must record them itself or extend `BackgroundActivityView`.
- **Listeners fire per settle, not per start** — the public contract is the empty transition, so a consumer that wants start-notifications must register its own listeners on the relevant subagent and jobs events.

<a id="dev-note"></a>
### Dev Note

<details>
<summary>Working context for maintainers — click to expand</summary>

When the scheduler patch `worker-profiles/0-alpha2-compat` adds `ctx.backgroundActivity` to a host, the goal-round-driver subscribes via `ctx.on('internal/service', ...)` rather than `internal/plugin` so the rebind runs once the service is visible to other fibers, not while the new fiber is still PENDING. The tracker retains no state across reloads: a `dispose()` clears every per-parent set and releases the jobs subscription so the Jobs service does not retain a reference past teardown.

</details>
