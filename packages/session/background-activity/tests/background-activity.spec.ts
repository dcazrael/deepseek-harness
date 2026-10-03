import { afterEach, describe, expect, it, vi } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import type { Fiber } from '@deepseek-ai/cordis'
import AgentRegistry from '@deepseek-ai/dsh-agent'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { createInboxStub } from '@deepseek-ai/dsh-agent-loop-testkit'
import { BackgroundActivity } from '../src/index.ts'
import { Session, SessionId } from '@deepseek-ai/dsh-session'
import { scopeTarget } from '@deepseek-ai/dsh-scope'
import LocalJobRegistry from '@deepseek-ai/dsh-jobs-local'
import type { JobHooks, JobOutcome, JobStart } from '@deepseek-ai/dsh-jobs'
import type { SubagentRunEndInfo, SubagentRunInfo } from '@deepseek-ai/dsh-subagent'
import { SubagentRunId } from '@deepseek-ai/dsh-subagent'

const parent = SessionId('parent-a')
const other = SessionId('parent-b')

let ctx: Context | undefined
afterEach(async () => {
  const current = ctx
  ctx = undefined
  await current?.fiber.dispose()
})

/** A live context with the required agent registry mounted. */
async function makeActivity(): Promise<BackgroundActivity> {
  ctx = new Context()
  await ctx.plugin(AgentRegistry)
  return new BackgroundActivity(ctx)
}

/** One registry entry the agent runtime owns like a real delegated child. */
interface StubAgent {
  readonly agent: Agent
  /** Detach this exact entry, the way its owner's teardown releases it. */
  readonly detach: () => void
  /** Mutable status, so a test can observe the child answering. */
  readonly setRunning: (running: boolean) => void
  /** Leave the child settled and idle with nothing queued. */
  readonly answer: () => void
}

/** The context under construction; every stub agent is scoped to it. */
function liveContext(): Context {
  if (ctx === undefined) throw new Error('registerStubAgent: no context is mounted')
  return ctx
}

/** Register a real agents-registry entry, optionally owned by a parent agent. */
async function registerStubAgent(id: string, owner?: Agent): Promise<StubAgent> {
  const context = liveContext()
  const sessionId = SessionId(id)
  const mutable = { status: 'running' as 'idle' | 'running' }
  const agent = {
    id: sessionId,
    options: {},
    session: Session.create(sessionId),
    inbox: createInboxStub(),
    get status() { return mutable.status },
    ctx: context,
    send: () => {},
    followup: () => {},
    steer: () => ({ outcome: Promise.resolve({ status: 'rejected' as const }) }),
    inject: () => {},
    cancel: () => {},
    runMaintenance: <T>(task: (signal: AbortSignal) => Promise<T>) => task(new AbortController().signal),
    whenIdle: () => Promise.resolve(),
  } satisfies Agent
  const detach = context.agents.enter(agent, owner)
  await context.agents.announce(agent, 'startup')
  return {
    agent,
    detach,
    setRunning: (running) => { mutable.status = running ? 'running' : 'idle' },
    answer: () => { agent.inbox.clear(); mutable.status = 'idle' },
  }
}

/** Build a stub agent object without registering it, for creation-window cases. */
function stubSessionId(id: string): Agent {
  const sessionId = SessionId(id)
  return {
    id: sessionId,
    options: {},
    session: Session.create(sessionId),
    inbox: createInboxStub(),
    status: 'running',
    ctx: liveContext(),
    send: () => {},
    followup: () => {},
    steer: () => ({ outcome: Promise.resolve({ status: 'rejected' as const }) }),
    inject: () => {},
    cancel: () => {},
    runMaintenance: <T>(task: (signal: AbortSignal) => Promise<T>) => task(new AbortController().signal),
    whenIdle: () => Promise.resolve(),
  } satisfies Agent
}

/** Attach a job controller the way `tool-jobs` does, from an injected context. */
function attachControllerIn(context: Context): Fiber {
  return context.plugin({
    inject: ['jobs'],
    apply(pluginCtx: Context) { pluginCtx.jobs.attachController('background-activity-test') },
  })
}

/** One real job producer whose settlement the test releases on demand. */
function jobProducer(owner: Agent): { spec: JobStart; settle: (outcome: JobOutcome) => void } {
  let settle!: (outcome: JobOutcome) => void
  const done = new Promise<JobOutcome>((resolve) => { settle = resolve })
  // Registry disposal cancels live work and awaits compliant producers, so the
  // teardown path must settle `done` rather than only record the request.
  const hooks: JobHooks = {
    cancel(reason) { settle({ status: 'killed', ...reason === undefined ? {} : { detail: reason } }) },
    done,
  }
  return {
    spec: { kind: 'bash', label: 'sleep 60', owner, run: () => hooks },
    settle: (outcome) => { settle(outcome) },
  }
}

/** Dispatch one parent-scoped subagent lifecycle edge through the real channel. */
function emitSubagent(
  context: Context,
  agent: Agent,
  name: 'subagent/start' | 'subagent/end',
  info: SubagentRunInfo | SubagentRunEndInfo,
): void {
  const carrier = scopeTarget(agent, agent)
  for (const callback of context.events.dispatch('emit', [carrier, name, info])) {
    void callback(info)
  }
}

/** Let registry continuations settle before the next assertion. */
const tick = (): Promise<void> => new Promise((resolve) => { setTimeout(resolve, 0) })

/**
 * Construct the tracker over the current context and let its optional jobs
 * binding activate. Cordis resolves an injected dependency on a microtask, so a
 * composition that starts work in the same tick as the load would otherwise
 * race the binding.
 */
async function mountActivity(): Promise<BackgroundActivity> {
  const activity = new BackgroundActivity(ctx!)
  await tick()
  return activity
}

describe('BackgroundActivity', () => {
  it('reports no active work before any activity', async () => {
    const activity = await makeActivity()
    expect(activity.hasActive(parent)).toBe(false)
  })

  it('tracks several parallel activities per parent independently', async () => {
    const activity = await makeActivity()
    activity.register(parent, 'subagent:run-1')
    activity.register(parent, 'subagent:run-2')
    activity.register(other, 'subagent:run-3')

    expect(activity.hasActive(parent)).toBe(true)
    expect(activity.hasActive(other)).toBe(true)
    // Removing one keeps the parent active while a sibling remains.
    expect(activity.unregister(parent, 'subagent:run-1')).toBe(false)
    expect(activity.hasActive(parent)).toBe(true)
    expect(activity.unregister(parent, 'subagent:run-2')).toBe(true)
    expect(activity.hasActive(parent)).toBe(false)
    expect(activity.hasActive(other)).toBe(true)
  })

  it('fires settled once on the last-activity transition, not per settlement', async () => {
    const activity = await makeActivity()
    const settled = vi.fn()
    activity.register(parent, 'subagent:run-1')
    activity.register(parent, 'subagent:run-2')
    activity.onSettled(parent, settled)

    // Mirror the plugin wiring: notify only when unregister empties the parent.
    if (activity.unregister(parent, 'subagent:run-1')) activity.notifySettled(parent)
    expect(settled).not.toHaveBeenCalled()
    if (activity.unregister(parent, 'subagent:run-2')) activity.notifySettled(parent)
    expect(settled).toHaveBeenCalledTimes(1)
    // A later unregister on a now-empty parent does not refire.
    if (activity.unregister(parent, 'subagent:run-2')) activity.notifySettled(parent)
    expect(settled).toHaveBeenCalledTimes(1)
  })

  it('does not fire for a parent that is already quiescent when subscribed', async () => {
    const activity = await makeActivity()
    const settled = vi.fn()
    activity.onSettled(parent, settled)
    expect(activity.hasActive(parent)).toBe(false)
  })

  it('unsubscribes a settled listener with its disposer', async () => {
    const activity = await makeActivity()
    const settled = vi.fn()
    const dispose = activity.onSettled(parent, settled)
    dispose()
    // A repeated disposal is a no-op rather than a second removal pass.
    dispose()
    activity.register(parent, 'subagent:run-1')
    activity.unregister(parent, 'subagent:run-1')
    expect(settled).not.toHaveBeenCalled()
  })

  it('fires all settled listeners and contains one that throws', async () => {
    const activity = await makeActivity()
    const first = vi.fn(() => { throw new Error('listener exploded') })
    const second = vi.fn()
    activity.register(parent, 'subagent:run-1')
    activity.onSettled(parent, first)
    activity.onSettled(parent, second)

    expect(() => { activity.notifySettled(parent) }).not.toThrow()
    expect(first).toHaveBeenCalled()
    expect(second).toHaveBeenCalled()
  })

  it('contains an asynchronously rejecting settled listener', async () => {
    const activity = await makeActivity()
    const rejecting = vi.fn(async () => { throw new Error('listener rejected later') })
    const peer = vi.fn()
    activity.register(parent, 'subagent:run-1')
    activity.onSettled(parent, rejecting)
    activity.onSettled(parent, peer)

    expect(() => { activity.notifySettled(parent) }).not.toThrow()
    // Allow the rejection microtask to settle so its catch handler runs.
    await new Promise<void>((resolve) => { setImmediate(resolve) })

    expect(rejecting).toHaveBeenCalled()
    expect(peer).toHaveBeenCalled()
  })

  it('reconciles job activity against the current live set', async () => {
    const activity = await makeActivity()
    activity.register(parent, 'subagent:run-1')
    const live = new Set(['job:1', 'job:2'])

    // Introducing jobs while a subagent remains does not settle the parent.
    expect(activity.reconcileJobs(parent, live)).toBe(false)
    expect(activity.hasActive(parent)).toBe(true)

    // A job leaving while others and the subagent remain stays active.
    expect(activity.reconcileJobs(parent, new Set(['job:1']))).toBe(false)
    expect(activity.hasActive(parent)).toBe(true)

    // Removing the remaining job and the subagent empties the parent.
    expect(activity.reconcileJobs(parent, new Set())).toBe(false)
    expect(activity.unregister(parent, 'subagent:run-1')).toBe(true)
  })

  it('drops a disposed parent\'s activity and settled listeners', async () => {
    ctx = new Context()
    await ctx.plugin(AgentRegistry)
    const activity = await mountActivity()
    const owner = await registerStubAgent('parent-disposed-agent')
    const settled = vi.fn()
    activity.onSettled(owner.agent.id, settled)
    emitSubagent(ctx, owner.agent, 'subagent/start', {
      runId: SubagentRunId('run-disposed'),
      provider: 'spawn',
      id: SessionId('child-disposed'),
      local: true,
    })
    expect(activity.hasActive(owner.agent.id)).toBe(true)

    owner.detach()
    expect(activity.hasActive(owner.agent.id)).toBe(false)
    // A settlement for the purged parent finds no listener left to wake.
    activity.notifySettled(owner.agent.id)
    expect(settled).not.toHaveBeenCalled()
  })

  it('releases the jobs subscription on service disposal', async () => {
    ctx = new Context()
    await ctx.plugin(AgentRegistry)
    attachControllerIn(ctx)
    await ctx.plugin(LocalJobRegistry)
    const activity = await mountActivity()
    const owner = await registerStubAgent('parent-disposed')
    const { spec, settle } = jobProducer(owner.agent)
    const id = ctx.jobs.start(spec)
    expect(activity.hasActive(owner.agent.id)).toBe(true)

    activity.dispose()
    await tick()
    // The released registry no longer drives a disposed tracker, so its own
    // settlement cannot resurrect activity state.
    settle({ status: 'completed' })
    await tick()
    expect(activity.hasActive(owner.agent.id)).toBe(false)
    expect(ctx.jobs.get(id, owner.agent).status).toBe('completed')
  })

  it('ignores edges and unregisters for a parent it never tracked', async () => {
    ctx = new Context()
    await ctx.plugin(AgentRegistry)
    const activity = await mountActivity()
    const owner = await registerStubAgent('parent-untracked')
    const settled = vi.fn()
    activity.onSettled(owner.agent.id, settled)

    // A terminal edge for a run this tracker never recorded, and an unregister
    // for a parent with no recorded activity, both leave it quiescent.
    emitSubagent(ctx, owner.agent, 'subagent/end', {
      runId: SubagentRunId('run-unknown'),
      provider: 'spawn',
      id: SessionId('child-unknown'),
      local: true,
      stopReason: 'completed',
    })
    expect(activity.unregister(owner.agent.id, 'subagent:child-unknown')).toBe(false)
    expect(settled).not.toHaveBeenCalled()
    expect(activity.hasActive(owner.agent.id)).toBe(false)
  })

  it('skips a registry entry whose creation has not been announced', async () => {
    ctx = new Context()
    await ctx.plugin(AgentRegistry)
    attachControllerIn(ctx)
    const creating = stubSessionId('child-in-creation')
    ctx.agents.enter(creating, undefined)
    // Binding seeds every attached parent; this entry has no tracker scope
    // until its creation dispatch announces it.
    await ctx.plugin(LocalJobRegistry)
    const activity = await mountActivity()
    expect(activity.hasActive(creating.id)).toBe(false)
    expect(ctx.agents.get(creating.id)).toBe(creating)
  })

  it('releases a scope-free disposal after service teardown', async () => {
    ctx = new Context()
    await ctx.plugin(AgentRegistry)
    const fiber = await ctx.plugin(BackgroundActivity)
    await tick()
    const owner = await registerStubAgent('parent-teardown-order')
    ctx.backgroundActivity.dispose()
    // The agent leaves the registry after the tracker released its scopes, the
    // teardown order a fiber unload and a host-owned agent disposal can take.
    owner.detach()
    await tick()
    expect(ctx.agents.get(owner.agent.id)).toBeUndefined()
    await fiber.dispose()
  })

  it('purges a parent and closes all state', async () => {
    const activity = await makeActivity()
    activity.register(parent, 'subagent:run-1')
    activity.onSettled(parent, () => {})
    activity.purge(parent)
    expect(activity.hasActive(parent)).toBe(false)

    activity.register(parent, 'subagent:run-1')
    activity.close()
    expect(activity.hasActive(parent)).toBe(false)
  })
})

describe('BackgroundActivity jobs binding', () => {
  it('observes a registry that loads after the tracker', async () => {
    ctx = new Context()
    await ctx.plugin(AgentRegistry)
    const activity = await mountActivity()
    const owner = await registerStubAgent('parent-late-jobs')
    attachControllerIn(ctx)
    // Late availability: the tracker was constructed with no jobs service, so
    // only a lifecycle-bound subscription can observe this registry.
    await ctx.plugin(LocalJobRegistry)
    const { spec, settle } = jobProducer(owner.agent)
    const id = ctx.jobs.start(spec)

    expect(activity.hasActive(owner.agent.id)).toBe(true)
    settle({ status: 'completed' })
    await tick()
    expect(activity.hasActive(owner.agent.id)).toBe(false)
    expect(ctx.jobs.get(id, owner.agent).status).toBe('completed')
  })

  it('releases a replaced registry and adopts the registry that serves now', async () => {
    ctx = new Context()
    await ctx.plugin(AgentRegistry)
    attachControllerIn(ctx)
    const first = await ctx.plugin(LocalJobRegistry)
    const activity = await mountActivity()
    const owner = await registerStubAgent('parent-replaced-jobs')
    const before = jobProducer(owner.agent)
    ctx.jobs.start(before.spec)

    // Reload: the replacement registry cancels the first registry's live work,
    // and the tracker must forget it rather than wait for a settlement that
    // the unloaded registry can no longer report.
    const firstSettled = vi.fn()
    activity.onSettled(owner.agent.id, firstSettled)
    await first.dispose()
    await tick()
    expect(activity.hasActive(owner.agent.id)).toBe(false)
    expect(firstSettled).toHaveBeenCalledTimes(1)

    await ctx.plugin(LocalJobRegistry)
    const after = jobProducer(owner.agent)
    ctx.jobs.start(after.spec)
    expect(activity.hasActive(owner.agent.id)).toBe(true)
    after.settle({ status: 'completed' })
    await tick()
    expect(activity.hasActive(owner.agent.id)).toBe(false)
  })

  it('adopts a live owner job that predates the tracker that loads next', async () => {
    ctx = new Context()
    await ctx.plugin(AgentRegistry)
    attachControllerIn(ctx)
    await ctx.plugin(LocalJobRegistry)
    const owner = await registerStubAgent('parent-adopted-job')
    const first = await ctx.plugin(BackgroundActivity)
    await tick()
    const unloaded = ctx.backgroundActivity
    const { spec, settle } = jobProducer(owner.agent)
    ctx.jobs.start(spec)
    expect(unloaded.hasActive(owner.agent.id)).toBe(true)

    // Unload and reload the tracker over work that started before it existed:
    // the new instance must adopt the live job instead of reporting a
    // quiescent parent.
    await first.dispose()
    await ctx.plugin(BackgroundActivity)
    await tick()
    const second = ctx.backgroundActivity
    expect(second).not.toBe(unloaded)
    expect(second.hasActive(owner.agent.id)).toBe(true)
    settle({ status: 'completed' })
    await tick()
    expect(second.hasActive(owner.agent.id)).toBe(false)
  })

  it('keeps a sibling subagent active when a job registry leaves', async () => {
    ctx = new Context()
    await ctx.plugin(AgentRegistry)
    attachControllerIn(ctx)
    const jobs = await ctx.plugin(LocalJobRegistry)
    const activity = await mountActivity()
    const owner = await registerStubAgent('parent-job-and-child')
    const child = await registerStubAgent('child-job-and-child', owner.agent)
    const { spec, settle } = jobProducer(owner.agent)
    ctx.jobs.start(spec)
    emitSubagent(ctx, owner.agent, 'subagent/start', {
      runId: SubagentRunId('run-sibling'),
      provider: 'spawn',
      id: child.agent.id,
      local: true,
    })
    expect(activity.hasActive(owner.agent.id)).toBe(true)

    const settled = vi.fn()
    activity.onSettled(owner.agent.id, settled)
    settle({ status: 'completed' })
    await jobs.dispose()
    await tick()
    // The child is still unresolved, so the parent must not report settlement.
    expect(activity.hasActive(owner.agent.id)).toBe(true)
    expect(settled).not.toHaveBeenCalled()
  })
})

describe('BackgroundActivity subagent recovery', () => {
  it('adopts a child the runtime still owns when the tracker loads over it', async () => {
    ctx = new Context()
    await ctx.plugin(AgentRegistry)
    const owner = await registerStubAgent('parent-live-child')
    const child = await registerStubAgent('child-live', owner.agent)
    const settled = vi.fn()

    // The run was admitted before any tracker existed, so nothing replays its
    // start to the instance that loads now.
    const activity = await mountActivity()
    activity.onSettled(owner.agent.id, settled)
    expect(activity.hasActive(owner.agent.id)).toBe(true)

    // The child's own terminal edge clears the recovered slot exactly once.
    emitSubagent(ctx, owner.agent, 'subagent/end', {
      runId: SubagentRunId('run-live-child'),
      provider: 'spawn',
      id: child.agent.id,
      local: true,
      stopReason: 'completed',
    })
    expect(activity.hasActive(owner.agent.id)).toBe(false)
    expect(settled).toHaveBeenCalledTimes(1)
  })

  it('adopts a child that holds queued work while it is idle', async () => {
    ctx = new Context()
    await ctx.plugin(AgentRegistry)
    const owner = await registerStubAgent('parent-queued-child')
    const child = await registerStubAgent('child-queued', owner.agent)
    child.setRunning(false)
    child.agent.inbox.append('next-turn', {
      id: 'queued-prompt',
      role: 'user',
      content: [],
      source: { kind: 'user' },
    } as never)

    const activity = await mountActivity()
    expect(activity.hasActive(owner.agent.id)).toBe(true)
  })

  it('does not adopt a child that already answered', async () => {
    ctx = new Context()
    await ctx.plugin(AgentRegistry)
    const owner = await registerStubAgent('parent-answered-child')
    const child = await registerStubAgent('child-answered', owner.agent)
    child.answer()

    const activity = await mountActivity()
    expect(activity.hasActive(owner.agent.id)).toBe(false)
  })

  it('does not adopt another parent\'s child', async () => {
    ctx = new Context()
    await ctx.plugin(AgentRegistry)
    const unrelated = await registerStubAgent('unrelated-parent')
    await registerStubAgent('child-of-unrelated', unrelated.agent)
    await registerStubAgent('parent-not-mine')

    const activity = await mountActivity()
    expect(activity.hasActive(SessionId('parent-not-mine'))).toBe(false)
  })
})
