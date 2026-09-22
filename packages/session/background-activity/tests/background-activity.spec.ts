import { afterEach, describe, expect, it, vi } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import AgentRegistry from '@deepseek-ai/dsh-agent'
import { BackgroundActivity } from '../src/index.ts'
import { SessionId } from '@deepseek-ai/dsh-session'

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

  it('releases the jobs subscription on service disposal', async () => {
    const unsubscribe = vi.fn()
    const onJobsChanged = vi.fn(() => unsubscribe)
    ctx = new Context()
    Object.defineProperty(ctx, 'get', {
      value: (name: string) => (name === 'jobs' ? { onJobsChanged, list: () => [] } : undefined),
    })
    // Required base service for the super() constructor; not exercised here.
    await ctx.plugin(AgentRegistry)
    const activity = new BackgroundActivity(ctx)
    expect(onJobsChanged).toHaveBeenCalledTimes(1)
    expect(unsubscribe).not.toHaveBeenCalled()

    activity.dispose()
    expect(unsubscribe).toHaveBeenCalledTimes(1)

    // A second disposal is a no-op rather than invoking the released disposer twice.
    activity.dispose()
    expect(unsubscribe).toHaveBeenCalledTimes(1)
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
