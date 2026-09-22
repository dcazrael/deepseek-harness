/**
 * Parent-scoped tracker of unresolved background work: subagent runs and
 * jobs launched by or on behalf of an agent. Consumers query whether a parent
 * still owns running work and subscribe to the moment its last owned activity
 * settles, without depending on how any particular worker is implemented.
 *
 * @module @deepseek-ai/dsh-background-activity
 */

import { Service } from '@deepseek-ai/cordis'
import type { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import type { JobsChangedListener } from '@deepseek-ai/dsh-jobs'
import type { Scope } from '@deepseek-ai/dsh-scope'
import { createScope } from '@deepseek-ai/dsh-scope'
import type { SubagentRunEndInfo, SubagentRunInfo } from '@deepseek-ai/dsh-subagent'
import type { SessionId } from '@deepseek-ai/dsh-session'

declare module '@deepseek-ai/cordis' {
  interface Context {
    /** Parent-scoped tracker of unresolved background work. */
    backgroundActivity: BackgroundActivity
  }
}

/** Called when a parent's last owned background activity settles. */
export interface BackgroundWorkSettledListener {
  (): void | PromiseLike<void>
}

/** Parent-scoped query/subscription surface for unresolved background work. */
export interface BackgroundActivityView {
  /**
   * Whether `parentId` still owns any running or stopping background work.
   * @param parentId - the owning parent agent's session id.
   * @returns whether at least one owned activity remains unresolved.
   */
  hasActive(parentId: SessionId): boolean
  /**
   * Subscribe to the moment `parentId`'s last owned background activity
   * settles. Fires once per non-empty-to-empty transition; a parent that is
   * already quiescent when subscribed never fires.
   * @param parentId - the owning parent agent's session id.
   * @param callback - invoked without arguments when the last activity settles.
   * @returns a disposer that unsubscribes `callback`.
   */
  onSettled(parentId: SessionId, callback: BackgroundWorkSettledListener): () => void
}

/** Distinguish subagent and job activity ids in one parent-scoped set. */
const SUBAGENT_PREFIX = 'subagent:'
const JOB_PREFIX = 'job:'

/**
 * Track each owned subagent run and job as a distinct slot in a per-parent set,
 * so parallel starts and settlements cannot desynchronize a counter and an
 * abnormal termination or removal still empties the set (see module docs).
 */
export class BackgroundActivity extends Service implements BackgroundActivityView {
  static inject = ['agents'] as const

  private readonly active = new Map<string, Set<string>>()
  private readonly settled = new Map<string, Set<BackgroundWorkSettledListener>>()
  private readonly agentScopes = new Map<string, Scope>()
  /** Disposer for the jobs subscription released on service disposal. */
  private unsubscribeJobs: (() => void) | undefined

  constructor(ctx: Context) {
    super(ctx, 'backgroundActivity')

    // The jobs service is optional; a composition without it still tracks
    // subagent work.
    const jobs = ctx.get('jobs')

    const reconcileJobs: JobsChangedListener = (owner: Agent | undefined): void => {
      if (owner === undefined || jobs === undefined) return
      const live = new Set<string>()
      for (const job of jobs.list(owner)) {
        if (isLive(job.status)) live.add(`${JOB_PREFIX}${job.id.toString()}`)
      }
      if (this.reconcileJobs(owner.id, live)) this.notifySettled(owner.id)
    }

    const attach = (agent: Agent): void => {
      const scope = createScope(ctx, agent)
      this.agentScopes.set(agent.id, scope)
      scope.ctx.on('subagent/start', (info: SubagentRunInfo): void => {
        this.register(agent.id, `${SUBAGENT_PREFIX}${info.runId}`)
      })
      scope.ctx.on('subagent/end', (info: SubagentRunEndInfo): void => {
        if (this.unregister(agent.id, `${SUBAGENT_PREFIX}${info.runId}`)) {
          this.notifySettled(agent.id)
        }
      })
      // Seed jobs that predate this plugin so existing live work suppresses the
      // parent; `onJobsChanged` covers every later change.
      reconcileJobs(agent)
    }

    const detach = (agent: Agent): void => {
      const scope = this.agentScopes.get(agent.id)
      this.agentScopes.delete(agent.id)
      if (scope !== undefined) void scope.dispose()
      this.purge(agent.id)
    }

    ctx.on('agent/created', ({ agent }: { agent: Agent }) => {
      attach(agent)
    })
    ctx.on('agent/disposed', ({ agent }: { agent: Agent }) => {
      detach(agent)
    })

    // Loading over existing agents must observe them without inheriting state
    // from an earlier plugin instance.
    for (const agent of ctx.agents.list()) attach(agent)

    if (jobs !== undefined) {
      // Registered from this unscoped host context, so it sees every owner.
      // The disposer is retained so plugin disposal/reload fully releases the
      // subscription; otherwise the Jobs service keeps a reference that
      // continues mutating this tracker after teardown.
      this.unsubscribeJobs = jobs.onJobsChanged(reconcileJobs)
    }

    const dispose = (): void => { this.dispose() }
    ctx.effect(function* () {
      yield dispose
    })
  }

  hasActive(parentId: SessionId): boolean {
    return (this.active.get(parentId)?.size ?? 0) > 0
  }

  onSettled(parentId: SessionId, callback: BackgroundWorkSettledListener): () => void {
    let callbacks = this.settled.get(parentId)
    if (callbacks === undefined) {
      callbacks = new Set()
      this.settled.set(parentId, callbacks)
    }
    callbacks.add(callback)
    const remove = (): void => {
      this.settled.get(parentId)?.delete(callback)
      if (this.settled.get(parentId)?.size === 0) this.settled.delete(parentId)
    }
    return remove
  }

  /** Record one resolved activity for `parentId`; a no-op when already present. */
  register(parentId: SessionId, activityId: string): void {
    let set = this.active.get(parentId)
    if (set === undefined) {
      set = new Set()
      this.active.set(parentId, set)
    }
    set.add(activityId)
  }

  /**
   * Remove one resolved activity for `parentId`.
   * @returns whether the parent's set emptied as a result (so it last settled).
   */
  unregister(parentId: SessionId, activityId: string): boolean {
    const set = this.active.get(parentId)
    if (set === undefined) return false
    set.delete(activityId)
    if (set.size === 0) {
      this.active.delete(parentId)
      return true
    }
    return false
  }

  /**
   * Diff a parent's job-prefixed activities against the current live set.
   * @returns whether the parent's set emptied as a result (so it last settled).
   */
  reconcileJobs(parentId: SessionId, live: ReadonlySet<string>): boolean {
    const set = this.active.get(parentId)
    if (set === undefined) {
      if (live.size === 0) return false
      this.active.set(parentId, new Set(live))
      return false
    }
    for (const activityId of set) {
      if (activityId.startsWith(JOB_PREFIX) && !live.has(activityId)) set.delete(activityId)
    }
    for (const activityId of live) set.add(activityId)
    if (set.size === 0) {
      this.active.delete(parentId)
      return true
    }
    return false
  }

  /** Invoke a parent's settled callbacks, containing their errors. */
  notifySettled(parentId: SessionId): void {
    const callbacks = this.settled.get(parentId)
    if (callbacks === undefined) return
    for (const callback of [...callbacks]) {
      try {
        // Catch both synchronous throws and asynchronous rejections so the
        // tracker never emits an unhandled rejection at this seam.
        void Promise.resolve(callback()).catch((error: unknown) => {
          this.ctx.logger.warn(`background-activity: settled listener rejected for parent "${parentId}": ${String(error)}`)
        })
      } catch (error: unknown) {
        this.ctx.logger.warn(`background-activity: settled listener threw for parent "${parentId}": ${String(error)}`)
      }
    }
  }

  /** Drop all state retained for a disposed parent without notifying it. */
  purge(parentId: SessionId): void {
    this.active.delete(parentId)
    this.settled.delete(parentId)
  }

  /** Drop all retained state (service teardown). */
  close(): void {
    this.active.clear()
    this.settled.clear()
  }

  /** Dispose per-agent scopes and all retained state (service teardown). */
  dispose(): void {
    // Release the jobs subscription first so the Jobs service does not keep
    // invoking a disposed tracker; a leaked listener would also retain this
    // service reference past the host fiber lifecycle.
    this.unsubscribeJobs?.()
    this.unsubscribeJobs = undefined
    for (const scope of this.agentScopes.values()) void scope.dispose()
    this.agentScopes.clear()
    this.close()
  }
}

/** Whether a stored job status still leaves the host waiting for the producer. */
function isLive(status: string): boolean {
  return status === 'running' || status === 'stopping'
}

export default BackgroundActivity
