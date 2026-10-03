/**
 * Parent-scoped tracker of unresolved background work: subagent runs and
 * jobs launched by or on behalf of an agent. Consumers query whether a parent
 * still owns running work and subscribe to the moment its last owned activity
 * settles, without depending on how any particular worker is implemented.
 *
 * Start and job-change events are one-shot, so loading a tracker over a
 * composition that already owns work adopts that work instead of reporting a
 * quiescent parent: subagent activity is recovered from the agents registry's
 * live ownership of a child that still owes its parent an answer, and job
 * activity from the bound registry's owner-scoped list. The optional jobs
 * binding follows the registry, so a registry that loads after this plugin, or
 * a reload that replaces it, is observed by the same tracker.
 *
 * @module @deepseek-ai/dsh-background-activity
 */

import { Service } from '@deepseek-ai/cordis'
import type { Context, Fiber } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import type { JobRegistry, JobsChangedListener } from '@deepseek-ai/dsh-jobs'
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

  private readonly active = new Map<SessionId, Set<string>>()
  private readonly settled = new Map<SessionId, Set<BackgroundWorkSettledListener>>()
  private readonly agentScopes = new Map<SessionId, Scope>()
  /** Fiber that binds whichever jobs registry is live, and swaps on reload. */
  private readonly jobsBinding: Fiber
  /** Registry the jobs binding fiber currently serves; absent until one loads. */
  private jobs: JobRegistry | undefined

  constructor(ctx: Context) {
    super(ctx, 'backgroundActivity')

    const reconcileJobs: JobsChangedListener = (owner: Agent | undefined): void => {
      const jobs = this.jobs
      if (owner === undefined || jobs === undefined) return
      const live = new Set<string>()
      for (const job of jobs.list(owner)) {
        if (isLive(job.status)) live.add(`${JOB_PREFIX}${job.id.toString()}`)
      }
      if (this.reconcileJobs(owner.id, live)) this.notifySettled(owner.id)
    }

    const seed = (agent: Agent): void => {
      // Both sources are seeded, because a start or job that predates this
      // tracker is one-shot: nothing replays it to the instance that loads
      // next. `onJobsChanged` and the subagent events cover every later change.
      this.reconcileSubagents(agent)
      reconcileJobs(agent)
    }

    const attach = (agent: Agent): void => {
      const scope = createScope(ctx, agent)
      this.agentScopes.set(agent.id, scope)
      scope.ctx.on('subagent/start', (info: SubagentRunInfo): void => {
        this.register(agent.id, `${SUBAGENT_PREFIX}${info.id}`)
      })
      scope.ctx.on('subagent/end', (info: SubagentRunEndInfo): void => {
        if (this.unregister(agent.id, `${SUBAGENT_PREFIX}${info.id}`)) {
          this.notifySettled(agent.id)
        }
      })
      seed(agent)
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

    // The jobs service is optional, so the tracker binds to whichever registry
    // is live now rather than to the one present at construction. Cordis
    // re-runs this callback for every registry a load or reload provides and
    // unwinds its effects before each swap, so the subscription and the
    // retained job slots always belong to the registry that is serving.
    this.jobsBinding = ctx.inject(['jobs'], (jobsCtx: Context) => {
      const registry = jobsCtx.jobs
      this.jobs = registry
      for (const agent of ctx.agents.list()) {
        /* v8 ignore else -- the constructor attaches every registered agent in
           the same tick, and every later entry arrives through `agent/created`,
           so a live agent is always attached before this binding runs. Seeding
           an unattached one would register slots no listener could remove. */
        if (this.agentScopes.has(agent.id)) seed(agent)
      }
      jobsCtx.effect(() => {
        // Registered from the injected host context, so it sees every owner.
        const unsubscribe = registry.onJobsChanged(reconcileJobs)
        return () => {
          unsubscribe()
          /* v8 ignore next 4 -- only this registry's own binding releases these
             slots; a replacement registry already claimed them. */
          if (this.jobs !== registry) return
          this.jobs = undefined
          for (const parentId of this.dropJobs()) this.notifySettled(parentId)
        }
      }, 'backgroundActivity.jobsBinding')
    })

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
   * Record every child the runtime still registers under `parent` as an
   * unresolved activity. Runtime ownership — not durable lineage — decides
   * membership, so a run admitted while no tracker was loaded suppresses the
   * parent exactly as an observed start does, and the child's own terminal
   * event removes the same slot.
   * @param parent - the owning parent whose live children are recorded.
   */
  reconcileSubagents(parent: Agent): void {
    for (const child of this.ctx.agents.list()) {
      if (!this.ctx.agents.isOwnedBy(child.id, parent)) continue
      if (!isUnresolved(child)) continue
      this.register(parent.id, `${SUBAGENT_PREFIX}${child.id}`)
    }
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

  /**
   * Drop every job-prefixed activity after its registry leaves the composition,
   * because no later change can report those jobs settling.
   * @returns the parents whose whole activity set emptied, so each last settled.
   */
  private dropJobs(): SessionId[] {
    const emptied: SessionId[] = []
    for (const [parentId, set] of this.active) {
      for (const activityId of set) {
        if (activityId.startsWith(JOB_PREFIX)) set.delete(activityId)
      }
      if (set.size === 0) {
        this.active.delete(parentId)
        emptied.push(parentId)
      }
    }
    return emptied
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
    // The binding fiber owns the registry subscription, so releasing it is
    // this service's teardown; a leaked listener would also retain this
    // service reference past the host fiber lifecycle.
    void this.jobsBinding.dispose()
    this.jobs = undefined
    for (const scope of this.agentScopes.values()) void scope.dispose()
    this.agentScopes.clear()
    this.close()
  }
}

/** Whether a stored job status still leaves the host waiting for the producer. */
function isLive(status: string): boolean {
  return status === 'running' || status === 'stopping'
}

/**
 * Whether one live child still owes its parent an answer: it is executing, or
 * its inbox holds work a later turn will run. A settled child still waiting for
 * its owner to dispose it has already answered, so recovery must not adopt it.
 */
function isUnresolved(child: Agent): boolean {
  return child.status !== 'idle' || child.inbox.nextTurn.length > 0 || child.inbox.nextStep.length > 0
}

export default BackgroundActivity
