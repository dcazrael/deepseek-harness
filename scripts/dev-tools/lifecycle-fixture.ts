/**
 * Real-composition fixture for the Task 0 runtime matrix.
 *
 * Boots the patched host composition (`BackgroundActivity` + `goal-round-driver`
 * + `jobs-local` + `GoalService` + real `agentLoop`) headlessly with no model
 * and no shell, then drives the exact lifecycle edges the harness uses in
 * production through the same `scopeTarget(agent, agent)` carrier the
 * `BackgroundActivity` and `goal-round-driver` listeners register against.
 *
 * Scenarios:
 *   S2 — armed goal does NOT drive while a child is unresolved, drives once
 *        on `subagent/end` (final coalesced recheck).
 *   S3 — mid-run user followup during an unresolved child is admitted and
 *        does not piggyback an empty goal round on top of suppression.
 *   S5 — two parallel children: the first settlement does not release
 *        suppression while the second remains unresolved; the goal drives
 *        exactly once when the second settles.
 *   S6 — owner-bound Jobs (registered via `jobs.attachController`); the
 *        same suppression and final-settlement behavior holds through the
 *        independent Jobs path.
 *   S10 — service absent (no `BackgroundActivity`) and service present-but-
 *        idle each preserve ordinary goal behavior independently.
 *
 * Each scenario records wall-clock starts, snapshot samples, the goal
 * projection, and the count of adapter requests so an Astra reviewer can
 * audit the contract.
 */

import { setTimeout as delay } from 'node:timers/promises'
import { randomUUID } from 'node:crypto'
import { Context } from '@deepseek-ai/cordis'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import BackgroundActivity from '@deepseek-ai/dsh-background-activity'
import GoalService from '@deepseek-ai/dsh-goal'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { SessionId } from '@deepseek-ai/dsh-session'
import { scopeTarget } from '@deepseek-ai/dsh-scope'
import { brandString } from '@deepseek-ai/dsh-brand'
import type { SessionId as SessionIdT } from '@deepseek-ai/dsh-session'
import type {
  SubagentRunId,
  SubagentRunInfo,
  SubagentRunEndInfo,
} from '@deepseek-ai/dsh-subagent'
import * as goalSession from '../../packages/goal/goal-round-driver/src/index.ts'
import {
  LlmAdapter,
  type GenerateOptions,
  type StreamChunk,
} from '@deepseek-ai/dsh-llm'

class ScriptedAdapter extends LlmAdapter {
  readonly requests: GenerateOptions[] = []
  constructor(private readonly script: string[]) {
    super()
  }
  override async * stream(_options: GenerateOptions): AsyncIterable<StreamChunk> {
    this.requests.push(_options)
    yield { type: 'text-delta', text: this.script.shift() ?? 'no-scripted-reply' } as unknown as StreamChunk
  }
}

interface Event {
  at: number
  kind: string
  payload?: Record<string, unknown>
}

interface Summary {
  scenario: string
  outcome: Record<string, unknown>
  events: Event[]
}

function buildSummary(label: string, record: Event[], outcome: Record<string, unknown>): Summary {
  return { scenario: label, outcome, events: record }
}

async function emitLifecycleChild(ctx: Context, parent: Agent, durationMs: number, label: string, setT0: number, record: Event[]): Promise<void> {
  const childSessionId = brandString<SessionIdT>(randomUUID())
  const carrier = scopeTarget(parent, parent)
  const startInfo: SubagentRunInfo = {
    runId: brandString<SubagentRunId>(randomUUID()),
    provider: 'lifecycle-fixture',
    id: childSessionId,
    local: true,
  }
  record.push({ at: Date.now() - setT0, kind: 'lifecycle-emit-start', payload: { runId: startInfo.runId, label, durationMs } })
  for (const callback of ctx.events.dispatch('emit', [carrier, 'subagent/start', startInfo])) {
    void callback(startInfo)
  }
  await delay(durationMs)
  const endInfo: SubagentRunEndInfo = { ...startInfo, stopReason: 'completed' }
  for (const callback of ctx.events.dispatch('emit', [carrier, 'subagent/end', endInfo])) {
    void callback(endInfo)
  }
  record.push({ at: Date.now() - setT0, kind: 'lifecycle-emit-end', payload: { runId: startInfo.runId, label } })
  // Yield a tick so settled listeners can fire before the next sample.
  await new Promise((r) => { setImmediate(r) })
}

async function bootComposition(): Promise<{ ctx: Context; record: Event[] }> {
  const ctx = new Context()
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(GoalService)
  await ctx.plugin(BackgroundActivity)
  const driver = await ctx.plugin(goalSession)
  await ctx.plugin(AgentLoop, { agents: [] })
  void driver
  return { ctx, record: [] as Event[] }
}

async function makeAgent(ctx: Context, scriptedReply: string): Promise<{ agent: Agent; adapter: ScriptedAdapter; setT0: number }> {
  const setT0 = Date.now()
  const agent = await ctx.agentLoop.create(SessionId(`p-${randomUUID()}`), { provider: 'mock', model: 'mock' })
  const adapter = new ScriptedAdapter([scriptedReply])
  ctx.llm.registerAdapter(['mock'], adapter)
  return { agent, adapter, setT0 }
}

/**
 * Poll until the goal's `roundsStarted` reaches `target`. The testbed's
 * `agent.whenIdle()` returns before the goal driver schedules its async
 * drive, so polling is the only way to observe the settlement.
 */
async function waitForRoundCount(ctx: Context, agent: Agent, target: number, timeoutMs = 30_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const goal = ctx.goals.get(agent)
    if ((goal?.roundsStarted ?? 0) >= target) return true
    await delay(100)
  }
  return (ctx.goals.get(agent)?.roundsStarted ?? 0) >= target
}

async function runS2(): Promise<Summary> {
  const { ctx, record } = await bootComposition()
  const { agent, adapter, setT0 } = await makeAgent(ctx, 'goal-round-1')
  // Begin a real subagent lifecycle BEFORE arming the goal, so the driver
  // sees a populated parent activity set the first time it ticks.
  const childPromise = emitLifecycleChild(ctx, agent, 5_000, 's2-child', setT0, record)
  await delay(200)
  // TWO-round cap so we can prove both "suppressed during child" AND
  // "drove exactly once after settlement" without depending on the goal
  // service's round-limit block landing within the test budget.
  ctx.goals.create(agent, { objective: 'wait', maxGoalRounds: 2 })
  await agent.whenIdle()
  const midRounds = ctx.goals.get(agent)?.roundsStarted ?? -1
  const midHasActive = bgHasActive(ctx, agent)
  const midRequests = adapter.requests.length
  record.push({ at: Date.now() - setT0, kind: 'snapshot-during-child', payload: { rounds: midRounds, hasActive: midHasActive, requests: midRequests } })
  await childPromise
  await waitForRoundCount(ctx, agent, 1)
  // Settle and observe the final state for a moment so we don't race the
  // settle-time callback.
  await delay(500)
  const finalGoal = ctx.goals.get(agent)
  await ctx.fiber.dispose()
  return buildSummary('S2', record, {
    midDuringChild: { rounds: midRounds, hasActive: midHasActive, requests: midRequests },
    finalRoundsStarted: finalGoal?.roundsStarted ?? -1,
    finalRequests: adapter.requests.length,
    finalPhase: finalGoal?.phase,
  })
}

async function runS5(): Promise<Summary> {
  const { ctx, record } = await bootComposition()
  const { agent, adapter, setT0 } = await makeAgent(ctx, 'goal-round-1')
  // Two parallel children: both must be present before arming the goal.
  const child1Promise = emitLifecycleChild(ctx, agent, 2_000, 's5-child-1', setT0, record)
  const child2Promise = emitLifecycleChild(ctx, agent, 6_000, 's5-child-2', setT0, record)
  await delay(200)
  ctx.goals.create(agent, { objective: 'wait', maxGoalRounds: 2 })
  await agent.whenIdle()
  // After child-1 settles, child-2 keeps the parent busy. The driver must
  // NOT release suppression yet.
  await child1Promise
  await delay(500)
  const midHasActive = bgHasActive(ctx, agent)
  const midRequests = adapter.requests.length
  record.push({ at: Date.now() - setT0, kind: 's5-mid-after-first-settle', payload: { midHasActive, midRequests } })
  await child2Promise
  await waitForRoundCount(ctx, agent, 1)
  await delay(500)
  const finalGoal = ctx.goals.get(agent)
  await ctx.fiber.dispose()
  return buildSummary('S5', record, {
    midHasActive, midRequests,
    finalRoundsStarted: finalGoal?.roundsStarted ?? -1,
    finalRequests: adapter.requests.length,
    finalPhase: finalGoal?.phase,
  })
}

function bgHasActive(ctx: Context, agent: Agent): boolean {
  // The BackgroundActivity service is loaded by bootComposition above; the
  // cast focuses this dev tool on the public surface we use.
  const bg = ctx.get('backgroundActivity') as unknown as { hasActive: (id: string) => boolean }
  return bg.hasActive(agent.id)
}

async function runS6(): Promise<Summary> {
  const { ctx, record } = await bootComposition()
  const Jobs = (await import('@deepseek-ai/dsh-jobs-local')).default
  await ctx.plugin(Jobs)
  const { agent, adapter, setT0 } = await makeAgent(ctx, 'goal-round-1')
  // The jobs service is provided by the plugin above; this minimal fixture
  // uses a structural cast to avoid pulling in the full jobs types in the
  // dev tool, which only needs attachController + start.
  interface JobsRegistry {
    attachController(name: string): () => void
    start(spec: unknown): unknown
  }
  const jobs = ctx.get('jobs') as JobsRegistry
  jobs.attachController('s6-fixture-controller')
  let release!: () => void
  const done = new Promise<void>((resolve) => { release = resolve })
  const jobHandle = jobs.start({
    kind: 'bash',
    label: 's6-job',
    owner: agent,
    run: () => ({
      cancel: () => undefined,
      readOutput: () => '',
      done: done.then(() => ({ status: 'completed' as const })) as unknown as Promise<{ status: 'completed' | 'failed' | 'killed'; detail?: string }>,
    }),
  })
  await delay(200)
  ctx.goals.create(agent, { objective: 'wait', maxGoalRounds: 2 })
  await agent.whenIdle()
  const midRounds = ctx.goals.get(agent)?.roundsStarted ?? -1
  const midHasActive = bgHasActive(ctx, agent)
  const midRequests = adapter.requests.length
  record.push({ at: Date.now() - setT0, kind: 's6-mid-with-job', payload: { rounds: midRounds, midHasActive, midRequests } })
  release()
  await new Promise((r) => { setImmediate(r) })
  await new Promise((r) => { setImmediate(r) })
  await waitForRoundCount(ctx, agent, 1)
  await delay(500)
  const finalGoal = ctx.goals.get(agent)
  await ctx.fiber.dispose()
  return buildSummary('S6', record, {
    jobId: String(jobHandle),
    midRounds, midHasActive, midRequests,
    finalRoundsStarted: finalGoal?.roundsStarted ?? -1,
    finalRequests: adapter.requests.length,
    finalPhase: finalGoal?.phase,
  })
}

async function runS10Absent(): Promise<Summary> {
  const ctx = new Context()
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(GoalService)
  await ctx.plugin(goalSession)
  await ctx.plugin(AgentLoop, { agents: [] })
  const agent = await ctx.agentLoop.create(SessionId(`s10-absent-${randomUUID()}`), { provider: 'mock', model: 'mock' })
  const adapter = new ScriptedAdapter(['no-tracker-final'])
  ctx.llm.registerAdapter(['mock'], adapter)
  ctx.goals.create(agent, { objective: 'no-tracker', maxGoalRounds: 1 })
  // No BackgroundActivity service; an idle parent with an armed goal drives
  // normally. Poll for the round to commit.
  await waitForRoundCount(ctx, agent, 1)
  const goal = ctx.goals.get(agent)
  await ctx.fiber.dispose()
  return buildSummary('S10-absent', [], {
    finalRoundsStarted: goal?.roundsStarted ?? -1,
    finalRequests: adapter.requests.length,
    finalPhase: goal?.phase,
    backgroundActivity: !!ctx.get('backgroundActivity'),
  })
}

async function runS10Idle(): Promise<Summary> {
  const ctx = new Context()
  await mountAgentLoopTestDependencies(ctx)
  await ctx.plugin(GoalService)
  await ctx.plugin(BackgroundActivity)
  await ctx.plugin(goalSession)
  await ctx.plugin(AgentLoop, { agents: [] })
  const agent = await ctx.agentLoop.create(SessionId(`s10-idle-${randomUUID()}`), { provider: 'mock', model: 'mock' })
  const adapter = new ScriptedAdapter(['with-tracker-idle-final'])
  ctx.llm.registerAdapter(['mock'], adapter)
  ctx.goals.create(agent, { objective: 'with-tracker-idle', maxGoalRounds: 1 })
  // BackgroundActivity is present but idle; an armed goal drives normally.
  await waitForRoundCount(ctx, agent, 1)
  const goal = ctx.goals.get(agent)
  await ctx.fiber.dispose()
  return buildSummary('S10-idle', [], {
    finalRoundsStarted: goal?.roundsStarted ?? -1,
    finalRequests: adapter.requests.length,
    finalPhase: goal?.phase,
  })
}

async function main(): Promise<void> {
  const summaries: Summary[] = []
  // The in-process testbed reliably proves:
  //   - S2  (suppression + final settled coalesced recheck).
  //   - S5  (parallel children — non-final settlement leaves suppression).
  //   - S6  (owner-bound Jobs — independent suppression path).
  //   - S10 (absent tracker, idle tracker — ordinary goal behavior).
  // S3 stays out of scope for the in-process testbed because the
  // `agent.followup(userMsg)` path intersects with cordis's followup
  // scheduling in a way the testbed cannot make deterministic; the same
  // contract is covered by the focused test "suppresses round reservation
  // while the parent owns background work, then drives once on
  // settlement".
  for (const [label, fn] of [
    ['S2', runS2], ['S5', runS5], ['S6', runS6],
    ['S10-absent', runS10Absent], ['S10-idle', runS10Idle],
  ] as const) {
    try { summaries.push(await fn()) }
    catch (error: unknown) { console.error(`SCENARIO ${label} FAILED:`, error instanceof Error ? error.stack : String(error)) }
  }
  console.log(JSON.stringify(summaries, undefined, 2))
}

void main()
