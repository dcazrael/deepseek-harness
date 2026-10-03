/**
 * Real-composition runtime matrix for the Task 0 scheduler patch.
 *
 * Every scenario boots the shipped host composition — `SubagentRuntime` with the
 * in-process `spawn` provider, the model-facing `subagent` and `job_*` tools, the
 * local job registry, `GoalService`, `BackgroundActivity`, the goal-round driver
 * and the real `AgentLoop` — and drives it with a routed scripted adapter. Only
 * the model is scripted: every delegation, job, subagent admission, completion
 * notice, goal reservation and settlement below is production code running in
 * this process.
 *
 * Request accounting is structural, not textual. `GenerateOptions.sessionId`
 * separates the parent's calls from the child's, and the last non-tool
 * user message's production `MessageSource` says which prompt caused a parent
 * call, so parent, user, goal, completion-inbox and child requests are counted
 * independently of arrival order.
 *
 * Scenarios, matching the plan's Task 1 runtime matrix. M1, M2 and M3 share one
 * composition because they are one lifecycle observed at three moments; the
 * other rows are separate compositions:
 *   M1 — a real background child stays unresolved for at least 60 seconds under
 *        an idle armed parent: no automatic reservation, no parent model call.
 *   M2 — a user message reaches that live parent without an empty goal round.
 *   M3 — final settlement fires one coalesced recheck and one goal reservation.
 *   M4 — two parallel children: the first settlement keeps suppression.
 *   M5 — a real owner-bound job with no subagent exercises the jobs path alone.
 *   M6 — a failed child, a cancelled child and a rejected admission are
 *        distinguished; none leaves stale activity or an orphan wakeup.
 *   M7 — unloading and reloading the tracker over live work keeps suppression and
 *        settles the parent exactly once.
 *   M8 — an absent tracker and a present-but-idle tracker are measured apart.
 *   M9 — the ordinary goal baseline: drive, pause, resume.
 *
 * Run with `pnpm exec tsx scripts/dev-tools/lifecycle-fixture.ts`; `--debug` traces
 * every model request and scenario boundary. `DSH_FIXTURE_ONLY=<substring>` runs
 * a subset, and `DSH_FIXTURE_WINDOW_MS` shortens the live-child window while
 * iterating; the recorded run uses the 60 000 ms default. Every failed assertion
 * and every bounded-wait timeout is reported and exits nonzero; each scenario
 * releases its composition in a `finally`.
 *
 * @module scripts/dev-tools/lifecycle-fixture
 */

import { setTimeout as delay } from 'node:timers/promises'
import { randomUUID } from 'node:crypto'
import { Context } from '@deepseek-ai/cordis'
import type { Fiber } from '@deepseek-ai/cordis'
import { mountAgentLoopTestDependencies } from '@deepseek-ai/dsh-agent-loop-testkit'
import AgentLoop from '@deepseek-ai/dsh-agent-loop'
import LocalJobRegistry from '@deepseek-ai/dsh-jobs-local'
import type { JobHooks, JobOutcome, JobStart } from '@deepseek-ai/dsh-jobs'
import SubagentRuntime from '@deepseek-ai/dsh-subagent'
import * as SpawnInProcess from '@deepseek-ai/dsh-subagent-spawn-in-process'
import * as ToolSubagent from '@deepseek-ai/dsh-tool-subagent'
import * as ToolJobs from '@deepseek-ai/dsh-tool-jobs'
import GoalService from '@deepseek-ai/dsh-goal'
import BackgroundActivity from '@deepseek-ai/dsh-background-activity'
import * as GoalRoundDriver from '../../packages/goal/goal-round-driver/src/index.ts'
import { createScope } from '@deepseek-ai/dsh-scope'
import { createUserMessage, LlmAdapter } from '@deepseek-ai/dsh-llm'
import type { GenerateOptions, Message, StreamChunk } from '@deepseek-ai/dsh-llm'
import { SessionId } from '@deepseek-ai/dsh-session'
import type { Session, SessionEvent, SessionId as SessionIdT, UserMessage } from '@deepseek-ai/dsh-session'
import { textResponse, toolCallResponse } from '../../packages/core/agent-loop/tests/mock-adapter.ts'
import type { Agent } from '@deepseek-ai/dsh-agent'
import type { GoalView } from '@deepseek-ai/dsh-goal'

/** Prompt text that identifies the delegation turn in the parent's history. */
const DELEGATE_PROMPT = 'start the background child'
/** Prompt marker that identifies the steered parent turn. */
const STEER_MARKER = 'USER-STEER'
/** Prompt marker the fixture gives every real child. */
const CHILD_MARKER = 'CHILD-TASK'

/** Minimum time a real child stays unresolved under an armed goal, in ms. */
const LONG_CHILD_WINDOW_MS = 60_000
/** Shorten the live window while iterating; the recorded run uses the default. */
const WINDOW_OVERRIDE = Number(process.env.DSH_FIXTURE_WINDOW_MS ?? LONG_CHILD_WINDOW_MS)
const CHILD_WINDOW_MS = Number.isFinite(WINDOW_OVERRIDE) && WINDOW_OVERRIDE > 0
  ? WINDOW_OVERRIDE
  : LONG_CHILD_WINDOW_MS
/** Log every model request and the scenarios that start. */
const DEBUG = process.argv.includes('--debug')
/** Run only the scenarios whose name contains this substring. */
const ONLY = process.env.DSH_FIXTURE_ONLY
/** How long the live window runs untouched before the steering message. */
const QUIET_HALF_MS = 20_000
/** Bounded-wait budget for one observable predicate, in ms. */
const SETTLE_TIMEOUT_MS = 60_000

/** One scripted answer for the requests an entry selects. */
interface ScriptEntry {
  /** `parent` entries answer the parent's calls, `child` the child's. */
  readonly session: 'parent' | 'child'
  /** Parent entries select on the causing prompt's production source kind. */
  readonly promptKind?: 'goal' | 'user' | 'completion'
  /**
   * Parent entries select on whether the newest message is a tool result, which
   * separates a delegation turn's first request from its post-tool request.
   * The default matches both.
   */
  readonly afterTool?: boolean
  /** Matched against the causing prompt (parent) or the whole request (child). */
  readonly match?: RegExp | ((text: string) => boolean)
  /** Chunks streamed for a matched request. */
  readonly chunks?: StreamChunk[]
  /** Awaited before streaming, so the scenario releases the call itself. */
  readonly gate?: PromiseWithResolvers<void>
  /** Stream `chunks`, then hold the call open until it is cancelled. */
  readonly hang?: boolean
  /** Fail the call instead of answering. */
  readonly fail?: boolean
}

/** Model requests, split by the production source that caused each one. */
interface RequestTally {
  /** Calls made by a session other than the parent — the real child. */
  child: number
  /** Every call the parent made. */
  parent: number
  /** Parent calls whose last prompt was an automatic goal round. */
  goal: number
  /** Parent calls whose last prompt was human input. */
  user: number
  /** Parent calls whose last prompt was a delivered completion notice. */
  completion: number
  /** Parent calls no scenario prompt explains. */
  unclassified: number
}

/** A request-counted adapter that answers the parent and child separately. */
class RoutedAdapter extends LlmAdapter {
  /** Every request the composition made, in arrival order. */
  readonly requests: GenerateOptions[] = []
  /** Independent counts per request source. */
  readonly tally: RequestTally = {
    child: 0, parent: 0, goal: 0, user: 0, completion: 0, unclassified: 0,
  }
  /** Set by the harness once the parent agent exists. */
  parentSession: SessionIdT | undefined

  constructor(private readonly script: ScriptEntry[]) {
    super()
  }

  override async * stream(options: GenerateOptions): AsyncIterable<StreamChunk> {
    this.requests.push(options)
    const isChild = this.parentSession !== undefined && options.sessionId !== this.parentSession
    const prompt = isChild ? undefined : causingPrompt(options.messages)
    if (DEBUG) {
      const first = (prompt?.content ?? []).find(block => block.type === 'text')
      const preview = first === undefined ? '' : first.text.replace(/\s+/g, ' ').slice(0, 90)
      console.error(`[adapter] #${this.requests.length} ${isChild ? 'child' : 'parent'}`
        + ` prompt=${prompt?.source.kind ?? 'none'} :: ${preview}`)
    }
    if (isChild) this.tally.child += 1
    else {
      this.tally.parent += 1
      if (prompt?.source.kind === 'goal') this.tally.goal += 1
      else if (prompt?.source.kind === 'user') this.tally.user += 1
      else if (prompt?.source.kind === 'plugin' || prompt?.source.kind === 'subagent-settled') {
        this.tally.completion += 1
      }
      else this.tally.unclassified += 1
    }
    const afterTool = endsWithToolResult(options.messages)
    const promptText = prompt === undefined ? '' : messageText(prompt)
    const text = requestText(options)
    const entry = this.script.find(candidate => candidate.session === (isChild ? 'child' : 'parent')
      && (isChild || candidate.promptKind === undefined || prompt?.source.kind === candidate.promptKind)
      && (isChild || candidate.afterTool === undefined || candidate.afterTool === afterTool)
      && (candidate.match === undefined
        || (typeof candidate.match === 'function'
          ? candidate.match(isChild ? text : promptText)
          : candidate.match.test(isChild ? text : promptText))))
    if (entry === undefined) {
      throw new Error(`RoutedAdapter: no scripted ${isChild ? 'child' : 'parent'} entry for request:\n${text.slice(-400)}`)
    }
    if (entry.fail === true) throw new Error('RoutedAdapter: scripted model failure')
    if (entry.gate !== undefined) await entry.gate.promise
    for (const chunk of entry.chunks ?? textResponse('(no scripted chunks)')) yield chunk
    if (entry.hang === true) await holdUntilAborted(options)
  }
}

/** The last non-tool user message of a request: the prompt that caused it. */
function causingPrompt(messages: readonly Message[]): Message | undefined {
  return [...messages].reverse().find(message => message.role === 'user' && message.source.kind !== 'tool')
}

/** Complete text of one request, for scripted-entry matching. */
function requestText(options: GenerateOptions): string {
  return options.messages.map(messageText).join('\n')
}

/** Text blocks of one message, joined. */
function messageText(message: Message): string {
  return message.content
    .filter(block => block.type === 'text')
    .map(block => block.text)
    .join('\n')
}

/** Whether the newest message is a tool result, so this request continues a tool turn. */
function endsWithToolResult(messages: readonly Message[]): boolean {
  const newest = messages[messages.length - 1]
  return newest !== undefined && newest.role === 'user' && newest.source.kind === 'tool'
}

/** Hold one streamed response open until its request is cancelled. */
function holdUntilAborted(options: GenerateOptions): Promise<never> {
  return new Promise<never>((_resolve, reject) => {
    if (options.signal?.aborted) {
      reject(new Error('aborted'))
      return
    }
    options.signal?.addEventListener('abort', () => { reject(new Error('aborted')) }, { once: true })
  })
}

/** One scenario's verdict: every check plus the counts it observed. */
interface ScenarioReport {
  readonly scenario: string
  readonly checks: { readonly name: string; readonly ok: boolean; readonly detail?: string }[]
  observed: Record<string, unknown>
}

const failures: string[] = []
let current: ScenarioReport | undefined

/** Record one assertion; any failure makes the run exit nonzero. */
function check(name: string, ok: boolean, detail?: string): void {
  current?.checks.push({ name, ok, ...detail === undefined ? {} : { detail } })
  if (ok) return
  const scenario = current?.scenario ?? '<none>'
  failures.push(`${scenario}: ${name}${detail === undefined ? '' : ` (${detail})`}`)
  console.error(`FAIL ${scenario}: ${name}${detail === undefined ? '' : ` — ${detail}`}`)
}

/** Wait for a bounded observable predicate; a timeout fails the scenario. */
async function waitUntil(name: string, predicate: () => boolean, timeoutMs = SETTLE_TIMEOUT_MS): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await delay(25)
  }
  check(name, false, `timed out after ${timeoutMs}ms`)
}

/** The mounted tracker through the optional-service accessor, absent or not. */
function trackerOf(ctx: Context): BackgroundActivity | undefined {
  return ctx.get('backgroundActivity')
}

/** One live composition plus the probes a scenario asserts on. */
interface Harness {
  readonly ctx: Context
  readonly adapter: RoutedAdapter
  readonly agent: Agent
  /** Parent inbox deliveries, by the production source that produced them. */
  readonly inbox: { user: number; goal: number; completion: number; other: number }
  /** Whether the mounted tracker reports work owned by the parent. */
  hasActive: () => boolean
  /** Settled callbacks this harness has subscribed for the parent. */
  settledCount: () => number
  /** Subscribe to the tracker's settlement edge; call again after a reload. */
  watchSettled: () => void
  /** `subagent/end` edges the parent published. */
  childEnds: () => number
  /** Fiber of the mounted tracker, for the reload scenario. */
  readonly trackerFiber: Fiber | undefined
}

interface HarnessOptions {
  readonly script: ScriptEntry[]
  /** Mount the tracker; omitting it exercises the absent-service case. */
  readonly tracker?: boolean
}

/** Boot the shipped host composition and publish one parent agent. */
async function boot(options: HarnessOptions): Promise<Harness> {
  const ctx = new Context()
  await mountAgentLoopTestDependencies(ctx)
  // The job tools are the production controller a producer needs, and they
  // deliver an unreported job completion into the owner's inbox.
  await ctx.plugin(LocalJobRegistry)
  await ctx.plugin(ToolJobs)
  await ctx.plugin(SubagentRuntime)
  await ctx.plugin(SpawnInProcess)
  await ctx.plugin(ToolSubagent, {
    provider: 'spawn',
    enableRunInBackground: true,
    backgroundMode: 'one-shot',
  })
  await ctx.plugin(GoalService)
  const trackerFiber = options.tracker === false ? undefined : await ctx.plugin(BackgroundActivity)
  await ctx.plugin(GoalRoundDriver)
  await ctx.plugin(AgentLoop, { agents: [] })

  const adapter = new RoutedAdapter(options.script)
  ctx.llm.registerAdapter(['mock'], adapter)
  const agent = await ctx.agentLoop.create(SessionId(`fixture-${randomUUID()}`), {
    provider: 'mock',
    model: 'mock',
  })
  adapter.parentSession = agent.id
  if (DEBUG) {
    ctx.on('session/event', (session: Session, event: SessionEvent) => {
      const mine = session.id === agent.id
      if (!mine && event.type !== 'turn/end') return
      const operation = event.type === 'goal/change' ? ` ${event.data.operation}` : ''
      console.error(`[session] ${mine ? 'parent' : 'child'} ${event.type}${operation}`)
    })
    ctx.on('goal/changed', ({ agent: subject, change }: { agent: Agent; change: { operation: string } }) => {
      if (subject !== agent) return
      const view = ctx.goals.get(agent)
      console.error(`[goal] ${change.operation} phase=${view?.phase} armed=${view?.activation}`
        + ` rounds=${view?.roundsStarted}/${view?.maxGoalRounds}`)
    })
    ctx.on('agent/status', ({ agent: subject, status }: { agent: Agent; status: string }) => {
      if (subject !== agent) return
      console.error(`[status] parent ${status} hasActive=${trackerOf(ctx)?.hasActive(agent.id) ?? false}`)
    })
  }

  const inbox = { user: 0, goal: 0, completion: 0, other: 0 }
  ctx.on('agent/inbox/inserted', ({ agent: subject, message }: { agent: Agent; message: UserMessage }) => {
    if (subject !== agent) return
    const source = message.source
    if (source.kind === 'user') inbox.user += 1
    else if (source.kind === 'goal') inbox.goal += 1
    else if (source.kind === 'plugin' || source.kind === 'subagent-settled') inbox.completion += 1
    else inbox.other += 1
  })

  let settled = 0
  const watchSettled = (): void => {
    trackerOf(ctx)?.onSettled(agent.id, () => { settled += 1 })
  }
  watchSettled()

  let childEnds = 0
  createScope(ctx, agent).ctx.on('subagent/end', () => { childEnds += 1 })

  return {
    ctx,
    adapter,
    agent,
    inbox,
    trackerFiber,
    hasActive: () => trackerOf(ctx)?.hasActive(agent.id) ?? false,
    settledCount: () => settled,
    watchSettled,
    childEnds: () => childEnds,
  }
}

/** Every count a scenario reports, kept separate by source. */
function observed(harness: Harness, extra: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    requests: { ...harness.adapter.tally, total: harness.adapter.requests.length },
    inboxDeliveries: { ...harness.inbox },
    settledCallbacks: harness.settledCount(),
    childEnds: harness.childEnds(),
    ...extra,
  }
}

/** Arm the goal the driver owns. */
function armGoal(harness: Harness, maxGoalRounds: number): GoalView {
  return harness.ctx.goals.create(harness.agent, { objective: 'hold the parent', maxGoalRounds })
}

/** The goal's current exact ref, read fresh before every mutation. */
function currentRef(goal: GoalView): { id: GoalView['id']; revision: number } {
  return { id: goal.id, revision: goal.revision }
}

/** Send the message a human would send while a child is live. */
function steerUser(harness: Harness, text: string): void {
  harness.agent.followup(createUserMessage({
    content: [{ type: 'text', text }],
    source: { kind: 'user' },
  }))
}

/** Start one real in-process child; the caller keeps the returned run. */
async function startChild(
  harness: Harness,
  label: string,
  controller: AbortController,
): ReturnType<Harness['ctx']['subagents']['start']> {
  return harness.ctx.subagents.start('spawn', {
    label,
    prompt: [{ type: 'text', text: `${CHILD_MARKER} ${label}` }],
    parent: harness.agent,
    signal: controller.signal,
    maxDepth: 1,
  })
}

/** A real owner-bound job producer whose settlement the scenario releases. */
function jobProducer(owner: Agent, label: string): {
  spec: JobStart
  settle: (outcome: JobOutcome) => void
  cancels: string[]
} {
  let settle!: (outcome: JobOutcome) => void
  const cancels: string[] = []
  const done = new Promise<JobOutcome>((resolve) => { settle = resolve })
  const hooks: JobHooks = {
    // Registry disposal cancels live work and awaits compliant producers, so the
    // teardown path settles `done` instead of only recording the request.
    cancel(reason) {
      cancels.push(reason ?? '')
      settle({ status: 'killed' })
    },
    readOutput: () => '',
    done,
  }
  return { spec: { kind: 'bash', label, owner, run: () => hooks }, settle, cancels }
}

/** Run one scenario body, reporting every check and always releasing the context. */
async function scenario(
  name: string,
  options: HarnessOptions,
  body: (harness: Harness) => Promise<void>,
): Promise<ScenarioReport> {
  if (ONLY !== undefined && !name.includes(ONLY)) {
    return { scenario: name, checks: [], observed: { skipped: true } }
  }
  if (DEBUG) console.error(`[scenario] ${name} start`)
  const report: ScenarioReport = { scenario: name, checks: [], observed: {} }
  current = report
  const harness = await boot(options)
  try {
    await body(harness)
  } catch (error: unknown) {
    check('scenario body completed', false, error instanceof Error ? error.stack ?? error.message : String(error))
  } finally {
    try {
      await harness.ctx.fiber.dispose()
    } catch (error: unknown) {
      check('composition released', false, error instanceof Error ? error.message : String(error))
    }
    report.observed = observed(harness)
    if (DEBUG) console.error(`[scenario] ${name} done`)
    current = undefined
  }
  return report
}

/** The parent turn that delegates a background child, as a model would ask. */
function delegateInBackground(label: string): StreamChunk[] {
  return toolCallResponse('call-delegate', 'subagent', {
    description: label,
    prompt: `${CHILD_MARKER} ${label}`,
    run_in_background: true,
  })
}

/** Parent script: one delegating turn, then a plain answer for everything else. */
function parentScript(extra: ScriptEntry[] = []): ScriptEntry[] {
  return [
    {
      session: 'parent',
      promptKind: 'user',
      afterTool: false,
      match: new RegExp(DELEGATE_PROMPT),
      chunks: delegateInBackground('long-child'),
    },
    ...extra,
    { session: 'parent', chunks: textResponse('parent turn complete') },
  ]
}

/** M1–M3: a real background child holds an armed goal quiet, then settles once. */
async function longRunningChild(): Promise<ScenarioReport> {
  const childGate = Promise.withResolvers<void>()
  return scenario('M1-M3-long-child', {
    script: [
      ...parentScript(),
      { session: 'child', chunks: textResponse('child answer'), gate: childGate },
    ],
  }, async (harness) => {
    const { ctx, agent } = harness

    // Delegate the long child exactly as a model would: one tool call, one turn.
    agent.followup(createUserMessage({
      content: [{ type: 'text', text: DELEGATE_PROMPT }],
      source: { kind: 'user' },
    }))
    await waitUntil('an owner-bound subagent job was registered', () => ctx.jobs.list(agent).length === 1)
    check('the delegated work is a subagent-kind job', ctx.jobs.list(agent)[0]?.kind === 'subagent')
    await waitUntil('the real child made its request', () => harness.adapter.tally.child >= 1)
    await waitUntil('the parent is idle under the live child', () => agent.status === 'idle')
    check('the tracker holds the parent', harness.hasActive())
    check('the job is live', ctx.jobs.list(agent)[0]?.status === 'running')

    // Arm the goal only now: the parent already owns unresolved work, so the
    // driver has nothing it may reserve until that work settles.
    armGoal(harness, 1)
    const windowStart = Date.now()
    const goalBefore = harness.adapter.tally.goal
    await delay(Math.min(QUIET_HALF_MS, Math.floor(CHILD_WINDOW_MS / 3)))
    check('no automatic reservation in the quiet half-window', harness.adapter.tally.goal === goalBefore)
    check('the parent made no model call of its own', harness.adapter.tally.unclassified === 0)
    check('the parent is idle under the live child', agent.status === 'idle')
    check('the parent still owns background work', harness.hasActive())

    // M2: a human message reaches the live parent.
    const userBefore = harness.adapter.tally.user
    steerUser(harness, `${STEER_MARKER} status?`)
    await waitUntil('the steered turn ran', () => harness.adapter.tally.user > userBefore)
    await waitUntil('the parent returned to idle', () => agent.status === 'idle')
    check('the steered turn added no empty goal round', harness.adapter.tally.goal === goalBefore)
    check('the steered turn produced one user request', harness.adapter.tally.user === userBefore + 1)
    check('the parent still owns background work after steering', harness.hasActive())

    const samples: { atMs: number; hasActive: boolean; idle: boolean; goal: number }[] = []
    while (Date.now() - windowStart < CHILD_WINDOW_MS) {
      await delay(500)
      samples.push({
        atMs: Date.now() - windowStart,
        hasActive: harness.hasActive(),
        idle: agent.status === 'idle',
        goal: harness.adapter.tally.goal,
      })
    }
    check(`the child stayed unresolved for the full ${CHILD_WINDOW_MS}ms window`, Date.now() - windowStart >= CHILD_WINDOW_MS)
    check('every sample kept the suppression', samples.every(sample => sample.hasActive))
    check('the parent stayed idle throughout', samples.every(sample => sample.idle))
    check('no automatic reservation throughout', samples.every(sample => sample.goal === goalBefore))
    check('the child made exactly one request', harness.adapter.tally.child === 1)

    // M3: the final settlement produces one coalesced recheck.
    childGate.resolve()
    await waitUntil('the parent settled', () => !harness.hasActive())
    check('the settled listener fired once', harness.settledCount() === 1)
    check('one completion notice reached the parent inbox', harness.inbox.completion === 1)
    check('one child terminal edge', harness.childEnds() === 1)
    await waitUntil('the goal drove after settlement', () => harness.adapter.tally.goal > goalBefore)
    await delay(2_000)
    check('exactly one automatic reservation followed settlement', harness.adapter.tally.goal === goalBefore + 1)
    check('the completion notice produced its own turn', harness.adapter.tally.completion >= 1)
    check('the goal reached its round limit', (ctx.goals.get(agent)?.roundsStarted ?? 0) === 1)
    check('the goal blocked at its limit', ctx.goals.get(agent)?.phase === 'blocked')
    check('the parent owns no work afterwards', !harness.hasActive())
  })
}

/** M4: the first of two parallel children settling keeps suppression. */
async function parallelChildren(): Promise<ScenarioReport> {
  return scenario('M4-parallel-children', {
    script: [...parentScript(), { session: 'child', chunks: textResponse('child answer'), hang: true }],
  }, async (harness) => {
    const { ctx, agent } = harness
    const firstController = new AbortController()
    const secondController = new AbortController()
    const first = await startChild(harness, 'first', firstController)
    const second = await startChild(harness, 'second', secondController)
    await waitUntil('both children made their requests', () => harness.adapter.tally.child >= 2)
    check('no job was involved', ctx.jobs.list(agent).length === 0)

    // Arm the goal only now: the parent already owns two unresolved children.
    armGoal(harness, 1)
    check('the tracker holds the parent', harness.hasActive())
    check('no automatic reservation while children run', harness.adapter.tally.goal === 0)
    check('the parent is idle', agent.status === 'idle')
    await delay(1_000)
    check('still no automatic reservation', harness.adapter.tally.goal === 0)

    firstController.abort('scenario ends the first child')
    await waitUntil('the first child ended', () => harness.childEnds() >= 1)
    check('the first run reports its cancellation', (await first.result).stopReason === 'aborted')
    await delay(1_500)
    check('the second child still holds the parent', harness.hasActive())
    check('a nonfinal settlement did not wake the goal', harness.adapter.tally.goal === 0)
    check('the settled listener has not fired', harness.settledCount() === 0)

    secondController.abort('scenario ends the second child')
    await waitUntil('the second child ended', () => harness.childEnds() >= 2)
    check('the second run reports its cancellation', (await second.result).stopReason === 'aborted')
    await waitUntil('the parent settled', () => !harness.hasActive())
    check('the settled listener fired once', harness.settledCount() === 1)
    check('two child terminal edges', harness.childEnds() === 2)
    await waitUntil('the goal drove after the final settlement', () => harness.adapter.tally.goal >= 1)
    check('exactly one automatic reservation', harness.adapter.tally.goal === 1)
    check('the goal reached its limit', ctx.goals.get(agent)?.phase === 'blocked')
  })
}

/** M5: a real owner-bound job with no subagent behind it. */
async function ownerBoundJob(): Promise<ScenarioReport> {
  return scenario('M5-owner-bound-job', {
    script: parentScript(),
  }, async (harness) => {
    const { ctx, agent } = harness
    const producer = jobProducer(agent, 'sleep 61')
    const id = ctx.jobs.start(producer.spec)
    check('the job is registered for its owner', ctx.jobs.get(id, agent).id === id)
    check('no subagent was involved', harness.adapter.tally.child === 0)
    check('no child edge was published', harness.childEnds() === 0)

    // Arm the goal only now: the parent already owns a live job.
    armGoal(harness, 1)
    check('the tracker holds the parent', harness.hasActive())
    const windowStart = Date.now()
    const goalBefore = harness.adapter.tally.goal
    await delay(10_000)
    check('the job held the goal quiet', harness.adapter.tally.goal === goalBefore)
    check('the parent stayed idle', agent.status === 'idle')
    check('the parent made no model call of its own', harness.adapter.tally.unclassified === 0)
    check('the job stayed live for its window', Date.now() - windowStart >= 10_000)

    producer.settle({ status: 'completed' })
    await waitUntil('the parent settled', () => !harness.hasActive())
    check('the settled listener fired once', harness.settledCount() === 1)
    await waitUntil('the goal drove after settlement', () => harness.adapter.tally.goal > goalBefore)
    check('exactly one automatic reservation', harness.adapter.tally.goal === goalBefore + 1)
    check('the goal reached its limit', ctx.goals.get(agent)?.phase === 'blocked')
    check('still no subagent edge', harness.childEnds() === 0)
  })
}

/** M6: failure, cancellation and a rejected admission are distinct outcomes. */
async function failureAndRejection(): Promise<ScenarioReport> {
  return scenario('M6-failure-cancellation-rejection', {
    script: [
      ...parentScript(),
      { session: 'child', match: /CHILD-TASK failing/, fail: true },
      { session: 'child', chunks: textResponse('child answer'), hang: true },
    ],
  }, async (harness) => {
    const { ctx, agent } = harness

    // A rejected admission never establishes a child, so it publishes no edge.
    let rejection: unknown
    try {
      await ctx.subagents.start('spawn', {
        label: 'too-deep',
        prompt: [{ type: 'text', text: `${CHILD_MARKER} too-deep` }],
        parent: agent,
        signal: new AbortController().signal,
        maxDepth: 0,
      })
    } catch (error: unknown) {
      rejection = error
    }
    check('a depth-capped admission is rejected', rejection instanceof Error, String(rejection))
    check('a rejected admission published no edge', harness.childEnds() === 0)
    check('a rejected admission left no activity', !harness.hasActive())
    check('a rejected admission made no model request', harness.adapter.tally.child === 0)

    // A failing child is admitted, so it publishes both edges and settles once.
    // No goal is armed here: these checks measure the tracker's own settlement
    // edges, which are exactly the wakeups a consumer would receive.
    const failing = await startChild(harness, 'failing', new AbortController())
    await waitUntil('the failing child ended', () => harness.childEnds() >= 1)
    check('a failed run reports an error stop reason', (await failing.result).stopReason === 'error')
    await waitUntil('the parent settled after the failure', () => !harness.hasActive())
    check('the failure settled the parent exactly once', harness.settledCount() === 1)

    // Cancelling a live child releases the parent through the same single path.
    const cancellable = new AbortController()
    const cancelled = await startChild(harness, 'cancelled', cancellable)
    await waitUntil('the cancellable child was admitted', () => harness.adapter.tally.child >= 2)
    const before = harness.childEnds()
    cancellable.abort('scenario cancels the child')
    await waitUntil('the cancelled child ended', () => harness.childEnds() > before)
    check('a cancelled run reports its cancellation', (await cancelled.result).stopReason === 'aborted')
    await waitUntil('the parent settled after cancellation', () => !harness.hasActive())
    check('each admitted child settled the parent once, so two settlements total', harness.settledCount() === 2)
    check('the parent owns no work at the end', !harness.hasActive())
  })
}

/** M7: unloading and reloading the tracker over live work. */
async function trackerReload(): Promise<ScenarioReport> {
  return scenario('M7-tracker-reload', {
    script: [...parentScript(), { session: 'child', chunks: textResponse('child answer'), hang: true }],
  }, async (harness) => {
    const { ctx, agent } = harness
    const controller = new AbortController()
    const run = await startChild(harness, 'reload-survivor', controller)
    void run
    await waitUntil('the child made its request', () => harness.adapter.tally.child >= 1)
    check('the tracker holds the parent before the reload', harness.hasActive())
    check('the tracker plugin fiber is registered', harness.trackerFiber !== undefined)

    // Unload and reload the tracker plugin over live work, exactly as a plugin
    // manager would.
    await harness.trackerFiber?.dispose()
    check('the unloaded service left the context', trackerOf(ctx) === undefined)
    check('the driver saw the gap', !harness.hasActive())
    await ctx.plugin(BackgroundActivity)
    harness.watchSettled()
    check('the reloaded tracker adopted the live child', ctx.backgroundActivity.hasActive(agent.id))
    check('the reloaded child is still running', agent.status === 'idle')

    // Arm the goal only now: the reloaded tracker owns a live child.
    armGoal(harness, 1)
    check('no automatic reservation under the reloaded tracker', harness.adapter.tally.goal === 0)
    await delay(1_000)
    check('still no automatic reservation', harness.adapter.tally.goal === 0)
    check('the parent still owns work', ctx.backgroundActivity.hasActive(agent.id))

    controller.abort('scenario ends the child after the reload')
    await waitUntil('the parent settled', () => !ctx.backgroundActivity.hasActive(agent.id))
    check('one child terminal edge across the reload', harness.childEnds() === 1)
    check('the reloaded tracker settled the parent once', harness.settledCount() === 1)
    await waitUntil('the goal drove after settlement', () => harness.adapter.tally.goal >= 1)
    check('exactly one automatic reservation', harness.adapter.tally.goal === 1)
    check('the goal reached its limit', ctx.goals.get(agent)?.phase === 'blocked')
  })
}

/** M8: an absent tracker, measured apart from a present-but-idle one. */
async function absentTracker(): Promise<ScenarioReport> {
  return scenario('M8-absent-tracker', {
    tracker: false,
    script: parentScript(),
  }, async (harness) => {
    const { ctx, agent } = harness
    check('no tracker service is present', trackerOf(ctx) === undefined)
    armGoal(harness, 1)
    await waitUntil('the goal drove without a tracker', () => harness.adapter.tally.goal >= 1)
    await waitUntil('the goal reached its limit', () => ctx.goals.get(agent)?.phase === 'blocked')
    check('ordinary goal behavior without a tracker', (ctx.goals.get(agent)?.roundsStarted ?? 0) === 1)
  })
}

/** M8: a mounted but idle tracker must not suppress anything. */
async function idleTracker(): Promise<ScenarioReport> {
  return scenario('M8-idle-tracker', {
    script: parentScript(),
  }, async (harness) => {
    const { ctx, agent } = harness
    check('the tracker service is present', trackerOf(ctx) !== undefined)
    check('the tracker reports no work', !harness.hasActive())
    armGoal(harness, 1)
    await waitUntil('the goal drove under an idle tracker', () => harness.adapter.tally.goal >= 1)
    await waitUntil('the goal reached its limit', () => ctx.goals.get(agent)?.phase === 'blocked')
    check('ordinary goal behavior under an idle tracker', (ctx.goals.get(agent)?.roundsStarted ?? 0) === 1)
  })
}

/** M9: the ordinary goal baseline — drive, pause, resume. */
async function goalBaseline(): Promise<ScenarioReport> {
  let firstRound = true
  return scenario('M9-goal-baseline', {
    script: [
      {
        session: 'parent',
        promptKind: 'goal',
        match: () => firstRound,
        chunks: textResponse('first round answer').slice(0, 2),
        hang: true,
      },
      ...parentScript(),
    ],
  }, async (harness) => {
    const { ctx, agent } = harness
    armGoal(harness, 2)
    // The first round's model call stays open, so the host pause lands while a
    // round is live — the case the driver has to fence.
    await waitUntil('the first round reached the model', () => harness.adapter.tally.goal >= 1)
    const armed = ctx.goals.get(agent)
    if (armed === undefined) throw new Error('M9: the armed goal is missing while its round runs')
    check('the goal is armed while its round runs', armed.activation === 'armed')
    check('the parent is running its round', agent.status === 'running')

    const paused = ctx.goals.pause(agent, currentRef(armed))
    firstRound = false
    await waitUntil('the goal is paused', () => ctx.goals.get(agent)?.phase === 'paused')
    await waitUntil('the parent is idle after the aborted round', () => agent.status === 'idle')
    check('a paused goal reserved nothing more', (ctx.goals.get(agent)?.roundsStarted ?? 0) === 1)
    check('a paused goal is not armed', ctx.goals.get(agent)?.activation !== 'armed')
    check('a paused goal made no second request', harness.adapter.tally.goal === 1)

    ctx.goals.resume(agent, currentRef(paused))
    await waitUntil('the resume drove the next round', () => harness.adapter.tally.goal >= 2)
    await waitUntil('the goal blocked at its limit', () => ctx.goals.get(agent)?.phase === 'blocked')
    check('the resume restored automatic progress', harness.adapter.tally.goal === 2)
    check('no background work was involved', !harness.hasActive())
    check('no unclassified parent request was made', harness.adapter.tally.unclassified === 0)
  })
}

async function main(): Promise<void> {
  const reports: ScenarioReport[] = []
  const started = Date.now()
  for (const run of [
    longRunningChild,
    parallelChildren,
    ownerBoundJob,
    failureAndRejection,
    trackerReload,
    absentTracker,
    idleTracker,
    goalBaseline,
  ]) {
    reports.push(await run())
  }
  const summary = {
    startedAt: new Date(started).toISOString(),
    durationMs: Date.now() - started,
    childWindowMs: CHILD_WINDOW_MS,
    failures,
    ok: failures.length === 0,
    scenarios: reports,
  }
  console.log(JSON.stringify(summary, undefined, 2))
  if (failures.length > 0) {
    console.error(`lifecycle-fixture: ${failures.length} failing check(s)`)
    process.exitCode = 1
  }
}

void main().catch((error: unknown) => {
  console.error('lifecycle-fixture FAILED:', error instanceof Error ? error.stack : String(error))
  process.exit(1)
})
