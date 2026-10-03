// ACP composition smoke for Task 0.
//
// This script composes against the alpha.2-plus-patch dsh build through the
// disposable profile, drives one real ACP session through one user prompt, and
// records:
//   - which plugins loaded (composition dump);
//   - whether the configured local model route answers, with the model id the
//     endpoint actually served;
//   - that the goal-round driver and the background-activity tracker are part
//     of that same composition (both names appear in the dump);
//   - the session timeline including tool-call lifecycle.
//
// It is a composition and model smoke, not the goal-scheduler matrix: the
// matrix scenarios (real child held unresolved, owner-bound jobs, settlement
// accounting) run in `lifecycle-fixture.ts` against the same shipped services.

import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process'
import { execFileSync } from 'node:child_process'
import { Readable, Writable } from 'node:stream'
import { setTimeout as delay } from 'node:timers/promises'
import {
  client as createAcpClientApp,
  ndJsonStream,
  methods,
} from '/tmp/opencode/wp-alpha2/node_modules/.pnpm/@agentclientprotocol+sdk@1.4.0_zod@4.4.3/node_modules/@agentclientprotocol/sdk/dist/acp.js'

const AGENT_BIN = '/tmp/opencode/wp-alpha2/apps/cli/lib/bin.js'
const HOME = '/home/azrael/.dsh'
const PROFILE = 'wp-task0'

interface MatrixEvent {
  kind: string
  at: number
  payload?: Record<string, unknown>
}

function compositionDump(): MatrixEvent {
  const stdout = execFileSync(process.execPath, [AGENT_BIN, '--profile', PROFILE, '--dump-config'], {
    env: { ...process.env, DSH_HOME: HOME, LLAMACPP_API_KEY: 'noop' },
    encoding: 'utf8',
    timeout: 10000,
  })
  const ids: string[] = []
  for (const line of stdout.split('\n')) {
    const match = /^(- (?:id:|insert:|replace:)|  - id:|\s+# == )/.exec(line)
    if (!match) continue
    if (match[1] === '# == ') ids.push(`layer:${line.replace(/^# == /, '').trim()}`)
    const idMatch = /^- id: ([\w-]+)/.exec(line)
    if (idMatch) ids.push(`row:${idMatch[1]}`)
    const innerMatch = /^\s+- id: ([\w-]+)/.exec(line)
    if (innerMatch) ids.push(`row:${innerMatch[1]}`)
  }
  return { kind: 'composition', at: 0, payload: { rowCount: ids.length, ids } }
}

async function runScenario(label: string, prompt: string, settleWindowMs = 8000): Promise<MatrixEvent[]> {
  const events: MatrixEvent[] = []
  const t0 = Date.now()
  const child: ChildProcessWithoutNullStreams = spawn(
    process.execPath,
    [AGENT_BIN, '--profile', PROFILE],
    { env: { ...process.env, DSH_HOME: HOME, LLAMACPP_API_KEY: 'noop', DSH_TELEMETRY_DISABLED: '1' }, cwd: '/tmp' },
  )
  const stderrChunks: string[] = []
  child.stderr.setEncoding('utf8')
  child.stderr.on('data', (chunk: string) => { stderrChunks.push(chunk) })
  const passthrough = new Readable({ read() {} })
  child.stdout.on('data', (buffer: Buffer) => { passthrough.push(buffer) })
  child.stdout.on('end', () => { passthrough.push(null) })
  const stream = ndJsonStream(
    Writable.toWeb(child.stdin),
    // The passthrough is a byte stream; `toWeb` widens its element type to any.
    Readable.toWeb(passthrough) as ReadableStream<Uint8Array>,
  )
  const app = createAcpClientApp({ name: 'task0-matrix' })
    .onNotification(methods.client.session.update, ({ params }) => {
      events.push({ kind: 'update', at: Date.now() - t0, payload: { update: params.update } })
    })
    .onRequest(methods.client.session.requestPermission, () =>
      Promise.resolve({ outcome: { outcome: 'cancelled' } }),
    )
  const connection = app.connect(stream)
  const agent = connection.agent

  try {
    const initRes = await agent.request(methods.agent.initialize, { protocolVersion: 1, clientCapabilities: {} })
    events.push({ kind: 'initialize', at: Date.now() - t0, payload: { agentCapabilities: (initRes as { agentCapabilities?: unknown }).agentCapabilities } })
    const newRes = await agent.request(methods.agent.session.new, { cwd: `/tmp/wp-task0-${label}`, mcpServers: [] })
    const sessionId = (newRes as { sessionId: string }).sessionId
    events.push({ kind: 'newSession', at: Date.now() - t0, payload: { sessionId, label } })
    const t_prompt = Date.now()
    try {
      const promptRes = await agent.request(methods.agent.session.prompt, {
        sessionId,
        prompt: [{ type: 'text', text: prompt }],
      })
      events.push({
        kind: 'prompt_done',
        at: Date.now() - t0,
        payload: { elapsedMs: Date.now() - t_prompt, stopReason: (promptRes as { stopReason?: string }).stopReason },
      })
    } catch (error: unknown) {
      events.push({ kind: 'prompt_error', at: Date.now() - t0, payload: { message: error instanceof Error ? error.message : String(error) } })
    }
    const settleTarget = Date.now() + settleWindowMs
    while (Date.now() < settleTarget) await delay(200)
    events.push({ kind: 'done', at: Date.now() - t0, payload: { stderrTail: stderrChunks.slice(-12).join('') } })
  } finally {
    if (!child.killed) child.kill('SIGTERM')
    await delay(300)
    if (!child.killed) child.kill('SIGKILL')
  }
  return events
}

async function main(): Promise<void> {
  const label = process.argv[2] ?? 'smoke'
  const prompt = process.argv[3] ?? 'Tell me: what is the value of HOME env?'
  const dump = compositionDump()
  const events = await runScenario(label, prompt)
  console.log(JSON.stringify({ scenario: label, composition: dump, events }, undefined, 2))
}

main().catch((error: unknown) => {
  console.error('SMOKE_ERROR', error instanceof Error ? error.stack : String(error))
  process.exit(1)
})
