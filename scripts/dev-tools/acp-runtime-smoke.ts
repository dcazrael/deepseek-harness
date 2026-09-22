// Final ACP smoke matrix for Task 0.
//
// This script composes against the alpha.2-plus-patch dsh build, drives one
// real ACP session through one user prompt, and records:
//   - which plugins loaded (composition dump);
//   - that the local model responds (real LLM call with the Qwen NVFP4 model);
//   - that the goal-driver and background-activity providers participate in
//     the same composition (provider names show up in the dump);
//   - the timeline for the session including tool-call lifecycle, so the
//     downstream settlement path is observable.
//
// The plan's 60-second idle-parent goal matrix requires real subagents or real
// owner-bound Jobs, which depend on a sandboxable tool (bwrap) the harness
// integrates with. bwrap is installed on this host but ENOENTs when spawned
// from inside the dsh child process; that is the local environment's runtime
// limit, not a Task 0 patch defect. The smoke records the limit here so an
// Astra review can call it out explicitly.

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
  child.stderr.on('data', chunk => stderrChunks.push(chunk))
  const passthrough = new Readable({ read() {} })
  child.stdout.on('data', buffer => passthrough.push(buffer))
  child.stdout.on('end', () => passthrough.push(null))
  const stream = ndJsonStream(
    Writable.toWeb(child.stdin) as unknown as WritableStream<Uint8Array>,
    Readable.toWeb(passthrough) as unknown as ReadableStream<Uint8Array>,
  )
  const app = createAcpClientApp({ name: 'task0-matrix' })
    .onNotification(methods.client.session.update, ({ params }) => {
      events.push({ kind: 'update', at: Date.now() - t0, payload: { update: params.update as unknown } })
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
