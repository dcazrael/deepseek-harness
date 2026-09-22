---
description: "面向 scheduler 的父 scoped 未完成 background work (subagents 与 jobs) 追踪器，让目标驱动在父代仍持有 live work 时抑制其自动轮次预订。"
kind: "package-reference"
---

# @deepseek-ai/dsh-background-activity

[English](README.md) | 中文

## 摘要

`dsh-background-activity` 回答一个简单问题：父 agent 是否还拥有由其 session id 启动的仍在运行的 subagent 或 job。消费者订阅“父代的最后一个 activity 沉降”这一时刻，而不依赖于具体 worker 的实现。scheduler 用该信号在父代仍持有活子任务时阻塞其自动目标轮次（goal-round），并在最终沉降时一次性唤醒驱动。

## 目录

- [Use this package](#use-this-package)
- [Understand the implementation](#understand-the-implementation)
- [Further Exploration](#further-exploration)
- [Model Experience](#model-experience)
- [Known Limitations and Deferred Work](#known-limitations-and-deferred-work)
- [Dev Note](#dev-note)

-----

<a id="use-this-package"></a>
## 如何使用

在需要让 scheduler 在父 agent 持有未结束的 background work 时挂起其目标驱动的组合中加载该插件。插件在 host context 上注册 `backgroundActivity` 服务，并把每个 agent 的 subagent 生命周期事件与来自 host 域 jobs 服务的变化绑定在一起。

### 组合

```yaml
- name: '@deepseek-ai/dsh-session'
- name: '@deepseek-ai/dsh-subagent'
- name: '@deepseek-ai/dsh-jobs-local'
- name: '@deepseek-ai/dsh-background-activity'
- name: '@deepseek-ai/dsh-goal'
- name: '@deepseek-ai/dsh-goal-round-driver'
```

### 条目含义

插件是一个 service：`static inject = ['agents']` 声明了唯一的必需依赖；具体加载哪个 worker provider 或 jobs 实现，与该插件无关。插件发布 `ctx.backgroundActivity`，goal-round-driver 通过 `ctx.get(name)` 读取，正因为该服务是可选的。

### 查询与订阅

公共接口由两个方法组成（`BackgroundActivityView`）：

| 方法 | 行为 |
|---|---|
| `hasActive(parentId)` | 返回给定父 agent id 是否仍持有任何运行中或正在停止的 subagent run 或 job。该查询只读，同步返回结果。 |
| `onSettled(parentId, callback)` | 订阅“父代的最后一个 owned activity 被清空”这一时刻。每次从非空到空的转换都会调用一次回调；订阅时已经处于静止状态的父代不会被再次触发。返回的函数用于取消订阅。 |

失败或缺失的 subagent、jobs 集成不会使插件崩溃：插件追踪它能观察到的所有来源，`hasActive` 报告的是它们的并集。

### 条目不做什么

插件不持有执行资源——subagent 由 subagent runtime 负责，job 由 jobs 服务负责。它只保存每个父 agent 的 activity 集合以及每个父 agent 的 settled 回调集合，从不向模型发送任何内容。

-----

<a id="understand-the-implementation"></a>
## 理解实现

<details>
<summary>实现内部——点击展开</summary>

本节解释 tracker 的数据结构与生命周期边界；可观察行为完全在 [如何使用](#use-this-package) 中说明。

### 设计思路

每个 owned activity 在父代集合中占据一个槽位，标识符为 `'subagent:<runId>'` 或 `'job:<jobId>'`。并行启动与结算不会让一个计数器失去同步，因为根本不存在计数器——每个槽位都是独立的。基于键的集合解决了计数器无法处理的边界情况：在同伴到达之前已经完成的 run、在 subagent 仍在时删除的 job、或者针对 tracker 已丢弃键的结算通知。

### 源码索引

| 文件 | 角色 |
|---|---|
| [`src/index.ts`](src/index.ts) | Service 声明、每个 agent 的 scope 订阅、jobs 调和、settled 调度。 |

### 生命周期边界

- **每个 agent 的 scope** — `createScope(ctx, agent)` 为每个 agent 创建一个 scope，并注册 `subagent/start` 与 `subagent/end` 监听，记录 activity 转换；在 `agent/disposed` 上释放 scope 并清空其状态。
- **可选 Jobs 集成** — 当 `ctx.get('jobs')` 返回 registry 时，订阅 `onJobsChanged`，将 owner-scoped live 集合与父代 store 中的 jobs 做 diff，并在父代集合清空时触发 settled；disposer 在插件 dispose 时释放，保证 reload 不会持有陈旧订阅。
- **Mounted-over-existing agents** — 构造函数遍历 `ctx.agents.list()` 并 seed 现存活工作，保证 tracker 可以观察到先于插件被创建的 agent。
- **Disposal** — effects-attached 清理会释放每个 agent 的 scope、释放 jobs 订阅、并清空保留的 activity 与 settled 状态；连续两次调用 `dispose()` 是空操作。

### settled 调度的容错

`notifySettled` 对每个回调都在内部 try/catch 与异步 catch 中执行，使单个抛出或拒绝的监听者不会在此处引发未处理的 rejection；同步 try/catch 仍会记录 warn。

</details>

-----

<a id="further-exploration"></a>
## 进一步阅读

合约不够时再读这些页面。它们会继续深入到相邻的 session 与 scheduler 包。

- [Session package map](../README.zh.md) — 相邻的 session、projection、persistence packages。
- [Goal subsystem](../../../docs/subsystems/goal.zh.md) — 消费此 tracker、用以抑制自动目标轮的 scheduler。
- [Jobs package](../../jobs/jobs/README.zh.md) — 该 tracker 调和进父代集合的并行桶生命周期。

-----

<a id="model-experience"></a>
## 模型体验

Tracker 仅观察 worker；它从不组装或发送 provider 请求。父代目标若本会在持有活工作的情况下预订一轮，会由 goal-round-driver 一直挂起，直到 tracker 触发 `onSettled`；驱动再预订恰好一轮一次性合并，并恢复既有 pause/resume 流程。

### Goal-round suppression signal——目标轮抑制信号

#### 模型看到的内容

模型从该包看不到任何内容。Tracker 是一个被动观察者，向其 consumer（goal-round-driver）暴露一个 service 句柄；它不发布任何 schema、prompt、result 文本或其它模型输入，goal driver 也只用它来对自己的自动调度决策放行。

#### Token 效果

无。该包对任何请求都不增加 token —— 每一轮 provider round 都归 goal driver 或模型侧工具所有，而非 tracker。

#### KV Cache 效果

无。该包不参与任何模型请求，因此不会影响任何 provider 调用的重建内容或缓存键。Tracker 的 settled 信号仅影响目标轮节奏，而不影响实际运行的那些 round 的内容。

## 已知限制与待办

<a id="known-limitations-and-deferred-work"></a>

这些限制定义了 tracker 所描述的内容以及它缺席时的行为。它们是当前包的约束。

- **可选服务意味着可选输入** — 不含 `ctx.jobs` 的组合仍可追踪 subagent work，但永远不会调和 jobs，并且 goal driver 无法压制未加入本作用域的 jobs 触发的 round。
- **只提供集合级查询** — `hasActive` 对一个父代 id 返回布尔值；需要枚举 live activity id 的消费者必须自行记录它们，或扩展 `BackgroundActivityView`。
- **监听器按 settle 触发，不按 start 触发** — 公共契约是清空转换，因此希望收到 start 通知的消费者必须自己注册 subagent 与 jobs 事件。

<a id="dev-note"></a>
### 维护者备忘

<details>
<summary>面向维护者的工作上下文——点击展开</summary>

当 scheduler 补丁 `worker-profiles/0-alpha2-compat` 在 host 上添加 `ctx.backgroundActivity` 时，goal-round-driver 订阅的是 `ctx.on('internal/service', ...)` 而不是 `internal/plugin`，因此 rebind 运行在新 service 对其它 fiber 可见的时刻，而不是新 fiber 仍处 PENDING 的时刻。Tracker 在 reload 之间不保留状态：`dispose()` 会清空每个父代的集合并释放 jobs 订阅，确保 Jobs 服务不会跨 teardown 持有引用。

</details>
