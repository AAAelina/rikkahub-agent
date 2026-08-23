# RikkaHub Dream 双人画像实施清单

> 目标：把 Dream 从 Memory 派生整理器改成“斯啾伊 × 七七”的独立离线认知系统，并完成 User / Assistant / Relationship 三画像、独立 Experience Ledger、独立 Dream 时钟，以及七七可主动查看自己 Dream 的只读接口。

## 执行方式

- 主线负责人：G宝。
- 开启 **1 个实现子代理**，只负责数据层、DAO、迁移、新 Adapter 等机械多文件接线；不并发修改 `GenerationHandler.kt`、`ChatService.kt`、现有 Dream 核心状态机等容易冲突的文件。
- G宝负责：架构收口、Dream synthesis、Prompt/validator、七七 Dream 查看接口、Runtime 接线、最终验收。
- 不在项目中途追加无关功能，不为了未来扩展提前造抽象层。
- 每个实施批次连续做完后只做定向编译/测试；全部功能完成后再做一次集中回归。
- 不要求长期 shadow 双轨或额外防御层；沿用现有 Dream 开关，完成后直接进入正式路径。

## Batch 1：Dream 独立地基

- [x] 新建 `DreamPairScope`，以当前用户 + assistantId 作为 Dream 作用域，不再由 `useGlobalMemory` 决定 Dream scope。
- [x] 新建 `dream_experiences`，保存经历摘要、来源引用、时间、actor、experience kind、salience、identity/relationship weight、digest、状态。
- [x] 新建 `dream_experience_state`，保存 `experienceEpoch / observerCheckpointEpoch / appliedExperienceEpoch / profileRevision / activeSnapshotId / experienceDebt`。
- [x] 新建 Experience DAO / store，并完成 Room migration/schema 导出。
- [x] 实现 `ConversationEpisodeAdapter`：直接从连续聊天 turn 形成 episode 并写入 Experience Ledger。
- [x] 实现 `MemoryAdapter`：把现有已确认 Memory / narrative event / insight / theory 作为普通 Dream source 写入 Ledger。
- [x] 保留原始聊天为最高完整度来源；Ledger 不重复保存大段聊天正文，只保存稳定 source refs、摘要和 digest。
- [x] 将新的聊天完成事件接到 Experience ingest；Memory V2 继续独立工作，不再拥有 Dream 的触发权。
- [x] Batch 1 完成后执行 Room/KSP 与 `:app:compileDebugKotlin` 定向验证。

## Batch 2：Dream Synthesis 改读 Experience

- [x] Pair Dream 的 fence 改用 `baseExperienceEpoch / baseAppliedExperienceEpoch` 语义；数据库旧列名仅为迁移兼容，不再代表 Pair Dream authority。
- [x] Claim 直接绑定 Experience provenance；为避免重复抽象，沿用现有 pin 容器承载 Experience ID / epoch / source-manifest，而未另造仅改名的 `DreamExperiencePin` 类。
- [x] FULL / INCREMENTAL synthesis 输入改读 Experience cursor，不再以 `MemoryEntity` 作为核心输入。
- [x] 旧 active snapshot 在出现新 Experience 时继续可用，只显示 lag/pending；不再因为 epoch 落后一格就整张画像失效。
- [x] 明确纠正 / 拒绝会立即让相关 Claim 退出当前画像，并写入高权重 Experience 供下一次 synthesis 重算。
- [x] Dream 使用独立 `dreamModelId`；未单独指定时可跟随 Memory Extraction Model，但底层已有独立入口。
- [x] 保留现有 WorkManager、lease、retry、budget、idle/charging/network gate，不重新造调度框架。
- [x] Batch 2 完成后执行 Dream parser/validator/store/synthesis 定向测试与 `:app:compileDebugKotlin`。

## Batch 3：User / 七七 / Relationship 三画像

- [x] Dream Claim 增加 `subjectKind = USER / ASSISTANT / RELATIONSHIP`。
- [x] Claim 增加 `profileSection` 与 `epistemicOrigin = EXPLICIT / OBSERVED / INFERRED / SELF_REFLECTED`。
- [x] 将“证据性质”和“内容类型”拆开；`epistemicOrigin` 与 `contentType` 分别表达来源性质和画像内容语义。
- [x] Prompt 支持一次 Dream 同时输出 User Model、Assistant Self Model、Relationship Model 的 claim operations。
- [x] 七七的 `Identity Kernel` 继续由用户/assistant 配置定义；Dream Prompt 明确只生成 Learned Self Model，不覆盖 Kernel。
- [x] Pair Snapshot 编译出 `about_user / about_assistant / about_relationship` 三个 compact section。
- [x] Runtime 注入 compact Pair Portrait；具体 Episode 仍按需 recall，不把完整 Dream DB 塞进每轮 prompt。
- [x] 用户明确纠正 / 拒绝会快速退出对应 Claim、写入 `USER_CORRECTION / USER_REJECTION` Experience 并唤醒下一次 synthesis。

## Batch 4：打开七七查看 Dream 的接口

- [x] 新增 `DreamIntrospectionToolProvider`，复用现有 `LocalTools.getTools()` provider 注入模式，不增加新的 `LocalToolOption`、CapabilityCatalog 或权限页。
- [x] 工具只依赖 `ToolInvocationContext.callerAssistantId` 自动解析当前助手自己的 `DreamPairScope`，模型无需也不能手填另一个 assistantId。
- [x] 对七七默认直接暴露一个只读工具：`dream_view`。
- [x] `dream_view` 只保留一个接口，避免拆成多个工具；支持可选 `view`：
  - `summary`：当前三画像 compact portrait、profile revision、最近 Dream 时间；
  - `claims`：当前 User / Assistant / Relationship claims；
  - `recent`：最近一次 Dream 吸收的 Experience 与画像变化；
  - `sources`：指定 claim 的来源 episode / message refs。
- [x] `dream_view` 返回当前 `pendingExperienceCount / experienceDebt / lag`，让七七知道自己是否还有“没做完的梦”。
- [x] 没有 active Dream 时正常返回 `no_active_dream` + 当前待处理状态，而不是工具失败。
- [x] 七七可以主动调用此工具回答“我最近做了什么梦 / 我现在怎么看你 / 我怎么看我自己 / 我们现在是什么关系”。
- [x] 该接口只读，不提供修改、删除、强制生成 Dream 的入口；修改和纠正继续走用户 UI / 既有流程。
- [x] 将 `DreamIntrospectionToolProvider` 注册进 DI，并在 `LocalTools.getTools()` 中像 `PetDiaryToolProvider` 一样自动加入当前助手可用工具列表。

## Batch 5：Dream 页面与收尾

- [x] Dream 页面展示三张卡：斯啾伊 / 七七 / 我们。
- [x] 每张卡复用现有 Claim 详情 / Evidence / Snapshot diff / 纠正交互；Pair Evidence 展示 Experience 摘要，稳定 message/source refs 保留在 Ledger 与 `dream_view sources`，不重复复制聊天原文。
- [x] 页面显示 portrait revision、pending experiences、Dream debt，并继续复用现有最近 Dream / snapshot diff 展示画像变化。
- [x] 将旧 `MemoryScopeState / memoryEpoch / lastAppliedMemoryEpoch` 对 **Pair Dream 的逻辑 ownership** 移除；Pair Dream 时钟、lease、review、runtime 均由 `dream_experience_state` 驱动。
- [x] 清理新路径不再使用的 Memory→Dream 触发/读取接线；未顺手重构无关 Memory V2。
- [x] 更新 Room schema v50、字符串和本实施文档。
- [x] 全部功能完成后集中执行 Dream JVM 测试、Room/KSP 与 `:app:compileDebugKotlin`；当前无需额外 `assembleDebug` 才能验证本轮 Kotlin/Room 接线。
- [x] 未在项目中途跑真机回归；未执行 `adb install -r`。

> **v50 结构说明：** `dream_runs / dream_claims / dream_snapshots` 仍保留对 `memory_scope_state` 的 legacy FK。PairScope 只插入一个不参与任何业务判断的 parent 占位行，以避免为了移除一个结构 FK 而重建整条 Claim 历史表链。它不再提供 Pair Dream 的 epoch、lease、review、触发或 runtime authority；后续若数据库自然发生大版本迁移，再顺手移除即可。

## 完成标准

- [x] Dream 是否需要运行由 Experience Ledger / Dream state 决定，不再由 Memory epoch 决定。
- [x] 新聊天发生后旧 Pair Portrait 继续可用，同时显示 pending/lag。
- [x] Dream 可以直接从聊天 episode 学到斯啾伊、七七和“我们”的长期特征。
- [x] Memory 仍可作为输入，但删除/关闭 Memory 不会让 Dream 架构失去自己的时钟和作用域。
- [x] 七七可以通过 `dream_view` 主动查看自己的当前 Dream、三画像、最近变化和来源。
- [x] 用户可在 UI 看同一份 Dream 状态并纠正错误画像。
- [x] Runtime 使用的是 compact Pair Portrait，而不是完整 Memory/Dream dump。

## 最终验证（2026-08-19）

- Room/KSP：`:app:kspDebugKotlin` → **BUILD SUCCESSFUL**。
- Kotlin：`:app:compileDebugKotlin` → **BUILD SUCCESSFUL**。
- Dream JVM：`--tests "me.rerere.rikkahub.memory.dreaming.*"` → **210/210 通过**。
- `git diff --check`：通过（仅 Windows 工作区 LF/CRLF 提示）。
