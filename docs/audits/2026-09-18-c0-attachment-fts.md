# C0 附件与 FTS 子段 — 2026-09-18

施工单尚未全部完成。本文只记录实际实施的附件/FTS 子段，不代表 C0 stale-write 已收口。

## 范围与边界

- 基线：`2c158adbc646adbdadb2436fa5d7408ac5d54fff`。
- 仅修改独立 worktree `H:\gbao_codex\rikkahub-gpt6-medium-20260918`。
- 未操作正式仓库 `H:\rikkahub-agent`，未执行 instrumentation、connected test 或 ADB。
- 根目录实际没有 `AGENTS.md`；遵守用户消息中提供的规则。
- 未修改 Owner/privileged/move/import/restore 的 full-object 写入语义。

## 已实施

1. `ChatService.updateConversation` 和 `mergeConversationState` 不再触发物理附件删除，删除仅供这两个入口使用的 `checkFilesDelete`。
   - session projection 没有 durable commit 或全局 ownership proof，不能授权不可逆的文件删除。
   - 基线 projection 区段有两个 helper 调用、一个 helper 定义和一个 `deleteChatFiles` 调用；修改后均不存在。
2. `ConversationRepository.deleteConversation` 继续执行原有 deletion policy、source invalidation、authority tombstone 和 Room transaction，但不再按该 conversation 的 URL 列表直接物理删除文件。
   - `OwnerSettingsOperationHandler.conversationBranch` 仅重建 node/message IDs，保留原 message parts，因此两个 conversation 可以共享文件 URL。
   - `FilesManager.deleteChatFiles` 直接 `File.delete()`，没有全局引用证明。
   - 第一版保留 orphan；未声称已经实现安全 GC。
3. 普通 insert/update 的 FTS index 和 delete 的 FTS cleanup 经 post-commit projection wrapper 执行。
   - projection 及诊断异常不再向调用者报告 authoritative write 失败，防止调用者进入恢复旧 graph 的错误处理。
   - 保留日志诊断；没有新增自动 FTS repair queue，失败时 search projection 可能暂时过期。
   - 没有改变 DB transaction 内的 authority 顺序与权限判断。

## 新增验证

- `ConversationProjectionAttachmentContractTest`：两个 session projection 入口及 conversation deletion 的无 ownership proof 删除禁令。
- `ConversationPostCommitProjectionTest`：FTS failure、projection cancellation、diagnostic failure 不进入调用者的 failed-write recovery。
- 原 `ConversationSessionTest`、source invalidation、transient finalization、command authority transaction tests 作为受影响范围回归。

这些测试包含 source contract 与纯 JVM 行为测试；不能替代完整的真实 Room close/reopen、rollback、shared-file 集成测试。

## 审计后保留的候选

- `GenerationAwait`：**当前有生产调用**，`ExternalAutomationDispatcher.kt:27,182`。该文件含 NUL，普通 `rg` 目录扫描会将其当作二进制而漏掉结果。用 `rg -a -n awaitGenerationTerminal app/src/main` 复核；必须等 tracked migration 后才考虑删除。
- `BrowserProfileClass`：`BrowserProfileManager.apply` 实际决定 WebView profile；`HeadlessBrowserSession` 遇到 `ISOLATION_UNAVAILABLE` 会拒绝继续，是真实隔离边界。
- `EmergencyStopCoordinator`：持久化 gate 后主动停止 Chat/managed execution 等多个 participant；不是仅有 bool 的展示层。
- 其它 `deleteChatFiles` 调用未一刀删除：`AssistantRemovalService` 清理的是 avatar/background；draft chips 经 `ChatInputState.shouldDeleteFileOnRemove` 保护编辑前已有的 attachment URL。它们有不同生命周期，本次未改变，亦未声称已证明所有入口的全局 ownership。
- `BrowserSessionCoordinator` 与 `McpToolExecutionHandle` 的文本模式搜索仍仅发现生产定义，没有消费者，但本次未实施删除。MCP 底层 SDK protocol cancellation/confirmation 尚未完成审计，不能声称已有可靠远端终止确认。

## 验证环境与结果

```powershell
$env:JAVA_HOME = 'C:\Program Files\Android\Android Studio\jbr'
$env:ANDROID_HOME = 'D:\Android\sdk'
$env:TEMP = (Resolve-Path .build-tmp).Path
$env:TMP = $env:TEMP
```

继承 PATH，保留 bun。SDK 路径来自用户提供的 test-only 验证副本的 `local.properties`，没有读取正式 dirty repo 的配置。

- 首次配置失败：默认 SDK 缺 NDK；更正 SDK 后排除。
- 第二次失败：pnpm 对工具临时目录 `realpath` 报 EPERM；仅设置当前进程 TEMP/TMP 到 worktree 后排除。
- 第一段：`:app:testDebugUnitTest --tests me.rerere.rikkahub.service.ConversationSessionTest --tests me.rerere.rikkahub.service.ConversationProjectionAttachmentContractTest --no-parallel --offline --console=plain`。
  - `BUILD SUCCESSFUL in 19m 13s`，XML：session 3/3、projection 1/1，无 failure/error/skipped。
- 第二段及回归：`BUILD SUCCESSFUL in 7m 59s`，34 tests / 0 failures / 0 errors / 0 skipped。

```powershell
.\gradlew.bat :app:testDebugUnitTest `
  --tests 'me.rerere.rikkahub.service.ConversationProjectionAttachmentContractTest' `
  --tests 'me.rerere.rikkahub.data.repository.ConversationPostCommitProjectionTest' `
  --tests 'me.rerere.rikkahub.service.ConversationSessionTest' `
  --tests 'me.rerere.rikkahub.data.repository.ConversationSourceInvalidationPlanTest' `
  --tests 'me.rerere.rikkahub.data.repository.TransientConversationFinalizationProductionContractTest' `
  --tests 'me.rerere.rikkahub.data.authority.transaction.ConversationCommandAuthorityTransactionsTest' `
  --no-parallel --max-workers=2 --offline --console=plain
```

| Suite | Tests | Failures / Errors / Skipped |
|---|---:|---|
| ConversationProjectionAttachmentContractTest | 2 | 0 / 0 / 0 |
| ConversationPostCommitProjectionTest | 2 | 0 / 0 / 0 |
| ConversationSessionTest | 3 | 0 / 0 / 0 |
| ConversationSourceInvalidationPlanTest | 9 | 0 / 0 / 0 |
| TransientConversationFinalizationProductionContractTest | 2 | 0 / 0 / 0 |
| ConversationCommandAuthorityTransactionsTest | 16 | 0 / 0 / 0 |

`git diff --check` 通过。完整日志、分段 XML 和包含新增文件的 patch 保存在 `build/codex-verification/`。`.build-tmp/` 与 `.pnpm-store/` 是本次构建产生的缓存，不包含在移植 patch 中。

## 尚未完成

- C0：typed UI intents、基于 current identity 的 branch selection、runtime metadata-preserving transaction、title FTS refresh、真实 Room reopen/rollback/共享附件集成 gates。
- 新会话 draft 与已删除 durable conversation 的区分需要完整处理；不能根据 DB missing 无条件重建。
- C2：ExternalAutomation tracked outcome、EXTERNAL_AUTOMATION origin、stop/fence/grace。
- C3：SkillTestRunner tracked outcome、quiescence cleanup、harvested file ownership。
- SubAgent atomic reservation、Dream keyset traversal、Restore P0 尚未实施。
- 工程减负与 SafetySnapshot/non-null registration/Owner builder 尚未实施。

未将上述未实施项目虚报为通过或外部 blocked，也未删除任何业务功能或安全层。

## 移植建议

先把本子段在干净 staging worktree 中应用和验证，再逐 hunk 移入正式 dirty repo；不要复制整份 ChatService/ConversationRepository 覆盖用户改动。可独立拆为：

1. session projection 移除 pre-commit 文件删除及相应 gate；
2. post-commit FTS 失败隔离、缺少 ownership proof 时保留文件及相关 tests。

不要把这两段标为完整 C0 stale-write 修复；其余施工项需要独立后续 patch。新文件未自动暂存，移植时应一并包含。
