# SkillTestRunner 接续收口（2026-09-20）

## 范围与基线

- 产品代际研究基线：up244.0；官方对照：2.5.2。版本名中的 2.3.1 不用于判断研究起点。
- 正式仓库 `H:/rikkahub-agent` 的 HEAD 为 `2c158adbc`，已有用户修改，未在此施工。
- 本批工作树：`H:/gbao_codex/rikkahub-skill-closeout-20260920`。
- 分支：`codex/skill-runner-closeout-20260920`，起点 `6e169cd51`，继承全部八份 C0/C2 补丁。
- 仅提取旧工作树的 SkillTestRunner.kt、SkillTestRunnerTest.kt、未跟踪的 SkillTrackedOutcomeTest.kt，并单独补入 AppModule 的 FilesManager 注入。没有复制旧 AppModule 整文件、构建缓存或其他功能差异。

## 接续核验与完成状态

| 项目 | 源码与证据 | 判定 |
| --- | --- | --- |
| C0/C2 八补丁 | staging HEAD `6e169cd51`，八份 patch 仍在原交付目录；实读 GMD XML 5 tests/0 failures/0 errors，以及 12 个指定 JVM XML 共 84 tests/0 failures/0 errors/0 skipped | 已在独立 staging 验证；未进入正式仓库 |
| 旧 Codex 历史 JVM | 实读 continuation-targeted-xml 的 17 suites/101 tests 与 continuation-regression-xml 的 52 suites/314 tests；均无 failure/error | 历史独立执行证据；两组有重叠，不相加为唯一用例数 |
| SkillTestRunner | 已有 tracked outcome、quiescence 和独立托管图片候选实现，历史 Skill 两类共 13 tests | 本批提取、补缺并重新验证；详见下文 |
| SubAgent | reservePending CAS、Engine lazy job/异常补偿与 11 项 registry 历史测试均存在 | 候选已实现；尚未拆为本轮已验收补丁，不能推断 Engine/ledger 完整集成通过 |
| Dream | DAO keyset query、adapter 接线、新增 DreamMemoryTraversal.kt 与 3 项历史遍历测试存在 | 候选已实现；尚未独立移植验证，辅助类测试不是 Room 回填集成证据 |
| SafetySnapshot、强制 execution registration、Owner envelope、死代码移除 | 旧工作树有真实源文件差异和部分专项/回归 XML | 候选代码，仍需逐批核对依赖并独立验证 |
| C2 Android 联动故障注入 | 已验收文档明确只覆盖 JVM helper/source contract，GMD 5 项是 C0 Room | 未完成；本批不将 Skill JVM 测试充当此门槛 |
| Restore v49→v50、sparse outbox、Settings 清理、Owner 分支所有权、后续窄重构、官方小更新、Full/Slim | 路线和审计定义待办；本批没有核验出可交付补丁 | 未完成；备份 1033 原因未查明，不作归因 |

## 原问题与本批边界

正式基线 Skill runner 使用 generation Job 的初始 null 判断完成，可能在真正执行开始前 harvest/删除临时会话。旧候选已迁移到 Deferred<CommandOutcome>，但仍有清理异常缺口：

1. 先 unmark 再 cleanup；数据库删除失败时失去恢复标记。
2. 生产 cleanup 吞掉 dropSession/deleteConversation 异常，不能提供清理成功证明。
3. startConversation 在 insert 后初始化失败时尚未 mark，留下未登记临时状态。
4. finally 内 cleanup 抛错可替换调用方取消；NonCancellable cleanup 自身没有总时限。
5. 图片托管复制失败时，候选仍删除临时会话，使源结果失去会话引用。

生产修改只涉及 SkillTestRunner.kt 与 AppModule.kt：

- 只对 Completed 收取结果，命令 outcome 未完成时不以初始 idle 判定结束；删除前要求 quiescence。
- 在 startConversation 前登记恢复标记，覆盖插入成功、初始化失败的部分设置路径。
- cleanup 不吞真实 Repository/ChatService 异常，并检查 Deleted/Missing；RetainedSecondUser 仍保留数据，不绕过删除保护。
- 确认删除后再 unmark。最终 NonCancellable 清理总预算默认 5 秒，异常/超时保留标记且不替换已交付结果或调用方取消。
- 图片复制成功后才允许 Done/清理；收取或复制异常时保留临时源会话。部分成功的托管副本保守保留，不新增猜测性文件 GC。
- 生产 filesManager 注入为必填；无新增依赖。file URI 分支和 URL 筛选同样使用大小写不敏感判断。

本批不改变工具授权、安全门、Emergency Stop、第二用户删除保护或 C0 附件保留策略。不删除 GenerationAwait，待调用链迁移整体完成后再作为独立工程减负批次处理。

## 验证记录

构建与 XML 证据保存在本工作树 `build/codex-verification/`，没有调用 ADB、connected 测试或安装 APK。

- 首次尝试：App 编译通过，新增测试使用未引入的 coroutines-test，测试编译失败；保留 `skill-test-fixture-compile-failed.log`。修正为项目已有 runBlocking/Deferred，不新增依赖，不把编译失败当行为 RED。
- RED：旧候选生产实现不变，运行 `:app:testDebugUnitTest --tests '*SkillCleanupFailureTest'`，5 tests / 5 failures / 0 errors / 0 skipped。覆盖删除前丢标记、部分初始化、清理异常替换成功/取消、图片复制失败后清理。证据：`skill-candidate-red.log`、`skill-red-xml/`、`skill-candidate-before-fix.patch`（相对八补丁基线的测试前源码快照）。
- GREEN：`:app:testDebugUnitTest --tests '*SkillTestRunnerTest' --tests '*SkillTrackedOutcomeTest' --tests '*SkillCleanupFailureTest'`，3 suites / 20 tests / 0 failures / 0 errors / 0 skipped，BUILD SUCCESSFUL in 2m 48s；实际重新编译 App Kotlin 与测试。证据：`skill-green.log`、`skill-green-xml/`。5 个 RED 用例保持原断言转绿，另补 pending outcome 不提前 harvest/cleanup 和清理时限测试。
- 回归：23 suites / 186 tests / 0 failures / 0 errors / 0 skipped，BUILD SUCCESSFUL in 2m 5s。包含 skills 包全部测试、原 C0/C2 的 84 项及 ToolExecutionGatePolicyTest 15 项；20 项 Skill 专项是此 186 项的子集，不叠加计数。证据：`skill-regression.log`、`skill-regression-xml/`、`skill-regression-summary.json`。

所有 Gradle 调用使用 `--no-daemon --no-parallel --max-workers=2 --build-cache`，设置 `JAVA_HOME=C:/Program Files/Android/Android Studio/jbr`、`ANDROID_HOME=D:/Android/sdk`。构建串行执行，未复制旧工作树构建缓存。

回归命令（PowerShell，环境与公共参数同上）：

```powershell
$filters = @(
    'me.rerere.rikkahub.skills.*',
    '*ExternalAutomationDispatcherTest', '*ExternalAutomationSetupContractTest',
    '*ExternalAutomationSetupGuardTest', '*ExternalAutomationTerminalReporterTest',
    '*ExternalHeadlessSetupContractTest', '*ExternalTrackedOutcomeTest',
    '*ContextRequestFactoryTest', '*ConversationPostCommitProjectionTest',
    '*ConversationSqlitePersistenceTest', '*TransientConversationFinalizationProductionContractTest',
    '*ConversationProjectionAttachmentContractTest', '*ConversationRuntimeTest',
    '*ToolExecutionGatePolicyTest'
)
$gradleArguments = @(':app:testDebugUnitTest', '--no-daemon', '--no-parallel', '--max-workers=2', '--build-cache')
foreach ($filter in $filters) { $gradleArguments += @('--tests', $filter) }
& .\gradlew.bat @gradleArguments
```

### 尚未覆盖的边界

这些 Skill 测试通过 Driver seam 和独立文件复制注入异常，App 编译证明生产接口兼容，但不实例化真实 Android ChatService、ContentResolver 或 Koin 容器，不构成完整 Android 集成验证。没有新增 durable reaper；headless 标记继续使用现有 SharedPreferences/app 启动清扫机制，不承诺进程死亡后的完整恢复协议。最终清理超时依赖挂起操作响应取消，不能强制中断阻塞 native I/O。

备份预定位只确认 BackupArchiveService 仍使用 `maximum == count`；源码未发现字面 `1033`，缺少实际异常链，不能据此认定 1033 来自该校验。

## 补丁交付

本批独立提交包含两个生产文件、三个测试文件和本记录，必须应用在已验收八补丁的 `6e169cd51` 基线上。导出目录：`H:/G宝的升级路程/RikkaHub官方对照/20260920_Skill_closeout_patches`；证据归档：同级 `_verify/skill-closeout-20260920`。最终 commit、patch SHA-256 与干净重放 tree hash 记录在导出目录的 `delivery.json`。未合入正式 dirty 仓库，不生成/安装手机测试包。

## 后续批次

下批接续 SubAgent 原子预留，重点建立 Engine 在 ledger open 失败、job attach 前取消、lazy body 未进入时的 registry + durable ledger 联动验证，再单独提交。之后接续 Dream 分页真实 DAO 回填、其他安全/减负候选以及 C2 Android 联动门。数据正确性、架构和 Full/Slim 阶段均不能标成已完成。
