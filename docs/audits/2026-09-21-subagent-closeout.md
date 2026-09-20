# SubAgent atomic reservation + job/ledger closeout — 2026-09-21

## Scope / provenance
- Desktop-only isolated worktree: `H:\gbao_codex\rikkahub-subagent-closeout-20260920`, branch `codex/subagent-closeout-20260920`, base `de9a2bbe8` (the separately accepted C0/C2 eight-patch chain plus SkillTestRunner patch).
- Original GPT-6 candidate comes exclusively from three SubAgent files in `H:\gbao_codex\rikkahub-gpt6-medium-20260918`; changes beyond those files in the old dirty worktree were not copied.
- Formal `H:\rikkahub-agent` with the user's ten pre-existing tracked modifications was not edited; no phone/ADB was used.

## Production changes
- Registry: atomic global/per-assistant `reservePending` CAS; cancellation before job attachment releases the reserved slot; stale job attachment cannot revive a terminal run; concurrent reservation test covers both caps.
- Engine: create ledger id before launching a lazy child; check caller cancellation and pending reservation after the ledger open suspension; preserve the original setup exception when cancelling a prelaunch Job.
- Engine / `SubAgentRunCompletion`: a lazy child cancelled before entering its body would otherwise skip `executeRun`'s `finally`. Its completion callback captures the ledger id and separately finalizes both the registry and any existing durable ledger record, without turning an already succeeded or failed run into cancelled. Foreground callers await finalizer completion. The repository itself treats terminal writes idempotently.

## Actual test and compile evidence
- `build/codex-verification/subagent-20260920-focused-current.log`: `:app:testDebugUnitTest --tests 'me.rerere.rikkahub.subagent.*'`, Android app Kotlin and test Kotlin compilation, BUILD SUCCESSFUL. Archived seven JUnit XML suites in `subagent-focused-xml/`: **35 tests, 0 failures, 0 errors, 0 skipped**, including six completion/failure tests.
- `build/codex-verification/subagent-20260921-cross-regression.log`: 30 JUnit XML suites in `subagent-cross-regression-xml/`: **221 tests, 0 failures, 0 errors, 0 skipped**, covering SubAgent, Skills, ExternalAutomation/C0, ConversationRuntime, and ToolExecutionGate. The 35 focused tests are a subset of the 221 regression tests; do not add them as distinct tests.
- `git diff --check` clean. Both commands use project JBR/SDK, offline Gradle, no-daemon, no-parallel, max workers 2.

## Verification limits / not yet complete
- These are JVM tests of production-backed registry/completion helper and full-app compile; no test has instantiated an Android Room-backed AgentRunDAO together with an actual SubAgentEngine -> ChatService generation and cancellation. The async completion callback is wired in production source, but full Android end-to-end crash/cancellation integration remains unverified.
- `AgentRunRepository.open/markTerminal` are **best-effort**: a failed DAO insert yields a fallback id, and failed terminal writes are logged rather than throwing. This patch closes an existing ledger row on prelaunch cancellation; it does not guarantee a durable row exists after real database failure, nor add a persistent retry queue.
- Cancellation finalization launched with `NonCancellable` covers the live process, not force-kill or process death. Boot recovery and real Room fault injection remain a separate gate. A blocking underlying DAO write cannot be forcibly terminated by this change.
- Dream real DAO/pagination, C2 Android receiver/ledger/ChatService integration, backup/restore, and subsequent architecture/safety/APK work remain outstanding.

## Reproduction / handoff
- Base commit `de9a2bbe8`; apply the standalone SubAgent commit exported under `H:\G宝的升级路程\RikkaHub官方对照\20260921_SubAgent_closeout_patches` only after the C0/C2 and Skill patch chain. Verify clean git replay and tree hash before transplanting into the formal dirty repository.
- This batch must not be taken as an authorization to reset, clean, or overwrite the formal repository.
