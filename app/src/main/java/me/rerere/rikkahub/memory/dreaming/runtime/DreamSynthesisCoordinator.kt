package me.rerere.rikkahub.memory.dreaming.runtime

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import me.rerere.rikkahub.memory.dreaming.model.DreamRunMode
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamingFeatureFlags
import me.rerere.rikkahub.memory.dreaming.orchestration.DreamEpochClock
import me.rerere.rikkahub.memory.dreaming.work.DreamSynthesisScanReason
import me.rerere.rikkahub.memory.dreaming.work.DreamSynthesisWorkScheduler
import kotlin.uuid.Uuid

const val DEFAULT_DREAM_SYNTHESIS_SCAN_LIMIT = 64
const val MAX_DREAM_SYNTHESIS_SCAN_LIMIT = 512

data class DreamSynthesisDirtyScope(
    val scopeId: DreamScopeId,
    val memoryEpoch: Long,
    val observerCheckpointEpoch: Long,
    val lastAppliedMemoryEpoch: Long,
    val dreamStateRevision: Long,
    val activeRunId: String?,
    val activeRunLeaseUntilMs: Long?,
    val updatedAtMs: Long,
) {
    init {
        require(memoryEpoch > lastAppliedMemoryEpoch)
        require(observerCheckpointEpoch == memoryEpoch)
        require(lastAppliedMemoryEpoch >= 0L && dreamStateRevision >= 0L && updatedAtMs >= 0L)
        require((activeRunId == null) == (activeRunLeaseUntilMs == null))
        require(activeRunLeaseUntilMs == null || activeRunLeaseUntilMs >= 0L)
    }
}

data class EnsurePendingSynthesisRunRequest(
    val scopeId: DreamScopeId,
    val runId: String,
    val mode: DreamRunMode,
    val createdAtMs: Long,
) {
    init {
        require(mode == DreamRunMode.INCREMENTAL || mode == DreamRunMode.FULL)
        me.rerere.rikkahub.memory.dreaming.model.requireCanonicalDreamRunId(runId)
        require(createdAtMs >= 0L)
    }
}

sealed interface EnsurePendingSynthesisRunResult {
    data class Ready(
        val runId: String,
        val mode: DreamRunMode,
        val created: Boolean,
        val running: Boolean,
    ) : EnsurePendingSynthesisRunResult

    data object ScopeNotDirty : EnsurePendingSynthesisRunResult
    data object ObserverNotCaughtUp : EnsurePendingSynthesisRunResult
    data object CreationDeferred : EnsurePendingSynthesisRunResult
    data object RunIdentityConflict : EnsurePendingSynthesisRunResult
    data object CorruptState : EnsurePendingSynthesisRunResult
}

interface DreamSynthesisSchedulingStore : DreamDailyUsageStore {
    suspend fun findDirtyScopes(limit: Int): List<DreamSynthesisDirtyScope>

    suspend fun readDirtyScope(scopeId: DreamScopeId): DreamSynthesisDirtyScope?

    /** One short transaction: reuse one live synthesis run or insert exactly one PENDING row. */
    suspend fun ensurePendingRun(
        request: EnsurePendingSynthesisRunRequest,
        allowCreate: Boolean = true,
    ): EnsurePendingSynthesisRunResult

    /** Pair-authoritative lifecycle reservations across every registered Pair scope. */
    suspend fun countGlobalPendingRuns(): Int

    /** Pair-authoritative active executions across every registered Pair scope. */
    suspend fun countGlobalRunningRuns(): Int

    /** Terminalizes queued/running synthesis for this scope; Observer work is never touched. */
    suspend fun cancelScopeRuns(scopeId: DreamScopeId, nowMs: Long): Int
}

enum class DreamSynthesisScheduleDeferral {
    FEATURE_DISABLED,
    POLICY_UNAVAILABLE,
    POLICY_BLOCKED,
    DAILY_BUDGET_EXHAUSTED,
    USAGE_UNMEASURED,
    OBSERVER_NOT_CAUGHT_UP,
    RUN_CONFLICT,
    STORE_FAILURE,
}

data class DreamSynthesisScanResult(
    val scheduledScopes: List<DreamScopeId>,
    val cancelledScopes: List<DreamScopeId>,
    val deferredScopes: Map<DreamScopeId, DreamSynthesisScheduleDeferral>,
    val saturated: Boolean,
)

/**
 * Turns durable synthesis dirtiness into identity-only WorkManager requests. Commit callbacks are
 * merely hints; startup and periodic scans reconstruct the same pending run after process death.
 */
class DreamSynthesisCoordinator(
    private val store: DreamSynthesisSchedulingStore,
    private val featureFlags: DreamingFeatureFlagSource,
    private val policySource: DreamingCostPolicySource,
    private val scheduler: DreamSynthesisWorkScheduler,
    private val clock: DreamEpochClock,
    private val runIdGenerator: () -> String = { Uuid.random().toString() },
) {
    /**
     * Serializes every path that can create or cancel synthesis Work. Settings persistence happens
     * before [onSettingsChanged]; once that callback completes, an older startup/cost/commit/scan
     * callback therefore cannot resume and re-arm Work from a stale feature decision.
     */
    private val schedulingMutex = Mutex()

    /** Called once by the app runtime after all M5 dependencies are bound. */
    suspend fun armStartupAndPeriodicRecovery() = schedulingMutex.withLock {
        armStartupAndPeriodicRecoveryLocked()
    }

    private suspend fun armStartupAndPeriodicRecoveryLocked() {
        if (!readAnyGenerationEnabled()) {
            disarmRecoveryScansBestEffort()
            return
        }
        try {
            scheduler.armRecoveryScans()
        } catch (_: Exception) {
            // Startup is a latency hint; durable dirtiness remains available to a later signal.
        }
    }

    /** Global policy changes use a scan hint; the Worker still re-checks the live policy. */
    suspend fun onCostPolicyChanged(
        previous: DreamingCostPolicy,
        current: DreamingCostPolicy,
    ) = schedulingMutex.withLock {
        onCostPolicyChangedLocked(previous, current)
    }

    private suspend fun onCostPolicyChangedLocked(
        previous: DreamingCostPolicy,
        current: DreamingCostPolicy,
    ) {
        requireNotNull(previous.validatedOrNull())
        requireNotNull(current.validatedOrNull())
        if (previous == current) return
        if (!readAnyGenerationEnabled()) {
            disarmRecoveryScansBestEffort()
            return
        }
        try {
            scheduler.enqueueDirtyScan(DreamSynthesisScanReason.COST_POLICY_CHANGED)
        } catch (_: Exception) {
            // The persisted setting is authoritative; a later startup/periodic scan replays it.
        }
    }

    suspend fun scanDirtyScopes(
        reason: DreamSynthesisScanReason,
        limit: Int = DEFAULT_DREAM_SYNTHESIS_SCAN_LIMIT,
    ): DreamSynthesisScanResult = schedulingMutex.withLock {
        scanDirtyScopesLocked(reason, limit)
    }

    private suspend fun scanDirtyScopesLocked(
        reason: DreamSynthesisScanReason,
        limit: Int,
    ): DreamSynthesisScanResult {
        require(limit in 1..MAX_DREAM_SYNTHESIS_SCAN_LIMIT)
        val now = clock.nowEpochMs()
        if (now < 0L) {
            return DreamSynthesisScanResult(
                scheduledScopes = emptyList(),
                cancelledScopes = emptyList(),
                deferredScopes = emptyMap(),
                saturated = false,
            )
        }
        val policy = readPolicyOrNull() ?: return allDeferred(
            scopes = readDirtyScopesOrEmpty(limit),
            reason = DreamSynthesisScheduleDeferral.POLICY_UNAVAILABLE,
            saturated = false,
        )
        val dirty = try {
            store.findDirtyScopes(limit)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return DreamSynthesisScanResult(emptyList(), emptyList(), emptyMap(), false)
        }
        val coarseBudget = readCoarseBudget(policy, now)
        var remainingNewRuns = coarseBudget.remainingNewRuns
        val scheduled = mutableListOf<DreamScopeId>()
        val cancelled = mutableListOf<DreamScopeId>()
        val deferred = linkedMapOf<DreamScopeId, DreamSynthesisScheduleDeferral>()
        var newlyCreatedRuns = 0
        dirty.forEach { scope ->
            val flags = readFlagsOrNull(scope.scopeId)
            if (flags == null) {
                deferred[scope.scopeId] = DreamSynthesisScheduleDeferral.POLICY_UNAVAILABLE
            } else if (!flags.allowsSynthesisGeneration()) {
                cancelRunsBestEffort(scope.scopeId, now)
                cancelScheduledWorkBestEffort(scope.scopeId)
                cancelled += scope.scopeId
                deferred[scope.scopeId] = DreamSynthesisScheduleDeferral.FEATURE_DISABLED
            } else if (coarseBudget.deferral != null) {
                deferred[scope.scopeId] = coarseBudget.deferral
            } else {
                when (val one = schedule(
                    scope = scope,
                    policy = policy,
                    nowMs = now,
                    allowCreate = remainingNewRuns > 0L,
                    // Startup replaces a queued PENDING request so stale WorkManager backoff and
                    // pre-upgrade attempt counters cannot delay durable recovery for hours. An
                    // exact idle recheck must also replace the just-finishing Worker: otherwise
                    // KEEP can discard its successor while the deferring Worker is still RUNNING,
                    // then leave the durable PENDING row with no Work request. A RUNNING provider
                    // call is still protected by schedule()'s running fence. Cost changes likewise
                    // rebuild immutable Work constraints.
                    replaceExisting = reason == DreamSynthesisScanReason.STARTUP ||
                        reason == DreamSynthesisScanReason.COST_POLICY_CHANGED ||
                        reason == DreamSynthesisScanReason.APP_IDLE_RECHECK,
                    idleDeadlineAlreadyObserved =
                        reason == DreamSynthesisScanReason.APP_IDLE_RECHECK,
                )) {
                    is ScheduleOneResult.Scheduled -> {
                        scheduled += scope.scopeId
                        if (one.created) {
                            newlyCreatedRuns += 1
                            remainingNewRuns -= 1L
                        }
                    }

                    is ScheduleOneResult.Deferred -> deferred[scope.scopeId] = one.reason
                }
            }
        }
        if (deferred.values.any {
                it == DreamSynthesisScheduleDeferral.DAILY_BUDGET_EXHAUSTED ||
                    it == DreamSynthesisScheduleDeferral.USAGE_UNMEASURED
            }
        ) {
            dreamUtcDayWindowOrNull(now)?.let { window ->
                scheduler.enqueueDirtyScan(
                    reason = DreamSynthesisScanReason.UTC_BUDGET_ROLLOVER,
                    earliestAtEpochMs = window.endExclusiveEpochMs,
                )
            }
        }
        return DreamSynthesisScanResult(
            scheduledScopes = scheduled,
            cancelledScopes = cancelled,
            deferredScopes = deferred,
            // A follow-up is useful only if this page made durable forward progress. Once every
            // returned scope already owns a pending/running run, chaining again would spin on the
            // same durable dirtiness until those Workers commit.
            saturated = dirty.size == limit && newlyCreatedRuns > 0,
        )
    }

    suspend fun onScopeHint(scopeId: DreamScopeId): DreamSynthesisScanResult =
        schedulingMutex.withLock {
            onScopeHintLocked(scopeId)
        }

    private suspend fun onScopeHintLocked(scopeId: DreamScopeId): DreamSynthesisScanResult {
        val scope = try {
            store.readDirtyScope(scopeId)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            null
        } ?: return DreamSynthesisScanResult(emptyList(), emptyList(), emptyMap(), false)
        val flags = readFlagsOrNull(scopeId)
        if (flags == null || !flags.allowsSynthesisGeneration()) {
            if (flags != null) {
                val now = clock.nowEpochMs()
                if (now >= 0L) cancelRunsBestEffort(scopeId, now)
                cancelScheduledWorkBestEffort(scopeId)
            }
            return DreamSynthesisScanResult(
                emptyList(),
                listOf(scopeId).takeIf { flags != null }.orEmpty(),
                mapOf(scopeId to if (flags == null) {
                    DreamSynthesisScheduleDeferral.POLICY_UNAVAILABLE
                } else {
                    DreamSynthesisScheduleDeferral.FEATURE_DISABLED
                }),
                false,
            )
        }
        val policy = readPolicyOrNull()
            ?: return deferred(scopeId, DreamSynthesisScheduleDeferral.POLICY_UNAVAILABLE)
        val now = clock.nowEpochMs()
        if (now < 0L) return deferred(scopeId, DreamSynthesisScheduleDeferral.STORE_FAILURE)
        val coarse = readCoarseBudget(policy, now)
        if (coarse.deferral != null) {
            val reason = coarse.deferral
            if (reason == DreamSynthesisScheduleDeferral.DAILY_BUDGET_EXHAUSTED ||
                reason == DreamSynthesisScheduleDeferral.USAGE_UNMEASURED
            ) {
                dreamUtcDayWindowOrNull(now)?.let { window ->
                    scheduler.enqueueDirtyScan(
                        DreamSynthesisScanReason.UTC_BUDGET_ROLLOVER,
                        window.endExclusiveEpochMs,
                    )
                }
            }
            return deferred(scopeId, reason)
        }
        return when (val one = schedule(
            scope,
            policy,
            now,
            allowCreate = coarse.remainingNewRuns > 0L,
            replaceExisting = false,
            idleDeadlineAlreadyObserved = false,
        )) {
            is ScheduleOneResult.Scheduled -> DreamSynthesisScanResult(
                listOf(scopeId), emptyList(), emptyMap(), false,
            )

            is ScheduleOneResult.Deferred -> {
                if (one.reason == DreamSynthesisScheduleDeferral.DAILY_BUDGET_EXHAUSTED) {
                    dreamUtcDayWindowOrNull(now)?.let { window ->
                        scheduler.enqueueDirtyScan(
                            DreamSynthesisScanReason.UTC_BUDGET_ROLLOVER,
                            window.endExclusiveEpochMs,
                        )
                    }
                }
                deferred(scopeId, one.reason)
            }
        }
    }

    suspend fun onSettingsChanged(
        scopeId: DreamScopeId,
        previous: DreamingScopePreferences,
        current: DreamingScopePreferences,
    ) = schedulingMutex.withLock {
        requireNotNull(previous.validatedOrNull())
        requireNotNull(current.validatedOrNull())
        // The callback can arrive out of order because each UI write runs in its own coroutine.
        // Re-read the persisted state under one coordinator mutex; transition arguments are only
        // audit inputs and never authorize scheduling.
        val liveFlags = readFlagsOrNull(scopeId)
        val anyGenerationEnabled = readAnyGenerationEnabled()
        if (liveFlags == null || !liveFlags.allowsSynthesisGeneration() || !anyGenerationEnabled) {
            val now = clock.nowEpochMs()
            if (now >= 0L) cancelRunsBestEffort(scopeId, now)
            cancelScheduledWorkBestEffort(scopeId)
            if (!anyGenerationEnabled) disarmRecoveryScansBestEffort()
        } else {
            // Includes use-only changes: use=false never cancels generation or shadow work.
            armRecoveryScansBestEffort()
            scheduler.enqueueDirtyScan(DreamSynthesisScanReason.SETTINGS_CHANGED)
        }
    }

    /** Post-commit hint. It must remain a no-op while every scope has generation disabled. */
    suspend fun onAuthorityCommitted() = schedulingMutex.withLock {
        onAuthorityCommittedLocked()
    }

    private suspend fun onAuthorityCommittedLocked() {
        if (!readAnyGenerationEnabled()) return
        armRecoveryScansBestEffort()
        try {
            scheduler.enqueueDirtyScan(DreamSynthesisScanReason.AUTHORITY_COMMIT)
        } catch (_: Exception) {
            // Durable dirty epochs plus startup/periodic recovery preserve correctness.
        }
    }

    private suspend fun schedule(
        scope: DreamSynthesisDirtyScope,
        policy: DreamingCostPolicy,
        nowMs: Long,
        allowCreate: Boolean,
        replaceExisting: Boolean,
        idleDeadlineAlreadyObserved: Boolean,
    ): ScheduleOneResult {
        val mode = if (scope.lastAppliedMemoryEpoch == 0L) DreamRunMode.FULL
        else DreamRunMode.INCREMENTAL
        val ensured = try {
            store.ensurePendingRun(
                EnsurePendingSynthesisRunRequest(
                    scopeId = scope.scopeId,
                    runId = runIdGenerator(),
                    mode = mode,
                    createdAtMs = maxOf(nowMs, scope.updatedAtMs),
                ),
                allowCreate = allowCreate,
            )
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return ScheduleOneResult.Deferred(DreamSynthesisScheduleDeferral.STORE_FAILURE)
        }
        return when (ensured) {
            is EnsurePendingSynthesisRunResult.Ready -> {
                val effectiveReplace = replaceExisting && !ensured.running
                if (idleDeadlineAlreadyObserved) {
                    scheduler.enqueueScopeAfterIdleRecheck(
                        scopeId = scope.scopeId,
                        runId = ensured.runId,
                        policy = policy,
                        replaceExisting = effectiveReplace,
                    )
                } else {
                    scheduler.enqueueScope(
                        scopeId = scope.scopeId,
                        runId = ensured.runId,
                        policy = policy,
                        // Rebuild immutable Work constraints only while queued. Replacing a
                        // RUNNING provider call would reset Work attempt identity and make unknown
                        // spend look like a first attempt.
                        replaceExisting = effectiveReplace,
                    )
                }
                ScheduleOneResult.Scheduled(created = ensured.created)
            }

            EnsurePendingSynthesisRunResult.ObserverNotCaughtUp ->
                ScheduleOneResult.Deferred(DreamSynthesisScheduleDeferral.OBSERVER_NOT_CAUGHT_UP)
            EnsurePendingSynthesisRunResult.CreationDeferred ->
                ScheduleOneResult.Deferred(DreamSynthesisScheduleDeferral.DAILY_BUDGET_EXHAUSTED)
            EnsurePendingSynthesisRunResult.ScopeNotDirty ->
                ScheduleOneResult.Deferred(DreamSynthesisScheduleDeferral.STORE_FAILURE)
            EnsurePendingSynthesisRunResult.RunIdentityConflict,
            EnsurePendingSynthesisRunResult.CorruptState,
            -> ScheduleOneResult.Deferred(DreamSynthesisScheduleDeferral.RUN_CONFLICT)
        }
    }

    private suspend fun readCoarseBudget(
        policy: DreamingCostPolicy,
        nowMs: Long,
    ): CoarseBudget {
        if (policy.dailyRunLimit == 0 || policy.dailyInputTokenLimit == 0L ||
            policy.dailyOutputTokenLimit == 0L
        ) {
            return CoarseBudget(0, DreamSynthesisScheduleDeferral.POLICY_BLOCKED)
        }
        val window = dreamUtcDayWindowOrNull(nowMs)
            ?: return CoarseBudget(0, DreamSynthesisScheduleDeferral.POLICY_UNAVAILABLE)
        val usage = try {
            store.readGlobalUtcUsage(DreamDailyUsageQuery(window, excludingRunId = null))
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return CoarseBudget(0, DreamSynthesisScheduleDeferral.POLICY_UNAVAILABLE)
        }
        if (!usage.isValid()) {
            return CoarseBudget(0, DreamSynthesisScheduleDeferral.POLICY_UNAVAILABLE)
        }
        val pendingRuns = try {
            store.countGlobalPendingRuns()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return CoarseBudget(0, DreamSynthesisScheduleDeferral.POLICY_UNAVAILABLE)
        }
        if (pendingRuns < 0) {
            return CoarseBudget(0, DreamSynthesisScheduleDeferral.POLICY_UNAVAILABLE)
        }
        val runningRuns = try {
            store.countGlobalRunningRuns()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return CoarseBudget(0, DreamSynthesisScheduleDeferral.POLICY_UNAVAILABLE)
        }
        if (runningRuns < 0) {
            return CoarseBudget(0, DreamSynthesisScheduleDeferral.POLICY_UNAVAILABLE)
        }
        if ((policy.dailyInputTokenLimit != null && usage.unmeasuredInputRunCount > 0) ||
            (policy.dailyOutputTokenLimit != null && usage.unmeasuredOutputRunCount > 0)
        ) {
            return CoarseBudget(0, DreamSynthesisScheduleDeferral.USAGE_UNMEASURED)
        }
        if ((policy.dailyInputTokenLimit?.let { usage.knownInputTokens >= it } == true) ||
            (policy.dailyOutputTokenLimit?.let { usage.knownOutputTokens >= it } == true)
        ) {
            return CoarseBudget(0, DreamSynthesisScheduleDeferral.DAILY_BUDGET_EXHAUSTED)
        }
        if (usage.startedRunCount >= policy.dailyRunLimit) {
            return CoarseBudget(0, DreamSynthesisScheduleDeferral.DAILY_BUDGET_EXHAUSTED)
        }
        val occupied = try {
            Math.addExact(usage.startedRunCount.toLong(), pendingRuns.toLong())
        } catch (_: ArithmeticException) {
            return CoarseBudget(0, DreamSynthesisScheduleDeferral.POLICY_UNAVAILABLE)
        }
        val dailyRemaining = (policy.dailyRunLimit.toLong() - occupied).coerceAtLeast(0L)
        return CoarseBudget(
            // Synthesis is globally serialized before model admission. Otherwise multiple runs
            // can all become RUNNING with NULL token audits before the process-wide budget permit,
            // causing every peer to fail closed against the others' unknown usage.
            remainingNewRuns = if (pendingRuns == 0 && runningRuns == 0) {
                minOf(dailyRemaining, 1L)
            } else {
                0L
            },
            deferral = null,
        )
    }

    private suspend fun readPolicyOrNull(): DreamingCostPolicy? = try {
        policySource.costPolicy().validatedOrNull()
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (_: Exception) {
        null
    }

    private suspend fun readFlagsOrNull(scopeId: DreamScopeId): DreamingFeatureFlags? = try {
        featureFlags.flagsFor(scopeId)
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (_: Exception) {
        null
    }

    private suspend fun readAnyGenerationEnabled(): Boolean = try {
        featureFlags.anySynthesisGenerationEnabled()
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (_: Exception) {
        false
    }

    private suspend fun readDirtyScopesOrEmpty(limit: Int): List<DreamSynthesisDirtyScope> = try {
        store.findDirtyScopes(limit)
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (_: Exception) {
        emptyList()
    }

    private suspend fun cancelRunsBestEffort(scopeId: DreamScopeId, nowMs: Long) {
        try {
            store.cancelScopeRuns(scopeId, nowMs)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            // The scope stays dirty; recovery scans can retry cancellation without data loss.
        }
    }

    private fun cancelScheduledWorkBestEffort(scopeId: DreamScopeId) {
        try {
            scheduler.cancelScope(scopeId)
        } catch (_: Exception) {
            // A disabled Worker re-reads flags and cannot reach the provider. Startup scanning
            // retries the exact unique-work cancellation without weakening the durable DB cancel.
        }
    }

    private fun armRecoveryScansBestEffort() {
        try {
            scheduler.armRecoveryScans()
        } catch (_: Exception) {
            // A scope-specific hint can still enqueue work; durable dirtiness remains authority.
        }
    }

    private fun disarmRecoveryScansBestEffort() {
        try {
            scheduler.disarmRecoveryScans()
        } catch (_: Exception) {
            // Workers also re-read flags before any model or network access.
        }
    }

    private fun allDeferred(
        scopes: List<DreamSynthesisDirtyScope>,
        reason: DreamSynthesisScheduleDeferral,
        saturated: Boolean,
    ) = DreamSynthesisScanResult(
        emptyList(),
        emptyList(),
        scopes.associate { it.scopeId to reason },
        saturated,
    )

    private fun deferred(scopeId: DreamScopeId, reason: DreamSynthesisScheduleDeferral) =
        DreamSynthesisScanResult(emptyList(), emptyList(), mapOf(scopeId to reason), false)

    private data class CoarseBudget(
        val remainingNewRuns: Long,
        val deferral: DreamSynthesisScheduleDeferral?,
    )

    private sealed interface ScheduleOneResult {
        data class Scheduled(val created: Boolean) : ScheduleOneResult
        data class Deferred(val reason: DreamSynthesisScheduleDeferral) : ScheduleOneResult
    }
}
