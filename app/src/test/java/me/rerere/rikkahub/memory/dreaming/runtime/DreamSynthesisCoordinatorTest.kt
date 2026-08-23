package me.rerere.rikkahub.memory.dreaming.runtime

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import me.rerere.rikkahub.memory.dreaming.model.DreamRunMode
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamingFeatureFlags
import me.rerere.rikkahub.memory.dreaming.orchestration.DreamEpochClock
import me.rerere.rikkahub.memory.dreaming.work.DreamSynthesisScanReason
import me.rerere.rikkahub.memory.dreaming.work.DreamSynthesisWorkScheduler
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class DreamSynthesisCoordinatorTest {
    @Test
    fun `all-off startup and commit create no synthesis work`() = runBlocking {
        val store = FakeSchedulingStore(mutableListOf(dirty(PRIVATE_SCOPE)))
        val scheduler = RecordingScheduler()
        val coordinator = coordinator(
            store = store,
            scheduler = scheduler,
            flags = mapOf(PRIVATE_SCOPE to DreamingFeatureFlags.M1AllOff),
        )

        coordinator.armStartupAndPeriodicRecovery()
        coordinator.onAuthorityCommitted()

        assertEquals(0, scheduler.armedRecoveryCount)
        assertEquals(1, scheduler.disarmedRecoveryCount)
        assertTrue(scheduler.scans.isEmpty())
        assertTrue(scheduler.enqueuedScopes.isEmpty())
    }

    @Test
    fun `enabled startup and commit arm recovery and enqueue a hint`() = runBlocking {
        val scheduler = RecordingScheduler()
        val coordinator = coordinator(
            FakeSchedulingStore(mutableListOf(dirty(PRIVATE_SCOPE))),
            scheduler,
        )

        coordinator.armStartupAndPeriodicRecovery()
        coordinator.onAuthorityCommitted()

        assertEquals(2, scheduler.armedRecoveryCount)
        assertEquals(0, scheduler.disarmedRecoveryCount)
        assertEquals(listOf(DreamSynthesisScanReason.AUTHORITY_COMMIT), scheduler.scans.map { it.first })
    }

    @Test
    fun `stale on callback re-reads final all-off state and cannot rearm work`() = runBlocking {
        val scheduler = RecordingScheduler()
        val liveFlags = mutableMapOf(PRIVATE_SCOPE to enabledFlags())
        val coordinator = coordinator(
            FakeSchedulingStore(mutableListOf(dirty(PRIVATE_SCOPE))),
            scheduler,
            liveFlags,
        )
        liveFlags[PRIVATE_SCOPE] = DreamingFeatureFlags.M1AllOff

        coordinator.onSettingsChanged(
            scopeId = PRIVATE_SCOPE,
            previous = DreamingScopePreferences(),
            current = DreamingScopePreferences(generate = true),
        )

        assertEquals(listOf(PRIVATE_SCOPE), scheduler.cancelledScopes)
        assertEquals(1, scheduler.disarmedRecoveryCount)
        assertEquals(0, scheduler.armedRecoveryCount)
        assertTrue(scheduler.scans.isEmpty())
    }

    @Test
    fun `completed all-off transition wins against older scheduling callbacks`() = runBlocking {
        val callbacks = listOf<Pair<String, suspend (DreamSynthesisCoordinator) -> Unit>>(
            "startup" to { it.armStartupAndPeriodicRecovery() },
            "cost" to {
                it.onCostPolicyChanged(
                    previous = DreamingCostPolicy(),
                    current = DreamingCostPolicy(requireCharging = false),
                )
            },
            "authority" to { it.onAuthorityCommitted() },
        )

        callbacks.forEach { (label, callback) ->
            assertAllOffWinsAgainstStaleCallback(label, callback)
        }
    }

    @Test
    fun `pending row survives enqueue loss and next scan reuses the same identity`() = runBlocking {
        val store = FakeSchedulingStore(mutableListOf(dirty(PRIVATE_SCOPE, lastApplied = 0L)))
        val failedScheduler = RecordingScheduler(throwOnScopeEnqueue = true)
        val first = coordinator(store, failedScheduler)

        try {
            first.scanDirtyScopes(DreamSynthesisScanReason.AUTHORITY_COMMIT)
        } catch (_: ExpectedEnqueueFailure) {
            // Simulates process death after the committed PENDING row and before Work enqueue.
        }
        assertEquals(1, store.createdRequests.size)
        assertEquals(DreamRunMode.FULL, store.createdRequests.single().mode)
        val durableRunId = store.pendingRuns.getValue(PRIVATE_SCOPE).first

        val recoveredScheduler = RecordingScheduler()
        val recovered = coordinator(store, recoveredScheduler)
            .scanDirtyScopes(DreamSynthesisScanReason.STARTUP)

        assertEquals(listOf(PRIVATE_SCOPE), recovered.scheduledScopes)
        assertEquals(1, store.createdRequests.size)
        assertEquals(durableRunId, recoveredScheduler.enqueuedScopes.single().runId)
        assertTrue(recoveredScheduler.enqueuedScopes.single().replaceExisting)
    }

    @Test
    fun `disabled generation cancels only that scope while use-off does not cancel`() = runBlocking {
        val store = FakeSchedulingStore(
            mutableListOf(dirty(PRIVATE_SCOPE), dirty(OTHER_SCOPE)),
        )
        val scheduler = RecordingScheduler()
        val flags = mapOf(
            PRIVATE_SCOPE to enabledFlags(generate = false, use = true),
            OTHER_SCOPE to enabledFlags(generate = true, use = false),
        )
        val coordinator = coordinator(store, scheduler, flags)

        val result = coordinator.scanDirtyScopes(DreamSynthesisScanReason.SETTINGS_CHANGED)

        assertEquals(listOf(PRIVATE_SCOPE), result.cancelledScopes)
        assertEquals(listOf(PRIVATE_SCOPE), scheduler.cancelledScopes)
        assertEquals(listOf(PRIVATE_SCOPE), store.cancelledScopes)
        assertEquals(listOf(OTHER_SCOPE), result.scheduledScopes)
        assertFalse(scheduler.enqueuedScopes.single().replaceExisting)

        coordinator.onSettingsChanged(
            scopeId = OTHER_SCOPE,
            previous = DreamingScopePreferences(generate = true, use = true),
            current = DreamingScopePreferences(generate = true, use = false),
        )
        assertEquals(listOf(PRIVATE_SCOPE), scheduler.cancelledScopes)
        assertEquals(DreamSynthesisScanReason.SETTINGS_CHANGED, scheduler.scans.last().first)
    }

    @Test
    fun `global UTC run budget applies across private and global scopes`() = runBlocking {
        val store = FakeSchedulingStore(
            mutableListOf(dirty(PRIVATE_SCOPE), dirty(DreamScopeId.Global)),
            usage = cleanUsage(startedRuns = 4),
        )
        val scheduler = RecordingScheduler()
        val result = coordinator(store, scheduler).scanDirtyScopes(DreamSynthesisScanReason.PERIODIC)

        assertTrue(result.scheduledScopes.isEmpty())
        assertEquals(
            setOf(DreamSynthesisScheduleDeferral.DAILY_BUDGET_EXHAUSTED),
            result.deferredScopes.values.toSet(),
        )
        assertEquals(DreamSynthesisScanReason.UTC_BUDGET_ROLLOVER, scheduler.scans.single().first)
        assertEquals(NOW + DREAM_UTC_DAY_MILLIS, scheduler.scans.single().second)
    }

    @Test
    fun `measured token cap fails closed when any historical usage is unknown`() = runBlocking {
        val store = FakeSchedulingStore(
            mutableListOf(dirty(PRIVATE_SCOPE)),
            usage = cleanUsage(startedRuns = 1).copy(unmeasuredInputRunCount = 1),
        )
        val scheduler = RecordingScheduler()

        val result = coordinator(store, scheduler).onScopeHint(PRIVATE_SCOPE)

        assertEquals(
            DreamSynthesisScheduleDeferral.USAGE_UNMEASURED,
            result.deferredScopes.getValue(PRIVATE_SCOPE),
        )
        assertTrue(scheduler.enqueuedScopes.isEmpty())
    }

    @Test
    fun `incremental mode is selected only after a committed synthesis watermark`() = runBlocking {
        val store = FakeSchedulingStore(mutableListOf(dirty(PRIVATE_SCOPE, lastApplied = 3L)))
        val scheduler = RecordingScheduler()

        coordinator(store, scheduler).scanDirtyScopes(DreamSynthesisScanReason.AUTHORITY_COMMIT)

        assertEquals(DreamRunMode.INCREMENTAL, store.createdRequests.single().mode)
    }

    @Test
    fun `cost policy refresh replaces stale constraints without conflating use settings`() = runBlocking {
        val store = FakeSchedulingStore(mutableListOf(dirty(PRIVATE_SCOPE)))
        val scheduler = RecordingScheduler()
        val coordinator = coordinator(store, scheduler)

        coordinator.onCostPolicyChanged(DreamingCostPolicy(), DreamingCostPolicy())
        assertTrue(scheduler.scans.isEmpty())
        coordinator.onCostPolicyChanged(
            previous = DreamingCostPolicy(),
            current = DreamingCostPolicy(requireCharging = false),
        )
        coordinator.scanDirtyScopes(DreamSynthesisScanReason.COST_POLICY_CHANGED)

        assertEquals(DreamSynthesisScanReason.COST_POLICY_CHANGED, scheduler.scans.single().first)
        assertTrue(scheduler.enqueuedScopes.single().replaceExisting)
    }

    @Test
    fun `exact idle recheck replaces a just-finished worker without applying idle delay twice`() = runBlocking {
        val scheduler = RecordingScheduler()

        val result = coordinator(
            FakeSchedulingStore(mutableListOf(dirty(PRIVATE_SCOPE))),
            scheduler,
        ).scanDirtyScopes(DreamSynthesisScanReason.APP_IDLE_RECHECK)

        assertEquals(listOf(PRIVATE_SCOPE), result.scheduledScopes)
        assertTrue(scheduler.enqueuedScopes.single().idleDeadlineAlreadyObserved)
        assertTrue(scheduler.enqueuedScopes.single().replaceExisting)
    }

    @Test
    fun `bounded follow-up stops once the page contains only durable existing runs`() = runBlocking {
        val store = FakeSchedulingStore(mutableListOf(dirty(PRIVATE_SCOPE)))
        val scheduler = RecordingScheduler()
        val coordinator = coordinator(store, scheduler)

        val created = coordinator.scanDirtyScopes(DreamSynthesisScanReason.STARTUP, limit = 1)
        val existing = coordinator.scanDirtyScopes(DreamSynthesisScanReason.FOLLOW_UP, limit = 1)

        assertTrue(created.saturated)
        assertFalse(existing.saturated)
        assertEquals(1, store.createdRequests.size)
    }

    @Test
    fun `pending reservations recover existing work but do not create a fifth daily run`() = runBlocking {
        val existingStore = FakeSchedulingStore(mutableListOf(dirty(PRIVATE_SCOPE))).also {
            it.pendingRuns[PRIVATE_SCOPE] = RUN_EXISTING to DreamRunMode.INCREMENTAL
            it.pendingRunCountOverride = 4
        }
        val existingScheduler = RecordingScheduler()
        val existing = coordinator(existingStore, existingScheduler)
            .onScopeHint(PRIVATE_SCOPE)

        assertEquals(listOf(PRIVATE_SCOPE), existing.scheduledScopes)
        assertEquals(RUN_EXISTING, existingScheduler.enqueuedScopes.single().runId)
        assertTrue(existingStore.createdRequests.isEmpty())

        val newStore = FakeSchedulingStore(mutableListOf(dirty(PRIVATE_SCOPE))).also {
            it.pendingRunCountOverride = 4
        }
        val deferred = coordinator(newStore, RecordingScheduler()).onScopeHint(PRIVATE_SCOPE)
        assertEquals(
            DreamSynthesisScheduleDeferral.DAILY_BUDGET_EXHAUSTED,
            deferred.deferredScopes.getValue(PRIVATE_SCOPE),
        )
        assertTrue(newStore.createdRequests.isEmpty())
    }

    private fun coordinator(
        store: FakeSchedulingStore,
        scheduler: RecordingScheduler,
        flags: Map<DreamScopeId, DreamingFeatureFlags> = mapOf(
            PRIVATE_SCOPE to enabledFlags(),
            OTHER_SCOPE to enabledFlags(),
            DreamScopeId.Global to enabledFlags(),
        ),
        flagSource: DreamingFeatureFlagSource? = null,
    ) = DreamSynthesisCoordinator(
        store = store,
        featureFlags = flagSource ?: object : DreamingFeatureFlagSource {
            override suspend fun flagsFor(scopeId: DreamScopeId): DreamingFeatureFlags =
                flags[scopeId] ?: DreamingFeatureFlags.M1AllOff

            override suspend fun anySynthesisGenerationEnabled(): Boolean =
                flags.values.any { it.allowsSynthesisGeneration() }
        },
        policySource = DreamingCostPolicySource { DreamingCostPolicy() },
        scheduler = scheduler,
        clock = DreamEpochClock { NOW },
        runIdGenerator = RunIds(),
    )

    private suspend fun assertAllOffWinsAgainstStaleCallback(
        label: String,
        callback: suspend (DreamSynthesisCoordinator) -> Unit,
    ) = coroutineScope {
        val scheduler = RecordingScheduler()
        val liveFlags = mutableMapOf(PRIVATE_SCOPE to enabledFlags())
        val predicateEntered = CompletableDeferred<Unit>()
        val releaseStaleDecision = CompletableDeferred<Unit>()
        var shouldPause = true
        val source = object : DreamingFeatureFlagSource {
            override suspend fun flagsFor(scopeId: DreamScopeId): DreamingFeatureFlags =
                liveFlags[scopeId] ?: DreamingFeatureFlags.M1AllOff

            override suspend fun anySynthesisGenerationEnabled(): Boolean {
                val capturedDecision = liveFlags.values.any { it.allowsSynthesisGeneration() }
                if (shouldPause) {
                    shouldPause = false
                    predicateEntered.complete(Unit)
                    releaseStaleDecision.await()
                }
                return capturedDecision
            }
        }
        val coordinator = coordinator(
            store = FakeSchedulingStore(mutableListOf(dirty(PRIVATE_SCOPE))),
            scheduler = scheduler,
            flags = liveFlags,
            flagSource = source,
        )

        val staleCallback = async { callback(coordinator) }
        predicateEntered.await()
        liveFlags[PRIVATE_SCOPE] = DreamingFeatureFlags.M1AllOff
        val disableCallback = async {
            coordinator.onSettingsChanged(
                scopeId = PRIVATE_SCOPE,
                previous = DreamingScopePreferences(generate = true),
                current = DreamingScopePreferences(),
            )
        }
        releaseStaleDecision.complete(Unit)
        staleCallback.await()
        disableCallback.await()

        assertFalse("$label left recovery armed", scheduler.recoveryArmed)
        assertEquals("$label did not finish with disarm", "disarm", scheduler.events.last())
    }

    private class FakeSchedulingStore(
        private val dirtyScopes: MutableList<DreamSynthesisDirtyScope>,
        var usage: DreamDailyUsage = cleanUsage(),
    ) : DreamSynthesisSchedulingStore {
        val pendingRuns = mutableMapOf<DreamScopeId, Pair<String, DreamRunMode>>()
        var pendingRunCountOverride: Int? = null
        var runningRunCount: Int = 0
        val createdRequests = mutableListOf<EnsurePendingSynthesisRunRequest>()
        val cancelledScopes = mutableListOf<DreamScopeId>()

        override suspend fun findDirtyScopes(limit: Int): List<DreamSynthesisDirtyScope> =
            dirtyScopes.sortedBy { it.scopeId.value }.take(limit)

        override suspend fun readDirtyScope(scopeId: DreamScopeId): DreamSynthesisDirtyScope? =
            dirtyScopes.singleOrNull { it.scopeId == scopeId }

        override suspend fun ensurePendingRun(
            request: EnsurePendingSynthesisRunRequest,
            allowCreate: Boolean,
        ): EnsurePendingSynthesisRunResult {
            pendingRuns[request.scopeId]?.let { (runId, mode) ->
                return EnsurePendingSynthesisRunResult.Ready(
                    runId,
                    mode,
                    created = false,
                    running = false,
                )
            }
            if (!allowCreate) return EnsurePendingSynthesisRunResult.CreationDeferred
            createdRequests += request
            pendingRuns[request.scopeId] = request.runId to request.mode
            return EnsurePendingSynthesisRunResult.Ready(
                request.runId,
                request.mode,
                created = true,
                running = false,
            )
        }

        override suspend fun countGlobalPendingRuns(): Int =
            pendingRunCountOverride ?: pendingRuns.size

        override suspend fun countGlobalRunningRuns(): Int = runningRunCount

        override suspend fun cancelScopeRuns(scopeId: DreamScopeId, nowMs: Long): Int {
            cancelledScopes += scopeId
            return if (pendingRuns.remove(scopeId) != null) 1 else 0
        }

        override suspend fun readGlobalUtcUsage(query: DreamDailyUsageQuery): DreamDailyUsage = usage
    }

    private class RecordingScheduler(
        private val throwOnScopeEnqueue: Boolean = false,
    ) : DreamSynthesisWorkScheduler {
        data class EnqueuedScope(
            val scopeId: DreamScopeId,
            val runId: String,
            val policy: DreamingCostPolicy,
            val replaceExisting: Boolean,
            val idleDeadlineAlreadyObserved: Boolean,
        )

        val enqueuedScopes = mutableListOf<EnqueuedScope>()
        val cancelledScopes = mutableListOf<DreamScopeId>()
        val scans = mutableListOf<Pair<DreamSynthesisScanReason, Long?>>()
        val events = mutableListOf<String>()
        var armedRecoveryCount = 0
        var disarmedRecoveryCount = 0
        var recoveryArmed = false

        override fun enqueueScope(
            scopeId: DreamScopeId,
            runId: String,
            policy: DreamingCostPolicy,
            replaceExisting: Boolean,
        ) {
            if (throwOnScopeEnqueue) throw ExpectedEnqueueFailure()
            enqueuedScopes += EnqueuedScope(
                scopeId,
                runId,
                policy,
                replaceExisting,
                idleDeadlineAlreadyObserved = false,
            )
        }

        override fun enqueueScopeAfterIdleRecheck(
            scopeId: DreamScopeId,
            runId: String,
            policy: DreamingCostPolicy,
            replaceExisting: Boolean,
        ) {
            if (throwOnScopeEnqueue) throw ExpectedEnqueueFailure()
            enqueuedScopes += EnqueuedScope(
                scopeId,
                runId,
                policy,
                replaceExisting,
                idleDeadlineAlreadyObserved = true,
            )
        }

        override fun cancelScope(scopeId: DreamScopeId) {
            cancelledScopes += scopeId
            events += "cancel_scope"
        }

        override fun enqueueDirtyScan(reason: DreamSynthesisScanReason, earliestAtEpochMs: Long?) {
            scans += reason to earliestAtEpochMs
            events += "scan"
        }

        override fun armRecoveryScans() {
            armedRecoveryCount += 1
            recoveryArmed = true
            events += "arm"
        }

        override fun disarmRecoveryScans() {
            disarmedRecoveryCount += 1
            recoveryArmed = false
            events += "disarm"
        }
    }

    private class RunIds : () -> String {
        private var value = 0
        override fun invoke(): String {
            value += 1
            return "00000000-0000-0000-0000-${value.toString().padStart(12, '0')}"
        }
    }

    private class ExpectedEnqueueFailure : RuntimeException()

    private companion object {
        const val NOW = DREAM_UTC_DAY_MILLIS * 20
        const val RUN_EXISTING = "00000000-0000-0000-0000-000000000099"
        val PRIVATE_SCOPE = DreamScopeId.requireCanonical("123e4567-e89b-12d3-a456-426614174000")
        val OTHER_SCOPE = DreamScopeId.requireCanonical("223e4567-e89b-12d3-a456-426614174000")

        fun dirty(
            scopeId: DreamScopeId,
            lastApplied: Long = 3L,
        ) = DreamSynthesisDirtyScope(
            scopeId = scopeId,
            memoryEpoch = 7L,
            observerCheckpointEpoch = 7L,
            lastAppliedMemoryEpoch = lastApplied,
            dreamStateRevision = 2L,
            activeRunId = null,
            activeRunLeaseUntilMs = null,
            updatedAtMs = NOW - 100,
        )

        fun enabledFlags(
            generate: Boolean = true,
            use: Boolean = false,
        ) = DreamingFeatureFlags(
            schemaReady = true,
            generate = generate,
            shadow = false,
            use = use,
        )

        fun cleanUsage(startedRuns: Int = 0) = DreamDailyUsage(
            startedRunCount = startedRuns,
            knownInputTokens = 0L,
            knownOutputTokens = 0L,
            unmeasuredInputRunCount = 0,
            unmeasuredOutputRunCount = 0,
        )
    }
}
