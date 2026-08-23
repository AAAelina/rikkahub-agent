package me.rerere.rikkahub.data.db

import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import me.rerere.rikkahub.data.db.dao.DreamDao
import me.rerere.rikkahub.data.db.dao.DreamExperienceDao
import me.rerere.rikkahub.data.db.entity.DreamExperienceStateEntity
import me.rerere.rikkahub.data.db.entity.DreamRunEntity
import me.rerere.rikkahub.data.db.entity.MemoryScopeChangeEntity
import me.rerere.rikkahub.data.db.entity.MemoryScopeStateEntity
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/** Emulator/disposable-device only. Never run this instrumentation test on the primary phone. */
@RunWith(AndroidJUnit4::class)
class DreamDaoContractTest {
    private lateinit var db: AppDatabase
    private lateinit var dao: DreamDao
    private lateinit var experienceDao: DreamExperienceDao

    @Before
    fun setUp() {
        db = Room.inMemoryDatabaseBuilder(
            ApplicationProvider.getApplicationContext(),
            AppDatabase::class.java,
        ).build()
        dao = db.dreamDao()
        experienceDao = db.dreamExperienceDao()
    }

    @After
    fun tearDown() {
        db.close()
    }

    @Test
    fun journalCheckpointAndPrune_areMonotonicAndReceiptIdentityIsCoalesced() = runBlocking {
        insertState()
        assertEquals(1, dao.bumpMemoryEpoch(SCOPE, 0, "MEMORY_UPDATED", 10))
        assertEquals(0, dao.bumpMemoryEpoch(SCOPE, 0, "STALE_WRITER", 11))
        dao.insertChanges(listOf(change(epoch = 1, operation = "UPDATE", revision = 1)))

        assertTrue(
            runCatching {
                dao.insertChanges(
                    listOf(change(epoch = 1, operation = "ARCHIVE", revision = 2)),
                )
            }.isFailure,
        )

        assertEquals(1, dao.bumpMemoryEpoch(SCOPE, 1, "MEMORY_ARCHIVED", 20))
        dao.insertChanges(listOf(change(epoch = 2, operation = "ARCHIVE", revision = 2)))
        assertEquals(
            listOf(1L, 2L),
            dao.listChanges(SCOPE, afterExclusiveEpoch = 0, throughInclusiveEpoch = 2)
                .map { it.memoryEpoch },
        )
        assertEquals(2, dao.countChangesThrough(SCOPE, 2))
        assertEquals(listOf(SCOPE), dao.findDirtyScopes(10).map { it.scopeId })

        dao.insertRun(run(id = "observer-run", baseMemoryEpoch = 2, createdAtMs = 25))
        assertEquals(
            1,
            dao.acquireScopeLease(SCOPE, "observer-run", 100, 25, "RUN_ACQUIRED"),
        )
        assertEquals(
            1,
            dao.startRunMirror("observer-run", SCOPE, 2, 0, 0, "worker", 100, 25),
        )
        assertEquals(0L, dao.getProtectedObserverWatermark(SCOPE))

        assertEquals(
            0,
            dao.advanceObserverCheckpoint(
                scopeId = SCOPE,
                runId = "observer-run",
                expectedMemoryEpoch = 1,
                expectedCheckpointEpoch = 0,
                targetCheckpointEpoch = 1,
                reasonCode = "OBSERVER_REPLAYED",
                nowMs = 30,
            ),
        )
        assertEquals(
            1,
            dao.advanceRunCheckpoint(
                runId = "observer-run",
                scopeId = SCOPE,
                leaseOwner = "worker",
                expectedCheckpointEpoch = 0,
                targetCheckpointEpoch = 2,
                nowMs = 29,
            ),
        )
        assertEquals(
            1,
            dao.advanceObserverCheckpoint(
                scopeId = SCOPE,
                runId = "observer-run",
                expectedMemoryEpoch = 2,
                expectedCheckpointEpoch = 0,
                targetCheckpointEpoch = 2,
                reasonCode = "OBSERVER_REPLAYED",
                nowMs = 30,
            ),
        )
        assertTrue(dao.findDirtyScopes(10).isEmpty())
        assertEquals(
            0,
            dao.advanceObserverCheckpoint(
                scopeId = SCOPE,
                runId = "observer-run",
                expectedMemoryEpoch = 2,
                expectedCheckpointEpoch = 2,
                targetCheckpointEpoch = 1,
                reasonCode = "OBSERVER_REPLAYED",
                nowMs = 31,
            ),
        )

        // The in-flight run keeps its frozen base checkpoint receipts until it is terminal.
        assertEquals(0, dao.pruneChangesThrough(SCOPE, throughInclusiveEpoch = 1))
        assertEquals(
            1,
            dao.finishRunMirror(
                runId = "observer-run",
                scopeId = SCOPE,
                leaseOwner = "worker",
                terminalStatus = "SUCCEEDED",
                failureCode = null,
                nowMs = 40,
            ),
        )
        assertEquals(1, dao.releaseScopeLease(SCOPE, "observer-run", "RUN_RELEASED", 40))
        assertNull(dao.getProtectedObserverWatermark(SCOPE))
        assertEquals(0L, dao.getSafeChangePruneWatermark(SCOPE))
        assertEquals(0, dao.pruneChangesThrough(SCOPE, throughInclusiveEpoch = 1))
        assertEquals(2, dao.countChangesThrough(SCOPE, 2))
        assertEquals(
            listOf(1L, 2L),
            dao.listChanges(SCOPE, afterExclusiveEpoch = 0, throughInclusiveEpoch = 2)
                .map { it.memoryEpoch },
        )
    }

    @Test
    fun scopeStateLease_isTheAuthorityAndFencesAnExpiredRun() = runBlocking {
        insertState()
        dao.insertRun(run(id = "run-a", createdAtMs = 10))
        dao.insertRun(run(id = "run-b", createdAtMs = 20))

        assertEquals(1, dao.acquireScopeLease(SCOPE, "run-a", 200, 100, "RUN_ACQUIRED"))
        assertEquals(1, dao.startRunMirror("run-a", SCOPE, 0, 0, 0, "worker-a", 200, 100))
        assertEquals(0, dao.acquireScopeLease(SCOPE, "run-b", 250, 150, "RUN_ACQUIRED"))

        // Once A expires, B can claim the authority row. A's mirror cannot complete afterward.
        assertEquals(listOf("run-a"), dao.findExpiredScopeLeases(200, 10).mapNotNull { it.activeRunId })
        assertEquals(listOf("run-a"), dao.findExpiredRunningRuns(200, 10).map { it.runId })
        assertEquals(1, dao.acquireScopeLease(SCOPE, "run-b", 350, 200, "LEASE_RECOVERED"))
        assertEquals(1, dao.startRunMirror("run-b", SCOPE, 0, 0, 0, "worker-b", 350, 200))
        assertEquals(
            0,
            dao.finishRunMirror(
                runId = "run-b",
                scopeId = SCOPE,
                leaseOwner = "worker-b",
                terminalStatus = "CANCELLED",
                failureCode = null,
                nowMs = 220,
            ),
        )
        assertEquals(0, dao.heartbeatScopeLease(SCOPE, "run-a", 400, 200, "RUN_HEARTBEAT"))
        assertEquals(
            0,
            dao.finishRunMirror(
                runId = "run-a",
                scopeId = SCOPE,
                leaseOwner = "worker-a",
                terminalStatus = "SUCCEEDED",
                failureCode = null,
                nowMs = 200,
            ),
        )
        assertEquals(0, dao.releaseScopeLease(SCOPE, "run-a", "RUN_RELEASED", 200))

        assertEquals(1, dao.failExpiredRunMirrors(200, "LEASE_EXPIRED"))
        assertEquals("FAILED", dao.getRun("run-a", SCOPE)?.status)
        assertEquals("LEASE_EXPIRED", dao.getRun("run-a", SCOPE)?.failureCode)
        assertNull(dao.getRun("run-a", SCOPE)?.leaseOwner)

        assertEquals(
            1,
            dao.finishRunMirror(
                runId = "run-b",
                scopeId = SCOPE,
                leaseOwner = "worker-b",
                terminalStatus = "SUCCEEDED",
                failureCode = null,
                nowMs = 250,
            ),
        )
        assertEquals(1, dao.releaseScopeLease(SCOPE, "run-b", "RUN_RELEASED", 250))
        assertNull(dao.getScopeState(SCOPE)?.activeRunId)
        assertEquals("SUCCEEDED", dao.getRun("run-b", SCOPE)?.status)
        assertNull(dao.getRun("run-b", SCOPE)?.leaseOwner)
    }

    @Test
    fun runCheckpoint_requiresCurrentOwnerUnexpiredLeaseAndExactCheckpoint() = runBlocking {
        insertState(memoryEpoch = 3, observerCheckpointEpoch = 2)
        dao.insertRun(run(id = "run-a", baseMemoryEpoch = 0, createdAtMs = 10))
        assertEquals(1, dao.acquireScopeLease(SCOPE, "run-a", 300, 100, "RUN_ACQUIRED"))
        assertEquals(1, dao.startRunMirror("run-a", SCOPE, 3, 2, 0, "worker-a", 300, 100))

        val claimed = dao.getRun("run-a", SCOPE)
        assertEquals(3L, claimed?.baseMemoryEpoch)
        assertEquals(2L, claimed?.baseObserverCheckpointEpoch)
        assertEquals(2L, claimed?.checkpointEpoch)

        // State is the lease authority. A mirror that does not exactly match cannot checkpoint
        // or terminalize until its diagnostic lease catches up in the same store transaction.
        assertEquals(1, dao.heartbeatScopeLease(SCOPE, "run-a", 400, 110, "RUN_HEARTBEAT"))
        assertEquals(0, dao.advanceRunCheckpoint("run-a", SCOPE, "worker-a", 2, 3, 120))
        assertEquals(
            0,
            dao.finishRunMirror("run-a", SCOPE, "worker-a", "CANCELLED", "STORE_FAILURE", 120),
        )
        assertEquals(1, dao.heartbeatRunMirror("run-a", SCOPE, "worker-a", 400, 110))
        assertEquals(1, dao.advanceRunCheckpoint("run-a", SCOPE, "worker-a", 2, 3, 120))
        assertEquals(0, dao.advanceRunCheckpoint("run-a", SCOPE, "worker-a", 2, 3, 130))
        assertEquals(0, dao.advanceRunCheckpoint("run-a", SCOPE, "worker-a", 3, 4, 130))
        assertEquals(3L, dao.getRun("run-a", SCOPE)?.checkpointEpoch)
    }

    @Test
    fun runClaim_refreshesDreamRevisionFromTheSameScopeStateSnapshot() = runBlocking {
        insertState(memoryEpoch = 3, observerCheckpointEpoch = 2, dreamStateRevision = 7)
        dao.insertRun(run(id = "run-a", baseMemoryEpoch = 0, createdAtMs = 10))
        assertEquals(1, dao.acquireScopeLease(SCOPE, "run-a", 300, 100, "RUN_ACQUIRED"))

        assertEquals(0, dao.startRunMirror("run-a", SCOPE, 3, 2, 6, "worker-a", 300, 100))
        assertEquals(1, dao.startRunMirror("run-a", SCOPE, 3, 2, 7, "worker-a", 300, 100))
        assertEquals(7L, dao.getRun("run-a", SCOPE)?.baseDreamRevision)
    }

    @Test
    fun runClaim_freezesOptionalSynthesisTimezoneAndRejectsAPendingMismatch() = runBlocking {
        insertState()
        dao.insertRun(run(id = "observer-run", createdAtMs = 10))
        assertEquals(1, dao.acquireScopeLease(SCOPE, "observer-run", 300, 100, "RUN_ACQUIRED"))
        assertEquals(
            1,
            dao.startRunMirror(
                "observer-run",
                SCOPE,
                0,
                0,
                0,
                "observer-worker",
                300,
                100,
                sourceTimezoneId = null,
            ),
        )
        assertNull(dao.getRun("observer-run", SCOPE)?.sourceTimezoneId)

        assertEquals(
            1,
            dao.finishRunMirror(
                runId = "observer-run",
                scopeId = SCOPE,
                leaseOwner = "observer-worker",
                terminalStatus = "SUCCEEDED",
                failureCode = null,
                nowMs = 110,
            ),
        )
        assertEquals(1, dao.releaseScopeLease(SCOPE, "observer-run", "RUN_RELEASED", 110))
        dao.insertRun(run(id = "synthesis-run", createdAtMs = 120))
        assertEquals(1, dao.acquireScopeLease(SCOPE, "synthesis-run", 400, 120, "RUN_ACQUIRED"))
        assertEquals(
            1,
            dao.startRunMirror(
                "synthesis-run",
                SCOPE,
                0,
                0,
                0,
                "synthesis-worker",
                400,
                120,
                sourceTimezoneId = "Asia/Shanghai",
            ),
        )
        val synthesis = dao.getRun("synthesis-run", SCOPE)
        assertEquals("Asia/Shanghai", synthesis?.sourceTimezoneId)
        assertEquals(120L, synthesis?.startedAtMs)

        assertEquals(
            1,
            dao.finishRunMirror(
                runId = "synthesis-run",
                scopeId = SCOPE,
                leaseOwner = "synthesis-worker",
                terminalStatus = "SUCCEEDED",
                failureCode = null,
                nowMs = 130,
            ),
        )
        assertEquals(1, dao.releaseScopeLease(SCOPE, "synthesis-run", "RUN_RELEASED", 130))

        dao.insertRun(
            run(id = "preseeded-run", createdAtMs = 140).copy(
                sourceTimezoneId = "Asia/Shanghai",
            ),
        )
        assertEquals(1, dao.acquireScopeLease(SCOPE, "preseeded-run", 500, 140, "RUN_ACQUIRED"))
        assertEquals(
            0,
            dao.startRunMirror(
                "preseeded-run",
                SCOPE,
                0,
                0,
                0,
                "synthesis-worker",
                500,
                140,
                sourceTimezoneId = "America/New_York",
            ),
        )
        assertEquals(
            1,
            dao.startRunMirror(
                "preseeded-run",
                SCOPE,
                0,
                0,
                0,
                "synthesis-worker",
                500,
                140,
                sourceTimezoneId = "Asia/Shanghai",
            ),
        )
        assertEquals("Asia/Shanghai", dao.getRun("preseeded-run", SCOPE)?.sourceTimezoneId)
    }

    @Test
    fun daoAuditTimestamps_neverMoveBackwardWhenClockRewinds() = runBlocking {
        assertTrue(
            dao.insertScopeStateIfAbsent(
                MemoryScopeStateEntity(scopeId = SCOPE, updatedAtMs = 100),
            ) != -1L,
        )
        dao.insertRun(run(id = "run-a", createdAtMs = 100))

        assertEquals(1, dao.acquireScopeLease(SCOPE, "run-a", 200, 50, "RUN_ACQUIRED"))
        assertEquals(1, dao.startRunMirror("run-a", SCOPE, 0, 0, 0, "worker-a", 200, 50))
        val running = dao.getRun("run-a", SCOPE)
        assertEquals(100L, running?.startedAtMs)
        assertEquals(100L, running?.updatedAtMs)
        assertEquals(100L, dao.getScopeState(SCOPE)?.updatedAtMs)

        assertEquals(
            1,
            dao.finishRunMirror(
                runId = "run-a",
                scopeId = SCOPE,
                leaseOwner = "worker-a",
                terminalStatus = "SUCCEEDED",
                failureCode = null,
                nowMs = 60,
            ),
        )
        assertEquals(1, dao.releaseScopeLease(SCOPE, "run-a", "RUN_RELEASED", 60))
        val finished = dao.getRun("run-a", SCOPE)
        assertEquals(100L, finished?.finishedAtMs)
        assertEquals(100L, finished?.updatedAtMs)
        assertEquals(100L, dao.getScopeState(SCOPE)?.updatedAtMs)
    }

    @Test
    fun synthesisSchedulingQueries_areScopedAndBudgetUsageIsGlobal() = runBlocking {
        insertState(
            memoryEpoch = 3,
            observerCheckpointEpoch = 3,
            lastAppliedMemoryEpoch = 0,
        )
        assertEquals(listOf(SCOPE), dao.findSynthesisDirtyScopes(10).map { it.scopeId })

        dao.insertScopeStateIfAbsent(
            MemoryScopeStateEntity(
                scopeId = GLOBAL_SCOPE,
                memoryEpoch = 1,
                observerCheckpointEpoch = 1,
                lastAppliedMemoryEpoch = 0,
                updatedAtMs = 1,
            ),
        )
        dao.insertRun(
            run(id = "private-synthesis", createdAtMs = 100).copy(
                mode = "FULL",
                status = "SUCCEEDED",
                startedAtMs = 120,
                finishedAtMs = 125,
                inputTokens = 10,
                outputTokens = 5,
            ),
        )
        dao.insertRun(
            run(id = "global-synthesis", createdAtMs = 105).copy(
                scopeId = GLOBAL_SCOPE,
                mode = "INCREMENTAL",
                status = "FAILED",
                startedAtMs = 130,
                finishedAtMs = 135,
                failureCode = "MODEL_PERMANENT_FAILURE",
                inputTokens = null,
                outputTokens = null,
            ),
        )
        dao.insertRun(
            run(id = "observer-audit", createdAtMs = 110).copy(
                status = "SUCCEEDED",
                startedAtMs = 140,
                finishedAtMs = 145,
                inputTokens = 999,
                outputTokens = 999,
            ),
        )
        val usage = dao.readGlobalDreamDailyUsage(100, 200, excludingRunId = null)
        assertEquals(2L, usage.startedRunCount)
        assertEquals(10L, usage.knownInputTokens)
        assertEquals(5L, usage.knownOutputTokens)
        assertEquals(1L, usage.unmeasuredInputRunCount)
        assertEquals(1L, usage.unmeasuredOutputRunCount)

        dao.insertRun(
            run(id = "pending-synthesis", createdAtMs = 150).copy(
                mode = "FULL",
                leaseOwner = "corrupt-owner",
                leaseUntilMs = 999,
            ),
        )
        experienceDao.insertStateIfAbsent(
            DreamExperienceStateEntity(pairScopeId = SCOPE, updatedAtMs = 150),
        )
        dao.insertRun(run(id = "pending-observer", createdAtMs = 151))
        assertEquals("pending-synthesis", dao.findPendingOrRunningSynthesisRun(SCOPE)?.runId)
        assertEquals(1L, dao.countPairPendingSynthesisRuns())
        assertEquals(
            SCOPE,
            dao.findSynthesisDirtyScopes(1).single().scopeId,
        )
        assertEquals(1, dao.cancelPendingSynthesisRuns(SCOPE, 160))
        assertEquals("CANCELLED", dao.getRunById("pending-synthesis")?.status)
        assertNull(dao.getRunById("pending-synthesis")?.leaseOwner)
        assertNull(dao.getRunById("pending-synthesis")?.leaseUntilMs)
        assertEquals("PENDING", dao.getRunById("pending-observer")?.status)
    }

    private suspend fun insertState(
        memoryEpoch: Long = 0,
        observerCheckpointEpoch: Long = 0,
        lastAppliedMemoryEpoch: Long = 0,
        dreamStateRevision: Long = 0,
    ) {
        assertTrue(
            dao.insertScopeStateIfAbsent(
                MemoryScopeStateEntity(
                    scopeId = SCOPE,
                    memoryEpoch = memoryEpoch,
                    observerCheckpointEpoch = observerCheckpointEpoch,
                    lastAppliedMemoryEpoch = lastAppliedMemoryEpoch,
                    dreamStateRevision = dreamStateRevision,
                    updatedAtMs = 1,
                ),
            ) != -1L,
        )
    }

    private fun change(epoch: Long, operation: String, revision: Long) =
        MemoryScopeChangeEntity(
            scopeId = SCOPE,
            memoryEpoch = epoch,
            entityKind = "MEMORY",
            entityId = "memory-1",
            entityRevision = revision,
            operation = operation,
            reasonCode = "AUTHORITY_CHANGED",
            createdAtMs = epoch * 10,
        )

    private fun run(
        id: String,
        baseMemoryEpoch: Long = 0,
        createdAtMs: Long,
    ) = DreamRunEntity(
        runId = id,
        scopeId = SCOPE,
        mode = "OBSERVER_REPLAY",
        status = "PENDING",
        baseMemoryEpoch = baseMemoryEpoch,
        baseObserverCheckpointEpoch = 0,
        createdAtMs = createdAtMs,
        updatedAtMs = createdAtMs,
    )

    private companion object {
        const val SCOPE = "11111111-1111-1111-1111-111111111111"
        const val GLOBAL_SCOPE = "__global__"
    }
}
