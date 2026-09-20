package me.rerere.rikkahub.memory.dreaming.store

import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.entity.MemoryScopeStateEntity
import me.rerere.rikkahub.memory.dreaming.model.DreamRunMode
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.runtime.EnsurePendingSynthesisRunRequest
import me.rerere.rikkahub.memory.dreaming.runtime.EnsurePendingSynthesisRunResult
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/** Emulator/disposable-device only. Never run this instrumentation test on the primary phone. */
@RunWith(AndroidJUnit4::class)
class RoomDreamSynthesisSchedulingStoreContractTest {
    private lateinit var database: AppDatabase
    private lateinit var store: RoomDreamSynthesisSchedulingStore

    @Before
    fun setUp() {
        database = Room.inMemoryDatabaseBuilder(
            ApplicationProvider.getApplicationContext(),
            AppDatabase::class.java,
        ).build()
        store = RoomDreamSynthesisSchedulingStore(
            database,
            database.dreamDao(),
            database.dreamExperienceDao(),
        )
    }

    @After
    fun tearDown() {
        database.close()
    }

    @Test
    fun ensurePendingRun_isIdempotentAndCancellationDoesNotCrossScope() = runBlocking {
        insertDirtyScope(PRIVATE_SCOPE)
        insertDirtyScope(OTHER_SCOPE)

        val first = store.ensurePendingRun(request(PRIVATE_SCOPE, RUN_A), allowCreate = true)
            as EnsurePendingSynthesisRunResult.Ready
        val replay = store.ensurePendingRun(request(PRIVATE_SCOPE, RUN_B), allowCreate = true)
            as EnsurePendingSynthesisRunResult.Ready
        val reserved = store.ensurePendingRun(request(OTHER_SCOPE, RUN_C), allowCreate = true)

        assertTrue(first.created)
        assertEquals(RUN_A, first.runId)
        assertEquals(RUN_A, replay.runId)
        assertEquals(false, replay.created)
        assertTrue(reserved === EnsurePendingSynthesisRunResult.CreationDeferred)
        assertEquals(1, database.dreamDao().listRecentRuns(PRIVATE_SCOPE.value, 10).size)

        assertEquals(1, store.cancelScopeRuns(PRIVATE_SCOPE, NOW + 10))
        assertEquals("CANCELLED", database.dreamDao().getRunById(RUN_A)?.status)
        val other = store.ensurePendingRun(request(OTHER_SCOPE, RUN_C), allowCreate = true)
            as EnsurePendingSynthesisRunResult.Ready
        assertEquals(RUN_C, other.runId)
        assertEquals("PENDING", database.dreamDao().getRunById(RUN_C)?.status)
    }

    private suspend fun insertDirtyScope(scopeId: DreamScopeId) {
        database.dreamDao().insertScopeStateIfAbsent(
            MemoryScopeStateEntity(
                scopeId = scopeId.value,
                memoryEpoch = 4,
                observerCheckpointEpoch = 4,
                lastAppliedMemoryEpoch = 0,
                updatedAtMs = NOW,
            ),
        )
    }

    private fun request(scopeId: DreamScopeId, runId: String) =
        EnsurePendingSynthesisRunRequest(
            scopeId = scopeId,
            runId = runId,
            mode = DreamRunMode.FULL,
            createdAtMs = NOW,
        )

    private companion object {
        const val NOW = 1_000L
        const val RUN_A = "00000000-0000-0000-0000-000000000001"
        const val RUN_B = "00000000-0000-0000-0000-000000000002"
        const val RUN_C = "00000000-0000-0000-0000-000000000003"
        val PRIVATE_SCOPE = DreamScopeId.requireCanonical("123e4567-e89b-12d3-a456-426614174000")
        val OTHER_SCOPE = DreamScopeId.requireCanonical("223e4567-e89b-12d3-a456-426614174000")
    }
}
