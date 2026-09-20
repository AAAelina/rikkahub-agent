package me.rerere.rikkahub.memory.dreaming.experience

import android.content.Context
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.entity.MemoryEntity
import me.rerere.rikkahub.memory.dreaming.model.DreamPairScope
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.uuid.Uuid

/** Disposable managed emulator only: actual v50 Room DAO and on-disk Dream experience store. */
@RunWith(AndroidJUnit4::class)
class RoomDreamMemoryBackfillAndroidTest {
    private val context: Context
        get() = ApplicationProvider.getApplicationContext()
    private lateinit var database: AppDatabase

    @Before
    fun setUp() {
        context.deleteDatabase(DB_NAME)
        database = Room.databaseBuilder(context, AppDatabase::class.java, DB_NAME).build()
    }

    @After
    fun tearDown() {
        database.close()
        context.deleteDatabase(DB_NAME)
    }

    @Test
    fun moreThan128MemoriesReachDurableExperienceStoreAndResumeIdempotently() = runBlocking {
        val assistantId = Uuid.parse(ASSISTANT_ID)
        val scopeId = assistantId.toString()
        val dao = database.memoryDao()
        val confirmedIds = (0 until 300).map { index ->
            dao.insertMemory(
                MemoryEntity(
                    assistantId = scopeId,
                    content = "confirmed-memory-$index",
                    createdAtMs = 1L,
                    updatedAtMs = 2L,
                ),
            ).toInt()
        }
        dao.insertMemory(
            MemoryEntity(
                assistantId = scopeId,
                content = "unconfirmed",
                truthStatus = "CANDIDATE",
            ),
        )
        dao.insertMemory(
            MemoryEntity(
                assistantId = scopeId,
                content = "archived",
                lifecycleStatus = "ARCHIVED",
            ),
        )
        dao.insertMemory(
            MemoryEntity(
                assistantId = scopeId,
                content = "expired",
                expiresAtMs = NOW_MS,
            ),
        )
        dao.insertMemory(MemoryEntity(assistantId = OTHER_SCOPE, content = "other-assistant"))

        val firstPage = dao.getActiveConfirmedMemoriesForDreamPage(
            scopeId = scopeId, nowMs = NOW_MS, afterId = Int.MIN_VALUE, pageSize = 128,
        )
        val secondPage = dao.getActiveConfirmedMemoriesForDreamPage(
            scopeId = scopeId, nowMs = NOW_MS, afterId = firstPage.last().id, pageSize = 128,
        )
        val thirdPage = dao.getActiveConfirmedMemoriesForDreamPage(
            scopeId = scopeId, nowMs = NOW_MS, afterId = secondPage.last().id, pageSize = 128,
        )
        assertEquals(128, firstPage.size)
        assertEquals(128, secondPage.size)
        assertEquals(44, thirdPage.size)
        assertEquals(confirmedIds.toSet(), (firstPage + secondPage + thirdPage).map { it.id }.toSet())

        val pairId = DreamPairScope.forAssistant(assistantId).id
        var store = RoomDreamExperienceStore(database, database.dreamExperienceDao(), database.dreamDao())
        var adapter = DreamMemoryAdapter(dao, store)
        assertEquals(300, adapter.ingestConfirmedMemories(assistantId, scopeId, nowMs = NOW_MS, limit = 128))
        assertEquals(300, store.pendingCount(pairId))
        assertEquals(0, adapter.ingestConfirmedMemories(assistantId, scopeId, nowMs = NOW_MS, limit = 128))
        val sourceRefs = store.pending(pairId, limit = 512).map { it.sourceRef }.toSet()
        assertEquals(300, sourceRefs.size)
        assertTrue(sourceRefs.contains("memory:$scopeId:${confirmedIds.last()}:1"))

        database.close()
        database = Room.databaseBuilder(context, AppDatabase::class.java, DB_NAME).build()
        store = RoomDreamExperienceStore(database, database.dreamExperienceDao(), database.dreamDao())
        adapter = DreamMemoryAdapter(database.memoryDao(), store)
        assertEquals(300, store.pendingCount(pairId))
        assertEquals(0, adapter.ingestConfirmedMemories(assistantId, scopeId, nowMs = NOW_MS, limit = 128))
        assertEquals(300, store.pendingCount(pairId))
    }

    private companion object {
        const val DB_NAME = "dream-keyset-backfill-contract.db"
        const val NOW_MS = 100_000L
        const val ASSISTANT_ID = "2cfe3fbd-f3ae-4ef1-bbe1-73c68bcb0f20"
        const val OTHER_SCOPE = "8d96faf2-b9e8-44f8-973e-361317f95f35"
    }
}
