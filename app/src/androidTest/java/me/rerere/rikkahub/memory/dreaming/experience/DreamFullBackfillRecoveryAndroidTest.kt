package me.rerere.rikkahub.memory.dreaming.experience

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import me.rerere.ai.core.MessageRole
import me.rerere.ai.ui.UIMessage
import me.rerere.ai.ui.UIMessagePart
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.dao.MemoryDAO
import me.rerere.rikkahub.data.db.entity.MemoryEntity
import me.rerere.rikkahub.data.db.entity.MessageNodeEntity
import me.rerere.rikkahub.data.model.Conversation
import me.rerere.rikkahub.data.model.toMessageNode
import me.rerere.rikkahub.data.repository.conversationToConversationEntity
import me.rerere.rikkahub.memory.dreaming.model.DreamPairScope
import me.rerere.rikkahub.utils.JsonInstant
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import androidx.test.ext.junit.runners.AndroidJUnit4
import org.koin.java.KoinJavaComponent.getKoin
import kotlin.uuid.Uuid

/** Full production ingestor: conversation history + paginated Room memories + durable marker.
 * Only the DAO read boundary fails; all successful reads/writes use the actual Room database. */
@RunWith(AndroidJUnit4::class)
class DreamFullBackfillRecoveryAndroidTest {
    @Test fun failedSecondPageRetainsHistoryDebtAndRetryDeduplicatesCommittedPages() = runBlocking {
        checkRecovery(cancel = false)
    }

    @Test fun cancelledSecondPagePropagatesWithoutMarkingHistoryCompleteOrIngestingNewTurn() = runBlocking {
        checkRecovery(cancel = true)
    }

    private suspend fun checkRecovery(cancel: Boolean) {
        val koin = getKoin()
        val database = koin.get<AppDatabase>()
        val assistant = Uuid.random()
        val scope = assistant.toString()
        val pair = DreamPairScope.forAssistant(assistant).id
        val store = RoomDreamExperienceStore(database, database.dreamExperienceDao(), database.dreamDao())
        val history = Conversation.ofId(Uuid.random(), assistant, listOf(
            UIMessage(role = MessageRole.USER, parts = listOf(UIMessagePart.Text("synthetic history question"))).toMessageNode(),
            UIMessage(role = MessageRole.ASSISTANT, parts = listOf(UIMessagePart.Text("synthetic history answer"))).toMessageNode(),
        ))
        database.conversationDao().insert(conversationToConversationEntity(history))
        database.messageNodeDao().insertAll(history.messageNodes.mapIndexed { index, node ->
            MessageNodeEntity(node.id.toString(), history.id.toString(), index,
                JsonInstant.encodeToString(node.messages), node.selectIndex)
        })
        repeat(300) { database.memoryDao().insertMemory(MemoryEntity(assistantId = scope,
            content = "synthetic history memory $it", createdAtMs = 1L, updatedAtMs = 2L)) }
        var interrupt = true
        val memory = object : MemoryDAO by database.memoryDao() {
            override suspend fun getActiveConfirmedMemoriesForDreamPage(
                scopeId: String, nowMs: Long, afterId: Int, pageSize: Int,
            ): List<MemoryEntity> {
                if (interrupt && afterId != Int.MIN_VALUE) {
                    if (cancel) throw CancellationException("injected page interruption")
                    throw java.io.IOException("injected page read failure")
                }
                return database.memoryDao().getActiveConfirmedMemoriesForDreamPage(scopeId, nowMs, afterId, pageSize)
            }
        }
        val ingestor = DreamExperienceIngestor(ConversationEpisodeAdapter(store), DreamMemoryAdapter(memory, store),
            store, koin.get(), koin.get())
        val conversation = Uuid.random()
        val user = Uuid.random()
        val answer = Uuid.random()
        suspend fun ingest() = ingestor.ingestCompletedTurn(assistant, conversation, user, answer,
            "synthetic current question", "synthetic current answer", scope, 100_000L)
        val failure = runCatching { ingest() }.exceptionOrNull()
        if (cancel) assertTrue(failure is CancellationException) else assertNull(failure)
        assertNull(store.state(pair)?.historyBackfilledAtMs)
        assertEquals(if (cancel) 129 else 130, store.pendingCount(pair))
        interrupt = false
        ingest()
        assertNotNull(store.state(pair)?.historyBackfilledAtMs)
        assertEquals(302, store.pendingCount(pair))
        // New orchestration instance, same durable Room state: neither the history nor the
        // just-completed turn may be duplicated on retry.
        val resumedStore = RoomDreamExperienceStore(database, database.dreamExperienceDao(), database.dreamDao())
        DreamExperienceIngestor(ConversationEpisodeAdapter(resumedStore), DreamMemoryAdapter(database.memoryDao(), resumedStore),
            resumedStore, koin.get(), koin.get()).ingestCompletedTurn(assistant, conversation, user, answer,
            "synthetic current question", "synthetic current answer", scope, 100_001L)
        assertEquals(302, resumedStore.pendingCount(pair))
        assertEquals(100_000L, resumedStore.state(pair)?.historyBackfilledAtMs)
    }
}
