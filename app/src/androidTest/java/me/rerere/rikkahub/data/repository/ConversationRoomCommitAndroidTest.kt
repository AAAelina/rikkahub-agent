package me.rerere.rikkahub.data.repository

import android.content.Context
import androidx.room.Room
import androidx.room.withTransaction
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import me.rerere.ai.core.MessageRole
import me.rerere.ai.ui.UIMessage
import me.rerere.ai.ui.UIMessagePart
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.entity.MessageNodeEntity
import me.rerere.rikkahub.data.model.Conversation
import me.rerere.rikkahub.data.model.MessageNode
import me.rerere.rikkahub.data.model.toMessageNode
import me.rerere.rikkahub.service.chat.MutateMessageCommand
import me.rerere.rikkahub.utils.JsonInstant
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.uuid.Uuid

/**
 * Isolated Android emulator ONLY. Exercises generated Room v50 DAOs, SQLite transaction
 * boundaries, production mapping and C0 mutation/graph policy. No user/device data.
 * The source-authority/ChatService orchestration is covered by separate gates.
 */
@RunWith(AndroidJUnit4::class)
class ConversationRoomCommitAndroidTest {
    private lateinit var context: Context
    private lateinit var db: AppDatabase
    private lateinit var dbName: String

    @Before fun setUp() {
        context = ApplicationProvider.getApplicationContext()
        dbName = "c0-disposable-" + java.util.UUID.randomUUID() + ".db"
        reopen()
    }

    @After fun tearDown() {
        db.close()
        context.deleteDatabase(dbName)
    }

    private fun reopen() {
        db = Room.databaseBuilder(context, AppDatabase::class.java, dbName).build()
    }

    private fun message(body: String): UIMessage =
        UIMessage(role = MessageRole.USER, parts = listOf(UIMessagePart.Text(body)))

    private suspend fun read(id: Uuid): Conversation? {
        val entity = db.conversationDao().getConversationById(id.toString()) ?: return null
        val nodes = db.messageNodeDao().getNodesOfConversation(id.toString()).map {
            MessageNode(
                id = Uuid.parse(it.id),
                messages = JsonInstant.decodeFromString(it.messages),
                selectIndex = it.selectIndex,
            )
        }
        return conversationEntityToConversation(entity, nodes)
    }

    private suspend fun put(c: Conversation, create: Boolean) {
        if (create) db.conversationDao().insert(conversationToConversationEntity(c))
        else db.conversationDao().update(conversationToConversationEntity(c))
        if (!create) db.messageNodeDao().deleteByConversation(c.id.toString())
        db.messageNodeDao().insertAll(c.messageNodes.mapIndexed { index, node ->
            MessageNodeEntity(
                id = node.id.toString(), conversationId = c.id.toString(),
                nodeIndex = index, messages = JsonInstant.encodeToString(node.messages),
                selectIndex = node.selectIndex,
            )
        })
    }

    @Test fun capturedMetadataCannotOverwriteLaterGraphInRealRoom() = runBlocking {
        val original = Conversation.ofId(Uuid.random(), Uuid.random(), listOf(message("A").toMessageNode()))
        val capturedA = original
        db.withTransaction { put(original, create = true) }
        val currentB = original.copy(messageNodes = original.messageNodes + message("B").toMessageNode())
        db.withTransaction { put(currentB, create = false) }

        // The old UI carries an intent, never a replacement graph.
        db.withTransaction {
            val current = requireNotNull(read(original.id))
            db.conversationDao().update(conversationToConversationEntity(
                ConversationMetadataMutation.SystemPrompt("from old UI").apply(current),
            ))
        }
        db.close()
        reopen()
        val durable = requireNotNull(read(original.id))
        assertEquals("from old UI", durable.customSystemPrompt)
        assertEquals(listOf("A", "B"), durable.currentMessages.map {
            (it.parts.single() as UIMessagePart.Text).text
        })
        assertEquals(capturedA.messageNodes.first().id, durable.messageNodes.first().id)
    }

    @Test fun lateRuntimeWritePreservesCommittedMetadataAfterRoomReopen() = runBlocking {
        val a = Conversation.ofId(Uuid.random(), Uuid.random(), listOf(message("A").toMessageNode()))
        db.withTransaction { put(a, create = true) }
        val frozenGenerationGraph = a.copy(messageNodes = a.messageNodes + message("B").toMessageNode())
        db.withTransaction {
            val current = requireNotNull(read(a.id))
            db.conversationDao().update(conversationToConversationEntity(
                ConversationMetadataMutation.Title("new title").apply(
                    ConversationMetadataMutation.Workspace("/latest").apply(current),
                ),
            ))
        }
        db.withTransaction {
            val durable = requireNotNull(read(a.id))
            put(durable.withRuntimeGraph(frozenGenerationGraph), create = false)
        }
        db.close()
        reopen()
        val actual = requireNotNull(read(a.id))
        assertEquals("new title", actual.title)
        assertEquals("/latest", actual.workspaceCwd)
        assertEquals(2, actual.messageNodes.size)
    }

    @Test fun missingIdentityAndTransactionRollbackNeverResurrectNodes() = runBlocking {
        val first = message("first")
        val second = message("second")
        val node = first.toMessageNode().copy(messages = listOf(first, second))
        val a = Conversation.ofId(Uuid.random(), Uuid.random(), listOf(node))
        db.withTransaction { put(a, create = true) }
        val mutation = MutateMessageCommand(node.id, second.id)

        assertNull(applyMessageMutation(a.copy(messageNodes = emptyList()), mutation))
        assertNull(applyMessageMutation(a.copy(messageNodes = listOf(
            node.copy(messages = listOf(first)),
        )), mutation))
        try {
            db.withTransaction {
                val selected = requireNotNull(applyMessageMutation(requireNotNull(read(a.id)), mutation))
                put(selected, create = false)
                error("inject rollback after Room DAO mutation")
            }
            fail("transaction must throw")
        } catch (expected: IllegalStateException) {
            assertEquals("inject rollback after Room DAO mutation", expected.message)
        }
        db.close()
        reopen()
        assertEquals(first.id, requireNotNull(read(a.id)).currentMessages.single().id)
        db.withTransaction { db.conversationDao().deleteById(a.id.toString()) }
        assertNull(read(a.id))
        db.withTransaction {
            val stored = read(a.id)
            assertNull(stored)
            // A no-longer-existing conversation is not an implicit creation.
        }
        assertTrue(db.messageNodeDao().getNodesOfConversation(a.id.toString()).isEmpty())
    }
}
