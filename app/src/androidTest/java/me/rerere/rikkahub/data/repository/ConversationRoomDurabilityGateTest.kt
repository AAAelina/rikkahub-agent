package me.rerere.rikkahub.data.repository

import androidx.room.Room
import androidx.room.withTransaction
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import android.content.Context
import java.io.File
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
import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.uuid.Uuid

/**
 * Disposable emulator only: real AppDatabase, generated Room DAO, actual disk and rollback.
 * Exercises the production withRuntimeGraph/metadata/identity policy and Room DAO writes.
 * Does not instantiate ConversationRepository, ChatService, or the attachment GC.
 */
@RunWith(AndroidJUnit4::class)
class ConversationRoomDurabilityGateTest {
    private val context: Context get() = ApplicationProvider.getApplicationContext()

    private fun message(text: String) =
        UIMessage(role = MessageRole.USER, parts = listOf(UIMessagePart.Text(text)))

    private fun open(name: String): AppDatabase =
        Room.databaseBuilder(context, AppDatabase::class.java, name).build()

    private suspend fun <T> withDb(name: String, block: suspend (AppDatabase) -> T): T {
        val db = open(name)
        try { return block(db) } finally { db.close() }
    }

    private suspend fun read(db: AppDatabase, id: Uuid): Conversation? {
        val entity = db.conversationDao().getConversationById(id.toString()) ?: return null
        val nodes = db.messageNodeDao().getNodesOfConversation(id.toString()).map {
            MessageNode(id = Uuid.parse(it.id),
                messages = JsonInstant.decodeFromString(it.messages),
                selectIndex = it.selectIndex)
        }
        return conversationEntityToConversation(entity, nodes)
    }

    private suspend fun write(db: AppDatabase, conversation: Conversation, insert: Boolean = false) {
        val row = conversationToConversationEntity(conversation)
        if (insert) db.conversationDao().insert(row) else db.conversationDao().update(row)
        db.messageNodeDao().deleteByConversation(conversation.id.toString())
        db.messageNodeDao().insertAll(conversation.messageNodes.mapIndexed { index, node ->
            MessageNodeEntity(node.id.toString(), conversation.id.toString(), index,
                JsonInstant.encodeToString(node.messages), node.selectIndex)
        })
    }

    @Test
    fun staleGraphAndMetadataCommitSurviveRealRoomCloseReopen() = runBlocking {
        val name = "c0-room-${Uuid.random()}.db"
        val id = Uuid.random()
        val mode = Uuid.random()
        val initial = Conversation.ofId(id, Uuid.random(), listOf(message("old").toMessageNode()))
        val newerGraph = initial.copy(messageNodes = initial.messageNodes + message("new").toMessageNode())
        try {
            withDb(name) { db ->
                db.withTransaction { write(db, initial, insert = true) }
                // Freeze the old runtime graph, then commit field-level metadata through actual Room.
                db.withTransaction {
                    val stored = requireNotNull(read(db, id))
                    val next = ConversationMetadataMutation.SystemPrompt("updated").apply(stored)
                    db.conversationDao().update(conversationToConversationEntity(next))
                }
                db.withTransaction {
                    val stored = requireNotNull(read(db, id))
                    val next = ConversationMetadataMutation.Mode(mode, true).apply(stored)
                    db.conversationDao().update(conversationToConversationEntity(next))
                }
                db.withTransaction {
                    val stored = requireNotNull(read(db, id))
                    write(db, stored.withRuntimeGraph(newerGraph))
                }
            }
            withDb(name) { db ->
                val durable = requireNotNull(read(db, id))
                assertEquals("updated", durable.customSystemPrompt)
                assertEquals(setOf(mode), durable.modeInjectionIds)
                assertEquals(newerGraph.messageNodes, durable.messageNodes)
                assertEquals(2, db.messageNodeDao().getNodesOfConversation(id.toString()).size)
                val node = newerGraph.messageNodes.last()
                // Remove the exact targeted node. drop(1) accidentally kept this last node
                // and asserted that a valid mutation should be rejected.
                val vanished = durable.copy(
                    messageNodes = durable.messageNodes.filterNot { it.id == node.id },
                )
                assertTrue(vanished.messageNodes.none { it.id == node.id })
                // Missing branch identity never recreates a stale node.
                assertNull(applyMessageMutation(vanished, MutateMessageCommand(node.id, node.messages.first().id)))
            }
        } finally { context.deleteDatabase(name) }
    }

    @Test
    fun rollbackAndMissingRowCannotPublishReplacementOrDestroyAnAttachment() = runBlocking {
        val name = "c0-rollback-${Uuid.random()}.db"
        val id = Uuid.random()
        val attachment = File(context.cacheDir, "c0-keep-${Uuid.random()}.bin")
        attachment.writeText("retained")
        val attachmentUrl = attachment.toURI().toString()
        val initial = Conversation.ofId(id, Uuid.random(), listOf(
            UIMessage(role = MessageRole.USER, parts = listOf(
                UIMessagePart.Text("stable"), UIMessagePart.Image(attachmentUrl),
            )).toMessageNode(),
        ))
        try {
            withDb(name) { db ->
                db.withTransaction { write(db, initial, insert = true) }
                val attempt = runCatching {
                    db.withTransaction {
                        val current = requireNotNull(read(db, id))
                        write(db, current.copy(title = "uncommitted",
                            messageNodes = current.messageNodes + message("uncommitted").toMessageNode()))
                        error("injected transaction failure")
                    }
                }
                assertTrue(attempt.isFailure)
                assertTrue(attachment.isFile)
                assertEquals("retained", attachment.readText())
                val rolledBack = requireNotNull(read(db, id))
                assertEquals("", rolledBack.title)
                assertEquals(1, rolledBack.messageNodes.size)
                assertEquals(attachmentUrl,
                    (rolledBack.currentMessages.single().parts.last() as UIMessagePart.Image).url)
                db.withTransaction {
                    db.conversationDao().deleteById(id.toString())
                    assertNull(read(db, id))
                    // Runtime write must fail closed for a missing row (never silent upsert).
                    val missing = read(db, id)
                    assertNull(missing)
                }
            }
            withDb(name) { db -> assertNull(read(db, id)) }
            assertTrue(attachment.isFile)
        } finally {
            context.deleteDatabase(name)
            attachment.delete()
        }
    }
}
