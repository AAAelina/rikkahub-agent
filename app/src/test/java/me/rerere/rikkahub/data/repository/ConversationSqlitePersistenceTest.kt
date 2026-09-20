package me.rerere.rikkahub.data.repository

import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import me.rerere.ai.ui.UIMessage
import me.rerere.ai.ui.UIMessagePart
import me.rerere.ai.core.MessageRole
import me.rerere.rikkahub.data.model.*
import me.rerere.rikkahub.data.db.entity.ConversationEntity
import me.rerere.rikkahub.service.chat.MutateMessageCommand
import me.rerere.rikkahub.utils.JsonInstant
import org.junit.Assert.*
import org.junit.Test
import kotlin.uuid.Uuid

/** Disk SQLite + production mapping/merge; does not claim to instantiate Android Room. */
class ConversationSqlitePersistenceTest {
    private fun message(text: String) = UIMessage(role = MessageRole.USER, parts = listOf(UIMessagePart.Text(text)))

    @Test fun `old runtime graph cannot roll back committed metadata after close reopen`() {
        val file = Files.createTempFile("conversation-c0", ".sqlite").toFile()
        val id = Uuid.random()
        val original = Conversation.ofId(id, Uuid.random(), listOf(message("A").toMessageNode()))
        val newerGraph = original.copy(messageNodes = original.messageNodes + message("B").toMessageNode())
        val mode = Uuid.random()
        try {
            open(file.path).use { db ->
                schema(db)
                write(db, original)
                // Frozen old runtime snapshot above, newer independent metadata commits below.
                var current = read(db, id)!!
                listOf(ConversationMetadataMutation.Title("new title"),
                    ConversationMetadataMutation.SystemPrompt("new prompt"),
                    ConversationMetadataMutation.Workspace("/work"),
                    ConversationMetadataMutation.Mode(mode, true),
                    ConversationMetadataMutation.Lorebook(mode, true)).forEach {
                    current = it.apply(current); write(db, current)
                }
                current = current.copy(isPinned = true, folderId = "folder", assistantId = Uuid.random())
                write(db, current)
                db.autoCommit = false
                write(db, read(db, id)!!.withRuntimeGraph(newerGraph))
                db.commit()
            }
            open(file.path).use { db ->
                val durable = read(db, id)!!
                assertEquals("new title", durable.title)
                assertEquals("new prompt", durable.customSystemPrompt)
                assertEquals("/work", durable.workspaceCwd)
                assertEquals(setOf(mode), durable.modeInjectionIds)
                assertEquals(setOf(mode), durable.lorebookIds)
                assertTrue(durable.isPinned)
                assertEquals("folder", durable.folderId)
                assertNotEquals(original.assistantId, durable.assistantId)
                assertEquals(newerGraph.messageNodes, durable.messageNodes)
                // Full-authority writes deliberately retain replacement semantics.
                write(db, original.copy(title = "owner"))
                assertEquals("owner", read(db, id)!!.title)
            }
        } finally { file.delete() }
    }

    @Test fun `branch identity does not resurrect missing messages and rollback stays durable`() {
        val file = Files.createTempFile("conversation-rollback", ".sqlite").toFile()
        val first = message("first")
        val second = message("second")
        val node = first.toMessageNode().copy(messages = listOf(first, second))
        val original = Conversation.ofId(Uuid.random(), Uuid.random(), listOf(node))
        try {
            open(file.path).use { db ->
                schema(db); write(db, original)
                val intent = MutateMessageCommand(node.id, second.id)
                assertNull(applyMessageMutation(original.copy(messageNodes = emptyList()), intent))
                assertNull(applyMessageMutation(original.copy(messageNodes = listOf(node.copy(messages = listOf(first)))), intent))
                val selected = applyMessageMutation(read(db, original.id)!!, intent)!!
                assertEquals(second.id, selected.currentMessages.single().id)
                db.autoCommit = false
                write(db, selected)
                db.rollback()
            }
            open(file.path).use { db ->
                assertEquals(first.id, read(db, original.id)!!.currentMessages.single().id)
                db.createStatement().use { it.executeUpdate("DELETE FROM conversationentity") }
                assertNull(read(db, original.id))
            }
        } finally { file.delete() }
    }

    @Test fun `typed metadata and edit retain current graph and concurrent set members`() {
        val first = message("a")
        val node = first.toMessageNode()
        val a = Conversation.ofId(Uuid.random(), Uuid.random(), listOf(node))
        val b = a.copy(messageNodes = a.messageNodes + message("b").toMessageNode())
        val id1 = Uuid.random(); val id2 = Uuid.random()
        val edited = ConversationMetadataMutation.Mode(id2, true).apply(
            ConversationMetadataMutation.Mode(id1, true).apply(b))
        assertEquals(b.messageNodes, edited.messageNodes)
        assertEquals(setOf(id1, id2), edited.modeInjectionIds)
        val result = applyMessageMutation(edited, MutateMessageCommand(node.id, first.id,
            listOf(UIMessagePart.Text("edited"))))!!
        assertEquals(2, result.messageNodes.size)
        assertEquals(2, result.messageNodes.first().messages.size)
        assertEquals(b.messageNodes.last(), result.messageNodes.last())
    }

    private fun open(path: String): Connection = DriverManager.getConnection("jdbc:sqlite:$path")
    private fun schema(db: Connection) = db.createStatement().use {
        it.execute("CREATE TABLE conversationentity (id TEXT PRIMARY KEY, assistant_id TEXT, title TEXT, nodes TEXT, create_at INTEGER, update_at INTEGER, suggestions TEXT, is_pinned INTEGER, custom_system_prompt TEXT, mode_injection_ids TEXT, lorebook_ids TEXT, workspace_cwd TEXT, folder_id TEXT)")
        it.execute("CREATE TABLE message_node (id TEXT PRIMARY KEY, conversation_id TEXT, node_index INTEGER, messages TEXT, select_index INTEGER)")
    }
    private fun write(db: Connection, c: Conversation) {
        val e = conversationToConversationEntity(c)
        db.prepareStatement("INSERT OR REPLACE INTO conversationentity VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)").use { q ->
            listOf(e.id, e.assistantId, e.title, e.nodes, e.createAt, e.updateAt, e.chatSuggestions,
                if (e.isPinned) 1 else 0, e.customSystemPrompt, e.modeInjectionIds, e.lorebookIds, e.workspaceCwd, e.folderId)
                .forEachIndexed { index, value -> q.setObject(index + 1, value) }
            q.executeUpdate()
        }
        db.prepareStatement("DELETE FROM message_node WHERE conversation_id=?").use { it.setString(1,e.id); it.executeUpdate() }
        c.messageNodes.forEachIndexed { index, node ->
            db.prepareStatement("INSERT INTO message_node VALUES (?,?,?,?,?)").use {
                it.setString(1,node.id.toString()); it.setString(2,e.id); it.setInt(3,index)
                it.setString(4,JsonInstant.encodeToString(node.messages)); it.setInt(5,node.selectIndex); it.executeUpdate()
            }
        }
    }
    private fun read(db: Connection, id: Uuid): Conversation? {
        val nodes = mutableListOf<MessageNode>()
        db.prepareStatement("SELECT * FROM message_node WHERE conversation_id=? ORDER BY node_index").use { q ->
            q.setString(1,id.toString()); q.executeQuery().use { rows -> while(rows.next()) {
                nodes += MessageNode(id = Uuid.parse(rows.getString("id")),
                    messages = JsonInstant.decodeFromString(rows.getString("messages")), selectIndex = rows.getInt("select_index"))
            } }
        }
        return db.prepareStatement("SELECT * FROM conversationentity WHERE id=?").use { q ->
            q.setString(1,id.toString()); q.executeQuery().use { v ->
                if (!v.next()) null else conversationEntityToConversation(ConversationEntity(
                    id=v.getString("id"), assistantId=v.getString("assistant_id"), title=v.getString("title"), nodes=v.getString("nodes"),
                    createAt=v.getLong("create_at"), updateAt=v.getLong("update_at"), chatSuggestions=v.getString("suggestions"), isPinned=v.getInt("is_pinned") != 0,
                    customSystemPrompt=v.getString("custom_system_prompt"), modeInjectionIds=v.getString("mode_injection_ids"), lorebookIds=v.getString("lorebook_ids"),
                    workspaceCwd=v.getString("workspace_cwd"), folderId=v.getString("folder_id")),nodes)
            }
        }
    }
}
