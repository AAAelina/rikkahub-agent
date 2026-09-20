package me.rerere.rikkahub.service

import java.nio.file.Files
import java.nio.file.Paths
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/** Projection has no durable before/after or global ownership proof, so cannot authorize GC. */
class ConversationProjectionAttachmentContractTest {
    @Test
    fun `conversation deletion cannot assume attachment URLs have exclusive ownership`() {
        val source = readSource("data/repository/ConversationRepository.kt")
        val start = source.indexOf("suspend fun deleteConversation(conversation:")
        val end = source.indexOf("suspend fun searchMessages(", start)
        assertTrue(start >= 0 && end > start)
        assertFalse(source.substring(start, end).contains("deleteChatFiles("))
    }

    @Test
    fun `whole and partial session projections cannot physically delete attachments`() {
        val source = readSource("service/ChatService.kt")
        val start = source.indexOf("private fun updateConversation(conversationId:")
        val end = source.indexOf("suspend fun saveConversation(", start)
        assertTrue(start >= 0 && end > start)
        val projection = source.substring(start, end)
        assertFalse(projection.contains("checkFilesDelete("))
        assertFalse(projection.contains("deleteChatFiles("))
        assertFalse(projection.contains("filesManager."))
    }

    private fun readSource(relative: String): String {
        var root = Paths.get(System.getProperty("user.dir")).toAbsolutePath()
        while (!Files.isDirectory(root.resolve("app/src/main/java"))) {
            root = requireNotNull(root.parent)
        }
        return Files.readString(root.resolve(
            "app/src/main/java/me/rerere/rikkahub/$relative",
        ))
    }
}
