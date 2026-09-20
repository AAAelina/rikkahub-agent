package me.rerere.rikkahub.context

import me.rerere.rikkahub.data.ai.ToolCallOrigin
import me.rerere.rikkahub.data.model.Assistant
import me.rerere.rikkahub.service.chat.CommandOrigin
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ContextRequestFactoryTest {
    @Test
    fun `external command stays on external automation context surface`() {
        val request = ContextRequestFactory.create(
            commandOrigin = CommandOrigin.EXTERNAL_AUTOMATION,
            toolCallOrigin = ToolCallOrigin.ExternalIntent,
            assistant = Assistant(), conversationId = "conversation", runId = "run", commandId = "command",
            isHeadless = true, isSubAgent = false,
        )!!
        assertEquals(ContextInvocationSurface.EXTERNAL_AUTOMATION, request.invocationSurface)
    }

    @Test
    fun `missing run or command identity closes collection`() {
        assertNull(request(runId = null))
        assertNull(request(commandId = null))
    }

    @Test
    fun `assistant settings map into a bounded local request`() {
        val assistant = Assistant(
            autoContextEnabled = true,
            autoContextOcrFallback = true,
            autoContextNotifications = true,
            autoContextMaxChars = Int.MAX_VALUE,
        )

        val request = request(assistant = assistant)!!

        assertEquals(ContextInvocationSurface.LOCAL_CHAT, request.invocationSurface)
        assertTrue(request.settings.enabled)
        assertTrue(request.settings.ocrFallback)
        assertTrue(request.settings.notifications)
        assertEquals(20_000, request.settings.maxChars)
    }

    private fun request(
        assistant: Assistant = Assistant(),
        runId: String? = "run",
        commandId: String? = "command",
    ) = ContextRequestFactory.create(
        commandOrigin = CommandOrigin.APP_UI,
        toolCallOrigin = ToolCallOrigin.LocalChat,
        assistant = assistant,
        conversationId = "conversation",
        runId = runId,
        commandId = commandId,
        isHeadless = false,
        isSubAgent = false,
    )
}
