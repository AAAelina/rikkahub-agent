package me.rerere.rikkahub.data.ai.transformers

import kotlinx.datetime.LocalDateTime
import me.rerere.ai.core.MessageRole
import me.rerere.ai.ui.UIMessage
import me.rerere.ai.ui.UIMessagePart
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

class TimeReminderTransformerTest {

    private fun message(
        role: MessageRole,
        text: String,
        createdAt: LocalDateTime,
    ) = UIMessage(
        role = role,
        parts = listOf(UIMessagePart.Text(text)),
        createdAt = createdAt,
    )

    private fun text(message: UIMessage): String =
        message.parts.filterIsInstance<UIMessagePart.Text>().joinToString("") { it.text }

    @Test
    fun `every user message gets its own persisted send time as a suffix`() {
        val firstTime = LocalDateTime(2026, 2, 22, 10, 0, 0)
        val secondTime = LocalDateTime(2026, 2, 22, 10, 1, 0)
        val result = applyTimeReminder(
            listOf(
                message(MessageRole.USER, "Hello", firstTime),
                message(MessageRole.ASSISTANT, "Hi", firstTime),
                message(MessageRole.USER, "Again", secondTime),
            ),
        )

        assertEquals(3, result.size)
        assertEquals("Hello\n\n<message_time>Sent at local time: $firstTime</message_time>", text(result[0]))
        assertEquals("Hi", text(result[1]))
        assertEquals("Again\n\n<message_time>Sent at local time: $secondTime</message_time>", text(result[2]))
    }

    @Test
    fun `timestamp is appended even when messages are close together`() {
        val firstTime = LocalDateTime(2026, 2, 22, 10, 0, 0)
        val secondTime = LocalDateTime(2026, 2, 22, 10, 0, 1)
        val result = applyTimeReminder(
            listOf(
                message(MessageRole.USER, "First", firstTime),
                message(MessageRole.USER, "Second", secondTime),
            ),
        )

        assertTrue(text(result[0]).contains(firstTime.toString()))
        assertTrue(text(result[1]).contains(secondTime.toString()))
    }

    @Test
    fun `non-user messages remain the same objects`() {
        val time = LocalDateTime(2026, 2, 22, 10, 0, 0)
        val system = message(MessageRole.SYSTEM, "System", time)
        val assistant = message(MessageRole.ASSISTANT, "Assistant", time)

        val result = applyTimeReminder(listOf(system, assistant))

        assertSame(system, result[0])
        assertSame(assistant, result[1])
    }

    @Test
    fun `media-only user message gets a final text part`() {
        val time = LocalDateTime(2026, 2, 22, 10, 0, 0)
        val input = UIMessage(
            role = MessageRole.USER,
            parts = listOf(UIMessagePart.Image("https://example.com/image.png")),
            createdAt = time,
        )

        val result = applyTimeReminder(listOf(input)).single()

        assertEquals(2, result.parts.size)
        assertSame(input.parts[0], result.parts[0])
        assertEquals(
            "<message_time>Sent at local time: $time</message_time>",
            (result.parts.last() as UIMessagePart.Text).text,
        )
    }

    @Test
    fun `transformation is idempotent`() {
        val input = listOf(
            message(
                role = MessageRole.USER,
                text = "Hello",
                createdAt = LocalDateTime(2026, 2, 22, 10, 0, 0),
            ),
        )

        val once = applyTimeReminder(input)
        val twice = applyTimeReminder(once)

        assertEquals(once, twice)
        assertEquals(1, Regex("<message_time>").findAll(text(twice.single())).count())
    }

    @Test
    fun `adding a new turn leaves the transformed cache prefix unchanged`() {
        val history = listOf(
            message(
                role = MessageRole.USER,
                text = "First",
                createdAt = LocalDateTime(2026, 2, 22, 10, 0, 0),
            ),
            message(
                role = MessageRole.ASSISTANT,
                text = "Answer",
                createdAt = LocalDateTime(2026, 2, 22, 10, 0, 1),
            ),
        )
        val nextTurn = message(
            role = MessageRole.USER,
            text = "Second",
            createdAt = LocalDateTime(2026, 2, 22, 11, 0, 0),
        )

        val firstRequest = applyTimeReminder(history)
        val secondRequest = applyTimeReminder(history + nextTurn)

        assertEquals(firstRequest, secondRequest.take(firstRequest.size))
    }

    @Test
    fun `stored input is not modified`() {
        val input = message(
            role = MessageRole.USER,
            text = "Stored text",
            createdAt = LocalDateTime(2026, 2, 22, 10, 0, 0),
        )

        val result = applyTimeReminder(listOf(input)).single()

        assertEquals("Stored text", text(input))
        assertFalse(input === result)
    }

    @Test
    fun `empty messages return empty`() {
        assertTrue(applyTimeReminder(emptyList()).isEmpty())
    }
}
