package me.rerere.rikkahub.data.ai.transformers

import me.rerere.ai.core.MessageRole
import me.rerere.ai.ui.UIMessage
import me.rerere.ai.ui.UIMessagePart

private const val MESSAGE_TIME_OPEN_TAG = "<message_time>"

/**
 * Adds the persisted send time to every user message in the provider-only projection.
 *
 * [UIMessage.createdAt] is deliberately used instead of the current clock: old message prefixes
 * therefore remain byte-for-byte stable on later turns and stay eligible for provider caching.
 * The stored conversation and the text rendered in the UI are not modified.
 */
object TimeReminderTransformer : InputMessageTransformer {
    override suspend fun transform(
        ctx: TransformerContext,
        messages: List<UIMessage>,
    ): List<UIMessage> {
        if (!ctx.assistant.enableTimeReminder) return messages
        return applyTimeReminder(messages)
    }
}

internal fun applyTimeReminder(messages: List<UIMessage>): List<UIMessage> = messages.map { message ->
    if (message.role != MessageRole.USER) {
        message
    } else {
        message.appendStableTimeSuffix()
    }
}

private fun UIMessage.appendStableTimeSuffix(): UIMessage {
    val suffix = "${MESSAGE_TIME_OPEN_TAG}Sent at local time: $createdAt</message_time>"
    val lastPart = parts.lastOrNull()
    return when {
        lastPart is UIMessagePart.Text && lastPart.text.endsWith(suffix) -> this
        lastPart is UIMessagePart.Text -> copy(
            parts = parts.dropLast(1) + lastPart.copy(text = "${lastPart.text}\n\n$suffix"),
        )
        else -> copy(parts = parts + UIMessagePart.Text(suffix))
    }
}
