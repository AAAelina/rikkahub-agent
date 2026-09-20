package me.rerere.rikkahub.data.repository

import me.rerere.rikkahub.data.model.Conversation
import kotlin.uuid.Uuid

sealed interface ConversationMetadataMutation {
    data class Injections(val modes: Set<Uuid>, val lorebooks: Set<Uuid>) : ConversationMetadataMutation
    data class GeneratedTitle(val value: String, val force: Boolean) : ConversationMetadataMutation
    data class Title(val value: String) : ConversationMetadataMutation
    data class SystemPrompt(val value: String?) : ConversationMetadataMutation
    data class Workspace(val value: String?) : ConversationMetadataMutation
    data class Mode(val id: Uuid, val enabled: Boolean) : ConversationMetadataMutation
    data class Lorebook(val id: Uuid, val enabled: Boolean) : ConversationMetadataMutation

    fun apply(current: Conversation): Conversation = when (this) {
        is Injections -> current.copy(modeInjectionIds = modes, lorebookIds = lorebooks)
        is GeneratedTitle -> if (force || current.title.isBlank()) current.copy(title = value) else current
        is Title -> current.copy(title = value)
        is SystemPrompt -> current.copy(customSystemPrompt = value)
        is Workspace -> current.copy(workspaceCwd = value)
        is Mode -> current.copy(modeInjectionIds = if (enabled) current.modeInjectionIds + id else current.modeInjectionIds - id)
        is Lorebook -> current.copy(lorebookIds = if (enabled) current.lorebookIds + id else current.lorebookIds - id)
    }
}

internal fun Conversation.withRuntimeGraph(incoming: Conversation): Conversation {
    require(id == incoming.id)
    return copy(messageNodes = incoming.messageNodes, chatSuggestions = incoming.chatSuggestions,
        updateAt = incoming.updateAt, newConversation = false)
}

internal fun applyMessageMutation(
    current: Conversation,
    command: me.rerere.rikkahub.service.chat.MutateMessageCommand,
): Conversation? {
    val node = current.messageNodes.find { it.id == command.nodeId } ?: return null
    val index = node.messages.indexOfFirst { it.id == command.messageId }
    if (index < 0) return null
    val next = if (command.replacementParts == null) node.copy(selectIndex = index)
    else node.copy(messages = node.messages + me.rerere.ai.ui.UIMessage(
        id = command.newMessageId, role = node.role, parts = command.replacementParts),
        selectIndex = node.messages.size)
    return current.copy(messageNodes = current.messageNodes.map { if (it.id == node.id) next else it })
}
