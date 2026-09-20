package me.rerere.rikkahub.context

import me.rerere.rikkahub.data.ai.ToolCallOrigin
import me.rerere.rikkahub.data.model.Assistant
import me.rerere.rikkahub.service.chat.CommandOrigin

object ContextRequestFactory {
    fun create(
        commandOrigin: CommandOrigin,
        toolCallOrigin: ToolCallOrigin,
        assistant: Assistant,
        conversationId: String,
        runId: String?,
        commandId: String?,
        isHeadless: Boolean,
        isSubAgent: Boolean,
        targetDisplaySessionId: String? = null,
        allowedSources: Set<ContextSource> = ContextSource.entries.toSet(),
    ): ContextRequest? {
        if (commandOrigin == CommandOrigin.PET_INTERACTION ||
            toolCallOrigin == ToolCallOrigin.PetInteraction
        ) return null
        if (runId.isNullOrBlank() || commandId.isNullOrBlank()) return null
        return ContextRequest(
            commandOrigin = commandOrigin,
            toolCallOrigin = toolCallOrigin,
            invocationSurface = invocationSurface(
                commandOrigin = commandOrigin,
                toolCallOrigin = toolCallOrigin,
                isSubAgent = isSubAgent,
            ),
            assistantId = assistant.id.toString(),
            conversationId = conversationId,
            runId = runId,
            commandId = commandId,
            isHeadless = isHeadless,
            isSubAgent = isSubAgent,
            targetDisplaySessionId = targetDisplaySessionId,
            settings = AssistantContextSettings(
                enabled = assistant.autoContextEnabled,
                foregroundWindow = assistant.autoContextForegroundWindow,
                uiTree = assistant.autoContextUiTree,
                deviceStatus = assistant.autoContextDeviceStatus,
                ocrFallback = assistant.autoContextOcrFallback,
                usageStats = assistant.autoContextUsageStats,
                notifications = assistant.autoContextNotifications,
                maxChars = assistant.autoContextMaxChars.coerceIn(0, MAX_CONTEXT_CHARS),
            ),
            allowedSources = allowedSources,
        )
    }

    private fun invocationSurface(
        commandOrigin: CommandOrigin,
        toolCallOrigin: ToolCallOrigin,
        isSubAgent: Boolean,
    ): ContextInvocationSurface {
        if (isSubAgent) return ContextInvocationSurface.SUBAGENT
        return when (commandOrigin) {
            CommandOrigin.EXTERNAL_AUTOMATION -> ContextInvocationSurface.EXTERNAL_AUTOMATION
            CommandOrigin.APP_UI -> ContextInvocationSurface.LOCAL_CHAT
            CommandOrigin.SYSTEM_ASSISTANT -> ContextInvocationSurface.SYSTEM_ASSISTANT
            CommandOrigin.SYSTEM_ASSISTANT_KEYGUARD -> ContextInvocationSurface.KEYGUARD
            CommandOrigin.QUICK_CAPTURE -> ContextInvocationSurface.QUICK_CAPTURE
            CommandOrigin.TELEGRAM -> ContextInvocationSurface.TELEGRAM
            CommandOrigin.WEB_API -> ContextInvocationSurface.WEB
            CommandOrigin.CRON -> ContextInvocationSurface.CRON
            CommandOrigin.PET_INTERACTION -> ContextInvocationSurface.KEYGUARD
            CommandOrigin.PET_HANDOFF_CONFIRMED -> ContextInvocationSurface.LOCAL_CHAT
            CommandOrigin.PET_HANDOFF_AUTO -> ContextInvocationSurface.WORKFLOW
            CommandOrigin.INTERNAL -> when (toolCallOrigin) {
                ToolCallOrigin.LocalChat -> ContextInvocationSurface.LOCAL_CHAT
                ToolCallOrigin.SystemAssistant -> ContextInvocationSurface.SYSTEM_ASSISTANT
                ToolCallOrigin.SystemAssistantKeyguard -> ContextInvocationSurface.KEYGUARD
                ToolCallOrigin.QuickCapture -> ContextInvocationSurface.QUICK_CAPTURE
                ToolCallOrigin.TrustedWorkflow -> ContextInvocationSurface.WORKFLOW
                ToolCallOrigin.Telegram -> ContextInvocationSurface.TELEGRAM
                ToolCallOrigin.WebServer -> ContextInvocationSurface.WEB
                ToolCallOrigin.MCP -> ContextInvocationSurface.MCP
                ToolCallOrigin.ExternalIntent -> ContextInvocationSurface.EXTERNAL_AUTOMATION
                ToolCallOrigin.PetInteraction -> ContextInvocationSurface.KEYGUARD
                ToolCallOrigin.PetHandoffConfirmed -> ContextInvocationSurface.LOCAL_CHAT
                ToolCallOrigin.PetHandoffAuto -> ContextInvocationSurface.WORKFLOW
            }
        }
    }

    private const val MAX_CONTEXT_CHARS = 20_000
}
