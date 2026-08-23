package me.rerere.rikkahub.context

import me.rerere.rikkahub.data.ai.ToolCallOrigin
import me.rerere.rikkahub.service.chat.CommandOrigin

enum class ContextInvocationSurface {
    LOCAL_CHAT,
    SYSTEM_ASSISTANT,
    QUICK_CAPTURE,
    TELEGRAM,
    WEB,
    WORKFLOW,
    CRON,
    SUBAGENT,
    KEYGUARD,
    MCP,
    EXTERNAL_AUTOMATION,
}

enum class ContextSource {
    FOREGROUND_WINDOW,
    UI_TREE,
    DEVICE_STATUS,
    OCR_FALLBACK,
    USAGE_STATS,
    NOTIFICATIONS,
}

enum class ContextOmissionReason {
    DISABLED,
    ORIGIN_BLOCKED,
    SOURCE_NOT_ALLOWED,
    UNAVAILABLE,
    EMPTY,
    UI_TREE_SUFFICIENT,
    TIMED_OUT,
    FAILED,
    BUDGET_TRUNCATED,
}

data class AssistantContextSettings(
    val enabled: Boolean = false,
    val foregroundWindow: Boolean = true,
    val uiTree: Boolean = true,
    val deviceStatus: Boolean = true,
    val ocrFallback: Boolean = false,
    val usageStats: Boolean = false,
    val notifications: Boolean = false,
    val maxChars: Int = 6_000,
)

data class ContextRequest(
    val commandOrigin: CommandOrigin,
    val toolCallOrigin: ToolCallOrigin,
    val invocationSurface: ContextInvocationSurface,
    val assistantId: String,
    val conversationId: String,
    val runId: String,
    val commandId: String,
    val isHeadless: Boolean,
    val isSubAgent: Boolean,
    val targetDisplaySessionId: String?,
    val settings: AssistantContextSettings,
    val allowedSources: Set<ContextSource>,
)

data class ContextFragment(
    val source: ContextSource,
    val text: String,
    val provider: String? = null,
    val validNodeCount: Int = 0,
    val nonSensitiveCharacterCount: Int = text.length,
)

data class ContextOmission(
    val source: ContextSource,
    val reason: ContextOmissionReason,
    val detailCode: String? = null,
)

data class ContextSourceTiming(
    val source: ContextSource,
    val durationNs: Long,
)

data class ContextSnapshot(
    val runId: String,
    val fragments: List<ContextFragment>,
    val omissions: List<ContextOmission>,
    val collectedAtMs: Long,
    val sourceTimings: List<ContextSourceTiming> = emptyList(),
) {
    val totalCharacters: Int = fragments.sumOf { it.text.length }

    /** Volatile provider context only. Callers must never persist this string as a message. */
    fun toSystemAddendum(): String? {
        if (fragments.isEmpty()) return null
        return buildString {
            appendLine("<volatile_device_context trust=\"untrusted_observation\">")
            fragments.forEach { fragment ->
                append("<source type=\"")
                append(fragment.source.name.lowercase())
                appendLine("\">")
                appendLine(fragment.text.escapeXml())
                appendLine("</source>")
            }
            append("</volatile_device_context>")
        }
    }
}

private fun String.escapeXml(): String = buildString(length) {
    this@escapeXml.forEach { character ->
        append(
            when (character) {
                '&' -> "&amp;"
                '<' -> "&lt;"
                '>' -> "&gt;"
                '"' -> "&quot;"
                '\'' -> "&apos;"
                else -> character
            }
        )
    }
}

fun interface ContextBroker {
    suspend fun collect(request: ContextRequest): ContextSnapshot
}

fun interface ContextSourceReader {
    suspend fun read(request: ContextRequest, source: ContextSource): ContextReadResult
}

sealed interface ContextReadResult {
    data class Available(val fragment: ContextFragment) : ContextReadResult
    data class Unavailable(val detailCode: String? = null) : ContextReadResult
}
