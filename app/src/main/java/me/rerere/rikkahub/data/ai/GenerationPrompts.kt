package me.rerere.rikkahub.data.ai

import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.put
import me.rerere.rikkahub.data.model.Assistant
import me.rerere.rikkahub.data.model.AssistantMemory
import me.rerere.rikkahub.data.repository.ConversationRepository
import me.rerere.rikkahub.utils.JsonInstantPretty
import me.rerere.rikkahub.utils.toLocalDate

private const val MAX_USER_NICKNAME_PROMPT_CHARS = 128
internal const val DEFAULT_MEMORY_PROMPT_MAX_TOKENS = DEFAULT_RECALL_PROMPT_MAX_TOKENS
internal const val MEMORY_PROMPT_COMPILER_REVISION = RECALL_PROMPT_COMPILER_REVISION

internal enum class MemoryPromptSection {
    STANDING,
    CONTEXTUAL,
}

internal enum class MemoryPromptDropReason {
    INVALID_BUDGET,
    CONTEXTUAL_DISABLED,
    DUPLICATE_ID,
    BUDGET_EXCEEDED,
}

internal data class MemoryPromptDrop(
    val memoryId: Int,
    val section: MemoryPromptSection,
    val reason: MemoryPromptDropReason,
)

internal data class MemoryPromptCompileResult(
    val text: String,
    val actualStandingIds: List<Int>,
    val actualContextualIds: List<Int>,
    val estimatedTokens: Int,
    val dropped: List<MemoryPromptDrop>,
    val compilerRevision: String = MEMORY_PROMPT_COMPILER_REVISION,
) {
    /** Runtime-only ids for lastAccess bookkeeping; persisted diagnostics must redact them. */
    val actualIncludedIds: List<Int>
        get() = actualStandingIds + actualContextualIds
}

internal fun buildUserIdentityPrompt(userNickname: String): String {
    val preferredName = userNickname.trim().take(MAX_USER_NICKNAME_PROMPT_CHARS)
    if (preferredName.isEmpty()) return ""
    val encodedName = JsonPrimitive(preferredName).toString()
    return """
        **User identity metadata (optional form of address)**
        The user's preferred name is the JSON string $encodedName.
        This is metadata for a natural direct form of address, not a request to mention the name.
        Use it only when addressing the user naturally; do not repeat it, output it by itself,
        or use it as a substitute for answering the user's request.
        When the user asks a task or question, answer that task directly and omit the name unless
        a direct form of address is genuinely useful.
        Never use "用户", "USER", "user", or similar internal role labels as the person's name or direct form of address.
        This is a user-owned preference, not untrusted conversation or tool content.
    """.trimIndent()
}

internal fun buildMemoryPrompt(
    memories: List<AssistantMemory>,
    includeContextual: Boolean = true,
    maxChars: Int = me.rerere.rikkahub.data.repository.DEFAULT_MEMORY_PROMPT_MAX_CHARS,
): String = compileMemoryPrompt(
    memories = memories,
    includeContextual = includeContextual,
    maxTokens = DEFAULT_MEMORY_PROMPT_MAX_TOKENS,
    maxChars = maxChars,
).text

/**
 * Packs complete memory records under one deterministic budget. A record is either represented by
 * one complete JSON object or absent; content, UTF-16 surrogate pairs and negations are never cut.
 * The caller must still bound [maxTokens] by the request's trusted total-context allocation.
 */
internal fun compileMemoryPrompt(
    memories: List<AssistantMemory>,
    includeContextual: Boolean = true,
    maxTokens: Int = DEFAULT_MEMORY_PROMPT_MAX_TOKENS,
    maxChars: Int = me.rerere.rikkahub.data.repository.DEFAULT_MEMORY_PROMPT_MAX_CHARS,
    tokenEstimator: (String) -> Int = ::estimateRecallPromptTokens,
): MemoryPromptCompileResult {
    val recall = compileRecallPrompt(
        memory = memories,
        budget = RecallPromptBudget(
            maxTokens = maxTokens,
            maxChars = maxChars,
        ),
        requestPurpose = if (includeContextual) {
            RecallRequestPurpose.NORMAL
        } else {
            RecallRequestPurpose.FINAL_ANSWER_RECOVERY
        },
        includeContextualMemory = includeContextual,
        tokenEstimator = tokenEstimator,
    )
    val memoryDrops = recall.dropped
        .filter { it.source == RecallPromptSource.MEMORY }
        .map { drop ->
            MemoryPromptDrop(
                memoryId = drop.id.toInt(),
                section = when (drop.section) {
                    RecallPromptSection.STANDING_MEMORY -> MemoryPromptSection.STANDING
                    RecallPromptSection.CONTEXTUAL_MEMORY -> MemoryPromptSection.CONTEXTUAL
                    else -> error("Non-memory section in a Memory drop")
                },
                reason = when (drop.reason) {
                    RecallPromptDropReason.INVALID_BUDGET -> MemoryPromptDropReason.INVALID_BUDGET
                    RecallPromptDropReason.CONTEXTUAL_DISABLED ->
                        MemoryPromptDropReason.CONTEXTUAL_DISABLED
                    RecallPromptDropReason.DUPLICATE_ID -> MemoryPromptDropReason.DUPLICATE_ID
                    else -> MemoryPromptDropReason.BUDGET_EXCEEDED
                },
            )
        }
    val actualStandingIds = recall.manifest.actualMemoryItems
        .filter { it.section == RecallPromptSection.STANDING_MEMORY }
        .map { it.id.toInt() }
    val actualContextualIds = recall.manifest.actualMemoryItems
        .filter { it.section == RecallPromptSection.CONTEXTUAL_MEMORY }
        .map { it.id.toInt() }
    return MemoryPromptCompileResult(
        text = recall.text,
        actualStandingIds = actualStandingIds,
        actualContextualIds = actualContextualIds,
        estimatedTokens = recall.estimatedTokens,
        dropped = memoryDrops,
        compilerRevision = recall.compilerRevision,
    )
}

internal suspend fun buildRecentChatsPrompt(
    assistant: Assistant,
    conversationRepo: ConversationRepository
): String {
    val recentConversations = conversationRepo.getRecentConversations(
        assistantId = assistant.id,
        limit = 10,
    )
    if (recentConversations.isNotEmpty()) {
        return buildString {
            appendLine()
            append("**Recent Chats**")
            appendLine()
            append("These are some of the user's recent conversations. You can use them to understand user preferences:")
            appendLine()
            val json = buildJsonArray {
                recentConversations.forEach { conversation ->
                    add(buildJsonObject {
                        put("title", conversation.title)
                        put("last_chat", conversation.updateAt.toLocalDate())
                    })
                }
            }
            append(JsonInstantPretty.encodeToString(json))
            appendLine()
        }
    }
    return ""
}
