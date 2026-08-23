package me.rerere.rikkahub.memory.dreaming.experience

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toInstant
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import me.rerere.ai.core.MessageRole
import me.rerere.rikkahub.data.db.dao.MemoryDAO
import me.rerere.rikkahub.data.db.entity.MemoryEntity
import me.rerere.rikkahub.data.repository.ConversationRepository
import me.rerere.rikkahub.memory.MemorySourceRole
import me.rerere.rikkahub.memory.memoryCaptureSourcesForMessage
import me.rerere.rikkahub.memory.memoryExtractionText
import me.rerere.rikkahub.memory.dreaming.model.DreamPairScope
import me.rerere.rikkahub.memory.dreaming.runtime.DreamSynthesisCoordinator

class ConversationEpisodeAdapter(
    private val store: RoomDreamExperienceStore,
) {
    suspend fun ingest(
        assistantId: kotlin.uuid.Uuid,
        conversationId: kotlin.uuid.Uuid,
        userMessageId: kotlin.uuid.Uuid,
        assistantMessageId: kotlin.uuid.Uuid,
        userText: String,
        assistantText: String,
        occurredAtMs: Long,
    ): DreamExperienceIngestResult {
        val pair = DreamPairScope.forAssistant(assistantId)
        val sourceRef = "conversation:$conversationId:$userMessageId:$assistantMessageId"
        val digest = sha256("$userText\n\u0000\n$assistantText")
        val summary = buildString {
            append("斯啾伊：")
            append(userText.compactForDream())
            append("\n七七：")
            append(assistantText.compactForDream())
        }.take(MAX_EXPERIENCE_SUMMARY_CHARS)
        val manifest = Json.encodeToString(
            listOf(
                DreamExperienceSourceManifestEntry(
                    conversationId = conversationId.toString(),
                    messageId = userMessageId.toString(),
                    role = "USER",
                    consumedTextDigest = sha256(userText),
                    evidenceGroupId = sourceRef,
                ),
                DreamExperienceSourceManifestEntry(
                    conversationId = conversationId.toString(),
                    messageId = assistantMessageId.toString(),
                    role = "ASSISTANT",
                    consumedTextDigest = sha256(assistantText),
                    evidenceGroupId = sourceRef,
                ),
            ),
        )
        return store.ingest(
            DreamExperienceRecord(
                id = stableExperienceId(pair.id.value, sourceRef),
                pairScopeId = pair.id,
                sourceKind = "CONVERSATION",
                sourceRef = sourceRef,
                conversationId = conversationId.toString(),
                occurredAtMs = occurredAtMs,
                actor = "SHARED",
                experienceKind = "CHAT_EPISODE",
                summary = summary,
                salience = 0.10,
                novelty = 0.50,
                identityWeight = 0.12,
                relationshipWeight = 0.15,
                confidence = 1.0,
                contentDigest = digest,
                sourceManifestJson = manifest,
            ),
            nowMs = occurredAtMs,
        )
    }
}

class DreamMemoryAdapter(
    private val memoryDao: MemoryDAO,
    private val store: RoomDreamExperienceStore,
) {
    suspend fun ingestConfirmedMemories(
        assistantId: kotlin.uuid.Uuid,
        memoryScopeId: String,
        nowMs: Long,
        limit: Int = 128,
    ): Int {
        val pair = DreamPairScope.forAssistant(assistantId)
        var inserted = 0
        memoryDao.getActiveConfirmedMemoriesForDream(memoryScopeId, nowMs, limit).forEach { memory ->
            val sourceRef = "memory:$memoryScopeId:${memory.id}:${memory.revision}"
            if (store.ingest(
                DreamExperienceRecord(
                    id = stableExperienceId(pair.id.value, sourceRef),
                    pairScopeId = pair.id,
                    sourceKind = "MEMORY",
                    sourceRef = sourceRef,
                    conversationId = memory.sourceConversationId,
                    occurredAtMs = memory.occurredAtMs ?: memory.updatedAtMs,
                    actor = memory.attribution,
                    experienceKind = memory.memoryKind,
                    summary = memory.toDreamSummary(),
                    salience = memory.importance.toDouble().coerceIn(0.0, 1.0),
                    novelty = 0.35,
                    identityWeight = memory.identityWeight(),
                    relationshipWeight = if (memory.memoryKind == "RELATIONSHIP") 0.9 else 0.2,
                    confidence = memory.confidence.toDouble().coerceIn(0.0, 1.0),
                    contentDigest = memory.contentHash.takeIf(SHA256::matches) ?: sha256(memory.content),
                    sourceManifestJson = JsonObject(
                        mapOf(
                            "memory_id" to JsonPrimitive(memory.id),
                            "memory_revision" to JsonPrimitive(memory.revision),
                            "memory_scope_id" to JsonPrimitive(memoryScopeId),
                        ),
                    ).toString(),
                ),
                nowMs = nowMs,
            ) is DreamExperienceIngestResult.Inserted) {
                inserted += 1
            }
        }
        return inserted
    }
}

@Serializable
data class DreamExperienceSourceManifestEntry(
    val conversationId: String,
    val messageId: String,
    val role: String,
    val sourceKind: String = "TEXT",
    val consumedTextDigest: String,
    val evidenceGroupId: String,
)

class DreamExperienceIngestor(
    private val conversationAdapter: ConversationEpisodeAdapter,
    private val memoryAdapter: DreamMemoryAdapter,
    private val store: RoomDreamExperienceStore,
    private val conversationRepository: ConversationRepository,
    private val synthesisCoordinator: DreamSynthesisCoordinator,
) {
    suspend fun ingestCompletedTurn(
        assistantId: kotlin.uuid.Uuid,
        conversationId: kotlin.uuid.Uuid,
        userMessageId: kotlin.uuid.Uuid,
        assistantMessageId: kotlin.uuid.Uuid,
        userText: String,
        assistantText: String,
        memoryScopeId: String,
        nowMs: Long,
    ) {
        val historyInserted = runCatching {
            backfillHistoryIfNeeded(assistantId, memoryScopeId, nowMs)
        }
            .getOrDefault(false)
        val result = conversationAdapter.ingest(
            assistantId = assistantId,
            conversationId = conversationId,
            userMessageId = userMessageId,
            assistantMessageId = assistantMessageId,
            userText = userText,
            assistantText = assistantText,
            occurredAtMs = nowMs,
        )
        if (historyInserted || result is DreamExperienceIngestResult.Inserted) {
            synthesisCoordinator.onAuthorityCommitted()
        }
    }

    private suspend fun backfillHistoryIfNeeded(
        assistantId: kotlin.uuid.Uuid,
        memoryScopeId: String,
        nowMs: Long,
    ): Boolean {
        val pair = DreamPairScope.forAssistant(assistantId)
        if (store.ensureState(pair.id, nowMs).historyBackfilledAtMs != null) return false
        var inserted = false
        val zone = TimeZone.currentSystemDefault()
        conversationRepository.getConversationIdsOfAssistant(assistantId).forEach conversationLoop@ { conversationId ->
            val conversation = conversationRepository.getConversationById(conversationId)
                ?: return@conversationLoop
            var pendingUser: me.rerere.ai.ui.UIMessage? = null
            conversation.currentMessages.forEach messageLoop@ { message ->
                when (message.role) {
                    MessageRole.USER -> pendingUser = message
                    MessageRole.ASSISTANT -> {
                        val userMessage = pendingUser ?: return@messageLoop
                        pendingUser = null
                        val sourceMessages =
                            memoryCaptureSourcesForMessage(userMessage) + memoryCaptureSourcesForMessage(message)
                        val userText = memoryExtractionText(sourceMessages, setOf(MemorySourceRole.USER))
                        val assistantText = memoryExtractionText(
                            sourceMessages,
                            setOf(MemorySourceRole.ASSISTANT, MemorySourceRole.TOOL),
                        )
                        if (userText.isBlank() || assistantText.isBlank()) return@messageLoop
                        val result = conversationAdapter.ingest(
                            assistantId = assistantId,
                            conversationId = conversation.id,
                            userMessageId = userMessage.id,
                            assistantMessageId = message.id,
                            userText = userText,
                            assistantText = assistantText,
                            occurredAtMs = message.createdAt.toInstant(zone).toEpochMilliseconds(),
                        )
                        if (result is DreamExperienceIngestResult.Inserted) inserted = true
                    }
                    else -> Unit
                }
            }
        }
        if (memoryAdapter.ingestConfirmedMemories(
                assistantId = assistantId,
                memoryScopeId = memoryScopeId,
                nowMs = nowMs,
            ) > 0
        ) {
            inserted = true
        }
        store.markHistoryBackfilled(pair.id, nowMs)
        return inserted
    }

    suspend fun syncConfirmedMemories(
        assistantId: kotlin.uuid.Uuid,
        memoryScopeId: String,
        nowMs: Long,
    ) {
        if (memoryAdapter.ingestConfirmedMemories(
            assistantId = assistantId,
            memoryScopeId = memoryScopeId,
            nowMs = nowMs,
        ) > 0) synthesisCoordinator.onAuthorityCommitted()
    }
}

private fun MemoryEntity.toDreamSummary(): String = buildString {
    title?.takeIf(String::isNotBlank)?.let {
        append(it.trim())
        append("：")
    }
    append(content.trim())
}.take(MAX_EXPERIENCE_SUMMARY_CHARS)

private fun MemoryEntity.identityWeight(): Double = when (memoryKind) {
    "USER_PROFILE", "PREFERENCE", "LONG_TERM_GOAL", "WORKING_CONSTRAINT" -> 0.8
    "RELATIONSHIP", "EPISODE", "DECISION", "INSIGHT", "THEORY" -> 0.6
    else -> 0.25
}

private fun String.compactForDream(): String = trim().replace(Regex("\\s+"), " ")
    .take(MAX_EPISODE_SIDE_CHARS)

private fun stableExperienceId(pairScopeId: String, sourceRef: String): String =
    UUID.nameUUIDFromBytes("$pairScopeId\n$sourceRef".toByteArray(StandardCharsets.UTF_8)).toString()

private fun sha256(text: String): String = MessageDigest.getInstance("SHA-256")
    .digest(text.toByteArray(StandardCharsets.UTF_8))
    .joinToString("") { byte -> "%02x".format(byte) }

private const val MAX_EPISODE_SIDE_CHARS = 900
private const val MAX_EXPERIENCE_SUMMARY_CHARS = 2_000
private val SHA256 = Regex("^[0-9a-f]{64}$")
