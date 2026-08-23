package me.rerere.rikkahub.memory.dreaming.source

import androidx.room.withTransaction
import java.nio.charset.StandardCharsets
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toInstant
import kotlinx.serialization.decodeFromString
import me.rerere.ai.ui.UIMessage
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.entity.ConversationEntity
import me.rerere.rikkahub.data.db.entity.MemorySourceTombstoneEntity
import me.rerere.rikkahub.data.db.entity.MessageNodeEntity
import me.rerere.rikkahub.memory.MemorySourceKind
import me.rerere.rikkahub.memory.dreaming.model.DreamPairScopeId
import me.rerere.rikkahub.memory.isValidMemoryScopeBinding
import me.rerere.rikkahub.memory.memoryCaptureSourcesForMessage
import me.rerere.rikkahub.memory.memorySourceTextDigest
import me.rerere.rikkahub.utils.JsonInstant

/**
 * Re-reads only persisted, currently selected host messages named by the caller's locators.
 *
 * The Room transaction copies a coherent set of raw rows and ends before JSON decoding or any
 * later model call. Exact source digests and the synthesis commit fence detect changes after this
 * read; no conversation text or opaque model token is ever used to construct a database query.
 */
class RoomDreamSourceReader(
    private val database: AppDatabase,
) : DreamSourceReader {
    override suspend fun read(request: DreamSourceReadRequest): List<DreamSourceReadResult> {
        if (request.locators.isEmpty()) return emptyList()
        val snapshots = loadSnapshots(request)
        // DreamSourceReadRequest validates this strict IANA identifier. Never fall back to the
        // device's mutable default zone: one run must resolve source time identically after a
        // process restart or a system-timezone change.
        val timeZone = TimeZone.of(request.sourceTimezoneId)
        val resolved = mutableMapOf<DreamSourceLocator, UnbudgetedRead>()
        var consumedBytes = 0

        return request.locators.map { locator ->
            val read = resolved.getOrPut(locator) {
                resolve(locator, snapshots[locator.conversationId], timeZone)
            }
            when (read) {
                is UnbudgetedRead.Unavailable -> DreamSourceReadResult.Unavailable(
                    locator = locator,
                    reason = read.reason,
                )

                is UnbudgetedRead.Found -> {
                    val itemBytes = read.text.toByteArray(StandardCharsets.UTF_8).size
                    if (read.text.length > MAX_DREAM_SOURCE_TEXT_CHARS ||
                        itemBytes > request.maxTotalUtf8Bytes - consumedBytes
                    ) {
                        DreamSourceReadResult.Unavailable(
                            locator = locator,
                            reason = DreamSourceUnavailableReason.BUDGET_EXCEEDED,
                        )
                    } else {
                        consumedBytes += itemBytes
                        DreamSourceReadResult.Found(
                            locator = locator,
                            text = read.text,
                            sourceTimestampEpochMs = read.sourceTimestampEpochMs,
                            consumedTextDigest = locator.expectedConsumedTextDigest,
                        )
                    }
                }
            }
        }
    }

    private suspend fun loadSnapshots(
        request: DreamSourceReadRequest,
    ): Map<String, ConversationSnapshot> = database.withTransaction {
        val conversationIds = request.locators.asSequence()
            .map(DreamSourceLocator::conversationId)
            .distinct()
            .sorted()
            .toList()
        val result = linkedMapOf<String, ConversationSnapshot>()
        for (conversationId in conversationIds) {
            result[conversationId] = ConversationSnapshot(
                conversation = database.conversationDao().getConversationById(conversationId),
                nodes = database.messageNodeDao().getNodesOfConversation(conversationId),
                tombstones = database.memoryV2Dao().getSourceTombstones(
                    scopeId = request.scopeId.value,
                    conversationId = conversationId,
                ),
            )
        }
        result
    }

    private fun resolve(
        locator: DreamSourceLocator,
        snapshot: ConversationSnapshot?,
        timeZone: TimeZone,
    ): UnbudgetedRead {
        if (locator.sourceKind != MemorySourceKind.TEXT) {
            return UnbudgetedRead.Unavailable(DreamSourceUnavailableReason.UNSUPPORTED_KIND)
        }
        snapshot ?: return UnbudgetedRead.Unavailable(DreamSourceUnavailableReason.MISSING)
        if (locator.isTombstonedBy(snapshot.tombstones)) {
            return UnbudgetedRead.Unavailable(DreamSourceUnavailableReason.TOMBSTONED)
        }
        val conversation = snapshot.conversation
            ?: return UnbudgetedRead.Unavailable(DreamSourceUnavailableReason.MISSING)
        val validScope = DreamPairScopeId.parseOrNull(locator.scopeId.value)?.let { pair ->
            runCatching { kotlin.uuid.Uuid.parse(conversation.assistantId) }
                .getOrNull()
                ?.let(DreamPairScopeId::forAssistant) == pair
        } ?: isValidMemoryScopeBinding(locator.scopeId.value, conversation.assistantId)
        if (!validScope) {
            return UnbudgetedRead.Unavailable(DreamSourceUnavailableReason.SCOPE_MISMATCH)
        }

        val selectedMessages = snapshot.nodes.mapNotNull(::selectedMessage)
        val matchingIdAndRole = selectedMessages.asSequence().flatMap { message ->
            memoryCaptureSourcesForMessage(message).asSequence().map { source -> message to source }
        }.filter { (_, source) ->
            source.messageId == locator.messageId && source.role == locator.role
        }.toList()
        if (matchingIdAndRole.isEmpty()) {
            return UnbudgetedRead.Unavailable(DreamSourceUnavailableReason.MISSING)
        }
        val exact = matchingIdAndRole.firstOrNull { (_, source) ->
            memorySourceTextDigest(source.text) == locator.expectedConsumedTextDigest.value
        } ?: return UnbudgetedRead.Unavailable(DreamSourceUnavailableReason.DIGEST_MISMATCH)
        val sourceTimestampEpochMs = try {
            exact.first.createdAt.toInstant(timeZone).toEpochMilliseconds()
        } catch (_: Exception) {
            null
        }?.takeIf { it >= 0L }
            ?: return UnbudgetedRead.Unavailable(DreamSourceUnavailableReason.MISSING)
        return UnbudgetedRead.Found(
            text = exact.second.text,
            sourceTimestampEpochMs = sourceTimestampEpochMs,
        )
    }

    private fun selectedMessage(node: MessageNodeEntity): UIMessage? = try {
        JsonInstant.decodeFromString<List<UIMessage>>(node.messages).getOrNull(node.selectIndex)
    } catch (_: Exception) {
        null
    }

    private data class ConversationSnapshot(
        val conversation: ConversationEntity?,
        val nodes: List<MessageNodeEntity>,
        val tombstones: List<MemorySourceTombstoneEntity>,
    )

    private sealed interface UnbudgetedRead {
        data class Found(
            val text: String,
            val sourceTimestampEpochMs: Long,
        ) : UnbudgetedRead

        data class Unavailable(val reason: DreamSourceUnavailableReason) : UnbudgetedRead
    }
}

private fun DreamSourceLocator.isTombstonedBy(
    tombstones: List<MemorySourceTombstoneEntity>,
): Boolean = tombstones.any { tombstone ->
    tombstone.scopeId == scopeId.value &&
        tombstone.conversationId == conversationId &&
        when (tombstone.sourceKind) {
            SOURCE_TOMBSTONE_CONVERSATION -> tombstone.sourceId == conversationId
            SOURCE_TOMBSTONE_MESSAGE -> tombstone.sourceId == messageId &&
                (tombstone.sourceDigest.isEmpty() ||
                    tombstone.sourceDigest == expectedConsumedTextDigest.value)
            else -> false
        }
}

private const val SOURCE_TOMBSTONE_CONVERSATION = "CONVERSATION"
private const val SOURCE_TOMBSTONE_MESSAGE = "MESSAGE"
private const val MAX_DREAM_SOURCE_TEXT_CHARS = 128_000
