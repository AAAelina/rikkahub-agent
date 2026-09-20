package me.rerere.rikkahub.data.db.entity

import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey
import kotlin.uuid.Uuid

/**
 * Narrow, append-only handoff row in the shared authority database. The table is registered in
 * the final unpublished v46 schema so source terminal transitions and their handoff event can
 * commit atomically.
 */
@Entity(
    tableName = "learning_outbox",
    indices = [
        Index(value = ["event_id"], unique = true),
        Index(value = ["stream_id", "seq"]),
        Index(value = ["event_type"]),
        Index(value = ["source_type", "source_id"]),
    ],
)
data class LearningOutboxEntity(
    @PrimaryKey(autoGenerate = true)
    val seq: Long = 0,
    @ColumnInfo(name = "stream_id")
    val streamId: String,
    @ColumnInfo(name = "event_id")
    val eventId: String,
    @ColumnInfo(name = "event_type")
    val eventType: String,
    @ColumnInfo(name = "event_schema_version")
    val eventSchemaVersion: Int,
    @ColumnInfo(name = "terminal_state")
    val terminalState: String?,
    @ColumnInfo(name = "source_type")
    val sourceType: String?,
    @ColumnInfo(name = "source_id")
    val sourceId: String?,
    @ColumnInfo(name = "source_revision")
    val sourceRevision: Long?,
    /** Prior authority revision for monotonic edit/delete/branch invalidation. */
    @ColumnInfo(name = "previous_source_revision")
    val previousSourceRevision: Long? = null,
    /** Typed source lifecycle state (ACTIVE/SUPERSEDED/TOMBSTONED); never source text. */
    @ColumnInfo(name = "source_state")
    val sourceState: String? = null,
    @ColumnInfo(name = "missing_revision_reason")
    val missingRevisionReason: String?,
    @ColumnInfo(name = "scope_kind")
    val scopeKind: String?,
    @ColumnInfo(name = "scope_id")
    val scopeId: String?,
    @ColumnInfo(name = "conversation_id")
    val conversationId: String?,
    /** Exact selected Conversation authority head observed by the owning commit. */
    @ColumnInfo(name = "conversation_source_revision")
    val conversationSourceRevision: Long? = null,
    @ColumnInfo(name = "command_id")
    val commandId: String?,
    @ColumnInfo(name = "lineage_id")
    val lineageId: String?,
    @ColumnInfo(name = "parent_command_id")
    val parentCommandId: String?,
    @ColumnInfo(name = "branch_anchor_message_id")
    val branchAnchorMessageId: String?,
    @ColumnInfo(name = "branch_anchor_message_revision")
    val branchAnchorMessageRevision: Long? = null,
    /** Typed command boundary; terminal state by itself cannot prove a final model save. */
    @ColumnInfo(name = "completion_kind")
    val completionKind: String? = null,
    @ColumnInfo(name = "generation_run_id")
    val generationRunId: String?,
    @ColumnInfo(name = "execution_id")
    val executionId: String?,
    @ColumnInfo(name = "tool_call_id")
    val toolCallId: String?,
    @ColumnInfo(name = "tool_name")
    val toolName: String? = null,
    @ColumnInfo(name = "tool_schema_fingerprint")
    val toolSchemaFingerprint: String? = null,
    @ColumnInfo(name = "message_id")
    val messageId: String?,
    @ColumnInfo(name = "message_revision")
    val messageRevision: Long? = null,
    /** Typed reward metadata; no feedback text or reconstructed model content is persisted. */
    @ColumnInfo(name = "reward_dimension")
    val rewardDimension: String? = null,
    @ColumnInfo(name = "reward_signal_kind")
    val rewardSignalKind: String? = null,
    @ColumnInfo(name = "reward_value_milli")
    val rewardValueMilli: Int? = null,
    /** Reserved v3 execution verification proof state; existing v1/v2 writers remain null. */
    @ColumnInfo(name = "execution_verification_state")
    val executionVerificationState: String? = null,
    @ColumnInfo(name = "occurred_at_ms")
    val occurredAtMs: Long?,
    @ColumnInfo(name = "created_at_ms")
    val createdAtMs: Long,
) {
    init {
        // Keep semantic/cross-field validation in LearningOutboxRowDecoder so an imported row can
        // be rejected with a bounded error code. The entity still refuses unsafe scalar values.
        require(seq >= 0L) { "Negative learning outbox sequence" }
        require(runCatching { Uuid.parse(streamId) }.isSuccess) { "Invalid learning stream ID" }
        require(isSafeLearningOutboxEventIdentity(eventId)) {
            "Invalid learning event ID"
        }
        require(eventType.isSafeLearningStorageCode()) { "Invalid learning event type" }
        require(eventSchemaVersion > 0) { "Invalid learning event schema version" }
        listOfNotNull(
            terminalState,
            sourceType,
            sourceState,
            missingRevisionReason,
            scopeKind,
            completionKind,
            rewardDimension,
            rewardSignalKind,
            executionVerificationState,
        ).forEach { code ->
            require(code.isSafeLearningStorageCode()) { "Invalid learning outbox code" }
        }
        listOfNotNull(
            sourceId,
            scopeId,
            conversationId,
            commandId,
            lineageId,
            parentCommandId,
            branchAnchorMessageId,
            generationRunId,
            executionId,
            toolCallId,
            toolName,
            toolSchemaFingerprint,
            messageId,
        ).forEach { id ->
            require(id.isSafeLearningStorageId(MAX_REFERENCE_ID_CHARS)) {
                "Invalid learning outbox reference ID"
            }
        }
        require(sourceRevision == null || sourceRevision > 0L) { "Invalid source revision" }
        require(previousSourceRevision == null || previousSourceRevision > 0L) {
            "Invalid previous source revision"
        }
        require(
            previousSourceRevision == null ||
                (sourceRevision != null && previousSourceRevision < sourceRevision),
        ) { "Previous source revision requires a newer current revision" }
        require(branchAnchorMessageRevision == null || branchAnchorMessageRevision > 0L) {
            "Invalid branch anchor revision"
        }
        require(conversationSourceRevision == null || conversationSourceRevision > 0L) {
            "Invalid Conversation source revision"
        }
        require(messageRevision == null || messageRevision > 0L) {
            "Invalid message revision"
        }
        require(rewardValueMilli == null || rewardValueMilli in -1000..1000) {
            "Invalid reward value"
        }
        require((toolName == null) == (toolSchemaFingerprint == null)) {
            "Outbox tool identity requires a name/fingerprint pair"
        }
        require(occurredAtMs == null || occurredAtMs >= 0L) { "Negative occurrence time" }
        require(createdAtMs >= 0L) { "Negative outbox creation time" }
    }

    override fun toString(): String =
        "LearningOutboxEntity(seq=$seq, schema=$eventSchemaVersion, " +
            "terminal=${terminalState != null}, source=${sourceId != null}, " +
            "scope=${scopeKind != null}, reward=${rewardDimension != null}, " +
            "codes-and-ids=<redacted>)"
}

private const val MAX_EVENT_ID_CHARS = 160
private const val MAX_REFERENCE_ID_CHARS = 256

private fun String.isSafeLearningStorageCode(): Boolean =
    length in 1..64 && first() in 'A'..'Z' && all { it in 'A'..'Z' || it in '0'..'9' || it == '_' }

private fun String.isSafeLearningStorageId(maxChars: Int): Boolean =
    length in 1..maxChars && all { char ->
        char in 'a'..'z' ||
            char in 'A'..'Z' ||
            char in '0'..'9' ||
            char == '-' ||
            char == '_' ||
            char == '.' ||
            char == ':' ||
            char == '@'
    }

/** Shared scalar identity contract for entity writes and raw backup/restore validation. */
internal fun isSafeLearningOutboxEventIdentity(value: String): Boolean =
    value.isSafeLearningStorageId(MAX_EVENT_ID_CHARS)
