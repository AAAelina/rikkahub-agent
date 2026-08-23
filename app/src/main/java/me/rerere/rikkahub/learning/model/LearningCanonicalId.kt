package me.rerere.rikkahub.learning.model

import java.security.MessageDigest
import kotlin.uuid.Uuid

private const val MAX_CANONICAL_FIELDS = 64
private const val MAX_CANONICAL_FIELD_BYTES = 4_096
private val HEX = "0123456789abcdef".toCharArray()

/** Versioned, length-prefixed SHA-256; field boundaries cannot collide by string concatenation. */
object LearningCanonicalId {
    fun digest(domainVersion: String, fields: List<String?>): String {
        require(isCanonicalDomain(domainVersion)) { "Invalid canonical ID domain" }
        require(fields.size <= MAX_CANONICAL_FIELDS) { "Too many canonical ID fields" }

        val digest = MessageDigest.getInstance("SHA-256")
        digest.updateLengthPrefixed(domainVersion.encodeToByteArray())
        fields.forEach { field ->
            if (field == null) {
                digest.updateInt(-1)
            } else {
                val bytes = field.encodeToByteArray()
                require(bytes.size <= MAX_CANONICAL_FIELD_BYTES) { "Canonical ID field too large" }
                digest.updateLengthPrefixed(bytes)
            }
        }
        return digest.digest().toLowerHex()
    }

    fun eventId(
        streamId: Uuid,
        eventType: LearningEventType,
        eventSchemaVersion: Int,
        sourceKindCode: String,
        sourceId: String,
        sourceRevision: Long?,
        terminalState: String?,
        previousSourceRevision: Long? = null,
        sourceStateCode: String? = null,
        correlation: LearningCorrelation? = null,
        rewardDimensionCode: String? = null,
        rewardSignalKindCode: String? = null,
        rewardValueMilli: Int? = null,
        executionVerificationStateCode: String? = null,
    ): String {
        require(isSafeLearningIdentifier(sourceId, 256)) { "Invalid source identifier" }
        require(eventSchemaVersion > 0) { "Invalid event schema version" }
        require(sourceKindCode.matches(Regex("[A-Z][A-Z0-9_]{0,63}"))) {
            "Invalid source kind code"
        }
        require(sourceRevision == null || sourceRevision >= 0L) { "Negative source revision" }
        require(terminalState == null || terminalState.matches(Regex("[A-Z][A-Z0-9_]{0,63}"))) {
            "Invalid terminal state"
        }
        if (eventSchemaVersion == 1) {
            require(
                rewardDimensionCode == null && rewardSignalKindCode == null &&
                    rewardValueMilli == null && executionVerificationStateCode == null,
            ) { "Schema v1 cannot carry v3 metadata" }
            return "learning-event-v1:" + digest(
                domainVersion = "learning-event-v1",
                fields = listOf(
                    streamId.toString(),
                    eventType.name,
                    eventSchemaVersion.toString(),
                    sourceKindCode,
                    sourceId,
                    sourceRevision?.toString(),
                    terminalState,
                ),
            )
        }
        require(eventSchemaVersion >= 2) { "Unsupported canonical event schema" }
        require(previousSourceRevision == null || previousSourceRevision > 0L)
        require(sourceStateCode == null || sourceStateCode.matches(Regex("[A-Z][A-Z0-9_]{0,63}")))
        val v2Fields = listOf(
                streamId.toString(),
                eventType.name,
                eventSchemaVersion.toString(),
                sourceKindCode,
                sourceId,
                sourceRevision?.toString(),
                terminalState,
                previousSourceRevision?.toString(),
                sourceStateCode,
                correlation?.conversationId,
                correlation?.conversationSourceRevision?.toString(),
                correlation?.commandId,
                correlation?.lineageId,
                correlation?.parentCommandId,
                correlation?.branchAnchorMessageId,
                correlation?.branchAnchorMessageRevision?.toString(),
                correlation?.completionKindCode,
                correlation?.generationRunId,
                correlation?.executionId,
                correlation?.toolCallId,
                correlation?.toolName,
                correlation?.toolSchemaFingerprint,
                correlation?.messageId,
                correlation?.messageRevision?.toString(),
            )
        if (eventSchemaVersion == 2) {
            require(
                rewardDimensionCode == null && rewardSignalKindCode == null &&
                    rewardValueMilli == null && executionVerificationStateCode == null,
            ) { "Schema v2 cannot carry v3 metadata" }
            return "learning-event-v2:" + digest(
                domainVersion = "learning-event-v2",
                fields = v2Fields,
            )
        }
        listOfNotNull(
            rewardDimensionCode,
            rewardSignalKindCode,
            executionVerificationStateCode,
        ).forEach { require(it.matches(Regex("[A-Z][A-Z0-9_]{0,63}"))) }
        require(rewardValueMilli == null || rewardValueMilli in -1000..1000)
        return "learning-event-v3:" + digest(
            domainVersion = "learning-event-v3",
            fields = v2Fields + listOf(
                rewardDimensionCode,
                rewardSignalKindCode,
                rewardValueMilli?.toString(),
                executionVerificationStateCode,
            ),
        )
    }

    /**
     * Compatibility identity for schema-v2 SOURCE_INVALIDATED rows when the legacy canonical ID
     * collides across Learning scopes. The original v2 ID intentionally remains unchanged for
     * already-persisted rows; this discriminator is only used as a deterministic collision lane.
     */
    fun scopedSourceInvalidationEventId(
        legacyEventId: String,
        scopeKindCode: String,
        scopeId: String,
    ): String {
        require(legacyEventId.startsWith("learning-event-v2:")) {
            "Scoped invalidation requires a legacy v2 event ID"
        }
        require(scopeKindCode.matches(Regex("[A-Z][A-Z0-9_]{0,63}"))) {
            "Invalid learning scope kind code"
        }
        require(isSafeLearningIdentifier(scopeId, 256)) { "Invalid learning scope identifier" }
        return "learning-source-invalidated-scope-v1:" + digest(
            domainVersion = "learning-source-invalidated-scope-v1",
            fields = listOf(legacyEventId, scopeKindCode, scopeId),
        )
    }

    /**
     * Bounded authority reference for an ExecutionEvent mutation id.
     *
     * Mutation ids may be longer than the handoff source-id contract and may expose runtime
     * naming details. Consumers resolve by `(executionId, sourceRevision)` and verify this digest;
     * they never reverse or parse it.
     */
    fun executionEventSourceId(eventId: String): String {
        require(eventId.isNotEmpty()) { "Execution event ID is empty" }
        require(eventId.encodeToByteArray().size <= MAX_CANONICAL_FIELD_BYTES) {
            "Execution event ID is too large"
        }
        return "execution-event-v1:" + digest(
            domainVersion = "execution-event-v1",
            fields = listOf(eventId),
        )
    }
}

private fun MessageDigest.updateLengthPrefixed(bytes: ByteArray) {
    updateInt(bytes.size)
    update(bytes)
}

private fun MessageDigest.updateInt(value: Int) {
    update((value ushr 24).toByte())
    update((value ushr 16).toByte())
    update((value ushr 8).toByte())
    update(value.toByte())
}

private fun ByteArray.toLowerHex(): String = buildString(size * 2) {
    this@toLowerHex.forEach { byte ->
        val value = byte.toInt() and 0xff
        append(HEX[value ushr 4])
        append(HEX[value and 0x0f])
    }
}

private fun isCanonicalDomain(value: String): Boolean =
    value.isNotEmpty() && value.length <= 64 && value.matches(Regex("[a-z][a-z0-9-]*-v[1-9][0-9]*"))
