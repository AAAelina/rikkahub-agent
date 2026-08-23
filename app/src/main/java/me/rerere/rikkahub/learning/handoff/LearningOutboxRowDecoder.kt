package me.rerere.rikkahub.learning.handoff

import me.rerere.rikkahub.data.db.entity.LearningOutboxEntity
import me.rerere.rikkahub.learning.model.LearningCanonicalId
import me.rerere.rikkahub.learning.model.LearningCorrelation
import me.rerere.rikkahub.learning.model.LearningEventCode
import me.rerere.rikkahub.learning.model.LearningEventContract
import me.rerere.rikkahub.learning.model.LearningEventDecodeState
import me.rerere.rikkahub.learning.model.LearningEventType
import me.rerere.rikkahub.learning.model.LearningScope
import me.rerere.rikkahub.learning.model.LearningSourceKind
import me.rerere.rikkahub.learning.model.LearningSourceRef
import me.rerere.rikkahub.learning.model.MissingSourceRevisionReason
import kotlin.uuid.Uuid

enum class LearningOutboxDecodeError {
    INVALID_STREAM,
    INVALID_EVENT_CODE,
    INVALID_SENTINEL,
    INVALID_SOURCE,
    INVALID_SCOPE,
    INVALID_CORRELATION,
    INVALID_TIME,
    EVENT_ID_MISMATCH,
    INVALID_EVENT_CONTRACT,
}

sealed interface LearningOutboxDecodeResult {
    data class Valid(val event: LearningHandoffEvent) : LearningOutboxDecodeResult

    data class Invalid(val error: LearningOutboxDecodeError) : LearningOutboxDecodeResult
}

/** Converts an imported/database row to a typed event without leaking malformed field contents. */
object LearningOutboxRowDecoder {
    fun decode(row: LearningOutboxEntity): LearningOutboxDecodeResult {
        val streamId = runCatching { Uuid.parse(row.streamId) }.getOrNull()
            ?: return LearningOutboxDecodeResult.Invalid(LearningOutboxDecodeError.INVALID_STREAM)
        val eventCode = runCatching {
            LearningEventCode(row.eventType, row.eventSchemaVersion)
        }.getOrNull()
            ?: return LearningOutboxDecodeResult.Invalid(LearningOutboxDecodeError.INVALID_EVENT_CODE)
        if (
            row.eventType == LearningEventType.STREAM_INIT.name &&
            row.eventSchemaVersion != 1
        ) {
            return LearningOutboxDecodeResult.Invalid(LearningOutboxDecodeError.INVALID_SENTINEL)
        }
        if (
            row.seq <= 0L ||
            row.createdAtMs < 0L ||
            (row.occurredAtMs != null &&
                (row.occurredAtMs < 0L || row.occurredAtMs > row.createdAtMs))
        ) {
            return LearningOutboxDecodeResult.Invalid(LearningOutboxDecodeError.INVALID_TIME)
        }
        val correlation = runCatching {
            LearningCorrelation(
                previousSourceRevision = row.previousSourceRevision,
                sourceStateCode = row.sourceState,
                conversationId = row.conversationId,
                conversationSourceRevision = row.conversationSourceRevision,
                commandId = row.commandId,
                lineageId = row.lineageId,
                parentCommandId = row.parentCommandId,
                branchAnchorMessageId = row.branchAnchorMessageId,
                branchAnchorMessageRevision = row.branchAnchorMessageRevision,
                completionKindCode = row.completionKind,
                generationRunId = row.generationRunId,
                executionId = row.executionId,
                toolCallId = row.toolCallId,
                toolName = row.toolName,
                toolSchemaFingerprint = row.toolSchemaFingerprint,
                messageId = row.messageId,
                messageRevision = row.messageRevision,
            )
        }.getOrNull()
            ?: return LearningOutboxDecodeResult.Invalid(LearningOutboxDecodeError.INVALID_CORRELATION)

        if (eventCode.knownType == LearningEventType.STREAM_INIT) {
            val isCleanSentinel = eventCode.decodeState == LearningEventDecodeState.KNOWN &&
                row.eventId == LEARNING_STREAM_INIT_EVENT_ID &&
                row.sourceType == null &&
                row.sourceId == null &&
                row.sourceRevision == null &&
                row.previousSourceRevision == null &&
                row.sourceState == null &&
                row.missingRevisionReason == null &&
                row.scopeKind == null &&
                row.scopeId == null &&
                row.terminalState == null &&
                row.conversationSourceRevision == null &&
                row.branchAnchorMessageRevision == null &&
                row.completionKind == null &&
                row.toolName == null &&
                row.toolSchemaFingerprint == null &&
                row.messageRevision == null &&
                row.rewardDimension == null &&
                row.rewardSignalKind == null &&
                row.rewardValueMilli == null &&
                row.executionVerificationState == null &&
                row.occurredAtMs == null &&
                correlation == LearningCorrelation()
            if (!isCleanSentinel) {
                return LearningOutboxDecodeResult.Invalid(LearningOutboxDecodeError.INVALID_SENTINEL)
            }
            val sentinel = runCatching {
                LearningHandoffEvent(
                    streamId = streamId,
                    eventId = row.eventId,
                    outboxSeq = row.seq,
                    eventCode = eventCode,
                    source = null,
                    correlation = correlation,
                    createdAtMs = row.createdAtMs,
                )
            }.getOrNull()
                ?: return LearningOutboxDecodeResult.Invalid(
                    LearningOutboxDecodeError.INVALID_SENTINEL,
                )
            return LearningOutboxDecodeResult.Valid(sentinel)
        }

        val sourceTypeCode = row.sourceType
            ?: return LearningOutboxDecodeResult.Invalid(LearningOutboxDecodeError.INVALID_SOURCE)
        if (!sourceTypeCode.matches(Regex("[A-Z][A-Z0-9_]{0,63}"))) {
            return LearningOutboxDecodeResult.Invalid(LearningOutboxDecodeError.INVALID_SOURCE)
        }
        val sourceId = row.sourceId
            ?: return LearningOutboxDecodeResult.Invalid(LearningOutboxDecodeError.INVALID_SOURCE)
        val occurredAtMs = row.occurredAtMs
            ?: return LearningOutboxDecodeResult.Invalid(LearningOutboxDecodeError.INVALID_TIME)
        val scopeKind = row.scopeKind
            ?: return LearningOutboxDecodeResult.Invalid(LearningOutboxDecodeError.INVALID_SCOPE)
        val scopeId = row.scopeId
            ?: return LearningOutboxDecodeResult.Invalid(LearningOutboxDecodeError.INVALID_SCOPE)
        val scope = LearningScope.parseOrNull(scopeKind, scopeId)
            ?: return LearningOutboxDecodeResult.Invalid(LearningOutboxDecodeError.INVALID_SCOPE)
        val missingReason = when {
            row.sourceRevision != null && row.missingRevisionReason == null -> null
            row.sourceRevision == null && row.missingRevisionReason != null ->
                MissingSourceRevisionReason.entries.firstOrNull {
                    it.name == row.missingRevisionReason
                } ?: MissingSourceRevisionReason.UNKNOWN
            else -> return LearningOutboxDecodeResult.Invalid(LearningOutboxDecodeError.INVALID_SOURCE)
        }
        val knownSourceKind = LearningSourceKind.entries.firstOrNull {
            it != LearningSourceKind.UNKNOWN && it.name == sourceTypeCode
        } ?: LearningSourceKind.UNKNOWN
        val source = runCatching {
            LearningSourceRef(
                sourceKind = knownSourceKind,
                sourceId = sourceId,
                sourceRevision = row.sourceRevision,
                missingRevisionReason = missingReason,
                databaseStreamId = streamId,
                scope = scope,
                occurredAtMs = occurredAtMs,
            )
        }.getOrNull()
            ?: return LearningOutboxDecodeResult.Invalid(LearningOutboxDecodeError.INVALID_SOURCE)

        if (
            LearningEventContract.violationOrNull(
                streamId = streamId,
                eventCode = eventCode,
                source = source,
                sourceTypeCode = sourceTypeCode,
                missingRevisionReasonCode = row.missingRevisionReason,
                terminalStateCode = row.terminalState,
                correlation = correlation,
                rewardDimensionCode = row.rewardDimension,
                rewardSignalKindCode = row.rewardSignalKind,
                rewardValueMilli = row.rewardValueMilli,
                executionVerificationStateCode = row.executionVerificationState,
            ) != null
        ) {
            return LearningOutboxDecodeResult.Invalid(
                LearningOutboxDecodeError.INVALID_EVENT_CONTRACT,
            )
        }
        val knownEventType = eventCode.knownType
        if (knownEventType != null && eventCode.decodeState == LearningEventDecodeState.KNOWN) {
            val expectedId = runCatching { LearningCanonicalId.eventId(
                streamId = streamId,
                eventType = knownEventType,
                eventSchemaVersion = eventCode.schemaVersion,
                sourceKindCode = sourceTypeCode,
                sourceId = sourceId,
                sourceRevision = row.sourceRevision,
                terminalState = row.terminalState,
                previousSourceRevision = row.previousSourceRevision,
                sourceStateCode = row.sourceState,
                correlation = correlation,
                rewardDimensionCode = row.rewardDimension,
                rewardSignalKindCode = row.rewardSignalKind,
                rewardValueMilli = row.rewardValueMilli,
                executionVerificationStateCode = row.executionVerificationState,
            ) }.getOrNull() ?: return LearningOutboxDecodeResult.Invalid(
                LearningOutboxDecodeError.EVENT_ID_MISMATCH,
            )
            val scopedExpectedId = if (
                knownEventType == LearningEventType.SOURCE_INVALIDATED &&
                eventCode.schemaVersion == 2 &&
                row.scopeKind != null &&
                row.scopeId != null
            ) {
                runCatching {
                    LearningCanonicalId.scopedSourceInvalidationEventId(
                        legacyEventId = expectedId,
                        scopeKindCode = row.scopeKind,
                        scopeId = row.scopeId,
                    )
                }.getOrNull()
            } else {
                null
            }
            if (row.eventId != expectedId && row.eventId != scopedExpectedId) {
                return LearningOutboxDecodeResult.Invalid(
                    LearningOutboxDecodeError.EVENT_ID_MISMATCH,
                )
            }
        }
        val event = runCatching {
            LearningHandoffEvent(
                streamId = streamId,
                eventId = row.eventId,
                outboxSeq = row.seq,
                eventCode = eventCode,
                source = source,
                sourceTypeCode = sourceTypeCode,
                missingRevisionReasonCode = row.missingRevisionReason,
                terminalStateCode = row.terminalState,
                correlation = correlation,
                rewardDimensionCode = row.rewardDimension,
                rewardSignalKindCode = row.rewardSignalKind,
                rewardValueMilli = row.rewardValueMilli,
                executionVerificationStateCode = row.executionVerificationState,
                createdAtMs = row.createdAtMs,
            )
        }.getOrNull()
            ?: return LearningOutboxDecodeResult.Invalid(
                LearningOutboxDecodeError.INVALID_EVENT_CONTRACT,
            )
        return LearningOutboxDecodeResult.Valid(event)
    }
}
