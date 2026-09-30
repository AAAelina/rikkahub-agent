package me.rerere.rikkahub.data.execution

import androidx.room.withTransaction
import me.rerere.rikkahub.data.db.AppDatabase

data class ExecutionMutation(
    val executionId: String,
    val mutationId: String,
    val expectedVersion: Long,
    val source: ExecutionStateSource,
    val reasonCode: String? = null,
    val targetStatus: ExecutionStatus? = null,
    val verificationState: VerificationState? = null,
    val runtime: ExecutionRuntime? = null,
    val executionKind: ExecutionKind? = null,
    val runtimeHandleSummary: String? = null,
    val completionPolicy: CompletionPolicy? = null,
    val runtimeInstanceMarker: String? = null,
    val cancellationResult: String? = null,
    val requestedTerminalOutcome: RequestedTerminalOutcome? = null,
    val terminalDetail: String? = null,
    val heartbeatAtMs: Long? = null,
    val probeAtMs: Long? = null,
    val appendEvent: Boolean = true,
)

sealed interface ExecutionMutationResult {
    data class Applied(val record: ExecutionRecord) : ExecutionMutationResult
    data class Duplicate(val record: ExecutionRecord) : ExecutionMutationResult
    data class Missing(val id: String) : ExecutionMutationResult
    data class Terminal(val record: ExecutionRecord) : ExecutionMutationResult
    data class Invalid(
        val current: ExecutionStatus,
        val requested: ExecutionStatus,
    ) : ExecutionMutationResult
    data class Conflict(val currentVersion: Long) : ExecutionMutationResult
}

/**
 * Receipt for an execution mutation committed inside an authority transaction owned elsewhere.
 * [insertedOutbox] is only a post-commit scheduling signal; it must not be dispatched until the
 * outermost Room transaction has returned successfully.
 */
data class ExecutionMutationCommit(
    val result: ExecutionMutationResult,
    val insertedOutbox: Boolean,
)

internal sealed interface ExecutionReduction {
    data class Next(val record: ExecutionRecord) : ExecutionReduction
    data class Terminal(val record: ExecutionRecord) : ExecutionReduction
    data class Invalid(
        val current: ExecutionStatus,
        val requested: ExecutionStatus,
    ) : ExecutionReduction
}

internal object ExecutionMutationReducer {
    fun reduce(
        existing: ExecutionRecord,
        mutation: ExecutionMutation,
        nowMs: Long,
    ): ExecutionReduction {
        val current = ExecutionStatus.fromWire(existing.status)
        val target = mutation.targetStatus ?: current
        if (current.isTerminal) return ExecutionReduction.Terminal(existing)
        if (!current.canTransitionTo(target)) return ExecutionReduction.Invalid(current, target)

        val next = existing.copy(
            status = target.name,
            runtime = mutation.runtime?.name ?: existing.runtime,
            executionKind = mutation.executionKind?.name ?: existing.executionKind,
            runtimeHandleSummary = mutation.runtimeHandleSummary ?: existing.runtimeHandleSummary,
            updatedAtMs = nowMs,
            startedAtMs = existing.startedAtMs ?: nowMs.takeIf {
                target == ExecutionStatus.starting || target == ExecutionStatus.running
            },
            heartbeatAtMs = mutation.heartbeatAtMs
                ?: nowMs.takeIf { target == ExecutionStatus.running }
                ?: existing.heartbeatAtMs,
            finishedAtMs = nowMs.takeIf { target.isTerminal } ?: existing.finishedAtMs,
            cancellationResult = mutation.cancellationResult ?: existing.cancellationResult,
            requestedTerminalOutcome = mutation.requestedTerminalOutcome?.name
                ?: existing.requestedTerminalOutcome,
            terminalDetail = mutation.terminalDetail ?: existing.terminalDetail,
            stateVersion = existing.stateVersion + 1,
            lastStateSource = mutation.source.name,
            lastReasonCode = mutation.reasonCode ?: existing.lastReasonCode,
            verificationState = mutation.verificationState?.name ?: existing.verificationState,
            lastProbeAtMs = mutation.probeAtMs ?: existing.lastProbeAtMs,
            completionPolicy = mutation.completionPolicy?.name ?: existing.completionPolicy,
            runtimeInstanceMarker = mutation.runtimeInstanceMarker ?: existing.runtimeInstanceMarker,
            cancellationRequestedAtMs = existing.cancellationRequestedAtMs ?: nowMs.takeIf {
                target == ExecutionStatus.cancel_requested
            },
        )
        return ExecutionReduction.Next(next)
    }
}

/** Database-level CAS boundary for execution snapshots and their append-only event journal. */
class ExecutionStateTransaction(
    private val database: AppDatabase,
    private val recordDao: ExecutionRecordDao,
    private val eventDao: ExecutionEventDao,
    private val nowMs: () -> Long = System::currentTimeMillis,
    private val metrics: ExecutionConsistencyMetrics? = null,
) {
    suspend fun open(
        draft: ExecutionRecordDraft,
        mutationId: String = "open:${draft.id}",
        source: ExecutionStateSource = ExecutionStateSource.LIVE_EVENT,
        reasonCode: String = "execution_opened",
    ): ExecutionRecord = database.withTransaction {
        openInCurrentTransaction(draft, mutationId, source, reasonCode)
    }

    suspend fun openInCurrentTransaction(
        draft: ExecutionRecordDraft,
        mutationId: String = "open:${draft.id}",
        source: ExecutionStateSource = ExecutionStateSource.LIVE_EVENT,
        reasonCode: String = "execution_opened",
    ): ExecutionRecord {
        check(database.inTransaction()) { "execution_open_requires_authority_transaction" }
        require(!draft.initialStatus.isTerminal) {
            "execution_open_terminal_requires_mutation"
        }
        recordDao.getById(draft.id)?.let { existing ->
            check(existing.hasSameAdmissionIdentityAs(draft)) {
                "execution_open_identity_conflict"
            }
            return existing
        }
        val now = nowMs()
        val created = draft.toRecord(now).copy(
            stateVersion = 1,
            lastStateSource = source.name,
            lastReasonCode = reasonCode,
        )
        if (recordDao.insertIgnore(created) == -1L) {
            val existing = checkNotNull(recordDao.getById(draft.id))
            check(existing.hasSameAdmissionIdentityAs(draft)) {
                "execution_open_identity_conflict"
            }
            return existing
        }
        eventDao.insert(
            ExecutionEventRecord(
                eventId = mutationId,
                executionId = created.id,
                sequence = created.stateVersion,
                previousStatus = null,
                nextStatus = created.status,
                previousVerification = null,
                nextVerification = created.verificationState,
                source = source.name,
                reasonCode = reasonCode,
                createdAtMs = now,
            ),
        )
        return created
    }

    suspend fun mutate(mutation: ExecutionMutation): ExecutionMutationResult {
        val commit = database.withTransaction { mutateInCurrentTransaction(mutation) }
        dispatchExternalPostCommit(commit)
        return commit.result
    }

    /**
     * Mutates snapshot, journal, and learning outbox inside the caller's existing Room
     * transaction. This method never schedules derived work.
     */
    suspend fun mutateInCurrentTransaction(
        mutation: ExecutionMutation,
    ): ExecutionMutationCommit {
        check(database.inTransaction()) { "execution_mutation_requires_authority_transaction" }
        val existing = recordDao.getById(mutation.executionId)
            ?: return ExecutionMutationCommit(
                ExecutionMutationResult.Missing(mutation.executionId),
                insertedOutbox = false,
            )
        val duplicateEvent = mutation.mutationId
            .takeIf { mutation.appendEvent }
            ?.let { eventDao.getById(it) }
        if (duplicateEvent != null) {
            check(duplicateEvent.hasSameJournalIdentityAs(mutation, existing.stateVersion)) {
                "execution_mutation_identity_conflict"
            }
            check(existing.stateVersion >= duplicateEvent.sequence) {
                "execution_event_ahead_of_snapshot"
            }
            return ExecutionMutationCommit(
                result = ExecutionMutationResult.Duplicate(existing),
                insertedOutbox = false,
            )
        }
        if (existing.stateVersion != mutation.expectedVersion) {
            metrics?.recordCasConflict()
            return ExecutionMutationCommit(
                ExecutionMutationResult.Conflict(existing.stateVersion),
                insertedOutbox = false,
            )
        }
        val result = when (val reduced = ExecutionMutationReducer.reduce(existing, mutation, nowMs())) {
            is ExecutionReduction.Invalid -> ExecutionMutationResult.Invalid(
                reduced.current,
                reduced.requested,
            )
            is ExecutionReduction.Terminal -> ExecutionMutationResult.Terminal(reduced.record)
            is ExecutionReduction.Next -> {
                val next = reduced.record
                if (ExecutionStatus.fromWire(next.status).isTerminal && !mutation.appendEvent) {
                    error("execution_terminal_event_required")
                }
                val updated = recordDao.compareAndSet(
                    id = next.id,
                    expectedVersion = existing.stateVersion,
                    nextVersion = next.stateVersion,
                    status = next.status,
                    runtime = next.runtime,
                    executionKind = next.executionKind,
                    runtimeHandleSummary = next.runtimeHandleSummary,
                    updatedAtMs = next.updatedAtMs,
                    startedAtMs = next.startedAtMs,
                    heartbeatAtMs = next.heartbeatAtMs,
                    finishedAtMs = next.finishedAtMs,
                    cancellationResult = next.cancellationResult,
                    terminalDetail = next.terminalDetail,
                    lastStateSource = next.lastStateSource,
                    lastReasonCode = next.lastReasonCode,
                    verificationState = next.verificationState,
                    lastProbeAtMs = next.lastProbeAtMs,
                    completionPolicy = next.completionPolicy,
                    runtimeInstanceMarker = next.runtimeInstanceMarker,
                    cancellationRequestedAtMs = next.cancellationRequestedAtMs,
                    requestedTerminalOutcome = next.requestedTerminalOutcome,
                )
                if (updated != 1) {
                    metrics?.recordCasConflict()
                    return ExecutionMutationCommit(
                        ExecutionMutationResult.Conflict(
                            recordDao.getById(next.id)?.stateVersion ?: existing.stateVersion,
                        ),
                        insertedOutbox = false,
                    )
                }
                if (mutation.appendEvent) {
                    val event = ExecutionEventRecord(
                        eventId = mutation.mutationId,
                        executionId = next.id,
                        sequence = next.stateVersion,
                        previousStatus = existing.status,
                        nextStatus = next.status,
                        previousVerification = existing.verificationState,
                        nextVerification = next.verificationState,
                        source = mutation.source.name,
                        reasonCode = mutation.reasonCode,
                        createdAtMs = next.updatedAtMs,
                    )
                    eventDao.insert(event)
                }
                ExecutionMutationResult.Applied(next)
            }
        }
        return ExecutionMutationCommit(result, insertedOutbox = false)
    }

    /** Dispatches a receipt only after the transaction that owns it has committed. */
    @Suppress("UNUSED_PARAMETER")
    fun dispatchExternalPostCommit(commit: ExecutionMutationCommit) = Unit
}

/** Columns intentionally omitted here are mutable execution state updated by the CAS reducer. */
internal fun ExecutionRecord.hasSameAdmissionIdentityAs(draft: ExecutionRecordDraft): Boolean =
    id == draft.id &&
        traceId == draft.traceId &&
        parentExecutionId == draft.parentExecutionId &&
        commandId == draft.commandId &&
        conversationId == draft.conversationId &&
        toolCallId == draft.toolCallId &&
        toolName == draft.toolName &&
        toolSchemaFingerprint == draft.toolSchemaFingerprint &&
        learningScopeKind == draft.learningScope.kind.name &&
        learningScopeId == draft.learningScope.storageId &&
        subjectId == draft.subjectId &&
        subjectType == draft.subjectType &&
        origin == draft.origin &&
        capabilityKeys == draft.capabilityKeys &&
        resourceSummary == draft.resourceSummary &&
        idempotencyKey == draft.idempotencyKey &&
        executionKind == draft.executionKind.name &&
        completionPolicy == draft.completionPolicy.name

/** Checks every mutation identity field represented by the append-only event journal. */
internal fun ExecutionEventRecord.hasSameJournalIdentityAs(
    mutation: ExecutionMutation,
    currentVersion: Long,
): Boolean {
    val previousStatus = previousStatus ?: return false
    val previousVerification = previousVerification ?: return false
    // Repository retries rebuild expectedVersion from the latest snapshot. Accept that replay
    // shape as well as the producer's original CAS version, but no unrelated version.
    val versionMatches = mutation.expectedVersion == sequence - 1L ||
        mutation.expectedVersion == currentVersion
    return eventId == mutation.mutationId &&
        executionId == mutation.executionId &&
        versionMatches &&
        source == mutation.source.name &&
        reasonCode == mutation.reasonCode &&
        nextStatus == (mutation.targetStatus?.name ?: previousStatus) &&
        nextVerification == (mutation.verificationState?.name ?: previousVerification)
}
