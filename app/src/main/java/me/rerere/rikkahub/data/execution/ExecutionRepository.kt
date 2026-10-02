package me.rerere.rikkahub.data.execution

import java.security.MessageDigest
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import me.rerere.rikkahub.data.scope.AgentScope

/** Input for a new authoritative execution row. All fields are deliberately non-secret. */
data class ExecutionRecordDraft(
    val id: String,
    val traceId: String,
    val parentExecutionId: String? = null,
    val commandId: String? = null,
    val conversationId: String? = null,
    val toolCallId: String? = null,
    val toolName: String? = null,
    val toolSchemaFingerprint: String? = null,
    val owningAssistantMessageId: String? = null,
    val owningAssistantMessageRevision: Long? = null,
    /** Frozen at admission. Legacy imported rows may be null, but new runtime rows may not. */
    val learningScope: AgentScope,
    val subjectId: String,
    val subjectType: String,
    val origin: String,
    val capabilityKeys: String,
    val resourceSummary: String,
    val runtime: ExecutionRuntime,
    val idempotencyKey: String? = null,
    val initialStatus: ExecutionStatus = ExecutionStatus.queued,
    val executionKind: ExecutionKind = ExecutionKind.TOOL_CALL,
    val completionPolicy: CompletionPolicy = CompletionPolicy.WAIT_FOR_CHILDREN,
    val verificationState: VerificationState = VerificationState.LIVE_CONFIRMED,
    val runtimeHandleSummary: String? = null,
    val runtimeInstanceMarker: String? = null,
    val requestedTerminalOutcome: RequestedTerminalOutcome = RequestedTerminalOutcome.NONE,
) {
    init {
        require(!initialStatus.isTerminal) {
            "Execution terminal state must be committed through the state transaction"
        }
        require(
            listOf(toolCallId, toolName, toolSchemaFingerprint).all { it == null } ||
                listOf(toolCallId, toolName, toolSchemaFingerprint).all { it != null },
        ) { "Execution tool identity must be complete" }
        require((owningAssistantMessageId == null) == (owningAssistantMessageRevision == null)) {
            "Execution owning message requires an exact revision"
        }
    }
}

sealed interface ExecutionTransitionResult {
    data class Applied(val record: ExecutionRecord) : ExecutionTransitionResult
    data class Missing(val id: String) : ExecutionTransitionResult
    data class Terminal(val record: ExecutionRecord) : ExecutionTransitionResult
    data class Invalid(
        val current: ExecutionStatus,
        val requested: ExecutionStatus,
    ) : ExecutionTransitionResult
    data class Conflict(val id: String) : ExecutionTransitionResult
}

/**
 * Public execution ledger facade.
 *
 * The mutex reduces avoidable local conflicts; correctness comes from [ExecutionStateTransaction]
 * and its database CAS, not from process-local serialisation.
 */
class ExecutionRepository(
    private val dao: ExecutionRecordDao,
    private val transaction: ExecutionStateTransaction,
    private val retention: ExecutionRetentionManager,
) {
    private val mutex = Mutex()

    suspend fun open(
        draft: ExecutionRecordDraft,
        mutationId: String = "open:${draft.id}",
        source: ExecutionStateSource = ExecutionStateSource.LIVE_EVENT,
        reasonCode: String = "execution_opened",
    ): ExecutionRecord = mutex.withLock {
        transaction.open(
            draft = draft.sanitized(),
            mutationId = mutationId.take(MAX_MUTATION_ID_CHARS),
            source = source,
            reasonCode = reasonCode.take(MAX_REASON_CHARS),
        )
    }

    suspend fun transition(
        id: String,
        target: ExecutionStatus,
        runtimeHandleSummary: String? = null,
        cancellationResult: String? = null,
        detail: String? = null,
        mutationId: String = stableMutationId(id, target, runtimeHandleSummary, detail),
        source: ExecutionStateSource = ExecutionStateSource.LIVE_EVENT,
        reasonCode: String? = "status_${target.name}",
        verificationState: VerificationState? = null,
        runtime: ExecutionRuntime? = null,
        executionKind: ExecutionKind? = null,
        completionPolicy: CompletionPolicy? = null,
        runtimeInstanceMarker: String? = null,
        probeAtMs: Long? = null,
        requestedTerminalOutcome: RequestedTerminalOutcome? = null,
    ): ExecutionTransitionResult = mutex.withLock {
        val result = mutateWithRetry(id) { existing ->
            ExecutionMutation(
                executionId = id,
                mutationId = mutationId.take(MAX_MUTATION_ID_CHARS),
                expectedVersion = existing.stateVersion,
                source = source,
                reasonCode = reasonCode?.take(MAX_REASON_CHARS),
                targetStatus = target,
                verificationState = verificationState,
                runtime = runtime,
                executionKind = executionKind,
                runtimeHandleSummary = runtimeHandleSummary?.take(MAX_HANDLE_CHARS),
                completionPolicy = completionPolicy,
                runtimeInstanceMarker = runtimeInstanceMarker?.take(MAX_INSTANCE_MARKER_CHARS),
                cancellationResult = cancellationResult?.take(MAX_CANCELLATION_CHARS),
                requestedTerminalOutcome = requestedTerminalOutcome,
                terminalDetail = detail?.take(MAX_DETAIL_CHARS),
                probeAtMs = probeAtMs,
            )
        }
        if (result is ExecutionTransitionResult.Applied) {
            retention.requestCleanup(
                executionId = result.record.id,
                includeGlobalRetention = ExecutionStatus.fromWire(result.record.status).isTerminal,
            )
        }
        result
    }

    suspend fun heartbeat(id: String): ExecutionTransitionResult = mutex.withLock {
        mutateWithRetry(id) { existing ->
            val now = System.currentTimeMillis()
            ExecutionMutation(
                executionId = id,
                mutationId = "heartbeat:$id:$now",
                expectedVersion = existing.stateVersion,
                source = ExecutionStateSource.LIVE_EVENT,
                reasonCode = "heartbeat",
                heartbeatAtMs = now,
                appendEvent = false,
            )
        }
    }

    suspend fun bindRuntime(
        id: String,
        runtime: ExecutionRuntime,
        runtimeHandleSummary: String,
        mutationId: String = "bind:$id:${runtime.name}:$runtimeHandleSummary",
    ): ExecutionTransitionResult = mutex.withLock {
        mutateWithRetry(id) { existing ->
            ExecutionMutation(
                executionId = id,
                mutationId = mutationId.take(MAX_MUTATION_ID_CHARS),
                expectedVersion = existing.stateVersion,
                source = ExecutionStateSource.LIVE_EVENT,
                reasonCode = "runtime_bound",
                runtime = runtime,
                runtimeHandleSummary = runtimeHandleSummary.take(MAX_HANDLE_CHARS),
            )
        }
    }

    suspend fun mutateObserved(mutation: ExecutionMutation): ExecutionMutationResult =
        transaction.mutate(mutation.sanitized())

    /** Caller must already own the AppDatabase authority transaction. No scheduler is invoked. */
    suspend fun mutateObservedInCurrentTransaction(
        mutation: ExecutionMutation,
    ): ExecutionMutationCommit = transaction.mutateInCurrentTransaction(mutation.sanitized())

    /** Dispatches only after the caller's outer authority transaction has committed. */
    fun dispatchObservedPostCommit(commit: ExecutionMutationCommit) {
        transaction.dispatchExternalPostCommit(commit)
    }

    /** Caller owns the AppDatabase authority transaction; no nested transaction is opened. */
    suspend fun openInCurrentAuthorityTransaction(
        draft: ExecutionRecordDraft,
        mutationId: String = "open:${draft.id}",
        source: ExecutionStateSource = ExecutionStateSource.LIVE_EVENT,
        reasonCode: String = "execution_opened",
    ): ExecutionRecord = mutex.withLock {
        transaction.openInCurrentTransaction(
            draft = draft.sanitized(),
            mutationId = mutationId.take(MAX_MUTATION_ID_CHARS),
            source = source,
            reasonCode = reasonCode.take(MAX_REASON_CHARS),
        )
    }

    suspend fun get(id: String): ExecutionRecord? = dao.getById(id)

    suspend fun getInFlight(): List<ExecutionRecord> = dao.getInFlight()

    suspend fun getInFlightForSubject(
        conversationId: String,
        subjectId: String,
    ): List<ExecutionRecord> = dao.getInFlightForSubject(conversationId, subjectId)

    suspend fun getInFlightForConversationSubjectType(
        conversationId: String,
        subjectType: String,
    ): List<ExecutionRecord> = dao.getInFlightForConversationSubjectType(conversationId, subjectType)

    suspend fun getChildren(parentExecutionId: String): List<ExecutionRecord> =
        dao.getChildren(parentExecutionId)

    suspend fun getByRuntimeHandle(runtime: ExecutionRuntime, handle: String): ExecutionRecord? =
        dao.getByRuntimeHandle(runtime.name, handle)

    suspend fun getStaleInFlight(beforeMs: Long): List<ExecutionRecord> =
        dao.getStaleInFlight(beforeMs)

    suspend fun findLatestByIdempotencyKey(key: String): ExecutionRecord? =
        dao.getLatestByIdempotencyKey(key)

    fun observeRecent(limit: Int = 100): Flow<List<ExecutionRecord>> = dao.observeRecent(limit)

    fun observeActiveForSubject(
        conversationId: String,
        subjectId: String,
    ): Flow<List<ExecutionRecord>> = dao.observeActiveForSubject(conversationId, subjectId)

    fun observeRecentTerminalForSubject(
        conversationId: String,
        subjectId: String,
        limit: Int = 20,
    ): Flow<List<ExecutionRecord>> =
        dao.observeRecentTerminalForSubject(conversationId, subjectId, limit)

    suspend fun getRecent(limit: Int = 100): List<ExecutionRecord> = dao.getRecent(limit)

    private suspend fun mutateWithRetry(
        id: String,
        build: (ExecutionRecord) -> ExecutionMutation,
    ): ExecutionTransitionResult {
        repeat(MAX_CAS_ATTEMPTS) {
            val existing = dao.getById(id) ?: return ExecutionTransitionResult.Missing(id)
            when (val result = transaction.mutate(build(existing).sanitized())) {
                is ExecutionMutationResult.Applied -> return ExecutionTransitionResult.Applied(result.record)
                is ExecutionMutationResult.Duplicate -> return ExecutionTransitionResult.Applied(result.record)
                is ExecutionMutationResult.Missing -> return ExecutionTransitionResult.Missing(result.id)
                is ExecutionMutationResult.Terminal -> return ExecutionTransitionResult.Terminal(result.record)
                is ExecutionMutationResult.Invalid -> return ExecutionTransitionResult.Invalid(
                    result.current,
                    result.requested,
                )
                is ExecutionMutationResult.Conflict -> Unit
            }
        }
        return ExecutionTransitionResult.Conflict(id)
    }

    companion object {
        private const val MAX_CAS_ATTEMPTS = 3
        private const val MAX_CAPABILITIES_CHARS = 500
        private const val MAX_RESOURCE_SUMMARY_CHARS = 500
        private const val MAX_IDEMPOTENCY_CHARS = 300
        private const val MAX_HANDLE_CHARS = 500
        private const val MAX_CANCELLATION_CHARS = 160
        private const val MAX_DETAIL_CHARS = 500
        private const val MAX_REASON_CHARS = 160
        private const val MAX_MUTATION_ID_CHARS = 500
        private const val MAX_INSTANCE_MARKER_CHARS = 160

        private fun stableMutationId(
            id: String,
            target: ExecutionStatus,
            handle: String?,
            detail: String?,
        ): String {
            val input = "transition\u0000$id\u0000${target.name}\u0000${handle.orEmpty()}\u0000${detail.orEmpty()}"
            val digest = MessageDigest.getInstance("SHA-256").digest(input.toByteArray())
            return "mutation:" + digest.joinToString("") { "%02x".format(it) }
        }
    }

    private fun ExecutionRecordDraft.sanitized(): ExecutionRecordDraft = copy(
        id = id.take(480),
        traceId = traceId.take(480),
        parentExecutionId = parentExecutionId?.take(480),
        commandId = commandId?.take(480),
        conversationId = conversationId?.take(480),
        toolCallId = toolCallId?.take(256),
        toolName = toolName?.take(256),
        toolSchemaFingerprint = toolSchemaFingerprint,
        owningAssistantMessageId = owningAssistantMessageId?.take(256),
        subjectId = subjectId.take(160),
        subjectType = subjectType.take(80),
        origin = origin.take(80),
        capabilityKeys = capabilityKeys.take(MAX_CAPABILITIES_CHARS),
        resourceSummary = resourceSummary.take(MAX_RESOURCE_SUMMARY_CHARS),
        idempotencyKey = idempotencyKey?.take(MAX_IDEMPOTENCY_CHARS),
        runtimeHandleSummary = runtimeHandleSummary?.take(MAX_HANDLE_CHARS),
        runtimeInstanceMarker = runtimeInstanceMarker?.take(MAX_INSTANCE_MARKER_CHARS),
    )

    private fun ExecutionMutation.sanitized(): ExecutionMutation = copy(
        executionId = executionId.take(480),
        mutationId = mutationId.take(MAX_MUTATION_ID_CHARS),
        reasonCode = reasonCode?.take(MAX_REASON_CHARS),
        runtimeHandleSummary = runtimeHandleSummary?.take(MAX_HANDLE_CHARS),
        runtimeInstanceMarker = runtimeInstanceMarker?.take(MAX_INSTANCE_MARKER_CHARS),
        cancellationResult = cancellationResult?.take(MAX_CANCELLATION_CHARS),
        terminalDetail = terminalDetail?.take(MAX_DETAIL_CHARS),
    )
}

internal fun ExecutionRecordDraft.toRecord(nowMs: Long): ExecutionRecord = ExecutionRecord(
    id = id,
    traceId = traceId,
    parentExecutionId = parentExecutionId,
    commandId = commandId,
    conversationId = conversationId,
    learningScopeKind = learningScope.kind.name,
    learningScopeId = learningScope.storageId,
    toolCallId = toolCallId,
    toolName = toolName,
    toolSchemaFingerprint = toolSchemaFingerprint,
    owningAssistantMessageId = owningAssistantMessageId,
    owningAssistantMessageRevision = owningAssistantMessageRevision,
    subjectId = subjectId,
    subjectType = subjectType,
    origin = origin,
    capabilityKeys = capabilityKeys,
    resourceSummary = resourceSummary,
    runtime = runtime.name,
    executionKind = executionKind.name,
    idempotencyKey = idempotencyKey,
    runtimeHandleSummary = runtimeHandleSummary,
    status = initialStatus.name,
    createdAtMs = nowMs,
    updatedAtMs = nowMs,
    startedAtMs = nowMs.takeIf {
        initialStatus == ExecutionStatus.starting || initialStatus == ExecutionStatus.running
    },
    heartbeatAtMs = nowMs.takeIf { initialStatus == ExecutionStatus.running },
    finishedAtMs = nowMs.takeIf { initialStatus.isTerminal },
    verificationState = verificationState.name,
    completionPolicy = completionPolicy.name,
    runtimeInstanceMarker = runtimeInstanceMarker,
    requestedTerminalOutcome = requestedTerminalOutcome.name,
)

/** Parses only the explicitly frozen scope columns; legacy rows are never inferred or promoted. */
fun ExecutionRecord.learningScopeOrNull(): AgentScope? {
    val kind = learningScopeKind ?: return null
    val id = learningScopeId ?: return null
    return AgentScope.parseOrNull(kind, id)
}
