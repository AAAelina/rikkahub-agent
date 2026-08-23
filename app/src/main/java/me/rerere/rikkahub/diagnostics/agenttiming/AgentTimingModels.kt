package me.rerere.rikkahub.diagnostics.agenttiming

import kotlin.uuid.Uuid

/**
 * A fixed vocabulary for the Agent Timing sidecar.
 *
 * Events deliberately contain no prompt, tool input/output, provider body, file path, or other
 * user content. New timing seams should extend this enum instead of attaching a free-form map.
 */
enum class AgentTimingEventKind {
    UI_SUBMITTED,
    DURABLE_ADMITTED,
    RUNTIME_DEQUEUED,
    RUN_STARTED,
    RUN_ENDED,

    MEMORY_RETRIEVAL_STARTED,
    MEMORY_RETRIEVAL_FINISHED,
    AUTO_CONTEXT_STARTED,
    AUTO_CONTEXT_FINISHED,
    TOOL_SURFACE_STARTED,
    TOOL_SURFACE_FINISHED,
    MCP_DISCOVERY_STARTED,
    MCP_DISCOVERY_FINISHED,
    RECENT_CHATS_STARTED,
    RECENT_CHATS_FINISHED,
    MEMORY_PROMPT_STARTED,
    MEMORY_PROMPT_FINISHED,
    TOOL_PROMPT_STARTED,
    TOOL_PROMPT_FINISHED,
    SYSTEM_PROMPT_STARTED,
    SYSTEM_PROMPT_FINISHED,
    CONTEXT_GATE_INITIAL_STARTED,
    CONTEXT_GATE_INITIAL_FINISHED,
    INPUT_TRANSFORM_STARTED,
    INPUT_TRANSFORM_FINISHED,
    CONTEXT_GATE_FINAL_STARTED,
    CONTEXT_GATE_FINAL_FINISHED,
    CONTEXT_COMPRESSION_STARTED,
    CONTEXT_COMPRESSION_FINISHED,
    TOKEN_COUNT_STARTED,
    TOKEN_COUNT_FINISHED,
    REQUEST_BUILD_STARTED,
    REQUEST_BUILD_FINISHED,
    REQUEST_BREAKDOWN_BUILD_STARTED,
    REQUEST_BREAKDOWN_BUILD_FINISHED,
    REQUEST_BREAKDOWN_WRITE_STARTED,
    REQUEST_BREAKDOWN_WRITE_FINISHED,
    MEMORY_LAST_ACCESS_STARTED,
    MEMORY_LAST_ACCESS_FINISHED,

    PROVIDER_PREPARE_STARTED,
    PROVIDER_PREPARE_FINISHED,
    APP_PROVIDER_DISPATCH,
    PROVIDER_FIRST_PROGRESS,
    PROVIDER_FIRST_TEXT,
    PROVIDER_FULL_RESPONSE,
    PROVIDER_STREAM_FINISHED,
    PROVIDER_ATTEMPT_TERMINAL,
    PROVIDER_RETRY,
    SESSION_STATE_APPLY_STARTED,
    SESSION_CONTENT_READY,
    FIRST_VISIBLE_DRAW,
    FIRST_VISIBLE_NOT_OBSERVED,

    TOOL_QUEUED,
    TOOL_PREFLIGHT_STARTED,
    TOOL_PREFLIGHT_FINISHED,
    TOOL_EXECUTION_STARTED,
    TOOL_RAW_RESULT_READY,
    TOOL_LEDGER_COMPLETED,
    TOOL_OUTPUT_NORMALIZE_STARTED,
    TOOL_OUTPUT_NORMALIZE_FINISHED,
    TOOL_OUTPUT_SPILL_STARTED,
    TOOL_OUTPUT_SPILL_FINISHED,
    TOOL_BATCH_STARTED,
    TOOL_BATCH_FINISHED,
    MODEL_RESULTS_READY,
    TOOL_RESULTS_EMITTED,
    TOOL_RESULTS_COLLECTOR_APPLIED,
    TOOL_TERMINAL,

    APPROVAL_PENDING,
    APPROVAL_DECISION,
    APPROVAL_GROUP_WAIT_FINISHED,
    APPROVAL_COMMIT,
    RESUME_ENQUEUED,
    RESUME_STARTED,

    FINAL_SAVE_STARTED,
    FINAL_SAVE_FINISHED,
    GENERATION_DONE_NOTIFY_STARTED,
    GENERATION_DONE_NOTIFY_FINISHED,
    WATCHDOG_RETRY,
    STEERING_APPLIED,
    TRACE_COMPLETED,
    TRACE_FAILED,
    TRACE_CANCELLED,
    TRACE_TIMED_OUT,
    TRACE_CONTEXT_OVERFLOW,
    TRACE_EVICTED,
}

enum class AgentTimingEventResult {
    NONE,
    SUCCESS,
    FAILED,
    CANCELLED,
    TIMED_OUT,
    SKIPPED,
    DENIED,
    ANSWERED,
}

enum class AgentTimingResponseMode {
    UNKNOWN,
    STREAMING,
    NON_STREAMING,
}

enum class AgentTimingTraceStatus {
    ACTIVE,
    WAITING_APPROVAL,
    COMPLETED,
    FAILED,
    CANCELLED,
    TIMED_OUT,
    CONTEXT_OVERFLOW,
    EVICTED,
    DISABLED,
    ;

    val isTerminal: Boolean
        get() = this != ACTIVE && this != WAITING_APPROVAL
}

/** Opaque round identity. Provider call and retry indices are labels, not identity. */
data class AgentTimingRoundRef internal constructor(
    val traceSequence: Long,
    val ordinal: Int,
    val runtimeRunId: Uuid?,
    val providerCallIndex: Int,
    val attemptIndex: Int,
)

/** Opaque tool identity. toolCallId may be empty or duplicated, so ordinal is authoritative. */
data class AgentTimingToolRef internal constructor(
    val traceSequence: Long,
    val ordinal: Int,
    val roundOrdinal: Int?,
)

data class AgentTimingEvent(
    val sequence: Int,
    val kind: AgentTimingEventKind,
    val atNs: Long,
    val result: AgentTimingEventResult = AgentTimingEventResult.NONE,
    val roundOrdinal: Int? = null,
    val toolOrdinal: Int? = null,
)

data class AgentTimingEventAggregate(
    val kind: AgentTimingEventKind,
    val count: Long,
    val firstAtNs: Long,
    val lastAtNs: Long,
)

data class AgentTimingRoundSnapshot(
    val ordinal: Int,
    val providerCallIndex: Int,
    val attemptIndex: Int,
    val responseMode: AgentTimingResponseMode,
    val runtimeRunId: Uuid?,
    val milestones: Map<AgentTimingEventKind, Long>,
    val terminalResult: AgentTimingEventResult? = null,
    val handoffFromPreviousResultsNs: Long? = null,
    val promptTokens: Int? = null,
    val completionTokens: Int? = null,
    val cachedTokens: Int? = null,
    val providerVisibleStream: AgentTimingStreamProgressSnapshot = AgentTimingStreamProgressSnapshot(),
    val sessionVisibleStream: AgentTimingStreamProgressSnapshot = AgentTimingStreamProgressSnapshot(),
    val uiVisibleStream: AgentTimingStreamProgressSnapshot = AgentTimingStreamProgressSnapshot(),
) {
    fun at(kind: AgentTimingEventKind): Long? = milestones[kind]

    fun durationNs(
        from: AgentTimingEventKind,
        to: AgentTimingEventKind,
    ): Long? = durationBetween(milestones[from], milestones[to])

    val ttftNs: Long?
        get() = durationNs(
            AgentTimingEventKind.APP_PROVIDER_DISPATCH,
            AgentTimingEventKind.PROVIDER_FIRST_PROGRESS,
        )

    val firstTextNs: Long?
        get() = durationNs(
            AgentTimingEventKind.APP_PROVIDER_DISPATCH,
            AgentTimingEventKind.PROVIDER_FIRST_TEXT,
        )
}

/** Bounded, content-free streaming throughput counters for one provider round. */
data class AgentTimingStreamProgressSnapshot(
    val estimatedTokens: Long = 0L,
    val sampleCount: Long = 0L,
    val firstAtNs: Long? = null,
    val lastAtNs: Long? = null,
) {
    val durationNs: Long?
        get() = durationBetween(firstAtNs, lastAtNs)
}

enum class AgentTimingStreamStage {
    PROVIDER_RAW_VISIBLE,
    SESSION_CONSUMER_VISIBLE,
}

/** Cheap language-neutral approximation used only for diagnostics, never billing/accounting. */
internal fun String.estimatedAgentTimingTokenUnits(): Long {
    var nonAscii = 0L
    var ascii = 0L
    codePoints().forEach { codePoint ->
        if (!Character.isWhitespace(codePoint)) {
            if (codePoint <= 0x7f) ascii += 1L else nonAscii += 1L
        }
    }
    return nonAscii + (ascii + 3L) / 4L
}

data class AgentTimingToolSnapshot(
    val ordinal: Int,
    val roundOrdinal: Int?,
    val toolCallId: String?,
    val assistantMessageId: Uuid?,
    val milestones: Map<AgentTimingEventKind, Long>,
    val terminalResult: AgentTimingEventResult? = null,
    val sharedModelResultsReadyAtNs: Long? = null,
    val nextProviderDispatchAtNs: Long? = null,
    val nextProviderFirstProgressAtNs: Long? = null,
) {
    fun at(kind: AgentTimingEventKind): Long? = milestones[kind]

    fun durationNs(
        from: AgentTimingEventKind,
        to: AgentTimingEventKind,
    ): Long? = durationBetween(milestones[from], milestones[to])

    val executionDurationNs: Long?
        get() = durationNs(
            AgentTimingEventKind.TOOL_EXECUTION_STARTED,
            AgentTimingEventKind.TOOL_RAW_RESULT_READY,
        )

    /** Includes this result's normalization and, for parallel batches, slower sibling tools. */
    val resultToModelReadyDurationNs: Long?
        get() = durationBetween(
            milestones[AgentTimingEventKind.TOOL_RAW_RESULT_READY],
            sharedModelResultsReadyAtNs,
        )

    val handoffDurationNs: Long?
        get() = durationBetween(sharedModelResultsReadyAtNs, nextProviderDispatchAtNs)

    val nextRoundTtftNs: Long?
        get() = durationBetween(nextProviderDispatchAtNs, nextProviderFirstProgressAtNs)

    val humanApprovalWaitDurationNs: Long?
        get() = durationNs(
            AgentTimingEventKind.APPROVAL_PENDING,
            AgentTimingEventKind.APPROVAL_DECISION,
        )
}

/** One active execution interval. Approval suspension deliberately creates a segment boundary. */
data class AgentTimingActiveSegmentSnapshot(
    val startedAtNs: Long,
    val finishedAtNs: Long?,
) {
    val durationNs: Long?
        get() = durationBetween(startedAtNs, finishedAtNs)
}

/**
 * One approval group suspension.
 *
 * Human wait ends with the last user decision. Suspension ends only when the resume run starts;
 * the interval between those two points is resolution/resume overhead.
 */
data class AgentTimingApprovalSegmentSnapshot(
    val startedAtNs: Long,
    val userDecisionAtNs: Long?,
    val resumedAtNs: Long?,
    val endedAtNs: Long?,
) {
    fun humanWaitDurationNs(snapshotAtNs: Long): Long =
        nonNegativeDelta(startedAtNs, userDecisionAtNs ?: endedAtNs ?: snapshotAtNs)

    fun suspendedDurationNs(snapshotAtNs: Long): Long =
        nonNegativeDelta(startedAtNs, resumedAtNs ?: endedAtNs ?: snapshotAtNs)

    fun resolutionDurationNs(snapshotAtNs: Long): Long {
        val decision = userDecisionAtNs ?: return 0L
        return nonNegativeDelta(decision, resumedAtNs ?: endedAtNs ?: snapshotAtNs)
    }
}

data class AgentTimingTraceSnapshot(
    val traceSequence: Long,
    val conversationId: Uuid,
    val commandId: Uuid?,
    val submittedAtNs: Long,
    val runtimeRunIds: List<Uuid>,
    val assistantMessageIds: List<Uuid>,
    val status: AgentTimingTraceStatus,
    val pendingApprovalCount: Int,
    val finishedAtNs: Long?,
    val lastEventAtNs: Long,
    val milestones: Map<AgentTimingEventKind, Long>,
    val rounds: List<AgentTimingRoundSnapshot>,
    val tools: List<AgentTimingToolSnapshot>,
    val events: List<AgentTimingEvent>,
    val aggregates: List<AgentTimingEventAggregate>,
    val activeSegments: List<AgentTimingActiveSegmentSnapshot>,
    val approvalSegments: List<AgentTimingApprovalSegmentSnapshot>,
    val droppedEventCount: Long,
    val droppedRoundCount: Long,
    val droppedToolCount: Long,
) {
    val isTerminal: Boolean
        get() = status.isTerminal

    val totalRoundCount: Long
        get() = rounds.size.toLong() + droppedRoundCount

    val totalToolCount: Long
        get() = tools.size.toLong() + droppedToolCount

    val totalEventCount: Long
        get() = events.size.toLong() + droppedEventCount

    val snapshotAtNs: Long
        get() = finishedAtNs ?: lastEventAtNs

    val wallDurationNs: Long
        get() = nonNegativeDelta(submittedAtNs, finishedAtNs ?: lastEventAtNs)

    val activeDurationNs: Long
        get() = activeSegments.sumOf { segment ->
            nonNegativeDelta(
                segment.startedAtNs,
                segment.finishedAtNs ?: snapshotAtNs,
            )
        }

    val humanWaitDurationNs: Long
        get() = approvalSegments.sumOf { it.humanWaitDurationNs(snapshotAtNs) }

    val approvalSuspendedDurationNs: Long
        get() = approvalSegments.sumOf { it.suspendedDurationNs(snapshotAtNs) }

    val approvalResolutionDurationNs: Long
        get() = approvalSegments.sumOf { it.resolutionDurationNs(snapshotAtNs) }

    val firstProviderProgressAtNs: Long?
        get() = milestones[AgentTimingEventKind.PROVIDER_FIRST_PROGRESS]

    val firstSessionContentReadyAtNs: Long?
        get() = milestones[AgentTimingEventKind.SESSION_CONTENT_READY]

    val firstContentAppliedAtNs: Long?
        get() = firstSessionContentReadyAtNs

    val firstVisibleDrawAtNs: Long?
        get() = milestones[AgentTimingEventKind.FIRST_VISIBLE_DRAW]

    fun at(kind: AgentTimingEventKind): Long? = milestones[kind]

    fun durationNs(
        from: AgentTimingEventKind,
        to: AgentTimingEventKind,
    ): Long? = durationBetween(milestones[from], milestones[to])
}

data class AgentTimingConversationSnapshot(
    val conversationId: Uuid,
    /** Newest trace first. Active traces are pinned and are never evicted by terminal limits. */
    val traces: List<AgentTimingTraceSnapshot> = emptyList(),
) {
    fun traceForMessage(messageId: Uuid): AgentTimingTraceSnapshot? =
        traces.firstOrNull { messageId in it.assistantMessageIds }
}

internal fun durationBetween(startNs: Long?, endNs: Long?): Long? {
    if (startNs == null || endNs == null || endNs < startNs) return null
    return endNs - startNs
}

private fun nonNegativeDelta(startNs: Long, endNs: Long): Long =
    (endNs - startNs).coerceAtLeast(0L)
