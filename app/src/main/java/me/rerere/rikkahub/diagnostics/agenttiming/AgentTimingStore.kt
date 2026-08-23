package me.rerere.rikkahub.diagnostics.agenttiming

import java.util.EnumMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlin.uuid.Uuid

data class AgentTimingLimits(
    val maxTerminalTracesGlobal: Int = 32,
    val maxTerminalTracesPerConversation: Int = 8,
    val maxRoundsPerTrace: Int = 128,
    val maxToolsPerTrace: Int = 128,
    val maxEventsPerTrace: Int = 512,
) {
    init {
        require(maxTerminalTracesGlobal > 0)
        require(maxTerminalTracesPerConversation > 0)
        require(maxRoundsPerTrace > 0)
        require(maxToolsPerTrace > 0)
        require(maxEventsPerTrace > 0)
    }
}

/**
 * Process-only, content-free Agent Timing sidecar.
 *
 * Mutation is deliberately synchronous and small: a recorder call only takes this store's lock,
 * updates bounded primitive metadata, and optionally publishes a checkpoint snapshot. No method
 * performs disk, database, network, or coroutine work.
 */
class AgentTimingStore(
    private val clock: AgentTimingClock,
    private val limits: AgentTimingLimits = AgentTimingLimits(),
) {
    private val lock = Any()
    private val recordingEnabled = AtomicBoolean(false)
    private val nextTraceSequence = AtomicLong(0L)
    private val traces = linkedMapOf<Long, MutableTrace>()
    private val traceByRunId = mutableMapOf<Uuid, Long>()
    private val traceByMessageId = mutableMapOf<Uuid, Long>()
    private val conversationFlows = mutableMapOf<Uuid, MutableStateFlow<AgentTimingConversationSnapshot>>()
    private var publicationCount = 0L

    val isEnabled: Boolean
        get() = recordingEnabled.get()

    /**
     * Disabling is an immediate fence. Open traces are removed and every existing handle is
     * disabled, so callbacks already queued on other threads cannot resurrect an entry.
     */
    fun setEnabled(enabled: Boolean) {
        try {
            if (enabled) {
                recordingEnabled.set(true)
                return
            }
            if (!recordingEnabled.getAndSet(false)) return
            synchronized(lock) {
                val affected = linkedSetOf<Uuid>()
                traces.values.forEach { trace -> trace.handle.disable() }
                traces.values
                    .filterNot { it.status.isTerminal }
                    .map { it.sequence }
                    .forEach { sequence ->
                        traces[sequence]?.let { affected += it.conversationId }
                        removeTraceLocked(sequence)
                    }
                publishLocked(affected)
            }
        } catch (_: Throwable) {
            // Diagnostics must never change Agent behavior.
        }
    }

    /** Disabled fast path: one volatile read, no clock read, token allocation, or store entry. */
    fun beginSubmission(conversationId: Uuid): AgentTimingSubmissionToken? {
        if (!recordingEnabled.get()) return null
        return try {
            synchronized(lock) {
                if (!recordingEnabled.get()) return@synchronized null
                val submittedAtNs = clock.elapsedRealtimeNanos()
                val sequence = nextTraceSequence.incrementAndGet()
                val handle = AgentTimingHandle(this, sequence)
                val trace = MutableTrace(
                    sequence = sequence,
                    conversationId = conversationId,
                    submittedAtNs = submittedAtNs,
                    handle = handle,
                )
                traces[sequence] = trace
                recordAtLocked(
                    trace = trace,
                    kind = AgentTimingEventKind.UI_SUBMITTED,
                    atNs = submittedAtNs,
                    result = AgentTimingEventResult.NONE,
                    round = null,
                    tool = null,
                    onlyOnce = true,
                )
                publishLocked(setOf(conversationId))
                AgentTimingSubmissionToken(
                    traceSequence = sequence,
                    conversationId = conversationId,
                    submittedAtNs = submittedAtNs,
                    handle = handle,
                )
            }
        } catch (_: Throwable) {
            null
        }
    }

    fun conversationFlow(conversationId: Uuid): StateFlow<AgentTimingConversationSnapshot> =
        synchronized(lock) {
            conversationFlows.getOrPut(conversationId) {
                MutableStateFlow(snapshotLocked(conversationId))
            }.asStateFlow()
        }

    fun snapshotForMessage(
        conversationId: Uuid,
        messageId: Uuid,
    ): AgentTimingTraceSnapshot? = try {
        synchronized(lock) {
            val sequence = traceByMessageId[messageId] ?: return@synchronized null
            val trace = traces[sequence]?.takeIf { it.conversationId == conversationId }
                ?: return@synchronized null
            trace.snapshot()
        }
    } catch (_: Throwable) {
        null
    }

    /** Selects one waiting-approval trace, otherwise requires exactly one unfinished trace. */
    fun openHandleForConversation(conversationId: Uuid): AgentTimingHandle? = try {
        synchronized(lock) {
            val open = traces.values
                .asSequence()
                .filter { it.conversationId == conversationId && !it.status.isTerminal }
                .toList()
            val waiting = open.filter { it.status == AgentTimingTraceStatus.WAITING_APPROVAL }
            val selected = when {
                waiting.size == 1 -> waiting.single()
                waiting.size > 1 -> null
                else -> open.singleOrNull()
            }
            selected?.handle?.takeIf { it.isRecording }
        }
    } catch (_: Throwable) {
        null
    }

    /**
     * Reattaches an in-process approval/resume envelope to the original logical trace. This does
     * not read the clock or create a store entry. Terminal traces are never continuation targets.
     */
    fun submissionTokenForMessage(
        conversationId: Uuid,
        messageId: Uuid,
    ): AgentTimingSubmissionToken? = try {
        synchronized(lock) {
            val sequence = traceByMessageId[messageId] ?: return@synchronized null
            val trace = mutableTraceForRecordingLocked(sequence)
                ?.takeIf { it.conversationId == conversationId && !it.status.isTerminal }
                ?: return@synchronized null
            trace.submissionToken()
        }
    } catch (_: Throwable) {
        null
    }

    fun tokenForHandle(handle: AgentTimingHandle): AgentTimingSubmissionToken? = try {
        synchronized(lock) {
            val trace = mutableTraceForRecordingLocked(handle.traceSequence)
                ?.takeIf { it.handle === handle && !it.status.isTerminal }
                ?: return@synchronized null
            trace.submissionToken()
        }
    } catch (_: Throwable) {
        null
    }

    fun handleForMessage(
        conversationId: Uuid,
        messageId: Uuid,
    ): AgentTimingHandle? = try {
        synchronized(lock) {
            val sequence = traceByMessageId[messageId] ?: return@synchronized null
            traces[sequence]
                ?.takeIf { it.conversationId == conversationId }
                ?.handle
                ?.takeIf { it.isRecording }
        }
    } catch (_: Throwable) {
        null
    }

    fun handleForRun(runId: Uuid): AgentTimingHandle? = try {
        synchronized(lock) {
            traceByRunId[runId]
                ?.let(traces::get)
                ?.handle
                ?.takeIf { it.isRecording }
        }
    } catch (_: Throwable) {
        null
    }

    /**
     * Resolve during composition, then keep the returned marker with `remember`.
     * [AgentTimingFirstVisibleDrawMarker.captureAfterDraw] is the draw-thread fast path.
     */
    fun firstVisibleDrawMarker(
        conversationId: Uuid,
        messageId: Uuid,
    ): AgentTimingFirstVisibleDrawMarker? = handleForMessage(conversationId, messageId)
        ?.let(::AgentTimingFirstVisibleDrawMarker)

    /** Records post-frame visible-text growth while the assistant message is streaming. */
    fun streamRenderMarker(
        conversationId: Uuid,
        messageId: Uuid,
    ): AgentTimingStreamRenderMarker? = handleForMessage(conversationId, messageId)
        ?.let(::AgentTimingStreamRenderMarker)

    /** First-write-wins marker safe to call after a trace has reached a terminal state. */
    fun markFirstVisibleDraw(
        conversationId: Uuid,
        messageId: Uuid,
    ): Boolean = handleForMessage(conversationId, messageId)
        ?.markFirstVisibleDraw()
        ?: false

    /** Records that no viewport-intersecting draw was observable for the message. */
    fun markFirstVisibleNotObserved(
        conversationId: Uuid,
        messageId: Uuid,
    ): Boolean = handleForMessage(conversationId, messageId)
        ?.markFirstVisibleNotObserved()
        ?: false

    internal fun isHandleEnabled(sequence: Long): Boolean =
        recordingEnabled.get() && synchronized(lock) {
            traces[sequence]?.handle?.isLocallyEnabled == true
        }

    internal fun bindCommand(
        sequence: Long,
        commandId: Uuid,
    ): Boolean = mutate(sequence) { trace, _ ->
        if (trace.status.isTerminal) return@mutate false
        if (trace.commandId == null) trace.commandId = commandId
        trace.commandId == commandId
    }

    internal fun bindRun(
        sequence: Long,
        runId: Uuid,
    ): Boolean = mutate(sequence) { trace, _ ->
        if (trace.status.isTerminal) return@mutate false
        bindRunLocked(trace, runId)
        true
    }

    internal fun bindAssistantMessage(
        sequence: Long,
        messageId: Uuid,
    ): Boolean = mutate(sequence, publish = true) { trace, _ ->
        val previous = traceByMessageId[messageId]
        if (previous != null && previous != sequence) return@mutate false
        if (previous == sequence && messageId in trace.assistantMessageIds) return@mutate false
        if (messageId !in trace.assistantMessageIds) trace.assistantMessageIds += messageId
        traceByMessageId[messageId] = sequence
        true
    }

    internal fun beginRound(
        sequence: Long,
        providerCallIndex: Int,
        attemptIndex: Int,
        responseMode: AgentTimingResponseMode,
        runtimeRunId: Uuid?,
    ): AgentTimingRoundRef? {
        if (!recordingEnabled.get()) return null
        return try {
            synchronized(lock) {
                val trace = mutableTraceForRecordingLocked(sequence) ?: return@synchronized null
                if (trace.status.isTerminal || trace.rounds.size >= limits.maxRoundsPerTrace) {
                    if (!trace.status.isTerminal) trace.droppedRoundCount++
                    return@synchronized null
                }
                runtimeRunId?.let { bindRunLocked(trace, it) }
                val ordinal = trace.rounds.size
                val ref = AgentTimingRoundRef(
                    traceSequence = sequence,
                    ordinal = ordinal,
                    runtimeRunId = runtimeRunId,
                    providerCallIndex = providerCallIndex,
                    attemptIndex = attemptIndex,
                )
                trace.rounds += MutableRound(
                    ref = ref,
                    responseMode = responseMode,
                    runtimeRunId = runtimeRunId,
                )
                ref
            }
        } catch (_: Throwable) {
            null
        }
    }

    internal fun updateRoundUsage(
        sequence: Long,
        round: AgentTimingRoundRef,
        promptTokens: Int,
        completionTokens: Int,
        cachedTokens: Int,
    ): Boolean {
        if (!recordingEnabled.get()) return false
        return try {
            synchronized(lock) {
                val trace = mutableTraceForRecordingLocked(sequence) ?: return@synchronized false
                val target = trace.rounds.getOrNull(round.ordinal)
                    ?.takeIf { it.ref == round }
                    ?: return@synchronized false
                target.promptTokens = promptTokens.coerceAtLeast(0)
                target.completionTokens = completionTokens.coerceAtLeast(0)
                target.cachedTokens = cachedTokens.coerceIn(0, target.promptTokens ?: 0)
                true
            }
        } catch (_: Throwable) {
            false
        }
    }

    internal fun recordRoundVisibleProgress(
        sequence: Long,
        round: AgentTimingRoundRef,
        stage: AgentTimingStreamStage,
        estimatedTokens: Long,
    ): Boolean {
        if (!recordingEnabled.get() || estimatedTokens <= 0L) return false
        return try {
            synchronized(lock) {
                val trace = mutableTraceForRecordingLocked(sequence) ?: return@synchronized false
                val target = trace.rounds.getOrNull(round.ordinal)
                    ?.takeIf { it.ref == round }
                    ?: return@synchronized false
                val now = clock.elapsedRealtimeNanos()
                when (stage) {
                    AgentTimingStreamStage.PROVIDER_RAW_VISIBLE ->
                        target.providerVisibleStream.record(estimatedTokens, now)
                    AgentTimingStreamStage.SESSION_CONSUMER_VISIBLE ->
                        target.sessionVisibleStream.record(estimatedTokens, now)
                }
                true
            }
        } catch (_: Throwable) {
            false
        }
    }

    internal fun recordUiVisibleFrame(
        sequence: Long,
        cumulativeEstimatedTokens: Long,
    ): Boolean {
        if (!recordingEnabled.get() || cumulativeEstimatedTokens <= 0L) return false
        return try {
            synchronized(lock) {
                val trace = mutableTraceForRecordingLocked(sequence) ?: return@synchronized false
                val delta = (cumulativeEstimatedTokens - trace.lastUiVisibleTokens).coerceAtLeast(0L)
                trace.lastUiVisibleTokens = maxOf(trace.lastUiVisibleTokens, cumulativeEstimatedTokens)
                if (delta <= 0L) return@synchronized false
                val target = trace.rounds.lastOrNull {
                    it.milestones[AgentTimingEventKind.APP_PROVIDER_DISPATCH] != null
                } ?: return@synchronized false
                target.uiVisibleStream.record(delta, clock.elapsedRealtimeNanos())
                true
            }
        } catch (_: Throwable) {
            false
        }
    }

    internal fun registerTool(
        sequence: Long,
        round: AgentTimingRoundRef?,
        toolCallId: String?,
        assistantMessageId: Uuid?,
    ): AgentTimingToolRef? {
        if (!recordingEnabled.get()) return null
        return try {
            synchronized(lock) {
                val trace = mutableTraceForRecordingLocked(sequence) ?: return@synchronized null
                if (trace.status.isTerminal || trace.tools.size >= limits.maxToolsPerTrace) {
                    if (!trace.status.isTerminal) trace.droppedToolCount++
                    return@synchronized null
                }
                val validRound = round?.takeIf {
                    it.traceSequence == sequence && trace.rounds.getOrNull(it.ordinal)?.ref == it
                }
                val ordinal = trace.tools.size
                val ref = AgentTimingToolRef(
                    traceSequence = sequence,
                    ordinal = ordinal,
                    roundOrdinal = validRound?.ordinal,
                )
                trace.tools += MutableTool(
                    ref = ref,
                    toolCallId = toolCallId?.takeIf { it.length <= MAX_ASSOCIATION_ID_LENGTH },
                    assistantMessageId = assistantMessageId,
                )
                ref
            }
        } catch (_: Throwable) {
            null
        }
    }

    internal fun bindToolMessage(
        sequence: Long,
        tool: AgentTimingToolRef,
        messageId: Uuid,
    ): Boolean = mutate(sequence) { trace, _ ->
        val mutableTool = trace.validTool(tool) ?: return@mutate false
        mutableTool.assistantMessageId = messageId
        true
    }

    internal fun mark(
        sequence: Long,
        kind: AgentTimingEventKind,
        result: AgentTimingEventResult,
        round: AgentTimingRoundRef?,
        tool: AgentTimingToolRef?,
        publish: Boolean,
        onlyOnce: Boolean = false,
    ): Boolean {
        if (!recordingEnabled.get()) return false
        return try {
            synchronized(lock) {
                val trace = mutableTraceForRecordingLocked(sequence) ?: return@synchronized false
                val allowedAfterTerminal = kind == AgentTimingEventKind.FIRST_VISIBLE_DRAW ||
                    kind == AgentTimingEventKind.FIRST_VISIBLE_NOT_OBSERVED
                if (trace.status.isTerminal && !allowedAfterTerminal) return@synchronized false
                val atNs = clock.elapsedRealtimeNanos()
                val changed = recordAtLocked(
                    trace = trace,
                    kind = kind,
                    atNs = atNs,
                    result = result,
                    round = round,
                    tool = tool,
                    onlyOnce = onlyOnce,
                )
                if (changed && publish) publishLocked(setOf(trace.conversationId))
                changed
            }
        } catch (_: Throwable) {
            false
        }
    }

    internal fun markCapturedVisibleOutcome(
        sequence: Long,
        kind: AgentTimingEventKind,
        atNs: Long,
    ): Boolean {
        if (!recordingEnabled.get()) return false
        return try {
            synchronized(lock) {
                val trace = mutableTraceForRecordingLocked(sequence) ?: return@synchronized false
                val allowedAfterTerminal = kind == AgentTimingEventKind.FIRST_VISIBLE_DRAW ||
                    kind == AgentTimingEventKind.FIRST_VISIBLE_NOT_OBSERVED
                if (trace.status.isTerminal && !allowedAfterTerminal) return@synchronized false
                val changed = recordAtLocked(
                    trace = trace,
                    kind = kind,
                    atNs = atNs,
                    result = AgentTimingEventResult.NONE,
                    round = null,
                    tool = null,
                    onlyOnce = true,
                )
                if (changed) publishLocked(setOf(trace.conversationId))
                changed
            }
        } catch (_: Throwable) {
            false
        }
    }

    /** Clock-only helper for the draw marker. It never acquires the store lock. */
    internal fun captureTimestampNs(): Long? {
        if (!recordingEnabled.get()) return null
        return try {
            clock.elapsedRealtimeNanos()
        } catch (_: Throwable) {
            null
        }
    }

    internal fun approvalPending(
        sequence: Long,
        pendingCount: Int,
        tool: AgentTimingToolRef?,
    ): Boolean {
        if (!recordingEnabled.get()) return false
        return try {
            synchronized(lock) {
                val trace = mutableTraceForRecordingLocked(sequence) ?: return@synchronized false
                if (trace.status.isTerminal) return@synchronized false
                val now = clock.elapsedRealtimeNanos()
                if (trace.currentApproval() == null) {
                    trace.closeCurrentActiveSegment(now)
                    trace.approvalSegments += MutableApprovalSegment(startedAtNs = now)
                }
                trace.status = AgentTimingTraceStatus.WAITING_APPROVAL
                trace.pendingApprovalCount = pendingCount.coerceAtLeast(1)
                recordAtLocked(
                    trace = trace,
                    kind = AgentTimingEventKind.APPROVAL_PENDING,
                    atNs = now,
                    result = AgentTimingEventResult.NONE,
                    round = null,
                    tool = tool,
                    onlyOnce = false,
                )
                publishLocked(setOf(trace.conversationId))
                true
            }
        } catch (_: Throwable) {
            false
        }
    }

    internal fun approvalDecision(
        sequence: Long,
        remainingPendingCount: Int,
        result: AgentTimingEventResult,
        tool: AgentTimingToolRef?,
    ): Boolean {
        if (!recordingEnabled.get()) return false
        return try {
            synchronized(lock) {
                val trace = mutableTraceForRecordingLocked(sequence) ?: return@synchronized false
                if (trace.status.isTerminal) return@synchronized false
                val now = clock.elapsedRealtimeNanos()
                trace.pendingApprovalCount = remainingPendingCount.coerceAtLeast(0)
                if (remainingPendingCount <= 0) {
                    trace.currentApproval()?.let { segment ->
                        if (segment.userDecisionAtNs == null) segment.userDecisionAtNs = now
                    }
                }
                recordAtLocked(
                    trace = trace,
                    kind = AgentTimingEventKind.APPROVAL_DECISION,
                    atNs = now,
                    result = result,
                    round = null,
                    tool = tool,
                    onlyOnce = false,
                )
                publishLocked(setOf(trace.conversationId))
                true
            }
        } catch (_: Throwable) {
            false
        }
    }

    internal fun approvalDecisionSubmitted(
        sequence: Long,
        result: AgentTimingEventResult,
        tool: AgentTimingToolRef?,
    ): Boolean {
        if (!recordingEnabled.get()) return false
        return try {
            synchronized(lock) {
                val trace = mutableTraceForRecordingLocked(sequence) ?: return@synchronized false
                if (trace.status.isTerminal) return@synchronized false
                val now = clock.elapsedRealtimeNanos()
                trace.pendingApprovalCount = (trace.pendingApprovalCount - 1).coerceAtLeast(0)
                if (trace.pendingApprovalCount == 0) {
                    trace.currentApproval()?.let { segment ->
                        if (segment.userDecisionAtNs == null) segment.userDecisionAtNs = now
                    }
                }
                recordAtLocked(
                    trace = trace,
                    kind = AgentTimingEventKind.APPROVAL_DECISION,
                    atNs = now,
                    result = result,
                    round = null,
                    tool = tool,
                    onlyOnce = false,
                )
                publishLocked(setOf(trace.conversationId))
                true
            }
        } catch (_: Throwable) {
            false
        }
    }

    internal fun resumeActiveSegment(
        sequence: Long,
        runId: Uuid,
    ): Boolean {
        if (!recordingEnabled.get()) return false
        return try {
            synchronized(lock) {
                val trace = mutableTraceForRecordingLocked(sequence) ?: return@synchronized false
                if (trace.status.isTerminal) return@synchronized false
                val now = clock.elapsedRealtimeNanos()
                bindRunLocked(trace, runId)
                trace.currentApproval()?.let { segment ->
                    segment.resumedAtNs = now
                    segment.endedAtNs = now
                    recordAtLocked(
                        trace = trace,
                        kind = AgentTimingEventKind.APPROVAL_GROUP_WAIT_FINISHED,
                        atNs = now,
                        result = AgentTimingEventResult.SUCCESS,
                        round = null,
                        tool = null,
                        onlyOnce = false,
                    )
                }
                if (trace.currentActive() == null) {
                    trace.activeSegments += MutableActiveSegment(startedAtNs = now)
                }
                trace.status = AgentTimingTraceStatus.ACTIVE
                trace.pendingApprovalCount = 0
                recordAtLocked(
                    trace = trace,
                    kind = AgentTimingEventKind.RESUME_STARTED,
                    atNs = now,
                    result = AgentTimingEventResult.NONE,
                    round = null,
                    tool = null,
                    onlyOnce = false,
                )
                publishLocked(setOf(trace.conversationId))
                true
            }
        } catch (_: Throwable) {
            false
        }
    }

    internal fun finish(
        sequence: Long,
        status: AgentTimingTraceStatus,
    ): Boolean {
        if (!recordingEnabled.get() || !status.isTerminal || status == AgentTimingTraceStatus.DISABLED) {
            return false
        }
        return try {
            synchronized(lock) {
                val trace = mutableTraceForRecordingLocked(sequence) ?: return@synchronized false
                if (trace.status.isTerminal) return@synchronized false
                val now = clock.elapsedRealtimeNanos()
                trace.closeCurrentActiveSegment(now)
                trace.currentApproval()?.let { segment -> segment.endedAtNs = now }
                trace.status = status
                trace.finishedAtNs = now
                recordAtLocked(
                    trace = trace,
                    kind = status.terminalEventKind(),
                    atNs = now,
                    result = status.terminalEventResult(),
                    round = null,
                    tool = null,
                    onlyOnce = true,
                )
                val affected = linkedSetOf(trace.conversationId)
                trimTerminalTracesLocked(affected)
                publishLocked(affected)
                true
            }
        } catch (_: Throwable) {
            false
        }
    }

    private inline fun mutate(
        sequence: Long,
        publish: Boolean = false,
        mutation: (MutableTrace, Long) -> Boolean,
    ): Boolean {
        if (!recordingEnabled.get()) return false
        return try {
            synchronized(lock) {
                val trace = mutableTraceForRecordingLocked(sequence) ?: return@synchronized false
                val changed = mutation(trace, trace.lastEventAtNs)
                if (changed && publish) publishLocked(setOf(trace.conversationId))
                changed
            }
        } catch (_: Throwable) {
            false
        }
    }

    private fun mutableTraceForRecordingLocked(sequence: Long): MutableTrace? {
        if (!recordingEnabled.get()) return null
        val trace = traces[sequence] ?: return null
        if (!trace.handle.isLocallyEnabled) return null
        return trace
    }

    private fun bindRunLocked(trace: MutableTrace, runId: Uuid) {
        val previous = traceByRunId[runId]
        if (previous != null && previous != trace.sequence) return
        if (runId !in trace.runtimeRunIds) trace.runtimeRunIds += runId
        traceByRunId[runId] = trace.sequence
    }

    private fun recordAtLocked(
        trace: MutableTrace,
        kind: AgentTimingEventKind,
        atNs: Long,
        result: AgentTimingEventResult,
        round: AgentTimingRoundRef?,
        tool: AgentTimingToolRef?,
        onlyOnce: Boolean,
    ): Boolean {
        if (
            (kind == AgentTimingEventKind.FIRST_VISIBLE_DRAW ||
                kind == AgentTimingEventKind.FIRST_VISIBLE_NOT_OBSERVED) &&
            (trace.milestones.containsKey(AgentTimingEventKind.FIRST_VISIBLE_DRAW) ||
                trace.milestones.containsKey(AgentTimingEventKind.FIRST_VISIBLE_NOT_OBSERVED))
        ) {
            return false
        }
        if (onlyOnce && trace.milestones.containsKey(kind)) return false
        trace.lastEventAtNs = maxOf(trace.lastEventAtNs, atNs)
        trace.milestones.mergeEarliest(kind, atNs)

        val validRound = round?.takeIf { it.traceSequence == trace.sequence }
            ?.let { trace.rounds.getOrNull(it.ordinal) }
            ?.takeIf { it.ref == round }
        validRound?.milestones?.mergeEarliest(kind, atNs)
        if (kind == AgentTimingEventKind.PROVIDER_ATTEMPT_TERMINAL) {
            validRound?.terminalResult = result
        }

        val validTool = tool?.takeIf { it.traceSequence == trace.sequence }
            ?.let { trace.tools.getOrNull(it.ordinal) }
            ?.takeIf { it.ref == tool }
        validTool?.milestones?.mergeEarliest(kind, atNs)
        if (kind == AgentTimingEventKind.TOOL_TERMINAL) {
            validTool?.terminalResult = result
        }

        trace.aggregates.getOrPut(kind) { MutableAggregate() }.record(atNs)
        if (trace.events.size < limits.maxEventsPerTrace) {
            trace.events += AgentTimingEvent(
                sequence = trace.events.size,
                kind = kind,
                atNs = atNs,
                result = result,
                roundOrdinal = validRound?.ref?.ordinal,
                toolOrdinal = validTool?.ref?.ordinal,
            )
        } else {
            trace.droppedEventCount++
        }
        return true
    }

    private fun trimTerminalTracesLocked(affected: MutableSet<Uuid>) {
        traces.values
            .groupBy { it.conversationId }
            .forEach { (conversationId, conversationTraces) ->
                val terminal = conversationTraces
                    .filter { it.status.isTerminal }
                    .sortedBy { it.finishedAtNs ?: Long.MAX_VALUE }
                    .toMutableList()
                while (terminal.size > limits.maxTerminalTracesPerConversation) {
                    val evicted = terminal.removeAt(0)
                    affected += conversationId
                    removeTraceLocked(evicted.sequence)
                }
            }

        val globalTerminal = traces.values
            .filter { it.status.isTerminal }
            .sortedBy { it.finishedAtNs ?: Long.MAX_VALUE }
            .toMutableList()
        while (globalTerminal.size > limits.maxTerminalTracesGlobal) {
            val evicted = globalTerminal.removeAt(0)
            affected += evicted.conversationId
            removeTraceLocked(evicted.sequence)
        }
    }

    private fun removeTraceLocked(sequence: Long) {
        val trace = traces.remove(sequence) ?: return
        trace.handle.disable()
        trace.runtimeRunIds.forEach { runId -> traceByRunId.remove(runId, sequence) }
        trace.assistantMessageIds.forEach { messageId -> traceByMessageId.remove(messageId, sequence) }
    }

    private fun publishLocked(conversationIds: Set<Uuid>) {
        conversationIds.forEach { conversationId ->
            conversationFlows[conversationId]?.let { flow ->
                flow.value = snapshotLocked(conversationId)
                publicationCount++
            }
        }
    }

    private fun snapshotLocked(conversationId: Uuid): AgentTimingConversationSnapshot =
        AgentTimingConversationSnapshot(
            conversationId = conversationId,
            traces = traces.values
                .asSequence()
                .filter { it.conversationId == conversationId }
                .sortedByDescending { it.sequence }
                .map { it.snapshot() }
                .toList(),
        )

    internal fun debugStats(): AgentTimingStoreDebugStats = synchronized(lock) {
        AgentTimingStoreDebugStats(
            entryCount = traces.size,
            activeEntryCount = traces.values.count { !it.status.isTerminal },
            terminalEntryCount = traces.values.count { it.status.isTerminal },
            publicationCount = publicationCount,
        )
    }

    private class MutableTrace(
        val sequence: Long,
        val conversationId: Uuid,
        val submittedAtNs: Long,
        val handle: AgentTimingHandle,
    ) {
        var commandId: Uuid? = null
        val runtimeRunIds = mutableListOf<Uuid>()
        val assistantMessageIds = mutableListOf<Uuid>()
        var status = AgentTimingTraceStatus.ACTIVE
        var finishedAtNs: Long? = null
        var lastEventAtNs: Long = submittedAtNs
        var pendingApprovalCount: Int = 0
        val milestones = EnumMap<AgentTimingEventKind, Long>(AgentTimingEventKind::class.java)
        val rounds = mutableListOf<MutableRound>()
        val tools = mutableListOf<MutableTool>()
        val events = mutableListOf<AgentTimingEvent>()
        val aggregates = EnumMap<AgentTimingEventKind, MutableAggregate>(AgentTimingEventKind::class.java)
        val activeSegments = mutableListOf(MutableActiveSegment(startedAtNs = submittedAtNs))
        val approvalSegments = mutableListOf<MutableApprovalSegment>()
        var droppedEventCount: Long = 0L
        var droppedRoundCount: Long = 0L
        var droppedToolCount: Long = 0L
        var lastUiVisibleTokens: Long = 0L

        fun currentActive(): MutableActiveSegment? =
            activeSegments.lastOrNull()?.takeIf { it.finishedAtNs == null }

        fun currentApproval(): MutableApprovalSegment? =
            approvalSegments.lastOrNull()?.takeIf { it.endedAtNs == null }

        fun closeCurrentActiveSegment(atNs: Long) {
            currentActive()?.finishedAtNs = atNs
        }

        fun validTool(ref: AgentTimingToolRef): MutableTool? =
            ref.takeIf { it.traceSequence == sequence }
                ?.let { tools.getOrNull(it.ordinal) }
                ?.takeIf { it.ref == ref }

        fun submissionToken() = AgentTimingSubmissionToken(
            traceSequence = sequence,
            conversationId = conversationId,
            submittedAtNs = submittedAtNs,
            handle = handle,
        )

        fun snapshot(): AgentTimingTraceSnapshot {
            val rawRounds = rounds.map { it.snapshot() }
            val roundSnapshots = rawRounds.mapIndexed { index, round ->
                val previousResultsReadyAtNs = rawRounds
                    .getOrNull(index - 1)
                    ?.at(AgentTimingEventKind.MODEL_RESULTS_READY)
                round.copy(
                    handoffFromPreviousResultsNs = durationBetween(
                        previousResultsReadyAtNs,
                        round.at(AgentTimingEventKind.APP_PROVIDER_DISPATCH),
                    ),
                )
            }
            val toolSnapshots = tools.map { tool ->
                val snapshot = tool.snapshot()
                val producingRoundOrdinal = snapshot.roundOrdinal
                val producingRound = producingRoundOrdinal?.let(roundSnapshots::getOrNull)
                val nextProviderRound = producingRoundOrdinal?.let { ordinal ->
                    roundSnapshots
                        .asSequence()
                        .drop(ordinal + 1)
                        .firstOrNull { it.at(AgentTimingEventKind.APP_PROVIDER_DISPATCH) != null }
                }
                snapshot.copy(
                    sharedModelResultsReadyAtNs =
                        producingRound?.at(AgentTimingEventKind.MODEL_RESULTS_READY),
                    nextProviderDispatchAtNs =
                        nextProviderRound?.at(AgentTimingEventKind.APP_PROVIDER_DISPATCH),
                    nextProviderFirstProgressAtNs =
                        nextProviderRound?.at(AgentTimingEventKind.PROVIDER_FIRST_PROGRESS),
                )
            }
            return AgentTimingTraceSnapshot(
                traceSequence = sequence,
                conversationId = conversationId,
                commandId = commandId,
                submittedAtNs = submittedAtNs,
                runtimeRunIds = runtimeRunIds.toList(),
                assistantMessageIds = assistantMessageIds.toList(),
                status = status,
                pendingApprovalCount = pendingApprovalCount,
                finishedAtNs = finishedAtNs,
                lastEventAtNs = lastEventAtNs,
                milestones = milestones.toMap(),
                rounds = roundSnapshots,
                tools = toolSnapshots,
                events = events.toList(),
                aggregates = aggregates.map { (kind, aggregate) -> aggregate.snapshot(kind) },
                activeSegments = activeSegments.map { it.snapshot() },
                approvalSegments = approvalSegments.map { it.snapshot() },
                droppedEventCount = droppedEventCount,
                droppedRoundCount = droppedRoundCount,
                droppedToolCount = droppedToolCount,
            )
        }
    }

    private data class MutableRound(
        val ref: AgentTimingRoundRef,
        val responseMode: AgentTimingResponseMode,
        val runtimeRunId: Uuid?,
        var terminalResult: AgentTimingEventResult? = null,
        var promptTokens: Int? = null,
        var completionTokens: Int? = null,
        var cachedTokens: Int? = null,
        val providerVisibleStream: MutableStreamProgress = MutableStreamProgress(),
        val sessionVisibleStream: MutableStreamProgress = MutableStreamProgress(),
        val uiVisibleStream: MutableStreamProgress = MutableStreamProgress(),
        val milestones: EnumMap<AgentTimingEventKind, Long> =
            EnumMap(AgentTimingEventKind::class.java),
    ) {
        fun snapshot() = AgentTimingRoundSnapshot(
            ordinal = ref.ordinal,
            providerCallIndex = ref.providerCallIndex,
            attemptIndex = ref.attemptIndex,
            responseMode = responseMode,
            runtimeRunId = runtimeRunId,
            milestones = milestones.toMap(),
            terminalResult = terminalResult,
            promptTokens = promptTokens,
            completionTokens = completionTokens,
            cachedTokens = cachedTokens,
            providerVisibleStream = providerVisibleStream.snapshot(),
            sessionVisibleStream = sessionVisibleStream.snapshot(),
            uiVisibleStream = uiVisibleStream.snapshot(),
        )
    }

    private data class MutableStreamProgress(
        var estimatedTokens: Long = 0L,
        var sampleCount: Long = 0L,
        var firstAtNs: Long? = null,
        var lastAtNs: Long? = null,
    ) {
        fun record(deltaTokens: Long, atNs: Long) {
            if (deltaTokens <= 0L) return
            if (firstAtNs == null) firstAtNs = atNs
            lastAtNs = atNs
            estimatedTokens += deltaTokens
            sampleCount += 1L
        }

        fun snapshot() = AgentTimingStreamProgressSnapshot(
            estimatedTokens = estimatedTokens,
            sampleCount = sampleCount,
            firstAtNs = firstAtNs,
            lastAtNs = lastAtNs,
        )
    }

    private data class MutableTool(
        val ref: AgentTimingToolRef,
        val toolCallId: String?,
        var assistantMessageId: Uuid?,
        var terminalResult: AgentTimingEventResult? = null,
        val milestones: EnumMap<AgentTimingEventKind, Long> =
            EnumMap(AgentTimingEventKind::class.java),
    ) {
        fun snapshot() = AgentTimingToolSnapshot(
            ordinal = ref.ordinal,
            roundOrdinal = ref.roundOrdinal,
            toolCallId = toolCallId,
            assistantMessageId = assistantMessageId,
            milestones = milestones.toMap(),
            terminalResult = terminalResult,
        )
    }

    private data class MutableActiveSegment(
        val startedAtNs: Long,
        var finishedAtNs: Long? = null,
    ) {
        fun snapshot() = AgentTimingActiveSegmentSnapshot(startedAtNs, finishedAtNs)
    }

    private data class MutableApprovalSegment(
        val startedAtNs: Long,
        var userDecisionAtNs: Long? = null,
        var resumedAtNs: Long? = null,
        var endedAtNs: Long? = null,
    ) {
        fun snapshot() = AgentTimingApprovalSegmentSnapshot(
            startedAtNs = startedAtNs,
            userDecisionAtNs = userDecisionAtNs,
            resumedAtNs = resumedAtNs,
            endedAtNs = endedAtNs,
        )
    }

    private class MutableAggregate {
        var count: Long = 0L
        var firstAtNs: Long = Long.MAX_VALUE
        var lastAtNs: Long = Long.MIN_VALUE

        fun record(atNs: Long) {
            count++
            firstAtNs = minOf(firstAtNs, atNs)
            lastAtNs = maxOf(lastAtNs, atNs)
        }

        fun snapshot(kind: AgentTimingEventKind) = AgentTimingEventAggregate(
            kind = kind,
            count = count,
            firstAtNs = firstAtNs,
            lastAtNs = lastAtNs,
        )
    }

    private fun EnumMap<AgentTimingEventKind, Long>.mergeEarliest(
        kind: AgentTimingEventKind,
        atNs: Long,
    ) {
        val existing = this[kind]
        if (existing == null || atNs < existing) this[kind] = atNs
    }

    private fun AgentTimingTraceStatus.terminalEventKind(): AgentTimingEventKind = when (this) {
        AgentTimingTraceStatus.COMPLETED -> AgentTimingEventKind.TRACE_COMPLETED
        AgentTimingTraceStatus.FAILED -> AgentTimingEventKind.TRACE_FAILED
        AgentTimingTraceStatus.CANCELLED -> AgentTimingEventKind.TRACE_CANCELLED
        AgentTimingTraceStatus.TIMED_OUT -> AgentTimingEventKind.TRACE_TIMED_OUT
        AgentTimingTraceStatus.CONTEXT_OVERFLOW -> AgentTimingEventKind.TRACE_CONTEXT_OVERFLOW
        AgentTimingTraceStatus.EVICTED -> AgentTimingEventKind.TRACE_EVICTED
        else -> error("Not a visible terminal status: $this")
    }

    private fun AgentTimingTraceStatus.terminalEventResult(): AgentTimingEventResult = when (this) {
        AgentTimingTraceStatus.COMPLETED -> AgentTimingEventResult.SUCCESS
        AgentTimingTraceStatus.CANCELLED, AgentTimingTraceStatus.EVICTED -> AgentTimingEventResult.CANCELLED
        AgentTimingTraceStatus.TIMED_OUT -> AgentTimingEventResult.TIMED_OUT
        AgentTimingTraceStatus.FAILED, AgentTimingTraceStatus.CONTEXT_OVERFLOW -> AgentTimingEventResult.FAILED
        else -> AgentTimingEventResult.NONE
    }

    companion object {
        private const val MAX_ASSOCIATION_ID_LENGTH = 256
    }
}

class AgentTimingSubmissionToken internal constructor(
    val traceSequence: Long,
    val conversationId: Uuid,
    val submittedAtNs: Long,
    val handle: AgentTimingHandle,
)

/** Request-scoped recorder. Keep it nullable at call sites; there is intentionally no no-op object. */
class AgentTimingHandle internal constructor(
    private val store: AgentTimingStore,
    val traceSequence: Long,
) {
    private val locallyEnabled = AtomicBoolean(true)
    private val encodedVisibleOutcome = AtomicLong(VISIBLE_OUTCOME_UNSET)
    private val visibleOutcomePublished = AtomicBoolean(false)

    val isRecording: Boolean
        get() = locallyEnabled.get() && store.isHandleEnabled(traceSequence)

    internal val isLocallyEnabled: Boolean
        get() = locallyEnabled.get()

    internal fun disable() {
        locallyEnabled.set(false)
    }

    fun bindCommand(commandId: Uuid): Boolean = safeCall {
        store.bindCommand(traceSequence, commandId)
    }

    fun bindRun(runId: Uuid): Boolean = safeCall {
        store.bindRun(traceSequence, runId)
    }

    fun bindAssistantMessage(messageId: Uuid): Boolean = safeCall {
        store.bindAssistantMessage(traceSequence, messageId)
    }

    fun beginRound(
        providerCallIndex: Int,
        attemptIndex: Int = 0,
        responseMode: AgentTimingResponseMode = AgentTimingResponseMode.UNKNOWN,
        runtimeRunId: Uuid? = null,
    ): AgentTimingRoundRef? {
        if (!locallyEnabled.get()) return null
        return try {
            store.beginRound(
                sequence = traceSequence,
                providerCallIndex = providerCallIndex,
                attemptIndex = attemptIndex,
                responseMode = responseMode,
                runtimeRunId = runtimeRunId,
            )
        } catch (_: Throwable) {
            null
        }
    }

    fun updateRoundUsage(
        round: AgentTimingRoundRef,
        promptTokens: Int,
        completionTokens: Int,
        cachedTokens: Int,
    ): Boolean = safeCall {
        store.updateRoundUsage(
            sequence = traceSequence,
            round = round,
            promptTokens = promptTokens,
            completionTokens = completionTokens,
            cachedTokens = cachedTokens,
        )
    }

    fun recordRoundVisibleProgress(
        round: AgentTimingRoundRef,
        stage: AgentTimingStreamStage,
        estimatedTokens: Long,
    ): Boolean = safeCall {
        store.recordRoundVisibleProgress(
            sequence = traceSequence,
            round = round,
            stage = stage,
            estimatedTokens = estimatedTokens,
        )
    }

    fun recordUiVisibleFrame(cumulativeEstimatedTokens: Long): Boolean = safeCall {
        store.recordUiVisibleFrame(traceSequence, cumulativeEstimatedTokens)
    }

    fun registerTool(
        round: AgentTimingRoundRef? = null,
        toolCallId: String? = null,
        assistantMessageId: Uuid? = null,
    ): AgentTimingToolRef? {
        if (!locallyEnabled.get()) return null
        return try {
            store.registerTool(
                sequence = traceSequence,
                round = round,
                toolCallId = toolCallId,
                assistantMessageId = assistantMessageId,
            )
        } catch (_: Throwable) {
            null
        }
    }

    fun bindToolMessage(
        tool: AgentTimingToolRef,
        messageId: Uuid,
    ): Boolean = safeCall {
        store.bindToolMessage(traceSequence, tool, messageId)
    }

    /** Records without publishing a StateFlow snapshot. Suitable for hot internal seams. */
    fun mark(
        kind: AgentTimingEventKind,
        round: AgentTimingRoundRef? = null,
        tool: AgentTimingToolRef? = null,
        result: AgentTimingEventResult = AgentTimingEventResult.NONE,
    ): Boolean = safeCall {
        store.mark(
            sequence = traceSequence,
            kind = kind,
            result = result,
            round = round,
            tool = tool,
            publish = false,
        )
    }

    /** Records and publishes one bounded conversation snapshot at a meaningful UI checkpoint. */
    fun checkpoint(
        kind: AgentTimingEventKind,
        round: AgentTimingRoundRef? = null,
        tool: AgentTimingToolRef? = null,
        result: AgentTimingEventResult = AgentTimingEventResult.NONE,
    ): Boolean = safeCall {
        store.mark(
            sequence = traceSequence,
            kind = kind,
            result = result,
            round = round,
            tool = tool,
            publish = true,
        )
    }

    /** First-write-wins checkpoint for one-shot UI boundaries such as session content ready. */
    fun checkpointOnce(
        kind: AgentTimingEventKind,
        round: AgentTimingRoundRef? = null,
        tool: AgentTimingToolRef? = null,
        result: AgentTimingEventResult = AgentTimingEventResult.NONE,
    ): Boolean = safeCall {
        store.mark(
            sequence = traceSequence,
            kind = kind,
            result = result,
            round = round,
            tool = tool,
            publish = true,
            onlyOnce = true,
        )
    }

    fun approvalPending(
        pendingCount: Int,
        tool: AgentTimingToolRef? = null,
    ): Boolean = safeCall {
        store.approvalPending(traceSequence, pendingCount, tool)
    }

    fun approvalDecision(
        remainingPendingCount: Int,
        result: AgentTimingEventResult = AgentTimingEventResult.SUCCESS,
        tool: AgentTimingToolRef? = null,
    ): Boolean = safeCall {
        store.approvalDecision(traceSequence, remainingPendingCount, result, tool)
    }

    /** Atomically accounts one submitted decision in the current multi-tool approval group. */
    fun approvalDecisionSubmitted(
        result: AgentTimingEventResult = AgentTimingEventResult.SUCCESS,
        tool: AgentTimingToolRef? = null,
    ): Boolean = safeCall {
        store.approvalDecisionSubmitted(traceSequence, result, tool)
    }

    /** Closes approval suspension, binds the resume run, and starts a new active interval. */
    fun resumeActiveSegment(runId: Uuid): Boolean = safeCall {
        store.resumeActiveSegment(traceSequence, runId)
    }

    /**
     * Draw-thread path: one enabled check, one clock read, and one CAS. It does not lock, allocate,
     * aggregate, or publish. Call [publishCapturedVisibleOutcome] later from a posted callback.
     */
    fun captureFirstVisibleDraw(): Boolean = captureVisibleOutcome(isDraw = true)

    fun captureFirstVisibleNotObserved(): Boolean =
        captureVisibleOutcome(isDraw = false)

    fun publishCapturedVisibleOutcome(): Boolean {
        if (!locallyEnabled.get() || visibleOutcomePublished.get()) return false
        val encoded = encodedVisibleOutcome.get()
        if (encoded == VISIBLE_OUTCOME_UNSET) return false
        val kind = if (encoded > 0L) {
            AgentTimingEventKind.FIRST_VISIBLE_DRAW
        } else {
            AgentTimingEventKind.FIRST_VISIBLE_NOT_OBSERVED
        }
        val capturedAtNs = if (encoded > 0L) encoded - 1L else -encoded - 1L
        val changed = try {
            store.markCapturedVisibleOutcome(
                sequence = traceSequence,
                kind = kind,
                atNs = capturedAtNs,
            )
        } catch (_: Throwable) {
            false
        }
        if (changed) visibleOutcomePublished.set(true)
        return changed
    }

    /** Convenience for non-draw callers. Draw modifiers should use capture + deferred publish. */
    fun markFirstVisibleDraw(): Boolean =
        captureFirstVisibleDraw() && publishCapturedVisibleOutcome()

    fun markFirstVisibleNotObserved(): Boolean =
        captureFirstVisibleNotObserved() && publishCapturedVisibleOutcome()

    fun finish(status: AgentTimingTraceStatus): Boolean = safeCall {
        store.finish(traceSequence, status)
    }

    private inline fun safeCall(block: () -> Boolean): Boolean {
        if (!locallyEnabled.get()) return false
        return try {
            block()
        } catch (_: Throwable) {
            false
        }
    }

    private fun captureVisibleOutcome(isDraw: Boolean): Boolean {
        if (!locallyEnabled.get() || encodedVisibleOutcome.get() != VISIBLE_OUTCOME_UNSET) return false
        val atNs = store.captureTimestampNs() ?: return false
        if (atNs < 0L || atNs == Long.MAX_VALUE) return false
        val encoded = if (isDraw) atNs + 1L else -(atNs + 1L)
        return encodedVisibleOutcome.compareAndSet(VISIBLE_OUTCOME_UNSET, encoded)
    }

    private companion object {
        const val VISIBLE_OUTCOME_UNSET = 0L
    }
}

class AgentTimingFirstVisibleDrawMarker internal constructor(
    private val handle: AgentTimingHandle,
) {
    /** Invoke immediately after `drawContent()`. */
    fun captureAfterDraw(): Boolean = handle.captureFirstVisibleDraw()

    /** Invoke from a posted callback, never from the draw pass. */
    fun publishCaptured(): Boolean = handle.publishCapturedVisibleOutcome()
}

class AgentTimingStreamRenderMarker internal constructor(
    private val handle: AgentTimingHandle,
) {
    /** Call after a Compose frame with the cumulative visible assistant-text estimate. */
    fun recordVisibleFrame(cumulativeEstimatedTokens: Long): Boolean =
        handle.recordUiVisibleFrame(cumulativeEstimatedTokens)
}

internal data class AgentTimingStoreDebugStats(
    val entryCount: Int,
    val activeEntryCount: Int,
    val terminalEntryCount: Int,
    val publicationCount: Long,
)
