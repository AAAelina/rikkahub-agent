package me.rerere.rikkahub.ui.components.message

import androidx.annotation.VisibleForTesting
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingEventKind
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingConversationSnapshot
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingResponseMode
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingRoundSnapshot
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingStreamProgressSnapshot
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingToolSnapshot
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingTraceSnapshot
import java.util.Locale

internal const val AGENT_TIMING_LONG_STAGE_NS: Long = 1_000_000_000L

internal enum class AgentTimingFirstResponseKind {
    FIRST_PROGRESS,
    FULL_RESPONSE,
}

internal data class AgentTimingSummaryPresentation(
    val totalNs: Long,
    val excludingHumanWaitNs: Long,
    val firstResponseNs: Long?,
    val firstResponseKind: AgentTimingFirstResponseKind,
    val roundCount: Long,
    val toolCount: Long,
)

internal enum class AgentTimingMetricKind {
    ADMISSION,
    RUN_PREFLIGHT,
    MEMORY_RETRIEVAL,
    AUTO_CONTEXT,
    TOOL_SURFACE,
    MCP_DISCOVERY,
    CONTEXT,
    RECENT_CHATS,
    MEMORY_PROMPT,
    TOOL_PROMPT,
    SYSTEM_PROMPT,
    CONTEXT_GATE_INITIAL,
    INPUT_TRANSFORM,
    CONTEXT_GATE_FINAL,
    CONTEXT_COMPRESSION,
    TOKEN_COUNT,
    REQUEST_BUILD,
    DIAGNOSTICS,
    REQUEST_BREAKDOWN_BUILD,
    REQUEST_BREAKDOWN_WRITE,
    MEMORY_LAST_ACCESS,
    PROVIDER_PREPARE,
    FIRST_PROGRESS,
    FIRST_TEXT,
    FULL_RESPONSE,
    PROVIDER_TOTAL,
    TOOL_BATCH,
    TOOL_EXECUTION,
    TOOL_POST_PROCESSING,
    TOOL_BATCH_WAIT,
    HANDOFF,
    HUMAN_WAIT,
    APPROVAL_RESOLUTION,
    FINAL_SAVE,
    GENERATION_DONE_NOTIFY,
    POST_PROVIDER_PROCESSING,
    RUN_COMPLETION_DRAIN,
    RUNTIME_FINALIZATION,
}

internal enum class AgentTimingSectionKind {
    OVERVIEW,
    CONTEXT,
    DIAGNOSTICS,
    PROVIDER,
    TOOLS,
    HANDOFF,
    APPROVAL,
}

internal data class AgentTimingMetricPresentation(
    val kind: AgentTimingMetricKind,
    val durationNs: Long,
    /** Non-null only for the per-tool rows in a round. */
    val ordinal: Int? = null,
) {
    val isLong: Boolean
        get() = durationNs > AGENT_TIMING_LONG_STAGE_NS
}

internal data class AgentTimingSectionPresentation(
    val kind: AgentTimingSectionKind,
    val metrics: List<AgentTimingMetricPresentation>,
)

internal data class AgentTimingRoundPresentation(
    /** Internal snapshot identity. Retries have their own ordinal but share a logical call. */
    val snapshotOrdinal: Int,
    /** One-based, gap-free UI number for the (runtimeRunId, providerCallIndex) logical call. */
    val logicalRoundNumber: Int,
    val attemptIndex: Int,
    val responseMode: AgentTimingResponseMode,
    val sections: List<AgentTimingSectionPresentation>,
    val providerStats: AgentTimingProviderStatsPresentation? = null,
)

internal data class AgentTimingProviderStatsPresentation(
    val outputTokens: Int,
    val decodeTps: Double?,
    val providerVisible: AgentTimingStreamStatsPresentation? = null,
    val sessionVisible: AgentTimingStreamStatsPresentation? = null,
    val uiVisible: AgentTimingStreamStatsPresentation? = null,
)

internal data class AgentTimingStreamStatsPresentation(
    val estimatedTokens: Long,
    val sampleCount: Long,
    val tps: Double?,
)

internal data class AgentTimingDetailPresentation(
    val overview: List<AgentTimingSectionPresentation>,
    val responseLayers: AgentTimingResponseLayersPresentation,
    val rounds: List<AgentTimingRoundPresentation>,
)

internal enum class AgentTimingVisibleDrawState {
    OBSERVED,
    NOT_OBSERVED,
    PENDING,
}

internal data class AgentTimingResponseLayersPresentation(
    val sessionContentFromDispatchNs: Long?,
    val visibleDrawFromSessionContentNs: Long?,
    val visibleDrawState: AgentTimingVisibleDrawState,
)

internal data class AgentToolTimingPresentation(
    val executionNs: Long?,
    val postProcessingNs: Long?,
    val batchReadyNs: Long?,
    val handoffNs: Long?,
    val nextResponseNs: Long?,
) {
    val hasAnyMetric: Boolean
        get() = executionNs != null || postProcessingNs != null || batchReadyNs != null ||
            handoffNs != null || nextResponseNs != null

    val hasLongMetric: Boolean
        get() = listOfNotNull(executionNs, postProcessingNs, batchReadyNs, handoffNs, nextResponseNs)
            .any { it > AGENT_TIMING_LONG_STAGE_NS }
}

internal fun buildAgentTimingSummary(
    trace: AgentTimingTraceSnapshot,
): AgentTimingSummaryPresentation {
    val sortedRounds = trace.rounds.sortedBy { it.ordinal }
    val logicalRoundNumbers = sortedRounds.logicalRoundNumbers()
    val firstResponseRound = sortedRounds.firstOrNull { it.firstResponseDurationNs() != null }
    val responseMode = firstResponseRound?.responseMode ?: sortedRounds.firstOrNull()?.responseMode
    return AgentTimingSummaryPresentation(
        totalNs = trace.wallDurationNs,
        excludingHumanWaitNs = (trace.wallDurationNs - trace.humanWaitDurationNs).coerceAtLeast(0L),
        firstResponseNs = firstResponseRound?.firstResponseDurationNs(),
        firstResponseKind = if (responseMode == AgentTimingResponseMode.NON_STREAMING) {
            AgentTimingFirstResponseKind.FULL_RESPONSE
        } else {
            AgentTimingFirstResponseKind.FIRST_PROGRESS
        },
        roundCount = logicalRoundNumbers.size.toLong(),
        toolCount = trace.totalToolCount,
    )
}

internal fun buildAgentTimingDetail(
    trace: AgentTimingTraceSnapshot,
): AgentTimingDetailPresentation {
    val preflightEndAtNs = trace.at(AgentTimingEventKind.MEMORY_RETRIEVAL_STARTED)
        ?: trace.at(AgentTimingEventKind.TOOL_SURFACE_STARTED)
    val lastProviderFinishedAtNs = trace.rounds
        .mapNotNull { round -> round.at(AgentTimingEventKind.PROVIDER_STREAM_FINISHED) }
        .maxOrNull()
    val finalSaveStartedAtNs = trace.at(AgentTimingEventKind.FINAL_SAVE_STARTED)
    val finalSaveFinishedAtNs = trace.at(AgentTimingEventKind.FINAL_SAVE_FINISHED)
    val runEndedAtNs = trace.at(AgentTimingEventKind.RUN_ENDED)
    val overview = buildList {
        section(
            AgentTimingSectionKind.OVERVIEW,
            metric(trace, AgentTimingMetricKind.ADMISSION, AgentTimingEventKind.UI_SUBMITTED, AgentTimingEventKind.RUN_STARTED),
            durationOrNull(trace.at(AgentTimingEventKind.RUN_STARTED), preflightEndAtNs)?.let {
                AgentTimingMetricPresentation(AgentTimingMetricKind.RUN_PREFLIGHT, it)
            },
            metric(trace, AgentTimingMetricKind.MEMORY_RETRIEVAL, AgentTimingEventKind.MEMORY_RETRIEVAL_STARTED, AgentTimingEventKind.MEMORY_RETRIEVAL_FINISHED),
            metric(trace, AgentTimingMetricKind.AUTO_CONTEXT, AgentTimingEventKind.AUTO_CONTEXT_STARTED, AgentTimingEventKind.AUTO_CONTEXT_FINISHED),
            metric(trace, AgentTimingMetricKind.TOOL_SURFACE, AgentTimingEventKind.TOOL_SURFACE_STARTED, AgentTimingEventKind.TOOL_SURFACE_FINISHED),
            metric(trace, AgentTimingMetricKind.MCP_DISCOVERY, AgentTimingEventKind.MCP_DISCOVERY_STARTED, AgentTimingEventKind.MCP_DISCOVERY_FINISHED),
            durationOrNull(lastProviderFinishedAtNs, finalSaveStartedAtNs)?.let {
                AgentTimingMetricPresentation(AgentTimingMetricKind.POST_PROVIDER_PROCESSING, it)
            },
            metric(trace, AgentTimingMetricKind.FINAL_SAVE, AgentTimingEventKind.FINAL_SAVE_STARTED, AgentTimingEventKind.FINAL_SAVE_FINISHED),
            metric(trace, AgentTimingMetricKind.GENERATION_DONE_NOTIFY, AgentTimingEventKind.GENERATION_DONE_NOTIFY_STARTED, AgentTimingEventKind.GENERATION_DONE_NOTIFY_FINISHED),
            durationOrNull(finalSaveFinishedAtNs, runEndedAtNs)?.let {
                AgentTimingMetricPresentation(AgentTimingMetricKind.RUN_COMPLETION_DRAIN, it)
            },
            durationOrNull(runEndedAtNs, trace.finishedAtNs)?.let {
                AgentTimingMetricPresentation(AgentTimingMetricKind.RUNTIME_FINALIZATION, it)
            },
        )
        section(
            AgentTimingSectionKind.APPROVAL,
            trace.humanWaitDurationNs.takeIf { it > 0L }?.let {
                AgentTimingMetricPresentation(AgentTimingMetricKind.HUMAN_WAIT, it)
            },
            trace.approvalResolutionDurationNs.takeIf { it > 0L }?.let {
                AgentTimingMetricPresentation(AgentTimingMetricKind.APPROVAL_RESOLUTION, it)
            },
        )
    }

    val toolsByRound = trace.tools.groupBy { it.roundOrdinal }
    val sortedRounds = trace.rounds.sortedBy { it.ordinal }
    val logicalRoundNumbers = sortedRounds.logicalRoundNumbers()
    val rounds = sortedRounds.map { round ->
        val contextStart = round.earliest(
            AgentTimingEventKind.RECENT_CHATS_STARTED,
            AgentTimingEventKind.MEMORY_PROMPT_STARTED,
            AgentTimingEventKind.TOOL_PROMPT_STARTED,
            AgentTimingEventKind.SYSTEM_PROMPT_STARTED,
            AgentTimingEventKind.CONTEXT_GATE_INITIAL_STARTED,
            AgentTimingEventKind.INPUT_TRANSFORM_STARTED,
        )
        val contextEnd = round.latest(
            AgentTimingEventKind.RECENT_CHATS_FINISHED,
            AgentTimingEventKind.MEMORY_PROMPT_FINISHED,
            AgentTimingEventKind.TOOL_PROMPT_FINISHED,
            AgentTimingEventKind.SYSTEM_PROMPT_FINISHED,
            AgentTimingEventKind.CONTEXT_GATE_FINAL_FINISHED,
            AgentTimingEventKind.REQUEST_BUILD_FINISHED,
        )
        val diagnosticsStart = round.earliest(
            AgentTimingEventKind.REQUEST_BREAKDOWN_BUILD_STARTED,
            AgentTimingEventKind.REQUEST_BREAKDOWN_WRITE_STARTED,
        )
        val diagnosticsEnd = round.latest(
            AgentTimingEventKind.REQUEST_BREAKDOWN_BUILD_FINISHED,
            AgentTimingEventKind.REQUEST_BREAKDOWN_WRITE_FINISHED,
        )

        val toolMetrics = buildList {
            metric(round, AgentTimingMetricKind.TOOL_BATCH, AgentTimingEventKind.TOOL_BATCH_STARTED, AgentTimingEventKind.MODEL_RESULTS_READY)
                ?.let(::add)
            toolsByRound[round.ordinal].orEmpty().sortedBy { it.ordinal }.forEach { tool ->
                tool.executionDurationNs?.let {
                    add(AgentTimingMetricPresentation(AgentTimingMetricKind.TOOL_EXECUTION, it, tool.ordinal))
                }
                tool.postProcessingDurationNs()?.let {
                    add(AgentTimingMetricPresentation(AgentTimingMetricKind.TOOL_POST_PROCESSING, it, tool.ordinal))
                }
                tool.batchReadyDurationNs()?.let {
                    add(AgentTimingMetricPresentation(AgentTimingMetricKind.TOOL_BATCH_WAIT, it, tool.ordinal))
                }
            }
        }

        AgentTimingRoundPresentation(
            snapshotOrdinal = round.ordinal,
            logicalRoundNumber = logicalRoundNumbers.getValue(round.logicalIdentity()),
            attemptIndex = round.attemptIndex,
            responseMode = round.responseMode,
            sections = buildList {
                section(
                    AgentTimingSectionKind.CONTEXT,
                    durationOrNull(contextStart, contextEnd)?.let {
                        AgentTimingMetricPresentation(AgentTimingMetricKind.CONTEXT, it)
                    },
                    metric(round, AgentTimingMetricKind.RECENT_CHATS, AgentTimingEventKind.RECENT_CHATS_STARTED, AgentTimingEventKind.RECENT_CHATS_FINISHED),
                    metric(round, AgentTimingMetricKind.MEMORY_PROMPT, AgentTimingEventKind.MEMORY_PROMPT_STARTED, AgentTimingEventKind.MEMORY_PROMPT_FINISHED),
                    metric(round, AgentTimingMetricKind.TOOL_PROMPT, AgentTimingEventKind.TOOL_PROMPT_STARTED, AgentTimingEventKind.TOOL_PROMPT_FINISHED),
                    metric(round, AgentTimingMetricKind.SYSTEM_PROMPT, AgentTimingEventKind.SYSTEM_PROMPT_STARTED, AgentTimingEventKind.SYSTEM_PROMPT_FINISHED),
                    metric(round, AgentTimingMetricKind.CONTEXT_GATE_INITIAL, AgentTimingEventKind.CONTEXT_GATE_INITIAL_STARTED, AgentTimingEventKind.CONTEXT_GATE_INITIAL_FINISHED),
                    metric(round, AgentTimingMetricKind.INPUT_TRANSFORM, AgentTimingEventKind.INPUT_TRANSFORM_STARTED, AgentTimingEventKind.INPUT_TRANSFORM_FINISHED),
                    metric(round, AgentTimingMetricKind.CONTEXT_GATE_FINAL, AgentTimingEventKind.CONTEXT_GATE_FINAL_STARTED, AgentTimingEventKind.CONTEXT_GATE_FINAL_FINISHED),
                    metric(round, AgentTimingMetricKind.CONTEXT_COMPRESSION, AgentTimingEventKind.CONTEXT_COMPRESSION_STARTED, AgentTimingEventKind.CONTEXT_COMPRESSION_FINISHED),
                    metric(round, AgentTimingMetricKind.TOKEN_COUNT, AgentTimingEventKind.TOKEN_COUNT_STARTED, AgentTimingEventKind.TOKEN_COUNT_FINISHED),
                    metric(round, AgentTimingMetricKind.REQUEST_BUILD, AgentTimingEventKind.REQUEST_BUILD_STARTED, AgentTimingEventKind.REQUEST_BUILD_FINISHED),
                )
                section(
                    AgentTimingSectionKind.DIAGNOSTICS,
                    durationOrNull(diagnosticsStart, diagnosticsEnd)?.let {
                        AgentTimingMetricPresentation(AgentTimingMetricKind.DIAGNOSTICS, it)
                    },
                    metric(round, AgentTimingMetricKind.REQUEST_BREAKDOWN_BUILD, AgentTimingEventKind.REQUEST_BREAKDOWN_BUILD_STARTED, AgentTimingEventKind.REQUEST_BREAKDOWN_BUILD_FINISHED),
                    metric(round, AgentTimingMetricKind.REQUEST_BREAKDOWN_WRITE, AgentTimingEventKind.REQUEST_BREAKDOWN_WRITE_STARTED, AgentTimingEventKind.REQUEST_BREAKDOWN_WRITE_FINISHED),
                )
                section(
                    AgentTimingSectionKind.PROVIDER,
                    metric(round, AgentTimingMetricKind.MEMORY_LAST_ACCESS, AgentTimingEventKind.MEMORY_LAST_ACCESS_STARTED, AgentTimingEventKind.MEMORY_LAST_ACCESS_FINISHED),
                    metric(round, AgentTimingMetricKind.PROVIDER_PREPARE, AgentTimingEventKind.PROVIDER_PREPARE_STARTED, AgentTimingEventKind.PROVIDER_PREPARE_FINISHED),
                    round.firstResponseDurationNs()?.let {
                        AgentTimingMetricPresentation(
                            if (round.responseMode == AgentTimingResponseMode.NON_STREAMING) {
                                AgentTimingMetricKind.FULL_RESPONSE
                            } else {
                                AgentTimingMetricKind.FIRST_PROGRESS
                            },
                            it,
                        )
                    },
                    round.firstTextNs?.let {
                        AgentTimingMetricPresentation(AgentTimingMetricKind.FIRST_TEXT, it)
                    },
                    round.providerTotalDurationNs()?.let {
                        AgentTimingMetricPresentation(AgentTimingMetricKind.PROVIDER_TOTAL, it)
                    },
                )
                section(AgentTimingSectionKind.TOOLS, *toolMetrics.toTypedArray())
                section(
                    AgentTimingSectionKind.HANDOFF,
                    round.handoffFromPreviousResultsNs?.let {
                        AgentTimingMetricPresentation(AgentTimingMetricKind.HANDOFF, it)
                    },
                )
            },
            providerStats = round.providerStats(),
        )
    }

    val firstDispatchAtNs = trace.at(AgentTimingEventKind.APP_PROVIDER_DISPATCH)
    val sessionContentAtNs = trace.firstSessionContentReadyAtNs
    val firstVisibleDrawAtNs = trace.firstVisibleDrawAtNs
    val responseLayers = AgentTimingResponseLayersPresentation(
        sessionContentFromDispatchNs = durationOrNull(firstDispatchAtNs, sessionContentAtNs),
        visibleDrawFromSessionContentNs = durationOrNull(sessionContentAtNs, firstVisibleDrawAtNs),
        visibleDrawState = when {
            firstVisibleDrawAtNs != null -> AgentTimingVisibleDrawState.OBSERVED
            trace.at(AgentTimingEventKind.FIRST_VISIBLE_NOT_OBSERVED) != null ->
                AgentTimingVisibleDrawState.NOT_OBSERVED
            else -> AgentTimingVisibleDrawState.PENDING
        },
    )

    return AgentTimingDetailPresentation(
        overview = overview,
        responseLayers = responseLayers,
        rounds = rounds,
    )
}

internal fun buildAgentToolTiming(
    tool: AgentTimingToolSnapshot?,
): AgentToolTimingPresentation? = tool?.let {
    AgentToolTimingPresentation(
        executionNs = it.executionDurationNs,
        postProcessingNs = it.postProcessingDurationNs(),
        batchReadyNs = it.batchReadyDurationNs(),
        handoffNs = it.handoffDurationNs,
        nextResponseNs = it.nextRoundTtftNs,
    ).takeIf(AgentToolTimingPresentation::hasAnyMetric)
}

/** Traces already content-ready when the chat observer is installed were not viewport-observable. */
internal fun AgentTimingConversationSnapshot.readyBeforeUiObservationTraceSequences(): Set<Long> =
    traces.asSequence()
        .filter { trace ->
            trace.firstSessionContentReadyAtNs != null &&
                trace.at(AgentTimingEventKind.FIRST_VISIBLE_DRAW) == null &&
                trace.at(AgentTimingEventKind.FIRST_VISIBLE_NOT_OBSERVED) == null
        }
        .mapTo(linkedSetOf<Long>()) { it.traceSequence }

/**
 * Matches UI tool parts to timing records without treating toolCallId as unique. Exact IDs win;
 * duplicate and blank IDs consume the next unused timing ordinal.
 */
@VisibleForTesting
internal fun matchAgentTimingTools(
    toolCallIds: List<String>,
    timingTools: List<AgentTimingToolSnapshot>,
): List<AgentTimingToolSnapshot?> {
    val ordered = timingTools.sortedBy { it.ordinal }
    val used = BooleanArray(ordered.size)
    val matched = MutableList<AgentTimingToolSnapshot?>(toolCallIds.size) { null }

    // Reserve every exact ID first. Otherwise an earlier blank/missing ID could steal a record
    // that is the only exact match for a later UI part.
    toolCallIds.forEachIndexed { uiIndex, rawId ->
        val id = rawId.takeIf(String::isNotBlank) ?: return@forEachIndexed
        val timingIndex = ordered.indices.firstOrNull { index ->
            !used[index] && ordered[index].toolCallId == id
        } ?: return@forEachIndexed
        used[timingIndex] = true
        matched[uiIndex] = ordered[timingIndex]
    }

    toolCallIds.indices.forEach { uiIndex ->
        if (matched[uiIndex] != null) return@forEach
        val timingIndex = ordered.indices.firstOrNull { !used[it] } ?: return@forEach
        used[timingIndex] = true
        matched[uiIndex] = ordered[timingIndex]
    }
    return matched
}

@VisibleForTesting
internal fun formatAgentTimingDuration(durationNs: Long?): String {
    if (durationNs == null) return "\u2014"
    val millis = durationNs.coerceAtLeast(0L) / 1_000_000.0
    return when {
        millis < 1.0 -> "<1 ms"
        millis < 1_000.0 -> String.format(Locale.ROOT, "%.0f ms", millis)
        millis < 10_000.0 -> String.format(Locale.ROOT, "%.2f s", millis / 1_000.0)
        millis < 60_000.0 -> String.format(Locale.ROOT, "%.1f s", millis / 1_000.0)
        else -> {
            val totalSeconds = (millis / 1_000.0).toLong()
            "%d:%02d".format(Locale.ROOT, totalSeconds / 60L, totalSeconds % 60L)
        }
    }
}

private fun AgentTimingRoundSnapshot.firstResponseDurationNs(): Long? {
    if (responseMode == AgentTimingResponseMode.NON_STREAMING) {
        durationNs(
            AgentTimingEventKind.APP_PROVIDER_DISPATCH,
            AgentTimingEventKind.PROVIDER_FULL_RESPONSE,
        )?.let { return it }
    }
    return ttftNs
}

private data class AgentTimingLogicalRoundIdentity(
    val runtimeRunId: kotlin.uuid.Uuid?,
    val providerCallIndex: Int,
)

private fun AgentTimingRoundSnapshot.logicalIdentity(): AgentTimingLogicalRoundIdentity =
    AgentTimingLogicalRoundIdentity(runtimeRunId, providerCallIndex)

/** Preserves first-observed call order while hiding internal gaps and per-runtime index resets. */
private fun List<AgentTimingRoundSnapshot>.logicalRoundNumbers(): Map<AgentTimingLogicalRoundIdentity, Int> =
    linkedMapOf<AgentTimingLogicalRoundIdentity, Int>().also { numbers ->
        forEach { round ->
            val identity = round.logicalIdentity()
            if (identity !in numbers) numbers[identity] = numbers.size + 1
        }
    }

/** Raw result handling owned by this tool; deliberately excludes waiting for slower siblings. */
private fun AgentTimingToolSnapshot.postProcessingDurationNs(): Long? {
    val start = listOfNotNull(
        at(AgentTimingEventKind.TOOL_OUTPUT_NORMALIZE_STARTED),
        at(AgentTimingEventKind.TOOL_OUTPUT_SPILL_STARTED),
    ).minOrNull()
    val end = postProcessingFinishedAtNs()
    return durationOrNull(start, end)
}

/** Time after this tool's own output handling until the parallel batch is model-ready. */
private fun AgentTimingToolSnapshot.batchReadyDurationNs(): Long? {
    val start = postProcessingFinishedAtNs()
        ?: at(AgentTimingEventKind.TOOL_RAW_RESULT_READY)
    return durationOrNull(start, sharedModelResultsReadyAtNs)
}

private fun AgentTimingToolSnapshot.postProcessingFinishedAtNs(): Long? = listOfNotNull(
    at(AgentTimingEventKind.TOOL_OUTPUT_NORMALIZE_FINISHED),
    at(AgentTimingEventKind.TOOL_OUTPUT_SPILL_FINISHED),
).maxOrNull()

private fun AgentTimingRoundSnapshot.providerTotalDurationNs(): Long? {
    val start = at(AgentTimingEventKind.APP_PROVIDER_DISPATCH)
    val end = latest(
        AgentTimingEventKind.PROVIDER_STREAM_FINISHED,
        AgentTimingEventKind.PROVIDER_FULL_RESPONSE,
        AgentTimingEventKind.PROVIDER_ATTEMPT_TERMINAL,
    )
    return durationOrNull(start, end)
}

private fun AgentTimingRoundSnapshot.providerStats(): AgentTimingProviderStatsPresentation? {
    val output = completionTokens?.takeIf { it > 0 } ?: return null
    val decodeNs = durationNs(
        AgentTimingEventKind.PROVIDER_FIRST_PROGRESS,
        AgentTimingEventKind.PROVIDER_STREAM_FINISHED,
    )
    val decodeTps = decodeNs
        ?.takeIf { it > 0L }
        ?.let { output.toDouble() * 1_000_000_000.0 / it.toDouble() }
    return AgentTimingProviderStatsPresentation(
        outputTokens = output,
        decodeTps = decodeTps,
        providerVisible = providerVisibleStream.presentation(),
        sessionVisible = sessionVisibleStream.presentation(),
        uiVisible = uiVisibleStream.presentation(),
    )
}

private fun AgentTimingStreamProgressSnapshot.presentation(): AgentTimingStreamStatsPresentation? {
    if (estimatedTokens <= 0L || sampleCount <= 0L) return null
    val tps = durationNs
        ?.takeIf { it > 0L }
        ?.let { estimatedTokens.toDouble() * 1_000_000_000.0 / it.toDouble() }
    return AgentTimingStreamStatsPresentation(
        estimatedTokens = estimatedTokens,
        sampleCount = sampleCount,
        tps = tps,
    )
}

private fun AgentTimingRoundSnapshot.earliest(vararg kinds: AgentTimingEventKind): Long? =
    kinds.mapNotNull(::at).minOrNull()

private fun AgentTimingRoundSnapshot.latest(vararg kinds: AgentTimingEventKind): Long? =
    kinds.mapNotNull(::at).maxOrNull()

private fun metric(
    trace: AgentTimingTraceSnapshot,
    kind: AgentTimingMetricKind,
    from: AgentTimingEventKind,
    to: AgentTimingEventKind,
): AgentTimingMetricPresentation? = trace.durationNs(from, to)?.let {
    AgentTimingMetricPresentation(kind, it)
}

private fun metric(
    round: AgentTimingRoundSnapshot,
    kind: AgentTimingMetricKind,
    from: AgentTimingEventKind,
    to: AgentTimingEventKind,
): AgentTimingMetricPresentation? = round.durationNs(from, to)?.let {
    AgentTimingMetricPresentation(kind, it)
}

private fun MutableList<AgentTimingSectionPresentation>.section(
    kind: AgentTimingSectionKind,
    vararg candidates: AgentTimingMetricPresentation?,
) {
    val metrics = candidates.filterNotNull()
    if (metrics.isNotEmpty()) add(AgentTimingSectionPresentation(kind, metrics))
}

private fun durationOrNull(startNs: Long?, endNs: Long?): Long? {
    if (startNs == null || endNs == null || endNs < startNs) return null
    return endNs - startNs
}
