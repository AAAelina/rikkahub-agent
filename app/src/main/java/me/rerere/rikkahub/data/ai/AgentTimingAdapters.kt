package me.rerere.rikkahub.data.ai

import java.util.concurrent.ConcurrentHashMap
import me.rerere.ai.core.TokenUsage
import me.rerere.ai.core.merge
import me.rerere.rikkahub.data.ai.execution.ToolExecutionTimingHook
import me.rerere.rikkahub.data.ai.execution.ToolExecutionTimingOutcome
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingEventKind
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingEventResult
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingHandle
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingResponseMode
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingRoundRef
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingStreamStage
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingToolRef
import kotlin.uuid.Uuid

/** Bridges provider seams to the process-only recorder without exposing timing to providers. */
internal class AgentTimingProviderHook(
    private val handle: AgentTimingHandle,
    private val providerCallIndex: Int,
    private val stream: Boolean,
    private val runtimeRunId: Uuid?,
    initialRound: AgentTimingRoundRef? = null,
    private val onRoundCreated: (AgentTimingRoundRef) -> Unit = {},
) : ProviderTurnTimingHook {
    private val rounds = ConcurrentHashMap<Int, AgentTimingRoundRef>().apply {
        initialRound?.let { put(0, it) }
    }
    private val usageByAttempt = ConcurrentHashMap<Int, TokenUsage>()

    fun round(attemptIndex: Int = 0): AgentTimingRoundRef? = rounds[attemptIndex]

    private fun roundFor(attemptIndex: Int): AgentTimingRoundRef? =
        rounds[attemptIndex] ?: handle.beginRound(
            providerCallIndex = providerCallIndex,
            attemptIndex = attemptIndex,
            responseMode = if (stream) {
                AgentTimingResponseMode.STREAMING
            } else {
                AgentTimingResponseMode.NON_STREAMING
            },
            runtimeRunId = runtimeRunId,
        )?.also { created ->
            val winner = rounds.putIfAbsent(attemptIndex, created) ?: created
            if (winner == created) onRoundCreated(created)
        }

    override fun onBeforeAttempt(attemptIndex: Int, isRetry: Boolean) {
        handle.mark(AgentTimingEventKind.PROVIDER_PREPARE_STARTED, roundFor(attemptIndex))
    }

    override fun onAppDispatch(attemptIndex: Int, stream: Boolean) {
        val round = roundFor(attemptIndex)
        handle.mark(AgentTimingEventKind.PROVIDER_PREPARE_FINISHED, round)
        handle.mark(AgentTimingEventKind.APP_PROVIDER_DISPATCH, round)
    }

    override fun onFirstMeaningfulProgress(attemptIndex: Int, kind: ProviderProgressKind) {
        val round = roundFor(attemptIndex)
        handle.mark(
            if (kind == ProviderProgressKind.FULL_RESPONSE) {
                AgentTimingEventKind.PROVIDER_FULL_RESPONSE
            } else {
                AgentTimingEventKind.PROVIDER_FIRST_PROGRESS
            },
            round,
        )
        if (kind == ProviderProgressKind.FULL_RESPONSE) {
            handle.mark(AgentTimingEventKind.PROVIDER_FIRST_PROGRESS, round)
        }
    }

    override fun onFirstTextProgress(attemptIndex: Int) {
        handle.mark(AgentTimingEventKind.PROVIDER_FIRST_TEXT, roundFor(attemptIndex))
    }

    override fun onRawVisibleTextProgress(attemptIndex: Int, estimatedTokens: Long) {
        roundFor(attemptIndex)?.let { round ->
            handle.recordRoundVisibleProgress(
                round = round,
                stage = AgentTimingStreamStage.PROVIDER_RAW_VISIBLE,
                estimatedTokens = estimatedTokens,
            )
        }
    }

    override fun onConsumerVisibleTextProgress(attemptIndex: Int, estimatedTokens: Long) {
        roundFor(attemptIndex)?.let { round ->
            handle.recordRoundVisibleProgress(
                round = round,
                stage = AgentTimingStreamStage.SESSION_CONSUMER_VISIBLE,
                estimatedTokens = estimatedTokens,
            )
        }
    }

    override fun onUsage(attemptIndex: Int, usage: TokenUsage) {
        val merged = usageByAttempt.compute(attemptIndex) { _, current -> current.merge(usage) }
            ?: usage
        roundFor(attemptIndex)?.let { round ->
            handle.updateRoundUsage(
                round = round,
                promptTokens = merged.promptTokens,
                completionTokens = merged.completionTokens,
                cachedTokens = merged.cachedTokens,
            )
        }
    }

    override fun onProviderResponseFinished(attemptIndex: Int) {
        handle.mark(AgentTimingEventKind.PROVIDER_STREAM_FINISHED, roundFor(attemptIndex))
    }

    override fun onAttemptTerminal(attemptIndex: Int, outcome: ProviderAttemptTimingOutcome) {
        handle.mark(
            kind = AgentTimingEventKind.PROVIDER_ATTEMPT_TERMINAL,
            round = roundFor(attemptIndex),
            result = when (outcome) {
                ProviderAttemptTimingOutcome.COMPLETED -> AgentTimingEventResult.SUCCESS
                ProviderAttemptTimingOutcome.CANCELLED,
                ProviderAttemptTimingOutcome.STEERING_CANCELLED,
                -> AgentTimingEventResult.CANCELLED
                ProviderAttemptTimingOutcome.STALLED -> AgentTimingEventResult.TIMED_OUT
                ProviderAttemptTimingOutcome.FAILED -> AgentTimingEventResult.FAILED
            },
        )
    }

    override fun onRetryScheduled(
        completedAttemptIndex: Int,
        nextAttemptIndex: Int,
        reason: ProviderStreamStallReason,
    ) {
        handle.mark(AgentTimingEventKind.PROVIDER_RETRY, roundFor(completedAttemptIndex))
        handle.mark(AgentTimingEventKind.WATCHDOG_RETRY, roundFor(completedAttemptIndex))
    }
}

internal class AgentTimingToolHook(
    private val handle: AgentTimingHandle,
    private val round: AgentTimingRoundRef?,
    private val tool: AgentTimingToolRef,
) : ToolExecutionTimingHook {
    override fun onQueued() = mark(AgentTimingEventKind.TOOL_QUEUED)
    override fun onPreflightStarted() = mark(AgentTimingEventKind.TOOL_PREFLIGHT_STARTED)
    override fun onPreflightFinished() = mark(AgentTimingEventKind.TOOL_PREFLIGHT_FINISHED)
    override fun onExecutionStarted() = mark(AgentTimingEventKind.TOOL_EXECUTION_STARTED)
    override fun onRawResultReady() = mark(AgentTimingEventKind.TOOL_RAW_RESULT_READY)
    override fun onCompletionLedgerFinished() = mark(AgentTimingEventKind.TOOL_LEDGER_COMPLETED)

    override fun onTerminal(outcome: ToolExecutionTimingOutcome) {
        handle.mark(
            kind = AgentTimingEventKind.TOOL_TERMINAL,
            round = round,
            tool = tool,
            result = when (outcome) {
                ToolExecutionTimingOutcome.COMPLETED -> AgentTimingEventResult.SUCCESS
                ToolExecutionTimingOutcome.REJECTED -> AgentTimingEventResult.DENIED
                ToolExecutionTimingOutcome.TIMED_OUT -> AgentTimingEventResult.TIMED_OUT
                ToolExecutionTimingOutcome.CANCELLED -> AgentTimingEventResult.CANCELLED
                ToolExecutionTimingOutcome.FAILED -> AgentTimingEventResult.FAILED
            },
        )
    }

    private fun mark(kind: AgentTimingEventKind) {
        handle.mark(kind = kind, round = round, tool = tool)
    }
}

/** Inline stage wrapper: a null receiver compiles down to the original operation. */
internal inline fun <T> AgentTimingHandle?.timedAgentStage(
    started: AgentTimingEventKind,
    finished: AgentTimingEventKind,
    round: AgentTimingRoundRef? = null,
    tool: AgentTimingToolRef? = null,
    block: () -> T,
): T {
    val handle = this ?: return block()
    handle.mark(started, round, tool)
    return try {
        block()
    } finally {
        handle.mark(finished, round, tool)
    }
}

internal suspend inline fun <T> AgentTimingHandle?.timedAgentStageSuspend(
    started: AgentTimingEventKind,
    finished: AgentTimingEventKind,
    round: AgentTimingRoundRef? = null,
    tool: AgentTimingToolRef? = null,
    crossinline block: suspend () -> T,
): T {
    val handle = this ?: return block()
    handle.mark(started, round, tool)
    return try {
        block()
    } finally {
        handle.mark(finished, round, tool)
    }
}
