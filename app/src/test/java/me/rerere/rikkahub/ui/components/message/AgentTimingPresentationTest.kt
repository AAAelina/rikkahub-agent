package me.rerere.rikkahub.ui.components.message

import me.rerere.ai.core.MessageRole
import me.rerere.ai.ui.UIMessage
import me.rerere.ai.ui.UIMessagePart
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingApprovalSegmentSnapshot
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingConversationSnapshot
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingEventKind
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingResponseMode
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingRoundSnapshot
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingToolSnapshot
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingTraceSnapshot
import me.rerere.rikkahub.diagnostics.agenttiming.hasAgentTimingRenderableContent
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingTraceStatus
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.uuid.Uuid

class AgentTimingPresentationTest {

    @Test
    fun `summary excludes human wait and labels non-streaming as full response`() {
        val round = AgentTimingRoundSnapshot(
            ordinal = 0,
            providerCallIndex = 0,
            attemptIndex = 0,
            responseMode = AgentTimingResponseMode.NON_STREAMING,
            runtimeRunId = null,
            milestones = mapOf(
                AgentTimingEventKind.APP_PROVIDER_DISPATCH to seconds(1),
                AgentTimingEventKind.PROVIDER_FULL_RESPONSE to seconds(3),
                // Some adapters also report first progress after returning the whole response.
                AgentTimingEventKind.PROVIDER_FIRST_PROGRESS to seconds(4),
            ),
        )
        val trace = trace(
            finishedAtNs = seconds(10),
            rounds = listOf(round),
            approvalSegments = listOf(
                AgentTimingApprovalSegmentSnapshot(
                    startedAtNs = seconds(2),
                    userDecisionAtNs = seconds(5),
                    resumedAtNs = seconds(6),
                    endedAtNs = seconds(6),
                )
            ),
        )

        val summary = buildAgentTimingSummary(trace)

        assertEquals(seconds(10), summary.totalNs)
        assertEquals(seconds(7), summary.excludingHumanWaitNs)
        assertEquals(seconds(2), summary.firstResponseNs)
        assertEquals(AgentTimingFirstResponseKind.FULL_RESPONSE, summary.firstResponseKind)
    }

    @Test
    fun `logical rounds are continuous across retries index gaps and resumed runs`() {
        val firstRun = Uuid.random()
        val resumedRun = Uuid.random()
        fun round(
            ordinal: Int,
            runtimeRunId: Uuid,
            providerCallIndex: Int,
            attemptIndex: Int,
        ) =
            AgentTimingRoundSnapshot(
                ordinal = ordinal,
                providerCallIndex = providerCallIndex,
                attemptIndex = attemptIndex,
                responseMode = AgentTimingResponseMode.STREAMING,
                runtimeRunId = runtimeRunId,
                milestones = emptyMap(),
            )

        val trace = trace(
            finishedAtNs = seconds(1),
            rounds = listOf(
                round(0, firstRun, providerCallIndex = 1, attemptIndex = 0),
                round(1, firstRun, providerCallIndex = 1, attemptIndex = 1),
                round(2, firstRun, providerCallIndex = 4, attemptIndex = 0),
                round(3, resumedRun, providerCallIndex = 1, attemptIndex = 0),
            ),
        )

        assertEquals(3L, buildAgentTimingSummary(trace).roundCount)
        assertEquals(
            listOf(1, 1, 2, 3),
            buildAgentTimingDetail(trace).rounds.map { it.logicalRoundNumber },
        )
    }

    @Test
    fun `duplicate and blank call ids consume distinct timing ordinals`() {
        val timing = listOf(
            tool(ordinal = 0, id = "duplicate"),
            tool(ordinal = 1, id = null),
            tool(ordinal = 2, id = "duplicate"),
        )

        val matched = matchAgentTimingTools(
            toolCallIds = listOf("duplicate", "duplicate", ""),
            timingTools = timing,
        )

        assertEquals(listOf(0, 2, 1), matched.map { it?.ordinal })
    }

    @Test
    fun `missing timing records remain absent rather than reusing one record`() {
        val matched = matchAgentTimingTools(
            toolCallIds = listOf("missing", "known"),
            timingTools = listOf(tool(ordinal = 0, id = "known")),
        )

        assertNull(matched[0])
        assertEquals(0, matched[1]?.ordinal)
    }

    @Test
    fun `tool presentation separates own post-processing from parallel batch wait`() {
        val timing = tool(
            ordinal = 0,
            id = "call",
            milestones = mapOf(
                AgentTimingEventKind.TOOL_EXECUTION_STARTED to seconds(1),
                AgentTimingEventKind.TOOL_RAW_RESULT_READY to seconds(3),
                AgentTimingEventKind.TOOL_OUTPUT_NORMALIZE_STARTED to seconds(3),
                AgentTimingEventKind.TOOL_OUTPUT_NORMALIZE_FINISHED to seconds(4),
            ),
            modelResultsReadyAtNs = seconds(6),
            nextDispatchAtNs = seconds(7),
            nextProgressAtNs = seconds(8),
        )

        val presentation = buildAgentToolTiming(timing)!!

        assertEquals(seconds(2), presentation.executionNs)
        assertEquals(seconds(1), presentation.postProcessingNs)
        assertEquals(seconds(2), presentation.batchReadyNs)
        assertEquals(seconds(1), presentation.handoffNs)
        assertEquals(seconds(1), presentation.nextResponseNs)
        assertTrue(presentation.hasLongMetric)
    }

    @Test
    fun `duration formatter is stable around unit boundaries`() {
        assertEquals("<1 ms", formatAgentTimingDuration(999_999L))
        assertEquals("1 ms", formatAgentTimingDuration(1_000_000L))
        assertEquals("1.50 s", formatAgentTimingDuration(1_500_000_000L))
        assertEquals("1:05", formatAgentTimingDuration(seconds(65)))
    }

    @Test
    fun `empty placeholder is not renderable but media placeholder is`() {
        val emptyText = UIMessage(
            role = MessageRole.ASSISTANT,
            parts = listOf(UIMessagePart.Text("")),
        )
        val imagePlaceholder = UIMessage(
            role = MessageRole.ASSISTANT,
            parts = listOf(UIMessagePart.Image("")),
        )

        assertFalse(emptyText.hasAgentTimingRenderableContent())
        assertTrue(imagePlaceholder.hasAgentTimingRenderableContent())
    }

    @Test
    fun `detail keeps context and diagnostics sub-stages visible`() {
        val round = AgentTimingRoundSnapshot(
            ordinal = 0,
            providerCallIndex = 0,
            attemptIndex = 0,
            responseMode = AgentTimingResponseMode.STREAMING,
            runtimeRunId = null,
            milestones = mapOf(
                AgentTimingEventKind.RECENT_CHATS_STARTED to seconds(1),
                AgentTimingEventKind.RECENT_CHATS_FINISHED to seconds(2),
                AgentTimingEventKind.CONTEXT_GATE_FINAL_FINISHED to seconds(3),
                AgentTimingEventKind.REQUEST_BREAKDOWN_BUILD_STARTED to seconds(4),
                AgentTimingEventKind.REQUEST_BREAKDOWN_BUILD_FINISHED to seconds(5),
            ),
        )

        val metrics = buildAgentTimingDetail(
            trace(finishedAtNs = seconds(6), rounds = listOf(round))
        ).rounds.single().sections.flatMap { it.metrics }.map { it.kind }

        assertTrue(AgentTimingMetricKind.RECENT_CHATS in metrics)
        assertTrue(AgentTimingMetricKind.REQUEST_BREAKDOWN_BUILD in metrics)
    }

    @Test
    fun `provider detail separates first progress from first text and computes decode speed`() {
        val round = AgentTimingRoundSnapshot(
            ordinal = 0,
            providerCallIndex = 0,
            attemptIndex = 0,
            responseMode = AgentTimingResponseMode.STREAMING,
            runtimeRunId = null,
            milestones = mapOf(
                AgentTimingEventKind.APP_PROVIDER_DISPATCH to seconds(1),
                AgentTimingEventKind.PROVIDER_FIRST_PROGRESS to seconds(2),
                AgentTimingEventKind.PROVIDER_FIRST_TEXT to seconds(4),
                AgentTimingEventKind.PROVIDER_STREAM_FINISHED to seconds(6),
            ),
            promptTokens = 100,
            completionTokens = 200,
            cachedTokens = 50,
        )

        val presentation = buildAgentTimingDetail(
            trace(finishedAtNs = seconds(7), rounds = listOf(round))
        ).rounds.single()
        val providerMetrics = presentation.sections
            .single { it.kind == AgentTimingSectionKind.PROVIDER }
            .metrics
            .associateBy { it.kind }

        assertEquals(seconds(1), providerMetrics.getValue(AgentTimingMetricKind.FIRST_PROGRESS).durationNs)
        assertEquals(seconds(3), providerMetrics.getValue(AgentTimingMetricKind.FIRST_TEXT).durationNs)
        assertEquals(200, presentation.providerStats?.outputTokens)
        assertEquals(50.0, presentation.providerStats?.decodeTps ?: 0.0, 0.001)
    }

    @Test
    fun `response layers separate session apply from visible draw`() {
        val detail = buildAgentTimingDetail(
            trace(
                finishedAtNs = seconds(5),
                milestones = mapOf(
                    AgentTimingEventKind.APP_PROVIDER_DISPATCH to seconds(1),
                    AgentTimingEventKind.SESSION_CONTENT_READY to seconds(3),
                    AgentTimingEventKind.FIRST_VISIBLE_DRAW to seconds(4),
                ),
            )
        )

        assertEquals(seconds(2), detail.responseLayers.sessionContentFromDispatchNs)
        assertEquals(seconds(1), detail.responseLayers.visibleDrawFromSessionContentNs)
        assertEquals(AgentTimingVisibleDrawState.OBSERVED, detail.responseLayers.visibleDrawState)
    }

    @Test
    fun `content ready before UI observation is classified for not observed`() {
        val waiting = trace(
            traceSequence = 7,
            finishedAtNs = seconds(3),
            milestones = mapOf(AgentTimingEventKind.SESSION_CONTENT_READY to seconds(2)),
        )
        val alreadyObserved = trace(
            traceSequence = 8,
            finishedAtNs = seconds(3),
            milestones = mapOf(
                AgentTimingEventKind.SESSION_CONTENT_READY to seconds(2),
                AgentTimingEventKind.FIRST_VISIBLE_DRAW to seconds(3),
            ),
        )

        val sequences = AgentTimingConversationSnapshot(
            conversationId = waiting.conversationId,
            traces = listOf(waiting, alreadyObserved),
        ).readyBeforeUiObservationTraceSequences()

        assertEquals(setOf(7L), sequences)
    }

    private fun trace(
        traceSequence: Long = 1,
        finishedAtNs: Long,
        rounds: List<AgentTimingRoundSnapshot> = emptyList(),
        approvalSegments: List<AgentTimingApprovalSegmentSnapshot> = emptyList(),
        milestones: Map<AgentTimingEventKind, Long> = emptyMap(),
    ) = AgentTimingTraceSnapshot(
        traceSequence = traceSequence,
        conversationId = Uuid.random(),
        commandId = null,
        submittedAtNs = 0,
        runtimeRunIds = emptyList(),
        assistantMessageIds = emptyList(),
        status = AgentTimingTraceStatus.COMPLETED,
        pendingApprovalCount = 0,
        finishedAtNs = finishedAtNs,
        lastEventAtNs = finishedAtNs,
        milestones = milestones,
        rounds = rounds,
        tools = emptyList(),
        events = emptyList(),
        aggregates = emptyList(),
        activeSegments = emptyList(),
        approvalSegments = approvalSegments,
        droppedEventCount = 0,
        droppedRoundCount = 0,
        droppedToolCount = 0,
    )

    private fun tool(
        ordinal: Int,
        id: String?,
        milestones: Map<AgentTimingEventKind, Long> = emptyMap(),
        modelResultsReadyAtNs: Long? = null,
        nextDispatchAtNs: Long? = null,
        nextProgressAtNs: Long? = null,
    ) = AgentTimingToolSnapshot(
        ordinal = ordinal,
        roundOrdinal = 0,
        toolCallId = id,
        assistantMessageId = null,
        milestones = milestones,
        sharedModelResultsReadyAtNs = modelResultsReadyAtNs,
        nextProviderDispatchAtNs = nextDispatchAtNs,
        nextProviderFirstProgressAtNs = nextProgressAtNs,
    )

    private fun seconds(value: Long): Long = value * 1_000_000_000L
}
