package me.rerere.rikkahub.diagnostics.agenttiming

import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.uuid.Uuid
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertTrue
import org.junit.Test

class AgentTimingStoreTest {
    @Test
    fun `disabled fast path reads no clock creates no entry and publishes nothing`() {
        val clock = FakeClock()
        val store = AgentTimingStore(clock)
        val conversationId = Uuid.random()
        val flow = store.conversationFlow(conversationId)

        assertNull(store.beginSubmission(conversationId))
        assertEquals(0L, clock.calls.get())
        assertTrue(flow.value.traces.isEmpty())
        assertEquals(
            AgentTimingStoreDebugStats(
                entryCount = 0,
                activeEntryCount = 0,
                terminalEntryCount = 0,
                publicationCount = 0,
            ),
            store.debugStats(),
        )
    }

    @Test
    fun `mark is quiet checkpoint publishes and disable fences late callbacks`() {
        val clock = FakeClock()
        val store = AgentTimingStore(clock)
        val conversationId = Uuid.random()
        val flow = store.conversationFlow(conversationId)
        store.setEnabled(true)

        val handle = store.beginSubmission(conversationId)!!.handle
        val afterSubmission = store.debugStats().publicationCount
        clock.advance(10)
        assertTrue(handle.mark(AgentTimingEventKind.MEMORY_RETRIEVAL_STARTED))
        assertEquals(afterSubmission, store.debugStats().publicationCount)
        clock.advance(10)
        assertTrue(handle.checkpoint(AgentTimingEventKind.MEMORY_RETRIEVAL_FINISHED))
        assertEquals(afterSubmission + 1, store.debugStats().publicationCount)

        val callsBeforeDisable = clock.calls.get()
        store.setEnabled(false)
        assertFalse(handle.mark(AgentTimingEventKind.RUN_ENDED))
        assertEquals(callsBeforeDisable, clock.calls.get())
        assertTrue(flow.value.traces.isEmpty())
        assertEquals(0, store.debugStats().entryCount)
    }

    @Test
    fun `trace bounds retain aggregates after detailed storage is full`() {
        val clock = FakeClock()
        val store = AgentTimingStore(
            clock = clock,
            limits = AgentTimingLimits(
                maxTerminalTracesGlobal = 2,
                maxTerminalTracesPerConversation = 2,
                maxRoundsPerTrace = 2,
                maxToolsPerTrace = 2,
                maxEventsPerTrace = 3,
            ),
        )
        val conversationId = Uuid.random()
        store.setEnabled(true)
        val handle = store.beginSubmission(conversationId)!!.handle

        val round0 = handle.beginRound(providerCallIndex = 0)!!
        assertTrue(handle.beginRound(providerCallIndex = 1) != null)
        assertNull(handle.beginRound(providerCallIndex = 2))
        assertTrue(handle.registerTool(round0, toolCallId = "duplicate") != null)
        assertTrue(handle.registerTool(round0, toolCallId = "duplicate") != null)
        assertNull(handle.registerTool(round0, toolCallId = "duplicate"))

        repeat(5) {
            clock.advance(1)
            assertTrue(handle.mark(AgentTimingEventKind.TOKEN_COUNT_STARTED))
        }

        val snapshot = store.conversationFlow(conversationId).value.traces.single()
        assertEquals(2, snapshot.rounds.size)
        assertEquals(2, snapshot.tools.size)
        assertEquals(3, snapshot.events.size)
        assertEquals(1L, snapshot.droppedRoundCount)
        assertEquals(1L, snapshot.droppedToolCount)
        assertEquals(3L, snapshot.droppedEventCount)
        assertEquals(
            5L,
            snapshot.aggregates.single { it.kind == AgentTimingEventKind.TOKEN_COUNT_STARTED }.count,
        )
        assertEquals(listOf(0, 1), snapshot.tools.map { it.ordinal })
    }

    @Test
    fun `round usage is retained with the provider attempt`() {
        val clock = FakeClock()
        val store = AgentTimingStore(clock)
        val conversationId = Uuid.random()
        store.setEnabled(true)
        val handle = store.beginSubmission(conversationId)!!.handle
        val round = handle.beginRound(providerCallIndex = 0)!!

        assertTrue(
            handle.updateRoundUsage(
                round = round,
                promptTokens = 120,
                completionTokens = 45,
                cachedTokens = 80,
            ),
        )
        handle.checkpoint(AgentTimingEventKind.PROVIDER_STREAM_FINISHED, round)

        val stored = store.conversationFlow(conversationId).value.traces.single().rounds.single()
        assertEquals(120, stored.promptTokens)
        assertEquals(45, stored.completionTokens)
        assertEquals(80, stored.cachedTokens)
    }

    @Test
    fun `terminal limits evict oldest terminal traces but pin active traces`() {
        val clock = FakeClock()
        val store = AgentTimingStore(
            clock = clock,
            limits = AgentTimingLimits(
                maxTerminalTracesGlobal = 3,
                maxTerminalTracesPerConversation = 2,
            ),
        )
        val firstConversation = Uuid.random()
        val secondConversation = Uuid.random()
        store.setEnabled(true)

        val pinned = store.beginSubmission(firstConversation)!!.handle
        repeat(3) { completeTrace(store, clock, firstConversation) }
        repeat(2) { completeTrace(store, clock, secondConversation) }

        val stats = store.debugStats()
        assertEquals(1, stats.activeEntryCount)
        assertEquals(3, stats.terminalEntryCount)
        assertEquals(4, stats.entryCount)
        assertTrue(pinned.isRecording)
        assertEquals(
            1,
            store.conversationFlow(firstConversation).value.traces.count { it.isTerminal },
        )
        assertEquals(
            2,
            store.conversationFlow(secondConversation).value.traces.count { it.isTerminal },
        )
    }

    @Test
    fun `approval timing separates active human wait suspension and resolution`() {
        val clock = FakeClock(nowNs = 0L)
        val store = AgentTimingStore(clock)
        val conversationId = Uuid.random()
        val resumeRunId = Uuid.random()
        store.setEnabled(true)
        val handle = store.beginSubmission(conversationId)!!.handle

        clock.set(100)
        assertTrue(handle.approvalPending(pendingCount = 2))
        clock.set(150)
        assertTrue(handle.approvalDecision(remainingPendingCount = 1))
        clock.set(200)
        assertTrue(handle.approvalDecision(remainingPendingCount = 0))
        clock.set(220)
        assertTrue(handle.mark(AgentTimingEventKind.APPROVAL_COMMIT))
        assertSame(handle, store.openHandleForConversation(conversationId))
        clock.set(250)
        assertTrue(handle.resumeActiveSegment(resumeRunId))
        assertSame(handle, store.handleForRun(resumeRunId))
        clock.set(400)
        assertTrue(handle.finish(AgentTimingTraceStatus.COMPLETED))

        val snapshot = store.conversationFlow(conversationId).value.traces.single()
        assertEquals(400L, snapshot.wallDurationNs)
        assertEquals(250L, snapshot.activeDurationNs)
        assertEquals(100L, snapshot.humanWaitDurationNs)
        assertEquals(150L, snapshot.approvalSuspendedDurationNs)
        assertEquals(50L, snapshot.approvalResolutionDurationNs)
        assertEquals(2, snapshot.activeSegments.size)
        assertEquals(1, snapshot.approvalSegments.size)
        assertEquals(AgentTimingTraceStatus.COMPLETED, snapshot.status)
    }

    @Test
    fun `message and run bindings stay conversation scoped and tolerate duplicate tool ids`() {
        val clock = FakeClock()
        val store = AgentTimingStore(clock)
        val firstConversation = Uuid.random()
        val secondConversation = Uuid.random()
        val messageId = Uuid.random()
        val runId = Uuid.random()
        store.setEnabled(true)
        val first = store.beginSubmission(firstConversation)!!.handle
        val second = store.beginSubmission(secondConversation)!!.handle

        assertTrue(first.bindRun(runId))
        assertTrue(first.bindAssistantMessage(messageId))
        assertFalse(second.bindAssistantMessage(messageId))
        val round = first.beginRound(0)!!
        val firstTool = first.registerTool(round, toolCallId = "same")!!
        val secondTool = first.registerTool(round, toolCallId = "same")!!

        assertTrue(firstTool.ordinal != secondTool.ordinal)
        assertSame(first, store.handleForRun(runId))
        assertSame(first, store.handleForMessage(firstConversation, messageId))
        assertNull(store.handleForMessage(secondConversation, messageId))
        assertTrue(store.conversationFlow(secondConversation).value.traces.single().assistantMessageIds.isEmpty())
    }

    @Test
    fun `approval continuation token reuses original trace without reading clock`() {
        val clock = FakeClock()
        val store = AgentTimingStore(clock)
        val conversationId = Uuid.random()
        val messageId = Uuid.random()
        store.setEnabled(true)
        val original = store.beginSubmission(conversationId)!!
        original.handle.bindAssistantMessage(messageId)
        original.handle.approvalPending(pendingCount = 1)
        val queued = store.beginSubmission(conversationId)!!
        val callsBeforeLookup = clock.calls.get()
        val entriesBeforeLookup = store.debugStats().entryCount

        val resumed = store.submissionTokenForMessage(conversationId, messageId)!!

        assertEquals(original.traceSequence, resumed.traceSequence)
        assertSame(original.handle, resumed.handle)
        assertEquals(callsBeforeLookup, clock.calls.get())
        assertEquals(entriesBeforeLookup, store.debugStats().entryCount)
        assertSame(original.handle, store.openHandleForConversation(conversationId))
        assertSame(queued.handle, store.tokenForHandle(queued.handle)?.handle)
    }

    @Test
    fun `first visible outcome is mutually exclusive and can arrive after terminal`() {
        val clock = FakeClock()
        val store = AgentTimingStore(clock)
        val conversationId = Uuid.random()
        val messageId = Uuid.random()
        store.setEnabled(true)
        val handle = store.beginSubmission(conversationId)!!.handle
        assertTrue(handle.bindAssistantMessage(messageId))
        clock.advance(10)
        assertTrue(handle.finish(AgentTimingTraceStatus.COMPLETED))

        clock.advance(5)
        assertTrue(store.markFirstVisibleDraw(conversationId, messageId))
        clock.advance(5)
        assertFalse(store.markFirstVisibleDraw(conversationId, messageId))
        assertFalse(store.markFirstVisibleNotObserved(conversationId, messageId))

        val snapshot = store.snapshotForMessage(conversationId, messageId)!!
        assertEquals(10L, snapshot.wallDurationNs)
        assertEquals(15L, snapshot.firstVisibleDrawAtNs)
        assertNull(snapshot.at(AgentTimingEventKind.FIRST_VISIBLE_NOT_OBSERVED))
    }

    @Test
    fun `tool snapshot derives execution handoff and next round ttft`() {
        val clock = FakeClock()
        val store = AgentTimingStore(clock)
        val conversationId = Uuid.random()
        store.setEnabled(true)
        val handle = store.beginSubmission(conversationId)!!.handle
        val toolRound = handle.beginRound(providerCallIndex = 0)!!
        val tool = handle.registerTool(toolRound, toolCallId = "tool")!!

        clock.set(10)
        handle.mark(AgentTimingEventKind.TOOL_EXECUTION_STARTED, round = toolRound, tool = tool)
        clock.set(20)
        handle.mark(AgentTimingEventKind.TOOL_RAW_RESULT_READY, round = toolRound, tool = tool)
        clock.set(30)
        handle.mark(AgentTimingEventKind.MODEL_RESULTS_READY, round = toolRound)
        val continuation = handle.beginRound(providerCallIndex = 1)!!
        clock.set(50)
        handle.mark(AgentTimingEventKind.APP_PROVIDER_DISPATCH, round = continuation)
        clock.set(80)
        handle.mark(AgentTimingEventKind.PROVIDER_FIRST_PROGRESS, round = continuation)

        val snapshot = store.conversationFlow(conversationId).value.traces.single()
        val toolSnapshot = snapshot.tools.single()
        assertEquals(10L, toolSnapshot.executionDurationNs)
        assertEquals(10L, toolSnapshot.resultToModelReadyDurationNs)
        assertEquals(20L, toolSnapshot.handoffDurationNs)
        assertEquals(30L, toolSnapshot.nextRoundTtftNs)
        assertEquals(20L, snapshot.rounds[1].handoffFromPreviousResultsNs)
        assertEquals(30L, snapshot.rounds[1].ttftNs)
    }

    @Test
    fun `draw marker capture does not publish until deferred flush`() {
        val clock = FakeClock()
        val store = AgentTimingStore(clock)
        val conversationId = Uuid.random()
        val messageId = Uuid.random()
        val flow = store.conversationFlow(conversationId)
        store.setEnabled(true)
        val handle = store.beginSubmission(conversationId)!!.handle
        handle.bindAssistantMessage(messageId)
        val marker = store.firstVisibleDrawMarker(conversationId, messageId)!!
        val publicationsBeforeDraw = store.debugStats().publicationCount

        clock.set(25)
        assertTrue(marker.captureAfterDraw())
        assertEquals(publicationsBeforeDraw, store.debugStats().publicationCount)
        assertNull(flow.value.traces.single().firstVisibleDrawAtNs)

        assertTrue(marker.publishCaptured())
        assertEquals(publicationsBeforeDraw + 1, store.debugStats().publicationCount)
        assertEquals(25L, flow.value.traces.single().firstVisibleDrawAtNs)
    }

    @Test
    fun `concurrent recorder calls remain bounded and lossless in aggregates`() {
        val clock = FakeClock()
        val store = AgentTimingStore(clock)
        val conversationId = Uuid.random()
        store.setEnabled(true)
        val handle = store.beginSubmission(conversationId)!!.handle
        val start = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(8)
        val futures = List(8) {
            executor.submit {
                start.await()
                repeat(100) {
                    handle.mark(AgentTimingEventKind.TOOL_RAW_RESULT_READY)
                }
            }
        }

        start.countDown()
        futures.forEach { it.get(10, TimeUnit.SECONDS) }
        executor.shutdownNow()

        val snapshot = store.conversationFlow(conversationId).value.traces.single()
        assertEquals(512, snapshot.events.size)
        assertEquals(289L, snapshot.droppedEventCount)
        assertEquals(
            800L,
            snapshot.aggregates.single { it.kind == AgentTimingEventKind.TOOL_RAW_RESULT_READY }.count,
        )
        assertTrue(executor.isShutdown)
    }

    @Test
    fun `clock failure is swallowed and never creates a partial submission`() {
        val clock = FakeClock().apply { shouldThrow.set(true) }
        val store = AgentTimingStore(clock)
        store.setEnabled(true)

        assertNull(store.beginSubmission(Uuid.random()))
        assertEquals(0, store.debugStats().entryCount)
    }

    private fun completeTrace(
        store: AgentTimingStore,
        clock: FakeClock,
        conversationId: Uuid,
    ) {
        clock.advance(1)
        val handle = store.beginSubmission(conversationId)!!.handle
        clock.advance(1)
        assertTrue(handle.finish(AgentTimingTraceStatus.COMPLETED))
    }

    private class FakeClock(nowNs: Long = 0L) : AgentTimingClock {
        private val now = AtomicLong(nowNs)
        val calls = AtomicLong(0L)
        val shouldThrow = AtomicBoolean(false)

        override fun elapsedRealtimeNanos(): Long {
            calls.incrementAndGet()
            if (shouldThrow.get()) error("clock unavailable")
            return now.get()
        }

        fun advance(deltaNs: Long) {
            now.addAndGet(deltaNs)
        }

        fun set(valueNs: Long) {
            now.set(valueNs)
        }
    }
}
