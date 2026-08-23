package me.rerere.rikkahub.data.ai

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import me.rerere.ai.core.MessageRole
import me.rerere.ai.core.TokenUsage
import me.rerere.ai.ui.MessageChunk
import me.rerere.ai.ui.UIMessage
import me.rerere.ai.ui.UIMessageChoice
import me.rerere.ai.ui.UIMessagePart
import me.rerere.rikkahub.data.ai.tools.CancelRequestResult
import me.rerere.rikkahub.data.ai.tools.ToolCancelReason
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.uuid.Uuid

class ProviderTurnRunnerTest {
    @Test
    fun `stream chunks are relayed back to the caller flow coroutine`() = runBlocking {
        val chunk = testChunk("chunk-1")

        val delivered = flow {
            val outcome = DefaultProviderTurnRunner(runControl = null).run(
                ProviderTurnRequest(
                    stream = true,
                    streamCall = { flowOf(chunk) },
                    singleCall = { error("single call must not run") },
                    // GenerationHandler emits its updated message snapshot here. Calling this
                    // callback from the provider Deferred violates Flow's single-coroutine rule.
                    onChunk = { emit(it) },
                ),
            )
            assertEquals(ProviderTurnOutcome.Completed, outcome)
        }.toList()

        assertEquals(listOf(chunk), delivered)
    }

    @Test
    fun `non stream result is relayed back to the caller flow coroutine`() = runBlocking {
        val chunk = testChunk("single-1")

        val delivered = flow {
            val outcome = DefaultProviderTurnRunner(runControl = null).run(
                ProviderTurnRequest(
                    stream = false,
                    streamCall = { error("stream call must not run") },
                    singleCall = { chunk },
                    onChunk = { emit(it) },
                ),
            )
            assertEquals(ProviderTurnOutcome.Completed, outcome)
        }.toList()

        assertEquals(listOf(chunk), delivered)
    }

    @Test
    fun `disabled timing and watchdog do not classify progress`() = runBlocking {
        var classifierCalls = 0

        val outcome = DefaultProviderTurnRunner(
            runControl = null,
            progressUnits = {
                classifierCalls += 1
                1L
            },
        ).run(
            ProviderTurnRequest(
                stream = true,
                streamCall = { flowOf(textChunk("stream", "content")) },
                singleCall = { error("single call must not run") },
                onChunk = {},
            ),
        )

        assertEquals(ProviderTurnOutcome.Completed, outcome)
        assertEquals(0, classifierCalls)
    }

    @Test
    fun `provider progress is recorded before a blocked chunk consumer`() = runBlocking {
        val firstProgress = CompletableDeferred<Unit>()
        val providerFinished = CompletableDeferred<Unit>()
        val releaseConsumer = CompletableDeferred<Unit>()
        var progressAttempt: Int? = null
        var progressKind: ProviderProgressKind? = null
        val hook = object : ProviderTurnTimingHook {
            override fun onFirstMeaningfulProgress(
                attemptIndex: Int,
                kind: ProviderProgressKind,
            ) {
                progressAttempt = attemptIndex
                progressKind = kind
                firstProgress.complete(Unit)
            }

            override fun onProviderResponseFinished(attemptIndex: Int) {
                providerFinished.complete(Unit)
            }
        }
        val turn = async {
            DefaultProviderTurnRunner(runControl = null).run(
                ProviderTurnRequest(
                    stream = true,
                    streamCall = { flowOf(textChunk("stream", "content")) },
                    singleCall = { error("single call must not run") },
                    onChunk = { releaseConsumer.await() },
                    timingHook = hook,
                ),
            )
        }

        firstProgress.await()
        providerFinished.await()
        assertEquals(0, progressAttempt)
        assertEquals(ProviderProgressKind.STREAM_PROGRESS, progressKind)
        assertFalse(turn.isCompleted)
        releaseConsumer.complete(Unit)
        assertEquals(ProviderTurnOutcome.Completed, turn.await())
    }

    @Test
    fun `empty chunks do not count as first meaningful progress`() = runBlocking {
        var progressEvents = 0
        val terminals = mutableListOf<ProviderAttemptTimingOutcome>()

        DefaultProviderTurnRunner(runControl = null).run(
            ProviderTurnRequest(
                stream = true,
                streamCall = { flowOf(testChunk("empty")) },
                singleCall = { error("single call must not run") },
                onChunk = {},
                timingHook = object : ProviderTurnTimingHook {
                    override fun onFirstMeaningfulProgress(
                        attemptIndex: Int,
                        kind: ProviderProgressKind,
                    ) {
                        progressEvents += 1
                    }

                    override fun onAttemptTerminal(
                        attemptIndex: Int,
                        outcome: ProviderAttemptTimingOutcome,
                    ) {
                        terminals += outcome
                    }
                },
            ),
        )

        assertEquals(0, progressEvents)
        assertEquals(listOf(ProviderAttemptTimingOutcome.COMPLETED), terminals)
    }

    @Test
    fun `reasoning progress is distinct from first visible text and usage`() = runBlocking {
        val events = mutableListOf<String>()

        DefaultProviderTurnRunner(runControl = null).run(
            ProviderTurnRequest(
                stream = true,
                streamCall = {
                    flow {
                        emit(reasoningChunk("reasoning", "thinking"))
                        emit(
                            textChunk("text", "answer").copy(
                                usage = TokenUsage(promptTokens = 20, completionTokens = 5),
                            ),
                        )
                    }
                },
                singleCall = { error("single call must not run") },
                onChunk = {},
                timingHook = object : ProviderTurnTimingHook {
                    override fun onFirstMeaningfulProgress(
                        attemptIndex: Int,
                        kind: ProviderProgressKind,
                    ) {
                        events += "progress:$kind"
                    }

                    override fun onFirstTextProgress(attemptIndex: Int) {
                        events += "text"
                    }

                    override fun onUsage(attemptIndex: Int, usage: TokenUsage) {
                        events += "usage:${usage.completionTokens}"
                    }
                },
            ),
        )

        assertEquals(
            listOf("progress:STREAM_PROGRESS", "text", "usage:5"),
            events,
        )
    }

    @Test
    fun `single response is distinguished from streaming first progress`() = runBlocking {
        val progressKinds = mutableListOf<ProviderProgressKind>()

        DefaultProviderTurnRunner(runControl = null).run(
            ProviderTurnRequest(
                stream = false,
                streamCall = { error("stream call must not run") },
                singleCall = { textChunk("single", "complete response") },
                onChunk = {},
                timingHook = object : ProviderTurnTimingHook {
                    override fun onFirstMeaningfulProgress(
                        attemptIndex: Int,
                        kind: ProviderProgressKind,
                    ) {
                        progressKinds += kind
                    }
                },
            ),
        )

        assertEquals(listOf(ProviderProgressKind.FULL_RESPONSE), progressKinds)
    }

    @Test
    fun `attempt preparation completes before app provider dispatch`() = runBlocking {
        val events = mutableListOf<String>()

        DefaultProviderTurnRunner(runControl = null).run(
            ProviderTurnRequest(
                stream = true,
                beforeAttempt = { isRetry -> events += "prepare-$isRetry" },
                streamCall = {
                    events += "provider-call"
                    flowOf(textChunk("stream", "content"))
                },
                singleCall = { error("single call must not run") },
                onChunk = {},
                timingHook = object : ProviderTurnTimingHook {
                    override fun onBeforeAttempt(attemptIndex: Int, isRetry: Boolean) {
                        events += "before-attempt"
                    }

                    override fun onAppDispatch(attemptIndex: Int, stream: Boolean) {
                        events += "app-dispatch"
                    }

                    override fun onFirstMeaningfulProgress(
                        attemptIndex: Int,
                        kind: ProviderProgressKind,
                    ) {
                        events += "first-progress"
                    }

                    override fun onProviderResponseFinished(attemptIndex: Int) {
                        events += "provider-finished"
                    }

                    override fun onAttemptTerminal(
                        attemptIndex: Int,
                        outcome: ProviderAttemptTimingOutcome,
                    ) {
                        events += "terminal-$outcome"
                    }
                },
            ),
        )

        assertEquals(
            listOf(
                "before-attempt",
                "prepare-false",
                "app-dispatch",
                "provider-call",
                "first-progress",
                "provider-finished",
                "terminal-COMPLETED",
            ),
            events,
        )
    }

    @Test
    fun `rejected pre-dispatch fence sends zero provider calls`() = runBlocking {
        var providerCalls = 0
        var usageCallbacks = 0
        val failure = runCatching {
            DefaultProviderTurnRunner(runControl = null).run(
                ProviderTurnRequest(
                    stream = true,
                    preDispatchFence = { ordinal, retry -> ordinal == 2 && retry },
                    afterAdapterInvocation = { usageCallbacks += 1 },
                    streamCall = {
                        providerCalls += 1
                        flowOf(textChunk("stream", "content"))
                    },
                    singleCall = { error("single call must not run") },
                    onChunk = {},
                ),
            )
        }.exceptionOrNull()

        assertTrue(failure is IllegalStateException)
        assertEquals(0, providerCalls)
        assertEquals(0, usageCallbacks)
    }

    @Test
    fun `rejected primary learned fence dispatches baseline exactly once`() = runBlocking {
        val calls = mutableListOf<String>()
        val delivered = mutableListOf<MessageChunk>()

        val outcome = DefaultProviderTurnRunner(runControl = null).run(
            ProviderTurnRequest(
                stream = true,
                preDispatchFence = { _, _ -> false },
                streamCall = {
                    calls += "learned"
                    flowOf(textChunk("learned", "learned"))
                },
                singleCall = { error("single call must not run") },
                primaryFallback = ProviderPrimaryFallback(
                    streamCall = {
                        calls += "baseline"
                        flowOf(textChunk("baseline", "baseline"))
                    },
                    singleCall = { error("fallback single call must not run") },
                ),
                onChunk = delivered::add,
            ),
        )

        assertEquals(ProviderTurnOutcome.Completed, outcome)
        assertEquals(listOf("baseline"), calls)
        assertEquals(listOf("baseline"), delivered.map(MessageChunk::id))
    }

    @Test
    fun `adapter invocation precedes usage boundary`() = runBlocking {
        val events = mutableListOf<String>()
        DefaultProviderTurnRunner(runControl = null).run(
            ProviderTurnRequest(
                stream = true,
                afterAdapterInvocation = { events += "usage" },
                streamCall = {
                    events += "adapter"
                    flowOf(textChunk("stream", "content"))
                },
                singleCall = { error("single call must not run") },
                onChunk = {},
            ),
        )
        assertEquals(listOf("adapter", "usage"), events)
    }

    @Test
    fun `provider failure reports failed terminal`() = runBlocking {
        val terminals = mutableListOf<ProviderAttemptTimingOutcome>()

        val failure = runCatching {
            DefaultProviderTurnRunner(runControl = null).run(
                ProviderTurnRequest(
                    stream = true,
                    streamCall = { error("provider failed") },
                    singleCall = { error("single call must not run") },
                    onChunk = {},
                    timingHook = object : ProviderTurnTimingHook {
                        override fun onAttemptTerminal(
                            attemptIndex: Int,
                            outcome: ProviderAttemptTimingOutcome,
                        ) {
                            terminals += outcome
                        }
                    },
                ),
            )
        }.exceptionOrNull()

        assertTrue(failure is IllegalStateException)
        assertEquals(listOf(ProviderAttemptTimingOutcome.FAILED), terminals)
    }

    @Test
    fun `provider timing callback failures never alter generation`() = runBlocking {
        val hook = object : ProviderTurnTimingHook {
            override fun onBeforeAttempt(attemptIndex: Int, isRetry: Boolean) =
                error("before-attempt diagnostics failed")

            override fun onAppDispatch(attemptIndex: Int, stream: Boolean) =
                error("dispatch diagnostics failed")

            override fun onFirstMeaningfulProgress(
                attemptIndex: Int,
                kind: ProviderProgressKind,
            ) = error("progress diagnostics failed")

            override fun onProviderResponseFinished(attemptIndex: Int) =
                error("finish diagnostics failed")

            override fun onAttemptTerminal(
                attemptIndex: Int,
                outcome: ProviderAttemptTimingOutcome,
            ) = error("terminal diagnostics failed")
        }

        val outcome = DefaultProviderTurnRunner(runControl = null).run(
            ProviderTurnRequest(
                stream = true,
                streamCall = { flowOf(textChunk("stream", "content")) },
                singleCall = { error("single call must not run") },
                onChunk = {},
                timingHook = hook,
            ),
        )

        assertEquals(ProviderTurnOutcome.Completed, outcome)
    }

    @Test
    fun `steering cancels only provider child and leaves active run coroutine alive`() = runBlocking {
        val control = GenerationRunControl(Uuid.random())
        val providerStarted = CompletableDeferred<Unit>()
        val terminals = mutableListOf<ProviderAttemptTimingOutcome>()
        val turn = async {
            DefaultProviderTurnRunner(control).run(
                ProviderTurnRequest(
                    stream = true,
                    streamCall = {
                        flow<MessageChunk> {
                            providerStarted.complete(Unit)
                            awaitCancellation()
                        }
                    },
                    singleCall = { error("single call must not run") },
                    onChunk = { error("cancelled provider must not deliver a chunk") },
                    timingHook = object : ProviderTurnTimingHook {
                        override fun onAttemptTerminal(
                            attemptIndex: Int,
                            outcome: ProviderAttemptTimingOutcome,
                        ) {
                            terminals += outcome
                        }
                    },
                )
            )
        }
        providerStarted.await()

        assertEquals(
            CancelRequestResult.Requested,
            control.requestProviderCancel(ToolCancelReason.STEERING_OVERRIDE),
        )
        assertEquals(ProviderTurnOutcome.CancelledForSteering, turn.await())
        assertEquals(listOf(ProviderAttemptTimingOutcome.STEERING_CANCELLED), terminals)
        assertTrue(isActive)
    }

    @Test
    fun `stream with no meaningful progress is retried once`() = runBlocking {
        var initialCalls = 0
        var retryCalls = 0
        var rollbacks = 0
        val delivered = mutableListOf<MessageChunk>()
        val recovered = textChunk("retry", "recovered response")
        val attempts = mutableListOf<Pair<Int, Boolean>>()
        val terminals = mutableListOf<Pair<Int, ProviderAttemptTimingOutcome>>()
        val retries = mutableListOf<Pair<Int, Int>>()

        val outcome = DefaultProviderTurnRunner(runControl = null).run(
            ProviderTurnRequest(
                stream = true,
                streamCall = {
                    initialCalls += 1
                    flow { awaitCancellation() }
                },
                retryStreamCall = {
                    retryCalls += 1
                    flowOf(recovered)
                },
                singleCall = { error("single call must not run") },
                onChunk = { delivered.add(it) },
                onBeforeRetry = {
                    rollbacks += 1
                    delivered.clear()
                },
                watchdogConfig = testWatchdog(
                    firstProgressTimeoutMillis = 120L,
                ),
                timingHook = object : ProviderTurnTimingHook {
                    override fun onBeforeAttempt(attemptIndex: Int, isRetry: Boolean) {
                        attempts += attemptIndex to isRetry
                    }

                    override fun onAttemptTerminal(
                        attemptIndex: Int,
                        outcome: ProviderAttemptTimingOutcome,
                    ) {
                        terminals += attemptIndex to outcome
                    }

                    override fun onRetryScheduled(
                        completedAttemptIndex: Int,
                        nextAttemptIndex: Int,
                        reason: ProviderStreamStallReason,
                    ) {
                        retries += completedAttemptIndex to nextAttemptIndex
                    }
                },
            ),
        )

        assertEquals(ProviderTurnOutcome.Completed, outcome)
        assertEquals(1, initialCalls)
        assertEquals(1, retryCalls)
        assertEquals(1, rollbacks)
        assertEquals(listOf(recovered), delivered)
        assertEquals(listOf(0 to false, 1 to true), attempts)
        assertEquals(
            listOf(
                0 to ProviderAttemptTimingOutcome.STALLED,
                1 to ProviderAttemptTimingOutcome.COMPLETED,
            ),
            terminals,
        )
        assertEquals(listOf(0 to 1), retries)
    }

    @Test
    fun `slow partial stream is rolled back before retry output`() = runBlocking {
        val partial = textChunk("partial", "x")
        val recovered = textChunk("retry", "clean retry")
        val delivered = mutableListOf<MessageChunk>()

        val outcome = DefaultProviderTurnRunner(runControl = null).run(
            ProviderTurnRequest(
                stream = true,
                streamCall = {
                    flow {
                        emit(partial)
                        awaitCancellation()
                    }
                },
                retryStreamCall = { flowOf(recovered) },
                singleCall = { error("single call must not run") },
                onChunk = { delivered.add(it) },
                onBeforeRetry = { delivered.clear() },
                watchdogConfig = testWatchdog(),
            ),
        )

        assertEquals(ProviderTurnOutcome.Completed, outcome)
        assertEquals(listOf(recovered), delivered)
    }

    @Test
    fun `slow chunk consumer is not mistaken for a slow provider`() = runBlocking {
        var retryCalls = 0
        val delivered = mutableListOf<MessageChunk>()

        val outcome = DefaultProviderTurnRunner(runControl = null).run(
            ProviderTurnRequest(
                stream = true,
                streamCall = {
                    flow {
                        repeat(6) { index ->
                            emit(textChunk("healthy-$index", "xxxxxxxxxxxxxxxxxxxx"))
                            delay(10L)
                        }
                    }
                },
                retryStreamCall = {
                    retryCalls += 1
                    error("a healthy provider stream must not be retried")
                },
                singleCall = { error("single call must not run") },
                onChunk = { chunk ->
                    delivered.add(chunk)
                    delay(80L)
                },
                watchdogConfig = testWatchdog(),
            ),
        )

        assertEquals(ProviderTurnOutcome.Completed, outcome)
        assertEquals(0, retryCalls)
        assertEquals(6, delivered.size)
    }

    @Test
    fun `second slow stream fails instead of retrying forever`() = runBlocking {
        var calls = 0
        var rollbacks = 0
        val slowCall: suspend () -> kotlinx.coroutines.flow.Flow<MessageChunk> = {
            calls += 1
            flow {
                emit(textChunk("slow-$calls", "x"))
                awaitCancellation()
            }
        }

        val error = runCatching {
            DefaultProviderTurnRunner(runControl = null).run(
                ProviderTurnRequest(
                    stream = true,
                    streamCall = slowCall,
                    retryStreamCall = slowCall,
                    singleCall = { error("single call must not run") },
                    onChunk = {},
                    onBeforeRetry = { rollbacks += 1 },
                    watchdogConfig = testWatchdog(),
                ),
            )
        }.exceptionOrNull()

        assertTrue(error is ProviderStreamStalledException)
        assertEquals(2, calls)
        assertEquals(1, rollbacks)
    }

    @Test
    fun `durable observer sees provider boundary milestones in order`() = runBlocking {
        val events = mutableListOf<ProviderAttemptEvent>()

        val outcome = DefaultProviderTurnRunner(runControl = null).run(
            ProviderTurnRequest(
                stream = false,
                streamCall = { error("stream call must not run") },
                singleCall = { textChunk("single", "meaningful") },
                onChunk = {},
                attemptObserver = ProviderAttemptObserver { event -> events.add(event) },
            ),
        )

        assertEquals(ProviderTurnOutcome.Completed, outcome)
        assertEquals(
            listOf(
                ProviderAttemptEvent.HostDispatched(1, stream = false),
                ProviderAttemptEvent.FirstProgress(1, ProviderProgressKind.FULL_RESPONSE),
                ProviderAttemptEvent.ResponseFinished(1),
                ProviderAttemptEvent.Terminal(1, ProviderAttemptTerminalOutcome.COMPLETED),
            ),
            events,
        )
    }

    @Test
    fun `observer storage failure does not dispatch a fallback request`() = runBlocking {
        var providerCalls = 0
        var observerCalls = 0

        val outcome = DefaultProviderTurnRunner(runControl = null).run(
            ProviderTurnRequest(
                stream = false,
                streamCall = { error("stream call must not run") },
                singleCall = {
                    providerCalls += 1
                    textChunk("single", "meaningful")
                },
                onChunk = {},
                attemptObserver = ProviderAttemptObserver {
                    observerCalls += 1
                    error("attribution store unavailable")
                },
            ),
        )

        assertEquals(ProviderTurnOutcome.Completed, outcome)
        assertEquals(1, providerCalls)
        assertTrue(observerCalls >= 1)
    }

    private fun testWatchdog(
        firstProgressTimeoutMillis: Long = 500L,
    ) = ProviderStreamWatchdogConfig(
        firstProgressTimeoutMillis = firstProgressTimeoutMillis,
        lowSpeedWindowMillis = 120L,
        checkIntervalMillis = 10L,
        minimumProgressUnitsPerWindow = 20L,
    )

    private fun testChunk(id: String) = MessageChunk(
        id = id,
        model = "test-model",
        choices = listOf(
            UIMessageChoice(
                index = 0,
                delta = UIMessage(
                    role = MessageRole.ASSISTANT,
                    parts = emptyList(),
                ),
                message = null,
                finishReason = null,
            ),
        ),
    )

    private fun textChunk(id: String, text: String) = MessageChunk(
        id = id,
        model = "test-model",
        choices = listOf(
            UIMessageChoice(
                index = 0,
                delta = UIMessage(
                    role = MessageRole.ASSISTANT,
                    parts = listOf(UIMessagePart.Text(text)),
                ),
                message = null,
                finishReason = null,
            ),
        ),
    )

    private fun reasoningChunk(id: String, text: String) = MessageChunk(
        id = id,
        model = "test-model",
        choices = listOf(
            UIMessageChoice(
                index = 0,
                delta = UIMessage(
                    role = MessageRole.ASSISTANT,
                    parts = listOf(UIMessagePart.Reasoning(text)),
                ),
                message = null,
                finishReason = null,
            ),
        ),
    )
}
