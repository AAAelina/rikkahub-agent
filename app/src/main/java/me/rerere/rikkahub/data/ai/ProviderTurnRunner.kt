package me.rerere.rikkahub.data.ai

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import me.rerere.ai.core.TokenUsage
import me.rerere.ai.ui.MessageChunk
import me.rerere.ai.ui.UIMessage
import me.rerere.ai.ui.UIMessagePart
import me.rerere.rikkahub.diagnostics.agenttiming.estimatedAgentTimingTokenUnits
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

data class ProviderTurnRequest(
    val stream: Boolean,
    /** Per-attempt preparation that must finish before app-provider dispatch (for example lastAccess). */
    val beforeAttempt: (suspend (isRetry: Boolean) -> Unit)? = null,
    /** Runs only after the provider adapter call has been entered successfully. */
    val afterAdapterInvocation: (suspend (isRetry: Boolean) -> Unit)? = null,
    /** Exact authority/config fence evaluated for every attempt immediately before dispatch. */
    val preDispatchFence: (suspend (attemptOrdinal: Int, isRetry: Boolean) -> Boolean)? = null,
    /** Optional baseline request selected only if the first learned pre-dispatch fence closes. */
    val primaryFallback: ProviderPrimaryFallback? = null,
    val streamCall: suspend () -> Flow<MessageChunk>,
    val retryStreamCall: (suspend () -> Flow<MessageChunk>)? = null,
    val singleCall: suspend () -> MessageChunk,
    val onChunk: suspend (MessageChunk) -> Unit,
    val onBeforeRetry: suspend (ProviderStreamStall) -> Unit = {},
    val watchdogConfig: ProviderStreamWatchdogConfig? = null,
    /**
     * Optional in-process observer for Agent Timing. It is deliberately independent from the
     * timing store so the provider path remains allocation-free when diagnostics are disabled.
     */
    val timingHook: ProviderTurnTimingHook? = null,
    /**
     * Durable, attribution-oriented attempt observer. Unlike [timingHook], this callback may
     * suspend while it records a fenced milestone. Non-cancellation failures are isolated from
     * provider execution; every watchdog retry is reported with a new one-based ordinal.
     */
    val attemptObserver: ProviderAttemptObserver? = null,
)

data class ProviderPrimaryFallback(
    val streamCall: suspend () -> Flow<MessageChunk>,
    val retryStreamCall: (suspend () -> Flow<MessageChunk>)? = null,
    val singleCall: suspend () -> MessageChunk,
    val afterAdapterInvocation: (suspend () -> Unit)? = null,
)

enum class ProviderProgressKind {
    STREAM_PROGRESS,
    /** A non-streaming provider returned a complete, meaningful response. Not a true first token. */
    FULL_RESPONSE,
}

enum class ProviderAttemptTimingOutcome {
    COMPLETED,
    STALLED,
    STEERING_CANCELLED,
    CANCELLED,
    FAILED,
}

/**
 * Lightweight provider timing seams. Implementations must not block. Every callback is isolated
 * from provider execution, so a diagnostics failure can never fail or cancel a generation.
 *
 * [onAppDispatch] means the app is about to invoke the provider adapter. It intentionally does not
 * claim that an HTTP request has reached the wire.
 */
interface ProviderTurnTimingHook {
    fun onBeforeAttempt(attemptIndex: Int, isRetry: Boolean) = Unit
    fun onAppDispatch(attemptIndex: Int, stream: Boolean) = Unit
    fun onFirstMeaningfulProgress(attemptIndex: Int, kind: ProviderProgressKind) = Unit
    fun onFirstTextProgress(attemptIndex: Int) = Unit
    fun onRawVisibleTextProgress(attemptIndex: Int, estimatedTokens: Long) = Unit
    fun onConsumerVisibleTextProgress(attemptIndex: Int, estimatedTokens: Long) = Unit
    fun onUsage(attemptIndex: Int, usage: TokenUsage) = Unit
    fun onProviderResponseFinished(attemptIndex: Int) = Unit
    fun onAttemptTerminal(attemptIndex: Int, outcome: ProviderAttemptTimingOutcome) = Unit
    fun onRetryScheduled(
        completedAttemptIndex: Int,
        nextAttemptIndex: Int,
        reason: ProviderStreamStallReason,
    ) = Unit
}

data class ProviderStreamWatchdogConfig(
    val firstProgressTimeoutMillis: Long = 180_000L,
    val lowSpeedWindowMillis: Long = 90_000L,
    val checkIntervalMillis: Long = 5_000L,
    /** Ten estimated tokens/second over the default 90-second rolling window. */
    val minimumProgressUnitsPerWindow: Long = 900L,
) {
    init {
        require(firstProgressTimeoutMillis > 0L)
        require(lowSpeedWindowMillis > 0L)
        require(checkIntervalMillis > 0L)
        require(checkIntervalMillis <= lowSpeedWindowMillis)
        require(minimumProgressUnitsPerWindow > 0L)
    }
}

enum class ProviderStreamStallReason {
    FIRST_PROGRESS_TIMEOUT,
    SUSTAINED_LOW_THROUGHPUT,
}

data class ProviderStreamStall(
    val reason: ProviderStreamStallReason,
    val observedProgressUnits: Long,
    val observationMillis: Long,
)

class ProviderStreamStalledException(
    val stall: ProviderStreamStall,
) : IllegalStateException(
    "Provider stream remained unhealthy after one fresh-connection retry " +
        "(${stall.reason.name.lowercase()})",
)

sealed interface ProviderTurnOutcome {
    data object Completed : ProviderTurnOutcome
    data object CancelledForSteering : ProviderTurnOutcome
}

interface ProviderTurnRunner {
    suspend fun run(request: ProviderTurnRequest): ProviderTurnOutcome
}

class DefaultProviderTurnRunner(
    private val runControl: GenerationRunControl?,
    private val nowMillis: () -> Long = { System.nanoTime() / 1_000_000L },
    private val progressUnits: (MessageChunk) -> Long = { it.estimatedProgressUnits() },
) : ProviderTurnRunner {
    override suspend fun run(request: ProviderTurnRequest): ProviderTurnOutcome {
        if (!request.stream) {
            request.timingHook.safeCall { onBeforeAttempt(attemptIndex = 0, isRetry = false) }
            val outcome = runAttemptSafely(
                request = request,
                streamCall = null,
                watchdogConfig = null,
                attemptIndex = 0,
                isRetry = false,
            )
            if (outcome is ProviderAttemptOutcome.PrimaryFallbackRequired) {
                return run(request.selectPrimaryFallback(outcome.fallback))
            }
            return outcome.toPublicOutcome(request, attemptIndex = 0)
        }

        var retry = false
        var attemptIndex = 0
        while (true) {
            val streamCall = if (retry) request.retryStreamCall else request.streamCall
            checkNotNull(streamCall) { "A watchdog retry requires retryStreamCall" }
            request.timingHook.safeCall { onBeforeAttempt(attemptIndex, retry) }
            when (val outcome = runAttemptSafely(
                request = request,
                streamCall = streamCall,
                watchdogConfig = request.watchdogConfig,
                attemptIndex = attemptIndex,
                isRetry = retry,
            )) {
                is ProviderAttemptOutcome.PrimaryFallbackRequired ->
                    return run(request.selectPrimaryFallback(outcome.fallback))
                ProviderAttemptOutcome.Completed -> {
                    request.attemptObserver.safeObserve(
                        ProviderAttemptEvent.Terminal(
                            attemptOrdinal = attemptIndex + 1,
                            outcome = ProviderAttemptTerminalOutcome.COMPLETED,
                        ),
                    )
                    request.timingHook.safeCall {
                        onAttemptTerminal(attemptIndex, ProviderAttemptTimingOutcome.COMPLETED)
                    }
                    return ProviderTurnOutcome.Completed
                }
                ProviderAttemptOutcome.CancelledForSteering -> {
                    request.attemptObserver.safeObserve(
                        ProviderAttemptEvent.Terminal(
                            attemptOrdinal = attemptIndex + 1,
                            outcome = ProviderAttemptTerminalOutcome.STEERING_CANCELLED,
                        ),
                    )
                    request.timingHook.safeCall {
                        onAttemptTerminal(
                            attemptIndex,
                            ProviderAttemptTimingOutcome.STEERING_CANCELLED,
                        )
                    }
                    return ProviderTurnOutcome.CancelledForSteering
                }

                is ProviderAttemptOutcome.Stalled -> {
                    request.attemptObserver.safeObserve(
                        ProviderAttemptEvent.Terminal(
                            attemptOrdinal = attemptIndex + 1,
                            outcome = ProviderAttemptTerminalOutcome.STALLED_RETRY,
                        ),
                    )
                    request.timingHook.safeCall {
                        onAttemptTerminal(attemptIndex, ProviderAttemptTimingOutcome.STALLED)
                    }
                    if (!retry && request.retryStreamCall != null) {
                        request.onBeforeRetry(outcome.stall)
                        request.timingHook.safeCall {
                            onRetryScheduled(
                                completedAttemptIndex = attemptIndex,
                                nextAttemptIndex = attemptIndex + 1,
                                reason = outcome.stall.reason,
                            )
                        }
                        retry = true
                        attemptIndex += 1
                        continue
                    }
                    throw ProviderStreamStalledException(outcome.stall)
                }
            }
        }
    }

    private suspend fun runAttemptSafely(
        request: ProviderTurnRequest,
        streamCall: (suspend () -> Flow<MessageChunk>)?,
        watchdogConfig: ProviderStreamWatchdogConfig?,
        attemptIndex: Int,
        isRetry: Boolean,
    ): ProviderAttemptOutcome = try {
        runAttempt(request, streamCall, watchdogConfig, attemptIndex, isRetry)
    } catch (cancelled: CancellationException) {
        request.attemptObserver.safeObserve(
            ProviderAttemptEvent.Terminal(
                attemptOrdinal = attemptIndex + 1,
                outcome = ProviderAttemptTerminalOutcome.CANCELLED,
            ),
        )
        request.timingHook.safeCall {
            onAttemptTerminal(attemptIndex, ProviderAttemptTimingOutcome.CANCELLED)
        }
        throw cancelled
    } catch (failure: Throwable) {
        request.attemptObserver.safeObserve(
            ProviderAttemptEvent.Terminal(
                attemptOrdinal = attemptIndex + 1,
                outcome = ProviderAttemptTerminalOutcome.FAILED,
            ),
        )
        request.timingHook.safeCall {
            onAttemptTerminal(attemptIndex, ProviderAttemptTimingOutcome.FAILED)
        }
        throw failure
    }

    private suspend fun runAttempt(
        request: ProviderTurnRequest,
        streamCall: (suspend () -> Flow<MessageChunk>)?,
        watchdogConfig: ProviderStreamWatchdogConfig?,
        attemptIndex: Int,
        isRetry: Boolean,
    ): ProviderAttemptOutcome = supervisorScope {
        val fencePassed = request.preDispatchFence?.invoke(attemptIndex + 1, isRetry) != false
        val attributionPrepared = fencePassed &&
            request.attemptObserver.safePrepare(attemptIndex + 1, isRetry)
        if (!fencePassed || !attributionPrepared) {
            val fallback = request.primaryFallback
            if (!isRetry && fallback != null) {
                return@supervisorScope ProviderAttemptOutcome.PrimaryFallbackRequired(fallback)
            }
            error("Provider pre-dispatch authority/attribution fence rejected the attempt")
        }
        val steeringCancellationRequested = AtomicBoolean(false)
        val cancellationOrigin = AtomicReference<ProviderCancellationOrigin?>(null)
        val detectedStall = AtomicReference<ProviderStreamStall?>(null)
        val progress = watchdogConfig?.let {
            ProviderStreamProgressTracker(it, nowMillis)
        }
        // Provider work must remain a cancellable child, while callbacks must run in this scoped
        // caller coroutine. GenerationHandler's callback emits from a `flow {}` collector, and
        // invoking it directly inside the Deferred violates Flow's single-coroutine invariant.
        // Keep provider collection independent from UI/database processing. Provider callback
        // flows are already losslessly buffered; a rendezvous channel here would reintroduce
        // backpressure and let a slow onChunk callback look like a slow network stream.
        val chunks = Channel<MessageChunk>(capacity = Channel.UNLIMITED)
        val providerChild = async {
            try {
                request.beforeAttempt?.invoke(isRetry)
                request.attemptObserver.safeObserve(
                    ProviderAttemptEvent.HostDispatched(
                        attemptOrdinal = attemptIndex + 1,
                        stream = request.stream,
                    ),
                )
                request.timingHook.safeCall {
                    onAppDispatch(attemptIndex, stream = request.stream)
                }
                var firstMeaningfulProgressReported = false
                var firstTextProgressReported = false
                if (request.stream) {
                    val providerFlow = checkNotNull(streamCall).invoke()
                    request.afterAdapterInvocation?.invoke(isRetry)
                    providerFlow.collect { chunk ->
                        val visibleTextUnits = chunk.estimatedVisibleTextUnits()
                        if (visibleTextUnits > 0L) {
                            request.timingHook.safeCall {
                                onRawVisibleTextProgress(attemptIndex, visibleTextUnits)
                            }
                        }
                        // Record at the provider boundary, before UI/database work. A slow
                        // collector must never be mistaken for a slow upstream stream.
                        if (progress != null ||
                            (request.timingHook != null && !firstMeaningfulProgressReported)
                        ) {
                            val units = progressUnits(chunk)
                            progress?.record(units)
                            if (units > 0L && !firstMeaningfulProgressReported) {
                                firstMeaningfulProgressReported = true
                                request.attemptObserver.safeObserve(
                                    ProviderAttemptEvent.FirstProgress(
                                        attemptOrdinal = attemptIndex + 1,
                                        kind = ProviderProgressKind.STREAM_PROGRESS,
                                    ),
                                )
                                request.timingHook.safeCall {
                                    onFirstMeaningfulProgress(
                                        attemptIndex,
                                        ProviderProgressKind.STREAM_PROGRESS,
                                    )
                                }
                            }
                        }
                        if (request.timingHook != null &&
                            !firstTextProgressReported &&
                            chunk.hasVisibleTextProgress()
                        ) {
                            firstTextProgressReported = true
                            request.timingHook.safeCall { onFirstTextProgress(attemptIndex) }
                        }
                        chunk.usage?.takeIf { request.timingHook != null }?.let { usage ->
                            request.timingHook.safeCall { onUsage(attemptIndex, usage) }
                        }
                        chunks.send(chunk)
                    }
                } else {
                    val chunk = request.singleCall()
                    request.afterAdapterInvocation?.invoke(isRetry)
                    if (request.timingHook != null && chunk.hasVisibleTextProgress()) {
                        request.timingHook.safeCall { onFirstTextProgress(attemptIndex) }
                    }
                    chunk.usage?.takeIf { request.timingHook != null }?.let { usage ->
                        request.timingHook.safeCall { onUsage(attemptIndex, usage) }
                    }
                    if ((request.timingHook != null || request.attemptObserver != null) &&
                        progressUnits(chunk) > 0L
                    ) {
                        request.attemptObserver.safeObserve(
                            ProviderAttemptEvent.FirstProgress(
                                attemptOrdinal = attemptIndex + 1,
                                kind = ProviderProgressKind.FULL_RESPONSE,
                            ),
                        )
                        request.timingHook.safeCall {
                            onFirstMeaningfulProgress(
                                attemptIndex,
                                ProviderProgressKind.FULL_RESPONSE,
                            )
                        }
                    }
                    chunks.send(chunk)
                }
                request.attemptObserver.safeObserve(
                    ProviderAttemptEvent.ResponseFinished(attemptOrdinal = attemptIndex + 1),
                )
                request.timingHook.safeCall { onProviderResponseFinished(attemptIndex) }
            } catch (error: Throwable) {
                chunks.close(error)
                throw error
            } finally {
                chunks.close()
            }
        }
        val registration = runControl?.registerProviderCancel {
            steeringCancellationRequested.set(true)
            if (cancellationOrigin.compareAndSet(null, ProviderCancellationOrigin.STEERING)) {
                providerChild.cancel(
                    CancellationException("Provider cancellation requested by steering"),
                )
            }
        }
        val watchdogJob = progress?.let { tracker ->
            launch {
                while (isActive && providerChild.isActive) {
                    delay(checkNotNull(watchdogConfig).checkIntervalMillis)
                    if (!providerChild.isActive) return@launch
                    val stall = tracker.detectStall() ?: continue
                    if (cancellationOrigin.compareAndSet(null, ProviderCancellationOrigin.WATCHDOG)) {
                        detectedStall.set(stall)
                        providerChild.cancel(
                            CancellationException("Provider stream watchdog requested retry"),
                        )
                    }
                    return@launch
                }
            }
        }
        try {
            for (chunk in chunks) {
                val visibleTextUnits = chunk.estimatedVisibleTextUnits()
                if (visibleTextUnits > 0L) {
                    request.timingHook.safeCall {
                        onConsumerVisibleTextProgress(attemptIndex, visibleTextUnits)
                    }
                }
                request.onChunk(chunk)
            }
            providerChild.await()
            // A successful await means a watchdog cancellation lost a race to natural stream
            // completion. Never retry a provider call that actually completed.
            ProviderAttemptOutcome.Completed
        } catch (cancelled: CancellationException) {
            when (cancellationOrigin.get()) {
                ProviderCancellationOrigin.WATCHDOG -> {
                    if (!currentCoroutineContext().isActive ||
                        runControl?.isRunCancellationRequested() == true
                    ) throw cancelled
                    if (steeringCancellationRequested.get()) {
                        ProviderAttemptOutcome.CancelledForSteering
                    } else {
                        ProviderAttemptOutcome.Stalled(
                            checkNotNull(detectedStall.get()) {
                                "Watchdog cancelled without a stall"
                            },
                        )
                    }
                }

                ProviderCancellationOrigin.STEERING -> {
                    if (!steeringCancellationRequested.get() ||
                        !currentCoroutineContext().isActive ||
                        runControl?.isRunCancellationRequested() == true
                    ) throw cancelled
                    ProviderAttemptOutcome.CancelledForSteering
                }

                null -> throw cancelled
            }
        } finally {
            registration?.close()
            watchdogJob?.cancel()
            chunks.cancel()
            providerChild.cancel()
        }
    }

    private suspend fun ProviderAttemptOutcome.toPublicOutcome(
        request: ProviderTurnRequest,
        attemptIndex: Int,
    ): ProviderTurnOutcome = when (this) {
        ProviderAttemptOutcome.Completed -> {
            request.attemptObserver.safeObserve(
                ProviderAttemptEvent.Terminal(
                    attemptOrdinal = attemptIndex + 1,
                    outcome = ProviderAttemptTerminalOutcome.COMPLETED,
                ),
            )
            request.timingHook.safeCall {
                onAttemptTerminal(attemptIndex, ProviderAttemptTimingOutcome.COMPLETED)
            }
            ProviderTurnOutcome.Completed
        }
        ProviderAttemptOutcome.CancelledForSteering -> {
            request.attemptObserver.safeObserve(
                ProviderAttemptEvent.Terminal(
                    attemptOrdinal = attemptIndex + 1,
                    outcome = ProviderAttemptTerminalOutcome.STEERING_CANCELLED,
                ),
            )
            request.timingHook.safeCall {
                onAttemptTerminal(
                    attemptIndex,
                    ProviderAttemptTimingOutcome.STEERING_CANCELLED,
                )
            }
            ProviderTurnOutcome.CancelledForSteering
        }
        is ProviderAttemptOutcome.Stalled -> throw ProviderStreamStalledException(stall)
        is ProviderAttemptOutcome.PrimaryFallbackRequired -> error(
            "Primary fallback must be resolved before public outcome conversion",
        )
    }
}

private inline fun ProviderTurnTimingHook?.safeCall(block: ProviderTurnTimingHook.() -> Unit) {
    val hook = this ?: return
    try {
        hook.block()
    } catch (_: Throwable) {
        // Timing is an optional sidecar and must never alter provider behavior.
    }
}

private suspend fun ProviderAttemptObserver?.safeObserve(event: ProviderAttemptEvent) {
    val observer = this ?: return
    try {
        observer.observe(event)
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (_: Throwable) {
        // Attribution is a fail-open sidecar. Losing a milestone must not dispatch a fallback,
        // duplicate a provider call, or change the provider result.
    }
}

private suspend fun ProviderAttemptObserver?.safePrepare(
    attemptOrdinal: Int,
    isRetry: Boolean,
): Boolean {
    val observer = this ?: return true
    return try {
        observer.prepareForDispatch(attemptOrdinal, isRetry)
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (_: Throwable) {
        false
    }
}

private enum class ProviderCancellationOrigin { STEERING, WATCHDOG }

private sealed interface ProviderAttemptOutcome {
    data object Completed : ProviderAttemptOutcome
    data object CancelledForSteering : ProviderAttemptOutcome
    data class Stalled(val stall: ProviderStreamStall) : ProviderAttemptOutcome
    data class PrimaryFallbackRequired(
        val fallback: ProviderPrimaryFallback,
    ) : ProviderAttemptOutcome
}

private fun ProviderTurnRequest.selectPrimaryFallback(
    fallback: ProviderPrimaryFallback,
): ProviderTurnRequest = copy(
    preDispatchFence = null,
    primaryFallback = null,
    attemptObserver = null,
    streamCall = fallback.streamCall,
    retryStreamCall = fallback.retryStreamCall,
    singleCall = fallback.singleCall,
    afterAdapterInvocation = { _ -> fallback.afterAdapterInvocation?.invoke() },
)

private data class ProviderProgressSample(val atMillis: Long, val units: Long)

private class ProviderStreamProgressTracker(
    private val config: ProviderStreamWatchdogConfig,
    private val nowMillis: () -> Long,
) {
    private val startedAtMillis = nowMillis()
    private var firstProgressAtMillis: Long? = null
    private val samples = ArrayDeque<ProviderProgressSample>()

    @Synchronized
    fun record(units: Long) {
        if (units <= 0L) return
        val now = nowMillis()
        if (firstProgressAtMillis == null) firstProgressAtMillis = now
        samples.addLast(ProviderProgressSample(now, units))
        discardOldSamples(now)
    }

    @Synchronized
    fun detectStall(): ProviderStreamStall? {
        val now = nowMillis()
        val first = firstProgressAtMillis
        if (first == null) {
            val elapsed = now - startedAtMillis
            return if (elapsed >= config.firstProgressTimeoutMillis) {
                ProviderStreamStall(
                    reason = ProviderStreamStallReason.FIRST_PROGRESS_TIMEOUT,
                    observedProgressUnits = 0L,
                    observationMillis = elapsed,
                )
            } else {
                null
            }
        }

        if (now - first < config.lowSpeedWindowMillis) return null
        discardOldSamples(now)
        val units = samples.sumOf(ProviderProgressSample::units)
        return if (units < config.minimumProgressUnitsPerWindow) {
            ProviderStreamStall(
                reason = ProviderStreamStallReason.SUSTAINED_LOW_THROUGHPUT,
                observedProgressUnits = units,
                observationMillis = config.lowSpeedWindowMillis,
            )
        } else {
            null
        }
    }

    private fun discardOldSamples(now: Long) {
        val cutoff = now - config.lowSpeedWindowMillis
        while (samples.firstOrNull()?.atMillis?.let { it <= cutoff } == true) {
            samples.removeFirst()
        }
    }
}

private fun MessageChunk.estimatedProgressUnits(): Long = choices.sumOf { choice ->
    (choice.delta ?: choice.message)?.estimatedProgressUnits() ?: 0L
}

private fun MessageChunk.hasVisibleTextProgress(): Boolean = choices.any { choice ->
    (choice.delta ?: choice.message)?.parts?.any { part ->
        part is UIMessagePart.Text && part.text.isNotBlank()
    } == true
}

private fun MessageChunk.estimatedVisibleTextUnits(): Long = choices.sumOf { choice ->
    (choice.delta ?: choice.message)?.parts?.sumOf { part ->
        if (part is UIMessagePart.Text) part.text.estimatedAgentTimingTokenUnits() else 0L
    } ?: 0L
}

@Suppress("DEPRECATION")
private fun UIMessage.estimatedProgressUnits(): Long = parts.sumOf { part ->
    when (part) {
        is UIMessagePart.Text -> part.text.estimatedTokenUnits()
        is UIMessagePart.Reasoning -> part.reasoning.estimatedTokenUnits()
        is UIMessagePart.Tool ->
            part.toolName.estimatedTokenUnits() + part.input.estimatedTokenUnits()
        is UIMessagePart.ToolCall ->
            part.toolName.estimatedTokenUnits() + part.arguments.estimatedTokenUnits()
        is UIMessagePart.ToolResult -> part.content.toString().estimatedTokenUnits()
        is UIMessagePart.Image,
        is UIMessagePart.Video,
        is UIMessagePart.Audio,
        is UIMessagePart.Document,
        UIMessagePart.Search,
            -> 1L
    }
}

/** Cheap language-neutral approximation: CJK/non-ASCII code points ~= 1 token, ASCII ~= 4. */
private fun String.estimatedTokenUnits(): Long {
    return estimatedAgentTimingTokenUnits()
}
