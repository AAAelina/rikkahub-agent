package me.rerere.rikkahub.data.ai

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import me.rerere.rikkahub.data.ai.tools.CancelRequestResult
import me.rerere.rikkahub.data.ai.tools.ToolCancelReason
import me.rerere.rikkahub.data.ai.tools.ToolExecutionHandle
import me.rerere.rikkahub.data.ai.tools.ToolTerminationState
import me.rerere.rikkahub.service.chat.SteeringHistoryMode
import me.rerere.rikkahub.service.chat.SteeringScope
import kotlin.time.Duration
import kotlin.uuid.Uuid

class GenerationRunControl(
    val runId: Uuid,
    private val onSteeringTransition: (SteeringTransition) -> Unit = {},
) {
    companion object {
        const val MAX_STEERING_CHARS = 4 * 1024
        const val MAX_STEERING_TOKENS = 512
    }
    private val activeTools = ConcurrentHashMap<String, ToolExecutionHandle>()
    private val toolCancellationResults = ConcurrentHashMap<String, CancelRequestResult>()
    private val providerCancel = AtomicReference<(() -> Unit)?>(null)
    private val providerCancellationResult = AtomicReference<CancelRequestResult?>(null)
    private val updateFenced = AtomicBoolean(false)
    private val updateFenceMutex = Mutex()
    private val cancellationCallbacks = ConcurrentHashMap.newKeySet<() -> Unit>()
    private val steeringLock = Any()
    private val steering = ArrayDeque<SteeringNote>()
    private val steeringNotes = mutableMapOf<Uuid, SteeringNote>()
    private val appliedSteering = mutableSetOf<Uuid>()
    private val steeringStates = mutableMapOf<Uuid, SteeringState>()
    private var steeringTokens: Int = 0
    private var steeringClosed: Boolean = false
    /** IDs admitted through the current execution boundary. Guarded by [steeringLock]. */
    private val executingToolCallIds = linkedSetOf<String>()
    /** Process-only authority slot. Provider request construction has no reference to this field. */
    private val commandAuthority = me.rerere.rikkahub.service.chat.RuntimeRunAuthoritySlot()
    internal suspend fun authorityResult(): me.rerere.rikkahub.service.chat.RuntimeAuthorityResult? =
        commandAuthority.current()

    suspend fun runtimeCommandAuthority(): me.rerere.rikkahub.service.chat.RuntimeRunAuthority? =
        commandAuthority.current()

    internal suspend fun attachRuntimeCommandAuthority(
        authority: me.rerere.rikkahub.service.chat.RuntimeRunAuthority,
    ) = commandAuthority.attach(authority)

    @Volatile var interruptedBy: Uuid? = null
        private set
    @Volatile var stoppedBy: Uuid? = null
        private set

    fun isRunCancellationRequested(): Boolean = interruptedBy != null || stoppedBy != null

    suspend fun fenceUpdates() = updateFenceMutex.withLock { updateFenced.set(true) }

    fun isUpdateFenced(): Boolean = updateFenced.get()

    suspend fun runIfUpdatesAllowed(block: suspend () -> Unit): Boolean =
        updateFenceMutex.withLock {
            if (updateFenced.get()) return@withLock false
            block()
            true
        }

    fun hasToolExecutionInFlight(): Boolean = synchronized(steeringLock) {
        executingToolCallIds.isNotEmpty() || activeTools.isNotEmpty()
    }

    fun executingToolCallIds(): Set<String> = synchronized(steeringLock) {
        executingToolCallIds.toSet()
    }

    fun registerTool(toolCallId: String, handle: ToolExecutionHandle) {
        activeTools[toolCallId] = handle
    }

    fun unregisterTool(toolCallId: String, handle: ToolExecutionHandle? = null) {
        if (handle == null) activeTools.remove(toolCallId)
        else activeTools.remove(toolCallId, handle)
    }

    fun activeToolCallIds(): Set<String> = activeTools.keys.toSet()

    fun hasActiveTools(): Boolean = activeTools.isNotEmpty()

    fun registerProviderCancel(cancel: () -> Unit): AutoCloseable {
        providerCancel.set(cancel)
        return AutoCloseable { providerCancel.compareAndSet(cancel, null) }
    }

    fun requestProviderCancel(reason: ToolCancelReason): CancelRequestResult {
        val callback = providerCancel.get()
        val result = if (callback == null) {
            CancelRequestResult.Unsupported
        } else {
            runCatching {
                callback.invoke()
                CancelRequestResult.Requested
            }.getOrElse { CancelRequestResult.Failed(it.message ?: reason.message) }
        }
        providerCancellationResult.set(result)
        return result
    }

    fun providerCancellationResult(): CancelRequestResult? = providerCancellationResult.get()

    fun requestCancelTool(toolCallId: String, reason: ToolCancelReason): CancelRequestResult {
        val handle = activeTools[toolCallId] ?: return CancelRequestResult.NotFound
        val result = runCatching { handle.requestCancel(reason) }
            .getOrElse { CancelRequestResult.Failed(it.message ?: reason.message) }
        toolCancellationResults[toolCallId] = result
        return result
    }

    suspend fun awaitToolTermination(
        toolCallId: String,
        gracePeriod: Duration,
    ): ToolTerminationState {
        val handle = activeTools[toolCallId] ?: return ToolTerminationState.Unknown
        return handle.awaitTermination(gracePeriod)
    }

    fun requestCancelAllTools(reason: ToolCancelReason): Map<String, CancelRequestResult> {
        val results = activeTools.mapValues { (_, handle) ->
            runCatching { handle.requestCancel(reason) }
                .getOrElse { CancelRequestResult.Failed(it.message ?: reason.message) }
        }
        toolCancellationResults.putAll(results)
        return results
    }

    fun toolCancellationResults(): Map<String, CancelRequestResult> = toolCancellationResults.toMap()

    /** True when the current tool was explicitly cancelled through the runtime control path. */
    fun isToolCancellationRequested(toolCallId: String): Boolean =
        toolCancellationResults.containsKey(toolCallId)

    fun toolCancellationResult(toolCallId: String): CancelRequestResult? =
        toolCancellationResults[toolCallId]

    suspend fun awaitToolTermination(gracePeriod: Duration): Map<String, ToolTerminationState> =
        activeTools.mapValues { (_, handle) -> handle.awaitTermination(gracePeriod) }

    fun registerCancellationCallback(callback: () -> Unit): AutoCloseable {
        if (isRunCancellationRequested()) {
            callback()
            return AutoCloseable { }
        }
        cancellationCallbacks.add(callback)
        if (isRunCancellationRequested() && cancellationCallbacks.remove(callback)) callback()
        return AutoCloseable { cancellationCallbacks.remove(callback) }
    }

    private fun notifyCancellationCallbacks() {
        cancellationCallbacks.toList().forEach { callback ->
            if (cancellationCallbacks.remove(callback)) runCatching(callback)
        }
    }

    fun markInterruptedBy(commandId: Uuid) {
        interruptedBy = commandId
        stoppedBy = null
        notifyCancellationCallbacks()
    }

    fun markStoppedBy(commandId: Uuid) {
        stoppedBy = commandId
        interruptedBy = null
        notifyCancellationCallbacks()
    }

    /**
     * Linearizes the boundary between an already-running tool and newly submitted steering.
     * A tool that wins this lock may finish normally; steering that wins first prevents the
     * old plan's next tool from starting.
     */
    fun beginToolExecutionOrYieldToSteering(toolCallId: String): ToolStartDecision =
        beginToolBatchOrYieldToSteering(setOf(toolCallId))

    /**
     * Linearizes one serial tool or one explicitly planned parallel batch against steering.
     * A batch that has started is allowed to settle as a unit; newly submitted guidance blocks
     * only the next boundary. Callers cannot replace an active batch with unrelated IDs.
     */
    fun beginToolBatchOrYieldToSteering(toolCallIds: Set<String>): ToolStartDecision {
        require(toolCallIds.isNotEmpty()) { "A tool execution batch cannot be empty" }
        return synchronized(steeringLock) {
            val stableIds = toolCallIds.toSet()
            if (isRunCancellationRequested() || updateFenced.get()) {
                ToolStartDecision.RunCancelled
            } else if (hasUndeliveredSteeringLocked()) {
                ToolStartDecision.YieldToSteering
            } else {
                check(executingToolCallIds.isEmpty() || executingToolCallIds == stableIds) {
                    "Tool batch $executingToolCallIds is already inside the execution boundary"
                }
                executingToolCallIds.addAll(stableIds)
                ToolStartDecision.Proceed
            }
        }
    }

    fun finishToolExecution(toolCallId: String) {
        synchronized(steeringLock) {
            executingToolCallIds.remove(toolCallId)
        }
    }

    fun finishToolBatch(toolCallIds: Set<String>) {
        synchronized(steeringLock) {
            executingToolCallIds.removeAll(toolCallIds)
        }
    }

    fun hasUndeliveredSteering(): Boolean = synchronized(steeringLock) {
        hasUndeliveredSteeringLocked()
    }

    private fun hasUndeliveredSteeringLocked(): Boolean = steering.any { note ->
        steeringStates[note.commandId] == SteeringState.PENDING ||
            steeringStates[note.commandId] == SteeringState.DELIVERING
    }

    fun submitSteering(note: SteeringNote): SteeringRegistrationResult {
        val result = synchronized(steeringLock) {
            when {
                steeringClosed -> SteeringRegistrationResult.RunClosed
                note.runId != runId -> SteeringRegistrationResult.Rejected("Steering belongs to another run")
                note.text.isBlank() || note.text.length > MAX_STEERING_CHARS ->
                    SteeringRegistrationResult.Rejected("Steering text exceeds the 4 KiB limit")
                steeringTokens + estimateTokens(note.text) > MAX_STEERING_TOKENS ->
                    SteeringRegistrationResult.Rejected("Steering token budget exhausted")
                else -> {
                    steeringTokens += estimateTokens(note.text)
                    steeringStates[note.commandId] = SteeringState.PENDING
                    steeringNotes[note.commandId] = note
                    steering.addLast(note)
                    SteeringRegistrationResult.Accepted
                }
            }
        }
        when (result) {
            SteeringRegistrationResult.Accepted ->
                onSteeringTransition(SteeringTransition(note.commandId, SteeringState.PENDING))
            SteeringRegistrationResult.RunClosed -> Unit
            is SteeringRegistrationResult.Rejected -> {
                synchronized(steeringLock) {
                    steeringStates[note.commandId] = SteeringState.REJECTED_NOT_STEERABLE
                }
                onSteeringTransition(
                    SteeringTransition(
                        note.commandId,
                        SteeringState.REJECTED_NOT_STEERABLE,
                        result.reason,
                    )
                )
            }
        }
        return result
    }

    fun takeSteeringForCheckpoint(modelCallIndex: Int): List<SteeringDelivery> {
        val transitions = mutableListOf<SteeringTransition>()
        val deliveries = synchronized(steeringLock) {
            if (steeringClosed) return@synchronized emptyList()
            buildList {
                steering.forEach { queuedNote ->
                    val note = steeringNotes[queuedNote.commandId] ?: queuedNote
                    when (steeringStates[note.commandId]) {
                        SteeringState.PENDING -> {
                            add(SteeringDelivery(note, firstApplication = true))
                            steeringStates[note.commandId] = SteeringState.DELIVERING
                            transitions += SteeringTransition(note.commandId, SteeringState.DELIVERING)
                        }
                        SteeringState.APPLIED -> if (note.scope == SteeringScope.REMAINDER_OF_RUN) {
                            add(SteeringDelivery(note, firstApplication = false))
                        }
                        else -> Unit
                    }
                }
            }
        }
        transitions.forEach(onSteeringTransition)
        return deliveries
    }

    /** Commits a delivery only after the provider has produced observable output. */
    fun markSteeringProviderStarted(deliveries: List<SteeringDelivery>) {
        val transitions = mutableListOf<SteeringTransition>()
        synchronized(steeringLock) {
            deliveries.filter { it.firstApplication }.forEach { delivery ->
                val note = steeringNotes[delivery.note.commandId] ?: delivery.note
                if (steeringStates[note.commandId] != SteeringState.DELIVERING) return@forEach
                appliedSteering += note.id
                steeringStates[note.commandId] = SteeringState.APPLIED
                if (note.scope == SteeringScope.NEXT_MODEL_CALL) {
                    steering.removeAll { it.commandId == note.commandId }
                }
                transitions += SteeringTransition(note.commandId, SteeringState.APPLIED)
            }
        }
        transitions.forEach(onSteeringTransition)
    }

    /** Makes a never-started provider delivery eligible for the next call in the same run. */
    fun markSteeringDeliveryFailed(deliveries: List<SteeringDelivery>) {
        val transitions = mutableListOf<SteeringTransition>()
        synchronized(steeringLock) {
            deliveries.filter { it.firstApplication }.forEach { delivery ->
                val commandId = delivery.note.commandId
                if (steeringStates[commandId] != SteeringState.DELIVERING) return@forEach
                steeringStates[commandId] = SteeringState.PENDING
                transitions += SteeringTransition(commandId, SteeringState.PENDING)
            }
        }
        transitions.forEach(onSteeringTransition)
    }

    fun updateSteeringHistoryMode(commandId: Uuid, historyMode: SteeringHistoryMode): Boolean {
        var transitionState: SteeringState? = null
        val updated = synchronized(steeringLock) {
            if (steeringClosed) return@synchronized false
            val state = steeringStates[commandId] ?: return@synchronized false
            if (state != SteeringState.PENDING &&
                state != SteeringState.DELIVERING &&
                state != SteeringState.APPLIED
            ) return@synchronized false
            transitionState = state
            val note = steeringNotes[commandId] ?: return@synchronized false
            steeringNotes[commandId] = note.copy(historyMode = historyMode)
            val index = steering.indexOfFirst { it.commandId == commandId }
            if (index >= 0) steering[index] = steeringNotes.getValue(commandId)
            true
        }
        if (updated) {
            onSteeringTransition(
                SteeringTransition(commandId, checkNotNull(transitionState), historyMode = historyMode)
            )
        }
        return updated
    }

    /**
     * Forgets guidance so it cannot be delivered at another checkpoint or persisted when the
     * run finishes. A provider request that already left the process cannot be recalled, but
     * cancelling while it is DELIVERING prevents that delivery from being committed as applied.
     */
    fun cancelSteering(commandId: Uuid): Boolean = synchronized(steeringLock) {
        val note = steeringNotes.remove(commandId) ?: return@synchronized false
        steering.removeAll { it.commandId == commandId }
        steeringStates.remove(commandId)
        appliedSteering.remove(note.id)
        steeringTokens = steering.sumOf { queued ->
            estimateTokens((steeringNotes[queued.commandId] ?: queued).text)
        }
        true
    }

    fun pendingSteering(): List<SteeringNote> = synchronized(steeringLock) {
        steering.map { steeringNotes[it.commandId] ?: it }
    }

    fun steeringNotes(): Map<Uuid, SteeringNote> = synchronized(steeringLock) { steeringNotes.toMap() }
    fun wasApplied(noteId: Uuid): Boolean = synchronized(steeringLock) { noteId in appliedSteering }

    fun closeSteering(): List<SteeringTransition> {
        val transitions = synchronized(steeringLock) {
            if (steeringClosed) return@synchronized emptyList()
            steeringClosed = true
            val pending = steering
                .filter {
                    steeringStates[it.commandId] == SteeringState.PENDING ||
                        steeringStates[it.commandId] == SteeringState.DELIVERING
                }
                .map {
                    steeringStates[it.commandId] = SteeringState.NOT_APPLIED_RUN_FINISHED
                    SteeringTransition(
                        it.commandId,
                        SteeringState.NOT_APPLIED_RUN_FINISHED,
                        "Run finished before the next model checkpoint",
                    )
                }
            steering.clear()
            steeringTokens = 0
            pending
        }
        transitions.forEach(onSteeringTransition)
        return transitions
    }

    fun markFallbackQueued(commandId: Uuid) {
        synchronized(steeringLock) {
            steeringStates[commandId] = SteeringState.FALLBACK_QUEUED
        }
        onSteeringTransition(SteeringTransition(commandId, SteeringState.FALLBACK_QUEUED))
    }

    fun steeringStates(): Map<Uuid, SteeringState> = synchronized(steeringLock) { steeringStates.toMap() }

    private fun estimateTokens(text: String): Int = (text.length + 3) / 4
}

sealed interface ToolStartDecision {
    data object Proceed : ToolStartDecision
    data object YieldToSteering : ToolStartDecision
    data object RunCancelled : ToolStartDecision
}

sealed interface SteeringRegistrationResult {
    data object Accepted : SteeringRegistrationResult
    data object RunClosed : SteeringRegistrationResult
    data class Rejected(val reason: String) : SteeringRegistrationResult
}

data class SteeringDelivery(
    val note: SteeringNote,
    val firstApplication: Boolean,
)

data class SteeringTransition(
    val commandId: Uuid,
    val state: SteeringState,
    val reason: String? = null,
    val historyMode: SteeringHistoryMode? = null,
)

enum class SteeringState {
    PENDING,
    DELIVERING,
    APPLIED,
    FALLBACK_QUEUED,
    NOT_APPLIED_RUN_FINISHED,
    REJECTED_NOT_STEERABLE,
}

data class SteeringNote(
    val id: Uuid = Uuid.random(),
    val commandId: Uuid,
    val runId: Uuid,
    val text: String,
    val source: me.rerere.rikkahub.service.chat.CommandOrigin,
    val scope: SteeringScope = SteeringScope.REMAINDER_OF_RUN,
    val historyMode: SteeringHistoryMode = SteeringHistoryMode.TRANSIENT,
)
