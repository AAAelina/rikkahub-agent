package me.rerere.rikkahub.subagent

import android.util.Log
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import me.rerere.ai.core.MessageRole
import me.rerere.ai.ui.UIMessage
import me.rerere.ai.ui.UIMessagePart
import me.rerere.rikkahub.AppScope
import me.rerere.rikkahub.data.agentrun.AgentRunKind
import me.rerere.rikkahub.data.agentrun.AgentRunRepository
import me.rerere.rikkahub.data.agentrun.AgentRunStatus
import me.rerere.rikkahub.data.ai.tools.HeadlessConversations
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.data.model.Conversation
import me.rerere.rikkahub.data.repository.ConversationRepository
import me.rerere.rikkahub.service.ChatService
import me.rerere.rikkahub.service.chat.CommandOrigin
import me.rerere.rikkahub.service.chat.CommandOutcome
import me.rerere.rikkahub.service.chat.StopCommand
import kotlin.uuid.Uuid

private const val TAG = "SubAgentEngine"
private const val STOP_SETTLE_TIMEOUT_MS = 10_000L

/**
 * Returns exactly the final assistant reply exposed by a child run.
 *
 * Earlier assistant messages may contain planning text, partial conclusions, or tool-call
 * preambles. Falling back to those messages when the final reply is empty leaks intermediate
 * work across the sub-agent seam and makes the result depend on retained history.
 */
internal fun selectSubAgentFinalText(messages: List<UIMessage>): String {
    val finalAssistantMessage = messages.lastOrNull { it.role == MessageRole.ASSISTANT }
        ?: return ""
    return finalAssistantMessage.parts
        .filterIsInstance<UIMessagePart.Text>()
        .joinToString("\n") { it.text }
        .trim()
}

/**
 * Dispatches bounded child conversations through the existing ChatService runtime.
 *
 * Model, prompt, tool and trip overrides are resolved once before a run is created, then handed
 * to ChatService through [SubAgentExecutionProfileRegistry]. No parent Assistant is mutated and
 * no second generation pipeline is introduced.
 */
class SubAgentEngine(
    private val registry: SubAgentRegistry,
    private val executionProfileRegistry: SubAgentExecutionProfileRegistry,
    private val conversationRepo: ConversationRepository,
    private val settingsStore: SettingsStore,
    private val appScope: AppScope,
    private val agentRunRepo: AgentRunRepository,
) {
    private val chatService: ChatService by lazy {
        org.koin.java.KoinJavaComponent.getKoin().get<ChatService>()
    }

    private val ledgerIds = java.util.concurrent.ConcurrentHashMap<String, String>()

    sealed class DispatchResult {
        data class Ok(val run: SubAgentRun) : DispatchResult()
        data class Reject(val error: String, val detail: String) : DispatchResult()
    }

    suspend fun dispatch(
        caller: SubAgentCallerContext,
        request: SubAgentRequest,
    ): DispatchResult = withContext(Dispatchers.Default) {
        caller.parentConversationId?.let { rawId ->
            val parentId = runCatching { Uuid.parse(rawId) }.getOrNull()
            if (parentId != null && HeadlessConversations.isHeadless(parentId)) {
                return@withContext DispatchResult.Reject(
                    "no_recursion",
                    "sub-agent dispatch is not allowed from inside another headless run",
                )
            }
        }

        val validation = SubAgentRequestValidator.validate(request)
        if (validation is SubAgentRequestValidator.Result.Reject) {
            return@withContext DispatchResult.Reject(validation.error, validation.detail)
        }
        val cleaned = (validation as SubAgentRequestValidator.Result.Ok).request
        val parentAssistantUuid = runCatching { Uuid.parse(caller.parentAssistantId) }.getOrNull()
            ?: return@withContext DispatchResult.Reject(
                "invalid_parent_assistant",
                "caller assistant id is missing or invalid",
            )
        val settings = settingsStore.settingsFlow.first { !it.init }
        val parentAssistant = settings.assistants.firstOrNull { it.id == parentAssistantUuid }
            ?: return@withContext DispatchResult.Reject(
                "unknown_parent_assistant",
                "caller assistant no longer exists",
            )

        val runId = Uuid.random().toString()
        val parentEffectiveModelId = caller.parentEffectiveModelId
            ?: parentAssistant.chatModelId
            ?: settings.chatModelId
        val availableModelIds = settings.providers
            .asSequence()
            .filter { it.enabled }
            .flatMap { it.models.asSequence() }
            .map { it.id }
            .toSet()
        val profile = when (val resolution = resolveSubAgentExecutionProfile(
            runId = runId,
            request = cleaned,
            inputs = SubAgentExecutionInputs(
                parentEffectiveModelId = parentEffectiveModelId,
                assistantDefaultModelId = parentAssistant.subAgentModelId,
                assistantSystemPrompt = parentAssistant.subAgentSystemPrompt,
                availableModelIds = availableModelIds,
                callerToolNames = caller.toolNames.available,
                headlessToolNames = subAgentHeadlessToolNames(caller.toolNames.available),
                knownToolNames = caller.toolNames.known,
            ),
        )) {
            is SubAgentExecutionProfileResolution.Resolved -> resolution.profile
            is SubAgentExecutionProfileResolution.Rejected ->
                return@withContext DispatchResult.Reject(resolution.error, resolution.detail)
        }

        val perAssistantCap = parentAssistant.maxConcurrentSubAgents.coerceIn(
            SubAgentDefaults.MIN_PER_ASSISTANT_CAP, SubAgentDefaults.MAX_PER_ASSISTANT_CAP)

        val initialRun = SubAgentRun(
            id = runId,
            parentChatId = caller.parentConversationId,
            parentAssistantId = caller.parentAssistantId,
            label = cleaned.label?.takeIf { it.isNotBlank() } ?: cleaned.task.take(60),
            task = cleaned.task,
            modelId = profile.effectiveModelId.toString(),
            tools = profile.effectiveToolNames.sorted(),
            runInBackground = cleaned.runInBackground,
            timeoutSeconds = cleaned.timeoutSeconds,
            maxTrips = profile.maxToolTrips,
            status = SubAgentStatus.PENDING,
            startedAtMs = System.currentTimeMillis(),
        )
        when (registry.reservePending(initialRun, SubAgentDefaults.GLOBAL_CONCURRENCY_CAP, perAssistantCap)) {
            SubAgentRegistry.Reservation.RESERVED -> Unit
            SubAgentRegistry.Reservation.GLOBAL_CAP -> return@withContext DispatchResult.Reject("global_cap_reached", "global concurrency cap reached")
            SubAgentRegistry.Reservation.ASSISTANT_CAP -> return@withContext DispatchResult.Reject("assistant_cap_reached", "assistant concurrency cap reached")
            SubAgentRegistry.Reservation.DUPLICATE -> return@withContext DispatchResult.Reject("duplicate_run", "run already reserved")
        }

        try {

        val ledgerId = agentRunRepo.open(
            kind = AgentRunKind.SubAgent,
            domainId = runId,
            parentRunId = caller.parentConversationId,
            status = AgentRunStatus.queued,
            metadata = buildJsonObject {
                put("label", initialRun.label)
                put("parent_assistant_id", caller.parentAssistantId)
                put("run_in_background", cleaned.runInBackground)
                cleaned.modelId?.let { put("requested_model_id", it) }
                put("effective_model_id", profile.effectiveModelId.toString())
                cleaned.tools?.let { put("requested_tool_count", it.distinct().size) }
                put("effective_tool_count", profile.effectiveToolNames.size)
                put("max_trips", profile.maxToolTrips)
                put("prompt_source", profile.promptSource.name.lowercase())
            },
        )
        ledgerIds[runId] = ledgerId
        // AgentRunRepository.open is best-effort and can swallow cancellation. Do not
        // launch an independent child if this dispatch was cancelled while opening it.
        currentCoroutineContext().ensureActive()

        // A caller can cancel the pending reservation while ledger.open is suspended.
        // In that case there will be no Job completion callback to close the ledger.
        if (registry.get(runId)?.status != SubAgentStatus.PENDING) {
            markTerminal(runId, SubAgentStatus.CANCELLED, "cancelled_before_launch")
            return@withContext DispatchResult.Ok(registry.get(runId) ?: initialRun)
        }

        val executionJob = appScope.launch(Dispatchers.IO, start = CoroutineStart.LAZY) {
            try {
                executeRun(runId, caller, cleaned, profile)
            } finally {
                withContext(NonCancellable) {
                    val status = registry.get(runId)?.status
                    if (status == SubAgentStatus.PENDING || status == SubAgentStatus.RUNNING) {
                        markTerminal(runId, SubAgentStatus.CANCELLED, "execution_ended_before_terminal")
                    }
                    registry.clearJob(runId)
                }
            }
        }
        val completionSettled = CompletableDeferred<Unit>()
        executionJob.invokeOnCompletion { cause ->
            // A cancelled lazy Job never enters executeRun or its finally. Complete
            // both the registry and the captured *durable* ledger row in that case.
            // NonCancellable detaches this finalizer from a cancelled appScope Job;
            // process death is handled separately by AgentRun boot recovery.
            appScope.launch(NonCancellable + Dispatchers.IO) {
                val settled = runCatching {
                    completeSubAgentJob(registry, runId, ledgerId, cause) { id, status, error ->
                        agentRunRepo.markTerminal(id, status, error)
                    }
                }
                settled.onFailure { Log.w(TAG, "sub-agent job completion ledger cleanup failed", it) }
                if (settled.isSuccess) ledgerIds.remove(runId, ledgerId)
                registry.clearJob(runId)
                completionSettled.complete(Unit)
            }
        }
        registry.setJob(runId, executionJob)
        executionJob.start()

        if (cleaned.runInBackground) {
            DispatchResult.Ok(registry.get(runId) ?: initialRun)
        } else {
            executionJob.join()
            completionSettled.await()
            DispatchResult.Ok(registry.get(runId) ?: initialRun)
        }
        } catch (failure: Throwable) {
            // Preserve the *actual* setup failure before stopping any attached Job.
            // Calling requestCancel first converts a pre-launch ledger/launch failure
            // into CANCELLED, causing the durable ledger to record the wrong reason.
            val failureStatus = if (failure is CancellationException) {
                SubAgentStatus.CANCELLED
            } else {
                SubAgentStatus.FAILED
            }
            val failureReason = "dispatch_failed:${failure::class.simpleName}"
            registry.terminalizeIfActive(runId, failureStatus, failureReason)
            withContext(NonCancellable) {
                try {
                    markTerminal(runId, failureStatus, failureReason)
                } catch (cleanupError: Throwable) {
                    Log.w(TAG, "sub-agent dispatch cleanup failed", cleanupError)
                } finally {
                    registry.clearJob(runId)
                }
            }
            throw failure
        }
    }

    private suspend fun executeRun(
        runId: String,
        caller: SubAgentCallerContext,
        request: SubAgentRequest,
        profile: SubAgentExecutionProfile,
    ) {
        registry.update(runId) { it.copy(status = SubAgentStatus.RUNNING) }
        ledgerIds[runId]?.let { agentRunRepo.setStatus(it, AgentRunStatus.running) }

        val parentAssistantId = runCatching { Uuid.parse(caller.parentAssistantId) }.getOrNull()
            ?: run {
                markTerminal(runId, SubAgentStatus.FAILED, "invalid_parent_assistant")
                return
            }
        val conversation = Conversation.ofId(
            id = Uuid.random(),
            assistantId = parentAssistantId,
            newConversation = true,
        ).copy(title = "[Sub-agent] ${request.label?.take(40) ?: request.task.take(40)}")
        var activeOutcome: Deferred<CommandOutcome>? = null

        try {
            conversationRepo.insertConversation(conversation)
            chatService.initializeConversation(conversation.id)
            HeadlessConversations.mark(conversation.id)
            if (!executionProfileRegistry.register(conversation.id, profile)) {
                markTerminal(runId, SubAgentStatus.FAILED, "execution_profile_conflict")
                return
            }

            val taskWithWrapup = buildString {
                append(request.task)
                appendLine()
                appendLine()
                append(
                    "When finished, end with one concise plain-text summary. Do not stop on " +
                        "a tool call: the dispatcher exposes only your final assistant reply.",
                )
            }
            val tracked = chatService.submitUserMessageTracked(
                conversationId = conversation.id,
                content = listOf(UIMessagePart.Text(taskWithWrapup)),
                origin = CommandOrigin.INTERNAL,
                dedupeKey = "subagent:$runId",
                assistantIdSnapshot = parentAssistantId,
            )
            activeOutcome = tracked.outcome
            val outcome = withTimeoutOrNull(request.timeoutSeconds * 1000L) {
                tracked.outcome.await()
            }
            if (outcome == null) {
                withContext(NonCancellable) {
                    stopAndAwait(conversation.id, tracked.outcome)
                }
                markTerminal(
                    runId,
                    SubAgentStatus.TIMED_OUT,
                    "exceeded ${request.timeoutSeconds}-second cap",
                )
                notifyParentIfBackground(caller, registry.get(runId))
                return
            }
            if (outcome != CommandOutcome.Completed) {
                val status = if (outcome == CommandOutcome.Cancelled) {
                    SubAgentStatus.CANCELLED
                } else {
                    SubAgentStatus.FAILED
                }
                markTerminal(runId, status, outcome.failureDescription())
                notifyParentIfBackground(caller, registry.get(runId))
                return
            }

            val finalText = harvestFinalText(conversation.id)
            if (finalText.isBlank()) {
                markTerminal(runId, SubAgentStatus.FAILED, "empty_final_answer")
                notifyParentIfBackground(caller, registry.get(runId))
                return
            }
            registry.update(runId) {
                it.copy(
                    status = SubAgentStatus.SUCCEEDED,
                    result = finalText,
                    finishedAtMs = System.currentTimeMillis(),
                )
            }
            ledgerIds.remove(runId)?.let {
                agentRunRepo.markTerminal(it, AgentRunStatus.succeeded)
            }
            notifyParentIfBackground(caller, registry.get(runId))
        } catch (cancelled: CancellationException) {
            withContext(NonCancellable) {
                activeOutcome?.let { stopAndAwait(conversation.id, it) }
                markTerminal(runId, SubAgentStatus.CANCELLED, "cancelled")
                notifyParentIfBackground(caller, registry.get(runId))
            }
        } catch (error: Throwable) {
            Log.w(TAG, "sub-agent run failed", error)
            markTerminal(
                runId,
                SubAgentStatus.FAILED,
                "${error::class.simpleName}: ${error.message.orEmpty()}",
            )
            notifyParentIfBackground(caller, registry.get(runId))
        } finally {
            executionProfileRegistry.remove(conversation.id, profile.runId)
            HeadlessConversations.unmark(conversation.id)
            registry.clearJob(runId)
        }
    }

    private suspend fun stopAndAwait(
        conversationId: Uuid,
        outcome: Deferred<CommandOutcome>,
    ) {
        chatService.submitEmergency(
            conversationId = conversationId,
            command = StopCommand(pauseQueue = true),
            origin = CommandOrigin.INTERNAL,
        )
        withTimeoutOrNull(STOP_SETTLE_TIMEOUT_MS) { outcome.await() }
    }

    private fun CommandOutcome.failureDescription(): String = when (this) {
        CommandOutcome.Completed -> "completed"
        CommandOutcome.Cancelled -> "cancelled"
        is CommandOutcome.Superseded -> "superseded:$byCommandId"
        is CommandOutcome.Rejected -> "rejected:$reason"
        is CommandOutcome.Conflict -> "conflict:$reason"
        is CommandOutcome.NotApplied -> "not_applied:$reason"
        is CommandOutcome.Failed ->
            "failed:${error::class.simpleName}:${error.message.orEmpty()}"
        is CommandOutcome.SkippedDependencyFailed -> "dependency_failed:$dependencyId"
    }

    private suspend fun markTerminal(runId: String, status: SubAgentStatus, error: String?) {
        registry.update(runId) {
            if (it.status == SubAgentStatus.PENDING || it.status == SubAgentStatus.RUNNING) {
                it.copy(status = status, error = error, finishedAtMs = System.currentTimeMillis())
            } else {
                it
            }
        }
        ledgerIds.remove(runId)?.let { ledgerId ->
            val ledgerStatus = when (registry.get(runId)?.status ?: status) {
                SubAgentStatus.CANCELLED -> AgentRunStatus.cancelled
                SubAgentStatus.SUCCEEDED -> AgentRunStatus.succeeded
                else -> AgentRunStatus.failed
            }
            agentRunRepo.markTerminal(ledgerId, ledgerStatus, error)
        }
    }

    /** Standalone background children wake their parent; coordinated research children do not. */
    private suspend fun notifyParentIfBackground(
        caller: SubAgentCallerContext,
        run: SubAgentRun?,
    ) {
        if (caller.completionPolicy != SubAgentParentCompletionPolicy.NOTIFY_PARENT) return
        val parentChatId = caller.parentConversationId
        if (parentChatId == null || run == null || !run.runInBackground) return
        val parentId = runCatching { Uuid.parse(parentChatId) }.getOrNull() ?: return
        if (HeadlessConversations.isHeadless(parentId)) return

        val message = buildString {
            appendLine("[Sub-agent ${run.label} — ${run.status.name}]")
            run.error?.takeIf { it.isNotBlank() }?.let { appendLine("Error: $it") }
            run.result?.takeIf { it.isNotBlank() }?.let {
                appendLine()
                append(it)
            }
        }.trimEnd()

        runCatching {
            withTimeoutOrNull(5 * 60_000L) {
                chatService.getGenerationJobStateFlow(parentId).first { it == null }
            }
            chatService.sendMessage(parentId, listOf(UIMessagePart.Text(message)))
        }.onFailure {
            Log.w(TAG, "failed to notify parent $parentChatId of subagent completion", it)
        }
    }

    private suspend fun harvestFinalText(conversationId: Uuid): String = runCatching {
        val conversation = conversationRepo.getConversationById(conversationId)
            ?: return@runCatching ""
        selectSubAgentFinalText(
            conversation.messageNodes.mapNotNull { node ->
                node.messages.getOrNull(node.selectIndex)
            },
        )
    }.getOrDefault("")
}
