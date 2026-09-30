package me.rerere.rikkahub.data.execution

import me.rerere.rikkahub.data.ai.execution.RedactedToolLifecycleEvent
import me.rerere.rikkahub.data.ai.execution.RedactedToolCallContext
import me.rerere.rikkahub.data.ai.execution.CriticalLifecyclePersistenceException
import me.rerere.rikkahub.data.ai.execution.CriticalToolLifecycleSink
import me.rerere.rikkahub.data.ai.tools.ToolTerminationState
import me.rerere.rikkahub.data.capability.ResourceScope
import me.rerere.rikkahub.data.capability.SubjectType
import me.rerere.rikkahub.data.capability.ToolCapabilityResolver
import me.rerere.rikkahub.data.scope.CanonicalId
import me.rerere.rikkahub.data.scope.AgentScope
import kotlin.uuid.Uuid

/**
 * Projects ToolRuntime lifecycle events into the authoritative execution table.
 *
 * It intentionally resolves resource metadata without argument values, so no command, file
 * content, credential, URL parameter, or tool output can leak into Room, audit, or logs.
 */
class ExecutionRecordCriticalToolLifecycleSink(
    private val repository: ExecutionRepository,
) : CriticalToolLifecycleSink {
    override suspend fun persist(event: RedactedToolLifecycleEvent) {
        val context = event.context
        val recordId = ExecutionRecordIds.tool(context.runId, context.toolCallId)
        when (event.phase) {
            RedactedToolLifecycleEvent.Phase.STARTING -> {
                val resolved = ToolCapabilityResolver.resolve(context.toolName)
                val record = repository.open(
                    ExecutionRecordDraft(
                        id = recordId,
                        traceId = context.runId,
                        commandId = context.commandId,
                        conversationId = context.conversationId,
                        toolCallId = context.toolCallId,
                        toolName = context.toolName,
                        toolSchemaFingerprint = context.toolSchemaFingerprint,
                        learningScope = context.toLearningScope(),
                        subjectId = context.subjectId.ifBlank { context.assistantId },
                        subjectType = (context.subjectType ?: SubjectType.LOCAL_ASSISTANT).name,
                        origin = context.origin.name,
                        capabilityKeys = resolved.capabilities
                            .map { it.value }
                            .sorted()
                            .joinToString(","),
                        resourceSummary = resolved.resource.toAuditSummary(),
                        runtime = runtimeFor(context.toolName, context.legacyExecution),
                        idempotencyKey = ExecutionRecordIds.toolIdempotency(
                            context.runId,
                            context.toolCallId,
                        ),
                        initialStatus = ExecutionStatus.starting,
                    ),
                )
                val current = ExecutionStatus.fromWire(record.status)
                if (current != ExecutionStatus.starting && current != ExecutionStatus.running) {
                    requireDurable(
                        repository.transition(
                            id = recordId,
                            target = ExecutionStatus.starting,
                            mutationId = mutationId(event),
                            reasonCode = "tool_starting",
                        ),
                        ExecutionStatus.starting,
                    )
                }
            }

            RedactedToolLifecycleEvent.Phase.RUNNING -> requireDurable(
                repository.transition(
                    id = recordId,
                    target = ExecutionStatus.running,
                    runtimeHandleSummary = event.executionId,
                    runtime = event.executionId?.let { runtimeForHandle(context.toolName, it) },
                    mutationId = mutationId(event),
                    reasonCode = "tool_running",
                ),
                ExecutionStatus.running,
            )

            RedactedToolLifecycleEvent.Phase.CANCEL_REQUESTED -> requireDurable(
                repository.transition(
                    id = recordId,
                    target = ExecutionStatus.cancel_requested,
                    runtimeHandleSummary = event.executionId,
                    detail = event.detail ?: "cancel_requested",
                    mutationId = mutationId(event),
                    reasonCode = event.detail ?: "cancel_requested",
                    requestedTerminalOutcome = event.requestedTerminalOutcome,
                ),
                ExecutionStatus.cancel_requested,
            )

            RedactedToolLifecycleEvent.Phase.TERMINATING -> requireDurable(
                repository.transition(
                    id = recordId,
                    target = ExecutionStatus.terminating,
                    runtimeHandleSummary = event.executionId,
                    mutationId = mutationId(event),
                    reasonCode = "termination_started",
                    requestedTerminalOutcome = event.requestedTerminalOutcome,
                ),
                ExecutionStatus.terminating,
            )

            RedactedToolLifecycleEvent.Phase.COMPLETED -> requireDurable(
                repository.transition(
                    id = recordId,
                    target = ExecutionStatus.succeeded,
                    runtimeHandleSummary = event.executionId,
                    mutationId = mutationId(event),
                    reasonCode = "tool_completed",
                    verificationState = VerificationState.LIVE_CONFIRMED,
                ),
                ExecutionStatus.succeeded,
            )

            RedactedToolLifecycleEvent.Phase.FAILED -> requireDurable(
                repository.transition(
                    id = recordId,
                    target = ExecutionStatus.failed,
                    runtimeHandleSummary = event.executionId,
                    detail = event.detail ?: "runtime_failed",
                    mutationId = mutationId(event),
                    reasonCode = event.detail ?: "runtime_failed",
                ),
                ExecutionStatus.failed,
            )

            RedactedToolLifecycleEvent.Phase.CANCELLED -> {
                val confirmed = event.terminationState == ToolTerminationState.StoppedConfirmed
                val target = if (confirmed) ExecutionStatus.cancelled else ExecutionStatus.terminating
                val reason = if (confirmed) "termination_confirmed" else "termination_unconfirmed"
                requireDurable(
                    repository.transition(
                        id = recordId,
                        target = target,
                        runtimeHandleSummary = event.executionId,
                        cancellationResult = event.terminationState?.name,
                        detail = reason,
                        mutationId = mutationId(event),
                        reasonCode = reason,
                        verificationState = if (confirmed) {
                            VerificationState.LIVE_CONFIRMED
                        } else {
                            VerificationState.STALE
                        },
                        requestedTerminalOutcome = RequestedTerminalOutcome.CANCELLED,
                    ),
                    target,
                )
            }

            RedactedToolLifecycleEvent.Phase.TIMED_OUT -> {
                val decision = decideTimedOutExecution(
                    terminationState = event.terminationState,
                    hasRuntimeHandle = event.executionId != null,
                )
                requireDurable(
                    repository.transition(
                        id = recordId,
                        target = decision.target,
                        runtimeHandleSummary = event.executionId,
                        cancellationResult = event.terminationState?.name,
                        detail = if (event.terminationState == ToolTerminationState.StoppedConfirmed) {
                            "wall_clock_timeout_termination_confirmed"
                        } else {
                            "wall_clock_timeout_termination_unconfirmed"
                        },
                        mutationId = mutationId(event),
                        reasonCode = event.detail ?: "wall_clock_timeout",
                        verificationState = decision.verification,
                        requestedTerminalOutcome = RequestedTerminalOutcome.TIMED_OUT,
                    ),
                    decision.target,
                )
            }
        }
    }

    private fun requireDurable(
        result: ExecutionTransitionResult,
        requested: ExecutionStatus,
    ) {
        when (result) {
            is ExecutionTransitionResult.Applied -> Unit
            is ExecutionTransitionResult.Terminal -> {
                if (ExecutionStatus.fromWire(result.record.status) != requested) {
                    throw CriticalLifecyclePersistenceException("execution_already_terminal")
                }
            }
            is ExecutionTransitionResult.Missing ->
                throw CriticalLifecyclePersistenceException("execution_record_missing")
            is ExecutionTransitionResult.Invalid ->
                throw CriticalLifecyclePersistenceException("execution_transition_invalid")
            is ExecutionTransitionResult.Conflict ->
                throw CriticalLifecyclePersistenceException("execution_cas_conflict")
        }
    }

    private fun mutationId(event: RedactedToolLifecycleEvent): String =
        ExecutionRecordIds.toolEvent(
            runId = event.context.runId,
            toolCallId = event.context.toolCallId,
            phase = event.phase.name,
            executionId = event.executionId,
        )

    // Runtime arguments may contain commands, paths, hosts, or user text. The execution ledger
    // stores only the stable resource class; the executable payload remains in the message graph.
    private fun ResourceScope.toAuditSummary(): String = kind

    private fun runtimeFor(toolName: String, legacy: Boolean): ExecutionRuntime = when {
        legacy -> ExecutionRuntime.LEGACY
        toolName.startsWith("termux_") -> ExecutionRuntime.TERMUX
        toolName.startsWith("ssh_") -> ExecutionRuntime.SSH
        toolName.startsWith("workspace_") -> ExecutionRuntime.WORKSPACE
        toolName.startsWith("mcp__") -> ExecutionRuntime.MCP
        toolName.startsWith("plugin__") -> ExecutionRuntime.PLUGIN
        toolName.startsWith("privileged_") || toolName.startsWith("external_bridge_") ->
            ExecutionRuntime.SHIZUKU
        else -> ExecutionRuntime.LOCAL_TOOL
    }

    private fun runtimeForHandle(toolName: String, handle: String): ExecutionRuntime = when {
        handle.startsWith("termux:") -> ExecutionRuntime.TERMUX
        handle.startsWith("workspace:") -> ExecutionRuntime.WORKSPACE
        toolName.startsWith("ssh_") -> ExecutionRuntime.SSH
        else -> runtimeFor(toolName, legacy = false)
    }

}

internal fun RedactedToolCallContext.toLearningScope(): AgentScope =
    if (subjectType == SubjectType.LOCAL_SECOND_USER) {
        AgentScope.AuthoritySubject(subjectId)
    } else {
        AgentScope.Assistant(Uuid.parse(assistantId))
    }

internal data class TimedOutExecutionDecision(
    val target: ExecutionStatus,
    val verification: VerificationState,
)

internal fun decideTimedOutExecution(
    terminationState: ToolTerminationState?,
    hasRuntimeHandle: Boolean,
): TimedOutExecutionDecision {
    val confirmed = terminationState == ToolTerminationState.StoppedConfirmed || !hasRuntimeHandle
    return if (confirmed) {
        TimedOutExecutionDecision(ExecutionStatus.timed_out, VerificationState.LIVE_CONFIRMED)
    } else {
        TimedOutExecutionDecision(ExecutionStatus.terminating, VerificationState.STALE)
    }
}

object ExecutionRecordIds {
    /**
     * Keeps the established readable ID for normal UUID runs, but hashes any non-canonical or
     * oversized identity with a versioned, length-prefixed digest. No identity is ever truncated.
     */
    fun tool(runId: String, toolCallId: String): String {
        validateIdentityField("runId", runId)
        validateIdentityField("toolCallId", toolCallId)
        val legacy = "tool:$runId:$toolCallId"
        val canonicalRun = runCatching { Uuid.parse(runId).toString() == runId }.getOrDefault(false)
        return if (canonicalRun && legacy.length <= MAX_EXECUTION_ID_CHARS) {
            legacy
        } else {
            "tool-v2:" + CanonicalId.digest(
                domainVersion = "execution-tool-record-v2",
                fields = listOf(runId, toolCallId),
            )
        }
    }

    fun toolIdempotency(runId: String, toolCallId: String): String {
        validateIdentityField("runId", runId)
        validateIdentityField("toolCallId", toolCallId)
        val legacy = "tool:$runId:$toolCallId"
        val canonicalRun = runCatching { Uuid.parse(runId).toString() == runId }.getOrDefault(false)
        return if (canonicalRun && legacy.length <= MAX_IDEMPOTENCY_CHARS) {
            legacy
        } else {
            "tool-idempotency-v2:" + CanonicalId.digest(
                domainVersion = "execution-tool-idempotency-v2",
                fields = listOf(runId, toolCallId),
            )
        }
    }

    fun toolEvent(
        runId: String,
        toolCallId: String,
        phase: String,
        executionId: String?,
    ): String {
        validateIdentityField("runId", runId)
        validateIdentityField("toolCallId", toolCallId)
        validateIdentityField("phase", phase)
        executionId?.let { validateIdentityField("executionId", it) }
        val legacy = buildString {
            append("tool-event:").append(runId).append(':').append(toolCallId).append(':').append(phase)
            executionId?.let { append(':').append(it) }
        }
        val canonicalRun = runCatching { Uuid.parse(runId).toString() == runId }.getOrDefault(false)
        val unambiguousToolCall = toolCallId.all {
            it.isLetterOrDigit() || it == '-' || it == '_' || it == '.'
        }
        return if (canonicalRun && unambiguousToolCall && legacy.length <= MAX_MUTATION_ID_CHARS) {
            legacy
        } else {
            "tool-event-v2:" + CanonicalId.digest(
                domainVersion = "execution-tool-event-v2",
                fields = listOf(runId, toolCallId, phase, executionId),
            )
        }
    }

    private fun validateIdentityField(name: String, value: String) {
        require(value.isNotBlank()) { "$name is empty" }
        require(value.none(Char::isISOControl)) { "$name contains control characters" }
        require(value.encodeToByteArray().size <= MAX_CANONICAL_FIELD_BYTES) {
            "$name is too large"
        }
    }

    private const val MAX_EXECUTION_ID_CHARS = 480
    private const val MAX_IDEMPOTENCY_CHARS = 300
    private const val MAX_MUTATION_ID_CHARS = 500
    private const val MAX_CANONICAL_FIELD_BYTES = 4_096
}
