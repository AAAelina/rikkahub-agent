package me.rerere.rikkahub.data.execution

import me.rerere.rikkahub.data.ai.tools.ToolExecutionContext
import me.rerere.rikkahub.data.ai.execution.ExecutionTrackingHealth
import me.rerere.rikkahub.data.capability.SubjectType
import me.rerere.rikkahub.data.scope.AgentScope
import kotlin.uuid.Uuid

data class ManagedExecutionReservation(
    val executionId: String,
    val runtime: ExecutionRuntime,
    val completionPolicy: CompletionPolicy,
    val runtimeInstanceMarker: String? = null,
)

class ManagedExecutionRegistrationException(
    val code: String,
    cause: Throwable? = null,
) : IllegalStateException(code, cause)

/** Registers a managed child before native start and owns all later child-state mutations. */
class ManagedExecutionRegistration(
    private val repository: ExecutionRepository,
    private val trackingHealth: ExecutionTrackingHealth? = null,
) {
    suspend fun reserve(
        context: ToolExecutionContext,
        reservation: ManagedExecutionReservation,
    ): ExecutionRecord {
        if (context.toolCallId.isBlank()) {
            throw ManagedExecutionRegistrationException("execution_tracking_unavailable")
        }
        return runCatching {
            val parentId = ExecutionRecordIds.tool(context.runId.toString(), context.toolCallId)
            val capability = when (reservation.runtime) {
                ExecutionRuntime.SSH -> "ssh.execute"
                ExecutionRuntime.TERMUX, ExecutionRuntime.WORKSPACE -> "linux.background"
                else -> "runtime.manage"
            }
            repository.open(
                draft = ExecutionRecordDraft(
                    id = reservation.executionId,
                    traceId = context.runId.toString(),
                    parentExecutionId = parentId,
                    commandId = context.commandId?.toString(),
                    conversationId = context.conversationId.toString(),
                    learningScope = if (context.capabilitySubject?.type == SubjectType.LOCAL_SECOND_USER) {
                        AgentScope.AuthoritySubject(context.capabilitySubject.id)
                    } else {
                        AgentScope.Assistant(Uuid.parse(context.assistantId))
                    },
                    subjectId = context.capabilitySubject?.id ?: context.assistantId,
                    subjectType = context.capabilitySubject?.type?.name ?: "LOCAL_ASSISTANT",
                    origin = context.callOrigin.name,
                    capabilityKeys = capability,
                    resourceSummary = reservation.runtime.name.lowercase(),
                    runtime = reservation.runtime,
                    idempotencyKey = "managed:${reservation.executionId}".take(300),
                    initialStatus = ExecutionStatus.starting,
                    executionKind = ExecutionKind.MANAGED_PROCESS,
                    completionPolicy = reservation.completionPolicy,
                    verificationState = VerificationState.DATABASE_CONFIRMED,
                    runtimeHandleSummary = reservation.executionId,
                    runtimeInstanceMarker = reservation.runtimeInstanceMarker,
                ),
                mutationId = "managed-reserve:${reservation.executionId}",
                source = ExecutionStateSource.DATABASE,
                reasonCode = "managed_process_reserved",
            ).also { record ->
                check(ExecutionStatus.fromWire(record.status) in setOf(
                    ExecutionStatus.starting,
                    ExecutionStatus.running,
                )) { "managed_execution_already_terminal" }
            }
        }.getOrElse { failure ->
            trackingHealth?.markDegraded("execution_tracking_unavailable")
            throw ManagedExecutionRegistrationException("execution_tracking_unavailable", failure)
        }
    }

    suspend fun running(
        executionId: String,
        runtimeInstanceMarker: String?,
        restarted: Boolean = false,
    ) {
        requireApplied(
            repository.transition(
                id = executionId,
                target = ExecutionStatus.running,
                runtimeHandleSummary = executionId,
                runtimeInstanceMarker = runtimeInstanceMarker,
                verificationState = VerificationState.RUNTIME_CONFIRMED,
                mutationId = if (restarted) {
                    "workspace-restarted:$executionId:${runtimeInstanceMarker.orEmpty()}"
                } else {
                    "managed-running:$executionId:${runtimeInstanceMarker.orEmpty()}"
                },
                source = ExecutionStateSource.LIVE_EVENT,
                reasonCode = if (restarted) "workspace_process_restarted" else "managed_process_running",
            ),
            expected = ExecutionStatus.running,
        )
    }

    suspend fun failed(executionId: String, reasonCode: String) {
        requireApplied(
            repository.transition(
                id = executionId,
                target = ExecutionStatus.failed,
                verificationState = VerificationState.LIVE_CONFIRMED,
                mutationId = "managed-failed:$executionId:${reasonCode.take(120)}",
                source = ExecutionStateSource.LIVE_EVENT,
                reasonCode = reasonCode.take(160),
                detail = reasonCode.take(160),
            ),
            expected = ExecutionStatus.failed,
        )
    }

    suspend fun cancelRequested(
        executionId: String,
        requestedOutcome: RequestedTerminalOutcome = RequestedTerminalOutcome.CANCELLED,
    ) {
        requireApplied(
            repository.transition(
                id = executionId,
                target = ExecutionStatus.cancel_requested,
                verificationState = VerificationState.DATABASE_CONFIRMED,
                mutationId = "managed-cancel-requested:$executionId",
                source = ExecutionStateSource.USER,
                reasonCode = "managed_cancel_requested",
                requestedTerminalOutcome = requestedOutcome,
            ),
            expected = ExecutionStatus.cancel_requested,
        )
    }

    suspend fun cancellationProbed(executionId: String, stopped: Boolean) {
        val current = repository.get(executionId) ?: failTracking("managed_execution_missing")
        val requested = RequestedTerminalOutcome.fromWire(current.requestedTerminalOutcome)
        val target = if (!stopped) {
            ExecutionStatus.terminating
        } else if (requested == RequestedTerminalOutcome.TIMED_OUT) {
            ExecutionStatus.timed_out
        } else {
            ExecutionStatus.cancelled
        }
        requireApplied(
            repository.transition(
                id = executionId,
                target = target,
                verificationState = if (stopped) {
                    VerificationState.RUNTIME_CONFIRMED
                } else {
                    VerificationState.STALE
                },
                mutationId = "managed-cancel-probe:$executionId:$stopped",
                source = ExecutionStateSource.PROBE,
                reasonCode = if (stopped) "managed_stop_confirmed" else "managed_stop_unconfirmed",
                cancellationResult = if (stopped) "STOPPED_CONFIRMED" else "STOP_UNCONFIRMED",
                probeAtMs = System.currentTimeMillis(),
                requestedTerminalOutcome = requested,
            ),
            expected = target,
        )
    }

    suspend fun exited(
        executionId: String,
        succeeded: Boolean,
        reasonCode: String,
    ) {
        val target = if (succeeded) ExecutionStatus.succeeded else ExecutionStatus.failed
        requireApplied(
            repository.transition(
                id = executionId,
                target = target,
                verificationState = VerificationState.RUNTIME_CONFIRMED,
                mutationId = "managed-exit:$executionId:${reasonCode.take(120)}",
                source = ExecutionStateSource.LIVE_EVENT,
                reasonCode = reasonCode.take(160),
                detail = reasonCode.take(160),
            ),
            expected = target,
        )
    }

    private fun requireApplied(result: ExecutionTransitionResult, expected: ExecutionStatus) {
        when (result) {
            is ExecutionTransitionResult.Applied -> trackingHealth?.markRecovered()
            is ExecutionTransitionResult.Terminal -> if (
                ExecutionStatus.fromWire(result.record.status) == expected
            ) {
                trackingHealth?.markRecovered()
            } else {
                failTracking("managed_execution_terminal_conflict")
            }
            is ExecutionTransitionResult.Missing -> failTracking("managed_execution_missing")
            is ExecutionTransitionResult.Invalid -> failTracking("managed_execution_transition_invalid")
            is ExecutionTransitionResult.Conflict -> failTracking("managed_execution_cas_conflict")
        }
    }

    private fun failTracking(reasonCode: String): Nothing {
        trackingHealth?.markDegraded(reasonCode)
        error(reasonCode)
    }
}
