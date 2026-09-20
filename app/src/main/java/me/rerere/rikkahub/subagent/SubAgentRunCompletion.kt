package me.rerere.rikkahub.subagent

import kotlinx.coroutines.CancellationException
import me.rerere.rikkahub.data.agentrun.AgentRunStatus

/**
 * Reconcile the registry and the durable run ledger when a child Job has *actually* finished.
 *
 * A lazy Job can be cancelled before its body (including executeRun's finally) ever runs.
 * The Engine's completion callback therefore calls this with the ledger id captured at open,
 * rather than relying on executeRun or on the in-memory ledgerIds side table to close it.
 * The repository's terminal transition is idempotent; an ordinary completed child may also
 * have written the same terminal status from its body.
 */
internal suspend fun completeSubAgentJob(
    registry: SubAgentRegistry,
    runId: String,
    ledgerId: String,
    cause: Throwable?,
    persistTerminal: suspend (ledgerId: String, status: AgentRunStatus, error: String?) -> Unit,
) {
    registry.terminalizeIfActive(
        id = runId,
        status = if (cause is CancellationException) SubAgentStatus.CANCELLED else SubAgentStatus.FAILED,
        error = if (cause is CancellationException) "cancelled_before_terminal" else "execution_ended_without_terminal",
    )

    val settled = registry.get(runId) ?: return
    val ledgerStatus = when (settled.status) {
        SubAgentStatus.SUCCEEDED -> AgentRunStatus.succeeded
        SubAgentStatus.CANCELLED -> AgentRunStatus.cancelled
        SubAgentStatus.FAILED, SubAgentStatus.TIMED_OUT -> AgentRunStatus.failed
        SubAgentStatus.PENDING, SubAgentStatus.RUNNING -> return
    }
    persistTerminal(ledgerId, ledgerStatus, settled.error)
}
