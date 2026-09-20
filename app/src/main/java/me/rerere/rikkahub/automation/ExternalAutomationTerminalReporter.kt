package me.rerere.rikkahub.automation

import java.util.concurrent.atomic.AtomicBoolean
import me.rerere.rikkahub.data.agentrun.AgentRunStatus

/** One terminal attempt per accepted dispatch; a broken ledger must not silence the caller. */
internal class ExternalAutomationTerminalReporter {
    private val claimed = AtomicBoolean(false)

    suspend fun reportOnce(
        status: String,
        ledgerStatus: AgentRunStatus,
        ledgerDetail: String?,
        callbackDetail: String?,
        persist: suspend (AgentRunStatus, String?) -> Unit,
        callback: (String, String?) -> Unit,
        onLedgerFailure: (Exception) -> Unit,
    ): Boolean {
        if (!claimed.compareAndSet(false, true)) return false
        try {
            persist(ledgerStatus, ledgerDetail)
        } catch (failure: Exception) {
            // A terminal command outcome is authoritative even if the diagnostic ledger fails.
            onLedgerFailure(failure)
        }
        callback(status, callbackDetail)
        return true
    }
}
