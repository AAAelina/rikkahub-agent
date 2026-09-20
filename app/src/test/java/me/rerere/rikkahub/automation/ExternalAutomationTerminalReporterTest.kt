package me.rerere.rikkahub.automation

import kotlinx.coroutines.runBlocking
import me.rerere.rikkahub.data.agentrun.AgentRunStatus
import org.junit.Assert.*
import org.junit.Test

class ExternalAutomationTerminalReporterTest {
    @Test fun ledgerFailureDoesNotDropOrRewriteCompletedCallback() = runBlocking {
        val reporter = ExternalAutomationTerminalReporter()
        val callbacks = mutableListOf<String>()
        val failures = mutableListOf<String>()
        assertTrue(reporter.reportOnce(
            status = "completed", ledgerStatus = AgentRunStatus.succeeded,
            ledgerDetail = null, callbackDetail = null,
            persist = { status, _ ->
                assertEquals(AgentRunStatus.succeeded, status)
                throw IllegalStateException("injected ledger write failure")
            },
            callback = { status, _ -> callbacks += status },
            onLedgerFailure = { failures += it.message.orEmpty() },
        ))
        assertFalse(reporter.reportOnce("failed", AgentRunStatus.failed, "replay", "replay",
            persist = { fail("duplicate ledger write") },
            callback = { status, _ -> callbacks += status },
            onLedgerFailure = { fail("duplicate error") },
        ))
        assertEquals(listOf("completed"), callbacks)
        assertEquals(listOf("injected ledger write failure"), failures)
    }

    @Test fun missingLedgerStillEmitsOneFailedTerminal() = runBlocking {
        val reporter = ExternalAutomationTerminalReporter()
        val callbacks = mutableListOf<Pair<String, String?>>()
        assertTrue(reporter.reportOnce("failed", AgentRunStatus.failed, "setup failed", "setup failed",
            persist = { _, _ -> }, callback = { status, detail -> callbacks += status to detail },
            onLedgerFailure = { fail("no ledger allocated") },
        ))
        assertEquals(listOf("failed" to "setup failed"), callbacks)
    }
}
