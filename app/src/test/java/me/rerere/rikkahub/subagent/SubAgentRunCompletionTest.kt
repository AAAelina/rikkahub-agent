package me.rerere.rikkahub.subagent

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import me.rerere.rikkahub.data.agentrun.AgentRunStatus
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class SubAgentRunCompletionTest {
    private fun run(id: String): SubAgentRun = SubAgentRun(
        id = id,
        parentChatId = "chat-1",
        parentAssistantId = "assistant-1",
        label = "child",
        task = "test",
        modelId = null,
        tools = null,
        runInBackground = true,
        timeoutSeconds = 60,
        maxTrips = 2,
        status = SubAgentStatus.PENDING,
        startedAtMs = 1L,
    )

    @Test fun `cancelled lazy child never enters body but closes its captured durable ledger`() = runBlocking {
        val registry = SubAgentRegistry()
        assertEquals(SubAgentRegistry.Reservation.RESERVED, registry.reservePending(run("lazy"), 1, 1))
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val completed = CompletableDeferred<Unit>()
        val terminalWrites = mutableListOf<Pair<String, AgentRunStatus>>()
        var bodyEntered = false
        try {
            val job = scope.launch(start = CoroutineStart.LAZY) { bodyEntered = true }
            job.invokeOnCompletion { cause ->
                scope.launch(NonCancellable + Dispatchers.Default) {
                    completeSubAgentJob(registry, "lazy", "ledger-lazy", cause) { ledgerId, status, _ ->
                        terminalWrites += ledgerId to status
                    }
                    completed.complete(Unit)
                }
            }
            registry.setJob("lazy", job)
            assertTrue(registry.requestCancel("lazy"))
            withTimeout(5_000L) { completed.await() }
            assertFalse(bodyEntered)
            assertEquals(SubAgentStatus.CANCELLED, registry.get("lazy")?.status)
            assertEquals(0, registry.globalActiveCount())
            assertEquals(listOf("ledger-lazy" to AgentRunStatus.cancelled), terminalWrites)
        } finally {
            scope.cancel()
        }
    }

    @Test fun `already succeeded child keeps success when completion callback also settles ledger`() = runBlocking {
        val registry = SubAgentRegistry()
        registry.reservePending(run("success"), 1, 1)
        registry.update("success") { it.copy(status = SubAgentStatus.SUCCEEDED, result = "done") }
        val writes = mutableListOf<AgentRunStatus>()
        completeSubAgentJob(registry, "success", "ledger-success", null) { _, status, _ -> writes += status }
        assertEquals(SubAgentStatus.SUCCEEDED, registry.get("success")?.status)
        assertEquals(0, registry.globalActiveCount())
        assertEquals(listOf(AgentRunStatus.succeeded), writes)
    }

    @Test fun `unexpected job completion terminalizes active registry and ledger`() = runBlocking {
        val registry = SubAgentRegistry()
        registry.reservePending(run("unexpected"), 1, 1)
        registry.update("unexpected") { it.copy(status = SubAgentStatus.RUNNING) }
        val writes = mutableListOf<AgentRunStatus>()
        completeSubAgentJob(registry, "unexpected", "ledger-failure", IllegalStateException("failed")) {
            _, status, _ -> writes += status
        }
        assertEquals(SubAgentStatus.FAILED, registry.get("unexpected")?.status)
        assertEquals(0, registry.globalActiveCount())
        assertEquals(listOf(AgentRunStatus.failed), writes)
    }

    @Test fun `prelaunch cancellation cannot be overwritten by a late normal completion`() = runBlocking {
        val registry = SubAgentRegistry()
        registry.reservePending(run("prelaunch"), 1, 1)
        assertTrue(registry.requestCancel("prelaunch"))
        val writes = mutableListOf<AgentRunStatus>()
        completeSubAgentJob(registry, "prelaunch", "ledger-prelaunch", null) { _, status, _ ->
            writes += status
        }
        assertEquals(SubAgentStatus.CANCELLED, registry.get("prelaunch")?.status)
        assertEquals(listOf(AgentRunStatus.cancelled), writes)
        assertEquals(0, registry.globalActiveCount())
    }

    @Test fun `prelaunch setup failure stays failed rather than becoming cancelled`() = runBlocking {
        val registry = SubAgentRegistry()
        registry.reservePending(run("setup-failure"), 1, 1)
        // Dispatch catch must record the original setup failure before attempting to
        // cancel a job, which can trigger a later CancellationException callback.
        registry.terminalizeIfActive("setup-failure", SubAgentStatus.FAILED, "dispatch_failed:IllegalStateException")
        val writes = mutableListOf<AgentRunStatus>()
        completeSubAgentJob(
            registry,
            "setup-failure",
            "ledger-setup-failure",
            CancellationException("job stopped after failed setup"),
        ) { _, status, _ -> writes += status }
        assertEquals(SubAgentStatus.FAILED, registry.get("setup-failure")?.status)
        assertEquals(listOf(AgentRunStatus.failed), writes)
        assertEquals(0, registry.globalActiveCount())
    }

    @Test fun `failed ledger write leaves registry terminal rather than ghost pending`() = runBlocking {
        val registry = SubAgentRegistry()
        registry.reservePending(run("failed-write"), 1, 1)
        val failure = runCatching {
            completeSubAgentJob(registry, "failed-write", "ledger-failed-write", CancellationException()) {
                _, _, _ -> error("injected durable ledger write failure")
            }
        }.exceptionOrNull()
        assertTrue(failure is IllegalStateException)
        assertEquals(SubAgentStatus.CANCELLED, registry.get("failed-write")?.status)
        assertEquals(0, registry.globalActiveCount())
    }
}
