package me.rerere.rikkahub.automation

import kotlinx.coroutines.*
import me.rerere.rikkahub.service.chat.*
import me.rerere.rikkahub.data.agentrun.AgentRunStatus
import org.junit.Assert.*
import org.junit.Test
import kotlin.uuid.Uuid

class ExternalTrackedOutcomeTest {
    @Test fun `timeout requests stop before grace and preserves authoritative cancellation`() = runBlocking {
        val outcome = CompletableDeferred<CommandOutcome>()
        var stopped = false
        val result = awaitExternalCommandOutcome(outcome, 10, 20) {
            stopped = true
            outcome.complete(CommandOutcome.Cancelled)
        }
        assertTrue(stopped)
        assertEquals(CommandOutcome.Cancelled, result)
    }
    @Test fun `unconfirmed timeout is not manufactured into success or cancellation`() = runBlocking {
        val outcome = CompletableDeferred<CommandOutcome>()
        var stopped = false
        assertNull(awaitExternalCommandOutcome(outcome, 10, 10) { stopped = true })
        assertTrue(stopped)
        assertFalse(outcome.isCompleted)
    }
    @Test fun `terminal mapping exhaustively follows command authority`() {
        assertEquals("completed" to AgentRunStatus.succeeded, CommandOutcome.Completed.externalTerminal())
        listOf(CommandOutcome.Cancelled, CommandOutcome.Superseded(Uuid.random())).forEach {
            assertEquals("cancelled" to AgentRunStatus.cancelled, it.externalTerminal())
        }
        assertEquals("rejected" to AgentRunStatus.failed, CommandOutcome.Rejected("blocked").externalTerminal())
        listOf(CommandOutcome.Conflict("blocked"), CommandOutcome.NotApplied("blocked"),
            CommandOutcome.Failed(Exception("blocked")), CommandOutcome.SkippedDependencyFailed(Uuid.random())).forEach {
            assertEquals("failed" to AgentRunStatus.failed, it.externalTerminal())
        }
    }
    @Test fun `external origin survives durable codec`() {
        val (_, payload) = CommandCodec.encodeDurable(StopCommand(), CommandOrigin.EXTERNAL_AUTOMATION)
        assertEquals(CommandOrigin.EXTERNAL_AUTOMATION, CommandCodec.decodeDurableOrigin(payload))
    }
    @Test fun `message mutation identity survives durable codec`() {
        val command = MutateMessageCommand(Uuid.random(), Uuid.random())
        val (type, payload) = CommandCodec.encodeDurable(command, CommandOrigin.APP_UI)
        assertEquals(command, CommandCodec.decode(type, payload))
    }
}
