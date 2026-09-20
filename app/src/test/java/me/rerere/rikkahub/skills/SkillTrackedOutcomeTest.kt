package me.rerere.rikkahub.skills

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.toList
import me.rerere.ai.ui.UIMessagePart
import me.rerere.rikkahub.data.ai.tools.HeadlessConversations
import me.rerere.rikkahub.data.model.Conversation
import me.rerere.rikkahub.service.chat.CommandOutcome
import org.junit.Assert.*
import org.junit.Test
import kotlin.uuid.Uuid
import java.nio.file.Files
import java.io.File
import java.net.URI

class SkillTrackedOutcomeTest {
    private class Driver(val terminal: CommandOutcome?, val quiet: Boolean = true) : SkillTestRunner.Driver {
        val events = mutableListOf<String>()
        val pending = CompletableDeferred<CommandOutcome>()
        val submitted = CompletableDeferred<Unit>()
        lateinit var conversationId: Uuid
        override suspend fun currentAssistantId() = Uuid.random()
        override suspend fun startConversation(conv: Conversation) { conversationId = conv.id; events += "start" }
        override suspend fun submit(conv: Conversation, parts: List<UIMessagePart>): SkillTestRunner.Submission {
            events += "submit"
            submitted.complete(Unit)
            terminal?.let { pending.complete(it) }
            return SkillTestRunner.Submission(Uuid.random(), pending)
        }
        override suspend fun stopAndAwaitQuiescence(conversationId: Uuid, commandId: Uuid?): Boolean {
            events += "stop"
            if (quiet) { pending.complete(CommandOutcome.Cancelled); events += "quiescent" }
            return quiet
        }
        override suspend fun harvest(conversationId: Uuid): SkillTestRunner.HarvestResult {
            events += "harvest"; return SkillTestRunner.HarvestResult("ok", emptyList())
        }
        override suspend fun cleanup(conv: Conversation) { events += "cleanup" }
    }

    @Test fun `only authoritative Completed harvests`() = runBlocking {
        val outcomes = listOf(CommandOutcome.Completed, CommandOutcome.Cancelled,
            CommandOutcome.Superseded(Uuid.random()), CommandOutcome.Rejected("denied"),
            CommandOutcome.Conflict("conflict"), CommandOutcome.NotApplied("not applied"),
            CommandOutcome.Failed(IllegalStateException("failed")), CommandOutcome.SkippedDependencyFailed(Uuid.random()))
        for (outcome in outcomes) {
            val driver = Driver(outcome)
            val states = SkillTestRunner(driver, { "skill" }, 20).runOnce("test", "prompt").toList()
            assertEquals(outcome.toString(), outcome == CommandOutcome.Completed, states.last() is SkillTestRunner.TestRunState.Done)
            assertEquals(outcome == CommandOutcome.Completed, "harvest" in driver.events)
            assertTrue(driver.events.indexOf("quiescent") < driver.events.indexOf("cleanup"))
        }
    }

    @Test fun `timeout stops before cleanup and never harvests`() = runBlocking {
        val driver = Driver(null)
        val states = SkillTestRunner(driver, { "skill" }, 10).runOnce("test", "prompt").toList()
        assertEquals("tester_timeout", (states.last() as SkillTestRunner.TestRunState.Error).error)
        assertFalse("harvest" in driver.events)
        assertTrue(driver.events.indexOf("stop") < driver.events.indexOf("cleanup"))
        assertTrue(driver.pending.isCompleted)
    }

    @Test fun `unconfirmed termination retains conversation`() = runBlocking {
        val driver = Driver(null, false)
        try {
            SkillTestRunner(driver, { "skill" }, 10).runOnce("test", "prompt").toList()
            assertFalse("cleanup" in driver.events)
            assertFalse("harvest" in driver.events)
            assertTrue(HeadlessConversations.isHeadless(driver.conversationId))
        } finally { HeadlessConversations.unmark(driver.conversationId) }
    }

    @Test fun `pending tracked outcome cannot harvest or clean up`() = runBlocking {
        val driver = Driver(null)
        val job = async { SkillTestRunner(driver, { "skill" }, 10_000).runOnce("test", "prompt").toList() }
        driver.submitted.await()
        assertFalse(job.isCompleted)
        assertFalse("harvest" in driver.events)
        assertFalse("cleanup" in driver.events)
        driver.pending.complete(CommandOutcome.Completed)
        assertTrue(job.await().last() is SkillTestRunner.TestRunState.Done)
        assertFalse(HeadlessConversations.isHeadless(driver.conversationId))
    }

    @Test fun `caller cancellation propagates after safe cleanup`() = runBlocking {
        val driver = Driver(null)
        val job = launch { SkillTestRunner(driver, { "skill" }, 10000).runOnce("test", "prompt").toList() }
        while ("submit" !in driver.events) yield()
        job.cancelAndJoin()
        assertTrue(job.isCancelled)
        assertFalse("harvest" in driver.events)
        assertTrue(driver.events.indexOf("quiescent") < driver.events.indexOf("cleanup"))
    }

    @Test fun `local result survives deletion of ephemeral source and remote is unchanged`() = runBlocking {
        val dir = Files.createTempDirectory("skill-owner").toFile()
        val source = File(dir,"source.png").apply { writeText("image") }
        val stable = File(dir,"managed.png")
        try {
            val urls = retainSkillResultImages(listOf(source.toURI().toString(), "https://example.test/image")) {
                File(URI(it)).copyTo(stable); stable.toURI().toString()
            }
            source.delete()
            assertEquals("image", File(URI(urls.first())).readText())
            assertEquals("https://example.test/image", urls.last())
        } finally { source.delete(); stable.delete(); dir.delete() }
    }
}
