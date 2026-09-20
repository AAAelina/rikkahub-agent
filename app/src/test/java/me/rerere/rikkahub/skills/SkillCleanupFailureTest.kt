package me.rerere.rikkahub.skills

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import me.rerere.ai.ui.UIMessagePart
import me.rerere.rikkahub.data.ai.tools.HeadlessConversations
import me.rerere.rikkahub.data.model.Conversation
import me.rerere.rikkahub.service.chat.CommandOutcome
import org.junit.Assert.*
import org.junit.Test
import java.io.IOException
import kotlin.uuid.Uuid

class SkillCleanupFailureTest {
    private class Driver(
        val failStart: Boolean = false,
        val failCleanup: Boolean = false,
        val complete: Boolean = true,
    ) : SkillTestRunner.Driver {
        lateinit var conversation: Conversation
        val submitted = CompletableDeferred<Unit>()
        val outcome = CompletableDeferred<CommandOutcome>()
        var cleanupWasMarked = false
        var quiet = true
        var hangCleanup = false
        var failHarvest = false
        var cleanupCalled = false
        override suspend fun currentAssistantId() = Uuid.random()
        override suspend fun startConversation(conv: Conversation) {
            conversation = conv // Simulates an insert succeeding before session initialization fails.
            if (failStart) throw IOException("session initialization failed")
        }
        override suspend fun submit(conv: Conversation, parts: List<UIMessagePart>): SkillTestRunner.Submission {
            submitted.complete(Unit)
            if (complete) outcome.complete(CommandOutcome.Completed)
            return SkillTestRunner.Submission(Uuid.random(), outcome)
        }
        override suspend fun stopAndAwaitQuiescence(conversationId: Uuid, commandId: Uuid?) = quiet
        override suspend fun harvest(conversationId: Uuid): SkillTestRunner.HarvestResult {
            if (failHarvest) throw IOException("artifact copy failed")
            return SkillTestRunner.HarvestResult("answer", emptyList())
        }
        override suspend fun cleanup(conv: Conversation) {
            cleanupCalled = true
            cleanupWasMarked = HeadlessConversations.isHeadless(conv.id)
            if (hangCleanup) awaitCancellation()
            if (failCleanup) throw IOException("database unavailable")
        }
    }

    @Test fun `cleanup failure retains recovery marker without replacing completed result`() = runBlocking {
        val driver = Driver(failCleanup = true)
        try {
            val states = SkillTestRunner(driver, { "body" }).runOnce("test", "prompt").toList()
            assertTrue(states.last() is SkillTestRunner.TestRunState.Done)
            assertTrue(driver.cleanupWasMarked)
            assertTrue(HeadlessConversations.isHeadless(driver.conversation.id))
        } finally { HeadlessConversations.unmark(driver.conversation.id) }
    }

    @Test fun `partial setup failure retains marker when quiescence is unknown`() = runBlocking {
        val driver = Driver(failStart = true).apply { quiet = false }
        try {
            val states = SkillTestRunner(driver, { "body" }).runOnce("test", "prompt").toList()
            assertTrue(states.last() is SkillTestRunner.TestRunState.Error)
            assertTrue(HeadlessConversations.isHeadless(driver.conversation.id))
        } finally { HeadlessConversations.unmark(driver.conversation.id) }
    }

    @Test fun `successful deletion happens before recovery marker removal`() = runBlocking {
        val driver = Driver()
        try {
            SkillTestRunner(driver, { "body" }).runOnce("test", "prompt").toList()
            assertTrue(driver.cleanupWasMarked)
            assertFalse(HeadlessConversations.isHeadless(driver.conversation.id))
        } finally { HeadlessConversations.unmark(driver.conversation.id) }
    }

    @Test fun `cleanup failure cannot replace caller cancellation`() = runBlocking {
        val driver = Driver(failCleanup = true, complete = false)
        try {
            val job = launch { SkillTestRunner(driver, { "body" }).runOnce("test", "prompt").toList() }
            driver.submitted.await()
            job.cancelAndJoin()
            assertTrue(job.isCancelled)
            assertTrue(HeadlessConversations.isHeadless(driver.conversation.id))
        } finally { HeadlessConversations.unmark(driver.conversation.id) }
    }

    @Test fun `failed artifact retention preserves ephemeral source for recovery`() = runBlocking {
        val driver = Driver().apply { failHarvest = true }
        try {
            val states = SkillTestRunner(driver, { "body" }).runOnce("test", "prompt").toList()
            assertTrue(states.last() is SkillTestRunner.TestRunState.Error)
            assertFalse(driver.cleanupCalled)
            assertTrue(HeadlessConversations.isHeadless(driver.conversation.id))
        } finally { HeadlessConversations.unmark(driver.conversation.id) }
    }

    @Test fun `unresponsive cleanup is bounded and keeps recovery marker`() = runBlocking {
        val driver = Driver().apply { hangCleanup = true }
        try {
            val states = withTimeout(2_000L) {
                SkillTestRunner(driver, { "body" }, cleanupTimeoutMs = 20L)
                    .runOnce("test", "prompt").toList()
            }
            assertTrue(states.last() is SkillTestRunner.TestRunState.Done)
            assertTrue(driver.cleanupCalled)
            assertTrue(HeadlessConversations.isHeadless(driver.conversation.id))
        } finally { HeadlessConversations.unmark(driver.conversation.id) }
    }
}
