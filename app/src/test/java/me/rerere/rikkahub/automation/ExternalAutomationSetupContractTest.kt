package me.rerere.rikkahub.automation

import java.nio.file.Files
import java.nio.file.Paths
import org.junit.Assert.assertTrue
import org.junit.Test

/** Source-level RED gate: accepted must have a terminal path even if any setup stage fails. */
class ExternalAutomationSetupContractTest {
    private val source by lazy {
        var root = Paths.get(System.getProperty("user.dir")).toAbsolutePath()
        while (!Files.isDirectory(root.resolve("app/src/main/java"))) {
            root = requireNotNull(root.parent)
        }
        Files.readString(root.resolve(
            "app/src/main/java/me/rerere/rikkahub/automation/ExternalAutomationDispatcher.kt",
        ))
    }

    @Test fun everyPotentiallyThrowingSetupStageIsInsideTerminalCatch() {
        val start = source.indexOf("private suspend fun runHeadless(")
        val end = source.indexOf("private fun sendCallback(", start)
        assertTrue(start >= 0 && end > start)
        val body = source.substring(start, end)
        val firstTry = body.indexOf("try {")
        assertTrue("setup must be protected by try/catch", firstTry >= 0)
        for (stage in listOf(
            "settingsStore.settingsFlow.first()",
            "conversationRepo.insertConversation(",
            "chatService.initializeConversation(",
            "HeadlessConversations.mark(",
            "agentRunRepo.open(",
        )) {
            val position = body.indexOf(stage)
            assertTrue("Stage missing: $stage", position >= 0)
            assertTrue("$stage occurs before the terminal catch", firstTry < position)
        }
        assertTrue("initialization failure must report a failed authoritative terminal",
            body.substring(body.indexOf("catch (t: Exception)")).contains(
                "reportTerminalOnce(\"failed\", AgentRunStatus.failed",
            ))
        assertTrue("terminal reporter must use the actual callback surface",
            body.contains("sendCallback(returnAction, returnPackage, requestId, phase, detail)"))
    }
}
