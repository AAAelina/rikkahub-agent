package me.rerere.rikkahub.workflow.execution

import java.io.File
import me.rerere.rikkahub.data.agentrun.AgentRunFailureCode
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class WorkflowRuntimePrivacyContractTest {
    @Test
    fun `durable workflow and agent run reasons collapse unknown runtime content`() {
        assertEquals(
            WorkflowFailureCode.ACTION_RUNTIME_FAILURE,
            WorkflowFailureCode.durableOrGeneric("secret tool output and exception detail"),
        )
        assertEquals(
            AgentRunFailureCode.RUNTIME_FAILURE,
            AgentRunFailureCode.sanitize("IllegalStateException: token=secret"),
        )
        assertEquals(
            WorkflowFailureCode.ACTION_TIMEOUT,
            WorkflowFailureCode.durableOrGeneric(WorkflowFailureCode.ACTION_TIMEOUT),
        )
    }

    @Test
    fun `workflow runner never builds durable detail or output summaries`() {
        val source = projectFile(
            "src/main/java/me/rerere/rikkahub/workflow/execution/WorkflowEngine.kt",
        ).readText()
        listOf(
            "t.message",
            "runtimeResult.detail",
            "Completed).output",
            "outputs.joinToString",
            "hardlineReason\"",
        ).forEach { forbidden ->
            assertFalse("runtime content must not enter workflow result: $forbidden", source.contains(forbidden))
        }
        assertTrue(source.contains("WorkflowFailureCode.ACTION_RUNTIME_FAILURE"))
        assertTrue(source.contains("WorkflowFailureCode.ACTIONS_COMPLETED"))
    }

    @Test
    fun `learned row fails closed before tool execution when no authority validator exists`() {
        val engine = projectFile(
            "src/main/java/me/rerere/rikkahub/workflow/execution/WorkflowEngine.kt",
        ).readText()

        // Learning was removed, so no validator is registered and every learned row must be
        // disabled as stale before any tool is resolved.
        assertTrue(engine.contains("getOrNull<LearnedWorkflowAuthorityValidator>()"))
        val authorityCheck = engine.indexOf("learnedAuthorityValidator?.isActive(authority) == true")
        val disable = engine.indexOf("repository.disableLearnedAsStale", authorityCheck)
        val toolResolution = engine.indexOf("val settings = settingsStore.settingsFlow.first()", disable)
        assertTrue(authorityCheck >= 0 && disable > authorityCheck && toolResolution > disable)
    }

    private fun projectFile(relative: String): File {
        var current = File(System.getProperty("user.dir")).absoluteFile
        repeat(5) {
            val direct = File(current, relative)
            if (direct.isFile) return direct
            val underApp = File(current, "app/$relative")
            if (underApp.isFile) return underApp
            current = current.parentFile ?: return@repeat
        }
        error("project file not found: $relative")
    }
}
