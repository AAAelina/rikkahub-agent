package me.rerere.rikkahub.data.execution

import me.rerere.rikkahub.data.scope.AgentScope
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.uuid.Uuid

class ExecutionIdentityContractTest {
    private val assistantId = Uuid.parse("00000000-0000-0000-0000-000000000001")

    @Test
    fun `reopened execution must preserve every frozen admission identity`() {
        val draft = draft()
        val record = draft.toRecord(nowMs = 1L)

        assertTrue(record.hasSameAdmissionIdentityAs(draft))
        assertTrue(
            record.copy(
                status = ExecutionStatus.running.name,
                runtime = ExecutionRuntime.TERMUX.name,
                stateVersion = 8L,
            ).hasSameAdmissionIdentityAs(draft),
        )
        assertFalse(record.hasSameAdmissionIdentityAs(draft.copy(commandId = "other-command")))
        assertFalse(record.hasSameAdmissionIdentityAs(draft.copy(subjectId = "other-subject")))
        assertFalse(
            record.hasSameAdmissionIdentityAs(
                draft.copy(learningScope = AgentScope.AuthoritySubject("other-subject")),
            ),
        )
        assertFalse(record.hasSameAdmissionIdentityAs(draft.copy(capabilityKeys = "tool.other")))
        assertFalse(
            record.hasSameAdmissionIdentityAs(
                draft.copy(executionKind = ExecutionKind.MANAGED_PROCESS),
            ),
        )
        assertFalse(
            record.hasSameAdmissionIdentityAs(
                draft.copy(completionPolicy = CompletionPolicy.DETACH_BACKGROUND),
            ),
        )
    }

    @Test
    fun `duplicate event must match its complete journal identity`() {
        val mutation = ExecutionMutation(
            executionId = "execution-1",
            mutationId = "mutation-1",
            expectedVersion = 6L,
            source = ExecutionStateSource.LIVE_EVENT,
            reasonCode = "tool_completed",
            targetStatus = ExecutionStatus.succeeded,
            verificationState = VerificationState.LIVE_CONFIRMED,
        )
        val event = ExecutionEventRecord(
            eventId = mutation.mutationId,
            executionId = mutation.executionId,
            sequence = 7L,
            previousStatus = ExecutionStatus.running.name,
            nextStatus = ExecutionStatus.succeeded.name,
            previousVerification = VerificationState.UNKNOWN.name,
            nextVerification = VerificationState.LIVE_CONFIRMED.name,
            source = ExecutionStateSource.LIVE_EVENT.name,
            reasonCode = "tool_completed",
            createdAtMs = 10L,
        )

        assertTrue(event.hasSameJournalIdentityAs(mutation, currentVersion = 7L))
        assertTrue(
            event.hasSameJournalIdentityAs(
                mutation.copy(expectedVersion = 7L),
                currentVersion = 7L,
            ),
        )
        assertFalse(
            event.hasSameJournalIdentityAs(
                mutation.copy(executionId = "execution-2"),
                currentVersion = 7L,
            ),
        )
        assertFalse(
            event.hasSameJournalIdentityAs(
                mutation.copy(expectedVersion = 8L),
                currentVersion = 7L,
            ),
        )
        assertFalse(
            event.hasSameJournalIdentityAs(
                mutation.copy(source = ExecutionStateSource.RECOVERY),
                currentVersion = 7L,
            ),
        )
        assertFalse(
            event.hasSameJournalIdentityAs(
                mutation.copy(reasonCode = "different"),
                currentVersion = 7L,
            ),
        )
        assertFalse(
            event.hasSameJournalIdentityAs(
                mutation.copy(targetStatus = ExecutionStatus.failed),
                currentVersion = 7L,
            ),
        )
        assertFalse(
            event.hasSameJournalIdentityAs(
                mutation.copy(verificationState = VerificationState.STALE),
                currentVersion = 7L,
            ),
        )
    }

    @Test
    fun `tool identities never collide by truncation or ambiguous separators`() {
        val canonicalRun = "00000000-0000-0000-0000-000000000010"
        assertEquals(
            "tool:$canonicalRun:call-1",
            ExecutionRecordIds.tool(canonicalRun, "call-1"),
        )

        val ambiguousLeft = ExecutionRecordIds.tool("a:b", "c")
        val ambiguousRight = ExecutionRecordIds.tool("a", "b:c")
        assertNotEquals(ambiguousLeft, ambiguousRight)
        assertTrue(ambiguousLeft.startsWith("tool-v2:"))

        val sharedPrefix = "x".repeat(479)
        val longLeft = ExecutionRecordIds.tool(canonicalRun, sharedPrefix + "a")
        val longRight = ExecutionRecordIds.tool(canonicalRun, sharedPrefix + "b")
        assertNotEquals(longLeft, longRight)
        assertTrue(longLeft.length <= 480)
        assertNotEquals(
            ExecutionRecordIds.toolIdempotency(canonicalRun, sharedPrefix + "a"),
            ExecutionRecordIds.toolIdempotency(canonicalRun, sharedPrefix + "b"),
        )
        assertNotEquals(
            ExecutionRecordIds.toolEvent("a:b", "c", "RUNNING", "handle"),
            ExecutionRecordIds.toolEvent("a", "b:c", "RUNNING", "handle"),
        )
    }

    @Test
    fun `tool identity rejects empty control and unbounded fields`() {
        assertThrows(IllegalArgumentException::class.java) {
            ExecutionRecordIds.tool("", "call")
        }
        assertThrows(IllegalArgumentException::class.java) {
            ExecutionRecordIds.tool("run", "call\nunsafe")
        }
        assertThrows(IllegalArgumentException::class.java) {
            ExecutionRecordIds.tool("run", "x".repeat(4_097))
        }
    }

    private fun draft() = ExecutionRecordDraft(
        id = "execution-1",
        traceId = "run-1",
        parentExecutionId = "parent-1",
        commandId = "command-1",
        conversationId = "conversation-1",
        learningScope = AgentScope.Assistant(assistantId),
        subjectId = "subject-1",
        subjectType = "LOCAL_ASSISTANT",
        origin = "LocalChat",
        capabilityKeys = "tool.run",
        resourceSummary = "tool",
        runtime = ExecutionRuntime.LOCAL_TOOL,
        idempotencyKey = "idempotency-1",
        initialStatus = ExecutionStatus.starting,
        verificationState = VerificationState.UNKNOWN,
    )
}
