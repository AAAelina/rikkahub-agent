package me.rerere.rikkahub.learning.model

import kotlin.uuid.Uuid
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class LearningCanonicalIdTest {
    private val streamId = Uuid.parse("10000000-0000-0000-0000-000000000001")

    @Test
    fun `canonical digest is deterministic and length prefixes prevent boundary ambiguity`() {
        val first = LearningCanonicalId.digest("learning-test-v1", listOf("a", "bc"))
        val repeated = List(100) {
            LearningCanonicalId.digest("learning-test-v1", listOf("a", "bc"))
        }
        val differentBoundary = LearningCanonicalId.digest("learning-test-v1", listOf("ab", "c"))

        assertTrue(repeated.all { it == first })
        assertNotEquals(first, differentBoundary)
        assertEquals(64, first.length)
        assertTrue(first.matches(Regex("[0-9a-f]{64}")))
    }

    @Test
    fun `event identity changes for every authoritative identity component`() {
        val baseline = eventId()

        assertEquals(baseline, eventId())
        assertNotEquals(baseline, eventId(sourceId = "command-2"))
        assertNotEquals(baseline, eventId(sourceRevision = 8L))
        assertNotEquals(baseline, eventId(terminalState = "FAILED"))
        assertNotEquals(baseline, eventId(eventSchemaVersion = 2))
        assertNotEquals(
            baseline,
            LearningCanonicalId.eventId(
                streamId = Uuid.parse("20000000-0000-0000-0000-000000000001"),
                eventType = LearningEventType.COMMAND_TERMINAL,
                eventSchemaVersion = 1,
                sourceKindCode = LearningSourceKind.COMMAND.name,
                sourceId = "command-1",
                sourceRevision = 7L,
                terminalState = "COMPLETED",
            ),
        )
    }

    @Test
    fun `canonical identity rejects unbounded or ambiguous fields`() {
        assertFails { LearningCanonicalId.digest("bad domain", emptyList()) }
        assertFails {
            LearningCanonicalId.digest(
                "learning-test-v1",
                List(65) { it.toString() },
            )
        }
        assertFails {
            LearningCanonicalId.digest("learning-test-v1", listOf("x".repeat(4_097)))
        }
        assertFails { eventId(sourceId = "bad\nid") }
        assertFails { eventId(terminalState = "completed") }
    }

    @Test
    fun `scoped source invalidation collision identity separates learning scopes`() {
        val legacy = LearningCanonicalId.eventId(
            streamId = streamId,
            eventType = LearningEventType.SOURCE_INVALIDATED,
            eventSchemaVersion = 2,
            sourceKindCode = LearningSourceKind.CONVERSATION_MESSAGE.name,
            sourceId = "message-1",
            sourceRevision = 2L,
            terminalState = null,
            previousSourceRevision = 1L,
            sourceStateCode = "SUPERSEDED",
            correlation = LearningCorrelation(
                conversationId = "conversation-1",
                conversationSourceRevision = 2L,
                messageId = "message-1",
                messageRevision = 2L,
            ),
        )
        val assistant = LearningCanonicalId.scopedSourceInvalidationEventId(
            legacy,
            LearningScopeKind.ASSISTANT.name,
            "10000000-0000-0000-0000-000000000001",
        )
        val subject = LearningCanonicalId.scopedSourceInvalidationEventId(
            legacy,
            LearningScopeKind.AUTHORITY_SUBJECT.name,
            "local-second-user:v1:test",
        )

        assertNotEquals(legacy, assistant)
        assertNotEquals(assistant, subject)
        assertEquals(assistant, LearningCanonicalId.scopedSourceInvalidationEventId(
            legacy,
            LearningScopeKind.ASSISTANT.name,
            "10000000-0000-0000-0000-000000000001",
        ))
    }

    private fun eventId(
        sourceId: String = "command-1",
        sourceRevision: Long? = 7L,
        terminalState: String = "COMPLETED",
        eventSchemaVersion: Int = 1,
    ): String = LearningCanonicalId.eventId(
        streamId = streamId,
        eventType = LearningEventType.COMMAND_TERMINAL,
        eventSchemaVersion = eventSchemaVersion,
        sourceKindCode = LearningSourceKind.COMMAND.name,
        sourceId = sourceId,
        sourceRevision = sourceRevision,
        terminalState = terminalState,
    )

    private fun assertFails(block: () -> Unit) {
        var failed = false
        try {
            block()
        } catch (_: IllegalArgumentException) {
            failed = true
        }
        assertTrue("Expected IllegalArgumentException", failed)
    }
}
