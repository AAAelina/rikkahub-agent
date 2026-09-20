package me.rerere.rikkahub.data.repository

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertSame
import org.junit.Test

class ConversationPostCommitProjectionTest {
    @Test
    fun `failed FTS cannot enter the caller's failed-write recovery`() = runBlocking {
        val failure = IllegalStateException("FTS unavailable")
        var durableGraph = "committed new graph"
        var reported: Exception? = null
        try {
            runConversationPostCommitProjection(
                project = { throw failure },
                onFailure = { reported = it },
            )
        } catch (_: Exception) {
            durableGraph = "restored old graph"
        }
        assertEquals("committed new graph", durableGraph)
        assertSame(failure, reported)
    }

    @Test
    fun `cancelled projection and failing diagnostics cannot undo successful commit`() = runBlocking {
        var result = "committed"
        try {
            runConversationPostCommitProjection(
                project = { throw CancellationException("index cancelled after commit") },
                onFailure = { throw IllegalStateException("diagnostics unavailable") },
            )
        } catch (_: Exception) {
            result = "failed"
        }
        assertEquals("committed", result)
    }
}
