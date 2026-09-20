package me.rerere.rikkahub.data.repository

import kotlinx.coroutines.*
import org.junit.Assert.*
import org.junit.Test

class ConversationPostCommitProjectionTest {
    @Test fun `projection failure is diagnostic only`() = runBlocking {
        val failure = IllegalStateException("FTS unavailable")
        var reported: Exception? = null
        runConversationPostCommitProjection({ throw failure }, { reported = it })
        assertSame(failure, reported)
    }

    @Test fun `projection cancellation is propagated to its owning job`() = runBlocking {
        val cancelled = CancellationException("projection cancelled")
        var caught: CancellationException? = null
        try { runConversationPostCommitProjection({ throw cancelled }, { fail("not diagnostic") }) }
        catch (e: CancellationException) { caught = e }
        assertSame(cancelled, caught)
    }

    @Test fun `diagnostic failure does not fail committed write`() = runBlocking {
        runConversationPostCommitProjection({ error("index") }, { error("logger") })
    }

    @Test fun `cancelled caller cannot cancel application owned projection`() = runBlocking {
        val owner = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val queue = ConversationPostCommitProjection(owner)
            val started = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val done = CompletableDeferred<Unit>()
            val caller = launch {
                queue.enqueue({ started.complete(Unit); release.await(); done.complete(Unit) }, { throw it })
                awaitCancellation()
            }
            started.await()
            caller.cancelAndJoin()
            assertTrue(caller.isCancelled)
            release.complete(Unit)
            withTimeout(2000) { done.await() }
        } finally { owner.cancel() }
    }

    @Test fun `one cancelled projection cannot cancel the owner or next work`() = runBlocking {
        val owner = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val queue = ConversationPostCommitProjection(owner)
            val first = queue.enqueue({ throw CancellationException("index") }, { fail() })
            first.join()
            assertTrue(first.isCancelled)
            val done = CompletableDeferred<Unit>()
            queue.enqueue({ done.complete(Unit) }, { fail() }).join()
            assertTrue(done.isCompleted)
            assertTrue(owner.isActive)
        } finally { owner.cancel() }
    }
}
