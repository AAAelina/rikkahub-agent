package me.rerere.rikkahub.service

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import me.rerere.rikkahub.data.model.Conversation
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.uuid.Uuid

class ConversationSessionTest {
    @Test
    fun `partial mutation of placeholder does not masquerade as durable hydration`() {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val id = Uuid.random()
        val session = ConversationSession(id, Conversation.ofId(id), scope, {})
        session.updateState { it.copy(title = "placeholder") }
        assertFalse(session.isHydrated)
        val stored = Conversation.ofId(id).copy(title = "durable")
        assertTrue(session.hydrateIfNeeded(stored))
        assertEquals("durable", session.state.value.title)
        scope.coroutineContext[Job]?.cancel()
    }

    @Test
    fun `late hydration cannot overwrite live conversation state`() {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val conversationId = Uuid.random()
        val stored = Conversation.ofId(conversationId, Uuid.random()).copy(title = "stored")
        val session = ConversationSession(
            id = conversationId,
            initial = Conversation.ofId(conversationId, Uuid.random()),
            scope = scope,
            onIdle = {},
        )

        assertTrue(session.hydrateIfNeeded(stored))
        session.replaceState(stored.copy(title = "live streaming state"))

        assertFalse(session.hydrateIfNeeded(stored.copy(title = "stale database state")))
        assertEquals("live streaming state", session.state.value.title)

        session.cleanup()
        scope.coroutineContext[Job]?.cancel()
    }

    @Test
    fun `attaching a new run does not cancel the previous run`() {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val session = ConversationSession(
            id = Uuid.random(),
            initial = Conversation.ofId(Uuid.random(), Uuid.random()),
            scope = scope,
            onIdle = {},
        )
        val first = scope.launch { delay(10_000) }
        val second = scope.launch { delay(10_000) }

        session.attachRunJob(first)
        session.attachRunJob(second)

        assertTrue(first.isActive)
        assertTrue(second.isActive)

        session.cleanup()
        scope.coroutineContext[Job]?.cancel()
    }

    @Test
    fun `idle eviction waits for runtime work and can be requested again`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        var runtimeBusy = true
        var idleCalls = 0
        val session = ConversationSession(
            id = Uuid.random(),
            initial = Conversation.ofId(Uuid.random(), Uuid.random()),
            scope = scope,
            onIdle = { idleCalls++ },
            canEvict = { !runtimeBusy },
            idleTimeoutMs = 20,
        )

        val completedRun = scope.launch { delay(1) }
        session.attachRunJob(completedRun)
        completedRun.join()
        delay(60)
        assertEquals(0, idleCalls)

        runtimeBusy = false
        session.requestIdleCheck()
        delay(60)
        assertEquals(1, idleCalls)

        session.cleanup()
        scope.coroutineContext[Job]?.cancel()
        Unit
    }
}
