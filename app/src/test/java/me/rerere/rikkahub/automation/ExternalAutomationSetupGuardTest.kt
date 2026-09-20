package me.rerere.rikkahub.automation

import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Test
import kotlin.uuid.Uuid

/** Injects failures into the real production setup/compensation seam. */
class ExternalAutomationSetupGuardTest {
    @Test fun eachSetupFailureCanReachOneTerminalAndReleaseItsHeadlessMark() = runBlocking {
        for (failureAt in listOf("allocate", "persist", "initialize", "mark", "ledger")) {
            val id = Uuid.random()
            val guard = ExternalAutomationSetupGuard()
            val events = mutableListOf<String>()
            var terminalCallbacks = 0
            try {
                guard.prepare(
                    allocate = {
                        events += "allocate"
                        if (failureAt == "allocate") error("injected:allocate")
                        id
                    },
                    persist = {
                        events += "persist"
                        if (failureAt == "persist") error("injected:persist")
                    },
                    initialize = {
                        events += "initialize"
                        if (failureAt == "initialize") error("injected:initialize")
                    },
                    mark = {
                        events += "mark"
                        if (failureAt == "mark") error("injected:mark-after-registration")
                    },
                    openLedger = {
                        events += "ledger"
                        if (failureAt == "ledger") error("injected:ledger")
                        "ledger-id"
                    },
                )
                fail("setup should have thrown at $failureAt")
            } catch (expected: IllegalStateException) {
                assertTrue(expected.message.orEmpty().startsWith("injected:"))
                val quiet = guard.finish(
                    // No command was submitted during setup: there is no generation to stop,
                    // even when session initialization itself did not finish.
                    knownQuiescent = true,
                    stop = { events += "stop"; error("setup-only failure must not need stop") },
                    unmark = { events += "unmark" },
                )
                assertTrue(quiet)
                terminalCallbacks += 1
            }
            assertEquals("$failureAt: terminal callback count", 1, terminalCallbacks)
            assertFalse("$failureAt: leaked headless ownership", guard.marked)
            if (failureAt == "allocate") {
                assertNull(guard.conversationId)
            } else {
                assertEquals(id, guard.conversationId)
            }
            assertFalse("setup-only failure must not wait on a non-existent run", "stop" in events)
            assertEquals(
                "$failureAt: release only if marking was attempted",
                failureAt in listOf("mark", "ledger"),
                "unmark" in events,
            )
        }
    }

    @Test fun unconfirmedTerminationMustRetainHeadlessMarker() = runBlocking {
        val guard = ExternalAutomationSetupGuard()
        guard.prepare(
            allocate = { Uuid.random() },
            persist = {},
            initialize = {},
            mark = {},
            openLedger = { "ledger" },
        )
        val actions = mutableListOf<String>()
        assertFalse(guard.finish(false, { actions += "stop"; false }, { actions += "unmark" }))
        assertTrue(guard.marked)
        assertEquals(listOf("stop"), actions)
        assertTrue(guard.finish(false, { actions += "stop"; true }, { actions += "unmark" }))
        assertFalse(guard.marked)
        assertEquals(listOf("stop", "stop", "unmark"), actions)
    }

    @Test fun successfulSetupAndQuiescentCompletionReleaseOnce() = runBlocking {
        val guard = ExternalAutomationSetupGuard()
        val id = Uuid.random()
        assertEquals("run", guard.prepare(
            allocate = { id }, persist = {}, initialize = {}, mark = {}, openLedger = { "run" },
        ))
        var released = 0
        assertTrue(guard.finish(true, { error("known quiescent must not stop again") }, { released++ }))
        assertTrue(guard.finish(true, { error("known quiescent must not stop again") }, { released++ }))
        assertEquals(1, released)
    }
}
