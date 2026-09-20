package me.rerere.rikkahub.automation

import kotlin.uuid.Uuid

/**
 * Tracks every partial external-run setup stage, including a mark that throws after
 * registering the ID. A completed stop/fence is required before removing the headless mark.
 * No Android dependencies: fault injection covers every stage on the local JVM.
 */
internal class ExternalAutomationSetupGuard {
    var conversationId: Uuid? = null
        private set
    var marked: Boolean = false
        private set

    suspend fun prepare(
        allocate: suspend () -> Uuid,
        persist: suspend (Uuid) -> Unit,
        initialize: suspend (Uuid) -> Unit,
        mark: (Uuid) -> Unit,
        openLedger: suspend (Uuid) -> String,
    ): String {
        val id = allocate()
        conversationId = id
        persist(id)
        initialize(id)
        // mark() can fail AFTER registering the ID (e.g. SharedPreferences persistence).
        // Record ownership before entering it so a partial mark is still compensated.
        marked = true
        mark(id)
        return openLedger(id)
    }

    suspend fun finish(
        knownQuiescent: Boolean,
        stop: suspend (Uuid) -> Boolean,
        unmark: (Uuid) -> Unit,
    ): Boolean {
        val id = conversationId ?: return true
        val quiet = knownQuiescent || stop(id)
        if (quiet && marked) {
            unmark(id)
            marked = false
        }
        return quiet
    }
}
