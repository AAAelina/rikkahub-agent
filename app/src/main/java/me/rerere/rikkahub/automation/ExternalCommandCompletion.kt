package me.rerere.rikkahub.automation

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.withTimeoutOrNull
import me.rerere.rikkahub.service.chat.CommandOutcome

internal suspend fun awaitExternalCommandOutcome(
    outcome: Deferred<CommandOutcome>,
    timeoutMs: Long,
    graceMs: Long = 1_000L,
    stopAndAwait: suspend () -> Unit,
): CommandOutcome? = withTimeoutOrNull(timeoutMs) { outcome.await() } ?: run {
    stopAndAwait()
    withTimeoutOrNull(graceMs) { outcome.await() }
}
