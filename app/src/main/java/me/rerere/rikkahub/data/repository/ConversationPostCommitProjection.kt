package me.rerere.rikkahub.data.repository

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/** Application-owned, serialized derived work; enqueue never suspends the committing caller.
 * Cancellation belongs to the projection job, not to the already committed DB write.
 * Process death can lose pending work; search remains rebuildable, not authoritative.
 */
internal class ConversationPostCommitProjection(private val scope: CoroutineScope) {
    private val mutex = Mutex()

    fun enqueue(project: suspend () -> Unit, onFailure: (Exception) -> Unit): Job =
        scope.launch(Dispatchers.IO) {
            mutex.withLock { runConversationPostCommitProjection(project, onFailure) }
        }
}

internal suspend fun runConversationPostCommitProjection(
    project: suspend () -> Unit,
    onFailure: (Exception) -> Unit,
) {
    try {
        project()
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (failure: Exception) {
        try {
            onFailure(failure)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            // Diagnostics carry no authority.
        }
    }
}
