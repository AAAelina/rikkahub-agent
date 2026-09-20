package me.rerere.rikkahub.data.repository

/**
 * Use only after the authority transaction has committed. Search and its diagnostics cannot
 * turn a committed write into a failed write (whose caller might try to restore an old graph).
 */
internal suspend fun runConversationPostCommitProjection(
    project: suspend () -> Unit,
    onFailure: (Exception) -> Unit,
) {
    try {
        project()
    } catch (failure: Exception) {
        try {
            onFailure(failure)
        } catch (_: Exception) {
            // Diagnostics carry no authority either.
        }
    }
}
