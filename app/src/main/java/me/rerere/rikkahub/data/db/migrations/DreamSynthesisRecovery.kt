package me.rerere.rikkahub.data.db.migrations

import androidx.sqlite.db.SupportSQLiteDatabase

internal val DREAM_SYNTHESIS_LEGACY_ORPHAN_RECOVERY_SQL =
    """
    UPDATE dream_runs
    SET status = 'CANCELLED',
        failure_code = 'LEGACY_ORPHAN_RECOVERY',
        lease_owner = NULL,
        lease_until_ms = NULL,
        finished_at_ms = MAX(updated_at_ms, strftime('%s','now') * 1000),
        updated_at_ms = MAX(updated_at_ms, strftime('%s','now') * 1000)
    WHERE mode IN ('INCREMENTAL', 'FULL')
      AND status = 'PENDING'
      AND NOT EXISTS (
          SELECT 1
          FROM dream_experience_state AS pair_state
          WHERE pair_state.pair_scope_id = dream_runs.scope_id
      )
    """.trimIndent()

/**
 * Repairs synthesis runs created before Pair-Dream became the authority.
 *
 * v50 moved scheduling ownership to dream_experience_state, but old global
 * synthesis rows could still occupy the old global run counter forever.
 */
fun recoverOrphanedDreamSynthesisRuns(db: SupportSQLiteDatabase) {
    db.execSQL(DREAM_SYNTHESIS_LEGACY_ORPHAN_RECOVERY_SQL)
}
