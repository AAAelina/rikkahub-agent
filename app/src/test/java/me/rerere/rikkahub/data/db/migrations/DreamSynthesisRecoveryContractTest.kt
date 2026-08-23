package me.rerere.rikkahub.data.db.migrations

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class DreamSynthesisRecoveryContractTest {
    @Test
    fun `legacy pending synthesis is terminalized only outside Pair authority`() {
        val sql = DREAM_SYNTHESIS_LEGACY_ORPHAN_RECOVERY_SQL.lowercase()

        assertTrue(sql.contains("status = 'cancelled'"))
        assertTrue(sql.contains("failure_code = 'legacy_orphan_recovery'"))
        assertTrue(sql.contains("status = 'pending'"))
        assertTrue(sql.contains("not exists"))
        assertTrue(sql.contains("pair_state.pair_scope_id = dream_runs.scope_id"))
        assertFalse(sql.contains("scope_id not in"))
    }
}
