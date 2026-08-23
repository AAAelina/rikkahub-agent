package me.rerere.rikkahub.memory.dreaming.store

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class DreamPrivacySnapshotInspectionTest {
    @Test
    fun `active snapshot requires snapshot inspection even when no claim is affected`() {
        assertTrue(
            shouldInspectDreamPrivacySnapshots(
                entireScope = false,
                hasAffectedClaims = false,
                activeSnapshotId = "active-snapshot",
            ),
        )
    }

    @Test
    fun `empty inactive scope can skip snapshot inspection`() {
        assertFalse(
            shouldInspectDreamPrivacySnapshots(
                entireScope = false,
                hasAffectedClaims = false,
                activeSnapshotId = null,
            ),
        )
    }
}
