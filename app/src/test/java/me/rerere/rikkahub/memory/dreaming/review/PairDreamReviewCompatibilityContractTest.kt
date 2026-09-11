package me.rerere.rikkahub.memory.dreaming.review

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.Path
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class PairDreamReviewCompatibilityContractTest {
    @Test
    fun `historical pair diff failure degrades without hiding current portrait`() {
        val source = Files.readString(
            locateAppRoot().resolve(
                "src/main/java/me/rerere/rikkahub/memory/dreaming/review/RoomDreamReviewStore.kt",
            ),
            StandardCharsets.UTF_8,
        )
        val pairBlock = source.substringAfter("/** Pair-Dream provenance is Experience-backed")
            .substringBefore("private suspend fun readClaimDetail")
        val diffBlock = pairBlock.substringAfter("val diff = snapshotDiff(scopeId, superseded, active)")
            .substringBefore("if (active != null && active.manifestReferencesOrNull()")

        assertTrue("diff is DreamSnapshotDiffResult.Unavailable" in diffBlock)
        assertTrue("degraded = true" in diffBlock)
        assertFalse("return invalidProjection(scopeId, state, usageMode, diff)" in diffBlock)
    }

    private fun locateAppRoot(): Path {
        var current = Path(System.getProperty("user.dir")).toAbsolutePath().normalize()
        repeat(8) {
            if (Files.isRegularFile(current.resolve("build.gradle.kts")) &&
                Files.isDirectory(current.resolve("src/main"))
            ) {
                return current
            }
            current = current.parent ?: return@repeat
        }
        error("Unable to locate app module root")
    }
}
