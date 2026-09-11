package me.rerere.rikkahub.memory.dreaming.store

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import org.junit.Assert.assertTrue
import org.junit.Test

class PairDreamInputSeedContractTest {
    @Test
    fun `pair synthesis reads a bounded authority-ranked reserve set`() {
        val appRoot = locateAppRoot()
        val dao = Files.readString(
            appRoot.resolve("src/main/java/me/rerere/rikkahub/data/db/dao/DreamExperienceDao.kt"),
            StandardCharsets.UTF_8,
        )
        val store = Files.readString(
            appRoot.resolve(
                "src/main/java/me/rerere/rikkahub/memory/dreaming/store/RoomDreamSynthesisStore.kt",
            ),
            StandardCharsets.UTF_8,
        )
        val pairSeed = store.substringAfter("if (DreamPairScopeId.parseOrNull(fence.scopeId.value) != null)")
            .substringBefore("val memories = when (fence.mode)")

        assertTrue("listSynthesisExperiences(" in pairSeed)
        assertTrue("limit = MAX_PAIR_DREAM_INPUT_CANDIDATE_SCAN" in pairSeed)
        assertTrue(".take(DREAM_SYNTHESIS_INPUT_BUDGET.maxMemories)" in pairSeed)
        assertTrue("private const val MAX_PAIR_DREAM_INPUT_CANDIDATE_SCAN = 512" in store)
        assertTrue("USER_CORRECTION" in dao)
        assertTrue("USER_REJECTION" in dao)
        assertTrue("status != 'DISCARDED'" in dao)
    }

    private fun locateAppRoot(): Path {
        val cwd = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize()
        return if (Files.isDirectory(cwd.resolve("src/main/java"))) cwd else cwd.resolve("app")
    }
}
