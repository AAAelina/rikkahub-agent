package me.rerere.rikkahub.memory.dreaming.snapshot

import me.rerere.rikkahub.memory.dreaming.DreamingTestFixtures
import me.rerere.rikkahub.memory.dreaming.model.DreamContentType
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicOrigin
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamSubjectKind
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class DreamPairSnapshotCompatibilityTest {
    private val pairScope = DreamScopeId.requireCanonical(
        "pair:local-user:0950e2dc-9bd5-4801-afa3-aa887aa36b4e",
    )

    @Test
    fun `recognizes historical pair metadata projection drift only`() {
        val current = DreamingTestFixtures.claim().copy(
            scopeId = pairScope,
            subjectKind = DreamSubjectKind.ASSISTANT,
            profileSection = "identity",
            epistemicOrigin = DreamEpistemicOrigin.EXPLICIT,
            contentType = DreamContentType.IDENTITY,
        )
        val buggyProjected = current.copy(
            subjectKind = DreamSubjectKind.USER,
            profileSection = "general",
            epistemicOrigin = DreamEpistemicOrigin.INFERRED,
            contentType = DreamContentType.OTHER,
        )
        val stored = DreamSnapshotCompiler.compile(
            DreamSnapshotCompileRequest(pairScope, V1, listOf(buggyProjected)),
        )
        val expected = DreamSnapshotCompiler.compile(
            DreamSnapshotCompileRequest(pairScope, V1, listOf(current)),
        )

        assertTrue(
            DreamPairSnapshotCompatibility.isLegacyV1MetadataProjectionDrift(
                scopeId = pairScope,
                compilerRevision = V1,
                storedPayloadJson = stored.payloadJson,
                expectedPayloadJson = expected.payloadJson,
            ),
        )
    }

    @Test
    fun `rejects content drift even when pair metadata also differs`() {
        val current = DreamingTestFixtures.claim().copy(
            scopeId = pairScope,
            subjectKind = DreamSubjectKind.ASSISTANT,
            profileSection = "identity",
            epistemicOrigin = DreamEpistemicOrigin.EXPLICIT,
            contentType = DreamContentType.IDENTITY,
        )
        val corrupt = current.copy(
            subjectKind = DreamSubjectKind.USER,
            profileSection = "general",
            epistemicOrigin = DreamEpistemicOrigin.INFERRED,
            contentType = DreamContentType.OTHER,
            statement = current.statement + " altered",
        )
        val stored = DreamSnapshotCompiler.compile(
            DreamSnapshotCompileRequest(pairScope, V1, listOf(corrupt)),
        )
        val expected = DreamSnapshotCompiler.compile(
            DreamSnapshotCompileRequest(pairScope, V1, listOf(current)),
        )

        assertFalse(
            DreamPairSnapshotCompatibility.isLegacyV1MetadataProjectionDrift(
                scopeId = pairScope,
                compilerRevision = V1,
                storedPayloadJson = stored.payloadJson,
                expectedPayloadJson = expected.payloadJson,
            ),
        )
    }

    private companion object {
        const val V1 = "dream-snapshot-compiler-v1"
    }
}
