package me.rerere.rikkahub.memory.dreaming.experience

import kotlinx.serialization.json.Json
import me.rerere.rikkahub.data.db.entity.DreamExperienceEntity
import me.rerere.rikkahub.memory.dreaming.input.DreamInputCandidateOrigin
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Test

class DreamExperienceProjectionTest {
    private val scopeId = DreamScopeId.requireCanonical(
        "pair:local-user:0950e2dc-9bd5-4801-afa3-aa887aa36b4e",
    )

    @Test
    fun `valid pair experience projects into a synthesis candidate`() {
        assertNotNull(experience().toCandidate())
    }

    @Test
    fun `one malformed legacy experience is skipped instead of aborting the seed`() {
        assertNull(experience(summary = "   ").toCandidate())
        assertNull(experience(occurredAtMs = 2_000L, ingestedAtMs = 1_000L).toCandidate())
    }

    private fun DreamExperienceEntity.toCandidate() = toDreamInputCandidate(
        scopeId = scopeId,
        origin = DreamInputCandidateOrigin.FULL_REBUILD,
        json = Json,
    )

    private fun experience(
        summary: String = "The user prefers concise, evidence-grounded answers.",
        occurredAtMs: Long = 1_000L,
        ingestedAtMs: Long = 2_000L,
    ) = DreamExperienceEntity(
        experienceId = "experience-1",
        pairScopeId = scopeId.value,
        experienceEpoch = 1L,
        sourceKind = "MANUAL",
        sourceRef = "test-source",
        conversationId = null,
        occurredAtMs = occurredAtMs,
        ingestedAtMs = ingestedAtMs,
        actor = "USER",
        experienceKind = "PREFERENCE",
        summary = summary,
        salience = 1.0,
        novelty = 1.0,
        identityWeight = 1.0,
        relationshipWeight = 0.5,
        emotionalWeight = 0.0,
        confidence = 1.0,
        contentDigest = "0".repeat(64),
        status = "PENDING",
        sourceManifestJson = "[]",
    )
}
