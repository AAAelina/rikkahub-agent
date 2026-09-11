package me.rerere.rikkahub.memory.dreaming.review

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import me.rerere.rikkahub.memory.dreaming.DreamingTestFixtures
import me.rerere.rikkahub.memory.dreaming.model.DREAM_SNAPSHOT_SCHEMA_VERSION
import me.rerere.rikkahub.memory.dreaming.model.DreamCanonicalJson
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimMutationReason
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimState
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimVersionCanonicalV1
import me.rerere.rikkahub.memory.dreaming.model.DreamContentType
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicOrigin
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamSubjectKind
import me.rerere.rikkahub.memory.dreaming.model.DreamValidatedClaimVersion
import me.rerere.rikkahub.memory.dreaming.model.canonicalMapOf
import me.rerere.rikkahub.memory.dreaming.model.jsonNumberOrNull
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotCompileRequest
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotCompiler
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class DreamSnapshotDiffTest {
    @Test
    fun `pair snapshot diff accepts pair portrait fragment metadata`() {
        val pairScope = DreamScopeId.requireCanonical(
            "pair:local-user:11111111-1111-4111-8111-111111111111",
        )
        val pairClaim = DreamingTestFixtures.claim().copy(
            scopeId = pairScope,
            subjectKind = DreamSubjectKind.ASSISTANT,
            profileSection = "identity",
            epistemicOrigin = DreamEpistemicOrigin.SELF_REFLECTED,
            contentType = DreamContentType.IDENTITY,
        )

        val result = DreamSnapshotDiff.compare(
            previous = null,
            current = document(
                snapshotId = "11111111-1111-4111-8111-111111111114",
                claims = listOf(pairClaim),
                scopeId = pairScope,
            ),
        )

        assertTrue(result is DreamSnapshotDiffResult.Available)
    }

    @Test
    fun `pair snapshot diff accepts legacy five-section predecessor`() {
        val pairScope = DreamScopeId.requireCanonical(
            "pair:local-user:11111111-1111-4111-8111-111111111111",
        )
        val pairClaim = DreamingTestFixtures.claim().copy(
            scopeId = pairScope,
            subjectKind = DreamSubjectKind.USER,
            profileSection = "identity",
            epistemicOrigin = DreamEpistemicOrigin.OBSERVED,
            contentType = DreamContentType.IDENTITY,
        )
        val legacy = legacyPairDocument(
            snapshotId = "11111111-1111-4111-8111-111111111115",
            scopeId = pairScope,
            claim = pairClaim,
        )
        val current = document(
            snapshotId = "11111111-1111-4111-8111-111111111116",
            claims = listOf(pairClaim.copy(revision = 2)),
            scopeId = pairScope,
        )

        assertTrue(DreamSnapshotDiff.compare(previous = legacy, current = current) is DreamSnapshotDiffResult.Available)
    }

    @Test
    fun `diff reports only added updated and retired with confidence and temporal flags`() {
        val first = DreamingTestFixtures.claim()
        val retired = DreamingTestFixtures.claim(
            id = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee",
            key = "retired",
        )
        val updated = first.copy(
            revision = 2,
            confidencePermille = 800,
            validToEpochMs = first.validToEpochMs?.plus(1_000),
        )
        val added = DreamingTestFixtures.claim(
            id = DreamingTestFixtures.NEW_CLAIM_ID,
            key = "added",
        )

        val result = DreamSnapshotDiff.compare(
            previous = document("11111111-1111-4111-8111-111111111112", listOf(first, retired)),
            current = document("11111111-1111-4111-8111-111111111113", listOf(updated, added)),
        ) as DreamSnapshotDiffResult.Available

        assertEquals(
            setOf(
                DreamSnapshotChangeType.ADDED,
                DreamSnapshotChangeType.UPDATED,
                DreamSnapshotChangeType.RETIRED,
            ),
            result.changes.mapTo(mutableSetOf(), DreamSnapshotChange::type),
        )
        val changed = result.changes.single { it.type == DreamSnapshotChangeType.UPDATED }
        assertTrue(changed.confidenceChanged)
        assertTrue(changed.temporalChanged)
    }

    @Test
    fun `payload hash mismatch fails closed`() {
        val document = document(
            "11111111-1111-4111-8111-111111111113",
            listOf(DreamingTestFixtures.claim()),
        ).copy(payloadHash = me.rerere.rikkahub.memory.dreaming.model.DreamSha256("0".repeat(64)))

        assertEquals(
            DreamSnapshotDiffResult.Unavailable(DreamSnapshotDiffFailure.PAYLOAD_HASH_MISMATCH),
            DreamSnapshotDiff.compare(previous = null, current = document),
        )
    }

    @Test
    fun `fragment mutation with recomputed payload hash still fails manifest binding`() {
        val original = document(
            "11111111-1111-4111-8111-111111111113",
            listOf(DreamingTestFixtures.claim()),
        )
        val mutatedJson = original.payloadJson.replace("Offline memory project", "Altered memory project")
        val mutated = original.copy(
            payloadJson = mutatedJson,
            payloadHash = DreamCanonicalJson.sha256(mutatedJson.toByteArray(Charsets.UTF_8)),
        )

        assertEquals(
            DreamSnapshotDiffResult.Unavailable(DreamSnapshotDiffFailure.FRAGMENT_INVALID),
            DreamSnapshotDiff.compare(previous = null, current = mutated),
        )
    }

    @Test
    fun `both review reasons are part of the canonical immutable claim codec`() {
        val head = DreamingTestFixtures.claim()
        listOf(
            DreamClaimMutationReason.USER_REJECTED to DreamClaimState.REJECTED,
            DreamClaimMutationReason.USER_CORRECTION to DreamClaimState.SUPERSEDED,
        ).forEach { (reason, state) ->
            val encoded = DreamClaimVersionCanonicalV1.encode(
                DreamValidatedClaimVersion(
                    claimId = head.claimId,
                    expectedPreviousRevision = head.revision,
                    nextRevision = head.revision + 1,
                    claimKey = head.claimKey,
                    storageClass = head.storageClass,
                    epistemicType = head.epistemicType,
                    nextState = state,
                    title = head.title,
                    statement = head.statement,
                    confidencePermille = head.confidencePermille,
                    temporalState = head.temporalState,
                    validFromEpochMs = head.validFromEpochMs,
                    validToEpochMs = head.validToEpochMs,
                    sources = head.sources,
                    reason = reason,
                ),
            )
            assertTrue(encoded.canonicalClaimJson.contains("\"reason\":\"${reason.name}\""))
        }
    }

    private fun document(
        snapshotId: String,
        claims: List<me.rerere.rikkahub.memory.dreaming.model.DreamClaimHead>,
        scopeId: DreamScopeId = DreamingTestFixtures.scope,
    ): DreamSnapshotDocument {
        val compiled = DreamSnapshotCompiler.compile(
            DreamSnapshotCompileRequest(
                scopeId = scopeId,
                compilerRevision = "review-test-v1",
                claims = claims,
            ),
        )
        return DreamSnapshotDocument(
            scopeId = scopeId,
            snapshotId = snapshotId,
            schemaVersion = DREAM_SNAPSHOT_SCHEMA_VERSION,
            compilerRevision = compiled.compilerRevision,
            payloadJson = compiled.payloadJson,
            payloadHash = compiled.payloadHash,
            manifestHash = compiled.manifestHash,
            claimCount = compiled.claimCount,
        )
    }

    private fun legacyPairDocument(
        snapshotId: String,
        scopeId: DreamScopeId,
        claim: me.rerere.rikkahub.memory.dreaming.model.DreamClaimHead,
    ): DreamSnapshotDocument {
        val fragment = JsonObject(
            canonicalMapOf(
                "claim_key" to JsonPrimitive(claim.claimKey),
                "confidence_permille" to JsonPrimitive(claim.confidencePermille),
                "epistemic_type" to JsonPrimitive(claim.epistemicType.name),
                "statement" to JsonPrimitive(claim.statement),
                "storage_class" to JsonPrimitive(claim.storageClass.name),
                "temporal_state" to JsonPrimitive(claim.temporalState.name),
                "title" to JsonPrimitive(claim.title),
                "valid_from_epoch_ms" to claim.validFromEpochMs.jsonNumberOrNull(),
                "valid_to_epoch_ms" to claim.validToEpochMs.jsonNumberOrNull(),
            ),
        )
        val fragmentHash = DreamCanonicalJson.sha256(fragment)
        val manifest = JsonArray(
            listOf(
                JsonObject(
                    canonicalMapOf(
                        "claim_id" to JsonPrimitive(claim.claimId),
                        "claim_revision" to JsonPrimitive(claim.revision),
                        "fragment_hash" to JsonPrimitive(fragmentHash.value),
                        "ordinal" to JsonPrimitive(0),
                        "section" to JsonPrimitive("current_projects"),
                    ),
                ),
            ),
        )
        val sections = JsonObject(
            canonicalMapOf(
                "active_constraints" to JsonArray(emptyList()),
                "active_plans" to JsonArray(emptyList()),
                "current_projects" to JsonArray(listOf(fragment)),
                "other_context" to JsonArray(emptyList()),
                "profile" to JsonArray(emptyList()),
            ),
        )
        val root = JsonObject(
            canonicalMapOf(
                "compiler_revision" to JsonPrimitive("review-test-v1"),
                "manifest" to manifest,
                "schema_version" to JsonPrimitive(DREAM_SNAPSHOT_SCHEMA_VERSION),
                "sections" to sections,
            ),
        )
        val payload = DreamCanonicalJson.encode(root)
        return DreamSnapshotDocument(
            scopeId = scopeId,
            snapshotId = snapshotId,
            schemaVersion = DREAM_SNAPSHOT_SCHEMA_VERSION,
            compilerRevision = "review-test-v1",
            payloadJson = payload,
            payloadHash = DreamCanonicalJson.sha256(payload.toByteArray(Charsets.UTF_8)),
            manifestHash = DreamCanonicalJson.sha256(manifest),
            claimCount = 1,
        )
    }
}
