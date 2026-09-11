package me.rerere.rikkahub.memory.dreaming.synthesis

import kotlinx.coroutines.runBlocking
import me.rerere.rikkahub.memory.MemoryApprovalSource
import me.rerere.rikkahub.memory.dreaming.DreamingTestFixtures
import me.rerere.rikkahub.memory.dreaming.input.DreamDeterministicInvalidation
import me.rerere.rikkahub.memory.dreaming.input.DreamDeterministicInvalidationReason
import me.rerere.rikkahub.memory.dreaming.model.DREAM_PROPOSAL_SCHEMA_VERSION
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimState
import me.rerere.rikkahub.memory.dreaming.model.DreamContentType
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicOrigin
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicType
import me.rerere.rikkahub.memory.dreaming.model.DreamSubjectKind
import me.rerere.rikkahub.memory.dreaming.model.DreamSupportType
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class DreamProposalValidatorTest {
    private val validator = DreamProposalValidator(
        claimIdFactory = DreamClaimIdFactory { DreamingTestFixtures.NEW_CLAIM_ID },
    )

    @Test
    fun `valid direct authority proposal produces active contextual claim`() = runBlocking {
        val input = DreamingTestFixtures.input()
        val result = validator.validate(
            DreamProposalValidationRequest(input, newClaimProposal(input, DreamEpistemicType.PROJECT_STATE)),
        ) as DreamProposalValidationResult.Valid

        assertEquals(DreamClaimState.ACTIVE_CONTEXTUAL, result.plan.upserts.single().nextState)
        assertTrue(result.plan.upserts.single().sources.single().directAuthority)
    }

    @Test
    fun `pair metadata survives validated version to resulting head projection`() = runBlocking {
        val input = DreamingTestFixtures.input()
        val proposed = proposedClaim(
            input.allowedMemories.keys.single(),
            DreamEpistemicType.BELIEF,
        ).copy(
            subjectKind = DreamSubjectKind.ASSISTANT,
            profileSection = "identity",
            epistemicOrigin = DreamEpistemicOrigin.EXPLICIT,
            contentType = DreamContentType.IDENTITY,
        )
        val proposal = envelope(
            input,
            listOf(DreamProposalOperation.UpsertClaim(null, null, proposed)),
        )

        val result = validator.validate(DreamProposalValidationRequest(input, proposal))
            as DreamProposalValidationResult.Valid
        val version = result.plan.upserts.single()
        val head = result.plan.resultingClaims.single()

        assertEquals(version.subjectKind, head.subjectKind)
        assertEquals(version.profileSection, head.profileSection)
        assertEquals(version.epistemicOrigin, head.epistemicOrigin)
        assertEquals(version.contentType, head.contentType)
    }

    @Test
    fun `inferred portrait claim is active with reduced confidence when evidence is clean`() = runBlocking {
        val input = DreamingTestFixtures.input()
        val result = validator.validate(
            DreamProposalValidationRequest(input, newClaimProposal(input, DreamEpistemicType.BELIEF)),
        ) as DreamProposalValidationResult.Valid

        assertEquals(DreamClaimState.ACTIVE_CONTEXTUAL, result.plan.upserts.single().nextState)
        assertEquals(750, result.plan.upserts.single().confidencePermille)
    }

    @Test
    fun `raw source prompt injection forces pending review`() = runBlocking {
        val input = DreamingTestFixtures.input().let { original ->
            original.copy(
                allowedMemories = original.allowedMemories.mapValues { (_, allowed) ->
                    allowed.copy(rawSourcePromptInjectionDetected = true)
                },
            )
        }

        val result = validator.validate(
            DreamProposalValidationRequest(input, newClaimProposal(input, DreamEpistemicType.PROJECT_STATE)),
        ) as DreamProposalValidationResult.Valid

        assertEquals(DreamClaimState.PENDING_REVIEW, result.plan.upserts.single().nextState)
    }

    @Test
    fun `manual correction memory may directly support a claim without conversation source`() = runBlocking {
        val manual = DreamingTestFixtures.memory(sources = emptyList()).copy(
            approvalSource = MemoryApprovalSource.USER_REVIEWED,
        )
        val input = DreamingTestFixtures.input(memory = manual)

        val result = validator.validate(
            DreamProposalValidationRequest(input, newClaimProposal(input, DreamEpistemicType.PROJECT_STATE)),
        ) as DreamProposalValidationResult.Valid

        assertEquals(DreamClaimState.ACTIVE_CONTEXTUAL, result.plan.upserts.single().nextState)
        assertEquals(DreamingTestFixtures.pin(manual), result.plan.modelEvidencePins.single())
    }

    @Test
    fun `automatic memory with empty source manifest is rejected defensively`() = runBlocking {
        val manual = DreamingTestFixtures.memory(sources = emptyList())
        val original = DreamingTestFixtures.input(memory = manual)
        val token = original.allowedMemories.keys.single()
        val automatic = manual.copy(approvalSource = MemoryApprovalSource.AUTO_SAFE)
        val input = original.copy(
            allowedMemories = mapOf(
                token to original.allowedMemories.getValue(token).copy(
                    memory = automatic,
                    pin = DreamingTestFixtures.pin(automatic),
                    sourceRereadComplete = false,
                ),
            ),
        )

        assertRejected(
            DreamProposalValidationFailure.AUTHORITY_SOURCE_IDENTITY_REQUIRED,
            validator.validate(
                DreamProposalValidationRequest(input, newClaimProposal(input, DreamEpistemicType.PROJECT_STATE)),
            ),
        )
    }

    @Test
    fun `automatic memory requires completed exact source reread defensively`() = runBlocking {
        val original = DreamingTestFixtures.input()
        val token = original.allowedMemories.keys.single()
        val automatic = original.allowedMemories.getValue(token).memory.copy(
            approvalSource = MemoryApprovalSource.AUTO_SAFE,
        )
        val input = original.copy(
            allowedMemories = mapOf(
                token to original.allowedMemories.getValue(token).copy(
                    memory = automatic,
                    pin = DreamingTestFixtures.pin(automatic),
                    sourceRereadComplete = false,
                ),
            ),
        )

        assertRejected(
            DreamProposalValidationFailure.AUTHORITY_SOURCE_REREAD_INCOMPLETE,
            validator.validate(
                DreamProposalValidationRequest(input, newClaimProposal(input, DreamEpistemicType.PROJECT_STATE)),
            ),
        )
    }

    @Test
    fun `authority expiry is rechecked after model call`() = runBlocking {
        val input = DreamingTestFixtures.input()
        val token = input.allowedMemories.keys.single()
        val expired = input.allowedMemories.getValue(token).memory.copy(
            expiresAtEpochMs = DreamingTestFixtures.NOW,
        )
        val tampered = input.copy(
            allowedMemories = input.allowedMemories + (
                token to input.allowedMemories.getValue(token).copy(
                    memory = expired,
                    pin = DreamingTestFixtures.pin(expired),
                )
                ),
        )

        assertRejected(
            DreamProposalValidationFailure.AUTHORITY_EXPIRED,
            validator.validate(
                DreamProposalValidationRequest(tampered, newClaimProposal(tampered, DreamEpistemicType.PROJECT_STATE)),
            ),
        )
    }

    @Test
    fun `host deterministic invalidation cannot be overwritten by model`() = runBlocking {
        val claim = DreamingTestFixtures.claim()
        val input = DreamingTestFixtures.input(claims = listOf(claim))
        val claimToken = input.allowedClaims.keys.single()
        val memoryToken = input.allowedMemories.keys.single()
        val invalidated = input.copy(
            deterministicInvalidations = listOf(
                DreamDeterministicInvalidation(
                    claim.claimId,
                    claim.revision,
                    DreamDeterministicInvalidationReason.SOURCE_HASH_CHANGED,
                ),
            ),
        )
        val proposal = envelope(
            input,
            listOf(
                DreamProposalOperation.UpsertClaim(
                    claimToken,
                    claim.revision,
                    proposedClaim(memoryToken, DreamEpistemicType.PROJECT_STATE),
                ),
            ),
        )

        assertRejected(
            DreamProposalValidationFailure.HOST_INVALIDATION_CONFLICT,
            validator.validate(DreamProposalValidationRequest(invalidated, proposal)),
        )
    }

    @Test
    fun `host invalidation writes a complete immutable next claim version`() = runBlocking {
        val claim = DreamingTestFixtures.claim()
        val input = DreamingTestFixtures.input(claims = listOf(claim)).copy(
            deterministicInvalidations = listOf(
                DreamDeterministicInvalidation(
                    claim.claimId,
                    claim.revision,
                    DreamDeterministicInvalidationReason.SOURCE_EXPIRED,
                ),
            ),
        )

        val result = validator.validate(
            DreamProposalValidationRequest(input, envelope(input, listOf(DreamProposalOperation.NoOp))),
        ) as DreamProposalValidationResult.Valid
        val transition = result.plan.transitions.single()

        assertEquals(claim.revision + 1, transition.nextVersion.nextRevision)
        assertEquals(claim.title, transition.nextVersion.title)
        assertEquals(claim.statement, transition.nextVersion.statement)
        assertEquals(claim.sources, transition.nextVersion.sources)
        assertTrue(result.plan.modelEvidencePins.isEmpty())
        assertEquals(DreamClaimState.STALE, result.plan.resultingClaims.single().state)
        assertEquals(claim.revision + 1, result.plan.resultingClaims.single().revision)
    }

    @Test
    fun `model invalidation evidence remains in the live recheck set`() = runBlocking {
        val claim = DreamingTestFixtures.claim()
        val input = DreamingTestFixtures.input(claims = listOf(claim))
        val proposal = envelope(
            input,
            listOf(
                DreamProposalOperation.InvalidateClaim(
                    targetClaimToken = input.allowedClaims.keys.single(),
                    expectedClaimRevision = claim.revision,
                    reason = DreamProposalInvalidationReason.CONTRADICTED_BY_AUTHORITY,
                    evidence = listOf(
                        DreamProposedEvidence(
                            experienceToken = input.allowedMemories.keys.single(),
                            expectedEpoch = 2,
                            supportType = DreamSupportType.CONTRADICTS,
                        ),
                    ),
                ),
            ),
        )

        val result = validator.validate(DreamProposalValidationRequest(input, proposal))
            as DreamProposalValidationResult.Valid

        assertEquals(1, result.plan.transitions.size)
        assertEquals(DreamingTestFixtures.pin(DreamingTestFixtures.memory()), result.plan.modelEvidencePins.single())
    }

    @Test
    fun `no-op retains claims omitted from bounded model view`() = runBlocking {
        val hidden = DreamingTestFixtures.claim()
        val input = DreamingTestFixtures.input(claims = listOf(hidden)).copy(allowedClaims = emptyMap())
        val proposal = envelope(input, listOf(DreamProposalOperation.NoOp))

        val result = validator.validate(DreamProposalValidationRequest(input, proposal))
            as DreamProposalValidationResult.Valid
        assertEquals(listOf(hidden.claimId), result.plan.resultingClaims.map { it.claimId })
    }

    @Test
    fun `relative or unsupported temporal text without trusted message timestamp fails closed`() = runBlocking {
        val input = DreamingTestFixtures.input().let { original ->
            original.copy(
                allowedMemories = original.allowedMemories.mapValues { (_, allowed) ->
                    allowed.copy(trustedSourceTimestampsEpochMs = emptyList())
                },
            )
        }
        val claim = proposedClaim(
            input.allowedMemories.keys.single(),
            DreamEpistemicType.PLAN,
        ).copy(temporalExpression = "tomorrow")

        assertRejected(
            DreamProposalValidationFailure.INVALID_TEMPORAL_EXPRESSION,
            validator.validate(
                DreamProposalValidationRequest(input, envelope(input, listOf(DreamProposalOperation.UpsertClaim(null, null, claim)))),
            ),
        )
    }

    private fun newClaimProposal(
        input: me.rerere.rikkahub.memory.dreaming.input.DreamInputBundle,
        type: DreamEpistemicType,
    ) = envelope(
        input,
        listOf(
            DreamProposalOperation.UpsertClaim(
                null,
                null,
                proposedClaim(input.allowedMemories.keys.single(), type),
            ),
        ),
    )

    private fun proposedClaim(
        memoryToken: me.rerere.rikkahub.memory.dreaming.model.DreamOpaqueToken,
        type: DreamEpistemicType,
    ) = DreamProposedClaim(
        claimKeyHint = "project.offline",
        subjectKind = DreamSubjectKind.USER,
        profileSection = "project",
        epistemicOrigin = when (type) {
            DreamEpistemicType.OBSERVATION -> DreamEpistemicOrigin.OBSERVED
            DreamEpistemicType.BELIEF -> DreamEpistemicOrigin.INFERRED
            else -> DreamEpistemicOrigin.EXPLICIT
        },
        contentType = when (type) {
            DreamEpistemicType.PROJECT_STATE -> DreamContentType.PROJECT_STATE
            DreamEpistemicType.PLAN -> DreamContentType.PLAN
            DreamEpistemicType.CONSTRAINT -> DreamContentType.CONSTRAINT
            DreamEpistemicType.PREFERENCE_SUMMARY -> DreamContentType.PREFERENCE
            DreamEpistemicType.OBSERVATION,
            DreamEpistemicType.BELIEF,
            -> DreamContentType.OTHER
        },
        title = "Offline project",
        statement = "The user is building an offline memory system.",
        temporalExpression = null,
        evidence = listOf(DreamProposedEvidence(memoryToken, 2, DreamSupportType.SUPPORTS)),
    )

    private fun envelope(
        input: me.rerere.rikkahub.memory.dreaming.input.DreamInputBundle,
        operations: List<DreamProposalOperation>,
    ) = DreamProposalEnvelope(
        schemaVersion = DREAM_PROPOSAL_SCHEMA_VERSION,
        proposalNonce = input.proposalNonce,
        baseMemoryEpoch = input.fence.baseMemoryEpoch,
        baseDreamRevision = input.fence.baseDreamRevision,
        mode = input.fence.mode,
        operations = operations,
    )

    private fun assertRejected(
        expected: DreamProposalValidationFailure,
        actual: DreamProposalValidationResult,
    ) {
        assertEquals(expected, (actual as DreamProposalValidationResult.Rejected).failure)
    }
}
