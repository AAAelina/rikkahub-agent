package me.rerere.rikkahub.memory.dreaming.synthesis

import java.util.Locale
import kotlin.uuid.Uuid
import me.rerere.rikkahub.memory.MemoryContentGuard
import me.rerere.rikkahub.memory.MemoryLifecycleStatus
import me.rerere.rikkahub.memory.MemoryTruthStatus
import me.rerere.rikkahub.memory.dreaming.input.DreamAllowedMemory
import me.rerere.rikkahub.memory.dreaming.input.DreamInputBundle
import me.rerere.rikkahub.memory.dreaming.model.DREAM_PROPOSAL_SCHEMA_VERSION
import me.rerere.rikkahub.memory.dreaming.model.DREAM_AUTHORITY_PIN_ORDER
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityFingerprintV1
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimHead
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimMutationReason
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimSourcePin
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimState
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimVersionCanonicalV1
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicType
import me.rerere.rikkahub.memory.dreaming.model.DreamSupportType
import me.rerere.rikkahub.memory.dreaming.model.DreamValidatedClaimTransition
import me.rerere.rikkahub.memory.dreaming.model.DreamValidatedClaimVersion
import me.rerere.rikkahub.memory.dreaming.model.DreamValidatedPlan
import me.rerere.rikkahub.memory.dreaming.model.normalizeDreamText
import me.rerere.rikkahub.memory.dreaming.temporal.DeterministicTemporalParser
import me.rerere.rikkahub.memory.dreaming.temporal.ExplicitTemporalOutcome
import me.rerere.rikkahub.memory.dreaming.temporal.TemporalParseRequest
import me.rerere.rikkahub.memory.dreaming.temporal.TemporalParseResult
import me.rerere.rikkahub.memory.dreaming.temporal.TemporalState
import me.rerere.rikkahub.memory.dreaming.temporal.TemporalStateEngine
import me.rerere.rikkahub.memory.dreaming.temporal.TemporalTransitionRequest

enum class DreamProposalValidationFailure {
    SCHEMA_VERSION_MISMATCH,
    NONCE_MISMATCH,
    MEMORY_EPOCH_MISMATCH,
    DREAM_REVISION_MISMATCH,
    MODE_MISMATCH,
    INVALID_NO_OP,
    UNKNOWN_MEMORY_TOKEN,
    UNKNOWN_CLAIM_TOKEN,
    TOKEN_REVISION_MISMATCH,
    AUTHORITY_SCOPE_MISMATCH,
    AUTHORITY_REVISION_MISMATCH,
    AUTHORITY_HASH_MISMATCH,
    AUTHORITY_SOURCE_MANIFEST_MISMATCH,
    AUTHORITY_SOURCE_IDENTITY_REQUIRED,
    AUTHORITY_SOURCE_REREAD_INCOMPLETE,
    AUTHORITY_NOT_ACTIVE_CONFIRMED,
    AUTHORITY_EXPIRED,
    AUTHORITY_TOMBSTONED,
    NO_DIRECT_PROVENANCE,
    TARGET_STATE_INVALID,
    CLAIM_KEY_INVALID,
    DUPLICATE_CLAIM_KEY,
    CLAIM_MUTATED_TWICE,
    HOST_INVALIDATION_CONFLICT,
    INVALID_TEMPORAL_EXPRESSION,
    UNSAFE_TEXT,
}

sealed interface DreamProposalValidationResult {
    data class Valid(val plan: DreamValidatedPlan) : DreamProposalValidationResult
    data class Rejected(val failure: DreamProposalValidationFailure) : DreamProposalValidationResult
}

data class DreamProposalValidationRequest(
    val input: DreamInputBundle,
    val proposal: DreamProposalEnvelope,
)

fun interface DreamClaimIdFactory {
    fun nextClaimId(): String
}

object RandomDreamClaimIdFactory : DreamClaimIdFactory {
    override fun nextClaimId(): String = Uuid.random().toString()
}

/** Host-owned semantic and authority validator. A single invalid operation rejects the whole output. */
class DreamProposalValidator(
    private val claimIdFactory: DreamClaimIdFactory = RandomDreamClaimIdFactory,
    private val contentGuard: MemoryContentGuard = MemoryContentGuard(),
) {
    fun validate(request: DreamProposalValidationRequest): DreamProposalValidationResult = try {
        DreamProposalValidationResult.Valid(validateOrThrow(request))
    } catch (rejection: ValidationRejection) {
        DreamProposalValidationResult.Rejected(rejection.failure)
    }

    private fun validateOrThrow(request: DreamProposalValidationRequest): DreamValidatedPlan {
        val proposal = request.proposal
        val input = request.input
        rejectUnless(proposal.schemaVersion == DREAM_PROPOSAL_SCHEMA_VERSION) {
            DreamProposalValidationFailure.SCHEMA_VERSION_MISMATCH
        }
        rejectUnless(proposal.proposalNonce == input.proposalNonce) {
            DreamProposalValidationFailure.NONCE_MISMATCH
        }
        rejectUnless(proposal.baseMemoryEpoch == input.fence.baseMemoryEpoch) {
            DreamProposalValidationFailure.MEMORY_EPOCH_MISMATCH
        }
        rejectUnless(proposal.baseDreamRevision == input.fence.baseDreamRevision) {
            DreamProposalValidationFailure.DREAM_REVISION_MISMATCH
        }
        rejectUnless(proposal.mode == input.fence.mode) { DreamProposalValidationFailure.MODE_MISMATCH }
        rejectUnless(proposal.operations.isNotEmpty()) { DreamProposalValidationFailure.INVALID_NO_OP }
        if (proposal.operations.any { it == DreamProposalOperation.NoOp }) {
            rejectUnless(proposal.operations.size == 1) { DreamProposalValidationFailure.INVALID_NO_OP }
        }

        val currentById = input.allCurrentClaims.associateBy { it.claimId }.toMutableMap()
        val upserts = mutableListOf<DreamValidatedClaimVersion>()
        val transitions = mutableListOf<DreamValidatedClaimTransition>()
        val mutated = hashSetOf<String>()
        val modelEvidencePins = mutableListOf<me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityPin>()

        input.deterministicInvalidations.forEach { invalidation ->
            val target = currentById[invalidation.claimId]
                ?: reject(DreamProposalValidationFailure.HOST_INVALIDATION_CONFLICT)
            rejectUnless(target.revision == invalidation.expectedRevision) {
                DreamProposalValidationFailure.HOST_INVALIDATION_CONFLICT
            }
            rejectUnless(mutated.add(target.claimId)) {
                DreamProposalValidationFailure.HOST_INVALIDATION_CONFLICT
            }
            val transition = transition(
                target = target,
                nextState = DreamClaimState.STALE,
                reason = when (invalidation.reason) {
                    me.rerere.rikkahub.memory.dreaming.input.DreamDeterministicInvalidationReason.SOURCE_EXPIRED ->
                        DreamClaimMutationReason.AUTHORITY_EXPIRED
                    else -> DreamClaimMutationReason.AUTHORITY_INVALIDATED
                },
            )
            transitions += transition
            currentById[target.claimId] = transition.nextVersion.toHead(input.fence.scopeId)
        }

        proposal.operations.forEach { operation ->
            when (operation) {
                DreamProposalOperation.NoOp -> Unit
                is DreamProposalOperation.UpsertClaim -> {
                    val target = operation.targetClaimToken?.let { token ->
                        input.allowedClaims[token]?.claim
                            ?: reject(DreamProposalValidationFailure.UNKNOWN_CLAIM_TOKEN)
                    }
                    if (target != null) {
                        rejectUnless(target.claimId !in mutated) {
                            DreamProposalValidationFailure.HOST_INVALIDATION_CONFLICT
                        }
                        rejectUnless(target.revision == operation.expectedClaimRevision) {
                            DreamProposalValidationFailure.TOKEN_REVISION_MISMATCH
                        }
                        requireMutableTarget(target)
                    }
                    val version = validateClaim(
                        request = request,
                        claim = operation.claim,
                        claimId = target?.claimId ?: newClaimId(),
                        previous = target,
                        reason = DreamClaimMutationReason.MODEL_PROPOSAL,
                    )
                    rejectUnless(mutated.add(version.claimId)) {
                        DreamProposalValidationFailure.CLAIM_MUTATED_TWICE
                    }
                    upserts += version
                    modelEvidencePins += version.sources.map { it.authority }
                    currentById[version.claimId] = version.toHead(input.fence.scopeId)
                }

                is DreamProposalOperation.SupersedeClaim -> {
                    val target = input.allowedClaims[operation.targetClaimToken]?.claim
                        ?: reject(DreamProposalValidationFailure.UNKNOWN_CLAIM_TOKEN)
                    rejectUnless(target.revision == operation.expectedClaimRevision) {
                        DreamProposalValidationFailure.TOKEN_REVISION_MISMATCH
                    }
                    rejectUnless(target.claimId !in mutated) {
                        DreamProposalValidationFailure.HOST_INVALIDATION_CONFLICT
                    }
                    requireMutableTarget(target)
                    rejectUnless(mutated.add(target.claimId)) {
                        DreamProposalValidationFailure.CLAIM_MUTATED_TWICE
                    }
                    val transition = transition(
                        target = target,
                        nextState = DreamClaimState.SUPERSEDED,
                        reason = DreamClaimMutationReason.SUPERSEDED_BY_PROPOSAL,
                    )
                    transitions += transition
                    currentById[target.claimId] = transition.nextVersion.toHead(input.fence.scopeId)

                    val replacement = validateClaim(
                        request = request,
                        claim = operation.replacement,
                        claimId = newClaimId(),
                        previous = null,
                        reason = DreamClaimMutationReason.SUPERSEDED_BY_PROPOSAL,
                    )
                    rejectUnless(mutated.add(replacement.claimId)) {
                        DreamProposalValidationFailure.CLAIM_MUTATED_TWICE
                    }
                    upserts += replacement
                    modelEvidencePins += replacement.sources.map { it.authority }
                    currentById[replacement.claimId] = replacement.toHead(input.fence.scopeId)
                }

                is DreamProposalOperation.InvalidateClaim -> {
                    val target = input.allowedClaims[operation.targetClaimToken]?.claim
                        ?: reject(DreamProposalValidationFailure.UNKNOWN_CLAIM_TOKEN)
                    rejectUnless(target.revision == operation.expectedClaimRevision) {
                        DreamProposalValidationFailure.TOKEN_REVISION_MISMATCH
                    }
                    rejectUnless(target.claimId !in mutated) {
                        DreamProposalValidationFailure.HOST_INVALIDATION_CONFLICT
                    }
                    requireMutableTarget(target)
                    val operationEvidence = validateEvidence(request, operation.evidence)
                    modelEvidencePins += operationEvidence.map { it.authority }
                    rejectUnless(mutated.add(target.claimId)) {
                        DreamProposalValidationFailure.CLAIM_MUTATED_TWICE
                    }
                    val transition = transition(
                        target = target,
                        nextState = DreamClaimState.INVALID,
                        reason = DreamClaimMutationReason.AUTHORITY_INVALIDATED,
                    )
                    transitions += transition
                    currentById[target.claimId] = transition.nextVersion.toHead(input.fence.scopeId)
                }
            }
        }

        val resulting = currentById.values.sortedWith(
            compareBy({ it.claimKey }, { it.claimId }, { it.revision }),
        )
        val liveKeys = resulting.filter {
            it.state in setOf(
                DreamClaimState.ACTIVE_CONTEXTUAL,
                DreamClaimState.PENDING_REVIEW,
                DreamClaimState.DIRTY,
                DreamClaimState.STALE,
            )
        }.map { it.claimKey }
        rejectUnless(liveKeys.size == liveKeys.distinct().size) {
            DreamProposalValidationFailure.DUPLICATE_CLAIM_KEY
        }
        return DreamValidatedPlan(
            fence = input.fence,
            proposalNonce = input.proposalNonce,
            upserts = upserts,
            transitions = transitions,
            resultingClaims = resulting,
            modelEvidencePins = modelEvidencePins.distinct().sortedWith(DREAM_AUTHORITY_PIN_ORDER),
        )
    }

    private fun validateClaim(
        request: DreamProposalValidationRequest,
        claim: DreamProposedClaim,
        claimId: String,
        previous: DreamClaimHead?,
        reason: DreamClaimMutationReason,
    ): DreamValidatedClaimVersion {
        val normalizedKey = normalizeClaimKey(claim.claimKeyHint)
            ?: reject(DreamProposalValidationFailure.CLAIM_KEY_INVALID)
        val evidence = validateEvidence(request, claim.evidence)
        rejectUnless(evidence.any {
            it.supportType == DreamSupportType.SUPPORTS || it.supportType == DreamSupportType.SUPERSEDES
        }) { DreamProposalValidationFailure.NO_DIRECT_PROVENANCE }
        val textRisks = contentGuard.inspect(claim.title + "\n" + claim.statement)
        val hasInputRisk = claim.evidence.any { proposed ->
            request.input.allowedMemories.getValue(proposed.memoryToken).let {
                !it.disclosureComplete || it.disclosedRiskFlags.isNotEmpty() ||
                    it.rawSourcePromptInjectionDetected
            }
        }
        val temporal = resolveTemporal(request, claim, evidence)
        val requiresReview = textRisks.isNotEmpty() || hasInputRisk || temporal.state == TemporalState.UNKNOWN ||
            claim.evidence.any { it.supportType == DreamSupportType.CONTRADICTS }
        val confidence = when {
            requiresReview -> 650
            claim.epistemicOrigin == me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicOrigin.INFERRED -> 750
            else -> 900
        }
        return DreamValidatedClaimVersion(
            claimId = claimId,
            expectedPreviousRevision = previous?.revision,
            nextRevision = (previous?.revision ?: 0L) + 1L,
            claimKey = normalizedKey,
            storageClass = claim.storageClass,
            epistemicType = claim.epistemicType,
            nextState = if (requiresReview) DreamClaimState.PENDING_REVIEW else DreamClaimState.ACTIVE_CONTEXTUAL,
            title = normalizeDreamText(claim.title).trim(),
            statement = normalizeDreamText(claim.statement).trim(),
            confidencePermille = confidence,
            temporalState = temporal.state,
            validFromEpochMs = temporal.validFromEpochMs,
            validToEpochMs = temporal.validToEpochMs,
            sources = evidence,
            reason = reason,
            subjectKind = claim.subjectKind,
            profileSection = claim.profileSection,
            epistemicOrigin = claim.epistemicOrigin,
            contentType = claim.contentType,
        )
    }

    private fun validateEvidence(
        request: DreamProposalValidationRequest,
        proposed: List<DreamProposedEvidence>,
    ): List<DreamClaimSourcePin> = proposed.map { evidence ->
        val allowed = request.input.allowedMemories[evidence.memoryToken]
            ?: reject(DreamProposalValidationFailure.UNKNOWN_MEMORY_TOKEN)
        rejectUnless(evidence.expectedRevision == allowed.memory.revision) {
            DreamProposalValidationFailure.TOKEN_REVISION_MISMATCH
        }
        validateAuthority(request, allowed)
        if (allowed.memory.requiresExactSourceRereadForSynthesis()) {
            rejectUnless(allowed.memory.sources.isNotEmpty()) {
                DreamProposalValidationFailure.AUTHORITY_SOURCE_IDENTITY_REQUIRED
            }
            rejectUnless(allowed.sourceRereadComplete) {
                DreamProposalValidationFailure.AUTHORITY_SOURCE_REREAD_INCOMPLETE
            }
        }
        DreamClaimSourcePin(allowed.pin, evidence.supportType, directAuthority = true)
    }.distinct().sortedWith(
        compareBy(
            { it.authority.memoryId },
            { it.authority.expectedRevision },
            { it.supportType.name },
        ),
    )

    private fun validateAuthority(request: DreamProposalValidationRequest, allowed: DreamAllowedMemory) {
        val memory = allowed.memory
        val pin = allowed.pin
        rejectUnless(memory.scopeId == request.input.fence.scopeId && pin.scopeId == memory.scopeId) {
            DreamProposalValidationFailure.AUTHORITY_SCOPE_MISMATCH
        }
        rejectUnless(pin.memoryId == memory.memoryId && pin.expectedRevision == memory.revision) {
            DreamProposalValidationFailure.AUTHORITY_REVISION_MISMATCH
        }
        rejectUnless(pin.expectedAuthorityFingerprint == DreamAuthorityFingerprintV1.compute(memory)) {
            DreamProposalValidationFailure.AUTHORITY_HASH_MISMATCH
        }
        rejectUnless(pin.expectedSourceManifestHash == DreamAuthorityFingerprintV1.sourceManifestHash(memory.sources)) {
            DreamProposalValidationFailure.AUTHORITY_SOURCE_MANIFEST_MISMATCH
        }
        rejectUnless(memory.lifecycleStatus == MemoryLifecycleStatus.ACTIVE && memory.truthStatus == MemoryTruthStatus.CONFIRMED) {
            DreamProposalValidationFailure.AUTHORITY_NOT_ACTIVE_CONFIRMED
        }
        rejectUnless(!memory.tombstoned) { DreamProposalValidationFailure.AUTHORITY_TOMBSTONED }
        rejectUnless(memory.expiresAtEpochMs == null || memory.expiresAtEpochMs > request.input.fence.frozenNowEpochMs) {
            DreamProposalValidationFailure.AUTHORITY_EXPIRED
        }
    }

    private fun resolveTemporal(
        request: DreamProposalValidationRequest,
        claim: DreamProposedClaim,
        evidence: List<DreamClaimSourcePin>,
    ): ResolvedTemporal {
        val sourceTimestamp = evidence.firstOrNull()?.authority?.let { pin ->
            request.input.allowedMemories.values.firstOrNull { it.pin == pin }
                ?.trustedSourceTimestampsEpochMs?.firstOrNull()
        }
        val parsed = DeterministicTemporalParser.parse(
            TemporalParseRequest(
                expression = claim.temporalExpression,
                frozenNowEpochMs = request.input.fence.frozenNowEpochMs,
                sourceTimestampEpochMs = sourceTimestamp,
                timezoneId = request.input.fence.sourceTimezoneId,
            ),
        )
        val projection = TemporalStateEngine.evaluate(
            TemporalTransitionRequest(
                parseResult = parsed,
                frozenNowEpochMs = request.input.fence.frozenNowEpochMs,
                sourceTimestampEpochMs = sourceTimestamp,
                timezoneId = request.input.fence.sourceTimezoneId,
                explicitOutcome = ExplicitTemporalOutcome.NONE,
            ),
        )
        if (claim.temporalExpression != null && parsed is TemporalParseResult.Unknown) {
            reject(DreamProposalValidationFailure.INVALID_TEMPORAL_EXPRESSION)
        }
        val window = (parsed as? TemporalParseResult.Parsed)?.window
        return ResolvedTemporal(projection.state, window?.startInclusiveEpochMs, window?.endExclusiveEpochMs)
    }

    private fun requireMutableTarget(target: DreamClaimHead) {
        rejectUnless(
            target.state !in setOf(
                DreamClaimState.TOMBSTONED,
                DreamClaimState.SUPERSEDED,
                DreamClaimState.INVALID,
                DreamClaimState.REJECTED,
            ),
        ) { DreamProposalValidationFailure.TARGET_STATE_INVALID }
    }

    private fun transition(
        target: DreamClaimHead,
        nextState: DreamClaimState,
        reason: DreamClaimMutationReason,
    ): DreamValidatedClaimTransition = DreamValidatedClaimTransition(
        expectedRevision = target.revision,
        nextVersion = DreamValidatedClaimVersion(
            claimId = target.claimId,
            expectedPreviousRevision = target.revision,
            nextRevision = target.revision + 1L,
            claimKey = target.claimKey,
            storageClass = target.storageClass,
            epistemicType = target.epistemicType,
            nextState = nextState,
            title = target.title,
            statement = target.statement,
            confidencePermille = target.confidencePermille,
            temporalState = target.temporalState,
            validFromEpochMs = target.validFromEpochMs,
            validToEpochMs = target.validToEpochMs,
            sources = target.sources,
            reason = reason,
            subjectKind = target.subjectKind,
            profileSection = target.profileSection,
            epistemicOrigin = target.epistemicOrigin,
            contentType = target.contentType,
        ),
    )

    private fun newClaimId(): String = claimIdFactory.nextClaimId().also { generated ->
        try {
            me.rerere.rikkahub.memory.dreaming.model.requireDreamStableId(generated)
        } catch (_: IllegalArgumentException) {
            reject(DreamProposalValidationFailure.CLAIM_KEY_INVALID)
        }
    }

    private fun normalizeClaimKey(raw: String): String? {
        val normalized = normalizeDreamText(raw).trim().lowercase(Locale.ROOT)
            .replace(Regex("[\\s-]+"), "_")
        return normalized.takeIf { CLAIM_KEY.matches(it) }
    }

    private fun DreamValidatedClaimVersion.toHead(
        scopeId: me.rerere.rikkahub.memory.dreaming.model.DreamScopeId,
    ): DreamClaimHead {
        val canonicalVersion = DreamClaimVersionCanonicalV1.encode(this)
        return DreamClaimHead(
            claimId = claimId,
            scopeId = scopeId,
            revision = nextRevision,
            claimKey = claimKey,
            storageClass = storageClass,
            epistemicType = epistemicType,
            state = nextState,
            title = title,
            statement = statement,
            confidencePermille = confidencePermille,
            temporalState = temporalState,
            validFromEpochMs = validFromEpochMs,
            validToEpochMs = validToEpochMs,
            versionHash = canonicalVersion.contentHash,
            sources = sources,
            subjectKind = subjectKind,
            profileSection = profileSection,
            epistemicOrigin = epistemicOrigin,
            contentType = contentType,
        )
    }

    private fun rejectUnless(condition: Boolean, failure: () -> DreamProposalValidationFailure) {
        if (!condition) reject(failure())
    }

    private fun reject(failure: DreamProposalValidationFailure): Nothing = throw ValidationRejection(failure)

    private data class ResolvedTemporal(
        val state: TemporalState,
        val validFromEpochMs: Long?,
        val validToEpochMs: Long?,
    )

    private class ValidationRejection(val failure: DreamProposalValidationFailure) : RuntimeException()

    companion object {
        private val CLAIM_KEY = Regex("^[a-z0-9][a-z0-9._:/]{0,511}$")
    }
}
