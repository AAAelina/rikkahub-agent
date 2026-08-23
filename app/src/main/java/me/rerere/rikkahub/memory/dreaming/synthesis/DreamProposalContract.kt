package me.rerere.rikkahub.memory.dreaming.synthesis

import me.rerere.rikkahub.memory.dreaming.model.DreamContentType
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicType
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicOrigin
import me.rerere.rikkahub.memory.dreaming.model.DreamOpaqueToken
import me.rerere.rikkahub.memory.dreaming.model.DreamProposalNonce
import me.rerere.rikkahub.memory.dreaming.model.DreamStorageClass
import me.rerere.rikkahub.memory.dreaming.model.DreamSubjectKind
import me.rerere.rikkahub.memory.dreaming.model.DreamSupportType
import me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisMode
import me.rerere.rikkahub.memory.dreaming.model.requireDreamValidUnicode

const val DREAM_PROMPT_CONTRACT_VERSION = "dream-pair-proposal-v2"
const val DREAM_VALIDATOR_VERSION = "dream-pair-validator-v2"

data class DreamProposalEnvelope(
    val schemaVersion: Int,
    val proposalNonce: DreamProposalNonce,
    val baseMemoryEpoch: Long,
    val baseDreamRevision: Long,
    val mode: DreamSynthesisMode,
    val operations: List<DreamProposalOperation>,
) {
    init {
        require(baseMemoryEpoch >= 0L && baseDreamRevision >= 0L)
        require(operations.size in 1..MAX_DREAM_PROPOSAL_OPERATIONS)
    }
}

sealed interface DreamProposalOperation {
    data class UpsertClaim(
        val targetClaimToken: DreamOpaqueToken?,
        val expectedClaimRevision: Long?,
        val claim: DreamProposedClaim,
    ) : DreamProposalOperation {
        init {
            require((targetClaimToken == null) == (expectedClaimRevision == null))
            require(expectedClaimRevision == null || expectedClaimRevision > 0L)
        }
    }

    data class SupersedeClaim(
        val targetClaimToken: DreamOpaqueToken,
        val expectedClaimRevision: Long,
        val replacement: DreamProposedClaim,
    ) : DreamProposalOperation {
        init {
            require(expectedClaimRevision > 0L)
        }
    }

    data class InvalidateClaim(
        val targetClaimToken: DreamOpaqueToken,
        val expectedClaimRevision: Long,
        val reason: DreamProposalInvalidationReason,
        val evidence: List<DreamProposedEvidence>,
    ) : DreamProposalOperation {
        init {
            require(expectedClaimRevision > 0L)
            require(evidence.size in 1..MAX_DREAM_EVIDENCE_PER_OPERATION)
        }
    }

    data object NoOp : DreamProposalOperation
}

data class DreamProposedClaim(
    val claimKeyHint: String,
    val subjectKind: DreamSubjectKind,
    val profileSection: String,
    val epistemicOrigin: DreamEpistemicOrigin,
    val contentType: DreamContentType,
    val title: String,
    val statement: String,
    val temporalExpression: String?,
    val evidence: List<DreamProposedEvidence>,
) {
    val storageClass: DreamStorageClass
        get() = if (contentType == DreamContentType.SHARED_HISTORY) {
            DreamStorageClass.EPISODIC
        } else {
            DreamStorageClass.PROFILE
        }

    val epistemicType: DreamEpistemicType
        get() = when (contentType) {
            DreamContentType.PROJECT_STATE -> DreamEpistemicType.PROJECT_STATE
            DreamContentType.PLAN, DreamContentType.GOAL -> DreamEpistemicType.PLAN
            DreamContentType.CONSTRAINT -> DreamEpistemicType.CONSTRAINT
            DreamContentType.PREFERENCE -> DreamEpistemicType.PREFERENCE_SUMMARY
            else -> if (epistemicOrigin == DreamEpistemicOrigin.OBSERVED) {
                DreamEpistemicType.OBSERVATION
            } else {
                DreamEpistemicType.BELIEF
            }
        }

    init {
        require(claimKeyHint.isNotBlank() && claimKeyHint.length <= 512)
        require(profileSection.matches(Regex("^[a-z0-9][a-z0-9._-]{0,63}$")))
        require(title.isNotBlank() && title.length <= 4_096)
        require(statement.isNotBlank() && statement.length <= 32_000)
        require(temporalExpression == null || temporalExpression.length <= 128)
        require(evidence.size in 1..MAX_DREAM_EVIDENCE_PER_OPERATION)
        requireDreamValidUnicode(claimKeyHint, title, statement, temporalExpression)
        require(!claimKeyHint.any(Char::isISOControl))
        require(listOf(title, statement, temporalExpression.orEmpty()).none { text ->
            text.any { it.isISOControl() && it != '\n' && it != '\t' }
        }) { "Claim text contains a prohibited control character" }
    }
}

data class DreamProposedEvidence(
    val experienceToken: DreamOpaqueToken,
    val expectedEpoch: Long,
    val supportType: DreamSupportType,
) {
    /** Compatibility names used by the established validator allowlist. */
    val memoryToken: DreamOpaqueToken get() = experienceToken
    val expectedRevision: Long get() = expectedEpoch

    init {
        require(expectedEpoch > 0L)
    }
}

enum class DreamProposalInvalidationReason {
    CONTRADICTED_BY_AUTHORITY,
    SUPERSEDED_BY_AUTHORITY,
    NO_LONGER_SUPPORTED,
}

const val MAX_DREAM_PROPOSAL_OPERATIONS = 256
const val MAX_DREAM_EVIDENCE_PER_OPERATION = 64
const val MAX_DREAM_PROPOSAL_UTF8_BYTES = 512_000
