package me.rerere.rikkahub.memory.dreaming.review

import me.rerere.rikkahub.memory.MemoryApprovalSource
import me.rerere.rikkahub.memory.MemoryKind
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimState
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicType
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamSha256
import me.rerere.rikkahub.memory.dreaming.model.DreamStorageClass
import me.rerere.rikkahub.memory.dreaming.model.DreamSupportType
import me.rerere.rikkahub.memory.dreaming.model.requireCanonicalDreamRunId
import me.rerere.rikkahub.memory.dreaming.model.requireDreamStableId
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotSection
import me.rerere.rikkahub.memory.dreaming.temporal.TemporalState

const val DREAM_CORRECTION_SOURCE_TYPE = "DREAM_USER_CORRECTION"
const val DREAM_EVIDENCE_EXCERPT_MAX_CHARS = 500
const val DREAM_REVIEW_MAX_CLAIMS = 1_024
const val DREAM_REVIEW_MAX_VERSIONS_PER_CLAIM = 256
const val DREAM_REVIEW_MAX_SOURCES_PER_VERSION = 4_096
const val DREAM_REVIEW_MAX_RECENT_RUNS = 50

enum class DreamDerivedStatus {
    EMPTY,
    RUNNING,
    DIRTY,
    READY,
    DEGRADED,
    INVALID,
}

enum class DreamUsageMode {
    OFF,
    GENERATED_ONLY,
    SHADOW,
    ACTIVE,
}

/** Complete optimistic-concurrency fence captured from one rendered scope projection. */
data class DreamReviewFence(
    val scopeId: DreamScopeId,
    val expectedMemoryEpoch: Long,
    val expectedLastAppliedMemoryEpoch: Long,
    val expectedDreamRevision: Long,
    val expectedActiveSnapshotId: String?,
) {
    init {
        require(expectedMemoryEpoch >= 0L)
        require(expectedLastAppliedMemoryEpoch in 0L..expectedMemoryEpoch)
        require(expectedDreamRevision >= 0L)
        expectedActiveSnapshotId?.let(::requireDreamStableId)
    }
}

data class DreamClaimMutationTarget(
    val fence: DreamReviewFence,
    val claimId: String,
    val expectedClaimRevision: Long,
) {
    init {
        requireDreamStableId(claimId)
        require(expectedClaimRevision > 0L)
    }
}

enum class DreamEvidenceValidity {
    VALID,
    MISSING,
    SCOPE_MISMATCH,
    REVISION_CHANGED,
    SEMANTIC_HASH_MISMATCH,
    SOURCE_MANIFEST_MISMATCH,
    TOMBSTONED,
    LIFECYCLE_INVALID,
    TRUTH_INVALID,
    EXPIRED,
    EVIDENCE_MISSING,
    CORRUPT,
}

data class DreamEvidenceReference(
    val scopeId: DreamScopeId,
    val claimId: String,
    val claimRevision: Long,
    val memoryId: String,
    val memoryRevision: Long,
    val expectedSemanticHash: DreamSha256,
    val expectedSourceManifestHash: DreamSha256,
    val supportType: DreamSupportType,
) {
    init {
        requireDreamStableId(claimId)
        require(claimRevision > 0L)
        require(memoryId.isNotBlank() && memoryId.length <= 512)
        require(memoryRevision > 0L)
    }
}

/** Deliberately contains no source text. */
data class DreamEvidenceSummary(
    val reference: DreamEvidenceReference,
    val validity: DreamEvidenceValidity,
    val sourceKind: String? = null,
    val qualityCode: String? = null,
    val excerptAvailable: Boolean = false,
) {
    init {
        require(sourceKind == null || sourceKind.length <= 128)
        require(qualityCode == null || qualityCode.length <= 128)
        require(!excerptAvailable || validity == DreamEvidenceValidity.VALID)
    }
}

data class DreamEvidenceExcerpt(
    val reference: DreamEvidenceReference,
    val text: String,
    val truncated: Boolean,
) {
    init {
        require(text.length <= DREAM_EVIDENCE_EXCERPT_MAX_CHARS)
    }
}

data class DreamClaimSummary(
    val claimId: String,
    val revision: Long,
    val section: DreamSnapshotSection,
    val state: DreamClaimState,
    val title: String,
    val statement: String,
    val confidencePermille: Int,
    val temporalState: TemporalState,
    val validFromEpochMs: Long?,
    val validToEpochMs: Long?,
    val evidenceCount: Int,
    /** Captured by the trusted projection store; never accepted from a correction form field. */
    val originAssistantId: String?,
) {
    init {
        requireDreamStableId(claimId)
        require(revision > 0L)
        require(title.isNotBlank() && statement.isNotBlank())
        require(confidencePermille in 0..1_000)
        require(evidenceCount >= 0)
        require(validFromEpochMs == null || validFromEpochMs >= 0L)
        require(validToEpochMs == null || validToEpochMs >= 0L)
        require(originAssistantId == null || originAssistantId.length <= 512)
    }
}

data class DreamClaimVersionSummary(
    val revision: Long,
    val state: DreamClaimState,
    val confidencePermille: Int,
    val temporalState: TemporalState,
    val validFromEpochMs: Long?,
    val validToEpochMs: Long?,
    val reasonCode: String,
    val createdAtEpochMs: Long,
) {
    init {
        require(revision > 0L)
        require(confidencePermille in 0..1_000)
        require(reasonCode.isNotBlank() && reasonCode.length <= 128)
        require(createdAtEpochMs >= 0L)
    }
}

data class DreamClaimDetail(
    val target: DreamClaimMutationTarget,
    val summary: DreamClaimSummary,
    val storageClass: DreamStorageClass,
    val epistemicType: DreamEpistemicType,
    val versions: List<DreamClaimVersionSummary>,
    /** Metadata only. Excerpts are available solely through revealEvidence. */
    val evidence: List<DreamEvidenceSummary>,
) {
    init {
        require(summary.claimId == target.claimId && summary.revision == target.expectedClaimRevision)
        require(versions.size <= DREAM_REVIEW_MAX_VERSIONS_PER_CLAIM)
        require(evidence.size <= DREAM_REVIEW_MAX_SOURCES_PER_VERSION)
        require(evidence.all {
            it.reference.scopeId == target.fence.scopeId &&
                it.reference.claimId == target.claimId &&
                it.reference.claimRevision == target.expectedClaimRevision
        })
    }
}

data class DreamSnapshotSummary(
    val snapshotId: String,
    val sourceMemoryEpoch: Long,
    val committedDreamRevision: Long,
    val payloadHash: DreamSha256,
    val compilerRevision: String,
    val claimCount: Int,
    val estimatedTokens: Int,
    val createdAtEpochMs: Long,
) {
    init {
        requireDreamStableId(snapshotId)
        require(sourceMemoryEpoch >= 0L && committedDreamRevision > 0L)
        require(compilerRevision.isNotBlank() && compilerRevision.length <= 64)
        require(claimCount >= 0 && estimatedTokens >= 0 && createdAtEpochMs >= 0L)
    }
}

data class DreamRunUsageSummary(
    val runId: String,
    val inputTokens: Long?,
    val outputTokens: Long?,
    val startedAtEpochMs: Long?,
    val finishedAtEpochMs: Long?,
    val statusCode: String,
) {
    init {
        requireCanonicalDreamRunId(runId)
        require(inputTokens == null || inputTokens >= 0L)
        require(outputTokens == null || outputTokens >= 0L)
        require(statusCode.isNotBlank() && statusCode.length <= 128)
        require(startedAtEpochMs == null || startedAtEpochMs >= 0L)
        require(finishedAtEpochMs == null || finishedAtEpochMs >= 0L)
        require(startedAtEpochMs == null || finishedAtEpochMs == null || finishedAtEpochMs >= startedAtEpochMs)
    }
}

data class DreamReviewProjection(
    val fence: DreamReviewFence,
    val derivedStatus: DreamDerivedStatus,
    val usageMode: DreamUsageMode,
    val claims: List<DreamClaimSummary>,
    val activeSnapshot: DreamSnapshotSummary?,
    val supersededSnapshot: DreamSnapshotSummary?,
    /** Canonically verified active-vs-superseded diff; corrupt inputs are explicitly Unavailable. */
    val snapshotDiff: DreamSnapshotDiffResult,
    val recentRuns: List<DreamRunUsageSummary>,
    val pendingExperienceCount: Int = 0,
    val experienceDebt: Double = 0.0,
) {
    init {
        require(claims.size <= DREAM_REVIEW_MAX_CLAIMS)
        require(recentRuns.size <= DREAM_REVIEW_MAX_RECENT_RUNS)
        require(claims.map(DreamClaimSummary::claimId).distinct().size == claims.size)
        require(pendingExperienceCount >= 0)
        require(experienceDebt >= 0.0)
    }
}

enum class DreamReviewConflict {
    SCOPE,
    CLAIM_REVISION,
    MEMORY_EPOCH,
    LAST_APPLIED_MEMORY_EPOCH,
    DREAM_REVISION,
    ACTIVE_SNAPSHOT,
}

sealed interface DreamReviewReadResult<out T> {
    data class Found<T>(val value: T) : DreamReviewReadResult<T>
    data class Conflict(val conflict: DreamReviewConflict) : DreamReviewReadResult<Nothing>
    data object NotFound : DreamReviewReadResult<Nothing>
    data object InvalidState : DreamReviewReadResult<Nothing>
    data object Corrupt : DreamReviewReadResult<Nothing>
}

sealed interface DreamEvidenceRevealResult {
    data class Revealed(val excerpt: DreamEvidenceExcerpt) : DreamEvidenceRevealResult
    data class Invalid(val validity: DreamEvidenceValidity) : DreamEvidenceRevealResult
    data object NotFound : DreamEvidenceRevealResult
    data object Corrupt : DreamEvidenceRevealResult
}

data class DreamRejectCommand(
    val mutationId: String,
    val target: DreamClaimMutationTarget,
    val nowEpochMs: Long,
) {
    init {
        requireDreamStableId(mutationId)
        require(nowEpochMs >= 0L)
    }
}

data class DreamClearDerivedCommand(
    val mutationId: String,
    val fence: DreamReviewFence,
    val nowEpochMs: Long,
) {
    init {
        requireDreamStableId(mutationId)
        require(nowEpochMs >= 0L)
    }
}

data class DreamCorrectionDraft(
    val target: DreamClaimMutationTarget,
    val title: String?,
    val content: String,
    val kind: MemoryKind = MemoryKind.OTHER,
    val tags: List<String> = emptyList(),
    val expiresAtEpochMs: Long? = null,
) {
    init {
        require(title == null || title.length <= 4_096)
        require(content.isNotBlank() && content.length <= 64_000)
        require(tags.size <= 256 && tags.all { it.length <= 512 })
        require(expiresAtEpochMs == null || expiresAtEpochMs >= 0L)
    }
}

/** Trusted result of validating all five scope fences plus the Claim head fence. */
data class DreamValidatedCorrectionTarget(
    val target: DreamClaimMutationTarget,
    val capturedOriginAssistantId: String?,
) {
    init {
        require(capturedOriginAssistantId == null || capturedOriginAssistantId.length <= 512)
    }
}

data class DreamAuthorityCorrectionRequest(
    val mutationId: String,
    val scopeId: DreamScopeId,
    val title: String?,
    val content: String,
    val kind: MemoryKind,
    val tags: List<String>,
    val expiresAtEpochMs: Long?,
    val capturedOriginAssistantId: String?,
    val approvalSource: MemoryApprovalSource = MemoryApprovalSource.USER_REVIEWED,
    val sourceType: String = DREAM_CORRECTION_SOURCE_TYPE,
    val confidence: Float = 1f,
) {
    init {
        requireDreamStableId(mutationId)
        require(content.isNotBlank() && content.length <= 64_000)
        require(approvalSource == MemoryApprovalSource.USER_REVIEWED)
        require(sourceType == DREAM_CORRECTION_SOURCE_TYPE)
        require(confidence == 1f)
        require(capturedOriginAssistantId == null || capturedOriginAssistantId.length <= 512)
    }
}

sealed interface DreamAuthorityCorrectionResult {
    data class Applied(
        val memoryId: Int,
        val revision: Int,
        /** Scope epoch immediately after this authority transaction committed. */
        val resultingMemoryEpoch: Long,
    ) : DreamAuthorityCorrectionResult {
        init {
            require(memoryId > 0 && revision > 0 && resultingMemoryEpoch > 0L)
        }
    }

    /**
     * The authoritative Memory transaction committed, but its resulting scope epoch could not be
     * read back safely. The caller must keep the Memory and leave Dream derived state dirty for a
     * later rebuild; it must never invite the user to repeat the authority write.
     */
    data class AppliedRebuildPending(
        val memoryId: Int,
        val revision: Int,
    ) : DreamAuthorityCorrectionResult {
        init {
            require(memoryId > 0 && revision > 0)
        }
    }

    data object Conflict : DreamAuthorityCorrectionResult
    data object NotFound : DreamAuthorityCorrectionResult
    data class Rejected(val code: String) : DreamAuthorityCorrectionResult
}

data class DreamMarkCorrectedCommand(
    val mutationId: String,
    val validatedTarget: DreamValidatedCorrectionTarget,
    val authorityMemoryId: Int,
    val authorityMemoryRevision: Int,
    /** Must be exactly the preflight epoch plus this one authority mutation. */
    val expectedAuthorityMemoryEpoch: Long,
    val nowEpochMs: Long,
) {
    init {
        requireDreamStableId(mutationId)
        require(authorityMemoryId > 0 && authorityMemoryRevision > 0)
        require(validatedTarget.target.fence.expectedMemoryEpoch < Long.MAX_VALUE)
        require(expectedAuthorityMemoryEpoch == validatedTarget.target.fence.expectedMemoryEpoch + 1L)
        require(nowEpochMs >= 0L)
    }
}

sealed interface DreamReviewStoreMutationResult {
    data class Applied(val fence: DreamReviewFence) : DreamReviewStoreMutationResult
    data class Conflict(val conflict: DreamReviewConflict) : DreamReviewStoreMutationResult
    data object NotFound : DreamReviewStoreMutationResult
    data object InvalidState : DreamReviewStoreMutationResult
    data object Corrupt : DreamReviewStoreMutationResult
    data object AlreadyClear : DreamReviewStoreMutationResult
}

sealed interface DreamReviewMutationResult {
    data class Applied(val fence: DreamReviewFence) : DreamReviewMutationResult
    data class Conflict(val conflict: DreamReviewConflict) : DreamReviewMutationResult
    data object NotFound : DreamReviewMutationResult
    data object InvalidState : DreamReviewMutationResult
    data object Corrupt : DreamReviewMutationResult
    data object AlreadyClear : DreamReviewMutationResult
}

sealed interface DreamCorrectionResult {
    data class Applied(
        val memoryId: Int,
        val memoryRevision: Int,
        val fence: DreamReviewFence,
    ) : DreamCorrectionResult

    /** Pair-Dream correction was recorded directly as an Experience, without creating Memory. */
    data class PairApplied(
        val experienceEpoch: Long,
    ) : DreamCorrectionResult

    data class AuthorityAppliedRebuildPending(
        val memoryId: Int,
        val memoryRevision: Int,
    ) : DreamCorrectionResult

    data class Conflict(val conflict: DreamReviewConflict?) : DreamCorrectionResult
    data object NotFound : DreamCorrectionResult
    data object InvalidState : DreamCorrectionResult
    data object Corrupt : DreamCorrectionResult
    data class AuthorityRejected(val code: String) : DreamCorrectionResult
}
