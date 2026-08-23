package me.rerere.rikkahub.memory.dreaming.review

import java.security.MessageDigest
import kotlin.uuid.Uuid
import kotlinx.coroutines.flow.Flow
import me.rerere.rikkahub.memory.dreaming.experience.DreamExperienceIngestResult
import me.rerere.rikkahub.memory.dreaming.experience.DreamExperienceRecord
import me.rerere.rikkahub.memory.dreaming.experience.RoomDreamExperienceStore
import me.rerere.rikkahub.memory.dreaming.model.DreamPairScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.runtime.DreamSynthesisCoordinator

class DefaultDreamReviewRepository(
    private val store: DreamReviewStore,
    private val authority: DreamAuthorityCorrectionPort,
    private val experienceStore: RoomDreamExperienceStore? = null,
    private val synthesisCoordinator: DreamSynthesisCoordinator? = null,
    private val nowMs: () -> Long = System::currentTimeMillis,
    private val mutationIdGenerator: () -> String = { Uuid.random().toString() },
) : DreamReviewRepository {
    override fun observeScope(scopeId: DreamScopeId): Flow<DreamReviewProjection> =
        store.observeProjection(scopeId)

    override suspend fun readClaim(
        target: DreamClaimMutationTarget,
    ): DreamReviewReadResult<DreamClaimDetail> = store.readClaim(target)

    override suspend fun revealEvidence(
        reference: DreamEvidenceReference,
    ): DreamEvidenceRevealResult = store.readEvidence(
        reference = reference,
        maxChars = DREAM_EVIDENCE_EXCERPT_MAX_CHARS,
    )

    override suspend fun reject(target: DreamClaimMutationTarget): DreamReviewMutationResult {
        val mutationId = mutationIdGenerator()
        val now = nowMs()
        val pairScopeId = DreamPairScopeId.parseOrNull(target.fence.scopeId.value)
        val prior = if (pairScopeId != null) {
            when (val read = store.readClaim(target)) {
                is DreamReviewReadResult.Found -> read.value
                else -> null
            }
        } else {
            null
        }
        val result = store.reject(
            DreamRejectCommand(
                mutationId = mutationId,
                target = target,
                nowEpochMs = now,
            ),
        ).toRepositoryResult()
        if (pairScopeId != null && result is DreamReviewMutationResult.Applied) {
            experienceStore?.ingest(
                DreamExperienceRecord(
                    id = mutationIdGenerator(),
                    pairScopeId = pairScopeId,
                    sourceKind = "DREAM_REVIEW",
                    sourceRef = "user-rejection:$mutationId",
                    occurredAtMs = now,
                    actor = "USER",
                    experienceKind = "USER_REJECTION",
                    summary = prior?.summary?.let { summary ->
                        "用户明确否定此前 Dream 判断：${summary.title}：${summary.statement}"
                    } ?: "用户明确否定此前 Dream 判断（claim=${target.claimId}）。",
                    salience = 1.0,
                    novelty = 1.0,
                    identityWeight = 1.0,
                    relationshipWeight = 1.0,
                    emotionalWeight = 0.25,
                    confidence = 1.0,
                    contentDigest = "${target.claimId}:${target.expectedClaimRevision}:rejected".sha256(),
                ),
                nowMs = now,
            )
            synthesisCoordinator?.onAuthorityCommitted()
        }
        return result
    }

    override suspend fun correct(draft: DreamCorrectionDraft): DreamCorrectionResult {
        val validated = when (val result = store.validateTarget(draft.target)) {
            is DreamReviewReadResult.Found -> result.value
            is DreamReviewReadResult.Conflict -> return DreamCorrectionResult.Conflict(result.conflict)
            DreamReviewReadResult.NotFound -> return DreamCorrectionResult.NotFound
            DreamReviewReadResult.InvalidState -> return DreamCorrectionResult.InvalidState
            DreamReviewReadResult.Corrupt -> return DreamCorrectionResult.Corrupt
        }
        val mutationId = mutationIdGenerator()
        val pairScopeId = DreamPairScopeId.parseOrNull(validated.target.fence.scopeId.value)
        if (pairScopeId != null) {
            return correctPair(
                draft = draft,
                pairScopeId = pairScopeId,
                mutationId = mutationId,
            )
        }
        val authorityApplied = when (val result = authority.create(
            DreamAuthorityCorrectionRequest(
                mutationId = mutationId,
                scopeId = validated.target.fence.scopeId,
                title = draft.title,
                content = draft.content,
                kind = draft.kind,
                tags = draft.tags,
                expiresAtEpochMs = draft.expiresAtEpochMs,
                capturedOriginAssistantId = validated.capturedOriginAssistantId,
            ),
        )) {
            is DreamAuthorityCorrectionResult.Applied -> result
            is DreamAuthorityCorrectionResult.AppliedRebuildPending -> {
                return DreamCorrectionResult.AuthorityAppliedRebuildPending(
                    memoryId = result.memoryId,
                    memoryRevision = result.revision,
                )
            }
            DreamAuthorityCorrectionResult.Conflict -> return DreamCorrectionResult.Conflict(null)
            DreamAuthorityCorrectionResult.NotFound -> return DreamCorrectionResult.NotFound
            is DreamAuthorityCorrectionResult.Rejected -> {
                return DreamCorrectionResult.AuthorityRejected(result.code)
            }
        }
        val preflightEpoch = validated.target.fence.expectedMemoryEpoch
        if (preflightEpoch == Long.MAX_VALUE || authorityApplied.resultingMemoryEpoch != preflightEpoch + 1L) {
            return DreamCorrectionResult.AuthorityAppliedRebuildPending(
                memoryId = authorityApplied.memoryId,
                memoryRevision = authorityApplied.revision,
            )
        }
        val derived = store.markCorrected(
            DreamMarkCorrectedCommand(
                mutationId = mutationId,
                validatedTarget = validated,
                authorityMemoryId = authorityApplied.memoryId,
                authorityMemoryRevision = authorityApplied.revision,
                expectedAuthorityMemoryEpoch = authorityApplied.resultingMemoryEpoch,
                nowEpochMs = nowMs(),
            ),
        )
        return if (derived is DreamReviewStoreMutationResult.Applied) {
            DreamCorrectionResult.Applied(
                memoryId = authorityApplied.memoryId,
                memoryRevision = authorityApplied.revision,
                fence = derived.fence,
            )
        } else {
            // Authority is formal truth. A stale derived phase is rebuilt from the advanced epoch.
            DreamCorrectionResult.AuthorityAppliedRebuildPending(
                memoryId = authorityApplied.memoryId,
                memoryRevision = authorityApplied.revision,
            )
        }
    }

    private suspend fun correctPair(
        draft: DreamCorrectionDraft,
        pairScopeId: DreamPairScopeId,
        mutationId: String,
    ): DreamCorrectionResult {
        val experienceStore = experienceStore ?: return DreamCorrectionResult.Corrupt
        val now = nowMs()
        when (val rejected = store.reject(
            DreamRejectCommand(
                mutationId = mutationId,
                target = draft.target,
                nowEpochMs = now,
            ),
        )) {
            is DreamReviewStoreMutationResult.Applied -> Unit
            is DreamReviewStoreMutationResult.Conflict -> return DreamCorrectionResult.Conflict(rejected.conflict)
            DreamReviewStoreMutationResult.NotFound -> return DreamCorrectionResult.NotFound
            DreamReviewStoreMutationResult.InvalidState -> return DreamCorrectionResult.InvalidState
            DreamReviewStoreMutationResult.Corrupt,
            DreamReviewStoreMutationResult.AlreadyClear,
            -> return DreamCorrectionResult.Corrupt
        }
        val normalizedContent = draft.content.trim()
        val normalizedTitle = draft.title?.trim().orEmpty()
        val digestInput = buildString {
            append(draft.target.claimId)
            append(':')
            append(draft.target.expectedClaimRevision)
            append(':')
            append(normalizedTitle)
            append('\n')
            append(normalizedContent)
        }
        val result = experienceStore.ingest(
            DreamExperienceRecord(
                id = mutationId,
                pairScopeId = pairScopeId,
                sourceKind = "DREAM_REVIEW",
                sourceRef = "user-correction:$mutationId",
                occurredAtMs = now,
                actor = "USER",
                experienceKind = "USER_CORRECTION",
                summary = if (normalizedTitle.isBlank()) {
                    normalizedContent
                } else {
                    "$normalizedTitle：$normalizedContent"
                },
                salience = 1.0,
                novelty = 1.0,
                identityWeight = 1.0,
                relationshipWeight = 1.0,
                emotionalWeight = 0.5,
                confidence = 1.0,
                contentDigest = digestInput.sha256(),
            ),
            nowMs = now,
        )
        synthesisCoordinator?.onAuthorityCommitted()
        return DreamCorrectionResult.PairApplied(
            experienceEpoch = when (result) {
                is DreamExperienceIngestResult.Inserted -> result.epoch
                is DreamExperienceIngestResult.Duplicate -> result.epoch
            },
        )
    }

    override suspend fun clearDerived(fence: DreamReviewFence): DreamReviewMutationResult =
        store.clearDerived(
            DreamClearDerivedCommand(
                mutationId = mutationIdGenerator(),
                fence = fence,
                nowEpochMs = nowMs(),
            ),
        ).toRepositoryResult()
}

private fun String.sha256(): String = MessageDigest.getInstance("SHA-256")
    .digest(toByteArray(Charsets.UTF_8))
    .joinToString(separator = "") { byte -> "%02x".format(byte) }

private fun DreamReviewStoreMutationResult.toRepositoryResult(): DreamReviewMutationResult = when (this) {
    is DreamReviewStoreMutationResult.Applied -> DreamReviewMutationResult.Applied(fence)
    is DreamReviewStoreMutationResult.Conflict -> DreamReviewMutationResult.Conflict(conflict)
    DreamReviewStoreMutationResult.NotFound -> DreamReviewMutationResult.NotFound
    DreamReviewStoreMutationResult.InvalidState -> DreamReviewMutationResult.InvalidState
    DreamReviewStoreMutationResult.Corrupt -> DreamReviewMutationResult.Corrupt
    DreamReviewStoreMutationResult.AlreadyClear -> DreamReviewMutationResult.AlreadyClear
}
