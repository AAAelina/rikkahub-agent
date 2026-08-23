package me.rerere.rikkahub.memory.dreaming.runtime

import me.rerere.rikkahub.memory.dreaming.model.DreamClaimState
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicType
import me.rerere.rikkahub.memory.dreaming.model.DreamPairScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamStorageClass
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotSection
import me.rerere.rikkahub.memory.dreaming.temporal.TemporalState

/**
 * V1 typed selector. It has no relation route: every accepted item is one direct Snapshot Claim.
 */
object DreamRuntimeSelector {
    fun select(request: DreamRuntimeSelectionRequest): DreamRuntimeSelectionResult {
        val projection = request.fence.projection
        val canonicalClaims = projection.claims.sortedWith(CANONICAL_CLAIM_ORDER)
        val claimsByRef = canonicalClaims.associateBy(DreamRuntimeClaimProjection::ref)
        val explicitRefs = (request.ranking as? DreamRuntimeRanking.Explicit)?.refs
        val requestFailures = buildList {
            if (request.frozenNowEpochMs < 0L) {
                add(DreamRuntimeRequestFailure.INVALID_FROZEN_NOW)
            }
            if (explicitRefs != null) {
                if (explicitRefs.size != explicitRefs.distinct().size) {
                    add(DreamRuntimeRequestFailure.DUPLICATE_RANK_REFERENCE)
                }
                if (explicitRefs.any { it !in claimsByRef }) {
                    add(DreamRuntimeRequestFailure.UNKNOWN_RANK_REFERENCE)
                }
            }
        }.distinct()
        if (requestFailures.isNotEmpty()) {
            return DreamRuntimeSelectionResult.Invalid(requestFailures)
        }

        val eligibleByRef = linkedMapOf<DreamRuntimeClaimRef, DreamRuntimeClaimProjection>()
        val droppedByRef = linkedMapOf<DreamRuntimeClaimRef, DreamRuntimeClaimDrop>()
        canonicalClaims.forEach { claim ->
            val reason = claim.ineligibilityReason(
                frozenNowEpochMs = request.frozenNowEpochMs,
                projectionScopeId = projection.scopeId,
            )
            if (reason == null) {
                eligibleByRef[claim.ref] = claim
            } else {
                droppedByRef[claim.ref] = DreamRuntimeClaimDrop(claim.ref, reason)
            }
        }

        val selected = if (explicitRefs == null) {
            eligibleByRef.values.toList()
        } else {
            val selectedRefs = explicitRefs.toSet()
            eligibleByRef.keys.filterNot { it in selectedRefs }.forEach { ref ->
                droppedByRef[ref] = DreamRuntimeClaimDrop(
                    ref = ref,
                    reason = DreamRuntimeDropReason.NOT_SELECTED_BY_RANKER,
                )
            }
            explicitRefs.mapNotNull(eligibleByRef::get)
        }

        return DreamRuntimeSelectionResult.Selected(
            DreamRuntimeSelection(
                claims = selected,
                dropped = canonicalClaims.mapNotNull { droppedByRef[it.ref] },
            ),
        )
    }

    private fun DreamRuntimeClaimProjection.ineligibilityReason(
        frozenNowEpochMs: Long,
        projectionScopeId: me.rerere.rikkahub.memory.dreaming.model.DreamScopeId,
    ): DreamRuntimeDropReason? {
        if (scopeId != projectionScopeId) return DreamRuntimeDropReason.CLAIM_SCOPE_MISMATCH
        when (currentState) {
            null -> return DreamRuntimeDropReason.CLAIM_HEAD_MISSING
            DreamClaimState.REJECTED -> return DreamRuntimeDropReason.CLAIM_REJECTED
            DreamClaimState.STALE -> return DreamRuntimeDropReason.CLAIM_STALE
            DreamClaimState.ACTIVE_CONTEXTUAL -> Unit
            else -> return DreamRuntimeDropReason.CLAIM_NOT_ACTIVE
        }
        if (currentRevision != ref.claimRevision) {
            return DreamRuntimeDropReason.CLAIM_REVISION_CHANGED
        }
        if (currentVersionHash != versionHash) {
            return DreamRuntimeDropReason.CLAIM_VERSION_HASH_CHANGED
        }
        if (snapshotState != DreamClaimState.ACTIVE_CONTEXTUAL) {
            return DreamRuntimeDropReason.SNAPSHOT_CLAIM_NOT_ACTIVE
        }
        val pairScope = DreamPairScopeId.parseOrNull(scopeId.value) != null
        if (pairScope) {
            if (section !in PAIR_RUNTIME_SECTIONS) return DreamRuntimeDropReason.SECTION_NOT_ALLOWED
        } else {
            when (epistemicType) {
                DreamEpistemicType.PREFERENCE_SUMMARY -> {
                    return DreamRuntimeDropReason.DERIVED_PREFERENCE_EXCLUDED
                }

                DreamEpistemicType.BELIEF -> return DreamRuntimeDropReason.BELIEF_EXCLUDED
                DreamEpistemicType.OBSERVATION -> {
                    return DreamRuntimeDropReason.EPISTEMIC_TYPE_UNSUPPORTED
                }

                DreamEpistemicType.PROJECT_STATE,
                DreamEpistemicType.PLAN,
                DreamEpistemicType.CONSTRAINT,
                -> Unit
            }
            if (storageClass == DreamStorageClass.PROFILE) {
                return DreamRuntimeDropReason.PROFILE_STORAGE_EXCLUDED
            }
            val expectedSection = when (epistemicType) {
                DreamEpistemicType.PROJECT_STATE -> DreamSnapshotSection.CURRENT_PROJECTS
                DreamEpistemicType.PLAN -> DreamSnapshotSection.ACTIVE_PLANS
                DreamEpistemicType.CONSTRAINT -> DreamSnapshotSection.ACTIVE_CONSTRAINTS
                else -> null
            }
            if (section !in RUNTIME_SECTIONS) return DreamRuntimeDropReason.SECTION_NOT_ALLOWED
            if (section != expectedSection) return DreamRuntimeDropReason.SECTION_TYPE_MISMATCH
        }
        if (sourceFence.validatedAtEpochMs != frozenNowEpochMs) {
            return DreamRuntimeDropReason.SOURCE_CHECK_TIME_MISMATCH
        }
        if (sourceFence.validatedClaimRevision != ref.claimRevision) {
            return DreamRuntimeDropReason.SOURCE_CHECK_REVISION_MISMATCH
        }
        if (sourceFence.directAuthoritySourceCount <= 0) {
            return DreamRuntimeDropReason.NO_DIRECT_AUTHORITY_SOURCE
        }
        if (sourceFence.directSupportingSourceCount <= 0 ||
            sourceFence.directSupportingSourceCount > sourceFence.directAuthoritySourceCount
        ) {
            return DreamRuntimeDropReason.NO_DIRECT_SUPPORTING_SOURCE
        }
        if (sourceFence.indirectDerivedSourceCount != 0) {
            return DreamRuntimeDropReason.MULTIHOP_SOURCE_EXCLUDED
        }
        if (sourceFence.validity != DreamRuntimeSourceValidity.CURRENT_CONFIRMED) {
            return DreamRuntimeDropReason.SOURCE_NOT_CURRENT
        }
        if (validFromEpochMs != null && validFromEpochMs < 0L ||
            validToEpochMs != null && validToEpochMs < 0L ||
            validFromEpochMs != null && validToEpochMs != null &&
            validToEpochMs <= validFromEpochMs
        ) {
            return DreamRuntimeDropReason.INVALID_TIME_WINDOW
        }
        if (validFromEpochMs != null && validFromEpochMs > frozenNowEpochMs) {
            return DreamRuntimeDropReason.NOT_YET_VALID
        }
        if (validToEpochMs != null && validToEpochMs <= frozenNowEpochMs) {
            return DreamRuntimeDropReason.EXPIRED
        }
        val temporalStateAllowed = if (pairScope) {
            temporalState in setOf(TemporalState.CURRENT, TemporalState.TIMELESS, TemporalState.UPCOMING)
        } else {
            when (epistemicType) {
                DreamEpistemicType.PROJECT_STATE -> temporalState == TemporalState.CURRENT
                DreamEpistemicType.PLAN -> temporalState in setOf(
                    TemporalState.CURRENT,
                    TemporalState.UPCOMING,
                )

                DreamEpistemicType.CONSTRAINT -> temporalState in setOf(
                    TemporalState.CURRENT,
                    TemporalState.TIMELESS,
                )

                else -> false
            }
        }
        if (!temporalStateAllowed) return DreamRuntimeDropReason.TEMPORAL_STATE_NOT_CURRENT
        if (!title.hasWellFormedUnicode() || !statement.hasWellFormedUnicode()) {
            return DreamRuntimeDropReason.INVALID_UNICODE
        }
        if (title.any(Char::isISOControl) || statement.any(Char::isISOControl)) {
            return DreamRuntimeDropReason.CONTROL_CHARACTER_EXCLUDED
        }
        if (title.isBlank() || statement.isBlank() || title.length > MAX_RUNTIME_TITLE_CHARS ||
            statement.length > MAX_RUNTIME_STATEMENT_CHARS || confidencePermille !in 0..1_000
        ) {
            return DreamRuntimeDropReason.CLAIM_FIELD_OUT_OF_BOUNDS
        }
        return null
    }

    private fun String.hasWellFormedUnicode(): Boolean {
        var index = 0
        while (index < length) {
            val char = this[index]
            when {
                char.isHighSurrogate() -> {
                    if (index + 1 >= length || !this[index + 1].isLowSurrogate()) return false
                    index += 2
                }

                char.isLowSurrogate() -> return false
                else -> index++
            }
        }
        return true
    }

    private val CANONICAL_CLAIM_ORDER = compareBy<DreamRuntimeClaimProjection>(
        { it.section.order },
        { it.ordinal },
        { it.ref.claimId },
        { it.ref.claimRevision },
    )
    private val RUNTIME_SECTIONS = setOf(
        DreamSnapshotSection.CURRENT_PROJECTS,
        DreamSnapshotSection.ACTIVE_PLANS,
        DreamSnapshotSection.ACTIVE_CONSTRAINTS,
    )
    private val PAIR_RUNTIME_SECTIONS = setOf(
        DreamSnapshotSection.ABOUT_USER,
        DreamSnapshotSection.ABOUT_ASSISTANT,
        DreamSnapshotSection.ABOUT_RELATIONSHIP,
    )
    private const val MAX_RUNTIME_TITLE_CHARS = 4_096
    private const val MAX_RUNTIME_STATEMENT_CHARS = 32_000
}
