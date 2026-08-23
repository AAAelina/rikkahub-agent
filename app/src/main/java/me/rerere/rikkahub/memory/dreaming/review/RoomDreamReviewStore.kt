package me.rerere.rikkahub.memory.dreaming.review

import androidx.room.withTransaction
import kotlin.math.roundToInt
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.mapLatest
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.longOrNull
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.dao.DreamDao
import me.rerere.rikkahub.data.db.dao.DreamExperienceDao
import me.rerere.rikkahub.data.db.dao.DreamReviewSourceRow
import me.rerere.rikkahub.data.db.dao.DreamSynthesisDao
import me.rerere.rikkahub.data.db.dao.MemoryDAO
import me.rerere.rikkahub.data.db.dao.MemoryV2Dao
import me.rerere.rikkahub.data.db.entity.DreamClaimEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimExperienceSourceEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimVersionEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimVersionSourceEntity
import me.rerere.rikkahub.data.db.entity.DreamExperienceStateEntity
import me.rerere.rikkahub.data.db.entity.DreamRunEntity
import me.rerere.rikkahub.data.db.entity.DreamSnapshotEntity
import me.rerere.rikkahub.data.db.entity.MemoryEntity
import me.rerere.rikkahub.data.db.entity.MemoryEvidenceEntity
import me.rerere.rikkahub.data.db.entity.MemoryRevisionEntity
import me.rerere.rikkahub.data.db.entity.MemoryScopeStateEntity
import me.rerere.rikkahub.memory.MemoryApprovalSource
import me.rerere.rikkahub.memory.MemoryAttribution
import me.rerere.rikkahub.memory.MemoryKind
import me.rerere.rikkahub.memory.MemoryLifecycleStatus
import me.rerere.rikkahub.memory.MemorySourceIdentity
import me.rerere.rikkahub.memory.MemoryTruthStatus
import me.rerere.rikkahub.memory.dreaming.experience.toDreamInputCandidate
import me.rerere.rikkahub.memory.dreaming.input.DreamInputCandidateOrigin
import me.rerere.rikkahub.memory.dreaming.model.DREAM_SNAPSHOT_SCHEMA_VERSION
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityFingerprintV1
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityMemory
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityPin
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthoritySource
import me.rerere.rikkahub.memory.dreaming.model.DreamCanonicalJson
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimHead
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimMutationReason
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimSourcePin
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimState
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimVersionCanonicalV1
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicType
import me.rerere.rikkahub.memory.dreaming.model.DreamPairScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamSha256
import me.rerere.rikkahub.memory.dreaming.model.DreamStorageClass
import me.rerere.rikkahub.memory.dreaming.model.DreamSubjectKind
import me.rerere.rikkahub.memory.dreaming.model.DreamSupportType
import me.rerere.rikkahub.memory.dreaming.model.DreamValidatedClaimVersion
import me.rerere.rikkahub.memory.dreaming.model.requireCanonicalDreamRunId
import me.rerere.rikkahub.memory.dreaming.runtime.DreamingFeatureFlagSource
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotCompileRequest
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotCompiler
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotSection
import me.rerere.rikkahub.memory.dreaming.temporal.TemporalState

/** Room-backed M5 review boundary. List projections never contain an evidence excerpt. */
class RoomDreamReviewStore(
    private val database: AppDatabase,
    private val dreamDao: DreamDao,
    private val experienceDao: DreamExperienceDao,
    private val synthesisDao: DreamSynthesisDao,
    private val memoryDao: MemoryDAO,
    private val memoryV2Dao: MemoryV2Dao,
    private val featureFlags: DreamingFeatureFlagSource,
    private val json: Json,
    private val nowEpochMs: () -> Long = System::currentTimeMillis,
) : DreamReviewStore {
    override fun observeProjection(scopeId: DreamScopeId): Flow<DreamReviewProjection> =
        if (DreamPairScopeId.parseOrNull(scopeId.value) != null) {
            experienceDao.observeState(scopeId.value)
                .mapLatest { observed ->
                    val usage = readUsageMode(scopeId)
                    try {
                        database.withTransaction {
                            buildProjection(scopeId, usage)
                        }
                    } catch (cancelled: CancellationException) {
                        throw cancelled
                    } catch (_: Exception) {
                        invalidProjection(scopeId, observed?.toReviewState(), usage)
                    }
                }
                .catch { failure ->
                    if (failure is CancellationException) throw failure
                    emit(invalidProjection(scopeId, null, DreamUsageMode.OFF))
                }
        } else {
            synthesisDao.observeReviewScopeState(scopeId.value)
                .mapLatest { observed ->
                    val usage = readUsageMode(scopeId)
                    try {
                        database.withTransaction {
                            buildProjection(scopeId, usage)
                        }
                    } catch (cancelled: CancellationException) {
                        throw cancelled
                    } catch (_: Exception) {
                        invalidProjection(scopeId, observed, usage)
                    }
                }
                .catch { failure ->
                    if (failure is CancellationException) throw failure
                    emit(invalidProjection(scopeId, null, DreamUsageMode.OFF))
                }
        }

    override suspend fun readClaim(
        target: DreamClaimMutationTarget,
    ): DreamReviewReadResult<DreamClaimDetail> = try {
        database.withTransaction {
            when (val lookup = lookupTarget(target, target.fence.expectedMemoryEpoch)) {
                is TargetLookup.Ready -> readClaimDetail(lookup.claim, target)
                is TargetLookup.Rejected -> lookup.toReadResult()
            }
        }
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (_: Exception) {
        DreamReviewReadResult.Corrupt
    }

    override suspend fun readEvidence(
        reference: DreamEvidenceReference,
        maxChars: Int,
    ): DreamEvidenceRevealResult {
        if (maxChars !in 1..DREAM_EVIDENCE_EXCERPT_MAX_CHARS) {
            return DreamEvidenceRevealResult.Corrupt
        }
        if (DreamPairScopeId.parseOrNull(reference.scopeId.value) != null) {
            return try {
                database.withTransaction { readPairEvidence(reference, maxChars) }
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                DreamEvidenceRevealResult.Corrupt
            }
        }
        return try {
            database.withTransaction {
                val memoryId = reference.memoryId.toIntOrNull()
                    ?: return@withTransaction DreamEvidenceRevealResult.Corrupt
                val memoryRevision = reference.memoryRevision.toIntExactOrNull()
                    ?: return@withTransaction DreamEvidenceRevealResult.Corrupt
                val claim = synthesisDao.getClaim(reference.claimId, reference.scopeId.value)
                    ?: return@withTransaction when (synthesisDao.findClaimScopeId(reference.claimId)) {
                        null -> DreamEvidenceRevealResult.NotFound
                        else -> DreamEvidenceRevealResult.Invalid(DreamEvidenceValidity.SCOPE_MISMATCH)
                    }
                if (claim.claimRevision != reference.claimRevision) {
                    return@withTransaction DreamEvidenceRevealResult.Invalid(
                        DreamEvidenceValidity.REVISION_CHANGED,
                    )
                }
                val source = synthesisDao.getScopedClaimVersionSource(
                    scopeId = reference.scopeId.value,
                    claimId = reference.claimId,
                    claimRevision = reference.claimRevision,
                    memoryId = memoryId,
                    memoryRevision = memoryRevision,
                    supportType = reference.supportType.name,
                ) ?: return@withTransaction DreamEvidenceRevealResult.NotFound
                if (source.memorySemanticHash != reference.expectedSemanticHash.value) {
                    return@withTransaction DreamEvidenceRevealResult.Invalid(
                        DreamEvidenceValidity.SEMANTIC_HASH_MISMATCH,
                    )
                }
                val revealNowMs = nowEpochMs()
                if (revealNowMs < 0L) return@withTransaction DreamEvidenceRevealResult.Corrupt
                val checked = checkExactEvidence(reference, source, revealNowMs)
                if (checked.validity != DreamEvidenceValidity.VALID) {
                    return@withTransaction DreamEvidenceRevealResult.Invalid(checked.validity)
                }
                val text = checked.excerpt?.takeIf(String::isNotEmpty)
                    ?: return@withTransaction DreamEvidenceRevealResult.Invalid(
                        DreamEvidenceValidity.EVIDENCE_MISSING,
                    )
                DreamEvidenceRevealResult.Revealed(
                    DreamEvidenceExcerpt(
                        reference = reference,
                        text = text.take(maxChars),
                        truncated = text.length > maxChars,
                    ),
                )
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            DreamEvidenceRevealResult.Corrupt
        }
    }

    override suspend fun validateTarget(
        target: DreamClaimMutationTarget,
    ): DreamReviewReadResult<DreamValidatedCorrectionTarget> = try {
        database.withTransaction {
            when (val lookup = lookupTarget(target, target.fence.expectedMemoryEpoch)) {
                is TargetLookup.Rejected -> lookup.toReadResult()
                is TargetLookup.Ready -> {
                    if (!lookup.claim.isUserReviewable()) {
                        DreamReviewReadResult.InvalidState
                    } else {
                        val detail = readClaimDetail(lookup.claim, target)
                        when (detail) {
                            is DreamReviewReadResult.Found -> DreamReviewReadResult.Found(
                                DreamValidatedCorrectionTarget(
                                    target = target,
                                    capturedOriginAssistantId = detail.value.summary.originAssistantId,
                                ),
                            )
                            is DreamReviewReadResult.Conflict -> detail
                            DreamReviewReadResult.NotFound -> DreamReviewReadResult.NotFound
                            DreamReviewReadResult.InvalidState -> DreamReviewReadResult.InvalidState
                            DreamReviewReadResult.Corrupt -> DreamReviewReadResult.Corrupt
                        }
                    }
                }
            }
        }
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (_: Exception) {
        DreamReviewReadResult.Corrupt
    }

    override suspend fun reject(command: DreamRejectCommand): DreamReviewStoreMutationResult =
        mutateOrReject {
            val target = command.target
            val lookup = lookupTarget(target, target.fence.expectedMemoryEpoch)
            if (lookup is TargetLookup.Rejected) return@mutateOrReject lookup.toMutationResult()
            lookup as TargetLookup.Ready
            if (!lookup.claim.isUserReviewable()) return@mutateOrReject DreamReviewStoreMutationResult.InvalidState
            val current = loadCurrentClaim(lookup.claim, target.fence.scopeId)
                ?: abortMutation(DreamReviewStoreMutationResult.Corrupt)
            val next = current.head.toNextVersion(
                state = DreamClaimState.REJECTED,
                reason = DreamClaimMutationReason.USER_REJECTED,
                sources = current.head.sources,
            )
            persistReviewMutation(
                mutationId = command.mutationId,
                target = target,
                stateBefore = lookup.state,
                currentMemoryEpoch = target.fence.expectedMemoryEpoch,
                nextVersion = next,
                sourceRows = current.sourceRows.map { row ->
                    row.copyForRevision(next.nextRevision, command.nowEpochMs)
                },
                nowMs = command.nowEpochMs,
                reason = REVIEW_REJECT_REASON,
            )
        }

    override suspend fun markCorrected(
        command: DreamMarkCorrectedCommand,
    ): DreamReviewStoreMutationResult = mutateOrReject {
        val target = command.validatedTarget.target
        val lookup = lookupTarget(target, command.expectedAuthorityMemoryEpoch)
        if (lookup is TargetLookup.Rejected) return@mutateOrReject lookup.toMutationResult()
        lookup as TargetLookup.Ready
        if (!lookup.claim.isUserReviewable()) return@mutateOrReject DreamReviewStoreMutationResult.InvalidState
        val expectedOldEpoch = target.fence.expectedMemoryEpoch
        if (expectedOldEpoch == Long.MAX_VALUE || command.expectedAuthorityMemoryEpoch != expectedOldEpoch + 1L) {
            return@mutateOrReject DreamReviewStoreMutationResult.Conflict(DreamReviewConflict.MEMORY_EPOCH)
        }
        val current = loadCurrentClaim(lookup.claim, target.fence.scopeId)
            ?: abortMutation(DreamReviewStoreMutationResult.Corrupt)
        val capturedOrigin = current.sourceRows
            .map(DreamReviewSourceRow::currentOriginAssistantId)
            .distinct()
            .singleOrNull()
        if (capturedOrigin != command.validatedTarget.capturedOriginAssistantId) {
            abortMutation(DreamReviewStoreMutationResult.Corrupt)
        }
        val correction = exactCorrectionPin(command, target.fence.scopeId)
            ?: abortMutation(DreamReviewStoreMutationResult.Corrupt)
        val next = current.head.toNextVersion(
            state = DreamClaimState.SUPERSEDED,
            reason = DreamClaimMutationReason.USER_CORRECTION,
            sources = listOf(
                DreamClaimSourcePin(
                    authority = correction,
                    supportType = DreamSupportType.SUPERSEDES,
                    directAuthority = true,
                ),
            ),
        )
        persistReviewMutation(
            mutationId = command.mutationId,
            target = target,
            stateBefore = lookup.state,
            currentMemoryEpoch = command.expectedAuthorityMemoryEpoch,
            nextVersion = next,
            sourceRows = listOf(
                DreamClaimVersionSourceEntity(
                    claimId = target.claimId,
                    claimRevision = next.nextRevision,
                    memoryId = command.authorityMemoryId,
                    memoryRevision = command.authorityMemoryRevision,
                    memorySemanticHash = correction.expectedAuthorityFingerprint.value,
                    memoryEvidenceId = null,
                    supportType = DreamSupportType.SUPERSEDES.name,
                    createdAtMs = command.nowEpochMs,
                ),
            ),
            nowMs = command.nowEpochMs,
            reason = REVIEW_CORRECTION_REASON,
        )
    }

    override suspend fun clearDerived(
        command: DreamClearDerivedCommand,
    ): DreamReviewStoreMutationResult = mutateOrReject {
        val state = reviewScopeState(command.fence.scopeId)
            ?: return@mutateOrReject DreamReviewStoreMutationResult.AlreadyClear
        compareFence(state, command.fence)?.let { conflict ->
            return@mutateOrReject DreamReviewStoreMutationResult.Conflict(conflict)
        }
        val claimCount = synthesisDao.countDerivedClaimsForScope(command.fence.scopeId.value)
        val snapshotCount = synthesisDao.countDerivedSnapshotsForScope(command.fence.scopeId.value)
        val hasDerivedState = claimCount > 0 || snapshotCount > 0 || state.activeSnapshotId != null ||
            state.lastAppliedMemoryEpoch != 0L || state.lastFullRebuildAtMs != null
        if (!hasDerivedState) return@mutateOrReject DreamReviewStoreMutationResult.AlreadyClear
        val cleared = if (DreamPairScopeId.parseOrNull(command.fence.scopeId.value) != null) {
            experienceDao.advancePairClearDerivedCas(
                pairScopeId = command.fence.scopeId.value,
                expectedExperienceEpoch = command.fence.expectedMemoryEpoch,
                expectedAppliedExperienceEpoch = command.fence.expectedLastAppliedMemoryEpoch,
                expectedProfileRevision = command.fence.expectedDreamRevision,
                expectedActiveSnapshotId = command.fence.expectedActiveSnapshotId,
                reasonCode = REVIEW_CLEAR_REASON,
                nowMs = command.nowEpochMs,
            )
        } else {
            synthesisDao.advanceClearDerivedCas(
                scopeId = command.fence.scopeId.value,
                expectedMemoryEpoch = command.fence.expectedMemoryEpoch,
                expectedLastAppliedMemoryEpoch = command.fence.expectedLastAppliedMemoryEpoch,
                expectedDreamRevision = command.fence.expectedDreamRevision,
                expectedActiveSnapshotId = command.fence.expectedActiveSnapshotId,
                reasonCode = REVIEW_CLEAR_REASON,
                nowMs = command.nowEpochMs,
            )
        }
        if (cleared != 1) {
            abortMutation(classifyFenceMutation(command.fence))
        }
        synthesisDao.deleteDerivedSourcesForScope(command.fence.scopeId.value)
        synthesisDao.deleteDerivedClaimsForScope(command.fence.scopeId.value)
        synthesisDao.deleteDerivedSnapshotsForScope(command.fence.scopeId.value)
        if (synthesisDao.countDerivedClaimsForScope(command.fence.scopeId.value) != 0 ||
            synthesisDao.countDerivedSnapshotsForScope(command.fence.scopeId.value) != 0
        ) {
            abortMutation(DreamReviewStoreMutationResult.Corrupt)
        }
        DreamReviewStoreMutationResult.Applied(
            DreamReviewFence(
                scopeId = command.fence.scopeId,
                expectedMemoryEpoch = command.fence.expectedMemoryEpoch,
                expectedLastAppliedMemoryEpoch = 0L,
                expectedDreamRevision = command.fence.expectedDreamRevision + 1L,
                expectedActiveSnapshotId = null,
            ),
        )
    }

    private suspend fun buildProjection(
        scopeId: DreamScopeId,
        usageMode: DreamUsageMode,
    ): DreamReviewProjection {
        val state = reviewScopeState(scopeId)
            ?: return emptyProjection(scopeId, usageMode)
        val fence = state.toReviewFence(scopeId) ?: return invalidProjection(scopeId, state, usageMode)
        val projectionNowMs = nowEpochMs()
        if (projectionNowMs < 0L) return invalidProjection(scopeId, state, usageMode)
        if (DreamPairScopeId.parseOrNull(scopeId.value) != null) {
            return buildPairProjection(scopeId, usageMode, state, fence, projectionNowMs)
        }
        val claims = synthesisDao.listClaims(scopeId.value, DREAM_REVIEW_MAX_CLAIMS + 1)
        if (claims.size > DREAM_REVIEW_MAX_CLAIMS) return invalidProjection(scopeId, state, usageMode)
        val headVersions = synthesisDao.listReviewHeadVersions(scopeId.value, claims.size + 1)
        if (headVersions.size != claims.size) return invalidProjection(scopeId, state, usageMode)
        val sourceRows = synthesisDao.listReviewSourceRows(
            scopeId = scopeId.value,
            claimId = null,
            headOnly = true,
            limit = MAX_REVIEW_PROJECTION_SOURCES + 1,
        )
        if (sourceRows.size > MAX_REVIEW_PROJECTION_SOURCES) {
            return invalidProjection(scopeId, state, usageMode)
        }
        val versionsByClaim = headVersions.associateBy(DreamClaimVersionEntity::claimId)
        val rowsByClaim = sourceRows.groupBy(DreamReviewSourceRow::claimId)
        var degraded = false
        val summaries = mutableListOf<DreamClaimSummary>()
        val activeHeads = mutableListOf<DreamClaimHead>()
        for (claim in claims.sortedBy(DreamClaimEntity::claimId)) {
            val rows = rowsByClaim[claim.claimId].orEmpty()
            if (rows.size > DREAM_REVIEW_MAX_SOURCES_PER_VERSION) {
                return invalidProjection(scopeId, state, usageMode)
            }
            if (claim.state == DreamClaimState.TOMBSTONED.name) {
                if (claim.title.isNotEmpty() || claim.statement.isNotEmpty() || rows.isNotEmpty()) {
                    return invalidProjection(scopeId, state, usageMode)
                }
                continue
            }
            val parsed = parseVersion(versionsByClaim[claim.claimId] ?: return invalidProjection(
                scopeId,
                state,
                usageMode,
            ), rows, scopeId) ?: return invalidProjection(scopeId, state, usageMode)
            val head = parsed.toHead(scopeId)
            if (!claim.matches(head)) return invalidProjection(scopeId, state, usageMode)
            val checks = rows.map { it.evidenceCheck(scopeId, projectionNowMs) }
            if (checks.any { it.validity != DreamEvidenceValidity.VALID }) degraded = true
            val origin = rows.map(DreamReviewSourceRow::currentOriginAssistantId).distinct()
                .singleOrNull()
            summaries += head.toSummary(rows.size, origin)
            if (head.state == DreamClaimState.ACTIVE_CONTEXTUAL) activeHeads += head
        }
        val active = state.activeSnapshotId?.let { snapshotId ->
            synthesisDao.getSnapshot(snapshotId, scopeId.value)
                ?: return invalidProjection(scopeId, state, usageMode)
        }
        if (active != null && !active.matchesProjectionState(state, state.memoryEpoch)) {
            return invalidProjection(scopeId, state, usageMode)
        }
        if (active == null && summaries.isNotEmpty()) degraded = true
        if (active != null && active.claimCount != activeHeads.size) {
            return invalidProjection(scopeId, state, usageMode)
        }
        if (active != null) {
            val expected = try {
                DreamSnapshotCompiler.compile(
                    DreamSnapshotCompileRequest(
                        scopeId = scopeId,
                        compilerRevision = active.compilerRevision,
                        claims = activeHeads,
                    ),
                )
            } catch (_: Exception) {
                return invalidProjection(scopeId, state, usageMode)
            }
            if (active.canonicalPayloadJson != expected.payloadJson ||
                active.payloadSha256 != expected.payloadHash.value ||
                active.claimCount != expected.claimCount ||
                active.estimatedTokens != expected.estimatedTokens
            ) {
                return invalidProjection(scopeId, state, usageMode)
            }
        }
        val superseded = active?.supersedesSnapshotId?.let { previousId ->
            synthesisDao.getSnapshot(previousId, scopeId.value)
                ?: return invalidProjection(scopeId, state, usageMode)
        }
        if (superseded != null && superseded.status != SNAPSHOT_SUPERSEDED) {
            return invalidProjection(scopeId, state, usageMode)
        }
        val diff = snapshotDiff(scopeId, superseded, active)
        if (diff is DreamSnapshotDiffResult.Unavailable) {
            return invalidProjection(scopeId, state, usageMode, diff)
        }
        if (active != null && active.manifestReferencesOrNull() !=
            activeHeads.map { head -> head.claimId to head.revision }.toSet()
        ) {
            return invalidProjection(scopeId, state, usageMode)
        }
        val runs = dreamDao.listRecentRuns(scopeId.value, DREAM_REVIEW_MAX_RECENT_RUNS + 1)
        if (runs.size > DREAM_REVIEW_MAX_RECENT_RUNS || !runsAreConsistent(state, runs)) {
            return invalidProjection(scopeId, state, usageMode, diff)
        }
        val hasLiveRun = state.activeRunId != null &&
            state.activeRunLeaseUntilMs?.let { leaseUntil -> leaseUntil > projectionNowMs } == true
        if (state.activeRunId != null && !hasLiveRun) degraded = true
        val empty = summaries.isEmpty() && active == null && state.activeRunId == null
        val status = when {
            hasLiveRun -> DreamDerivedStatus.RUNNING
            state.memoryEpoch != state.lastAppliedMemoryEpoch -> DreamDerivedStatus.DIRTY
            degraded -> DreamDerivedStatus.DEGRADED
            empty -> DreamDerivedStatus.EMPTY
            else -> DreamDerivedStatus.READY
        }
        val runSummaries = runs.mapNotNull(DreamRunEntity::toUsageSummaryOrNull)
        if (runSummaries.size != runs.size) return invalidProjection(scopeId, state, usageMode, diff)
        val activeSummary = active?.toSummary()
        if (active != null && activeSummary == null) return invalidProjection(scopeId, state, usageMode, diff)
        val supersededSummary = superseded?.toSummary()
        if (superseded != null && supersededSummary == null) {
            return invalidProjection(scopeId, state, usageMode, diff)
        }
        return DreamReviewProjection(
            fence = fence,
            derivedStatus = status,
            usageMode = usageMode,
            claims = summaries,
            activeSnapshot = activeSummary,
            supersededSnapshot = supersededSummary,
            snapshotDiff = diff,
            recentRuns = runSummaries,
        )
    }

    /** Pair-Dream provenance is Experience-backed, so the legacy Memory evidence join is skipped. */
    private suspend fun buildPairProjection(
        scopeId: DreamScopeId,
        usageMode: DreamUsageMode,
        state: MemoryScopeStateEntity,
        fence: DreamReviewFence,
        projectionNowMs: Long,
    ): DreamReviewProjection {
        val claims = synthesisDao.listClaims(scopeId.value, DREAM_REVIEW_MAX_CLAIMS + 1)
        if (claims.size > DREAM_REVIEW_MAX_CLAIMS) return invalidProjection(scopeId, state, usageMode)
        val heads = loadAllHeadsForCompile(scopeId)
            ?: return invalidProjection(scopeId, state, usageMode)
        val headById = heads.associateBy(DreamClaimHead::claimId)
        val summaries = mutableListOf<DreamClaimSummary>()
        for (claim in claims.sortedBy(DreamClaimEntity::claimId)) {
            if (claim.state == DreamClaimState.TOMBSTONED.name) continue
            val head = headById[claim.claimId] ?: return invalidProjection(scopeId, state, usageMode)
            val sourceCount = synthesisDao.listClaimExperienceSources(claim.claimId, claim.claimRevision).size
            if (sourceCount > DREAM_REVIEW_MAX_SOURCES_PER_VERSION) {
                return invalidProjection(scopeId, state, usageMode)
            }
            summaries += head.toSummary(sourceCount, null)
        }
        val activeHeads = heads.filter { it.state == DreamClaimState.ACTIVE_CONTEXTUAL }
        var degraded = false
        val active = state.activeSnapshotId?.let { snapshotId ->
            synthesisDao.getSnapshot(snapshotId, scopeId.value)
                ?: return invalidProjection(scopeId, state, usageMode)
        }
        if (active != null && !active.matchesProjectionState(state, state.memoryEpoch)) {
            return invalidProjection(scopeId, state, usageMode)
        }
        if (active == null && summaries.isNotEmpty()) degraded = true
        if (active != null && active.claimCount != activeHeads.size) {
            return invalidProjection(scopeId, state, usageMode)
        }
        if (active != null) {
            val expected = try {
                DreamSnapshotCompiler.compile(
                    DreamSnapshotCompileRequest(
                        scopeId = scopeId,
                        compilerRevision = active.compilerRevision,
                        claims = activeHeads,
                    ),
                )
            } catch (_: Exception) {
                return invalidProjection(scopeId, state, usageMode)
            }
            if (active.canonicalPayloadJson != expected.payloadJson ||
                active.payloadSha256 != expected.payloadHash.value ||
                active.claimCount != expected.claimCount ||
                active.estimatedTokens != expected.estimatedTokens
            ) {
                return invalidProjection(scopeId, state, usageMode)
            }
        }
        val superseded = active?.supersedesSnapshotId?.let { previousId ->
            synthesisDao.getSnapshot(previousId, scopeId.value)
                ?: return invalidProjection(scopeId, state, usageMode)
        }
        if (superseded != null && superseded.status != SNAPSHOT_SUPERSEDED) {
            return invalidProjection(scopeId, state, usageMode)
        }
        val diff = snapshotDiff(scopeId, superseded, active)
        if (diff is DreamSnapshotDiffResult.Unavailable) {
            return invalidProjection(scopeId, state, usageMode, diff)
        }
        if (active != null && active.manifestReferencesOrNull() !=
            activeHeads.map { head -> head.claimId to head.revision }.toSet()
        ) {
            return invalidProjection(scopeId, state, usageMode)
        }
        val runs = dreamDao.listRecentRuns(scopeId.value, DREAM_REVIEW_MAX_RECENT_RUNS + 1)
        if (runs.size > DREAM_REVIEW_MAX_RECENT_RUNS || !runsAreConsistent(state, runs)) {
            return invalidProjection(scopeId, state, usageMode, diff)
        }
        val hasLiveRun = state.activeRunId != null &&
            state.activeRunLeaseUntilMs?.let { leaseUntil -> leaseUntil > projectionNowMs } == true
        if (state.activeRunId != null && !hasLiveRun) degraded = true
        val empty = summaries.isEmpty() && active == null && state.activeRunId == null
        val status = when {
            hasLiveRun -> DreamDerivedStatus.RUNNING
            state.memoryEpoch != state.lastAppliedMemoryEpoch -> DreamDerivedStatus.DIRTY
            degraded -> DreamDerivedStatus.DEGRADED
            empty -> DreamDerivedStatus.EMPTY
            else -> DreamDerivedStatus.READY
        }
        val runSummaries = runs.mapNotNull(DreamRunEntity::toUsageSummaryOrNull)
        if (runSummaries.size != runs.size) return invalidProjection(scopeId, state, usageMode, diff)
        val activeSummary = active?.toSummary()
        if (active != null && activeSummary == null) return invalidProjection(scopeId, state, usageMode, diff)
        val supersededSummary = superseded?.toSummary()
        if (superseded != null && supersededSummary == null) {
            return invalidProjection(scopeId, state, usageMode, diff)
        }
        val pairState = experienceDao.getState(scopeId.value)
            ?: return invalidProjection(scopeId, state, usageMode, diff)
        return DreamReviewProjection(
            fence = fence,
            derivedStatus = status,
            usageMode = usageMode,
            claims = summaries,
            activeSnapshot = activeSummary,
            supersededSnapshot = supersededSummary,
            snapshotDiff = diff,
            recentRuns = runSummaries,
            pendingExperienceCount = experienceDao.countPending(
                scopeId.value,
                pairState.appliedExperienceEpoch,
            ),
            experienceDebt = pairState.experienceDebt,
        )
    }

    private suspend fun readClaimDetail(
        claim: DreamClaimEntity,
        target: DreamClaimMutationTarget,
    ): DreamReviewReadResult<DreamClaimDetail> {
        if (claim.state == DreamClaimState.TOMBSTONED.name) return DreamReviewReadResult.InvalidState
        if (DreamPairScopeId.parseOrNull(target.fence.scopeId.value) != null) {
            val head = claim.toPairHead(target.fence.scopeId) ?: return DreamReviewReadResult.Corrupt
            val version = synthesisDao.getClaimVersion(claim.claimId, claim.claimRevision)
                ?: return DreamReviewReadResult.Corrupt
            val pairSources = synthesisDao.listClaimExperienceSources(claim.claimId, claim.claimRevision)
            if (pairSources.size > DREAM_REVIEW_MAX_SOURCES_PER_VERSION) return DreamReviewReadResult.Corrupt
            val evidence = pairSources.map { source ->
                val experience = experienceDao.getExperience(source.experienceId, target.fence.scopeId.value)
                    ?: return DreamReviewReadResult.Corrupt
                val candidate = experience.toDreamInputCandidate(
                    scopeId = target.fence.scopeId,
                    origin = DreamInputCandidateOrigin.AUTHORITY_CHANGE,
                    json = json,
                ) ?: return DreamReviewReadResult.Corrupt
                if (source.experienceEpoch != experience.experienceEpoch ||
                    source.contentDigest != experience.contentDigest ||
                    source.sourceManifestHash != candidate.pin.expectedSourceManifestHash.value
                ) return DreamReviewReadResult.Corrupt
                DreamEvidenceSummary(
                    reference = DreamEvidenceReference(
                        scopeId = target.fence.scopeId,
                        claimId = claim.claimId,
                        claimRevision = claim.claimRevision,
                        memoryId = experience.experienceId,
                        memoryRevision = experience.experienceEpoch,
                        expectedSemanticHash = candidate.pin.expectedAuthorityFingerprint,
                        expectedSourceManifestHash = candidate.pin.expectedSourceManifestHash,
                        supportType = runCatching { enumValueOf<DreamSupportType>(source.supportType) }.getOrNull()
                            ?: return DreamReviewReadResult.Corrupt,
                    ),
                    validity = DreamEvidenceValidity.VALID,
                    sourceKind = experience.sourceKind,
                    qualityCode = experience.experienceKind,
                    excerptAvailable = true,
                )
            }
            return DreamReviewReadResult.Found(
                DreamClaimDetail(
                    target = target,
                    summary = head.toSummary(pairSources.size, null),
                    storageClass = head.storageClass,
                    epistemicType = head.epistemicType,
                    versions = listOf(
                        DreamClaimVersionSummary(
                            revision = head.revision,
                            state = head.state,
                            confidencePermille = head.confidencePermille,
                            temporalState = head.temporalState,
                            validFromEpochMs = head.validFromEpochMs,
                            validToEpochMs = head.validToEpochMs,
                            reasonCode = version.reasonCode,
                            createdAtEpochMs = version.createdAtMs,
                        ),
                    ),
                    evidence = evidence,
                ),
            )
        }
        val versions = synthesisDao.listReviewClaimVersions(
            claimId = claim.claimId,
            scopeId = target.fence.scopeId.value,
            limit = DREAM_REVIEW_MAX_VERSIONS_PER_CLAIM + 1,
        )
        if (versions.isEmpty() || versions.size > DREAM_REVIEW_MAX_VERSIONS_PER_CLAIM ||
            claim.claimRevision != versions.size.toLong() ||
            versions.withIndex().any { (index, version) ->
                version.claimRevision != index.toLong() + 1L
            }
        ) return DreamReviewReadResult.Corrupt
        val rows = synthesisDao.listReviewSourceRows(
            scopeId = target.fence.scopeId.value,
            claimId = claim.claimId,
            headOnly = false,
            limit = MAX_REVIEW_DETAIL_SOURCES + 1,
        )
        if (rows.size > MAX_REVIEW_DETAIL_SOURCES) return DreamReviewReadResult.Corrupt
        val grouped = rows.groupBy(DreamReviewSourceRow::claimRevision)
        if (grouped.values.any { it.size > DREAM_REVIEW_MAX_SOURCES_PER_VERSION }) {
            return DreamReviewReadResult.Corrupt
        }
        val parsed = versions.map { version ->
            parseVersion(version, grouped[version.claimRevision].orEmpty(), target.fence.scopeId)
                ?: return DreamReviewReadResult.Corrupt
        }
        val current = parsed.last()
        val head = current.toHead(target.fence.scopeId)
        if (!claim.matches(head)) return DreamReviewReadResult.Corrupt
        val currentRows = grouped[claim.claimRevision].orEmpty()
        val detailNowMs = nowEpochMs()
        if (detailNowMs < 0L) return DreamReviewReadResult.Corrupt
        val evidence = currentRows.map { row ->
            val checked = row.evidenceCheck(target.fence.scopeId, detailNowMs)
            DreamEvidenceSummary(
                reference = row.toReference(target.fence.scopeId) ?: return DreamReviewReadResult.Corrupt,
                validity = checked.validity,
                sourceKind = checked.sourceKind,
                qualityCode = checked.qualityCode,
                excerptAvailable = checked.validity == DreamEvidenceValidity.VALID &&
                    !checked.excerpt.isNullOrEmpty(),
            )
        }
        val origins = currentRows.map(DreamReviewSourceRow::currentOriginAssistantId).distinct()
        return DreamReviewReadResult.Found(
            DreamClaimDetail(
                target = target,
                summary = head.toSummary(currentRows.size, origins.singleOrNull()),
                storageClass = head.storageClass,
                epistemicType = head.epistemicType,
                versions = parsed.mapIndexed { index, version ->
                    DreamClaimVersionSummary(
                        revision = version.version.nextRevision,
                        state = version.version.nextState,
                        confidencePermille = version.version.confidencePermille,
                        temporalState = version.version.temporalState,
                        validFromEpochMs = version.version.validFromEpochMs,
                        validToEpochMs = version.version.validToEpochMs,
                        reasonCode = version.version.reason.name,
                        createdAtEpochMs = versions[index].createdAtMs,
                    )
                }.asReversed(),
                evidence = evidence,
            ),
        )
    }

    private suspend fun persistReviewMutation(
        mutationId: String,
        target: DreamClaimMutationTarget,
        stateBefore: MemoryScopeStateEntity,
        currentMemoryEpoch: Long,
        nextVersion: DreamValidatedClaimVersion,
        sourceRows: List<DreamClaimVersionSourceEntity>,
        nowMs: Long,
        reason: String,
    ): DreamReviewStoreMutationResult {
        val canonical = DreamClaimVersionCanonicalV1.encode(nextVersion)
        val pairScope = DreamPairScopeId.parseOrNull(target.fence.scopeId.value) != null
        val claimAdvanced = if (pairScope) {
            experienceDao.advanceClaimForPairUserReviewCas(
                pairScopeId = target.fence.scopeId.value,
                claimId = target.claimId,
                expectedClaimRevision = target.expectedClaimRevision,
                nextClaimRevision = nextVersion.nextRevision,
                currentExperienceEpoch = currentMemoryEpoch,
                expectedAppliedExperienceEpoch = target.fence.expectedLastAppliedMemoryEpoch,
                expectedProfileRevision = target.fence.expectedDreamRevision,
                expectedActiveSnapshotId = target.fence.expectedActiveSnapshotId,
                nextState = nextVersion.nextState.name,
                claimHash = canonical.contentHash.value,
                mutationId = mutationId,
                reasonCode = reason,
                nowMs = nowMs,
            )
        } else {
            synthesisDao.advanceClaimForUserReviewCas(
                scopeId = target.fence.scopeId.value,
                claimId = target.claimId,
                expectedClaimRevision = target.expectedClaimRevision,
                nextClaimRevision = nextVersion.nextRevision,
                currentMemoryEpoch = currentMemoryEpoch,
                expectedLastAppliedMemoryEpoch = target.fence.expectedLastAppliedMemoryEpoch,
                expectedDreamRevision = target.fence.expectedDreamRevision,
                expectedActiveSnapshotId = target.fence.expectedActiveSnapshotId,
                nextState = nextVersion.nextState.name,
                claimHash = canonical.contentHash.value,
                mutationId = mutationId,
                reasonCode = reason,
                nowMs = nowMs,
            )
        }
        if (claimAdvanced != 1) {
            abortMutation(classifyTargetMutation(target, currentMemoryEpoch))
        }
        synthesisDao.insertClaimVersion(
            DreamClaimVersionEntity(
                claimId = nextVersion.claimId,
                claimRevision = nextVersion.nextRevision,
                canonicalClaimJson = canonical.canonicalClaimJson,
                contentHash = canonical.contentHash.value,
                sourceManifestHash = canonical.sourceManifestHash.value,
                reasonCode = nextVersion.reason.name,
                createdByRunId = mutationId,
                createdAtMs = nowMs,
            ),
        )
        if (pairScope) {
            synthesisDao.insertClaimExperienceSources(
                nextVersion.sources.map { source ->
                    val experience = experienceDao.getExperience(
                        source.authority.memoryId,
                        target.fence.scopeId.value,
                    ) ?: abortMutation(DreamReviewStoreMutationResult.Corrupt)
                    if (experience.experienceEpoch != source.authority.expectedRevision) {
                        abortMutation(DreamReviewStoreMutationResult.Conflict(DreamReviewConflict.MEMORY_EPOCH))
                    }
                    DreamClaimExperienceSourceEntity(
                        claimId = nextVersion.claimId,
                        claimRevision = nextVersion.nextRevision,
                        experienceId = experience.experienceId,
                        experienceEpoch = experience.experienceEpoch,
                        contentDigest = experience.contentDigest,
                        sourceManifestHash = source.authority.expectedSourceManifestHash.value,
                        supportType = source.supportType.name,
                        createdAtMs = nowMs,
                    )
                },
            )
        } else {
            synthesisDao.insertClaimVersionSources(sourceRows)
        }
        val heads = loadAllHeadsForCompile(target.fence.scopeId)
            ?: abortMutation(DreamReviewStoreMutationResult.Corrupt)
        val previous = target.fence.expectedActiveSnapshotId?.let { snapshotId ->
            synthesisDao.getSnapshot(snapshotId, target.fence.scopeId.value)
                ?: abortMutation(DreamReviewStoreMutationResult.Corrupt)
        }
        if (previous != null && !previous.matchesProjectionState(stateBefore, currentMemoryEpoch)) {
            abortMutation(DreamReviewStoreMutationResult.Corrupt)
        }
        val compiled = try {
            DreamSnapshotCompiler.compile(
                DreamSnapshotCompileRequest(
                    scopeId = target.fence.scopeId,
                    compilerRevision = previous?.compilerRevision ?: DEFAULT_REVIEW_COMPILER_REVISION,
                    claims = heads,
                ),
            )
        } catch (_: Exception) {
            abortMutation(DreamReviewStoreMutationResult.Corrupt)
        }
        val nextDreamRevision = target.fence.expectedDreamRevision + 1L
        if (nextDreamRevision <= 0L) abortMutation(DreamReviewStoreMutationResult.Corrupt)
        synthesisDao.insertSnapshot(
            DreamSnapshotEntity(
                snapshotId = mutationId,
                scopeId = target.fence.scopeId.value,
                snapshotRevision = nextDreamRevision,
                sourceMemoryEpoch = currentMemoryEpoch,
                committedDreamRevision = nextDreamRevision,
                status = SNAPSHOT_ACTIVE,
                canonicalPayloadJson = compiled.payloadJson,
                payloadSha256 = compiled.payloadHash.value,
                compilerRevision = compiled.compilerRevision,
                estimatedTokens = compiled.estimatedTokens,
                claimCount = compiled.claimCount,
                createdByRunId = mutationId,
                createdAtMs = nowMs,
                supersedesSnapshotId = previous?.snapshotId,
                reasonCode = reason,
            ),
        )
        previous?.let { old ->
            if (synthesisDao.supersedeActiveSnapshot(old.snapshotId, target.fence.scopeId.value) != 1) {
                abortMutation(DreamReviewStoreMutationResult.Conflict(DreamReviewConflict.ACTIVE_SNAPSHOT))
            }
        }
        val snapshotAdvanced = if (pairScope) {
            experienceDao.advancePairUserReviewSnapshotCas(
                pairScopeId = target.fence.scopeId.value,
                currentExperienceEpoch = currentMemoryEpoch,
                expectedAppliedExperienceEpoch = target.fence.expectedLastAppliedMemoryEpoch,
                expectedProfileRevision = target.fence.expectedDreamRevision,
                expectedActiveSnapshotId = target.fence.expectedActiveSnapshotId,
                newSnapshotId = mutationId,
                mutationId = mutationId,
                reasonCode = reason,
                nowMs = nowMs,
            )
        } else {
            synthesisDao.advanceUserReviewSnapshotCas(
                scopeId = target.fence.scopeId.value,
                currentMemoryEpoch = currentMemoryEpoch,
                expectedLastAppliedMemoryEpoch = target.fence.expectedLastAppliedMemoryEpoch,
                expectedDreamRevision = target.fence.expectedDreamRevision,
                expectedActiveSnapshotId = target.fence.expectedActiveSnapshotId,
                newSnapshotId = mutationId,
                mutationId = mutationId,
                reasonCode = reason,
                nowMs = nowMs,
            )
        }
        if (snapshotAdvanced != 1) {
            abortMutation(classifyTargetMutation(target, currentMemoryEpoch))
        }
        return DreamReviewStoreMutationResult.Applied(
            DreamReviewFence(
                scopeId = target.fence.scopeId,
                expectedMemoryEpoch = currentMemoryEpoch,
                expectedLastAppliedMemoryEpoch = target.fence.expectedLastAppliedMemoryEpoch,
                expectedDreamRevision = nextDreamRevision,
                expectedActiveSnapshotId = mutationId,
            ),
        )
    }

    private suspend fun loadCurrentClaim(
        claim: DreamClaimEntity,
        scopeId: DreamScopeId,
    ): LoadedClaim? {
        if (DreamPairScopeId.parseOrNull(scopeId.value) != null) {
            return LoadedClaim(
                head = claim.toPairHead(scopeId) ?: return null,
                sourceRows = emptyList(),
            )
        }
        val version = synthesisDao.getClaimVersion(claim.claimId, claim.claimRevision) ?: return null
        val rows = synthesisDao.listReviewSourceRows(
            scopeId = scopeId.value,
            claimId = claim.claimId,
            headOnly = true,
            limit = DREAM_REVIEW_MAX_SOURCES_PER_VERSION + 1,
        )
        if (rows.size > DREAM_REVIEW_MAX_SOURCES_PER_VERSION) return null
        val parsed = parseVersion(version, rows, scopeId) ?: return null
        val head = parsed.toHead(scopeId)
        if (!claim.matches(head)) return null
        return LoadedClaim(head, rows)
    }

    private suspend fun loadAllHeadsForCompile(scopeId: DreamScopeId): List<DreamClaimHead>? {
        val claims = synthesisDao.listClaims(scopeId.value, DREAM_REVIEW_MAX_CLAIMS + 1)
        if (claims.size > DREAM_REVIEW_MAX_CLAIMS) return null
        if (DreamPairScopeId.parseOrNull(scopeId.value) != null) {
            return claims.mapNotNull { claim ->
                if (claim.state == DreamClaimState.TOMBSTONED.name) return@mapNotNull null
                claim.toPairHead(scopeId) ?: return null
            }
        }
        val versions = synthesisDao.listReviewHeadVersions(scopeId.value, claims.size + 1)
        if (versions.size != claims.size) return null
        val rows = synthesisDao.listReviewSourceRows(
            scopeId = scopeId.value,
            claimId = null,
            headOnly = true,
            limit = MAX_REVIEW_PROJECTION_SOURCES + 1,
        )
        if (rows.size > MAX_REVIEW_PROJECTION_SOURCES) return null
        val versionsByClaim = versions.associateBy(DreamClaimVersionEntity::claimId)
        val rowsByClaim = rows.groupBy(DreamReviewSourceRow::claimId)
        return claims.mapNotNull { claim ->
            if (claim.state == DreamClaimState.TOMBSTONED.name) return@mapNotNull null
            val claimRows = rowsByClaim[claim.claimId].orEmpty()
            if (claimRows.size > DREAM_REVIEW_MAX_SOURCES_PER_VERSION) return null
            val parsed = parseVersion(versionsByClaim[claim.claimId] ?: return null, claimRows, scopeId)
                ?: return null
            parsed.toHead(scopeId).takeIf(claim::matches) ?: return null
        }
    }

    private suspend fun DreamClaimEntity.toPairHead(scopeId: DreamScopeId): DreamClaimHead? {
        val parsedState = runCatching { enumValueOf<DreamClaimState>(state) }.getOrNull() ?: return null
        val sources = if (parsedState == DreamClaimState.TOMBSTONED) {
            emptyList()
        } else {
            synthesisDao.listClaimExperienceSources(claimId, claimRevision).map { source ->
                val experience = experienceDao.getExperience(source.experienceId, scopeId.value)
                    ?: return null
                if (experience.experienceEpoch != source.experienceEpoch ||
                    experience.contentDigest != source.contentDigest
                ) return null
                val candidate = experience.toDreamInputCandidate(
                    scopeId = scopeId,
                    origin = DreamInputCandidateOrigin.AUTHORITY_CHANGE,
                    json = json,
                ) ?: return null
                if (candidate.pin.expectedSourceManifestHash.value != source.sourceManifestHash) return null
                DreamClaimSourcePin(
                    authority = candidate.pin,
                    supportType = runCatching { enumValueOf<DreamSupportType>(source.supportType) }.getOrNull()
                        ?: return null,
                    directAuthority = true,
                )
            }
        }
        val confidencePermille = (confidence * 1_000.0).roundToInt()
        if (confidencePermille !in 0..1_000) return null
        return try {
            DreamClaimHead(
                claimId = claimId,
                scopeId = scopeId,
                revision = claimRevision,
                claimKey = claimKey,
                storageClass = enumValueOf(storageClass),
                epistemicType = enumValueOf(epistemicType),
                state = parsedState,
                title = title,
                statement = statement,
                confidencePermille = confidencePermille,
                temporalState = enumValueOf(temporalState),
                validFromEpochMs = validFromMs,
                validToEpochMs = validToMs,
                versionHash = DreamSha256(claimHash),
                sources = sources,
                subjectKind = enumValueOf(subjectKind),
                profileSection = profileSection,
                epistemicOrigin = enumValueOf(epistemicOrigin),
                contentType = enumValueOf(contentType),
            )
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun exactCorrectionPin(
        command: DreamMarkCorrectedCommand,
        scopeId: DreamScopeId,
    ): DreamAuthorityPin? {
        val memory = memoryDao.getMemoryById(command.authorityMemoryId, scopeId.value) ?: return null
        if (memory.revision != command.authorityMemoryRevision ||
            memory.lifecycleStatus != MemoryLifecycleStatus.ACTIVE.name ||
            memory.truthStatus != MemoryTruthStatus.CONFIRMED.name ||
            memory.approvalSource != MemoryApprovalSource.USER_REVIEWED.name ||
            memory.sourceType != DREAM_CORRECTION_SOURCE_TYPE || memory.confidence != 1f ||
            memory.originAssistantId != command.validatedTarget.capturedOriginAssistantId ||
            (memory.expiresAtMs != null && memory.expiresAtMs <= command.nowEpochMs)
        ) return null
        if (memoryV2Dao.findRevision(memory.id, memory.revision, scopeId.value) == null) return null
        val authority = memory.toAuthority(scopeId) ?: return null
        return DreamAuthorityPin(
            scopeId = scopeId,
            memoryId = memory.id.toString(),
            expectedRevision = memory.revision.toLong(),
            expectedAuthorityFingerprint = DreamAuthorityFingerprintV1.compute(authority),
            expectedSourceManifestHash = DreamAuthorityFingerprintV1.sourceManifestHash(authority.sources),
        )
    }

    private suspend fun readPairEvidence(
        reference: DreamEvidenceReference,
        maxChars: Int,
    ): DreamEvidenceRevealResult {
        val source = synthesisDao.listClaimExperienceSources(reference.claimId, reference.claimRevision)
            .firstOrNull { row ->
                row.experienceId == reference.memoryId &&
                    row.experienceEpoch == reference.memoryRevision &&
                    row.supportType == reference.supportType.name
            } ?: return DreamEvidenceRevealResult.NotFound
        val experience = experienceDao.getExperience(source.experienceId, reference.scopeId.value)
            ?: return DreamEvidenceRevealResult.NotFound
        val candidate = experience.toDreamInputCandidate(
            scopeId = reference.scopeId,
            origin = DreamInputCandidateOrigin.AUTHORITY_CHANGE,
            json = json,
        ) ?: return DreamEvidenceRevealResult.Invalid(DreamEvidenceValidity.CORRUPT)
        val validity = when {
            experience.status == "DISCARDED" -> DreamEvidenceValidity.TOMBSTONED
            experience.experienceEpoch != reference.memoryRevision -> DreamEvidenceValidity.REVISION_CHANGED
            experience.contentDigest != source.contentDigest -> DreamEvidenceValidity.SEMANTIC_HASH_MISMATCH
            candidate.pin.expectedAuthorityFingerprint != reference.expectedSemanticHash ->
                DreamEvidenceValidity.SEMANTIC_HASH_MISMATCH
            candidate.pin.expectedSourceManifestHash != reference.expectedSourceManifestHash ||
                candidate.pin.expectedSourceManifestHash.value != source.sourceManifestHash ->
                DreamEvidenceValidity.SOURCE_MANIFEST_MISMATCH
            else -> DreamEvidenceValidity.VALID
        }
        if (validity != DreamEvidenceValidity.VALID) return DreamEvidenceRevealResult.Invalid(validity)
        val text = experience.summary
        return DreamEvidenceRevealResult.Revealed(
            DreamEvidenceExcerpt(
                reference = reference,
                text = text.take(maxChars),
                truncated = text.length > maxChars,
            ),
        )
    }

    private suspend fun checkExactEvidence(
        reference: DreamEvidenceReference,
        source: DreamClaimVersionSourceEntity,
        nowMs: Long,
    ): EvidenceCheck {
        val memoryId = source.memoryId
        val actualScope = synthesisDao.findMemoryScopeIdForReview(memoryId)
            ?: return EvidenceCheck(DreamEvidenceValidity.MISSING)
        if (actualScope != reference.scopeId.value) return EvidenceCheck(DreamEvidenceValidity.SCOPE_MISMATCH)
        val memory = memoryDao.getMemoryById(memoryId, reference.scopeId.value)
            ?: return EvidenceCheck(DreamEvidenceValidity.MISSING)
        if (memory.revision.toLong() != reference.memoryRevision) {
            return EvidenceCheck(DreamEvidenceValidity.REVISION_CHANGED)
        }
        val revision = memoryV2Dao.findRevision(memory.id, memory.revision, reference.scopeId.value)
            ?: return EvidenceCheck(DreamEvidenceValidity.REVISION_CHANGED)
        val revisionSources = revision.toAuthoritySources()
            ?: return EvidenceCheck(DreamEvidenceValidity.CORRUPT)
        if (DreamAuthorityFingerprintV1.sourceManifestHash(revisionSources) !=
            reference.expectedSourceManifestHash
        ) return EvidenceCheck(DreamEvidenceValidity.SOURCE_MANIFEST_MISMATCH)
        val authority = memory.toAuthority(reference.scopeId)
            ?: return EvidenceCheck(DreamEvidenceValidity.CORRUPT)
        if (DreamAuthorityFingerprintV1.compute(authority) != reference.expectedSemanticHash) {
            return EvidenceCheck(DreamEvidenceValidity.SEMANTIC_HASH_MISMATCH)
        }
        memory.invalidityAt(nowMs)?.let { return EvidenceCheck(it) }
        val evidence = source.memoryEvidenceId?.let { evidenceId ->
            synthesisDao.getScopedEvidenceForReview(reference.scopeId.value, evidenceId, memory.id)
                ?: return EvidenceCheck(DreamEvidenceValidity.EVIDENCE_MISSING)
        }
        if (evidence?.quality == SOURCE_DELETED_QUALITY) {
            return EvidenceCheck(DreamEvidenceValidity.TOMBSTONED)
        }
        if (evidence != null && revisionSources.none { source ->
                evidence.matchesAuthoritySource(source)
            }
        ) {
            return EvidenceCheck(DreamEvidenceValidity.CORRUPT)
        }
        val excerpt = if (evidence != null) {
            evidence.excerpt
        } else {
            if (memory.approvalSource !in DIRECT_REVEAL_APPROVALS) {
                return EvidenceCheck(DreamEvidenceValidity.EVIDENCE_MISSING)
            }
            memory.content
        }
        return EvidenceCheck(
            validity = DreamEvidenceValidity.VALID,
            excerpt = excerpt,
            sourceKind = evidence?.sourceKind ?: revisionSources
                .map { source -> source.sourceKind.name }
                .distinct()
                .singleOrNull(),
            qualityCode = evidence?.quality,
        )
    }

    private fun DreamReviewSourceRow.evidenceCheck(scopeId: DreamScopeId, nowMs: Long): EvidenceCheck {
        if (currentScopeId == null) return EvidenceCheck(DreamEvidenceValidity.MISSING)
        if (currentScopeId != scopeId.value) return EvidenceCheck(DreamEvidenceValidity.SCOPE_MISMATCH)
        if (revisionRowId == null) return EvidenceCheck(DreamEvidenceValidity.REVISION_CHANGED)
        if (currentMemoryRevision != memoryRevision) return EvidenceCheck(DreamEvidenceValidity.REVISION_CHANGED)
        val reference = toReference(scopeId) ?: return EvidenceCheck(DreamEvidenceValidity.CORRUPT)
        val revisionSources = revisionSourceIdentitiesJson?.decodeAuthoritySourcesOrNull()
            ?: return EvidenceCheck(DreamEvidenceValidity.CORRUPT)
        val authority = toCurrentAuthority(scopeId) ?: return EvidenceCheck(DreamEvidenceValidity.CORRUPT)
        if (DreamAuthorityFingerprintV1.compute(authority) != reference.expectedSemanticHash) {
            return EvidenceCheck(DreamEvidenceValidity.SEMANTIC_HASH_MISMATCH)
        }
        if (DreamAuthorityFingerprintV1.sourceManifestHash(authority.sources) !=
            reference.expectedSourceManifestHash
        ) return EvidenceCheck(DreamEvidenceValidity.SOURCE_MANIFEST_MISMATCH)
        when {
            currentLifecycleStatus != MemoryLifecycleStatus.ACTIVE.name ->
                return EvidenceCheck(DreamEvidenceValidity.LIFECYCLE_INVALID)
            currentTruthStatus != MemoryTruthStatus.CONFIRMED.name ->
                return EvidenceCheck(DreamEvidenceValidity.TRUTH_INVALID)
            currentExpiresAtMs != null && currentExpiresAtMs <= nowMs ->
                return EvidenceCheck(DreamEvidenceValidity.EXPIRED)
        }
        if (memoryEvidenceId != null && (evidenceRowId == null || evidenceMemoryId != memoryId)) {
            return EvidenceCheck(DreamEvidenceValidity.EVIDENCE_MISSING)
        }
        if (evidenceQuality == SOURCE_DELETED_QUALITY) return EvidenceCheck(DreamEvidenceValidity.TOMBSTONED)
        if (memoryEvidenceId != null && revisionSources.none { source ->
                matchesEvidenceProjection(source)
            }
        ) {
            return EvidenceCheck(DreamEvidenceValidity.CORRUPT)
        }
        if (memoryEvidenceId == null && currentApprovalSource !in DIRECT_REVEAL_APPROVALS) {
            return EvidenceCheck(DreamEvidenceValidity.EVIDENCE_MISSING)
        }
        val excerpt = if (memoryEvidenceId != null) {
            evidenceExcerpt
        } else if (currentApprovalSource in DIRECT_REVEAL_APPROVALS) {
            currentContent
        } else {
            null
        }
        return EvidenceCheck(
            validity = DreamEvidenceValidity.VALID,
            excerpt = excerpt,
            sourceKind = evidenceSourceKind ?: revisionSources
                .map { source -> source.sourceKind.name }
                .distinct()
                .singleOrNull(),
            qualityCode = evidenceQuality,
        )
    }

    private suspend fun lookupTarget(
        target: DreamClaimMutationTarget,
        currentMemoryEpoch: Long,
    ): TargetLookup {
        val state = reviewScopeState(target.fence.scopeId)
            ?: return TargetLookup.Rejected(DreamReviewStoreMutationResult.NotFound)
        compareFence(state, target.fence, currentMemoryEpoch)?.let { conflict ->
            return TargetLookup.Rejected(DreamReviewStoreMutationResult.Conflict(conflict))
        }
        val claim = synthesisDao.getClaim(target.claimId, target.fence.scopeId.value)
            ?: return when (synthesisDao.findClaimScopeId(target.claimId)) {
                null -> TargetLookup.Rejected(DreamReviewStoreMutationResult.NotFound)
                else -> TargetLookup.Rejected(
                    DreamReviewStoreMutationResult.Conflict(DreamReviewConflict.SCOPE),
                )
            }
        if (claim.claimRevision != target.expectedClaimRevision) {
            return TargetLookup.Rejected(
                DreamReviewStoreMutationResult.Conflict(DreamReviewConflict.CLAIM_REVISION),
            )
        }
        return TargetLookup.Ready(state, claim)
    }

    private suspend fun classifyTargetMutation(
        target: DreamClaimMutationTarget,
        currentMemoryEpoch: Long,
    ): DreamReviewStoreMutationResult = when (val lookup = lookupTarget(target, currentMemoryEpoch)) {
        is TargetLookup.Ready -> DreamReviewStoreMutationResult.Corrupt
        is TargetLookup.Rejected -> lookup.result
    }

    private suspend fun classifyFenceMutation(
        fence: DreamReviewFence,
    ): DreamReviewStoreMutationResult {
        val state = reviewScopeState(fence.scopeId)
            ?: return DreamReviewStoreMutationResult.NotFound
        val conflict = compareFence(state, fence)
        return if (conflict == null) DreamReviewStoreMutationResult.Corrupt
        else DreamReviewStoreMutationResult.Conflict(conflict)
    }

    private suspend fun reviewScopeState(scopeId: DreamScopeId): MemoryScopeStateEntity? =
        if (DreamPairScopeId.parseOrNull(scopeId.value) != null) {
            experienceDao.getState(scopeId.value)?.toReviewState()
        } else {
            dreamDao.getScopeState(scopeId.value)
        }

    private suspend fun mutateOrReject(
        block: suspend () -> DreamReviewStoreMutationResult,
    ): DreamReviewStoreMutationResult = try {
        database.withTransaction { block() }
    } catch (abort: ReviewMutationAbort) {
        abort.result
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (_: Exception) {
        DreamReviewStoreMutationResult.Corrupt
    }

    private suspend fun readUsageMode(scopeId: DreamScopeId): DreamUsageMode = try {
        val flags = featureFlags.flagsFor(scopeId)
        when {
            !flags.schemaReady || (!flags.generate && !flags.use) -> DreamUsageMode.OFF
            flags.shadow -> DreamUsageMode.SHADOW
            flags.use -> DreamUsageMode.ACTIVE
            else -> DreamUsageMode.GENERATED_ONLY
        }
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (_: Exception) {
        DreamUsageMode.OFF
    }
}

private data class LoadedClaim(
    val head: DreamClaimHead,
    val sourceRows: List<DreamReviewSourceRow>,
)

private data class ParsedVersion(
    val version: DreamValidatedClaimVersion,
    val canonicalHash: DreamSha256,
) {
    fun toHead(scopeId: DreamScopeId) = DreamClaimHead(
        claimId = version.claimId,
        scopeId = scopeId,
        revision = version.nextRevision,
        claimKey = version.claimKey,
        storageClass = version.storageClass,
        epistemicType = version.epistemicType,
        state = version.nextState,
        title = version.title,
        statement = version.statement,
        confidencePermille = version.confidencePermille,
        temporalState = version.temporalState,
        validFromEpochMs = version.validFromEpochMs,
        validToEpochMs = version.validToEpochMs,
        versionHash = canonicalHash,
        sources = version.sources,
    )
}

private data class EvidenceCheck(
    val validity: DreamEvidenceValidity,
    val excerpt: String? = null,
    val sourceKind: String? = null,
    val qualityCode: String? = null,
)

private sealed interface TargetLookup {
    data class Ready(
        val state: MemoryScopeStateEntity,
        val claim: DreamClaimEntity,
    ) : TargetLookup

    data class Rejected(val result: DreamReviewStoreMutationResult) : TargetLookup {
        fun toMutationResult(): DreamReviewStoreMutationResult = result

        fun <T> toReadResult(): DreamReviewReadResult<T> = when (result) {
            is DreamReviewStoreMutationResult.Conflict -> DreamReviewReadResult.Conflict(result.conflict)
            DreamReviewStoreMutationResult.NotFound -> DreamReviewReadResult.NotFound
            DreamReviewStoreMutationResult.InvalidState -> DreamReviewReadResult.InvalidState
            else -> DreamReviewReadResult.Corrupt
        }
    }
}

private class ReviewMutationAbort(
    val result: DreamReviewStoreMutationResult,
) : RuntimeException()

private fun abortMutation(result: DreamReviewStoreMutationResult): Nothing =
    throw ReviewMutationAbort(result)

private fun compareFence(
    state: MemoryScopeStateEntity,
    fence: DreamReviewFence,
    currentMemoryEpoch: Long = fence.expectedMemoryEpoch,
): DreamReviewConflict? = when {
    state.scopeId != fence.scopeId.value -> DreamReviewConflict.SCOPE
    state.memoryEpoch != currentMemoryEpoch -> DreamReviewConflict.MEMORY_EPOCH
    state.lastAppliedMemoryEpoch != fence.expectedLastAppliedMemoryEpoch ->
        DreamReviewConflict.LAST_APPLIED_MEMORY_EPOCH
    state.dreamStateRevision != fence.expectedDreamRevision -> DreamReviewConflict.DREAM_REVISION
    state.activeSnapshotId != fence.expectedActiveSnapshotId -> DreamReviewConflict.ACTIVE_SNAPSHOT
    else -> null
}

private fun MemoryScopeStateEntity.toReviewFence(scopeId: DreamScopeId): DreamReviewFence? = try {
    DreamReviewFence(
        scopeId = scopeId,
        expectedMemoryEpoch = memoryEpoch,
        expectedLastAppliedMemoryEpoch = lastAppliedMemoryEpoch,
        expectedDreamRevision = dreamStateRevision,
        expectedActiveSnapshotId = activeSnapshotId,
    )
} catch (_: Exception) {
    null
}

/** Reuses the mature review projection/fence machinery with Pair-Dream's independent clock. */
private fun DreamExperienceStateEntity.toReviewState() = MemoryScopeStateEntity(
    scopeId = pairScopeId,
    memoryEpoch = experienceEpoch,
    observerCheckpointEpoch = observerCheckpointEpoch,
    activeRunId = activeRunId,
    activeRunLeaseUntilMs = activeRunLeaseUntilMs,
    updatedAtMs = updatedAtMs,
    lastReasonCode = lastReasonCode,
    dreamStateRevision = profileRevision,
    lastAppliedMemoryEpoch = appliedExperienceEpoch,
    activeSnapshotId = activeSnapshotId,
    lastFullRebuildAtMs = null,
)

private fun emptyProjection(scopeId: DreamScopeId, usageMode: DreamUsageMode) = DreamReviewProjection(
    fence = DreamReviewFence(scopeId, 0L, 0L, 0L, null),
    derivedStatus = DreamDerivedStatus.EMPTY,
    usageMode = usageMode,
    claims = emptyList(),
    activeSnapshot = null,
    supersededSnapshot = null,
    snapshotDiff = DreamSnapshotDiffResult.Available(emptyList()),
    recentRuns = emptyList(),
)

private fun invalidProjection(
    scopeId: DreamScopeId,
    state: MemoryScopeStateEntity?,
    usageMode: DreamUsageMode,
    diff: DreamSnapshotDiffResult = DreamSnapshotDiffResult.Unavailable(
        DreamSnapshotDiffFailure.MANIFEST_INVALID,
    ),
): DreamReviewProjection {
    val memoryEpoch = state?.memoryEpoch?.coerceAtLeast(0L) ?: 0L
    val lastApplied = state?.lastAppliedMemoryEpoch?.coerceIn(0L, memoryEpoch) ?: 0L
    val dreamRevision = state?.dreamStateRevision?.coerceAtLeast(0L) ?: 0L
    val snapshotId = state?.activeSnapshotId?.takeIf { value ->
        try {
            me.rerere.rikkahub.memory.dreaming.model.requireDreamStableId(value)
            true
        } catch (_: Exception) {
            false
        }
    }
    return DreamReviewProjection(
        fence = DreamReviewFence(scopeId, memoryEpoch, lastApplied, dreamRevision, snapshotId),
        derivedStatus = DreamDerivedStatus.INVALID,
        usageMode = usageMode,
        claims = emptyList(),
        activeSnapshot = null,
        supersededSnapshot = null,
        snapshotDiff = diff,
        recentRuns = emptyList(),
    )
}

private fun DreamClaimEntity.isUserReviewable(): Boolean = state in USER_REVIEWABLE_STATES

private fun DreamClaimEntity.matches(head: DreamClaimHead): Boolean =
    claimId == head.claimId && claimRevision == head.revision && claimKey == head.claimKey &&
        storageClass == head.storageClass.name && epistemicType == head.epistemicType.name &&
        state == head.state.name && title == head.title && statement == head.statement &&
        (confidence * 1_000.0).roundToInt() == head.confidencePermille &&
        temporalState == head.temporalState.name && validFromMs == head.validFromEpochMs &&
        validToMs == head.validToEpochMs && claimHash == head.versionHash.value

private fun DreamClaimHead.toNextVersion(
    state: DreamClaimState,
    reason: DreamClaimMutationReason,
    sources: List<DreamClaimSourcePin>,
) = DreamValidatedClaimVersion(
    claimId = claimId,
    expectedPreviousRevision = revision,
    nextRevision = revision + 1L,
    claimKey = claimKey,
    storageClass = storageClass,
    epistemicType = epistemicType,
    nextState = state,
    title = title,
    statement = statement,
    confidencePermille = confidencePermille,
    temporalState = temporalState,
    validFromEpochMs = validFromEpochMs,
    validToEpochMs = validToEpochMs,
    sources = sources,
    reason = reason,
)

private fun DreamClaimHead.toSummary(
    evidenceCount: Int,
    originAssistantId: String?,
) = DreamClaimSummary(
    claimId = claimId,
    revision = revision,
    section = reviewSection(),
    state = state,
    title = title,
    statement = statement,
    confidencePermille = confidencePermille,
    temporalState = temporalState,
    validFromEpochMs = validFromEpochMs,
    validToEpochMs = validToEpochMs,
    evidenceCount = evidenceCount,
    originAssistantId = originAssistantId,
)

private fun DreamClaimHead.reviewSection(): DreamSnapshotSection = when {
    DreamPairScopeId.parseOrNull(scopeId.value) != null -> when (subjectKind) {
        DreamSubjectKind.USER -> DreamSnapshotSection.ABOUT_USER
        DreamSubjectKind.ASSISTANT -> DreamSnapshotSection.ABOUT_ASSISTANT
        DreamSubjectKind.RELATIONSHIP -> DreamSnapshotSection.ABOUT_RELATIONSHIP
    }
    epistemicType == DreamEpistemicType.PROJECT_STATE -> DreamSnapshotSection.CURRENT_PROJECTS
    epistemicType == DreamEpistemicType.PLAN -> DreamSnapshotSection.ACTIVE_PLANS
    epistemicType == DreamEpistemicType.CONSTRAINT -> DreamSnapshotSection.ACTIVE_CONSTRAINTS
    storageClass == DreamStorageClass.PROFILE -> DreamSnapshotSection.PROFILE
    else -> DreamSnapshotSection.OTHER_CONTEXT
}

private fun parseVersion(
    entity: DreamClaimVersionEntity,
    rows: List<DreamReviewSourceRow>,
    scopeId: DreamScopeId,
): ParsedVersion? {
    val root = try {
        REVIEW_JSON.parseToJsonElement(entity.canonicalClaimJson) as? JsonObject
    } catch (_: Exception) {
        null
    } ?: return null
    if (root.keys != CLAIM_VERSION_KEYS || DreamCanonicalJson.encode(root) != entity.canonicalClaimJson) {
        return null
    }
    val revision = root.long("revision") ?: return null
    if (revision != entity.claimRevision) return null
    val sources = rows.map { it.toSourcePin(scopeId) ?: return null }
    val validFrom = root.nullableLongValue("valid_from_epoch_ms") ?: return null
    val validTo = root.nullableLongValue("valid_to_epoch_ms") ?: return null
    val version = try {
        DreamValidatedClaimVersion(
            claimId = root.string("claim_id") ?: return null,
            expectedPreviousRevision = if (revision == 1L) null else revision - 1L,
            nextRevision = revision,
            claimKey = root.string("claim_key") ?: return null,
            storageClass = enumValueOf(root.string("storage_class") ?: return null),
            epistemicType = enumValueOf(root.string("epistemic_type") ?: return null),
            nextState = enumValueOf(root.string("state") ?: return null),
            title = root.string("title") ?: return null,
            statement = root.string("statement") ?: return null,
            confidencePermille = root.int("confidence_permille") ?: return null,
            temporalState = enumValueOf(root.string("temporal_state") ?: return null),
            validFromEpochMs = validFrom.value,
            validToEpochMs = validTo.value,
            sources = sources,
            reason = enumValueOf(root.string("reason") ?: return null),
        )
    } catch (_: Exception) {
        return null
    }
    val canonical = DreamClaimVersionCanonicalV1.encode(version)
    if (canonical.canonicalClaimJson != entity.canonicalClaimJson ||
        canonical.contentHash.value != entity.contentHash ||
        canonical.sourceManifestHash.value != entity.sourceManifestHash ||
        version.reason.name != entity.reasonCode ||
        root.string("source_manifest_hash") != canonical.sourceManifestHash.value
    ) return null
    return ParsedVersion(version, canonical.contentHash)
}

private fun DreamReviewSourceRow.toSourcePin(scopeId: DreamScopeId): DreamClaimSourcePin? {
    val manifest = revisionSourceIdentitiesJson?.decodeAuthoritySourcesOrNull() ?: return null
    return try {
        DreamClaimSourcePin(
            authority = DreamAuthorityPin(
                scopeId = scopeId,
                memoryId = memoryId.toString(),
                expectedRevision = memoryRevision.toLong(),
                expectedAuthorityFingerprint = DreamSha256(memorySemanticHash),
                expectedSourceManifestHash = DreamAuthorityFingerprintV1.sourceManifestHash(manifest),
            ),
            supportType = enumValueOf(supportType),
            directAuthority = true,
        )
    } catch (_: Exception) {
        null
    }
}

private fun DreamReviewSourceRow.toReference(scopeId: DreamScopeId): DreamEvidenceReference? {
    val source = toSourcePin(scopeId) ?: return null
    return DreamEvidenceReference(
        scopeId = scopeId,
        claimId = claimId,
        claimRevision = claimRevision,
        memoryId = memoryId.toString(),
        memoryRevision = memoryRevision.toLong(),
        expectedSemanticHash = source.authority.expectedAuthorityFingerprint,
        expectedSourceManifestHash = source.authority.expectedSourceManifestHash,
        supportType = source.supportType,
    )
}

private fun DreamReviewSourceRow.toCurrentAuthority(scopeId: DreamScopeId): DreamAuthorityMemory? = try {
    DreamAuthorityMemory(
        scopeId = scopeId,
        memoryId = memoryId.toString(),
        revision = currentMemoryRevision?.toLong() ?: return null,
        title = currentTitle,
        content = currentContent ?: return null,
        kind = enumValueOf(currentMemoryKind ?: return null),
        attribution = enumValueOf(currentAttribution ?: return null),
        truthStatus = enumValueOf(currentTruthStatus ?: return null),
        lifecycleStatus = enumValueOf(currentLifecycleStatus ?: return null),
        approvalSource = enumValueOf(currentApprovalSource ?: return null),
        tags = currentTagsJson?.decodeStringsOrNull() ?: return null,
        createdAtEpochMs = currentCreatedAtMs ?: return null,
        updatedAtEpochMs = currentUpdatedAtMs ?: return null,
        occurredAtEpochMs = currentOccurredAtMs,
        expiresAtEpochMs = currentExpiresAtMs,
        originAssistantId = currentOriginAssistantId,
        participants = currentParticipantsJson?.decodeStringsOrNull() ?: return null,
        outcome = currentOutcome,
        sources = currentSourceIdentitiesJson?.decodeAuthoritySourcesOrNull() ?: return null,
        tombstoned = false,
    )
} catch (_: Exception) {
    null
}

private fun MemoryEntity.toAuthority(scopeId: DreamScopeId): DreamAuthorityMemory? = try {
    DreamAuthorityMemory(
        scopeId = scopeId,
        memoryId = id.toString(),
        revision = revision.toLong(),
        title = title,
        content = content,
        kind = enumValueOf(memoryKind),
        attribution = enumValueOf(attribution),
        truthStatus = enumValueOf(truthStatus),
        lifecycleStatus = enumValueOf(lifecycleStatus),
        approvalSource = enumValueOf(approvalSource),
        tags = tagsJson.decodeStringsOrNull() ?: return null,
        createdAtEpochMs = createdAtMs,
        updatedAtEpochMs = updatedAtMs,
        occurredAtEpochMs = occurredAtMs,
        expiresAtEpochMs = expiresAtMs,
        originAssistantId = originAssistantId,
        participants = participantsJson.decodeStringsOrNull() ?: return null,
        outcome = outcome,
        sources = sourceIdentitiesJson.decodeAuthoritySourcesOrNull() ?: return null,
        tombstoned = false,
    )
} catch (_: Exception) {
    null
}

private fun MemoryRevisionEntity.toAuthoritySources(): List<DreamAuthoritySource>? =
    sourceIdentitiesJson.decodeAuthoritySourcesOrNull()

private fun MemoryEvidenceEntity.matchesAuthoritySource(source: DreamAuthoritySource): Boolean =
    conversationId == source.conversationId && messageId == source.messageId &&
        role == source.role.name && sourceKind == source.sourceKind.name &&
        sourceDigest == source.consumedTextDigest.value && evidenceGroupId == source.evidenceGroupId

private fun DreamReviewSourceRow.matchesEvidenceProjection(source: DreamAuthoritySource): Boolean =
    evidenceConversationId == source.conversationId && evidenceMessageId == source.messageId &&
        evidenceRole == source.role.name && evidenceSourceKind == source.sourceKind.name &&
        evidenceSourceDigest == source.consumedTextDigest.value &&
        evidenceGroupId == source.evidenceGroupId

private fun String.decodeStringsOrNull(): List<String>? = try {
    REVIEW_JSON.decodeFromString<List<String>>(this)
} catch (_: Exception) {
    null
}

private fun String.decodeAuthoritySourcesOrNull(): List<DreamAuthoritySource>? = try {
    REVIEW_JSON.decodeFromString<List<MemorySourceIdentity>>(this).map { source ->
        DreamAuthoritySource(
            conversationId = source.conversationId,
            messageId = source.messageId,
            role = source.role,
            sourceKind = source.sourceKind,
            consumedTextDigest = DreamSha256(source.consumedTextDigest),
            evidenceGroupId = source.evidenceGroupId,
        )
    }
} catch (_: Exception) {
    null
}

private fun MemoryEntity.invalidityAt(nowMs: Long): DreamEvidenceValidity? = when {
    lifecycleStatus != MemoryLifecycleStatus.ACTIVE.name -> DreamEvidenceValidity.LIFECYCLE_INVALID
    truthStatus != MemoryTruthStatus.CONFIRMED.name -> DreamEvidenceValidity.TRUTH_INVALID
    expiresAtMs != null && expiresAtMs <= nowMs -> DreamEvidenceValidity.EXPIRED
    else -> null
}

private fun DreamReviewSourceRow.copyForRevision(nextRevision: Long, nowMs: Long) =
    DreamClaimVersionSourceEntity(
        claimId = claimId,
        claimRevision = nextRevision,
        memoryId = memoryId,
        memoryRevision = memoryRevision,
        memorySemanticHash = memorySemanticHash,
        memoryEvidenceId = memoryEvidenceId,
        supportType = supportType,
        createdAtMs = nowMs,
    )

private fun DreamSnapshotEntity.matchesProjectionState(
    state: MemoryScopeStateEntity,
    maximumSourceMemoryEpoch: Long,
): Boolean = snapshotId == state.activeSnapshotId && scopeId == state.scopeId &&
    status == SNAPSHOT_ACTIVE && sourceMemoryEpoch in 0L..maximumSourceMemoryEpoch &&
    committedDreamRevision == state.dreamStateRevision && snapshotRevision == state.dreamStateRevision

private fun DreamSnapshotEntity.manifestReferencesOrNull(): Set<Pair<String, Long>>? {
    val root = try {
        REVIEW_JSON.parseToJsonElement(canonicalPayloadJson) as? JsonObject
    } catch (_: Exception) {
        null
    } ?: return null
    val manifest = root["manifest"] as? JsonArray ?: return null
    val result = linkedSetOf<Pair<String, Long>>()
    for (raw in manifest) {
        val entry = raw as? JsonObject ?: return null
        val claimId = entry.string("claim_id") ?: return null
        val revision = entry.long("claim_revision")?.takeIf { it > 0L } ?: return null
        if (!result.add(claimId to revision)) return null
    }
    return result
}

private fun DreamSnapshotEntity.toSummary(): DreamSnapshotSummary? = try {
    DreamSnapshotSummary(
        snapshotId = snapshotId,
        sourceMemoryEpoch = sourceMemoryEpoch,
        committedDreamRevision = committedDreamRevision,
        payloadHash = DreamSha256(payloadSha256),
        compilerRevision = compilerRevision,
        claimCount = claimCount,
        estimatedTokens = estimatedTokens,
        createdAtEpochMs = createdAtMs,
    )
} catch (_: Exception) {
    null
}

private fun snapshotDiff(
    scopeId: DreamScopeId,
    previous: DreamSnapshotEntity?,
    current: DreamSnapshotEntity?,
): DreamSnapshotDiffResult {
    if (current == null) return DreamSnapshotDiffResult.Available(emptyList())
    val currentDocument = current.toDocument(scopeId)
        ?: return DreamSnapshotDiffResult.Unavailable(DreamSnapshotDiffFailure.NON_CANONICAL_PAYLOAD)
    val previousDocument = previous?.toDocument(scopeId)
        ?: if (previous == null) null else return DreamSnapshotDiffResult.Unavailable(
            DreamSnapshotDiffFailure.NON_CANONICAL_PAYLOAD,
        )
    return DreamSnapshotDiff.compare(previousDocument, currentDocument)
}

private fun DreamSnapshotEntity.toDocument(scopeId: DreamScopeId): DreamSnapshotDocument? {
    val root = try {
        REVIEW_JSON.parseToJsonElement(canonicalPayloadJson) as? JsonObject
    } catch (_: Exception) {
        null
    } ?: return null
    val manifest = root["manifest"] ?: return null
    return try {
        DreamSnapshotDocument(
            scopeId = scopeId,
            snapshotId = snapshotId,
            schemaVersion = DREAM_SNAPSHOT_SCHEMA_VERSION,
            compilerRevision = compilerRevision,
            payloadJson = canonicalPayloadJson,
            payloadHash = DreamSha256(payloadSha256),
            manifestHash = DreamCanonicalJson.sha256(manifest),
            claimCount = claimCount,
        )
    } catch (_: Exception) {
        null
    }
}

private fun runsAreConsistent(state: MemoryScopeStateEntity, runs: List<DreamRunEntity>): Boolean {
    val activeId = state.activeRunId ?: return state.activeRunLeaseUntilMs == null &&
        runs.none { run -> run.status == "RUNNING" }
    val run = runs.singleOrNull { it.runId == activeId } ?: return false
    return state.activeRunLeaseUntilMs != null && run.scopeId == state.scopeId &&
        run.status == "RUNNING" && run.leaseOwner?.isNotBlank() == true &&
        run.startedAtMs != null && run.finishedAtMs == null && run.failureCode == null &&
        run.leaseUntilMs == state.activeRunLeaseUntilMs &&
        runs.count { candidate -> candidate.status == "RUNNING" } == 1
}

private fun DreamRunEntity.toUsageSummaryOrNull(): DreamRunUsageSummary? = try {
    requireCanonicalDreamRunId(runId)
    DreamRunUsageSummary(
        runId = runId,
        inputTokens = inputTokens,
        outputTokens = outputTokens,
        startedAtEpochMs = startedAtMs,
        finishedAtEpochMs = finishedAtMs,
        statusCode = status,
    )
} catch (_: Exception) {
    null
}

private fun JsonObject.string(key: String): String? = (this[key] as? JsonPrimitive)
    ?.takeIf(JsonPrimitive::isString)?.contentOrNull

private fun JsonObject.int(key: String): Int? = (this[key] as? JsonPrimitive)
    ?.takeUnless(JsonPrimitive::isString)?.intOrNull

private fun JsonObject.long(key: String): Long? = (this[key] as? JsonPrimitive)
    ?.takeUnless(JsonPrimitive::isString)?.longOrNull

private fun JsonObject.nullableLongValue(key: String): NullableLongValue? {
    val value = this[key] ?: return null
    if (value === JsonNull) return NullableLongValue(null)
    return (value as? JsonPrimitive)
        ?.takeUnless(JsonPrimitive::isString)
        ?.longOrNull
        ?.let(::NullableLongValue)
}

private data class NullableLongValue(val value: Long?)

private fun Long.toIntExactOrNull(): Int? = takeIf { it in 1L..Int.MAX_VALUE.toLong() }?.toInt()

private val REVIEW_JSON = Json { ignoreUnknownKeys = false; isLenient = false }
private val USER_REVIEWABLE_STATES = setOf(
    DreamClaimState.PENDING_REVIEW.name,
    DreamClaimState.ACTIVE_CONTEXTUAL.name,
)
private val DIRECT_REVEAL_APPROVALS = setOf(
    MemoryApprovalSource.MANUAL_UI.name,
    MemoryApprovalSource.USER_REVIEWED.name,
)
private val CLAIM_VERSION_KEYS = setOf(
    "claim_id",
    "claim_key",
    "confidence_permille",
    "epistemic_type",
    "reason",
    "revision",
    "source_manifest_hash",
    "state",
    "statement",
    "storage_class",
    "temporal_state",
    "title",
    "valid_from_epoch_ms",
    "valid_to_epoch_ms",
)
private const val MAX_REVIEW_PROJECTION_SOURCES = 32_768
private const val MAX_REVIEW_DETAIL_SOURCES = 16_384
private const val DEFAULT_REVIEW_COMPILER_REVISION = "dream-snapshot-compiler-v1"
private const val REVIEW_REJECT_REASON = "USER_REJECTED"
private const val REVIEW_CORRECTION_REASON = "USER_CORRECTION"
private const val REVIEW_CLEAR_REASON = "USER_CLEAR_DERIVED"
private const val SNAPSHOT_ACTIVE = "ACTIVE"
private const val SNAPSHOT_SUPERSEDED = "SUPERSEDED"
private const val SOURCE_DELETED_QUALITY = "SOURCE_DELETED"
