package me.rerere.rikkahub.memory.dreaming.store

import androidx.room.withTransaction
import kotlin.math.roundToInt
import kotlin.uuid.Uuid
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.dao.DreamDao
import me.rerere.rikkahub.data.db.dao.DreamExperienceDao
import me.rerere.rikkahub.data.db.dao.DreamSynthesisDao
import me.rerere.rikkahub.data.db.dao.MemoryDAO
import me.rerere.rikkahub.data.db.dao.MemoryV2Dao
import me.rerere.rikkahub.data.db.entity.DreamClaimEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimExperienceSourceEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimVersionEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimVersionSourceEntity
import me.rerere.rikkahub.data.db.entity.DreamRunEntity
import me.rerere.rikkahub.data.db.entity.DreamSnapshotEntity
import me.rerere.rikkahub.data.db.entity.MemoryEntity
import me.rerere.rikkahub.data.db.entity.MemoryRevisionEntity
import me.rerere.rikkahub.memory.MemoryApprovalSource
import me.rerere.rikkahub.memory.MemoryAttribution
import me.rerere.rikkahub.memory.MemoryKind
import me.rerere.rikkahub.memory.MemoryLifecycleStatus
import me.rerere.rikkahub.memory.MemorySourceIdentity
import me.rerere.rikkahub.memory.MemoryTruthStatus
import me.rerere.rikkahub.memory.dreaming.input.DreamDeterministicInvalidation
import me.rerere.rikkahub.memory.dreaming.input.DreamDeterministicInvalidationReason
import me.rerere.rikkahub.memory.dreaming.input.DreamInputBudget
import me.rerere.rikkahub.memory.dreaming.input.DreamInputBuildRequest
import me.rerere.rikkahub.memory.dreaming.input.DreamInputCandidate
import me.rerere.rikkahub.memory.dreaming.input.DreamInputCandidateOrigin
import me.rerere.rikkahub.memory.dreaming.experience.toDreamInputCandidate
import me.rerere.rikkahub.memory.dreaming.model.AuthorityChangeReason
import me.rerere.rikkahub.memory.dreaming.model.AuthorityEntityKind
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityFingerprintV1
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityMemory
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityPin
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthoritySource
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimHead
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimSourcePin
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimState
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimVersionCanonicalV1
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicType
import me.rerere.rikkahub.memory.dreaming.model.DreamRunFailureCode
import me.rerere.rikkahub.memory.dreaming.model.DreamRunMode
import me.rerere.rikkahub.memory.dreaming.model.DreamRunStatus
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamPairScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamSha256
import me.rerere.rikkahub.memory.dreaming.model.DreamStorageClass
import me.rerere.rikkahub.memory.dreaming.model.DreamSupportType
import me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisFence
import me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisMode
import me.rerere.rikkahub.memory.dreaming.model.DreamValidatedClaimVersion
import me.rerere.rikkahub.memory.dreaming.runtime.DreamingFeatureFlagSource
import me.rerere.rikkahub.memory.dreaming.runtime.allowsSynthesisGeneration
import me.rerere.rikkahub.memory.dreaming.source.DreamSourceLocator
import me.rerere.rikkahub.memory.dreaming.temporal.TemporalState

/** Room implementation with no provider dependency; every public method is one short transaction. */
class RoomDreamSynthesisStore(
    private val database: AppDatabase,
    private val dreamDao: DreamDao,
    private val experienceDao: DreamExperienceDao,
    private val synthesisDao: DreamSynthesisDao,
    private val memoryDao: MemoryDAO,
    private val memoryV2Dao: MemoryV2Dao,
    private val observerStore: DreamObserverStore,
    private val featureFlags: DreamingFeatureFlagSource,
    private val json: Json,
    private val idGenerator: () -> String = { Uuid.random().toString() },
) : DreamSynthesisStore {
    override suspend fun begin(request: BeginDreamSynthesisRequest): BeginDreamSynthesisResult {
        if (!generationEnabled(request.scopeId)) {
            return BeginDreamSynthesisResult.Rejected(DreamSynthesisStoreRejection.FEATURE_DISABLED)
        }
        if (DreamPairScopeId.parseOrNull(request.scopeId.value) != null) {
            return beginPair(request)
        }
        return database.withTransaction {
            val initial = observerStore.ensureScopeState(request.scopeId, request.attemptNowEpochMs)
            val stateBefore = dreamDao.getScopeState(request.scopeId.value)
                ?: return@withTransaction BeginDreamSynthesisResult.Rejected(
                    DreamSynthesisStoreRejection.STORE_CORRUPTION,
                )
            val pairStateBefore = DreamPairScopeId.parseOrNull(request.scopeId.value)?.let {
                experienceDao.getState(request.scopeId.value)
                    ?: return@withTransaction BeginDreamSynthesisResult.Rejected(
                        DreamSynthesisStoreRejection.STORE_CORRUPTION,
                    )
            }
            if (initial.memoryEpoch != stateBefore.memoryEpoch ||
                (pairStateBefore != null && (
                    stateBefore.memoryEpoch != pairStateBefore.experienceEpoch ||
                        stateBefore.observerCheckpointEpoch != pairStateBefore.observerCheckpointEpoch ||
                        stateBefore.lastAppliedMemoryEpoch != pairStateBefore.appliedExperienceEpoch ||
                        stateBefore.dreamStateRevision != pairStateBefore.profileRevision ||
                        stateBefore.activeSnapshotId != pairStateBefore.activeSnapshotId
                    ))
            ) {
                return@withTransaction BeginDreamSynthesisResult.Rejected(
                    DreamSynthesisStoreRejection.STORE_CORRUPTION,
                )
            }
            val appliedEpoch = pairStateBefore?.appliedExperienceEpoch ?: stateBefore.lastAppliedMemoryEpoch
            val effectiveMode = if (appliedEpoch == 0L) {
                DreamSynthesisMode.FULL
            } else {
                request.mode
            }
            val started = observerStore.startRun(
                StartDreamRunRequest(
                    runId = request.runId,
                    scopeId = request.scopeId,
                    mode = effectiveMode.toRunMode(),
                    leaseOwner = request.leaseOwner,
                    nowMs = request.attemptNowEpochMs,
                    leaseDurationMs = DREAM_SYNTHESIS_INITIAL_LEASE_MS,
                    sourceTimezoneId = request.sourceTimezoneId,
                ),
            )
            when (started) {
                is StartDreamRunResult.Terminal -> BeginDreamSynthesisResult.Terminal(
                    succeeded = started.run.status == DreamRunStatus.SUCCEEDED,
                )

                is StartDreamRunResult.Rejected -> BeginDreamSynthesisResult.Rejected(
                    started.reason.toSynthesisRejection(),
                )

                is StartDreamRunResult.Started,
                is StartDreamRunResult.Resumed,
                -> {
                    val run = dreamDao.getRunById(request.runId)
                        ?: return@withTransaction BeginDreamSynthesisResult.Rejected(
                            DreamSynthesisStoreRejection.RUN_NOT_FOUND,
                        )
                    val state = dreamDao.getScopeState(request.scopeId.value)
                        ?: return@withTransaction BeginDreamSynthesisResult.Rejected(
                            DreamSynthesisStoreRejection.STORE_CORRUPTION,
                        )
                    if (!run.isOwnedBy(request.scopeId, request.leaseOwner) ||
                        state.activeRunId != request.runId || run.status != DreamRunStatus.RUNNING.name ||
                        run.sourceTimezoneId != request.sourceTimezoneId || run.startedAtMs == null ||
                        run.startedAtMs > request.attemptNowEpochMs
                    ) {
                        return@withTransaction BeginDreamSynthesisResult.Rejected(
                            DreamSynthesisStoreRejection.FENCE_CONFLICT,
                        )
                    }
                    BeginDreamSynthesisResult.Ready(
                        DreamSynthesisFence(
                            scopeId = request.scopeId,
                            runId = request.runId,
                            leaseOwner = request.leaseOwner,
                            baseMemoryEpoch = pairStateBefore?.experienceEpoch ?: run.baseMemoryEpoch,
                            baseLastAppliedMemoryEpoch = pairStateBefore?.appliedExperienceEpoch
                                ?: state.lastAppliedMemoryEpoch,
                            baseDreamRevision = pairStateBefore?.profileRevision ?: run.baseDreamRevision,
                            expectedActiveSnapshotId = pairStateBefore?.activeSnapshotId
                                ?: state.activeSnapshotId,
                            frozenNowEpochMs = run.startedAtMs,
                            sourceTimezoneId = run.sourceTimezoneId,
                            mode = effectiveMode,
                        ),
                    )
                }
            }
        }
    }

    private suspend fun beginPair(request: BeginDreamSynthesisRequest): BeginDreamSynthesisResult =
        database.withTransaction {
            val state = experienceDao.getState(request.scopeId.value)
                ?: return@withTransaction BeginDreamSynthesisResult.Rejected(
                    DreamSynthesisStoreRejection.STORE_CORRUPTION,
                )
            val run = dreamDao.getRunById(request.runId)
                ?: return@withTransaction BeginDreamSynthesisResult.Rejected(
                    DreamSynthesisStoreRejection.RUN_NOT_FOUND,
                )
            if (run.scopeId != request.scopeId.value) {
                return@withTransaction BeginDreamSynthesisResult.Rejected(
                    DreamSynthesisStoreRejection.SCOPE_MISMATCH,
                )
            }
            val status = runCatching { DreamRunStatus.valueOf(run.status) }.getOrNull()
                ?: return@withTransaction BeginDreamSynthesisResult.Rejected(
                    DreamSynthesisStoreRejection.STORE_CORRUPTION,
                )
            if (status.isTerminal) {
                return@withTransaction BeginDreamSynthesisResult.Terminal(
                    succeeded = status == DreamRunStatus.SUCCEEDED,
                )
            }
            val effectiveMode = if (state.appliedExperienceEpoch == 0L) {
                DreamSynthesisMode.FULL
            } else {
                request.mode
            }
            if (run.mode != effectiveMode.toRunMode().name) {
                return@withTransaction BeginDreamSynthesisResult.Rejected(
                    DreamSynthesisStoreRejection.FENCE_CONFLICT,
                )
            }
            val leaseUntilMs = runCatching {
                Math.addExact(request.attemptNowEpochMs, DREAM_SYNTHESIS_INITIAL_LEASE_MS)
            }.getOrNull() ?: return@withTransaction BeginDreamSynthesisResult.Rejected(
                DreamSynthesisStoreRejection.STORE_CORRUPTION,
            )
            when (status) {
                DreamRunStatus.PENDING -> {
                    if (experienceDao.acquireLease(
                            pairScopeId = request.scopeId.value,
                            runId = request.runId,
                            leaseUntilMs = leaseUntilMs,
                            nowMs = request.attemptNowEpochMs,
                            reasonCode = AuthorityChangeReason.RUN_CLAIMED.name,
                        ) != 1
                    ) {
                        return@withTransaction BeginDreamSynthesisResult.Rejected(
                            DreamSynthesisStoreRejection.FENCE_CONFLICT,
                        )
                    }
                    if (experienceDao.startRunMirror(
                            runId = request.runId,
                            pairScopeId = request.scopeId.value,
                            baseExperienceEpoch = state.experienceEpoch,
                            baseObserverCheckpointEpoch = state.observerCheckpointEpoch,
                            baseProfileRevision = state.profileRevision,
                            leaseOwner = request.leaseOwner,
                            leaseUntilMs = leaseUntilMs,
                            nowMs = request.attemptNowEpochMs,
                            sourceTimezoneId = request.sourceTimezoneId,
                        ) != 1
                    ) {
                        experienceDao.releaseLease(
                            pairScopeId = request.scopeId.value,
                            runId = request.runId,
                            nowMs = request.attemptNowEpochMs,
                            reasonCode = AuthorityChangeReason.RUN_FINISHED.name,
                        )
                        return@withTransaction BeginDreamSynthesisResult.Rejected(
                            DreamSynthesisStoreRejection.FENCE_CONFLICT,
                        )
                    }
                }

                DreamRunStatus.RUNNING -> {
                    if (run.leaseOwner != request.leaseOwner || run.leaseUntilMs == null ||
                        run.leaseUntilMs <= request.attemptNowEpochMs ||
                        state.activeRunId != run.runId || state.activeRunLeaseUntilMs != run.leaseUntilMs
                    ) {
                        return@withTransaction BeginDreamSynthesisResult.Rejected(
                            DreamSynthesisStoreRejection.FENCE_CONFLICT,
                        )
                    }
                }

                else -> error("terminal Dream status handled above")
            }
            val startedRun = dreamDao.getRunById(request.runId)
                ?: return@withTransaction BeginDreamSynthesisResult.Rejected(
                    DreamSynthesisStoreRejection.RUN_NOT_FOUND,
                )
            val startedState = experienceDao.getState(request.scopeId.value)
                ?: return@withTransaction BeginDreamSynthesisResult.Rejected(
                    DreamSynthesisStoreRejection.STORE_CORRUPTION,
                )
            if (startedRun.status != DreamRunStatus.RUNNING.name ||
                startedRun.leaseOwner != request.leaseOwner || startedRun.startedAtMs == null ||
                startedRun.sourceTimezoneId != request.sourceTimezoneId ||
                startedState.activeRunId != request.runId ||
                startedState.activeRunLeaseUntilMs != startedRun.leaseUntilMs
            ) {
                return@withTransaction BeginDreamSynthesisResult.Rejected(
                    DreamSynthesisStoreRejection.FENCE_CONFLICT,
                )
            }
            BeginDreamSynthesisResult.Ready(
                DreamSynthesisFence(
                    scopeId = request.scopeId,
                    runId = request.runId,
                    leaseOwner = request.leaseOwner,
                    baseMemoryEpoch = startedRun.baseMemoryEpoch,
                    baseLastAppliedMemoryEpoch = state.appliedExperienceEpoch,
                    baseDreamRevision = startedRun.baseDreamRevision,
                    expectedActiveSnapshotId = state.activeSnapshotId,
                    frozenNowEpochMs = startedRun.startedAtMs,
                    sourceTimezoneId = startedRun.sourceTimezoneId,
                    mode = effectiveMode,
                ),
            )
        }

    override suspend fun readInputSeed(
        fence: DreamSynthesisFence,
        attemptNowEpochMs: Long,
    ): ReadDreamInputSeedResult {
        require(attemptNowEpochMs >= fence.frozenNowEpochMs)
        if (!generationEnabled(fence.scopeId)) {
            return ReadDreamInputSeedResult.Rejected(DreamSynthesisStoreRejection.FEATURE_DISABLED)
        }
        return database.withTransaction {
            val owned = when (val context = ownedContext(fence, attemptNowEpochMs)) {
                is OwnedContextResult.Ready -> context.run
                is OwnedContextResult.Rejected -> return@withTransaction ReadDreamInputSeedResult.Rejected(
                    context.storeReason,
                )
            }
            if (DreamPairScopeId.parseOrNull(fence.scopeId.value) != null) {
                val allClaims = loadClaims(fence.scopeId)
                    ?: return@withTransaction ReadDreamInputSeedResult.Rejected(
                        DreamSynthesisStoreRejection.STORE_CORRUPTION,
                    )
                val fromExclusive = if (fence.mode == DreamSynthesisMode.FULL) {
                    0L
                } else {
                    fence.baseAppliedExperienceEpoch
                }
                val experiences = experienceDao.listExperiences(
                    pairScopeId = fence.scopeId.value,
                    afterExclusiveEpoch = fromExclusive,
                    throughInclusiveEpoch = fence.baseExperienceEpoch,
                    limit = MAX_DREAM_INPUT_CANDIDATES + 1,
                )
                if (experiences.size > MAX_DREAM_INPUT_CANDIDATES) {
                    return@withTransaction ReadDreamInputSeedResult.Rejected(
                        DreamSynthesisStoreRejection.STORE_CORRUPTION,
                    )
                }
                val candidates = experiences.mapNotNull { experience ->
                    experience.toDreamInputCandidate(
                        scopeId = fence.scopeId,
                        origin = if (fence.mode == DreamSynthesisMode.FULL) {
                            DreamInputCandidateOrigin.FULL_REBUILD
                        } else {
                            DreamInputCandidateOrigin.AUTHORITY_CHANGE
                        },
                        json = json,
                    )
                }
                val invalidations = deterministicInvalidations(allClaims, fence)
                    ?: return@withTransaction ReadDreamInputSeedResult.Rejected(
                        DreamSynthesisStoreRejection.STORE_CORRUPTION,
                    )
                if (owned.checkpointEpoch != fence.baseExperienceEpoch &&
                    experienceDao.advanceRunCheckpoint(
                        runId = fence.runId,
                        pairScopeId = fence.scopeId.value,
                        leaseOwner = fence.leaseOwner,
                        expectedCheckpointEpoch = owned.checkpointEpoch,
                        targetCheckpointEpoch = fence.baseExperienceEpoch,
                        nowMs = attemptNowEpochMs,
                    ) != 1
                ) {
                    return@withTransaction ReadDreamInputSeedResult.Rejected(
                        DreamSynthesisStoreRejection.FENCE_CONFLICT,
                    )
                }
                return@withTransaction ReadDreamInputSeedResult.Ready(
                    DreamInputBuildRequest(
                        fence = fence,
                        candidates = candidates,
                        currentClaims = allClaims,
                        deterministicInvalidations = invalidations,
                        budget = DREAM_SYNTHESIS_INPUT_BUDGET,
                    ),
                )
            }
            // A first FULL rebuild is the upgrade/bootstrap boundary. Its authority comes from the
            // current scoped tables, not from a v45 journal that may legitimately have been pruned
            // before synthesis existed. Only incremental replay requires an unbroken epoch window.
            val changes = if (fence.mode == DreamSynthesisMode.INCREMENTAL) {
                dreamDao.listChanges(
                    scopeId = fence.scopeId.value,
                    afterExclusiveEpoch = fence.baseLastAppliedMemoryEpoch,
                    throughInclusiveEpoch = fence.baseMemoryEpoch,
                ).also { journal ->
                    if (!hasCompleteEpochs(
                            journal.map { it.memoryEpoch },
                            fence.baseLastAppliedMemoryEpoch,
                            fence.baseMemoryEpoch,
                        )
                    ) {
                        return@withTransaction ReadDreamInputSeedResult.Rejected(
                            DreamSynthesisStoreRejection.STORE_CORRUPTION,
                        )
                    }
                }
            } else {
                emptyList()
            }
            val allClaims = loadClaims(fence.scopeId)
                ?: return@withTransaction ReadDreamInputSeedResult.Rejected(
                    DreamSynthesisStoreRejection.STORE_CORRUPTION,
                )
            val memories = when (fence.mode) {
                DreamSynthesisMode.FULL -> memoryDao.getActiveConfirmedMemoriesForDream(
                    scopeId = fence.scopeId.value,
                    nowMs = fence.frozenNowEpochMs,
                    limit = MAX_DREAM_INPUT_CANDIDATES + 1,
                )

                DreamSynthesisMode.INCREMENTAL -> {
                    val ids = changes.asSequence()
                        .filter { it.entityKind == AuthorityEntityKind.MEMORY.name }
                        .mapNotNull { it.entityId.toIntOrNull() }
                        .distinct()
                        .sorted()
                        .toList()
                    if (ids.size > MAX_DREAM_INPUT_CANDIDATES) {
                        return@withTransaction ReadDreamInputSeedResult.Rejected(
                            DreamSynthesisStoreRejection.STORE_CORRUPTION,
                        )
                    }
                    val loaded = mutableListOf<MemoryEntity>()
                    for (chunk in ids.chunked(DREAM_MEMORY_ID_QUERY_CHUNK)) {
                        loaded += memoryDao.getMemoriesByIds(chunk, fence.scopeId.value)
                    }
                    loaded.filter { it.isActiveAt(fence.frozenNowEpochMs) }
                }
            }.sortedBy(MemoryEntity::id)
            if (memories.size > MAX_DREAM_INPUT_CANDIDATES) {
                return@withTransaction ReadDreamInputSeedResult.Rejected(
                    DreamSynthesisStoreRejection.STORE_CORRUPTION,
                )
            }
            val candidates = memories.map { memory ->
                val authority = memory.toDreamAuthority(fence.scopeId)
                    ?: return@withTransaction ReadDreamInputSeedResult.Rejected(
                        DreamSynthesisStoreRejection.STORE_CORRUPTION,
                    )
                val pin = authority.toPin()
                DreamInputCandidate(
                    origin = if (fence.mode == DreamSynthesisMode.FULL) {
                        DreamInputCandidateOrigin.FULL_REBUILD
                    } else {
                        DreamInputCandidateOrigin.AUTHORITY_CHANGE
                    },
                    memory = authority,
                    pin = pin,
                    sourceLocators = authority.sources.map { source -> source.toLocator(fence.scopeId) },
                    requireSourceReread = authority.sources.isNotEmpty(),
                )
            }
            val invalidations = deterministicInvalidations(allClaims, fence)
                ?: return@withTransaction ReadDreamInputSeedResult.Rejected(
                    DreamSynthesisStoreRejection.STORE_CORRUPTION,
                )
            if (owned.checkpointEpoch != fence.baseMemoryEpoch) {
                if (dreamDao.advanceRunCheckpoint(
                        runId = fence.runId,
                        scopeId = fence.scopeId.value,
                        leaseOwner = fence.leaseOwner,
                        expectedCheckpointEpoch = owned.checkpointEpoch,
                        targetCheckpointEpoch = fence.baseMemoryEpoch,
                        nowMs = attemptNowEpochMs,
                    ) != 1
                ) {
                    return@withTransaction ReadDreamInputSeedResult.Rejected(
                        DreamSynthesisStoreRejection.FENCE_CONFLICT,
                    )
                }
            }
            ReadDreamInputSeedResult.Ready(
                DreamInputBuildRequest(
                    fence = fence,
                    candidates = candidates,
                    currentClaims = allClaims,
                    deterministicInvalidations = invalidations,
                    budget = DREAM_SYNTHESIS_INPUT_BUDGET,
                ),
            )
        }
    }

    override suspend fun heartbeat(
        fence: DreamSynthesisFence,
        nowMs: Long,
        leaseDurationMs: Long,
    ): DreamSynthesisStoreResult {
        if (!generationEnabled(fence.scopeId)) {
            return DreamSynthesisStoreResult.Rejected(DreamSynthesisStoreRejection.FEATURE_DISABLED)
        }
        if (DreamPairScopeId.parseOrNull(fence.scopeId.value) != null) {
            return heartbeatPair(fence, nowMs, leaseDurationMs)
        }
        return when (val result = observerStore.heartbeat(
            DreamRunLeaseRequest(
                runId = fence.runId,
                scopeId = fence.scopeId,
                leaseOwner = fence.leaseOwner,
                nowMs = nowMs,
                leaseDurationMs = leaseDurationMs,
            ),
        )) {
            is HeartbeatDreamRunResult.Extended -> DreamSynthesisStoreResult.Accepted
            is HeartbeatDreamRunResult.Rejected -> DreamSynthesisStoreResult.Rejected(
                result.reason.toSynthesisRejection(),
            )
        }
    }

    private suspend fun heartbeatPair(
        fence: DreamSynthesisFence,
        nowMs: Long,
        leaseDurationMs: Long,
    ): DreamSynthesisStoreResult = try {
        database.withTransaction {
            when (val owned = ownedContext(fence, nowMs)) {
                is OwnedContextResult.Rejected ->
                    return@withTransaction DreamSynthesisStoreResult.Rejected(owned.storeReason)
                is OwnedContextResult.Ready -> Unit
            }
            val leaseUntilMs = runCatching { Math.addExact(nowMs, leaseDurationMs) }
                .getOrElse { throw PairLeaseAbort() }
            if (experienceDao.heartbeatLease(
                    pairScopeId = fence.scopeId.value,
                    runId = fence.runId,
                    leaseUntilMs = leaseUntilMs,
                    nowMs = nowMs,
                    reasonCode = AuthorityChangeReason.RUN_HEARTBEAT.name,
                ) != 1
            ) throw PairLeaseAbort()
            if (experienceDao.heartbeatRunMirror(
                    runId = fence.runId,
                    pairScopeId = fence.scopeId.value,
                    leaseOwner = fence.leaseOwner,
                    leaseUntilMs = leaseUntilMs,
                    nowMs = nowMs,
                ) != 1
            ) throw PairLeaseAbort()
            DreamSynthesisStoreResult.Accepted
        }
    } catch (_: PairLeaseAbort) {
        DreamSynthesisStoreResult.Rejected(DreamSynthesisStoreRejection.FENCE_CONFLICT)
    }

    override suspend fun markProviderDispatch(
        request: DreamProviderDispatchRequest,
    ): DreamSynthesisStoreResult {
        val fence = request.fence
        if (!generationEnabled(fence.scopeId)) {
            return DreamSynthesisStoreResult.Rejected(DreamSynthesisStoreRejection.FEATURE_DISABLED)
        }
        return database.withTransaction {
            val marked = synthesisDao.markRunProviderDispatch(
                runId = fence.runId,
                scopeId = fence.scopeId.value,
                leaseOwner = fence.leaseOwner,
                promptContractVersion = request.promptContractVersion,
                validatorVersion = request.validatorVersion,
                inputMemoryCount = request.inputMemoryCount,
                inputManifestHash = request.inputManifestHash.value,
                nowMs = request.markedAtEpochMs,
            )
            if (marked == 1) {
                DreamSynthesisStoreResult.Accepted
            } else {
                when (val owned = ownedContext(fence, request.markedAtEpochMs)) {
                    is OwnedContextResult.Rejected -> DreamSynthesisStoreResult.Rejected(owned.storeReason)
                    is OwnedContextResult.Ready -> {
                        val run = owned.run
                        if (run.promptContractVersion == request.promptContractVersion &&
                            run.validatorVersion == request.validatorVersion &&
                            run.inputMemoryCount == request.inputMemoryCount &&
                            run.inputManifestHash == request.inputManifestHash.value
                        ) {
                            DreamSynthesisStoreResult.Accepted
                        } else {
                            DreamSynthesisStoreResult.Rejected(DreamSynthesisStoreRejection.STORE_CORRUPTION)
                        }
                    }
                }
            }
        }
    }

    override suspend fun commit(request: DreamSynthesisCommitRequest): DreamSynthesisCommitResult {
        if (!generationEnabled(request.fence.scopeId)) {
            return DreamSynthesisCommitResult.Rejected(DreamSynthesisCommitRejection.LEASE_MISSING)
        }
        return try {
            database.withTransaction { commitOrAbort(request) }
        } catch (abort: CommitAbort) {
            DreamSynthesisCommitResult.Rejected(abort.reason)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            DreamSynthesisCommitResult.Rejected(DreamSynthesisCommitRejection.STORE_CORRUPTION)
        }
    }

    override suspend fun terminalizeConflict(
        fence: DreamSynthesisFence,
        reason: DreamSynthesisCommitRejection,
        nowMs: Long,
    ): DreamSynthesisStoreResult = failOwnedRun(
        fence = fence,
        failureCode = when (reason) {
            DreamSynthesisCommitRejection.MEMORY_EPOCH_CONFLICT ->
                DreamRunFailureCode.MEMORY_EPOCH_CONFLICT
            DreamSynthesisCommitRejection.LEASE_EXPIRED -> DreamRunFailureCode.LEASE_EXPIRED
            DreamSynthesisCommitRejection.LEASE_OWNER_MISMATCH -> DreamRunFailureCode.OWNER_MISMATCH
            else -> DreamRunFailureCode.STORE_FAILURE
        },
        nowMs = nowMs,
    )

    override suspend fun fail(
        fence: DreamSynthesisFence,
        failure: DreamSynthesisFailure,
        nowMs: Long,
    ): DreamSynthesisStoreResult = failOwnedRun(
        fence = fence,
        failureCode = if (!generationEnabled(fence.scopeId)) {
            DreamRunFailureCode.FEATURE_DISABLED
        } else {
            failure.toRunFailureCode()
        },
        nowMs = nowMs,
    )

    private suspend fun commitOrAbort(
        request: DreamSynthesisCommitRequest,
    ): DreamSynthesisCommitResult.Committed {
        val fence = request.fence
        val run = when (val context = ownedContext(fence, request.committedAtEpochMs)) {
            is OwnedContextResult.Ready -> context.run
            is OwnedContextResult.Rejected -> abort(context.commitReason)
        }
        if (run.checkpointEpoch != fence.baseMemoryEpoch) abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
        request.liveAuthorityPins.forEach { pin -> validatePin(pin, request.committedAtEpochMs) }
        request.historicalTransitionPins.forEach { pin -> validateHistoricalPin(pin, fence.scopeId) }
        val versions = (request.plan.upserts + request.plan.transitions.map { it.nextVersion })
            .sortedWith(compareBy({ it.claimId }, { it.nextRevision }))
        val versionedClaimIds = versions.mapTo(hashSetOf()) { it.claimId }
        request.plan.resultingClaims
            .filter { claim ->
                claim.state == DreamClaimState.ACTIVE_CONTEXTUAL && claim.claimId !in versionedClaimIds
            }
            .forEach { claim -> touchUnchangedActiveClaim(claim, request) }
        versions.forEach { version -> persistVersion(version, request) }

        val nextDreamRevision = fence.baseDreamRevision + 1L
        if (nextDreamRevision <= 0L) abort(DreamSynthesisCommitRejection.DREAM_REVISION_CONFLICT)
        val snapshotId = idGenerator()
        try {
            me.rerere.rikkahub.memory.dreaming.model.requireDreamStableId(snapshotId)
        } catch (_: Exception) {
            abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
        }
        synthesisDao.insertSnapshot(
            DreamSnapshotEntity(
                snapshotId = snapshotId,
                scopeId = fence.scopeId.value,
                snapshotRevision = nextDreamRevision,
                sourceMemoryEpoch = fence.baseMemoryEpoch,
                committedDreamRevision = nextDreamRevision,
                status = "ACTIVE",
                canonicalPayloadJson = request.snapshot.payloadJson,
                payloadSha256 = request.snapshot.payloadHash.value,
                compilerRevision = request.snapshot.compilerRevision,
                estimatedTokens = request.snapshot.estimatedTokens,
                claimCount = request.snapshot.claimCount,
                createdByRunId = fence.runId,
                createdAtMs = request.committedAtEpochMs,
                supersedesSnapshotId = fence.expectedActiveSnapshotId,
                reasonCode = SYNTHESIS_COMMIT_REASON,
            ),
        )
        fence.expectedActiveSnapshotId?.let { oldId ->
            if (synthesisDao.supersedeActiveSnapshot(oldId, fence.scopeId.value) != 1) {
                abort(DreamSynthesisCommitRejection.ACTIVE_SNAPSHOT_CONFLICT)
            }
        }
        if (synthesisDao.recordRunSynthesisAudit(
                runId = fence.runId,
                scopeId = fence.scopeId.value,
                leaseOwner = fence.leaseOwner,
                modelIdentityDigest = request.modelAudit.modelIdentityDigest.value,
                providerKind = request.modelAudit.providerKind,
                promptContractVersion = request.modelAudit.promptContractVersion,
                validatorVersion = request.modelAudit.validatorVersion,
                inputMemoryCount = request.inputMemoryCount,
                inputTokens = request.modelAudit.inputTokens?.toLong(),
                outputClaimCount = request.outputOperationCount,
                outputTokens = request.modelAudit.outputTokens?.toLong(),
                inputManifestHash = request.inputManifestHash.value,
                outputManifestHash = request.outputManifestHash.value,
                nowMs = request.committedAtEpochMs,
            ) != 1
        ) {
            abort(DreamSynthesisCommitRejection.LEASE_MISSING)
        }
        if (DreamPairScopeId.parseOrNull(fence.scopeId.value) != null) {
            if (experienceDao.advanceApplied(
                pairScopeId = fence.scopeId.value,
                expectedExperienceEpoch = fence.baseExperienceEpoch,
                expectedAppliedEpoch = fence.baseAppliedExperienceEpoch,
                targetEpoch = fence.baseExperienceEpoch,
                expectedProfileRevision = fence.baseDreamRevision,
                nextProfileRevision = nextDreamRevision,
                activeSnapshotId = snapshotId,
                remainingDebt = 0.0,
                nowMs = request.committedAtEpochMs,
            ) != 1
            ) {
                abort(classifyCommitCas(fence, request.committedAtEpochMs))
            }
        } else {
            if (synthesisDao.commitActiveSnapshotCas(
                    scopeId = fence.scopeId.value,
                    runId = fence.runId,
                    leaseOwner = fence.leaseOwner,
                    baseMemoryEpoch = fence.baseMemoryEpoch,
                    baseDreamRevision = fence.baseDreamRevision,
                    expectedLastAppliedMemoryEpoch = fence.baseLastAppliedMemoryEpoch,
                    expectedActiveSnapshotId = fence.expectedActiveSnapshotId,
                    newSnapshotId = snapshotId,
                    fullRebuildAtMs = request.committedAtEpochMs.takeIf {
                        fence.mode == DreamSynthesisMode.FULL
                    },
                    reasonCode = SYNTHESIS_COMMIT_REASON,
                    nowMs = request.committedAtEpochMs,
                ) != 1
            ) {
                abort(classifyCommitCas(fence, request.committedAtEpochMs))
            }
        }
        finishCommittedSynthesisRun(fence, request.committedAtEpochMs)
        return DreamSynthesisCommitResult.Committed(snapshotId, nextDreamRevision)
    }

    /**
     * A first FULL rebuild validates the bounded authority tables rather than an old journal that
     * may legitimately be absent after upgrade. Only this post-snapshot path may close such a run
     * without replaying that journal; every write remains inside the enclosing commit transaction.
     */
    private suspend fun finishCommittedSynthesisRun(fence: DreamSynthesisFence, nowMs: Long) {
        if (DreamPairScopeId.parseOrNull(fence.scopeId.value) != null) {
            val run = dreamDao.getRunById(fence.runId)
                ?: abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
            if (run.scopeId != fence.scopeId.value || run.mode != fence.mode.toRunMode().name ||
                run.baseMemoryEpoch != fence.baseExperienceEpoch ||
                run.baseDreamRevision != fence.baseDreamRevision ||
                run.checkpointEpoch != fence.baseExperienceEpoch
            ) {
                abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
            }
            if (experienceDao.finishRunMirror(
                    runId = fence.runId,
                    pairScopeId = fence.scopeId.value,
                    leaseOwner = fence.leaseOwner,
                    terminalStatus = DreamRunStatus.SUCCEEDED.name,
                    failureCode = null,
                    nowMs = nowMs,
                ) != 1
            ) {
                abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
            }
            if (experienceDao.releaseLease(
                    pairScopeId = fence.scopeId.value,
                    runId = fence.runId,
                    nowMs = nowMs,
                    reasonCode = AuthorityChangeReason.RUN_FINISHED.name,
                ) != 1
            ) {
                abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
            }
            return
        }
        if (fence.mode != DreamSynthesisMode.FULL) {
            val finished = observerStore.finish(
                FinishDreamRunRequest(
                    runId = fence.runId,
                    scopeId = fence.scopeId,
                    leaseOwner = fence.leaseOwner,
                    outcome = DreamRunFinishOutcome.SUCCEEDED,
                    nowMs = nowMs,
                ),
            )
            if (finished !is FinishDreamRunResult.Finished ||
                finished.run.status != DreamRunStatus.SUCCEEDED
            ) {
                abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
            }
            return
        }

        val run = dreamDao.getRunById(fence.runId)
            ?: abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
        if (run.scopeId != fence.scopeId.value || run.mode != DreamRunMode.FULL.name ||
            run.baseMemoryEpoch != fence.baseMemoryEpoch ||
            run.baseDreamRevision != fence.baseDreamRevision ||
            run.checkpointEpoch != fence.baseMemoryEpoch
        ) {
            abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
        }
        if (dreamDao.finishRunMirror(
                runId = fence.runId,
                scopeId = fence.scopeId.value,
                leaseOwner = fence.leaseOwner,
                terminalStatus = DreamRunStatus.SUCCEEDED.name,
                failureCode = null,
                nowMs = nowMs,
            ) != 1
        ) {
            abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
        }
        if (dreamDao.advanceObserverCheckpoint(
                scopeId = fence.scopeId.value,
                runId = fence.runId,
                expectedMemoryEpoch = fence.baseMemoryEpoch,
                expectedCheckpointEpoch = run.baseObserverCheckpointEpoch,
                targetCheckpointEpoch = fence.baseMemoryEpoch,
                reasonCode = AuthorityChangeReason.OBSERVER_CHECKPOINT_ADVANCED.name,
                nowMs = nowMs,
            ) != 1
        ) {
            abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
        }
        if (dreamDao.releaseScopeLease(
                scopeId = fence.scopeId.value,
                runId = fence.runId,
                reasonCode = AuthorityChangeReason.OBSERVER_CHECKPOINT_ADVANCED.name,
                nowMs = nowMs,
            ) != 1
        ) {
            abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
        }
    }

    private suspend fun touchUnchangedActiveClaim(
        claim: DreamClaimHead,
        request: DreamSynthesisCommitRequest,
    ) {
        val current = synthesisDao.getClaim(claim.claimId, request.fence.scopeId.value)
            ?: abort(DreamSynthesisCommitRejection.CLAIM_REVISION_CONFLICT)
        val persisted = current.toModel(request.fence.scopeId)
            ?: abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
        // An unchanged head is immutable at this revision, including its complete provenance.
        // Rechecking only the head hash would permit a caller to substitute a different live pin
        // without writing a new ClaimVersion/source history row.
        if (persisted != claim ||
            current.state != DreamClaimState.ACTIVE_CONTEXTUAL.name ||
            current.lastValidatedMemoryEpoch > request.fence.baseMemoryEpoch
        ) {
            abort(DreamSynthesisCommitRejection.CLAIM_REVISION_CONFLICT)
        }
        if (current.lastValidatedMemoryEpoch == request.fence.baseMemoryEpoch) return
        if (synthesisDao.touchClaimValidationEpochCas(
                scopeId = request.fence.scopeId.value,
                claimId = claim.claimId,
                expectedRevision = claim.revision,
                targetEpoch = request.fence.baseMemoryEpoch,
                nowMs = request.committedAtEpochMs,
            ) != 1
        ) {
            abort(DreamSynthesisCommitRejection.CLAIM_REVISION_CONFLICT)
        }
    }

    private suspend fun persistVersion(
        version: DreamValidatedClaimVersion,
        request: DreamSynthesisCommitRequest,
    ) {
        val fence = request.fence
        val canonical = DreamClaimVersionCanonicalV1.encode(version)
        val resulting = request.plan.resultingClaims.singleOrNull {
            it.claimId == version.claimId && it.revision == version.nextRevision
        } ?: abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
        if (resulting.versionHash != canonical.contentHash ||
            resulting.state != version.nextState || resulting.sources != version.sources
        ) {
            abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
        }
        val current = synthesisDao.getClaim(version.claimId, fence.scopeId.value)
        if (version.expectedPreviousRevision == null) {
            if (current != null || synthesisDao.getClaimByKey(fence.scopeId.value, version.claimKey) != null) {
                abort(DreamSynthesisCommitRejection.CLAIM_REVISION_CONFLICT)
            }
            synthesisDao.insertClaim(version.toEntity(request, learnedAtMs = request.committedAtEpochMs))
        } else {
            if (current == null || current.claimRevision != version.expectedPreviousRevision) {
                abort(DreamSynthesisCommitRejection.CLAIM_REVISION_CONFLICT)
            }
            if (synthesisDao.updateClaimHeadCas(
                    claimId = version.claimId,
                    scopeId = fence.scopeId.value,
                    expectedClaimRevision = version.expectedPreviousRevision,
                    nextClaimRevision = version.nextRevision,
                    claimKey = version.claimKey,
                    storageClass = version.storageClass.name,
                    epistemicType = version.epistemicType.name,
                    subjectKind = version.subjectKind.name,
                    profileSection = version.profileSection,
                    epistemicOrigin = version.epistemicOrigin.name,
                    contentType = version.contentType.name,
                    title = version.title,
                    statement = version.statement,
                    state = version.nextState.name,
                    confidence = version.confidencePermille / 1_000.0,
                    temporalState = version.temporalState.name,
                    validFromMs = version.validFromEpochMs,
                    validToMs = version.validToEpochMs,
                    learnedAtMs = current.learnedAtMs,
                    sourceTimezone = fence.sourceTimezoneId,
                    claimHash = canonical.contentHash.value,
                    createdByRunId = fence.runId,
                    lastValidatedMemoryEpoch = fence.baseMemoryEpoch,
                    invalidatedAtMs = request.committedAtEpochMs.takeIf {
                        version.nextState !in setOf(
                            DreamClaimState.ACTIVE_CONTEXTUAL,
                            DreamClaimState.PENDING_REVIEW,
                        )
                    },
                    invalidationReason = version.reason.name.takeIf {
                        version.nextState !in setOf(
                            DreamClaimState.ACTIVE_CONTEXTUAL,
                            DreamClaimState.PENDING_REVIEW,
                        )
                    },
                    nowMs = request.committedAtEpochMs,
                ) != 1
            ) {
                abort(DreamSynthesisCommitRejection.CLAIM_REVISION_CONFLICT)
            }
        }
        synthesisDao.insertClaimVersion(
            DreamClaimVersionEntity(
                claimId = version.claimId,
                claimRevision = version.nextRevision,
                canonicalClaimJson = canonical.canonicalClaimJson,
                contentHash = canonical.contentHash.value,
                sourceManifestHash = canonical.sourceManifestHash.value,
                reasonCode = version.reason.name,
                createdByRunId = fence.runId,
                createdAtMs = request.committedAtEpochMs,
            ),
        )
        if (DreamPairScopeId.parseOrNull(fence.scopeId.value) != null) {
            synthesisDao.insertClaimExperienceSources(
                version.sources.map { source ->
                    val experience = experienceDao.getExperience(
                        source.authority.memoryId,
                        fence.scopeId.value,
                    ) ?: abort(DreamSynthesisCommitRejection.EVIDENCE_TOMBSTONED)
                    if (experience.experienceEpoch != source.authority.expectedRevision) {
                        abort(DreamSynthesisCommitRejection.EVIDENCE_REVISION_MISMATCH)
                    }
                    DreamClaimExperienceSourceEntity(
                        claimId = version.claimId,
                        claimRevision = version.nextRevision,
                        experienceId = experience.experienceId,
                        experienceEpoch = experience.experienceEpoch,
                        contentDigest = experience.contentDigest,
                        sourceManifestHash = source.authority.expectedSourceManifestHash.value,
                        supportType = source.supportType.name,
                        createdAtMs = request.committedAtEpochMs,
                    )
                },
            )
        } else {
            synthesisDao.insertClaimVersionSources(
                version.sources.map { source ->
                    DreamClaimVersionSourceEntity(
                        claimId = version.claimId,
                        claimRevision = version.nextRevision,
                        memoryId = source.authority.memoryId.toIntOrNull()
                            ?: abort(DreamSynthesisCommitRejection.STORE_CORRUPTION),
                        memoryRevision = source.authority.expectedRevision.toIntExactOrAbort(),
                        memorySemanticHash = source.authority.expectedAuthorityFingerprint.value,
                        memoryEvidenceId = null,
                        supportType = source.supportType.name,
                        createdAtMs = request.committedAtEpochMs,
                    )
                },
            )
        }
    }

    private fun DreamValidatedClaimVersion.toEntity(
        request: DreamSynthesisCommitRequest,
        learnedAtMs: Long,
    ): DreamClaimEntity {
        val canonical = DreamClaimVersionCanonicalV1.encode(this)
        return DreamClaimEntity(
            claimId = claimId,
            scopeId = request.fence.scopeId.value,
            claimRevision = nextRevision,
            claimKey = claimKey,
            storageClass = storageClass.name,
            epistemicType = epistemicType.name,
            subjectKind = subjectKind.name,
            profileSection = profileSection,
            epistemicOrigin = epistemicOrigin.name,
            contentType = contentType.name,
            title = title,
            statement = statement,
            state = nextState.name,
            confidence = confidencePermille / 1_000.0,
            temporalState = temporalState.name,
            validFromMs = validFromEpochMs,
            validToMs = validToEpochMs,
            learnedAtMs = learnedAtMs,
            sourceTimezone = request.fence.sourceTimezoneId,
            claimHash = canonical.contentHash.value,
            createdByRunId = request.fence.runId,
            lastValidatedMemoryEpoch = request.fence.baseMemoryEpoch,
            invalidatedAtMs = request.committedAtEpochMs.takeIf {
                nextState !in setOf(DreamClaimState.ACTIVE_CONTEXTUAL, DreamClaimState.PENDING_REVIEW)
            },
            invalidationReason = reason.name.takeIf {
                nextState !in setOf(DreamClaimState.ACTIVE_CONTEXTUAL, DreamClaimState.PENDING_REVIEW)
            },
            createdAtMs = request.committedAtEpochMs,
            updatedAtMs = request.committedAtEpochMs,
        )
    }

    private suspend fun validatePin(pin: DreamAuthorityPin, nowMs: Long) {
        if (DreamPairScopeId.parseOrNull(pin.scopeId.value) != null) {
            val experience = experienceDao.getExperience(pin.memoryId, pin.scopeId.value)
                ?: abort(DreamSynthesisCommitRejection.EVIDENCE_TOMBSTONED)
            if (experience.status == "DISCARDED") {
                abort(DreamSynthesisCommitRejection.EVIDENCE_TOMBSTONED)
            }
            if (experience.experienceEpoch != pin.expectedRevision) {
                abort(DreamSynthesisCommitRejection.EVIDENCE_REVISION_MISMATCH)
            }
            val projected = experience.toDreamInputCandidate(
                scopeId = pin.scopeId,
                origin = DreamInputCandidateOrigin.AUTHORITY_CHANGE,
                json = json,
            ) ?: abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
            if (projected.pin.expectedAuthorityFingerprint != pin.expectedAuthorityFingerprint) {
                abort(DreamSynthesisCommitRejection.EVIDENCE_FINGERPRINT_MISMATCH)
            }
            if (projected.pin.expectedSourceManifestHash != pin.expectedSourceManifestHash) {
                abort(DreamSynthesisCommitRejection.EVIDENCE_SOURCE_MANIFEST_MISMATCH)
            }
            return
        }
        val memoryId = pin.memoryId.toIntOrNull()
            ?: abort(DreamSynthesisCommitRejection.EVIDENCE_REVISION_MISMATCH)
        val memory = memoryDao.getMemoryById(memoryId, pin.scopeId.value)
            ?: abort(DreamSynthesisCommitRejection.EVIDENCE_TOMBSTONED)
        if (pin.scopeId != DreamScopeId.requireCanonical(memory.assistantId)) {
            abort(DreamSynthesisCommitRejection.EVIDENCE_SCOPE_MISMATCH)
        }
        if (memory.revision.toLong() != pin.expectedRevision ||
            pin.expectedRevision > Int.MAX_VALUE
        ) {
            abort(DreamSynthesisCommitRejection.EVIDENCE_REVISION_MISMATCH)
        }
        if (!memory.isActiveAt(nowMs)) abort(DreamSynthesisCommitRejection.EVIDENCE_TOMBSTONED)
        if (memoryV2Dao.findRevision(memoryId, memory.revision, pin.scopeId.value) == null) {
            abort(DreamSynthesisCommitRejection.EVIDENCE_REVISION_MISMATCH)
        }
        val authority = memory.toDreamAuthority(pin.scopeId)
            ?: abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
        if (DreamAuthorityFingerprintV1.compute(authority) != pin.expectedAuthorityFingerprint) {
            abort(DreamSynthesisCommitRejection.EVIDENCE_FINGERPRINT_MISMATCH)
        }
        if (DreamAuthorityFingerprintV1.sourceManifestHash(authority.sources) !=
            pin.expectedSourceManifestHash
        ) {
            abort(DreamSynthesisCommitRejection.EVIDENCE_SOURCE_MANIFEST_MISMATCH)
        }
    }

    /** Historical invalidation provenance requires only scoped referential existence. */
    private suspend fun validateHistoricalPin(pin: DreamAuthorityPin, scopeId: DreamScopeId) {
        if (pin.scopeId != scopeId) {
            abort(DreamSynthesisCommitRejection.EVIDENCE_SCOPE_MISMATCH)
        }
        if (DreamPairScopeId.parseOrNull(scopeId.value) != null) {
            val experience = experienceDao.getExperience(pin.memoryId, scopeId.value)
                ?: abort(DreamSynthesisCommitRejection.EVIDENCE_REVISION_MISMATCH)
            if (experience.experienceEpoch != pin.expectedRevision) {
                abort(DreamSynthesisCommitRejection.EVIDENCE_REVISION_MISMATCH)
            }
            return
        }
        val memoryId = pin.memoryId.toIntOrNull()
            ?: abort(DreamSynthesisCommitRejection.EVIDENCE_REVISION_MISMATCH)
        val revision = pin.expectedRevision
        if (revision !in 1L..Int.MAX_VALUE.toLong() ||
            memoryV2Dao.findRevision(memoryId, revision.toInt(), scopeId.value) == null
        ) {
            abort(DreamSynthesisCommitRejection.EVIDENCE_REVISION_MISMATCH)
        }
    }

    private suspend fun loadClaims(scopeId: DreamScopeId): List<DreamClaimHead>? {
        val entities = synthesisDao.listClaims(scopeId.value, MAX_DREAM_CLAIMS + 1)
        if (entities.size > MAX_DREAM_CLAIMS) return null
        return entities.map { entity -> entity.toModel(scopeId) ?: return null }
            .sortedWith(compareBy({ it.claimKey }, { it.claimId }, { it.revision }))
    }

    private suspend fun DreamClaimEntity.toModel(scopeId: DreamScopeId): DreamClaimHead? {
        val state = enumOrNull<DreamClaimState>(state) ?: return null
        val sources = if (state == DreamClaimState.TOMBSTONED) {
            emptyList()
        } else if (DreamPairScopeId.parseOrNull(scopeId.value) != null) {
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
                    supportType = enumOrNull<DreamSupportType>(source.supportType) ?: return null,
                    directAuthority = true,
                )
            }
        } else {
            synthesisDao.listClaimVersionSources(claimId, claimRevision).map { source ->
                val revision = memoryV2Dao.findRevision(source.memoryId, source.memoryRevision, scopeId.value)
                    ?: return null
                val manifest = revision.toAuthoritySources() ?: return null
                DreamClaimSourcePin(
                    authority = DreamAuthorityPin(
                        scopeId = scopeId,
                        memoryId = source.memoryId.toString(),
                        expectedRevision = source.memoryRevision.toLong(),
                        expectedAuthorityFingerprint = try {
                            DreamSha256(source.memorySemanticHash)
                        } catch (_: Exception) {
                            return null
                        },
                        expectedSourceManifestHash = DreamAuthorityFingerprintV1.sourceManifestHash(manifest),
                    ),
                    supportType = enumOrNull<DreamSupportType>(source.supportType) ?: return null,
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
                storageClass = enumValueOf<DreamStorageClass>(storageClass),
                epistemicType = enumValueOf<DreamEpistemicType>(epistemicType),
                subjectKind = enumValueOf(subjectKind),
                profileSection = profileSection,
                epistemicOrigin = enumValueOf(epistemicOrigin),
                contentType = enumValueOf(contentType),
                state = state,
                title = title,
                statement = statement,
                confidencePermille = confidencePermille,
                temporalState = enumValueOf<TemporalState>(temporalState),
                validFromEpochMs = validFromMs,
                validToEpochMs = validToMs,
                versionHash = DreamSha256(claimHash),
                sources = sources,
            )
        } catch (_: Exception) {
            null
        }
    }

    private suspend fun deterministicInvalidations(
        claims: List<DreamClaimHead>,
        fence: DreamSynthesisFence,
    ): List<DreamDeterministicInvalidation>? {
        val invalidations = mutableListOf<DreamDeterministicInvalidation>()
        for (claim in claims) {
            if (claim.state in setOf(
                    DreamClaimState.TOMBSTONED,
                    DreamClaimState.INVALID,
                    DreamClaimState.SUPERSEDED,
                )
            ) {
                continue
            }
            var reason: DreamDeterministicInvalidationReason? = null
            for (source in claim.sources) {
                reason = sourceInvalidationReason(source, fence)
                if (reason != null) break
            }
            if (reason != null) {
                invalidations += DreamDeterministicInvalidation(claim.claimId, claim.revision, reason)
            }
        }
        return invalidations.sortedBy(DreamDeterministicInvalidation::claimId)
    }

    private suspend fun sourceInvalidationReason(
        source: DreamClaimSourcePin,
        fence: DreamSynthesisFence,
    ): DreamDeterministicInvalidationReason? {
        if (DreamPairScopeId.parseOrNull(fence.scopeId.value) != null) {
            val experience = experienceDao.getExperience(source.authority.memoryId, fence.scopeId.value)
                ?: return DreamDeterministicInvalidationReason.SOURCE_MISSING
            if (experience.status == "DISCARDED") {
                return DreamDeterministicInvalidationReason.SOURCE_TOMBSTONED
            }
            if (experience.experienceEpoch != source.authority.expectedRevision) {
                return DreamDeterministicInvalidationReason.SOURCE_REVISION_CHANGED
            }
            val projected = experience.toDreamInputCandidate(
                scopeId = fence.scopeId,
                origin = DreamInputCandidateOrigin.AUTHORITY_CHANGE,
                json = json,
            ) ?: return DreamDeterministicInvalidationReason.SOURCE_HASH_CHANGED
            if (projected.pin.expectedAuthorityFingerprint != source.authority.expectedAuthorityFingerprint ||
                projected.pin.expectedSourceManifestHash != source.authority.expectedSourceManifestHash
            ) {
                return DreamDeterministicInvalidationReason.SOURCE_HASH_CHANGED
            }
            return null
        }
        val memoryId = source.authority.memoryId.toIntOrNull()
            ?: return DreamDeterministicInvalidationReason.SOURCE_MISSING
        val memory = memoryDao.getMemoryById(memoryId, fence.scopeId.value)
            ?: return DreamDeterministicInvalidationReason.SOURCE_MISSING
        if (memory.lifecycleStatus != MemoryLifecycleStatus.ACTIVE.name) {
            return DreamDeterministicInvalidationReason.SOURCE_TOMBSTONED
        }
        if (memory.expiresAtMs != null && memory.expiresAtMs <= fence.frozenNowEpochMs) {
            return DreamDeterministicInvalidationReason.SOURCE_EXPIRED
        }
        if (memory.revision.toLong() != source.authority.expectedRevision) {
            return DreamDeterministicInvalidationReason.SOURCE_REVISION_CHANGED
        }
        val authority = memory.toDreamAuthority(fence.scopeId)
            ?: return DreamDeterministicInvalidationReason.SOURCE_HASH_CHANGED
        if (DreamAuthorityFingerprintV1.compute(authority) != source.authority.expectedAuthorityFingerprint ||
            DreamAuthorityFingerprintV1.sourceManifestHash(authority.sources) !=
            source.authority.expectedSourceManifestHash
        ) {
            return DreamDeterministicInvalidationReason.SOURCE_HASH_CHANGED
        }
        return null
    }

    private fun MemoryEntity.toDreamAuthority(scopeId: DreamScopeId): DreamAuthorityMemory? {
        val projectedUpdatedAtMs = dreamUpdatedAtEpochMsOrNull() ?: return null
        return try {
            DreamAuthorityMemory(
                scopeId = scopeId,
                memoryId = id.toString(),
                revision = revision.toLong(),
                title = title,
                content = content,
                kind = enumValueOf<MemoryKind>(memoryKind),
                attribution = enumValueOf<MemoryAttribution>(attribution),
                truthStatus = enumValueOf<MemoryTruthStatus>(truthStatus),
                lifecycleStatus = enumValueOf<MemoryLifecycleStatus>(lifecycleStatus),
                approvalSource = enumValueOf<MemoryApprovalSource>(approvalSource),
                tags = json.decodeStringList(tagsJson),
                createdAtEpochMs = createdAtMs,
                updatedAtEpochMs = projectedUpdatedAtMs,
                occurredAtEpochMs = occurredAtMs,
                expiresAtEpochMs = expiresAtMs,
                originAssistantId = originAssistantId,
                participants = json.decodeStringList(participantsJson),
                outcome = outcome,
                sources = json.decodeSourceIdentities(sourceIdentitiesJson).map { it.toAuthoritySource() },
                // Hard-deleted authority has no row. Non-active rows retain their explicit lifecycle
                // and are rejected by the active-authority projection before reaching this mapping.
                tombstoned = false,
            )
        } catch (_: Exception) {
            null
        }
    }

    private fun DreamAuthorityMemory.toPin() = DreamAuthorityPin(
        scopeId = scopeId,
        memoryId = memoryId,
        expectedRevision = revision,
        expectedAuthorityFingerprint = DreamAuthorityFingerprintV1.compute(this),
        expectedSourceManifestHash = DreamAuthorityFingerprintV1.sourceManifestHash(sources),
    )

    private fun MemoryRevisionEntity.toAuthoritySources(): List<DreamAuthoritySource>? = try {
        json.decodeSourceIdentities(sourceIdentitiesJson).map { it.toAuthoritySource() }
    } catch (_: Exception) {
        null
    }

    private fun MemorySourceIdentity.toAuthoritySource() = DreamAuthoritySource(
        conversationId = conversationId,
        messageId = messageId,
        role = role,
        sourceKind = sourceKind,
        consumedTextDigest = DreamSha256(consumedTextDigest),
        evidenceGroupId = evidenceGroupId,
    )

    private fun DreamAuthoritySource.toLocator(scopeId: DreamScopeId) = DreamSourceLocator(
        scopeId = scopeId,
        conversationId = conversationId,
        messageId = messageId,
        role = role,
        sourceKind = sourceKind,
        expectedConsumedTextDigest = consumedTextDigest,
        evidenceGroupId = evidenceGroupId,
    )

    private fun Json.decodeSourceIdentities(raw: String): List<MemorySourceIdentity> =
        decodeFromString<List<MemorySourceIdentity>>(raw)

    private fun Json.decodeStringList(raw: String): List<String> = decodeFromString<List<String>>(raw)

    private fun MemoryEntity.isActiveAt(nowMs: Long): Boolean =
        lifecycleStatus == MemoryLifecycleStatus.ACTIVE.name &&
            truthStatus == MemoryTruthStatus.CONFIRMED.name &&
            (expiresAtMs == null || expiresAtMs > nowMs)

    private suspend fun ownedContext(fence: DreamSynthesisFence, nowMs: Long): OwnedContextResult {
        val pairState = DreamPairScopeId.parseOrNull(fence.scopeId.value)?.let {
            experienceDao.getState(fence.scopeId.value)
                ?: return ownedRejected(
                    DreamSynthesisStoreRejection.RUN_NOT_FOUND,
                    DreamSynthesisCommitRejection.LEASE_MISSING,
                )
        }
        val activeRunId: String?
        val activeRunLeaseUntilMs: Long?
        if (pairState != null) {
            if (pairState.experienceEpoch != fence.baseExperienceEpoch) return ownedRejected(
                DreamSynthesisStoreRejection.FENCE_CONFLICT,
                DreamSynthesisCommitRejection.MEMORY_EPOCH_CONFLICT,
            )
            if (pairState.appliedExperienceEpoch != fence.baseAppliedExperienceEpoch ||
                pairState.profileRevision != fence.baseDreamRevision
            ) return ownedRejected(
                DreamSynthesisStoreRejection.FENCE_CONFLICT,
                DreamSynthesisCommitRejection.DREAM_REVISION_CONFLICT,
            )
            if (pairState.activeSnapshotId != fence.expectedActiveSnapshotId) return ownedRejected(
                DreamSynthesisStoreRejection.FENCE_CONFLICT,
                DreamSynthesisCommitRejection.ACTIVE_SNAPSHOT_CONFLICT,
            )
            activeRunId = pairState.activeRunId
            activeRunLeaseUntilMs = pairState.activeRunLeaseUntilMs
        } else {
            val state = dreamDao.getScopeState(fence.scopeId.value)
                ?: return ownedRejected(
                    DreamSynthesisStoreRejection.RUN_NOT_FOUND,
                    DreamSynthesisCommitRejection.LEASE_MISSING,
                )
            if (state.memoryEpoch != fence.baseMemoryEpoch) return ownedRejected(
                DreamSynthesisStoreRejection.FENCE_CONFLICT,
                DreamSynthesisCommitRejection.MEMORY_EPOCH_CONFLICT,
            )
            if (state.lastAppliedMemoryEpoch != fence.baseLastAppliedMemoryEpoch ||
                state.dreamStateRevision != fence.baseDreamRevision
            ) return ownedRejected(
                DreamSynthesisStoreRejection.FENCE_CONFLICT,
                DreamSynthesisCommitRejection.DREAM_REVISION_CONFLICT,
            )
            if (state.activeSnapshotId != fence.expectedActiveSnapshotId) return ownedRejected(
                DreamSynthesisStoreRejection.FENCE_CONFLICT,
                DreamSynthesisCommitRejection.ACTIVE_SNAPSHOT_CONFLICT,
            )
            activeRunId = state.activeRunId
            activeRunLeaseUntilMs = state.activeRunLeaseUntilMs
        }
        if (activeRunId != fence.runId || activeRunLeaseUntilMs == null) return ownedRejected(
            DreamSynthesisStoreRejection.FENCE_CONFLICT,
            DreamSynthesisCommitRejection.LEASE_MISSING,
        )
        if (activeRunLeaseUntilMs <= nowMs) return ownedRejected(
            DreamSynthesisStoreRejection.LEASE_EXPIRED,
            DreamSynthesisCommitRejection.LEASE_EXPIRED,
        )
        val run = dreamDao.getRunById(fence.runId) ?: return ownedRejected(
            DreamSynthesisStoreRejection.RUN_NOT_FOUND,
            DreamSynthesisCommitRejection.RUN_NOT_RUNNING,
        )
        if (run.scopeId != fence.scopeId.value) return ownedRejected(
            DreamSynthesisStoreRejection.SCOPE_MISMATCH,
            DreamSynthesisCommitRejection.FENCE_CONFLICT,
        )
        if (run.status != DreamRunStatus.RUNNING.name) return ownedRejected(
            DreamSynthesisStoreRejection.RUN_NOT_RUNNING,
            DreamSynthesisCommitRejection.RUN_NOT_RUNNING,
        )
        if (run.leaseOwner != fence.leaseOwner) return ownedRejected(
            DreamSynthesisStoreRejection.OWNER_MISMATCH,
            DreamSynthesisCommitRejection.LEASE_OWNER_MISMATCH,
        )
        if (run.leaseUntilMs != activeRunLeaseUntilMs || run.leaseUntilMs <= nowMs) return ownedRejected(
            DreamSynthesisStoreRejection.LEASE_EXPIRED,
            DreamSynthesisCommitRejection.LEASE_EXPIRED,
        )
        if (run.baseMemoryEpoch != fence.baseMemoryEpoch || run.baseDreamRevision != fence.baseDreamRevision) {
            return ownedRejected(
                DreamSynthesisStoreRejection.FENCE_CONFLICT,
                DreamSynthesisCommitRejection.FENCE_CONFLICT,
            )
        }
        if (nowMs < run.updatedAtMs) return ownedRejected(
            DreamSynthesisStoreRejection.FENCE_CONFLICT,
            DreamSynthesisCommitRejection.FENCE_CONFLICT,
        )
        return OwnedContextResult.Ready(run)
    }

    private suspend fun classifyCommitCas(
        fence: DreamSynthesisFence,
        nowMs: Long,
    ): DreamSynthesisCommitRejection {
        return when (val context = ownedContext(fence, nowMs)) {
            is OwnedContextResult.Ready -> DreamSynthesisCommitRejection.STORE_CORRUPTION
            is OwnedContextResult.Rejected -> context.commitReason
        }
    }

    private suspend fun failOwnedRun(
        fence: DreamSynthesisFence,
        failureCode: DreamRunFailureCode,
        nowMs: Long,
    ): DreamSynthesisStoreResult {
        if (DreamPairScopeId.parseOrNull(fence.scopeId.value) != null) {
            return try {
                database.withTransaction {
                    when (val owned = ownedContext(fence, nowMs)) {
                        is OwnedContextResult.Rejected ->
                            return@withTransaction DreamSynthesisStoreResult.Rejected(owned.storeReason)
                        is OwnedContextResult.Ready -> Unit
                    }
                    if (experienceDao.finishRunMirror(
                            runId = fence.runId,
                            pairScopeId = fence.scopeId.value,
                            leaseOwner = fence.leaseOwner,
                            terminalStatus = DreamRunStatus.FAILED.name,
                            failureCode = failureCode.name,
                            nowMs = nowMs,
                        ) != 1
                    ) throw PairLeaseAbort()
                    if (experienceDao.releaseLease(
                            pairScopeId = fence.scopeId.value,
                            runId = fence.runId,
                            nowMs = nowMs,
                            reasonCode = AuthorityChangeReason.RUN_FINISHED.name,
                        ) != 1
                    ) throw PairLeaseAbort()
                    DreamSynthesisStoreResult.Accepted
                }
            } catch (_: PairLeaseAbort) {
                DreamSynthesisStoreResult.Rejected(DreamSynthesisStoreRejection.FENCE_CONFLICT)
            }
        }
        return when (val result = observerStore.fail(
            FailDreamRunRequest(
                runId = fence.runId,
                scopeId = fence.scopeId,
                leaseOwner = fence.leaseOwner,
                failureCode = failureCode,
                nowMs = nowMs,
            ),
        )) {
            is FinishDreamRunResult.Finished -> DreamSynthesisStoreResult.Accepted
            is FinishDreamRunResult.Rejected -> DreamSynthesisStoreResult.Rejected(
                result.reason.toSynthesisRejection(),
            )
        }
    }

    private suspend fun generationEnabled(scopeId: DreamScopeId): Boolean {
        val flags = featureFlags.flagsFor(scopeId)
        return flags.allowsSynthesisGeneration()
    }

}

/**
 * Pre-V2 legacy rows used `0` as the persisted `updated_at_ms` sentinel even after a real
 * `created_at_ms` had been assigned. Preserve the authoritative row unchanged and normalize only
 * this exact historical shape at the Dream projection boundary. Modern timestamp inversions still
 * fail closed instead of being silently repaired.
 */
internal fun MemoryEntity.dreamUpdatedAtEpochMsOrNull(): Long? = when {
    createdAtMs < 0L -> null
    updatedAtMs >= createdAtMs -> updatedAtMs
    sourceType == "LEGACY" && updatedAtMs == 0L && createdAtMs > 0L -> createdAtMs
    else -> null
}

private sealed interface OwnedContextResult {
    data class Ready(val run: DreamRunEntity) : OwnedContextResult

    data class Rejected(
        val storeReason: DreamSynthesisStoreRejection,
        val commitReason: DreamSynthesisCommitRejection,
    ) : OwnedContextResult
}

private fun ownedRejected(
    storeReason: DreamSynthesisStoreRejection,
    commitReason: DreamSynthesisCommitRejection,
) = OwnedContextResult.Rejected(storeReason, commitReason)

private fun DreamSynthesisMode.toRunMode(): DreamRunMode = when (this) {
    DreamSynthesisMode.INCREMENTAL -> DreamRunMode.INCREMENTAL
    DreamSynthesisMode.FULL -> DreamRunMode.FULL
}

private fun DreamRunEntity.isOwnedBy(scopeId: DreamScopeId, owner: String): Boolean =
    this.scopeId == scopeId.value && leaseOwner == owner

private fun DreamStoreRejection.toSynthesisRejection(): DreamSynthesisStoreRejection = when (this) {
    DreamStoreRejection.NOT_FOUND -> DreamSynthesisStoreRejection.RUN_NOT_FOUND
    DreamStoreRejection.SCOPE_MISMATCH -> DreamSynthesisStoreRejection.SCOPE_MISMATCH
    DreamStoreRejection.OWNER_MISMATCH -> DreamSynthesisStoreRejection.OWNER_MISMATCH
    DreamStoreRejection.LEASE_EXPIRED,
    DreamStoreRejection.CLOCK_ROLLBACK,
    -> DreamSynthesisStoreRejection.LEASE_EXPIRED
    DreamStoreRejection.STATUS_MISMATCH -> DreamSynthesisStoreRejection.RUN_NOT_RUNNING
    else -> DreamSynthesisStoreRejection.FENCE_CONFLICT
}

private fun hasCompleteEpochs(values: List<Long>, fromExclusive: Long, throughInclusive: Long): Boolean {
    val epochs = values.distinct()
    var expected = fromExclusive
    for (epoch in epochs) {
        if (expected == Long.MAX_VALUE || epoch != expected + 1L) return false
        expected = epoch
    }
    return expected == throughInclusive
}

private inline fun <reified T : Enum<T>> enumOrNull(raw: String): T? =
    enumValues<T>().firstOrNull { it.name == raw }

private fun Long.toIntExactOrAbort(): Int {
    if (this !in 1L..Int.MAX_VALUE.toLong()) abort(DreamSynthesisCommitRejection.STORE_CORRUPTION)
    return toInt()
}

private class CommitAbort(val reason: DreamSynthesisCommitRejection) : RuntimeException()
private class PairLeaseAbort : RuntimeException()

private fun abort(reason: DreamSynthesisCommitRejection): Nothing = throw CommitAbort(reason)

private const val DREAM_SYNTHESIS_INITIAL_LEASE_MS = 15L * 60_000L
private const val MAX_DREAM_INPUT_CANDIDATES = 8_192
private const val MAX_DREAM_CLAIMS = 10_000
private const val DREAM_MEMORY_ID_QUERY_CHUNK = 500
private const val SYNTHESIS_COMMIT_REASON = "DREAM_SYNTHESIS_COMMIT"
private val DREAM_SYNTHESIS_INPUT_BUDGET = DreamInputBudget(
    maxMemories = 128,
    maxClaims = 128,
    maxInputUtf8Bytes = 96_000,
    maxSourceUtf8Bytes = 32_000,
)
