package me.rerere.rikkahub.memory.dreaming.review

import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.entity.DreamClaimEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimVersionEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimVersionSourceEntity
import me.rerere.rikkahub.data.db.entity.DreamRunEntity
import me.rerere.rikkahub.data.db.entity.DreamSnapshotEntity
import me.rerere.rikkahub.data.db.entity.MemoryEntity
import me.rerere.rikkahub.data.db.entity.MemoryEvidenceEntity
import me.rerere.rikkahub.data.db.entity.MemoryRevisionEntity
import me.rerere.rikkahub.data.db.entity.MemoryScopeChangeEntity
import me.rerere.rikkahub.data.db.entity.MemoryScopeStateEntity
import me.rerere.rikkahub.memory.MemoryApprovalSource
import me.rerere.rikkahub.memory.MemoryAttribution
import me.rerere.rikkahub.memory.MemoryKind
import me.rerere.rikkahub.memory.MemoryLifecycleStatus
import me.rerere.rikkahub.memory.MemorySourceIdentity
import me.rerere.rikkahub.memory.MemorySourceRole
import me.rerere.rikkahub.memory.MemoryTruthStatus
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityFingerprintV1
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityMemory
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityPin
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimHead
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimMutationReason
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimSourcePin
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimState
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimVersionCanonicalV1
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicType
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamStorageClass
import me.rerere.rikkahub.memory.dreaming.model.DreamSupportType
import me.rerere.rikkahub.memory.dreaming.model.DreamValidatedClaimVersion
import me.rerere.rikkahub.memory.dreaming.model.DreamingFeatureFlags
import me.rerere.rikkahub.memory.dreaming.runtime.DreamingFeatureFlagSource
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotCompileRequest
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotCompiler
import me.rerere.rikkahub.memory.dreaming.temporal.TemporalState
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/** Emulator/disposable-device only. Never run this instrumentation test on the primary phone. */
@RunWith(AndroidJUnit4::class)
class RoomDreamReviewStoreContractTest {
    private lateinit var db: AppDatabase
    private lateinit var store: RoomDreamReviewStore
    private val json = Json { ignoreUnknownKeys = false; isLenient = false }

    @Before
    fun setUp() {
        db = Room.inMemoryDatabaseBuilder(
            ApplicationProvider.getApplicationContext(),
            AppDatabase::class.java,
        ).build()
        store = RoomDreamReviewStore(
            database = db,
            dreamDao = db.dreamDao(),
            experienceDao = db.dreamExperienceDao(),
            synthesisDao = db.dreamSynthesisDao(),
            memoryDao = db.memoryDao(),
            memoryV2Dao = db.memoryV2Dao(),
            featureFlags = DreamingFeatureFlagSource {
                DreamingFeatureFlags(schemaReady = true, generate = true, use = true)
            },
            json = json,
            nowEpochMs = { REVIEW_NOW },
        )
    }

    @After
    fun tearDown() {
        db.close()
    }

    @Test
    fun projectionAndEvidenceReveal_areBoundedExactAndContainNoListExcerpt() = runBlocking {
        seedReviewGraph(includeEvidence = true)

        val projection = store.observeProjection(SCOPE).first()
        assertEquals(DreamDerivedStatus.READY, projection.derivedStatus)
        assertEquals(DreamUsageMode.ACTIVE, projection.usageMode)
        assertEquals(SNAPSHOT_ONE, projection.activeSnapshot?.snapshotId)
        assertTrue(projection.snapshotDiff is DreamSnapshotDiffResult.Available)

        val target = DreamClaimMutationTarget(projection.fence, CLAIM_ID, 1L)
        val detail = (store.readClaim(target) as DreamReviewReadResult.Found).value
        val evidence = detail.evidence.single()
        assertEquals(DreamEvidenceValidity.VALID, evidence.validity)
        assertEquals("TEXT", evidence.sourceKind)
        assertEquals("ORIGINAL_MESSAGE", evidence.qualityCode)
        assertTrue(evidence.excerptAvailable)

        val revealed = store.readEvidence(evidence.reference, DREAM_EVIDENCE_EXCERPT_MAX_CHARS)
            as DreamEvidenceRevealResult.Revealed
        assertEquals(DREAM_EVIDENCE_EXCERPT_MAX_CHARS, revealed.excerpt.text.length)
        assertTrue(revealed.excerpt.truncated)
        assertEquals(
            DreamEvidenceRevealResult.Corrupt,
            store.readEvidence(evidence.reference, DREAM_EVIDENCE_EXCERPT_MAX_CHARS + 1),
        )

        insertAuthorityMemory(
            id = MEMORY_ID,
            revision = 2,
            content = "A concurrently revised authority value.",
            sourceType = "TEST",
            replace = true,
        )
        val stale = store.readEvidence(evidence.reference, 100)
            as DreamEvidenceRevealResult.Invalid
        assertEquals(DreamEvidenceValidity.REVISION_CHANGED, stale.validity)
    }

    @Test
    fun reject_isFiveFenceAtomicAndCrossScopeWritesNothing() = runBlocking {
        seedReviewGraph(includeEvidence = false)
        seedEmptyScope(OTHER_SCOPE)
        val initial = store.observeProjection(SCOPE).first()
        val target = DreamClaimMutationTarget(initial.fence, CLAIM_ID, 1L)

        val applied = store.reject(DreamRejectCommand(REJECT_MUTATION, target, 200L))
            as DreamReviewStoreMutationResult.Applied
        assertEquals(2L, applied.fence.expectedDreamRevision)
        assertEquals(REJECT_MUTATION, applied.fence.expectedActiveSnapshotId)
        assertEquals(DreamClaimState.REJECTED.name, currentClaim().state)
        assertEquals(2L, currentClaim().claimRevision)
        assertEquals(2, db.dreamSynthesisDao().listClaimVersions(CLAIM_ID, SCOPE.value).size)
        assertEquals(
            listOf(1L, 2L),
            db.dreamSynthesisDao().listClaimVersions(CLAIM_ID, SCOPE.value)
                .map(DreamClaimVersionEntity::claimRevision),
        )
        assertEquals("SUPERSEDED", snapshot(SNAPSHOT_ONE).status)
        assertEquals("ACTIVE", snapshot(REJECT_MUTATION).status)
        val reviewed = store.observeProjection(SCOPE).first()
        assertEquals(SNAPSHOT_ONE, reviewed.supersededSnapshot?.snapshotId)
        val changes = (reviewed.snapshotDiff as DreamSnapshotDiffResult.Available).changes
        assertEquals(listOf(DreamSnapshotChangeType.RETIRED), changes.map { it.type })

        val snapshotCount = db.dreamSynthesisDao().listSnapshots(SCOPE.value, 10).size
        val stale = store.reject(DreamRejectCommand(STALE_MUTATION, target, 201L))
            as DreamReviewStoreMutationResult.Conflict
        assertEquals(DreamReviewConflict.DREAM_REVISION, stale.conflict)
        assertEquals(snapshotCount, db.dreamSynthesisDao().listSnapshots(SCOPE.value, 10).size)

        val otherTarget = DreamClaimMutationTarget(
            fence = checkNotNull(db.dreamDao().getScopeState(OTHER_SCOPE.value)).toFence(OTHER_SCOPE),
            claimId = CLAIM_ID,
            expectedClaimRevision = 2L,
        )
        val crossScope = store.reject(DreamRejectCommand(CROSS_SCOPE_MUTATION, otherTarget, 202L))
            as DreamReviewStoreMutationResult.Conflict
        assertEquals(DreamReviewConflict.SCOPE, crossScope.conflict)
        assertEquals(snapshotCount, db.dreamSynthesisDao().listSnapshots(SCOPE.value, 10).size)
    }

    @Test
    fun correction_acceptsOnlyTheSoleAuthorityEpoch_andRecompilesDerivedState() = runBlocking {
        seedReviewGraph(includeEvidence = false)
        val projection = store.observeProjection(SCOPE).first()
        val target = DreamClaimMutationTarget(projection.fence, CLAIM_ID, 1L)
        val validated = (store.validateTarget(target) as DreamReviewReadResult.Found).value

        insertAuthorityMemory(
            id = CORRECTION_MEMORY_ID,
            revision = 1,
            content = "The user corrected the project state.",
            sourceType = DREAM_CORRECTION_SOURCE_TYPE,
            replace = false,
        )
        db.openHelper.writableDatabase.execSQL(
            "UPDATE memory_scope_state SET memory_epoch = 7, updated_at_ms = 210 " +
                "WHERE scope_id = ? AND memory_epoch = 5",
            arrayOf(SCOPE.value),
        )
        val command = DreamMarkCorrectedCommand(
            mutationId = CORRECTION_MUTATION,
            validatedTarget = validated,
            authorityMemoryId = CORRECTION_MEMORY_ID,
            authorityMemoryRevision = 1,
            expectedAuthorityMemoryEpoch = 6L,
            nowEpochMs = 220L,
        )
        val extraEpoch = store.markCorrected(command) as DreamReviewStoreMutationResult.Conflict
        assertEquals(DreamReviewConflict.MEMORY_EPOCH, extraEpoch.conflict)
        assertEquals(1L, currentClaim().claimRevision)
        assertEquals(1, db.dreamSynthesisDao().listSnapshots(SCOPE.value, 10).size)

        db.openHelper.writableDatabase.execSQL(
            "UPDATE memory_scope_state SET memory_epoch = 6 WHERE scope_id = ? AND memory_epoch = 7",
            arrayOf(SCOPE.value),
        )
        val result = store.markCorrected(
            command,
        ) as DreamReviewStoreMutationResult.Applied

        assertEquals(6L, result.fence.expectedMemoryEpoch)
        assertEquals(5L, result.fence.expectedLastAppliedMemoryEpoch)
        assertEquals(2L, result.fence.expectedDreamRevision)
        assertEquals(DreamClaimState.SUPERSEDED.name, currentClaim().state)
        val nextSources = db.dreamSynthesisDao().listClaimVersionSources(CLAIM_ID, 2L)
        assertEquals(CORRECTION_MEMORY_ID, nextSources.single().memoryId)
        assertEquals(DreamSupportType.SUPERSEDES.name, nextSources.single().supportType)
        assertEquals(0, snapshot(CORRECTION_MUTATION).claimCount)

        val dirty = store.observeProjection(SCOPE).first()
        assertEquals(DreamDerivedStatus.DIRTY, dirty.derivedStatus)
    }

    @Test
    fun clearDerived_isChildFirstAndPreservesAuthorityJournalAndRunAudit() = runBlocking {
        seedReviewGraph(includeEvidence = false)
        db.dreamDao().insertChanges(
            listOf(
                MemoryScopeChangeEntity(
                    scopeId = SCOPE.value,
                    memoryEpoch = 5L,
                    entityKind = "MEMORY",
                    entityId = MEMORY_ID.toString(),
                    entityRevision = 1L,
                    operation = "UPDATE",
                    reasonCode = "TEST",
                    createdAtMs = 40L,
                ),
            ),
        )
        db.dreamDao().insertRun(
            DreamRunEntity(
                runId = AUDIT_RUN,
                scopeId = SCOPE.value,
                mode = "FULL",
                status = "SUCCEEDED",
                baseMemoryEpoch = 5L,
                baseObserverCheckpointEpoch = 5L,
                checkpointEpoch = 5L,
                createdAtMs = 30L,
                startedAtMs = 31L,
                updatedAtMs = 32L,
                finishedAtMs = 32L,
                baseDreamRevision = 0L,
                sourceTimezoneId = "Asia/Shanghai",
            ),
        )
        val fence = store.observeProjection(SCOPE).first().fence
        val cleared = store.clearDerived(DreamClearDerivedCommand(CLEAR_MUTATION, fence, 300L))
            as DreamReviewStoreMutationResult.Applied

        assertEquals(0L, cleared.fence.expectedLastAppliedMemoryEpoch)
        assertEquals(2L, cleared.fence.expectedDreamRevision)
        assertNull(cleared.fence.expectedActiveSnapshotId)
        assertEquals(0, db.dreamSynthesisDao().countDerivedClaimsForScope(SCOPE.value))
        assertEquals(0, db.dreamSynthesisDao().countDerivedSnapshotsForScope(SCOPE.value))
        assertEquals(1, db.dreamDao().countChangesThrough(SCOPE.value, 5L))
        assertEquals(AUDIT_RUN, db.dreamDao().getRunById(AUDIT_RUN)?.runId)
        assertEquals(MEMORY_ID, db.memoryDao().getMemoryById(MEMORY_ID, SCOPE.value)?.id)
        assertEquals(
            DreamReviewStoreMutationResult.AlreadyClear,
            store.clearDerived(DreamClearDerivedCommand(CLEAR_AGAIN_MUTATION, cleared.fence, 301L)),
        )
        assertEquals(2L, db.dreamDao().getScopeState(SCOPE.value)?.dreamStateRevision)
    }

    private suspend fun seedReviewGraph(includeEvidence: Boolean) {
        val sourceIdentity = MemorySourceIdentity(
            conversationId = "conversation-1",
            messageId = "message-1",
            role = MemorySourceRole.USER,
            consumedTextDigest = HASH_A,
            evidenceGroupId = "capture-1",
        )
        val sourceJson = json.encodeToString(listOf(sourceIdentity))
        db.dreamDao().insertScopeStateIfAbsent(
            MemoryScopeStateEntity(
                scopeId = SCOPE.value,
                memoryEpoch = 5L,
                observerCheckpointEpoch = 5L,
                updatedAtMs = 20L,
                dreamStateRevision = 1L,
                lastAppliedMemoryEpoch = 5L,
                activeSnapshotId = SNAPSHOT_ONE,
            ),
        )
        val memory = authorityMemoryEntity(
            id = MEMORY_ID,
            revision = 1,
            content = "The user maintains an offline memory project.",
            sourceType = "TEST",
            sourceJson = sourceJson,
        )
        db.memoryDao().insertMemory(memory)
        db.memoryV2Dao().insertRevision(
            MemoryRevisionEntity(
                id = MEMORY_REVISION_ONE,
                memoryId = MEMORY_ID,
                revision = 1,
                operation = "CREATE",
                actor = "TEST",
                sourceIdentitiesJson = sourceJson,
                createdAtMs = 20L,
            ),
        )
        if (includeEvidence) {
            db.memoryV2Dao().insertEvidence(
                listOf(
                    MemoryEvidenceEntity(
                        id = EVIDENCE_ID,
                        memoryId = MEMORY_ID,
                        conversationId = sourceIdentity.conversationId,
                        messageId = sourceIdentity.messageId,
                        role = sourceIdentity.role.name,
                        excerpt = "e".repeat(700),
                        contentHash = HASH_B,
                        capturedAtMs = 20L,
                        quality = "ORIGINAL_MESSAGE",
                        evidenceGroupId = sourceIdentity.evidenceGroupId,
                        sourceDigest = sourceIdentity.consumedTextDigest,
                        sourceKind = sourceIdentity.sourceKind.name,
                    ),
                ),
            )
        }
        val authority = memory.toAuthority(SCOPE, listOf(sourceIdentity))
        val pin = DreamAuthorityPin(
            scopeId = SCOPE,
            memoryId = MEMORY_ID.toString(),
            expectedRevision = 1L,
            expectedAuthorityFingerprint = DreamAuthorityFingerprintV1.compute(authority),
            expectedSourceManifestHash = DreamAuthorityFingerprintV1.sourceManifestHash(authority.sources),
        )
        val source = DreamClaimSourcePin(pin, DreamSupportType.SUPPORTS, directAuthority = true)
        val version = DreamValidatedClaimVersion(
            claimId = CLAIM_ID,
            expectedPreviousRevision = null,
            nextRevision = 1L,
            claimKey = "project.offline",
            storageClass = DreamStorageClass.EPISODIC,
            epistemicType = DreamEpistemicType.PROJECT_STATE,
            nextState = DreamClaimState.ACTIVE_CONTEXTUAL,
            title = "Offline project",
            statement = "The user maintains an offline memory project.",
            confidencePermille = 900,
            temporalState = TemporalState.CURRENT,
            validFromEpochMs = null,
            validToEpochMs = null,
            sources = listOf(source),
            reason = DreamClaimMutationReason.MODEL_PROPOSAL,
        )
        val canonical = DreamClaimVersionCanonicalV1.encode(version)
        val head = DreamClaimHead(
            claimId = CLAIM_ID,
            scopeId = SCOPE,
            revision = 1L,
            claimKey = version.claimKey,
            storageClass = version.storageClass,
            epistemicType = version.epistemicType,
            state = version.nextState,
            title = version.title,
            statement = version.statement,
            confidencePermille = version.confidencePermille,
            temporalState = version.temporalState,
            validFromEpochMs = null,
            validToEpochMs = null,
            versionHash = canonical.contentHash,
            sources = version.sources,
        )
        db.dreamSynthesisDao().insertClaim(
            DreamClaimEntity(
                claimId = CLAIM_ID,
                scopeId = SCOPE.value,
                claimRevision = 1L,
                claimKey = head.claimKey,
                storageClass = head.storageClass.name,
                epistemicType = head.epistemicType.name,
                title = head.title,
                statement = head.statement,
                state = head.state.name,
                confidence = 0.9,
                temporalState = head.temporalState.name,
                learnedAtMs = 20L,
                sourceTimezone = "Asia/Shanghai",
                claimHash = head.versionHash.value,
                createdByRunId = SEED_RUN,
                lastValidatedMemoryEpoch = 5L,
                createdAtMs = 20L,
                updatedAtMs = 20L,
            ),
        )
        db.dreamSynthesisDao().insertClaimVersion(
            DreamClaimVersionEntity(
                claimId = CLAIM_ID,
                claimRevision = 1L,
                canonicalClaimJson = canonical.canonicalClaimJson,
                contentHash = canonical.contentHash.value,
                sourceManifestHash = canonical.sourceManifestHash.value,
                reasonCode = version.reason.name,
                createdByRunId = SEED_RUN,
                createdAtMs = 20L,
            ),
        )
        db.dreamSynthesisDao().insertClaimVersionSources(
            listOf(
                DreamClaimVersionSourceEntity(
                    claimId = CLAIM_ID,
                    claimRevision = 1L,
                    memoryId = MEMORY_ID,
                    memoryRevision = 1,
                    memorySemanticHash = pin.expectedAuthorityFingerprint.value,
                    memoryEvidenceId = EVIDENCE_ID.takeIf { includeEvidence },
                    supportType = DreamSupportType.SUPPORTS.name,
                    createdAtMs = 20L,
                ),
            ),
        )
        val compiled = DreamSnapshotCompiler.compile(
            DreamSnapshotCompileRequest(SCOPE, COMPILER_REVISION, listOf(head)),
        )
        db.dreamSynthesisDao().insertSnapshot(
            DreamSnapshotEntity(
                snapshotId = SNAPSHOT_ONE,
                scopeId = SCOPE.value,
                snapshotRevision = 1L,
                sourceMemoryEpoch = 5L,
                committedDreamRevision = 1L,
                status = "ACTIVE",
                canonicalPayloadJson = compiled.payloadJson,
                payloadSha256 = compiled.payloadHash.value,
                compilerRevision = compiled.compilerRevision,
                estimatedTokens = compiled.estimatedTokens,
                claimCount = compiled.claimCount,
                createdByRunId = SEED_RUN,
                createdAtMs = 20L,
                reasonCode = "TEST_SEED",
            ),
        )
    }

    private suspend fun seedEmptyScope(scopeId: DreamScopeId) {
        db.dreamDao().insertScopeStateIfAbsent(
            MemoryScopeStateEntity(scopeId = scopeId.value, updatedAtMs = 10L),
        )
    }

    private suspend fun insertAuthorityMemory(
        id: Int,
        revision: Int,
        content: String,
        sourceType: String,
        replace: Boolean,
    ) {
        if (replace) {
            db.openHelper.writableDatabase.execSQL(
                "UPDATE MemoryEntity SET content = ?, updated_at_ms = ?, revision = ?, " +
                    "source_identities_json = '[]' WHERE id = ? AND assistant_id = ?",
                arrayOf<Any>(content, 210L, revision, id, SCOPE.value),
            )
        } else {
            db.memoryDao().insertMemory(
                authorityMemoryEntity(id, revision, content, sourceType, "[]"),
            )
        }
        db.memoryV2Dao().insertRevision(
            MemoryRevisionEntity(
                id = if (id == MEMORY_ID) MEMORY_REVISION_TWO else CORRECTION_REVISION,
                memoryId = id,
                revision = revision,
                operation = if (revision == 1) "CREATE" else "UPDATE",
                actor = "TEST",
                sourceIdentitiesJson = "[]",
                createdAtMs = 210L,
            ),
        )
    }

    private fun authorityMemoryEntity(
        id: Int,
        revision: Int,
        content: String,
        sourceType: String,
        sourceJson: String,
    ) = MemoryEntity(
        id = id,
        assistantId = SCOPE.value,
        title = "Project",
        content = content,
        createdAtMs = 10L,
        updatedAtMs = if (revision == 1) 20L else 210L,
        memoryKind = MemoryKind.PROJECT_FACT.name,
        confidence = 1f,
        sourceType = sourceType,
        sourceIdentitiesJson = sourceJson,
        lifecycleStatus = MemoryLifecycleStatus.ACTIVE.name,
        approvalSource = MemoryApprovalSource.USER_REVIEWED.name,
        revision = revision,
        attribution = MemoryAttribution.USER.name,
        truthStatus = MemoryTruthStatus.CONFIRMED.name,
    )

    private fun MemoryEntity.toAuthority(
        scopeId: DreamScopeId,
        sources: List<MemorySourceIdentity>,
    ) = DreamAuthorityMemory(
        scopeId = scopeId,
        memoryId = id.toString(),
        revision = revision.toLong(),
        title = title,
        content = content,
        kind = MemoryKind.valueOf(memoryKind),
        attribution = MemoryAttribution.valueOf(attribution),
        truthStatus = MemoryTruthStatus.valueOf(truthStatus),
        lifecycleStatus = MemoryLifecycleStatus.valueOf(lifecycleStatus),
        approvalSource = MemoryApprovalSource.valueOf(approvalSource),
        tags = emptyList(),
        createdAtEpochMs = createdAtMs,
        updatedAtEpochMs = updatedAtMs,
        occurredAtEpochMs = null,
        expiresAtEpochMs = null,
        originAssistantId = originAssistantId,
        participants = emptyList(),
        outcome = null,
        sources = sources.map { source ->
            me.rerere.rikkahub.memory.dreaming.model.DreamAuthoritySource(
                conversationId = source.conversationId,
                messageId = source.messageId,
                role = source.role,
                sourceKind = source.sourceKind,
                consumedTextDigest = me.rerere.rikkahub.memory.dreaming.model.DreamSha256(
                    source.consumedTextDigest,
                ),
                evidenceGroupId = source.evidenceGroupId,
            )
        },
    )

    private suspend fun currentClaim() = checkNotNull(
        db.dreamSynthesisDao().getClaim(CLAIM_ID, SCOPE.value),
    )

    private suspend fun snapshot(id: String) = checkNotNull(
        db.dreamSynthesisDao().getSnapshot(id, SCOPE.value),
    )

    private fun MemoryScopeStateEntity.toFence(scopeId: DreamScopeId) = DreamReviewFence(
        scopeId = scopeId,
        expectedMemoryEpoch = memoryEpoch,
        expectedLastAppliedMemoryEpoch = lastAppliedMemoryEpoch,
        expectedDreamRevision = dreamStateRevision,
        expectedActiveSnapshotId = activeSnapshotId,
    )

    private companion object {
        val SCOPE = DreamScopeId.requireCanonical("11111111-1111-4111-8111-111111111111")
        val OTHER_SCOPE = DreamScopeId.requireCanonical("22222222-2222-4222-8222-222222222222")
        const val MEMORY_ID = 1
        const val CORRECTION_MEMORY_ID = 2
        const val CLAIM_ID = "33333333-3333-4333-8333-333333333333"
        const val SNAPSHOT_ONE = "44444444-4444-4444-8444-444444444444"
        const val REJECT_MUTATION = "55555555-5555-4555-8555-555555555555"
        const val CORRECTION_MUTATION = "66666666-6666-4666-8666-666666666666"
        const val AUDIT_RUN = "77777777-7777-4777-8777-777777777777"
        const val MEMORY_REVISION_ONE = "88888888-8888-4888-8888-888888888888"
        const val MEMORY_REVISION_TWO = "99999999-9999-4999-8999-999999999999"
        const val CORRECTION_REVISION = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        const val SEED_RUN = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        const val CLEAR_MUTATION = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        const val CLEAR_AGAIN_MUTATION = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
        const val STALE_MUTATION = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
        const val CROSS_SCOPE_MUTATION = "ffffffff-ffff-4fff-8fff-ffffffffffff"
        const val EVIDENCE_ID = "evidence-review-1"
        const val COMPILER_REVISION = "dream-snapshot-compiler-v1"
        val HASH_A = "a" + "0".repeat(63)
        val HASH_B = "b" + "0".repeat(63)
        const val REVIEW_NOW = 1_000L
    }
}
