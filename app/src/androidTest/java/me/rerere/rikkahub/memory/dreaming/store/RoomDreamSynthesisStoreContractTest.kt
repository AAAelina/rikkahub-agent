package me.rerere.rikkahub.memory.dreaming.store

import androidx.room.Room
import androidx.room.withTransaction
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.entity.DreamClaimEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimVersionEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimVersionSourceEntity
import me.rerere.rikkahub.data.db.entity.MemoryEntity
import me.rerere.rikkahub.data.db.entity.MemoryRevisionEntity
import me.rerere.rikkahub.data.db.entity.MemoryScopeStateEntity
import me.rerere.rikkahub.memory.MemoryApprovalSource
import me.rerere.rikkahub.memory.MemoryAttribution
import me.rerere.rikkahub.memory.MemoryKind
import me.rerere.rikkahub.memory.MemoryLifecycleStatus
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
import me.rerere.rikkahub.memory.dreaming.model.DreamProposalNonce
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamSha256
import me.rerere.rikkahub.memory.dreaming.model.DreamStorageClass
import me.rerere.rikkahub.memory.dreaming.model.DreamSupportType
import me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisMode
import me.rerere.rikkahub.memory.dreaming.model.DreamValidatedClaimTransition
import me.rerere.rikkahub.memory.dreaming.model.DreamValidatedClaimVersion
import me.rerere.rikkahub.memory.dreaming.model.DreamValidatedPlan
import me.rerere.rikkahub.memory.dreaming.model.DreamingFeatureFlags
import me.rerere.rikkahub.memory.dreaming.runtime.DreamingFeatureFlagSource
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotCompileRequest
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotCompiler
import me.rerere.rikkahub.memory.dreaming.synthesis.DREAM_PROMPT_CONTRACT_VERSION
import me.rerere.rikkahub.memory.dreaming.synthesis.DREAM_VALIDATOR_VERSION
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamModelAudit
import me.rerere.rikkahub.memory.dreaming.temporal.TemporalState
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/** Emulator/disposable-device only. Never run this instrumentation test on the primary phone. */
@RunWith(AndroidJUnit4::class)
class RoomDreamSynthesisStoreContractTest {
    private lateinit var db: AppDatabase
    private lateinit var observer: RoomDreamObserverStore
    private lateinit var store: RoomDreamSynthesisStore
    private lateinit var privacy: RoomDreamPrivacyScrubber
    private val json = Json { ignoreUnknownKeys = false }

    @Before
    fun setUp() {
        db = Room.inMemoryDatabaseBuilder(
            ApplicationProvider.getApplicationContext(),
            AppDatabase::class.java,
        ).build()
        observer = RoomDreamObserverStore(db, db.dreamDao())
        val flags = DreamingFeatureFlagSource {
            // ACTIVE generation is valid without shadow; shadow controls later injection policy.
            DreamingFeatureFlags(schemaReady = true, generate = true, use = true, shadow = false)
        }
        store = RoomDreamSynthesisStore(
            database = db,
            dreamDao = db.dreamDao(),
            experienceDao = db.dreamExperienceDao(),
            synthesisDao = db.dreamSynthesisDao(),
            memoryDao = db.memoryDao(),
            memoryV2Dao = db.memoryV2Dao(),
            observerStore = observer,
            featureFlags = flags,
            json = json,
            idGenerator = { SNAPSHOT_ID },
        )
        privacy = RoomDreamPrivacyScrubber(
            database = db,
            dreamDao = db.dreamDao(),
            synthesisDao = db.dreamSynthesisDao(),
            memoryDao = db.memoryDao(),
            memoryV2Dao = db.memoryV2Dao(),
            json = json,
        )
    }

    @After
    fun tearDown() {
        db.close()
    }

    @Test
    fun fullBootstrap_resumeCommitDuplicateAndPrivacy_areFencedAndChildFirst() = runBlocking {
        val seeded = seedAuthorityAndClaim(MemoryLifecycleStatus.ACTIVE)
        val first = begin(attemptNow = 100L) as BeginDreamSynthesisResult.Ready
        assertEquals(DreamSynthesisMode.FULL, first.fence.mode)
        assertEquals(100L, first.fence.frozenNowEpochMs)
        assertEquals(TIMEZONE, first.fence.sourceTimezoneId)

        val resumed = begin(attemptNow = 200L) as BeginDreamSynthesisResult.Ready
        assertEquals(first.fence.frozenNowEpochMs, resumed.fence.frozenNowEpochMs)
        assertEquals(first.fence.sourceTimezoneId, resumed.fence.sourceTimezoneId)
        assertEquals(200L + INITIAL_LEASE_MS, db.dreamDao().getRunById(RUN_ID)?.leaseUntilMs)
        val mismatch = store.begin(
            beginRequest(attemptNow = 201L, timezone = "America/New_York"),
        ) as BeginDreamSynthesisResult.Rejected
        assertEquals(DreamSynthesisStoreRejection.FENCE_CONFLICT, mismatch.reason)

        // There are no v45 journal rows for epochs 1..5. FULL must still read bounded authority.
        val seed = store.readInputSeed(resumed.fence, attemptNowEpochMs = 201L)
            as ReadDreamInputSeedResult.Ready
        assertEquals(1, seed.request.candidates.size)
        val current = seed.request.currentClaims.single()
        val plan = DreamValidatedPlan(
            fence = resumed.fence,
            proposalNonce = NONCE,
            upserts = emptyList(),
            transitions = emptyList(),
            resultingClaims = listOf(current),
            modelEvidencePins = emptyList(),
        )
        val snapshot = DreamSnapshotCompiler.compile(
            DreamSnapshotCompileRequest(SCOPE, COMPILER_REVISION, plan.resultingClaims),
        )
        val request = commitRequest(
            fence = resumed.fence,
            plan = plan,
            snapshot = snapshot,
            livePins = listOf(seeded.pin),
            historicalPins = emptyList(),
            inputMemoryCount = 1,
        )
        val committed = store.commit(request) as DreamSynthesisCommitResult.Committed
        assertEquals(SNAPSHOT_ID, committed.snapshotId)
        assertEquals(5L, db.dreamSynthesisDao().getClaim(CLAIM_ID, SCOPE.value)?.lastValidatedMemoryEpoch)
        assertNull(db.dreamDao().getRunById(RUN_ID)?.inputTokens)
        assertNull(db.dreamDao().getRunById(RUN_ID)?.outputTokens)

        val duplicate = store.commit(request) as DreamSynthesisCommitResult.Rejected
        assertEquals(DreamSynthesisCommitRejection.DREAM_REVISION_CONFLICT, duplicate.reason)
        assertEquals(1, db.dreamSynthesisDao().listSnapshots(SCOPE.value, 10).size)
        assertEquals(1L, db.dreamDao().getScopeState(SCOPE.value)?.dreamStateRevision)

        val scrubbed = db.withTransaction {
            privacy.scrubInCurrentTransaction(
                DreamPrivacyScrubRequest(
                    scopeId = SCOPE,
                    targets = listOf(DreamPrivacyTarget.AuthorityMemory(MEMORY_ID.toString())),
                    scrubbedAtEpochMs = 400L,
                ),
            )
        } as DreamPrivacyScrubResult.Scrubbed
        assertTrue(scrubbed.activeSnapshotCleared)
        assertEquals(2L, scrubbed.nextDreamRevision)
        assertEquals("TOMBSTONED", db.dreamSynthesisDao().getClaim(CLAIM_ID, SCOPE.value)?.state)
        assertTrue(
            db.dreamSynthesisDao().listClaimVersions(CLAIM_ID, SCOPE.value)
                .all { it.canonicalClaimJson.isEmpty() },
        )
        assertEquals(0, db.dreamSynthesisDao().countMemoryRevisionPins(MEMORY_ID, 1))
        assertNull(db.dreamDao().getScopeState(SCOPE.value)?.activeSnapshotId)
        assertEquals("TOMBSTONED", db.dreamSynthesisDao().getSnapshot(SNAPSHOT_ID, SCOPE.value)?.status)
        assertFalse(
            db.dreamSynthesisDao().getSnapshot(SNAPSHOT_ID, SCOPE.value)
                ?.canonicalPayloadJson.orEmpty().isNotEmpty(),
        )
    }

    @Test
    fun historicalExpiredPin_canCommitStale_withoutPassingLiveAuthorityGate() = runBlocking {
        seedAuthorityAndClaim(MemoryLifecycleStatus.EXPIRED)
        val fence = (begin(100L) as BeginDreamSynthesisResult.Ready).fence
        val seed = store.readInputSeed(fence, 101L) as ReadDreamInputSeedResult.Ready
        assertTrue(seed.request.candidates.isEmpty())
        val old = seed.request.currentClaims.single()
        val next = DreamValidatedClaimVersion(
            claimId = old.claimId,
            expectedPreviousRevision = old.revision,
            nextRevision = old.revision + 1,
            claimKey = old.claimKey,
            storageClass = old.storageClass,
            epistemicType = old.epistemicType,
            nextState = DreamClaimState.STALE,
            title = old.title,
            statement = old.statement,
            confidencePermille = old.confidencePermille,
            temporalState = old.temporalState,
            validFromEpochMs = old.validFromEpochMs,
            validToEpochMs = old.validToEpochMs,
            sources = old.sources,
            reason = DreamClaimMutationReason.AUTHORITY_EXPIRED,
        )
        val canonical = DreamClaimVersionCanonicalV1.encode(next)
        val resulting = old.copy(
            revision = next.nextRevision,
            state = next.nextState,
            versionHash = canonical.contentHash,
        )
        val plan = DreamValidatedPlan(
            fence = fence,
            proposalNonce = NONCE,
            upserts = emptyList(),
            transitions = listOf(DreamValidatedClaimTransition(old.revision, next)),
            resultingClaims = listOf(resulting),
            modelEvidencePins = emptyList(),
        )
        val snapshot = DreamSnapshotCompiler.compile(
            DreamSnapshotCompileRequest(SCOPE, COMPILER_REVISION, plan.resultingClaims),
        )

        val result = store.commit(
            commitRequest(
                fence = fence,
                plan = plan,
                snapshot = snapshot,
                livePins = emptyList(),
                historicalPins = listOf(old.sources.single().authority),
                inputMemoryCount = 0,
            ),
        )

        assertTrue(result is DreamSynthesisCommitResult.Committed)
        assertEquals("STALE", db.dreamSynthesisDao().getClaim(CLAIM_ID, SCOPE.value)?.state)
        assertEquals(2, db.dreamSynthesisDao().listClaimVersions(CLAIM_ID, SCOPE.value).size)
    }

    @Test
    fun liveExpiredPin_isRejected_andCommitRollsBackEveryDerivedWrite() = runBlocking {
        seedAuthorityAndClaim(MemoryLifecycleStatus.EXPIRED)
        val fence = (begin(100L) as BeginDreamSynthesisResult.Ready).fence
        val seed = store.readInputSeed(fence, 101L) as ReadDreamInputSeedResult.Ready
        val current = seed.request.currentClaims.single()
        val plan = DreamValidatedPlan(
            fence = fence,
            proposalNonce = NONCE,
            upserts = emptyList(),
            transitions = emptyList(),
            resultingClaims = listOf(current),
            modelEvidencePins = emptyList(),
        )
        val snapshot = DreamSnapshotCompiler.compile(
            DreamSnapshotCompileRequest(SCOPE, COMPILER_REVISION, plan.resultingClaims),
        )

        val rejected = store.commit(
            commitRequest(
                fence = fence,
                plan = plan,
                snapshot = snapshot,
                livePins = listOf(current.sources.single().authority),
                historicalPins = emptyList(),
                inputMemoryCount = 0,
            ),
        ) as DreamSynthesisCommitResult.Rejected

        assertEquals(DreamSynthesisCommitRejection.EVIDENCE_TOMBSTONED, rejected.reason)
        assertTrue(db.dreamSynthesisDao().listSnapshots(SCOPE.value, 10).isEmpty())
        assertEquals(0L, db.dreamDao().getScopeState(SCOPE.value)?.dreamStateRevision)
        assertEquals(1, db.dreamSynthesisDao().listClaimVersions(CLAIM_ID, SCOPE.value).size)
    }

    @Test
    fun fullAuthorityRead_rejects8193RowsThroughBoundedLimit() = runBlocking {
        seedState()
        db.openHelper.writableDatabase.execSQL(
            "WITH digits(d) AS (VALUES(0),(1),(2),(3),(4),(5),(6),(7),(8),(9)), " +
                "ids(value) AS (SELECT 1 + a.d + 10*b.d + 100*c.d + 1000*d.d " +
                "FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d " +
                "ORDER BY 1 LIMIT 8193) INSERT INTO MemoryEntity(id, assistant_id, content, " +
                "revision, lifecycle_status, truth_status) " +
                "SELECT value, '${SCOPE.value}', 'bounded', 1, 'ACTIVE', 'CONFIRMED' FROM ids",
        )
        val fence = (begin(100L) as BeginDreamSynthesisResult.Ready).fence

        val result = store.readInputSeed(fence, 101L)

        assertEquals(
            DreamSynthesisStoreRejection.STORE_CORRUPTION,
            (result as ReadDreamInputSeedResult.Rejected).reason,
        )
    }

    private suspend fun seedAuthorityAndClaim(
        lifecycle: MemoryLifecycleStatus,
    ): SeededClaim {
        seedState()
        val memory = MemoryEntity(
            id = MEMORY_ID,
            assistantId = SCOPE.value,
            title = "Project",
            content = "The user maintains an offline memory project.",
            createdAtMs = 10L,
            updatedAtMs = 20L,
            memoryKind = MemoryKind.PROJECT_FACT.name,
            attribution = MemoryAttribution.USER.name,
            truthStatus = MemoryTruthStatus.CONFIRMED.name,
            lifecycleStatus = lifecycle.name,
            approvalSource = MemoryApprovalSource.USER_REVIEWED.name,
            revision = 1,
        )
        db.memoryDao().insertMemory(memory)
        db.memoryV2Dao().insertRevision(
            MemoryRevisionEntity(
                id = MEMORY_REVISION_ID,
                memoryId = MEMORY_ID,
                revision = 1,
                operation = "CREATE",
                actor = "TEST",
                createdAtMs = 20L,
            ),
        )
        val authority = DreamAuthorityMemory(
            scopeId = SCOPE,
            memoryId = MEMORY_ID.toString(),
            revision = 1,
            title = memory.title,
            content = memory.content,
            kind = MemoryKind.PROJECT_FACT,
            attribution = MemoryAttribution.USER,
            truthStatus = MemoryTruthStatus.CONFIRMED,
            lifecycleStatus = lifecycle,
            approvalSource = MemoryApprovalSource.USER_REVIEWED,
            tags = emptyList(),
            createdAtEpochMs = memory.createdAtMs,
            updatedAtEpochMs = memory.updatedAtMs,
            occurredAtEpochMs = null,
            expiresAtEpochMs = null,
            originAssistantId = null,
            participants = emptyList(),
            outcome = null,
            sources = emptyList(),
        )
        val pin = DreamAuthorityPin(
            scopeId = SCOPE,
            memoryId = MEMORY_ID.toString(),
            expectedRevision = 1,
            expectedAuthorityFingerprint = DreamAuthorityFingerprintV1.compute(authority),
            expectedSourceManifestHash = DreamAuthorityFingerprintV1.sourceManifestHash(emptyList()),
        )
        val source = DreamClaimSourcePin(pin, DreamSupportType.SUPPORTS, directAuthority = true)
        val version = DreamValidatedClaimVersion(
            claimId = CLAIM_ID,
            expectedPreviousRevision = null,
            nextRevision = 1,
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
        db.dreamSynthesisDao().insertClaim(
            DreamClaimEntity(
                claimId = CLAIM_ID,
                scopeId = SCOPE.value,
                claimRevision = 1,
                claimKey = version.claimKey,
                storageClass = version.storageClass.name,
                epistemicType = version.epistemicType.name,
                title = version.title,
                statement = version.statement,
                state = version.nextState.name,
                confidence = 0.9,
                temporalState = version.temporalState.name,
                learnedAtMs = 20L,
                sourceTimezone = TIMEZONE,
                claimHash = canonical.contentHash.value,
                createdByRunId = OLD_RUN_ID,
                lastValidatedMemoryEpoch = 1L,
                createdAtMs = 20L,
                updatedAtMs = 20L,
            ),
        )
        db.dreamSynthesisDao().insertClaimVersion(
            DreamClaimVersionEntity(
                claimId = CLAIM_ID,
                claimRevision = 1,
                canonicalClaimJson = canonical.canonicalClaimJson,
                contentHash = canonical.contentHash.value,
                sourceManifestHash = canonical.sourceManifestHash.value,
                reasonCode = version.reason.name,
                createdByRunId = OLD_RUN_ID,
                createdAtMs = 20L,
            ),
        )
        db.dreamSynthesisDao().insertClaimVersionSources(
            listOf(
                DreamClaimVersionSourceEntity(
                    claimId = CLAIM_ID,
                    claimRevision = 1,
                    memoryId = MEMORY_ID,
                    memoryRevision = 1,
                    memorySemanticHash = pin.expectedAuthorityFingerprint.value,
                    supportType = DreamSupportType.SUPPORTS.name,
                    createdAtMs = 20L,
                ),
            ),
        )
        return SeededClaim(pin)
    }

    private suspend fun seedState() {
        db.dreamDao().insertScopeStateIfAbsent(
            MemoryScopeStateEntity(
                scopeId = SCOPE.value,
                memoryEpoch = 5L,
                observerCheckpointEpoch = 0L,
                updatedAtMs = 10L,
            ),
        )
    }

    private suspend fun begin(attemptNow: Long): BeginDreamSynthesisResult =
        store.begin(beginRequest(attemptNow, TIMEZONE))

    private fun beginRequest(attemptNow: Long, timezone: String) = BeginDreamSynthesisRequest(
        scopeId = SCOPE,
        runId = RUN_ID,
        leaseOwner = OWNER,
        attemptNowEpochMs = attemptNow,
        sourceTimezoneId = timezone,
        mode = DreamSynthesisMode.INCREMENTAL,
    )

    private fun commitRequest(
        fence: me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisFence,
        plan: DreamValidatedPlan,
        snapshot: me.rerere.rikkahub.memory.dreaming.snapshot.DreamCompiledSnapshot,
        livePins: List<DreamAuthorityPin>,
        historicalPins: List<DreamAuthorityPin>,
        inputMemoryCount: Int,
    ) = DreamSynthesisCommitRequest(
        fence = fence,
        plan = plan,
        snapshot = snapshot,
        liveAuthorityPins = livePins,
        historicalTransitionPins = historicalPins,
        inputManifestHash = HASH,
        outputManifestHash = snapshot.manifestHash,
        modelAudit = DreamModelAudit(
            providerKind = "test",
            modelIdentityDigest = HASH,
            promptContractVersion = DREAM_PROMPT_CONTRACT_VERSION,
            validatorVersion = DREAM_VALIDATOR_VERSION,
            inputTokens = null,
            outputTokens = null,
        ),
        inputMemoryCount = inputMemoryCount,
        outputOperationCount = 1,
        committedAtEpochMs = 300L,
    )

    private data class SeededClaim(val pin: DreamAuthorityPin)

    private companion object {
        val SCOPE = DreamScopeId.requireCanonical("11111111-1111-4111-8111-111111111111")
        val HASH = DreamSha256("a".repeat(64))
        val NONCE = DreamProposalNonce("p_" + "N".repeat(43))
        const val RUN_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        const val OLD_RUN_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        const val CLAIM_ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        const val SNAPSHOT_ID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
        const val MEMORY_REVISION_ID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
        const val MEMORY_ID = 1
        const val OWNER = "dream-test-worker"
        const val TIMEZONE = "Asia/Shanghai"
        const val COMPILER_REVISION = "dream-snapshot-compiler-v1"
        const val INITIAL_LEASE_MS = 15L * 60_000L
    }
}
