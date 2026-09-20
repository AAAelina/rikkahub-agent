package me.rerere.rikkahub.memory.dreaming.runtime

import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.entity.DreamClaimEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimVersionEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimVersionSourceEntity
import me.rerere.rikkahub.data.db.entity.DreamSnapshotEntity
import me.rerere.rikkahub.data.db.entity.MemoryEntity
import me.rerere.rikkahub.data.db.entity.MemoryRevisionEntity
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
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthoritySource
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimHead
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimMutationReason
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimSourcePin
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimState
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimVersionCanonicalV1
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicType
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamSha256
import me.rerere.rikkahub.memory.dreaming.model.DreamStorageClass
import me.rerere.rikkahub.memory.dreaming.model.DreamSupportType
import me.rerere.rikkahub.memory.dreaming.model.DreamValidatedClaimVersion
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotCompileRequest
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotCompiler
import me.rerere.rikkahub.memory.dreaming.temporal.TemporalState
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/** Emulator/disposable-device only. Never run this instrumentation test on the primary phone. */
@RunWith(AndroidJUnit4::class)
class RoomDreamSnapshotProjectionReaderContractTest {
    private lateinit var database: AppDatabase
    private lateinit var reader: RoomDreamSnapshotProjectionReader
    private val json = Json { ignoreUnknownKeys = false; isLenient = false }

    @Before
    fun setUp() {
        database = Room.inMemoryDatabaseBuilder(
            ApplicationProvider.getApplicationContext(),
            AppDatabase::class.java,
        ).build()
        reader = RoomDreamSnapshotProjectionReader(
            database,
            database.dreamSynthesisDao(),
            database.dreamExperienceDao(),
        )
    }

    @After
    fun tearDown() {
        database.close()
    }

    @Test
    fun read_rebuildsExactAtomicProjection_andUsesFrozenNowForAuthorityExpiry() = runBlocking {
        seed(expiresAtMs = NOW + 1L)

        val first = reader.read(DreamSnapshotProjectionReadRequest(SCOPE, NOW))
            as DreamSnapshotProjection.Available
        assertEquals(DreamRuntimeReadConsistency.ATOMIC, first.readConsistency)
        assertEquals(DreamRuntimePayloadIntegrity.VERIFIED, first.payloadIntegrity)
        assertEquals(DreamRuntimeFragmentIntegrity.VERIFIED, first.claims.single().fragmentIntegrity)
        assertEquals(DreamRuntimeSourceValidity.CURRENT_CONFIRMED, first.claims.single().sourceFence.validity)

        val expired = reader.read(DreamSnapshotProjectionReadRequest(SCOPE, NOW + 1L))
            as DreamSnapshotProjection.Available
        assertEquals(DreamRuntimeSourceValidity.EXPIRED, expired.claims.single().sourceFence.validity)
        val compiled = DreamContextCompiler.compile(
            DreamContextCompileRequest(
                useDreams = true,
                expectedScopeId = SCOPE,
                projection = expired,
                frozenNowEpochMs = NOW + 1L,
                limits = DreamRuntimeCompileLimits(1_000, 4_000, 8_000, 10),
            ),
        )
        assertEquals(DreamRuntimeCompileStatus.EMPTY, compiled.status)
    }

    @Test
    fun read_rejectsStaleEpochTamperedPayload_andNeverFallsAcrossScopes() = runBlocking {
        seed(expiresAtMs = null)
        database.openHelper.writableDatabase.execSQL(
            "UPDATE memory_scope_state SET memory_epoch = memory_epoch + 1 WHERE scope_id = ?",
            arrayOf(SCOPE.value),
        )
        assertEquals(
            DreamSnapshotProjection.Unavailable(
                DreamSnapshotProjectionUnavailableReason.ACTIVE_SNAPSHOT_MISSING,
            ),
            reader.read(DreamSnapshotProjectionReadRequest(SCOPE, NOW)),
        )

        database.openHelper.writableDatabase.execSQL(
            "UPDATE memory_scope_state SET last_applied_memory_epoch = memory_epoch WHERE scope_id = ?",
            arrayOf(SCOPE.value),
        )
        assertTrue(reader.read(DreamSnapshotProjectionReadRequest(SCOPE, NOW)) is DreamSnapshotProjection.Unavailable)
        assertEquals(
            DreamSnapshotProjection.Unavailable(
                DreamSnapshotProjectionUnavailableReason.SCOPE_STATE_MISSING,
            ),
            reader.read(DreamSnapshotProjectionReadRequest(DreamScopeId.Global, NOW)),
        )
    }

    private suspend fun seed(expiresAtMs: Long?) {
        val identity = MemorySourceIdentity(
            conversationId = "conversation-runtime",
            messageId = "message-runtime",
            role = MemorySourceRole.USER,
            consumedTextDigest = HASH_A,
            evidenceGroupId = "group-runtime",
        )
        val identityJson = json.encodeToString(listOf(identity))
        database.dreamDao().insertScopeStateIfAbsent(
            MemoryScopeStateEntity(
                scopeId = SCOPE.value,
                memoryEpoch = 4L,
                observerCheckpointEpoch = 4L,
                updatedAtMs = 10L,
                dreamStateRevision = 1L,
                lastAppliedMemoryEpoch = 4L,
                activeSnapshotId = SNAPSHOT_ID,
            ),
        )
        val memory = MemoryEntity(
            id = MEMORY_ID,
            assistantId = SCOPE.value,
            title = "Offline project",
            content = "The offline project is currently active.",
            createdAtMs = 1L,
            updatedAtMs = 10L,
            expiresAtMs = expiresAtMs,
            memoryKind = MemoryKind.PROJECT_FACT.name,
            confidence = 1f,
            sourceType = "TEST",
            sourceIdentitiesJson = identityJson,
            lifecycleStatus = MemoryLifecycleStatus.ACTIVE.name,
            approvalSource = MemoryApprovalSource.USER_REVIEWED.name,
            revision = 1,
            attribution = MemoryAttribution.USER.name,
            truthStatus = MemoryTruthStatus.CONFIRMED.name,
        )
        database.memoryDao().insertMemory(memory)
        database.memoryV2Dao().insertRevision(
            MemoryRevisionEntity(
                id = MEMORY_REVISION_ID,
                memoryId = MEMORY_ID,
                revision = 1,
                operation = "CREATE",
                actor = "TEST",
                sourceIdentitiesJson = identityJson,
                createdAtMs = 10L,
            ),
        )
        val source = identity.toAuthoritySource()
        val authority = memory.toAuthority(source)
        val pin = DreamAuthorityPin(
            scopeId = SCOPE,
            memoryId = MEMORY_ID.toString(),
            expectedRevision = 1L,
            expectedAuthorityFingerprint = DreamAuthorityFingerprintV1.compute(authority),
            expectedSourceManifestHash = DreamAuthorityFingerprintV1.sourceManifestHash(listOf(source)),
        )
        val version = DreamValidatedClaimVersion(
            claimId = CLAIM_ID,
            expectedPreviousRevision = null,
            nextRevision = 1L,
            claimKey = "project.offline",
            storageClass = DreamStorageClass.EPISODIC,
            epistemicType = DreamEpistemicType.PROJECT_STATE,
            nextState = DreamClaimState.ACTIVE_CONTEXTUAL,
            title = "Offline project",
            statement = "The offline project is currently active.",
            confidencePermille = 900,
            temporalState = TemporalState.CURRENT,
            validFromEpochMs = null,
            validToEpochMs = null,
            sources = listOf(DreamClaimSourcePin(pin, DreamSupportType.SUPPORTS, true)),
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
        database.dreamSynthesisDao().insertClaim(
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
                learnedAtMs = 10L,
                sourceTimezone = "Asia/Shanghai",
                claimHash = head.versionHash.value,
                createdByRunId = RUN_ID,
                lastValidatedMemoryEpoch = 4L,
                createdAtMs = 10L,
                updatedAtMs = 10L,
            ),
        )
        database.dreamSynthesisDao().insertClaimVersion(
            DreamClaimVersionEntity(
                claimId = CLAIM_ID,
                claimRevision = 1L,
                canonicalClaimJson = canonical.canonicalClaimJson,
                contentHash = canonical.contentHash.value,
                sourceManifestHash = canonical.sourceManifestHash.value,
                reasonCode = version.reason.name,
                createdByRunId = RUN_ID,
                createdAtMs = 10L,
            ),
        )
        database.dreamSynthesisDao().insertClaimVersionSources(
            listOf(
                DreamClaimVersionSourceEntity(
                    claimId = CLAIM_ID,
                    claimRevision = 1L,
                    memoryId = MEMORY_ID,
                    memoryRevision = 1,
                    memorySemanticHash = pin.expectedAuthorityFingerprint.value,
                    supportType = DreamSupportType.SUPPORTS.name,
                    createdAtMs = 10L,
                ),
            ),
        )
        val snapshot = DreamSnapshotCompiler.compile(
            DreamSnapshotCompileRequest(SCOPE, COMPILER_REVISION, listOf(head)),
        )
        database.dreamSynthesisDao().insertSnapshot(
            DreamSnapshotEntity(
                snapshotId = SNAPSHOT_ID,
                scopeId = SCOPE.value,
                snapshotRevision = 1L,
                sourceMemoryEpoch = 4L,
                committedDreamRevision = 1L,
                status = "ACTIVE",
                canonicalPayloadJson = snapshot.payloadJson,
                payloadSha256 = snapshot.payloadHash.value,
                compilerRevision = snapshot.compilerRevision,
                estimatedTokens = snapshot.estimatedTokens,
                claimCount = snapshot.claimCount,
                createdByRunId = RUN_ID,
                createdAtMs = 10L,
                reasonCode = "TEST",
            ),
        )
    }

    private fun MemorySourceIdentity.toAuthoritySource() = DreamAuthoritySource(
        conversationId = conversationId,
        messageId = messageId,
        role = role,
        sourceKind = sourceKind,
        consumedTextDigest = DreamSha256(consumedTextDigest),
        evidenceGroupId = evidenceGroupId,
    )

    private fun MemoryEntity.toAuthority(source: DreamAuthoritySource) = DreamAuthorityMemory(
        scopeId = SCOPE,
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
        occurredAtEpochMs = occurredAtMs,
        expiresAtEpochMs = expiresAtMs,
        originAssistantId = originAssistantId,
        participants = emptyList(),
        outcome = outcome,
        sources = listOf(source),
    )

    private companion object {
        val SCOPE = DreamScopeId.requireCanonical("10000000-0000-0000-0000-000000000001")
        const val CLAIM_ID = "20000000-0000-0000-0000-000000000001"
        const val SNAPSHOT_ID = "30000000-0000-0000-0000-000000000001"
        const val RUN_ID = "40000000-0000-0000-0000-000000000001"
        const val MEMORY_REVISION_ID = "50000000-0000-0000-0000-000000000001"
        const val MEMORY_ID = 701
        const val NOW = 1_000L
        const val COMPILER_REVISION = "dream-snapshot-compiler-v1"
        const val HASH_A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
}
