package me.rerere.rikkahub.learning.runtime

import android.app.ActivityManager
import android.app.Application
import android.content.Context
import android.database.sqlite.SQLiteException
import android.os.Build
import android.os.Process
import android.os.SystemClock
import androidx.room.Room
import androidx.room.RoomDatabase
import androidx.room.withTransaction
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import me.rerere.rikkahub.learning.grant.PolicyGrantAuthoritySnapshot
import me.rerere.rikkahub.learning.grant.PolicyGrantAuthoritySource
import me.rerere.rikkahub.learning.grant.PolicyGrantAuthorityState
import me.rerere.rikkahub.learning.grant.PolicyGrantLifecyclePendingReason
import me.rerere.rikkahub.learning.grant.PolicyGrantLifecycleProjectionResult
import me.rerere.rikkahub.learning.grant.PolicyGrantLifecycleProjector
import me.rerere.rikkahub.learning.grant.PolicyGrantLifecycleProjectionStep
import me.rerere.rikkahub.learning.grant.PolicyGrantRebindCatchUp
import me.rerere.rikkahub.learning.grant.PolicyGrantRebindCatchUpResult
import me.rerere.rikkahub.learning.grant.exactCompletePolicyGrantRebindStreamOrNull
import me.rerere.rikkahub.learning.grant.nextPolicyGrantLifecycleProjectionStep
import me.rerere.rikkahub.learning.episode.EpisodeIdFactory
import me.rerere.rikkahub.learning.exposure.PolicyExposureRuntimeAnchor
import me.rerere.rikkahub.learning.exposure.PolicyExposureRuntimeAnchorRequest
import me.rerere.rikkahub.learning.exposure.PolicyExposureRuntimeAnchorSource
import me.rerere.rikkahub.learning.exposure.PolicyExposureStore
import me.rerere.rikkahub.learning.exposure.PolicyExposureStoreResult
import me.rerere.rikkahub.learning.exposure.PolicyExposureReservation
import me.rerere.rikkahub.learning.exposure.PolicyExposureMetadata
import me.rerere.rikkahub.learning.exposure.PolicyExposureState
import me.rerere.rikkahub.learning.exposure.PolicyExposureOutcomeAuthority
import me.rerere.rikkahub.learning.exposure.RoomPolicyExposureStore
import me.rerere.rikkahub.data.ai.ProviderAttemptEvent
import me.rerere.rikkahub.learning.handoff.LearningOutboxReader
import me.rerere.rikkahub.learning.handoff.LearningReconciliationScanner
import me.rerere.rikkahub.learning.diagnostics.LearningDiagnosticCode
import me.rerere.rikkahub.learning.diagnostics.LearningDiagnosticSample
import me.rerere.rikkahub.learning.diagnostics.LearningDiagnosticState
import me.rerere.rikkahub.learning.diagnostics.LearningDiagnosticsStore
import me.rerere.rikkahub.learning.jobs.LearningDrainResult
import me.rerere.rikkahub.learning.jobs.LearningJobClock
import me.rerere.rikkahub.learning.jobs.LearningJobClockRollbackException
import me.rerere.rikkahub.learning.jobs.LearningJobCoordinator
import me.rerere.rikkahub.learning.jobs.LearningJobHandlerRegistry
import me.rerere.rikkahub.learning.jobs.P1LearningRuntimeBindings
import me.rerere.rikkahub.learning.jobs.P1LearningRuntimeDependencyFactory
import me.rerere.rikkahub.learning.jobs.UnconfiguredP1LearningRuntimeDependencyFactory
import me.rerere.rikkahub.learning.jobs.P1DerivedJobCatchUp
import me.rerere.rikkahub.learning.jobs.P1DerivedJobCatchUpResult
import me.rerere.rikkahub.learning.jobs.NoOpP1DerivedJobCatchUp
import me.rerere.rikkahub.learning.jobs.LearningJobStartupRecoveryResult
import me.rerere.rikkahub.learning.model.LearningFeatureFlagSource
import me.rerere.rikkahub.learning.model.LearningScope
import me.rerere.rikkahub.learning.policy.LearningPolicyStatus
import me.rerere.rikkahub.learning.policy.PolicyCandidateType
import me.rerere.rikkahub.learning.policy.policyArtifactSha256
import me.rerere.rikkahub.learning.policy.PolicyGrantBindingProof
import me.rerere.rikkahub.learning.policy.PolicyLifecycleReason
import me.rerere.rikkahub.learning.policy.PolicyDriftGovernor
import me.rerere.rikkahub.learning.policy.PolicyMutationActor
import me.rerere.rikkahub.learning.policy.PolicyMutationFence
import me.rerere.rikkahub.learning.policy.PolicyMutationRequest
import me.rerere.rikkahub.learning.policy.PolicyMutationResult
import me.rerere.rikkahub.learning.privacy.LearningDerivedEraseFailureCode
import me.rerere.rikkahub.learning.privacy.LearningDerivedEraseUnavailableException
import me.rerere.rikkahub.learning.privacy.LearningEphemeralScopeEraser
import me.rerere.rikkahub.learning.privacy.ExactScopeLearnedWorkflowErasePort
import me.rerere.rikkahub.learning.privacy.DurableLearnedWorkflowPrivacyPort
import me.rerere.rikkahub.learning.promotion.RoomWorkflowPromotionCandidateStore
import me.rerere.rikkahub.learning.promotion.LearnedWorkflowSourceAuthorityPort
import me.rerere.rikkahub.learning.promotion.WorkflowPromotionCandidateRuntime
import me.rerere.rikkahub.learning.workflow.model.LearnedWorkflowCandidate
import me.rerere.rikkahub.learning.workflow.model.LearnedWorkflowCandidateState
import me.rerere.rikkahub.learning.workflow.runtime.WorkflowCandidateInsertResult
import me.rerere.rikkahub.learning.workflow.runtime.WorkflowCandidateReadResult
import me.rerere.rikkahub.learning.workflow.runtime.WorkflowCandidateSubmissionRuntime
import me.rerere.rikkahub.learning.workflow.runtime.WorkflowCandidateTransition
import me.rerere.rikkahub.learning.workflow.runtime.WorkflowCandidateTransitionResult
import me.rerere.rikkahub.learning.workflow.runtime.MAX_PROPOSAL_EVIDENCE
import me.rerere.rikkahub.learning.workflow.runtime.ReviewedPolicyWorkflowEvidenceRecord
import me.rerere.rikkahub.learning.workflow.runtime.ReviewedPolicyWorkflowProposalRejection
import me.rerere.rikkahub.learning.workflow.runtime.ReviewedPolicyWorkflowProposalRequest
import me.rerere.rikkahub.learning.workflow.runtime.ReviewedPolicyWorkflowSourceResult
import me.rerere.rikkahub.learning.workflow.runtime.ReviewedPolicyWorkflowSourceRuntimePort
import me.rerere.rikkahub.learning.workflow.runtime.projectExactReviewedPolicyWorkflowSource
import me.rerere.rikkahub.learning.workflow.review.MAX_WORKFLOW_REVIEW_PAGE_SIZE
import me.rerere.rikkahub.learning.workflow.review.MAX_WORKFLOW_REVIEW_REVISIONS
import me.rerere.rikkahub.learning.workflow.review.WorkflowReviewReadResult
import me.rerere.rikkahub.learning.workflow.review.WorkflowReviewRuntimePort
import me.rerere.rikkahub.learning.workflow.review.WorkflowReviewUnavailableReason
import me.rerere.rikkahub.learning.workflow.review.toWorkflowReviewDetailOrNull
import me.rerere.rikkahub.learning.workflow.review.toWorkflowReviewListItemOrNull
import me.rerere.rikkahub.learning.review.MAX_POLICY_REDACTED_REPORT_CHARS
import me.rerere.rikkahub.learning.review.MAX_POLICY_REVIEW_PAGE_SIZE
import me.rerere.rikkahub.learning.review.MAX_POLICY_REVIEW_REVISIONS
import me.rerere.rikkahub.learning.review.PolicyReviewDetail
import me.rerere.rikkahub.learning.review.PolicyReviewExportResult
import me.rerere.rikkahub.learning.review.PolicyReviewExposureSummary
import me.rerere.rikkahub.learning.review.PolicyReviewFence
import me.rerere.rikkahub.learning.review.PolicyReviewLifecycleAction
import me.rerere.rikkahub.learning.review.PolicyReviewLifecycleCommand
import me.rerere.rikkahub.learning.review.PolicyReviewListItem
import me.rerere.rikkahub.learning.review.PolicyReviewReadResult
import me.rerere.rikkahub.learning.review.PolicyReviewRevision
import me.rerere.rikkahub.learning.review.PolicyReviewRuntimeMutationResult
import me.rerere.rikkahub.learning.review.PolicyReviewRuntimePort
import me.rerere.rikkahub.learning.review.PolicyReviewUnavailableReason
import me.rerere.rikkahub.learning.retrieval.PolicyOpaqueIdFactory
import me.rerere.rikkahub.learning.retrieval.LearnedPolicyCandidatePacket
import me.rerere.rikkahub.learning.retrieval.LearnedPolicyContextItem
import me.rerere.rikkahub.learning.retrieval.LearnedPolicyContextTrust
import me.rerere.rikkahub.learning.retrieval.LearnedPolicyGrantReceipt
import me.rerere.rikkahub.learning.retrieval.LearnedPolicyQuery
import me.rerere.rikkahub.learning.retrieval.LearnedPolicyRetrievalResult
import me.rerere.rikkahub.learning.retrieval.LearnedPolicySource
import me.rerere.rikkahub.learning.retrieval.MAX_LEARNED_POLICY_CONTEXT_ITEM_CHARS
import me.rerere.rikkahub.learning.retrieval.PolicyRetriever
import me.rerere.rikkahub.learning.retrieval.PolicyShadowFeatureGate
import me.rerere.rikkahub.learning.retrieval.PolicyShadowObservationCommitResult
import me.rerere.rikkahub.learning.retrieval.PolicyShadowRuntimePort
import me.rerere.rikkahub.learning.retrieval.PolicyShadowRuntimeRequest
import me.rerere.rikkahub.learning.retrieval.PolicyShadowRuntimeResult
import me.rerere.rikkahub.learning.retrieval.PolicyDispatchSurfaceObservationResult
import me.rerere.rikkahub.learning.retrieval.RoomPolicyShadowRetriever
import me.rerere.rikkahub.learning.retrieval.RoomPolicyShadowObservationStore
import me.rerere.rikkahub.learning.policy.runtime.ActivePolicyApplicabilitySnapshot
import me.rerere.rikkahub.learning.policy.runtime.PolicyDriftObservationKind
import me.rerere.rikkahub.learning.policy.runtime.PolicyExactDispatchSchemaObserver
import me.rerere.rikkahub.learning.policy.ObservedUtilityAssignmentMethod
import me.rerere.rikkahub.learning.policy.ObservedUtilityAttributionUnit
import me.rerere.rikkahub.learning.policy.ObservedUtilityCohortIdentity
import me.rerere.rikkahub.learning.policy.ObservedUtilityDesign
import me.rerere.rikkahub.learning.policy.ObservedUtilitySelectionMethod
import me.rerere.rikkahub.learning.policy.runtime.ObservedUtilityLedgerWriteResult
import me.rerere.rikkahub.learning.policy.runtime.ObservedUtilityMatchedAssignmentIntent
import me.rerere.rikkahub.learning.policy.runtime.ObservedUtilityMatchedAssignmentIntentPort
import me.rerere.rikkahub.learning.policy.runtime.ObservedUtilityPreTreatmentAssignment
import me.rerere.rikkahub.learning.policy.runtime.RoomObservedUtilityLedger
import me.rerere.rikkahub.learning.policy.runtime.ObservedUtilityMaintenanceCoordinator
import me.rerere.rikkahub.learning.policy.runtime.ObservedUtilityMaintenanceCursor
import me.rerere.rikkahub.learning.policy.runtime.ObservedUtilityMaintenancePageResult
import me.rerere.rikkahub.learning.policy.runtime.ProductionObservedUtilityRuntime
import me.rerere.rikkahub.learning.policy.runtime.MAX_OBSERVED_UTILITY_MAINTENANCE_DESIGNS
import me.rerere.rikkahub.learning.retention.LearningRetentionMaintenancePort
import me.rerere.rikkahub.learning.retention.LearningRetentionMaintenanceReceipt
import me.rerere.rikkahub.learning.retention.LearningRetentionMaintenanceRequest
import me.rerere.rikkahub.learning.retention.LearningRetentionRuntimeResult
import me.rerere.rikkahub.learning.retention.LearningOutboxRetentionResult
import me.rerere.rikkahub.learning.retention.LearningPrimaryOutboxRetentionPort
import me.rerere.rikkahub.learning.retention.freezeDerivedOutboxConsumerCheckpointOrNull
import me.rerere.rikkahub.learning.retention.prunePrimaryOutboxFromFrozenCheckpoint
import me.rerere.rikkahub.learning.storage.LearningDatabase
import me.rerere.rikkahub.learning.storage.LearningDerivedDataEraseStore
import me.rerere.rikkahub.learning.storage.LearningJobType
import me.rerere.rikkahub.learning.storage.LearningRetentionPolicyV1
import me.rerere.rikkahub.learning.storage.LearningRetentionResult
import me.rerere.rikkahub.learning.storage.LearningRetentionStore
import me.rerere.rikkahub.learning.storage.LearningPolicyEntity
import me.rerere.rikkahub.learning.storage.LearningPolicyRevisionActor
import me.rerere.rikkahub.learning.storage.LearningPolicyRevisionReason
import me.rerere.rikkahub.learning.storage.PolicyRevisionEntity
import me.rerere.rikkahub.learning.storage.PolicyApplicabilityWire
import me.rerere.rikkahub.learning.storage.PolicyIdentityApplicability
import me.rerere.rikkahub.learning.storage.entity.toDomainOrNull
import me.rerere.rikkahub.learning.storage.entity.toEntity
import me.rerere.rikkahub.learning.storage.RoomPolicyLifecycleMutationStore
import me.rerere.rikkahub.learning.storage.LearningScopeEraseResult
import me.rerere.rikkahub.learning.storage.LEARNING_MIGRATION_1_2
import me.rerere.rikkahub.learning.storage.LEARNING_MIGRATION_2_3
import me.rerere.rikkahub.learning.storage.LEARNING_MIGRATION_3_4
import me.rerere.rikkahub.learning.storage.LEARNING_MIGRATION_4_5
import me.rerere.rikkahub.learning.storage.LEARNING_MIGRATION_5_6
import me.rerere.rikkahub.learning.storage.LEARNING_MIGRATION_6_7
import me.rerere.rikkahub.learning.storage.LEARNING_MIGRATION_7_8
import me.rerere.rikkahub.learning.storage.LEARNING_MIGRATION_8_9
import me.rerere.rikkahub.learning.storage.restore.LearningRestoreFailureReason
import me.rerere.rikkahub.learning.storage.restore.LearningRestoreRuntimeFence
import me.rerere.rikkahub.learning.storage.restore.LearningRuntimeRestorePort
import me.rerere.rikkahub.learning.trace.TraceSanitizationResult
import me.rerere.rikkahub.learning.trace.TraceSanitizer
import kotlin.uuid.Uuid

enum class LearningRuntimeState {
    CLOSED,
    READY,
    RESTORING,
    DEGRADED,
    DISABLED,
}

enum class LearningRuntimeErrorCode {
    DATABASE_OPEN_FAILED,
    DATABASE_OPERATION_FAILED,
    DATABASE_RETRY_BACKOFF,
    FLAG_SOURCE_FAILED,
    RESTORE_IN_PROGRESS,
    RESTORE_FAILED_RESTART_REQUIRED,
    RUNTIME_NOT_CONFIGURED,
    WRONG_PROCESS,
}

sealed interface LearningRuntimeAccess {
    data object Ready : LearningRuntimeAccess

    data object Disabled : LearningRuntimeAccess

    data class Unavailable(val errorCode: LearningRuntimeErrorCode) : LearningRuntimeAccess
}

internal fun interface LearningRuntimeInitializer {
    /** Must finish all database work before returning and must never retain [database] or its DAOs. */
    suspend fun initialize(database: LearningDatabase, runtimeGeneration: Long, frozenNowMs: Long)
}

/** Short-lived handle. Late callbacks must check [isCurrent] before committing derived output. */
class LearningRuntimeSession internal constructor(
    val generation: Long,
    private val currentGeneration: () -> Long,
    private val restoreLatched: () -> Boolean,
) {
    private val active = AtomicBoolean(true)

    fun isCurrent(): Boolean =
        active.get() && !restoreLatched() && generation == currentGeneration()

    internal fun expire() {
        active.set(false)
    }

    override fun toString(): String =
        "LearningRuntimeSession(generation=$generation)"
}

/**
 * Lazy, process-local access to the rebuildable Learning database.
 *
 * The mutex plus structured [coroutineScope] is the restore quiescence boundary. Learning code is
 * forbidden from using GlobalScope or retaining [LearningRuntimeSession]/Room objects after the
 * operation returns. A successful main-database restore permanently latches this facade closed;
 * only a new process may open the derived database again.
 */
class LearningRuntimeFacade internal constructor(
    context: Context,
    private val isEnabled: () -> Boolean,
    private val initializer: LearningRuntimeInitializer = LearningRuntimeInitializer { _, _, _ -> },
    private val clock: () -> Long = System::currentTimeMillis,
    private val retryBackoffMs: Long = 30_000L,
    private val isMainProcess: () -> Boolean = { isCurrentMainProcess(context.applicationContext) },
    private val outboxReader: LearningOutboxReader? = null,
    private val reconciliationScanner: LearningReconciliationScanner? = null,
    private val monotonicMs: () -> Long = SystemClock::elapsedRealtime,
    /** Fresh for this OS process; never persisted or reused as a durable identity. */
    private val processSessionId: Uuid = Uuid.random(),
    private val diagnosticsStore: LearningDiagnosticsStore? = null,
    private val jobHandlerRegistry: LearningJobHandlerRegistry? = null,
    private val p1RuntimeDependencyFactory: P1LearningRuntimeDependencyFactory =
        UnconfiguredP1LearningRuntimeDependencyFactory,
    private val policyShadowFeatureGate: PolicyShadowFeatureGate? = null,
    private val policyOpaqueIds: PolicyOpaqueIdFactory? = null,
    /** Optional for old tests; production wiring supplies both P2 provider-effect fences. */
    private val learningFeatureFlags: LearningFeatureFlagSource? = null,
    private val policyGrantAuthority: PolicyGrantAuthoritySource? = null,
    /** Primary AppDatabase boundary; production always supplies it, legacy fixtures may omit it. */
    private val primaryOutboxRetention: LearningPrimaryOutboxRetentionPort? = null,
    /** AppDatabase privacy fences; production supplies both from one singleton adapter. */
    private val learnedWorkflowErasePort: ExactScopeLearnedWorkflowErasePort? = null,
    private val durableLearnedWorkflowPrivacyPort: DurableLearnedWorkflowPrivacyPort? = null,
    private val sqliteOpenHelperFactory: androidx.sqlite.db.SupportSQLiteOpenHelper.Factory? = null,
) : LearningRuntimeMaintenancePort,
    LearningRetentionMaintenancePort,
    PolicyShadowRuntimePort,
    LearnedPolicySource,
    PolicyExposureRuntimeAnchorSource,
    PolicyExposureStore,
    ObservedUtilityMatchedAssignmentIntentPort,
    PolicyGrantLifecycleProjector,
    PolicyReviewRuntimePort,
    WorkflowReviewRuntimePort,
    WorkflowPromotionCandidateRuntime,
    LearnedWorkflowSourceAuthorityPort,
    WorkflowCandidateSubmissionRuntime,
    ReviewedPolicyWorkflowSourceRuntimePort,
    me.rerere.rikkahub.learning.curator.CuratorCandidateProductionStore,
    me.rerere.rikkahub.learning.curator.CuratorReviewRuntimeStore,
    me.rerere.rikkahub.learning.curator.CuratorApplyRuntimeStore,
    me.rerere.rikkahub.assistant.SecondUserDerivedAuthorityInvalidationPort {
    private val applicationContext = context.applicationContext
    private val mutex = LearningRuntimeOperationFence()
    private val restoreFenceLock = Any()
    private val mutableState = MutableStateFlow(LearningRuntimeState.CLOSED)
    private val restoreLatched = AtomicBoolean(false)
    @Volatile
    private var latchedRestoreState = LearningRuntimeState.RESTORING
    private val runtimeGeneration = AtomicLong(1L)
    private var database: LearningDatabase? = null
    private var initializedDatabase: LearningDatabase? = null
    private var initializedJobHandlerRegistry: LearningJobHandlerRegistry? = null
    private var initializedP1CatchUp: P1DerivedJobCatchUp = NoOpP1DerivedJobCatchUp
    private var initializedPolicyGrantRebindCatchUp: PolicyGrantRebindCatchUp? = null
    private var nextOpenAttemptAtMs: Long = 0L

    init {
        require(retryBackoffMs in 1_000L..10L * 60L * 1_000L) { "Unsafe retry backoff" }
        require(processSessionId != Uuid.parse("00000000-0000-0000-0000-000000000000")) {
            "Learning process session UUID cannot be nil"
        }
    }

    val state: StateFlow<LearningRuntimeState> = mutableState.asStateFlow()

    fun currentGeneration(): Long = runtimeGeneration.get()

    override suspend fun invalidateExactAuthorityBatch(
        request: me.rerere.rikkahub.assistant.SecondUserDerivedAuthorityInvalidationRequest,
    ): me.rerere.rikkahub.assistant.SecondUserDerivedAuthorityInvalidationResult {
        // No derived database means there is provably no Policy/candidate projection to stale.
        // Do not strand an otherwise unrelated second-user unassignment merely because Learning
        // has never been enabled on this installation.
        if (!applicationContext.getDatabasePath(LearningDatabase.FILE_NAME).isFile) {
            return me.rerere.rikkahub.assistant.SecondUserDerivedAuthorityInvalidationResult.Ready(
                me.rerere.rikkahub.assistant.SecondUserDerivedAuthorityInvalidationBatch(
                    policiesMadeStale = 0,
                    workflowCandidatesMadeStale = 0,
                    complete = true,
                ),
            )
        }
        var result: me.rerere.rikkahub.assistant.SecondUserDerivedAuthorityInvalidationResult? =
            null
        val access = try {
            withDatabase { session ->
                if (!session.isCurrent()) return@withDatabase
                val opened = checkNotNull(database)
                result = me.rerere.rikkahub.learning.authority
                    .RoomSecondUserDerivedAuthorityInvalidationPort(opened)
                    .invalidateExactAuthorityBatch(request)
                if (!session.isCurrent()) {
                    result = me.rerere.rikkahub.assistant
                        .SecondUserDerivedAuthorityInvalidationResult.Unavailable
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            return me.rerere.rikkahub.assistant
                .SecondUserDerivedAuthorityInvalidationResult.Unavailable
        }
        return result.takeIf { access == LearningRuntimeAccess.Ready }
            ?: me.rerere.rikkahub.assistant
                .SecondUserDerivedAuthorityInvalidationResult.Unavailable
    }

    override suspend fun readExactReviewedPolicyWorkflowSource(
        request: ReviewedPolicyWorkflowProposalRequest,
    ): ReviewedPolicyWorkflowSourceResult {
        val expectedStreamId = request.fence.sourceStreamId
            ?: return ReviewedPolicyWorkflowSourceResult.Rejected(
                ReviewedPolicyWorkflowProposalRejection.POLICY_FENCE_CONFLICT,
            )
        var result: ReviewedPolicyWorkflowSourceResult? = null
        val access = try {
            withDatabase {
                val opened = checkNotNull(database)
                result = opened.withTransaction {
                    opened.readExactReviewedPolicyWorkflowSourceInTransaction(
                        request,
                        expectedStreamId,
                    )
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return ReviewedPolicyWorkflowSourceResult.Unavailable
        }
        return if (access == LearningRuntimeAccess.Ready) {
            result ?: ReviewedPolicyWorkflowSourceResult.Unavailable
        } else {
            ReviewedPolicyWorkflowSourceResult.Unavailable
        }
    }

    override suspend fun listForReview(
        consumingAssistantId: Uuid,
        limit: Int,
    ): PolicyReviewReadResult<List<PolicyReviewListItem>> {
        if (limit !in 1..MAX_POLICY_REVIEW_PAGE_SIZE) {
            return PolicyReviewReadResult.Unavailable(
                PolicyReviewUnavailableReason.ACTION_NOT_ALLOWED,
            )
        }
        var result: List<PolicyReviewListItem>? = null
        val access = try {
            withDatabase {
                val opened = checkNotNull(database)
                val streamId = opened.currentReadyReviewStreamId()
                result = opened.policyDao()
                    .listForBoundedReview(consumingAssistantId.toString(), limit)
                    .map { policy -> opened.toPolicyReviewListItem(policy, streamId) }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return PolicyReviewReadResult.Unavailable(
                PolicyReviewUnavailableReason.STORAGE_FAILURE,
            )
        }
        return when (access) {
            LearningRuntimeAccess.Ready -> PolicyReviewReadResult.Ready(checkNotNull(result))
            else -> PolicyReviewReadResult.Unavailable(access.toPolicyReviewUnavailableReason())
        }
    }

    override suspend fun list(
        request: me.rerere.rikkahub.learning.curator.CuratorReviewListRequest,
    ): List<me.rerere.rikkahub.learning.curator.CuratorReviewListItem> {
        var result = emptyList<me.rerere.rikkahub.learning.curator.CuratorReviewListItem>()
        val access = withDatabase {
            result = me.rerere.rikkahub.learning.storage.curator.RoomCuratorReviewRuntimeStore(
                checkNotNull(database),
            ).list(request)
        }
        return if (access == LearningRuntimeAccess.Ready) result else emptyList()
    }

    override suspend fun propose(
        request: me.rerere.rikkahub.learning.curator.CuratorCandidateProductionRequest,
    ): me.rerere.rikkahub.learning.curator.CuratorCandidateProductionResult {
        var result: me.rerere.rikkahub.learning.curator.CuratorCandidateProductionResult =
            me.rerere.rikkahub.learning.curator.CuratorCandidateProductionResult.Conflict(
                me.rerere.rikkahub.learning.curator.CuratorCandidateProductionConflict
                    .RUNTIME_UNAVAILABLE,
            )
        val access = withDatabase {
            result = me.rerere.rikkahub.learning.storage.curator
                .RoomCuratorCandidateProductionStore(checkNotNull(database))
                .propose(request)
        }
        return if (access == LearningRuntimeAccess.Ready) result else
            me.rerere.rikkahub.learning.curator.CuratorCandidateProductionResult.Conflict(
                me.rerere.rikkahub.learning.curator.CuratorCandidateProductionConflict
                    .RUNTIME_UNAVAILABLE,
            )
    }

    override suspend fun listExactReviewedSources(
        consumingAssistantId: Uuid,
        limit: Int,
    ): List<me.rerere.rikkahub.learning.curator.CuratorProductionSourceProjection> {
        var result = emptyList<
            me.rerere.rikkahub.learning.curator.CuratorProductionSourceProjection
            >()
        val access = withDatabase {
            result = me.rerere.rikkahub.learning.storage.curator
                .RoomCuratorCandidateProductionStore(checkNotNull(database))
                .listExactReviewedSources(consumingAssistantId, limit)
        }
        return if (access == LearningRuntimeAccess.Ready) result else emptyList()
    }

    override suspend fun read(
        candidateId: String,
        scope: LearningScope,
    ): me.rerere.rikkahub.learning.curator.CuratorReviewDetail? {
        var result: me.rerere.rikkahub.learning.curator.CuratorReviewDetail? = null
        val access = withDatabase {
            result = me.rerere.rikkahub.learning.storage.curator.RoomCuratorReviewRuntimeStore(
                checkNotNull(database),
            ).read(candidateId, scope)
        }
        return result.takeIf { access == LearningRuntimeAccess.Ready }
    }

    override suspend fun approve(
        request: me.rerere.rikkahub.learning.curator.CuratorReviewMutationRequest,
    ): me.rerere.rikkahub.learning.curator.CuratorReviewMutationResult =
        mutateCuratorReview { approve(request) }

    override suspend fun reject(
        request: me.rerere.rikkahub.learning.curator.CuratorReviewMutationRequest,
    ): me.rerere.rikkahub.learning.curator.CuratorReviewMutationResult =
        mutateCuratorReview { reject(request) }

    override suspend fun archive(
        request: me.rerere.rikkahub.learning.curator.CuratorReviewMutationRequest,
    ): me.rerere.rikkahub.learning.curator.CuratorReviewMutationResult =
        mutateCuratorReview { archive(request) }

    override suspend fun listRetentionArchivable(
        cutoffMs: Long,
        after: me.rerere.rikkahub.learning.curator.CuratorRetentionArchiveCursor,
        limit: Int,
    ): List<me.rerere.rikkahub.learning.curator.CuratorReviewListItem> {
        var result = emptyList<me.rerere.rikkahub.learning.curator.CuratorReviewListItem>()
        val access = withDatabase {
            result = me.rerere.rikkahub.learning.storage.curator.RoomCuratorReviewRuntimeStore(
                checkNotNull(database),
            ).listRetentionArchivable(cutoffMs, after, limit)
        }
        return if (access == LearningRuntimeAccess.Ready) result else emptyList()
    }

    override suspend fun archiveRetention(
        request: me.rerere.rikkahub.learning.curator.CuratorRetentionArchiveRequest,
    ): me.rerere.rikkahub.learning.curator.CuratorReviewMutationResult =
        mutateCuratorReview { archiveRetention(request) }

    override suspend fun applyApproved(
        request: me.rerere.rikkahub.learning.curator.CuratorRuntimeApplyRequest,
    ): me.rerere.rikkahub.learning.curator.CuratorRuntimeMutationResult =
        mutateCuratorRuntime { applyApproved(request) }

    override suspend fun rollbackApplied(
        request: me.rerere.rikkahub.learning.curator.CuratorRuntimeRollbackRequest,
    ): me.rerere.rikkahub.learning.curator.CuratorRuntimeMutationResult =
        mutateCuratorRuntime { rollbackApplied(request) }

    private suspend fun mutateCuratorReview(
        operation: suspend me.rerere.rikkahub.learning.storage.curator.RoomCuratorReviewRuntimeStore.() ->
            me.rerere.rikkahub.learning.curator.CuratorReviewMutationResult,
    ): me.rerere.rikkahub.learning.curator.CuratorReviewMutationResult {
        var result: me.rerere.rikkahub.learning.curator.CuratorReviewMutationResult =
            me.rerere.rikkahub.learning.curator.CuratorReviewMutationResult.Conflict(
                me.rerere.rikkahub.learning.curator.CuratorReviewConflict.FENCE_CONFLICT,
            )
        val access = withDatabase {
            result = me.rerere.rikkahub.learning.storage.curator.RoomCuratorReviewRuntimeStore(
                checkNotNull(database),
            ).operation()
        }
        return if (access == LearningRuntimeAccess.Ready) result else
            me.rerere.rikkahub.learning.curator.CuratorReviewMutationResult.Conflict(
                me.rerere.rikkahub.learning.curator.CuratorReviewConflict.FENCE_CONFLICT,
            )
    }

    private suspend fun mutateCuratorRuntime(
        operation: suspend me.rerere.rikkahub.learning.storage.curator.RoomCuratorApplyRuntimeStore.() ->
            me.rerere.rikkahub.learning.curator.CuratorRuntimeMutationResult,
    ): me.rerere.rikkahub.learning.curator.CuratorRuntimeMutationResult {
        var result: me.rerere.rikkahub.learning.curator.CuratorRuntimeMutationResult =
            me.rerere.rikkahub.learning.curator.CuratorRuntimeMutationResult.Conflict(
                me.rerere.rikkahub.learning.curator.CuratorRuntimeConflict.CANDIDATE_FENCE_CONFLICT,
            )
        val access = withDatabase {
            result = me.rerere.rikkahub.learning.storage.curator.RoomCuratorApplyRuntimeStore(
                checkNotNull(database),
            ).operation()
        }
        return if (access == LearningRuntimeAccess.Ready) result else
            me.rerere.rikkahub.learning.curator.CuratorRuntimeMutationResult.Conflict(
                me.rerere.rikkahub.learning.curator.CuratorRuntimeConflict.CANDIDATE_FENCE_CONFLICT,
            )
    }

    override suspend fun readForReview(
        consumingAssistantId: Uuid,
        policyId: String,
    ): PolicyReviewReadResult<PolicyReviewDetail> {
        if (policyId.isBlank() || policyId.length > 256) {
            return PolicyReviewReadResult.NotFound
        }
        var result: PolicyReviewDetail? = null
        var found = false
        val access = try {
            withDatabase {
                val opened = checkNotNull(database)
                val policy = opened.policyDao().findPolicy(policyId) ?: return@withDatabase
                val scope = LearningScope.parseOrNull(policy.scopeKind, policy.scopeId)
                    ?: return@withDatabase
                if (scope is LearningScope.Assistant &&
                    scope.assistantId != consumingAssistantId
                ) {
                    return@withDatabase
                }
                found = true
                val streamId = opened.currentReadyReviewStreamId()
                val item = opened.toPolicyReviewListItem(policy, streamId)
                result = PolicyReviewDetail(
                    item = item,
                    policyType = policy.policyType,
                    taskSignature = policy.taskSignature,
                    procedureSummary = policy.procedureSummary,
                    verificationSummary = policy.verificationSummary,
                    boundarySummary = policy.boundarySummary,
                    failureModeSummary = policy.failureModeSummary,
                    producerModelIdentity = policy.producerModelIdentity,
                    producerProviderIdentity = policy.producerProviderIdentity,
                    producerProviderKind = policy.producerProviderKind,
                    producerPromptIdentity = policy.producerPromptIdentity,
                    producerTemplateIdentity = policy.producerTemplateIdentity,
                    producerSchemaIdentity = policy.producerSchemaIdentity,
                    revisions = opened.policyDao()
                        .listRevisions(policy.id, MAX_POLICY_REVIEW_REVISIONS)
                        .map { revision ->
                            PolicyReviewRevision(
                                revision = revision.revision,
                                reasonCode = revision.reasonCode,
                                actor = revision.actor,
                                artifactSha256 = revision.afterArtifactSha256,
                                createdAtMs = revision.createdAtMs,
                                isCurrent = revision.revision == policy.stateVersion &&
                                    revision.afterArtifactSha256 == policy.artifactSha256,
                                historicContentRestorable = revision.afterSnapshot
                                    .startsWith("policy-candidate-snapshot-v3\n"),
                                changedFields = changedPolicySnapshotFields(
                                    revision.beforeSnapshot,
                                    revision.afterSnapshot,
                                ),
                            )
                        },
                )
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return PolicyReviewReadResult.Unavailable(
                PolicyReviewUnavailableReason.STORAGE_FAILURE,
            )
        }
        return when (access) {
            LearningRuntimeAccess.Ready -> if (found) {
                PolicyReviewReadResult.Ready(checkNotNull(result))
            } else {
                PolicyReviewReadResult.NotFound
            }
            else -> PolicyReviewReadResult.Unavailable(access.toPolicyReviewUnavailableReason())
        }
    }

    override suspend fun mutateForReview(
        command: PolicyReviewLifecycleCommand,
    ): PolicyReviewRuntimeMutationResult {
        var result: PolicyReviewRuntimeMutationResult? = null
        val access = try {
            withDatabase {
                val opened = checkNotNull(database)
                if (command.action == PolicyReviewLifecycleAction.RESTORE_ARCHIVED_REVISION &&
                    command.selectedRevision != command.fence.stateVersion
                ) {
                    result = opened.restoreHistoricPolicyRevision(command)
                    return@withDatabase
                }
                val target = when (command.action) {
                    PolicyReviewLifecycleAction.ARCHIVE -> LearningPolicyStatus.ARCHIVED
                    PolicyReviewLifecycleAction.RESTORE_ARCHIVED_REVISION ->
                        LearningPolicyStatus.SHADOW
                    PolicyReviewLifecycleAction.SUSPEND -> LearningPolicyStatus.SUSPENDED
                    PolicyReviewLifecycleAction.RESUME -> LearningPolicyStatus.ACTIVE
                }
                val reason = when (command.action) {
                    PolicyReviewLifecycleAction.ARCHIVE -> PolicyLifecycleReason.USER_ARCHIVED
                    PolicyReviewLifecycleAction.RESTORE_ARCHIVED_REVISION ->
                        PolicyLifecycleReason.USER_RESTORED_REVISION
                    PolicyReviewLifecycleAction.SUSPEND -> PolicyLifecycleReason.USER_SUSPENDED
                    PolicyReviewLifecycleAction.RESUME ->
                        PolicyLifecycleReason.USER_APPROVED_CONTEXTUAL_ADVICE
                }
                val mutation = RoomPolicyLifecycleMutationStore(opened).mutate(
                    PolicyMutationRequest.Transition(
                        fence = PolicyMutationFence(
                            policyId = command.fence.policyId,
                            scope = command.fence.scope,
                            expectedRevision = command.fence.stateVersion,
                            expectedContentRevision = command.fence.contentRevision,
                            expectedArtifactHash = command.fence.artifactSha256,
                        ),
                        target = target,
                        reason = reason,
                        frozenNowMs = command.frozenNowMs,
                        actor = PolicyMutationActor.USER,
                    ),
                )
                result = when (mutation) {
                    is PolicyMutationResult.Applied -> PolicyReviewRuntimeMutationResult.Applied(
                        revision = mutation.revision,
                        status = mutation.status,
                    )
                    is PolicyMutationResult.Duplicate -> PolicyReviewRuntimeMutationResult.Duplicate(
                        revision = mutation.revision,
                    )
                    is PolicyMutationResult.Conflict -> PolicyReviewRuntimeMutationResult.Conflict
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return PolicyReviewRuntimeMutationResult.Unavailable(
                PolicyReviewUnavailableReason.STORAGE_FAILURE,
            )
        }
        return when (access) {
            LearningRuntimeAccess.Ready -> checkNotNull(result)
            else -> PolicyReviewRuntimeMutationResult.Unavailable(
                access.toPolicyReviewUnavailableReason(),
            )
        }
    }

    override suspend fun exportRedactedReviewReport(
        consumingAssistantId: Uuid,
        policyId: String,
    ): PolicyReviewExportResult = when (
        val read = readForReview(consumingAssistantId, policyId)
    ) {
        is PolicyReviewReadResult.Ready -> PolicyReviewExportResult.Ready(
            read.value.toRedactedPolicyReviewReport(),
        )
        PolicyReviewReadResult.NotFound -> PolicyReviewExportResult.NotFound
        is PolicyReviewReadResult.Unavailable -> PolicyReviewExportResult.Unavailable(read.reason)
    }

    override suspend fun listWorkflowCandidates(
        consumingAssistantId: Uuid,
        limit: Int,
    ): WorkflowReviewReadResult<List<me.rerere.rikkahub.learning.workflow.review.WorkflowReviewListItem>> {
        if (limit !in 1..MAX_WORKFLOW_REVIEW_PAGE_SIZE) {
            return WorkflowReviewReadResult.Unavailable(
                WorkflowReviewUnavailableReason.ACTION_NOT_ALLOWED,
            )
        }
        var result: List<me.rerere.rikkahub.learning.workflow.review.WorkflowReviewListItem>? = null
        val access = try {
            withDatabase {
                result = checkNotNull(database).learnedWorkflowCandidateDao().listAssistantPage(
                    assistantId = consumingAssistantId.toString(),
                    beforeUpdatedAtMs = Long.MAX_VALUE,
                    beforeId = "\uffff",
                    limit = limit,
                ).mapNotNull { entity ->
                    entity.toDomainOrNull()?.toWorkflowReviewListItemOrNull()
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return WorkflowReviewReadResult.Unavailable(
                WorkflowReviewUnavailableReason.STORAGE_FAILURE,
            )
        }
        return when (access) {
            LearningRuntimeAccess.Ready -> WorkflowReviewReadResult.Ready(checkNotNull(result))
            else -> WorkflowReviewReadResult.Unavailable(access.toWorkflowReviewUnavailableReason())
        }
    }

    override suspend fun readWorkflowCandidate(
        consumingAssistantId: Uuid,
        candidateId: String,
    ): WorkflowReviewReadResult<me.rerere.rikkahub.learning.workflow.review.WorkflowReviewDetail> {
        if (candidateId.isBlank() || candidateId.length > 128) {
            return WorkflowReviewReadResult.NotFound
        }
        var found = false
        var result: me.rerere.rikkahub.learning.workflow.review.WorkflowReviewDetail? = null
        val access = try {
            withDatabase {
                val dao = checkNotNull(database).learnedWorkflowCandidateDao()
                val candidate = dao.find(candidateId)?.toDomainOrNull() ?: return@withDatabase
                if (candidate.assistantId != consumingAssistantId.toString()) return@withDatabase
                found = true
                result = candidate.toWorkflowReviewDetailOrNull(
                    dao.listRevisionPage(
                        candidateId = candidateId,
                        beforeStateVersion = Long.MAX_VALUE,
                        limit = MAX_WORKFLOW_REVIEW_REVISIONS,
                    ),
                )
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return WorkflowReviewReadResult.Unavailable(
                WorkflowReviewUnavailableReason.STORAGE_FAILURE,
            )
        }
        return when (access) {
            LearningRuntimeAccess.Ready -> when {
                !found -> WorkflowReviewReadResult.NotFound
                result == null -> WorkflowReviewReadResult.Unavailable(
                    WorkflowReviewUnavailableReason.VALIDATION_UNAVAILABLE,
                )
                else -> WorkflowReviewReadResult.Ready(checkNotNull(result))
            }
            else -> WorkflowReviewReadResult.Unavailable(access.toWorkflowReviewUnavailableReason())
        }
    }

    override suspend fun insertCompiledExact(
        candidate: LearnedWorkflowCandidate,
    ): WorkflowCandidateInsertResult {
        var result: WorkflowCandidateInsertResult? = null
        val access = try {
            withDatabase {
                val dao = checkNotNull(database).learnedWorkflowCandidateDao()
                val existing = dao.find(candidate.id)?.toDomainOrNull()
                if (existing != null) {
                    result = if (existing.sameCompiledSubmission(candidate)) {
                        WorkflowCandidateInsertResult.Ready(existing, inserted = false)
                    } else {
                        WorkflowCandidateInsertResult.Conflict
                    }
                    return@withDatabase
                }
                try {
                    dao.insertCompiled(candidate.toEntity())
                    result = WorkflowCandidateInsertResult.Ready(candidate, inserted = true)
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (_: Exception) {
                    val raced = dao.find(candidate.id)?.toDomainOrNull()
                    result = if (raced?.sameCompiledSubmission(candidate) == true) {
                        WorkflowCandidateInsertResult.Ready(raced, inserted = false)
                    } else {
                        WorkflowCandidateInsertResult.Conflict
                    }
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return WorkflowCandidateInsertResult.Unavailable
        }
        return when (access) {
            LearningRuntimeAccess.Ready -> result ?: WorkflowCandidateInsertResult.Unavailable
            else -> WorkflowCandidateInsertResult.Unavailable
        }
    }

    override suspend fun readExact(candidateId: String): WorkflowCandidateReadResult {
        var found: LearnedWorkflowCandidate? = null
        val access = try {
            withDatabase {
                found = checkNotNull(database).learnedWorkflowCandidateDao().find(candidateId)
                    ?.toDomainOrNull()
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return WorkflowCandidateReadResult.Unavailable
        }
        return when (access) {
            LearningRuntimeAccess.Ready -> found?.let(WorkflowCandidateReadResult::Ready)
                ?: WorkflowCandidateReadResult.Missing
            else -> WorkflowCandidateReadResult.Unavailable
        }
    }

    override suspend fun transitionExact(
        expected: LearnedWorkflowCandidate,
        next: LearnedWorkflowCandidate,
        transition: WorkflowCandidateTransition,
    ): WorkflowCandidateTransitionResult {
        val reason = when (transition) {
            WorkflowCandidateTransition.VALIDATION_STARTED ->
                me.rerere.rikkahub.learning.storage.entity
                    .LearnedWorkflowCandidateRevisionReason.VALIDATION_STARTED
            WorkflowCandidateTransition.VALIDATION_PASSED ->
                me.rerere.rikkahub.learning.storage.entity
                    .LearnedWorkflowCandidateRevisionReason.VALIDATION_PASSED
            WorkflowCandidateTransition.VALIDATION_FAILED ->
                me.rerere.rikkahub.learning.storage.entity
                    .LearnedWorkflowCandidateRevisionReason.VALIDATION_FAILED
        }
        var applied = false
        val access = try {
            withDatabase {
                applied = checkNotNull(database).learnedWorkflowCandidateDao().transitionFenced(
                    expected = expected.toEntity(),
                    next = next.toEntity(),
                    reason = reason,
                    actor = me.rerere.rikkahub.learning.storage.entity
                        .LearnedWorkflowCandidateRevisionActor.VALIDATOR,
                )
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return WorkflowCandidateTransitionResult.Unavailable
        }
        return when {
            access != LearningRuntimeAccess.Ready -> WorkflowCandidateTransitionResult.Unavailable
            !applied -> WorkflowCandidateTransitionResult.Conflict
            else -> WorkflowCandidateTransitionResult.Applied(next)
        }
    }

    override suspend fun runMaintenance(
        request: LearningRuntimeMaintenanceRequest,
    ): LearningRuntimeMaintenanceResult {
        val configuredOutboxReader = outboxReader
            ?: return LearningRuntimeMaintenanceResult.Unavailable(
                LearningRuntimeErrorCode.RUNTIME_NOT_CONFIGURED,
            )
        val configuredScanner = reconciliationScanner
            ?: return LearningRuntimeMaintenanceResult.Unavailable(
                LearningRuntimeErrorCode.RUNTIME_NOT_CONFIGURED,
            )
        var drainResult: LearningDrainResult? = null
        val access = withDatabase { session ->
            val openedDatabase = checkNotNull(database) {
                "Learning database unavailable inside runtime operation"
            }
            // Rollout-off stops positive capture/provider work, never authority-loss repair. The
            // type-bounded mandatory lane is still event-gated by its production resolvers, so an
            // initial feedback cannot become a positive write while capture is disabled.
            val effectiveRequest = request.withMandatoryAuthorityInvalidationLane()
            val frozenNowMs = clock().coerceAtLeast(0L)
            val p1Maintenance = initializedP1CatchUp.catchUp(openedDatabase, frozenNowMs)
            val cycleResult = runLearningRuntimeMaintenanceCycle(
                database = openedDatabase,
                session = session,
                request = effectiveRequest,
                outboxReader = configuredOutboxReader,
                reconciliationScanner = configuredScanner,
                frozenNowMs = frozenNowMs,
                wallClockMs = { clock().coerceAtLeast(0L) },
                monotonicMs = monotonicMs,
                processSessionId = processSessionId,
                jobHandlerRegistry = checkNotNull(initializedJobHandlerRegistry) {
                    "Learning job registry unavailable inside runtime operation"
                },
                learnedWorkflowErasePort = learnedWorkflowErasePort,
                durableLearnedWorkflowPrivacyPort = durableLearnedWorkflowPrivacyPort,
            )
            val beforeGrantRebind = cycleResult.withP1Maintenance(p1Maintenance)
            val grantRebind = runPolicyGrantRebindCatchUp(
                database = openedDatabase,
                session = session,
                catchUp = initializedPolicyGrantRebindCatchUp,
                outboxReader = configuredOutboxReader,
                monotonicDeadlineMs = request.monotonicDeadlineMs,
                monotonicMs = monotonicMs,
            )
            val afterGrantRebind = beforeGrantRebind.withPolicyGrantRebind(grantRebind)
            val observedUtilityMaintenance = if (
                request.processJobs &&
                learningFeatureFlags?.policyInjectionEnabledFailClosed() == true
            ) {
                val ledger = RoomObservedUtilityLedger(
                    openedDatabase,
                    openedDatabase.observedUtilityDao(),
                )
                ObservedUtilityMaintenanceCoordinator(
                    candidates = ledger,
                    runtime = ProductionObservedUtilityRuntime(
                        source = ledger,
                        store = ledger,
                    ),
                ).runPage(
                    after = ObservedUtilityMaintenanceCursor.START,
                    frozenNowMs = frozenNowMs,
                    limit = minOf(
                        request.maxJobs.coerceAtLeast(1),
                        MAX_OBSERVED_UTILITY_MAINTENANCE_DESIGNS,
                    ),
                )
            } else {
                null
            }
            drainResult = afterGrantRebind.withObservedUtilityMaintenance(
                observedUtilityMaintenance,
            )
            diagnosticsStore?.let { store ->
                recordMaintenanceHealthBestEffort(
                    database = openedDatabase,
                    outboxReader = configuredOutboxReader,
                    store = store,
                    recordedAtMs = frozenNowMs,
                )
            }
            // Old-timeline quarantine is recoverable until the derived DB proves complete
            // bootstrap for the exact restored stream. Cleanup failure is fail-closed and must
            // never affect Chat or the maintenance result.
            runCatching {
                val checkpoint = openedDatabase.checkpointDao().listAll().singleOrNull()
                val bootstrapHead = checkpoint?.bootstrapHeadSeq
                if (checkpoint != null && checkpoint.bootstrapState == "COMPLETE" &&
                    bootstrapHead != null && checkpoint.lastContiguousSeq >= bootstrapHead
                ) {
                    me.rerere.rikkahub.learning.storage.restore.ColdRestoreRebuildFinalizer
                        .completeIfProven(
                            context = applicationContext,
                            streamId = checkpoint.streamId,
                            bootstrapHeadSeq = bootstrapHead,
                            lastContiguousSeq = checkpoint.lastContiguousSeq,
                        )
                }
            }
        }
        return when (access) {
            LearningRuntimeAccess.Ready -> LearningRuntimeMaintenanceResult.Completed(
                checkNotNull(drainResult) { "Maintenance completed without a drain result" },
            )

            LearningRuntimeAccess.Disabled -> LearningRuntimeMaintenanceResult.Disabled
            is LearningRuntimeAccess.Unavailable -> LearningRuntimeMaintenanceResult.Unavailable(
                access.errorCode,
            )
        }
    }

    /**
     * P5 retention has its own low-priority worker and remains runnable after rollout is switched
     * off. It opens no new derived database for a never-enabled installation.
     */
    override suspend fun sweepOnce(
        request: LearningRetentionMaintenanceRequest,
    ): LearningRetentionRuntimeResult {
        val databaseFileExists = applicationContext
            .getDatabasePath(LearningDatabase.FILE_NAME)
            .isFile
        val rolloutEnabled = try {
            learningFeatureFlags?.current()?.let { it.isValid && it.effective.handoff } == true
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            false
        }
        if (!databaseFileExists && !rolloutEnabled) {
            return LearningRetentionRuntimeResult.NoDerivedDatabase
        }
        val frozenNowMs = try {
            clock().coerceAtLeast(0L)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return LearningRetentionRuntimeResult.Unavailable
        }
        var receipt: LearningRetentionMaintenanceReceipt? = null
        val access = try {
            withDatabase {
                val opened = checkNotNull(database)
                // Freeze stream + replay generation + contiguous position while this facade owns
                // the same reset/restore lane that protects the cross-database prune call below.
                val frozenCheckpoint = freezeDerivedOutboxConsumerCheckpointOrNull(
                    opened.checkpointDao().listAll(),
                )
                val outboxResult = if (
                    frozenCheckpoint != null && primaryOutboxRetention != null
                ) {
                    prunePrimaryOutboxFromFrozenCheckpoint(
                        port = primaryOutboxRetention,
                        checkpoint = frozenCheckpoint,
                        frozenNowMs = frozenNowMs,
                        batchSize = request.batchSize,
                    )
                } else {
                    null
                }
                val result = LearningRetentionStore(
                    database = opened,
                    policy = LearningRetentionPolicyV1(
                        clock = { frozenNowMs },
                        preferences = request.preferences,
                    ),
                    batchLimit = request.batchSize,
                ).sweepOnce()
                receipt = result.toMaintenanceReceipt(request.batchSize, outboxResult)
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return LearningRetentionRuntimeResult.Unavailable
        }
        return when (access) {
            LearningRuntimeAccess.Ready -> LearningRetentionRuntimeResult.Completed(
                checkNotNull(receipt) { "Retention completed without a receipt" },
            )
            LearningRuntimeAccess.Disabled -> LearningRetentionRuntimeResult.NoDerivedDatabase
            is LearningRuntimeAccess.Unavailable -> LearningRetentionRuntimeResult.Unavailable
        }
    }

    /** P1 shadow retrieval returns only a content-free trace and never touches provider bytes. */
    override suspend fun retrieveShadow(
        request: PolicyShadowRuntimeRequest,
    ): PolicyShadowRuntimeResult {
        val shadowGate = policyShadowFeatureGate ?: return PolicyShadowRuntimeResult.Disabled
        if (!shadowGate.enabled() || request.admissionGateIdentity != shadowGate.gateIdentity) {
            return PolicyShadowRuntimeResult.Disabled
        }
        val configuredOpaqueIds = policyOpaqueIds ?: return PolicyShadowRuntimeResult.Unavailable
        var trace: me.rerere.rikkahub.learning.retrieval.PolicyRetrievalTrace? = null
        val access = withDatabase {
            val opened = checkNotNull(database) {
                "Learning database unavailable inside shadow retrieval"
            }
            val retrieval = RoomPolicyShadowRetriever(
                database = opened,
                retriever = PolicyRetriever(configuredOpaqueIds, monotonicNanos = {
                    monotonicMs() * 1_000_000L
                }),
            ).retrieve(request.retrieval)
            trace = when (
                val committed = RoomPolicyShadowObservationStore(
                    database = opened,
                    admissionEnabled = shadowGate::enabled,
                ).record(
                    request = request,
                    result = retrieval,
                    frozenNowMs = clock().coerceAtLeast(0L),
                )
            ) {
                is PolicyShadowObservationCommitResult.Committed -> committed.trace
                is PolicyShadowObservationCommitResult.Duplicate -> committed.trace
                is PolicyShadowObservationCommitResult.Rejected -> null
            }
        }
        return when (access) {
            LearningRuntimeAccess.Ready -> {
                val completed = trace ?: return PolicyShadowRuntimeResult.Unavailable
                diagnosticsStore?.record(
                    LearningDiagnosticSample(
                        recordedAtMs = clock().coerceAtLeast(0L),
                        code = LearningDiagnosticCode.POLICY_RETRIEVAL_SHADOW,
                        state = if (completed.selectedCount == 0) {
                            LearningDiagnosticState.IDLE
                        } else {
                            LearningDiagnosticState.DONE
                        },
                        primaryValue = completed.selectedCount.toLong(),
                        secondaryValue = completed.latencyMicros,
                    ),
                )
                PolicyShadowRuntimeResult.Completed(completed)
            }
            LearningRuntimeAccess.Disabled -> PolicyShadowRuntimeResult.Disabled
            is LearningRuntimeAccess.Unavailable -> PolicyShadowRuntimeResult.Unavailable
        }
    }

    /**
     * Provider-affecting P2 read. This path is independent from shadow retrieval and cannot
     * promote CANDIDATE/SHADOW/PROBATION rows. It joins AppDatabase authority to the rebuildable
     * LearningDatabase only through the frozen stream/scope/Policy content identity.
     */
    override suspend fun retrieve(input: LearnedPolicyQuery): LearnedPolicyRetrievalResult {
        fun empty(truncated: Boolean = false) = LearnedPolicyRetrievalResult(
            packet = LearnedPolicyCandidatePacket(
                scope = input.scope,
                taskSignature = input.taskSignature,
                candidates = emptyList(),
                retrievalRevision = ACTIVE_POLICY_RETRIEVAL_REVISION,
                truncated = truncated,
            ),
            grantReceipts = emptyList(),
        )

        val flagsSource = learningFeatureFlags ?: return empty()
        val grantsSource = policyGrantAuthority ?: return empty()
        val configuredOutboxReader = outboxReader ?: return empty()
        if (!flagsSource.policyInjectionEnabledFailClosed()) return empty()

        val descriptor = try {
            configuredOutboxReader.inspect()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            return empty()
        }
        val streamId = descriptor.streamId.toString()
        val grants = try {
            grantsSource.listExactGranted(
                scope = input.scope,
                consumingAssistantId = input.consumingAssistantId,
                sourceStreamId = streamId,
                limit = MAX_ACTIVE_POLICY_GRANT_SCAN,
            )
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            return empty()
        }
        if (grants.isEmpty()) return empty()

        var exactMatches = emptyList<GrantedActivePolicyMatch>()
        var authorityInvalidationBarrierClear = false
        val access = try {
            withDatabase {
                val opened = checkNotNull(database) {
                    "Learning database unavailable inside active Policy retrieval"
                }
                val checkpoint = opened.checkpointDao().listAll().singleOrNull()
                    ?: return@withDatabase
                if (checkpoint.streamId != streamId || checkpoint.bootstrapState != "COMPLETE" ||
                    checkpoint.lastContiguousSeq < descriptor.headSequence ||
                    opened.hasNonDoneAuthorityInvalidationBarrier(
                        streamId,
                        checkpoint.replayGeneration,
                    )
                ) return@withDatabase
                exactMatches = grants.mapNotNull { grant ->
                    opened.policyDao().findExactGrantedActivePolicy(
                        streamId = streamId,
                        scopeKind = input.scope.kind.name,
                        scopeId = input.scope.storageId,
                        taskSignature = input.taskSignature.value,
                        policyId = grant.policyId,
                        contentRevision = grant.contentRevision,
                        artifactSha256 = grant.artifactSha256,
                    )?.let { policy -> GrantedActivePolicyMatch(grant, policy) }
                }
                authorityInvalidationBarrierClear = true
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            return empty()
        }
        if (access != LearningRuntimeAccess.Ready || !authorityInvalidationBarrierClear) {
            return empty()
        }

        val preparedActiveQuery = me.rerere.rikkahub.learning.retrieval.PolicyFtsManager
            .prepareQuery(input.query)
        if (preparedActiveQuery.terms.isEmpty()) return empty()
        val projections = exactMatches
            .mapNotNull { match ->
                projectExactGrantedActivePolicyOrNull(input, match.grant, match.policy)
                    ?.let { projection ->
                        val lexical = me.rerere.rikkahub.learning.retrieval.PolicyFtsManager
                            .lexicalScore(preparedActiveQuery, projection.renderedFragment)
                        if (lexical <= 0.0) null else Triple(match.grant, projection, lexical)
                    }
            }
            .sortedWith(
                compareByDescending<Triple<PolicyGrantAuthoritySnapshot, ActivePolicyProjection, Double>> {
                    it.third
                }.thenByDescending { it.second.confidence }
                    .thenByDescending { it.second.distinctEpisodeSupport }
                    .thenByDescending { it.second.updatedAtMs }
                    .thenBy { it.second.policyId },
            )

        val selected = mutableListOf<ActivePolicyProjection>()
        var estimatedTokens = 0
        var truncated = false
        for ((grant, projection, _) in projections) {
            if (selected.size >= input.maxCandidates ||
                estimatedTokens + projection.estimatedTokens > input.maxEstimatedTokens
            ) {
                truncated = true
                continue
            }
            val stillGranted = try {
                grantsSource.revalidateExact(grant)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Throwable) {
                false
            }
            if (!stillGranted) continue
            selected += projection
            estimatedTokens += projection.estimatedTokens
        }

        // A restore can atomically replace the main database between the first inspection and
        // grant revalidation. A different authoritative stream makes every old-stream grant inert.
        val sameStream = try {
            configuredOutboxReader.inspect().let { current ->
                current.streamId.toString() == streamId &&
                    current.headSequence == descriptor.headSequence
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            false
        }
        if (!sameStream || !flagsSource.policyInjectionEnabledFailClosed()) return empty()

        val selectedById = selected.associateBy(ActivePolicyProjection::policyId)
        val selectedGrants = projections.asSequence()
            .filter { (_, projection, _) -> projection.policyId in selectedById }
            .associate { (grant, projection, _) -> projection.policyId to grant }
        return LearnedPolicyRetrievalResult(
            packet = LearnedPolicyCandidatePacket(
                scope = input.scope,
                taskSignature = input.taskSignature,
                candidates = selected.mapIndexed { index, projection ->
                    projection.toContextItem(input.scope, rank = index + 1)
                },
                retrievalRevision = ACTIVE_POLICY_RETRIEVAL_REVISION,
                truncated = truncated,
            ),
            grantReceipts = selected.map { projection ->
                checkNotNull(selectedGrants[projection.policyId])
                    .toDispatchReceipt(input.taskSignature)
            },
        )
    }

    override suspend fun revalidateForDispatch(
        receipts: List<LearnedPolicyGrantReceipt>,
        consumingAssistantId: kotlin.uuid.Uuid,
    ): Boolean {
        val source = policyGrantAuthority ?: return false
        val configuredOutboxReader = outboxReader ?: return false
        if (receipts.isEmpty() || receipts.size > MAX_ACTIVE_POLICY_GRANT_SCAN) return false
        if (learningFeatureFlags?.policyInjectionEnabledFailClosed() != true) return false
        val streamId = receipts.first().sourceStreamId
        if (receipts.any { receipt ->
                receipt.sourceStreamId != streamId ||
                    receipt.consumingAssistantId != consumingAssistantId
            }
        ) return false

        val descriptor = try {
            configuredOutboxReader.inspect()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            return false
        }
        if (descriptor.streamId.toString() != streamId) return false

        var learningEligibilityExact = false
        val access = try {
            withDatabase {
                val opened = checkNotNull(database)
                val checkpoint = opened.checkpointDao().listAll().singleOrNull()
                    ?: return@withDatabase
                if (checkpoint.streamId != streamId || checkpoint.bootstrapState != "COMPLETE" ||
                    checkpoint.lastContiguousSeq < descriptor.headSequence ||
                    opened.hasNonDoneAuthorityInvalidationBarrier(
                        streamId,
                        checkpoint.replayGeneration,
                    )
                ) return@withDatabase
                learningEligibilityExact = receipts.all { receipt ->
                    opened.policyDao().findExactGrantedActivePolicy(
                        streamId = streamId,
                        scopeKind = receipt.scope.kind.name,
                        scopeId = receipt.scope.storageId,
                        taskSignature = receipt.taskSignature.value,
                        policyId = receipt.policyId,
                        contentRevision = receipt.policyRevision,
                        artifactSha256 = receipt.artifactSha256,
                    ) != null
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            return false
        }
        if (access != LearningRuntimeAccess.Ready || !learningEligibilityExact) return false

        // AppDatabase is the grant/stream authority. Re-read it only after the derived exact
        // eligibility check so a concurrent revoke or restore is the last durable decision before
        // provider dispatch. The ProviderTurnRunner invokes this fence again for watchdog retry.
        val grantsExact = receipts.all { receipt ->
            try {
                source.revalidateExact(receipt.authority)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Throwable) {
                false
            }
        }
        if (!grantsExact || !learningFeatureFlags.policyInjectionEnabledFailClosed()) {
            return false
        }
        return try {
            configuredOutboxReader.inspect().let { current ->
                current.streamId.toString() == streamId &&
                    current.headSequence == descriptor.headSequence
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            false
        }
    }

    override suspend fun observeFinalDispatchSurface(
        receipts: List<LearnedPolicyGrantReceipt>,
        consumingAssistantId: kotlin.uuid.Uuid,
        availableToolSchemaFingerprints: Set<String>,
        frozenNowMs: Long,
    ): PolicyDispatchSurfaceObservationResult {
        val grants = policyGrantAuthority
            ?: return PolicyDispatchSurfaceObservationResult.Unavailable
        val configuredOutboxReader = outboxReader
            ?: return PolicyDispatchSurfaceObservationResult.Unavailable
        if (receipts.isEmpty() || receipts.size > MAX_ACTIVE_POLICY_GRANT_SCAN ||
            receipts.map { it.policyId }.distinct().size != receipts.size ||
            availableToolSchemaFingerprints.size > 256 ||
            availableToolSchemaFingerprints.any { !it.matches(POLICY_DISPATCH_SHA256) } ||
            frozenNowMs < 0L || learningFeatureFlags?.policyInjectionEnabledFailClosed() != true
        ) return PolicyDispatchSurfaceObservationResult.Unavailable
        val streamId = receipts.first().sourceStreamId
        if (receipts.any { receipt ->
                receipt.sourceStreamId != streamId ||
                    receipt.consumingAssistantId != consumingAssistantId
            }
        ) return PolicyDispatchSurfaceObservationResult.Unavailable
        val descriptor = try {
            configuredOutboxReader.inspect()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            return PolicyDispatchSurfaceObservationResult.Unavailable
        }
        if (descriptor.streamId.toString() != streamId) {
            return PolicyDispatchSurfaceObservationResult.Unavailable
        }
        val initiallyGranted = receipts.all { receipt ->
            try {
                grants.revalidateExact(receipt.authority)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Throwable) {
                false
            }
        }
        if (!initiallyGranted) return PolicyDispatchSurfaceObservationResult.Unavailable

        var observed: PolicyDispatchSurfaceObservationResult.Ready? = null
        val access = try {
            withDatabase {
                val opened = checkNotNull(database)
                val checkpoint = opened.checkpointDao().listAll().singleOrNull()
                    ?: return@withDatabase
                if (checkpoint.streamId != streamId || checkpoint.bootstrapState != "COMPLETE" ||
                    checkpoint.lastContiguousSeq < descriptor.headSequence ||
                    opened.hasNonDoneAuthorityInvalidationBarrier(
                        streamId,
                        checkpoint.replayGeneration,
                    )
                ) return@withDatabase
                val policyDao = opened.policyDao()
                val schemaObserver = PolicyExactDispatchSchemaObserver(
                    PolicyDriftGovernor(RoomPolicyLifecycleMutationStore(opened)),
                )
                val eligible = linkedSetOf<String>()
                val stale = linkedSetOf<String>()
                val capabilityUnknown = linkedSetOf<String>()
                receipts.forEach { receipt ->
                    val policy = policyDao.findExactGrantedActivePolicy(
                        streamId = streamId,
                        scopeKind = receipt.scope.kind.name,
                        scopeId = receipt.scope.storageId,
                        taskSignature = receipt.taskSignature.value,
                        policyId = receipt.policyId,
                        contentRevision = receipt.policyRevision,
                        artifactSha256 = receipt.artifactSha256,
                    ) ?: return@forEach
                    val snapshot = policy.toActiveApplicabilitySnapshotOrNull()
                        ?: return@forEach
                    val observation = schemaObserver.observe(
                        policy = snapshot,
                        availableToolSchemaFingerprints = availableToolSchemaFingerprints,
                        frozenNowMs = frozenNowMs,
                        revalidateExact = { expected ->
                            policyDao.findPolicy(expected.fence.policyId)
                                ?.matchesExactApplicabilitySnapshot(expected) == true
                        },
                    )
                    when (observation.kind) {
                        PolicyDriftObservationKind.NO_DRIFT -> eligible += receipt.policyId
                        PolicyDriftObservationKind.CAPABILITY_BASELINE_UNKNOWN -> {
                            capabilityUnknown += receipt.policyId
                        }
                        PolicyDriftObservationKind.TOOL_SCHEMA_DOWNGRADE ->
                            stale += receipt.policyId
                        PolicyDriftObservationKind.CAPABILITY_DOWNGRADE,
                        PolicyDriftObservationKind.CURRENT_SURFACE_UNKNOWN,
                        PolicyDriftObservationKind.COHORT_BOUNDARY,
                        PolicyDriftObservationKind.CONFLICT,
                        PolicyDriftObservationKind.UNAVAILABLE,
                        -> Unit
                    }
                }
                observed = PolicyDispatchSurfaceObservationResult.Ready(
                    eligiblePolicyIds = eligible,
                    staleSchemaPolicyIds = stale,
                    capabilityUnknownPolicyIds = capabilityUnknown,
                )
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            return PolicyDispatchSurfaceObservationResult.Unavailable
        }
        val ready = observed
            ?.takeIf { access == LearningRuntimeAccess.Ready }
            ?: return PolicyDispatchSurfaceObservationResult.Unavailable

        // AppDatabase owns grants and stream identity. A revoke/restore that races the derived
        // observation removes only the affected Policy; a stream replacement closes the bundle.
        val currentStream = try {
            configuredOutboxReader.inspect()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            return PolicyDispatchSurfaceObservationResult.Unavailable
        }
        if (currentStream.streamId.toString() != streamId ||
            currentStream.headSequence != descriptor.headSequence ||
            !learningFeatureFlags.policyInjectionEnabledFailClosed()
        ) return PolicyDispatchSurfaceObservationResult.Unavailable
        val finallyGranted = receipts.filter { receipt ->
            receipt.policyId in ready.eligiblePolicyIds && try {
                grants.revalidateExact(receipt.authority)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Throwable) {
                false
            }
        }.mapTo(linkedSetOf()) { it.policyId }
        return ready.copy(
            eligiblePolicyIds = finallyGranted,
            capabilityUnknownPolicyIds = ready.capabilityUnknownPolicyIds.intersect(finallyGranted),
        )
    }

    /**
     * AppDatabase-first grant saga projection. This callback opens only the derived database and
     * never retains a Room handle. A non-ready runtime or any exact-tuple/CAS mismatch remains a
     * replayable Pending result rather than false completion.
     */
    override suspend fun project(
        snapshot: PolicyGrantAuthoritySnapshot,
    ): PolicyGrantLifecycleProjectionResult {
        var result: PolicyGrantLifecycleProjectionResult? = null
        val access = try {
            withDatabase {
                val opened = checkNotNull(database)
                result = projectPolicyGrantInOpenRuntime(opened, snapshot)
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            return PolicyGrantLifecycleProjectionResult.Pending(
                PolicyGrantLifecyclePendingReason.STORAGE_FAILURE,
            )
        }
        return result ?: when (access) {
            LearningRuntimeAccess.Ready -> PolicyGrantLifecycleProjectionResult.Pending(
                PolicyGrantLifecyclePendingReason.STORAGE_FAILURE,
            )
            LearningRuntimeAccess.Disabled,
            is LearningRuntimeAccess.Unavailable,
            -> PolicyGrantLifecycleProjectionResult.Pending(
                PolicyGrantLifecyclePendingReason.RUNTIME_UNAVAILABLE,
            )
        }
    }

    override suspend fun resolve(
        request: PolicyExposureRuntimeAnchorRequest,
    ): PolicyExposureRuntimeAnchor? {
        val configuredOutboxReader = outboxReader ?: return null
        if (learningFeatureFlags?.policyInjectionEnabledFailClosed() != true) return null
        val descriptor = try {
            configuredOutboxReader.inspect()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            return null
        }
        val expectedEpisodeId = EpisodeIdFactory.create(
            streamId = descriptor.streamId,
            lineageId = request.command.lineageId,
            branchAnchorMessageId = request.command.branchAnchorMessageId,
        )
        var resolved: PolicyExposureRuntimeAnchor? = null
        val access = try {
            withDatabase { session ->
                val opened = checkNotNull(database)
                // Stage-E generation cannot wait for WorkManager to materialize the OPEN Episode.
                // Consume the already-committed authority outbox and run only bounded local work
                // under this existing database/mutex owner before the exact anchor read.
                val monotonicNow = monotonicMs().coerceAtLeast(0L)
                val catchUpDeadline = if (
                    monotonicNow > Long.MAX_VALUE - POLICY_EXPOSURE_ANCHOR_CATCH_UP_BUDGET_MS
                ) {
                    Long.MAX_VALUE
                } else {
                    monotonicNow + POLICY_EXPOSURE_ANCHOR_CATCH_UP_BUDGET_MS
                }
                runLearningRuntimeMaintenanceCycle(
                    database = opened,
                    session = session,
                    request = LearningRuntimeMaintenanceRequest(
                        maxJobs = POLICY_EXPOSURE_ANCHOR_CATCH_UP_MAX_JOBS,
                        monotonicDeadlineMs = catchUpDeadline,
                        processJobs = true,
                        mode = me.rerere.rikkahub.learning.jobs.LearningDrainMode.DRAIN_ONLY,
                        eligibleJobTypes = setOf(
                            me.rerere.rikkahub.learning.storage.LearningJobType
                                .ASSEMBLE_EPISODE_SHADOW,
                        ),
                    ),
                    outboxReader = configuredOutboxReader,
                    reconciliationScanner = reconciliationScanner ?: return@withDatabase,
                    frozenNowMs = clock().coerceAtLeast(0L),
                    wallClockMs = { clock().coerceAtLeast(0L) },
                    monotonicMs = monotonicMs,
                    processSessionId = processSessionId,
                    jobHandlerRegistry = initializedJobHandlerRegistry ?: return@withDatabase,
                    learnedWorkflowErasePort = learnedWorkflowErasePort,
                    durableLearnedWorkflowPrivacyPort = durableLearnedWorkflowPrivacyPort,
                )
                if (!session.isCurrent()) return@withDatabase
                val checkpoint = opened.checkpointDao().listAll().singleOrNull()
                    ?: return@withDatabase
                if (checkpoint.streamId != descriptor.streamId.toString() ||
                    checkpoint.bootstrapState != "COMPLETE" ||
                    checkpoint.lastContiguousSeq < descriptor.headSequence
                ) {
                    return@withDatabase
                }
                val episode = opened.episodeDao().findEpisode(expectedEpisodeId.value)
                    ?: return@withDatabase
                if (episode.streamId != descriptor.streamId.toString() ||
                    episode.replayGeneration != checkpoint.replayGeneration ||
                    episode.status != me.rerere.rikkahub.learning.storage
                        .StoredLearningEpisodeStatus.OPEN.name ||
                    episode.scopeKind != request.command.scope.kind.name ||
                    episode.scopeId != request.command.scope.storageId ||
                    episode.taskSignature != request.taskSignature.value ||
                    episode.lineageId != request.command.lineageId.toString() ||
                    episode.branchAnchorMessageId != request.command.branchAnchorMessageId.toString() ||
                    episode.branchAnchorMessageRevision !=
                    request.command.branchAnchorMessageRevision ||
                    episode.generationRunId != request.command.logicalRunId.toString()
                ) {
                    return@withDatabase
                }
                resolved = PolicyExposureRuntimeAnchor(
                    streamId = descriptor.streamId,
                    replayGeneration = checkpoint.replayGeneration,
                    episodeId = expectedEpisodeId,
                    logicalRunId = request.command.logicalRunId,
                )
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            return null
        }
        if (access != LearningRuntimeAccess.Ready || resolved == null) return null
        val streamStillExact = try {
            configuredOutboxReader.inspect().let {
                it.streamId == descriptor.streamId && it.headSequence >= descriptor.headSequence
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            false
        }
        return resolved.takeIf { streamStillExact }
    }

    override suspend fun reserve(
        reservation: PolicyExposureReservation,
        metadata: PolicyExposureMetadata,
        frozenNowEpochMs: Long,
    ): PolicyExposureStoreResult = withExposureStore { store ->
        store.reserve(reservation, metadata, frozenNowEpochMs)
    }

    override suspend fun observeMilestone(
        reservationId: String,
        expectedStateVersion: Long,
        state: PolicyExposureState,
        frozenNowEpochMs: Long,
    ): PolicyExposureStoreResult = withExposureStore { store ->
        store.observeMilestone(reservationId, expectedStateVersion, state, frozenNowEpochMs)
    }

    override suspend fun recordDrops(
        reservationId: String,
        expectedStateVersion: Long,
        reasonByPolicyId: Map<String, String>,
        frozenNowEpochMs: Long,
    ): PolicyExposureStoreResult = withExposureStore { store ->
        store.recordDrops(
            reservationId,
            expectedStateVersion,
            reasonByPolicyId,
            frozenNowEpochMs,
        )
    }

    override suspend fun observeProviderAttempt(
        reservationId: String,
        expectedStateVersion: Long,
        event: ProviderAttemptEvent,
        frozenNowEpochMs: Long,
    ): PolicyExposureStoreResult = withExposureStore { store ->
        store.observeProviderAttempt(reservationId, expectedStateVersion, event, frozenNowEpochMs)
    }

    override suspend fun linkOutcome(
        reservationId: String,
        expectedStateVersion: Long,
        authority: PolicyExposureOutcomeAuthority,
        frozenNowEpochMs: Long,
    ): PolicyExposureStoreResult = withExposureStore { store ->
        store.linkOutcome(reservationId, expectedStateVersion, authority, frozenNowEpochMs)
    }

    override suspend fun load(reservationId: String): PolicyExposureStoreResult =
        withExposureStore { store -> store.load(reservationId) }

    override suspend fun reserveMatched(
        intent: ObservedUtilityMatchedAssignmentIntent,
    ): ObservedUtilityLedgerWriteResult {
        if (learningFeatureFlags?.policyInjectionEnabledFailClosed() != true) {
            return ObservedUtilityLedgerWriteResult.Unavailable
        }
        var result: ObservedUtilityLedgerWriteResult? = null
        val access = try {
            withDatabase {
                val opened = checkNotNull(database)
                val primaryRef = intent.reservation.bundle.policies.singleOrNull {
                    it.policyId == intent.primaryPolicyId
                } ?: return@withDatabase
                val policy = opened.policyDao().findPolicy(primaryRef.policyId)
                    ?: return@withDatabase
                if (policy.scopeKind != intent.metadata.scope.kind.name ||
                    policy.scopeId != intent.metadata.scope.storageId ||
                    policy.id != primaryRef.policyId ||
                    policy.contentRevision != primaryRef.policyRevision ||
                    policy.artifactSha256 != primaryRef.artifactSha256 ||
                    intent.reservation.bundle.policies.any { ref ->
                        ref.scope != intent.metadata.scope
                    } ||
                    policy.status != me.rerere.rikkahub.learning.storage
                        .StoredLearningPolicyStatus.ACTIVE.name ||
                    !policy.sourceValid || !policy.schemaValid || policy.staleReason != null
                ) {
                    result = ObservedUtilityLedgerWriteResult.Conflict(
                        me.rerere.rikkahub.learning.policy.runtime
                            .ObservedUtilityLedgerConflict.POLICY_FENCE_CHANGED,
                    )
                    return@withDatabase
                }
                val singlePolicy = intent.reservation.bundle.policies.size == 1
                val assignment = ObservedUtilityPreTreatmentAssignment(
                    streamId = intent.reservation.key.streamId,
                    replayGeneration = intent.metadata.replayGeneration,
                    episodeId = intent.reservation.key.episodeId,
                    logicalRunId = intent.reservation.key.logicalRunId,
                    attemptOrdinal = intent.reservation.key.attemptOrdinal,
                    fence = PolicyMutationFence(
                        policyId = policy.id,
                        scope = intent.metadata.scope,
                        expectedRevision = policy.stateVersion,
                        expectedContentRevision = policy.contentRevision,
                        expectedArtifactHash = policy.artifactSha256,
                    ),
                    design = ObservedUtilityDesign(
                        targetPolicySetDigest = intent.reservation.bundle.policySetDigest,
                        assignmentMethod = ObservedUtilityAssignmentMethod.MATCHED_NON_EXPOSURE,
                        selectionMethod = ObservedUtilitySelectionMethod.EXACT_MATCHED_COHORT,
                        preRegisteredDesignDigest = null,
                        exposureRecordingReliable = true,
                        exposureContractVersion = 1,
                        eligibilityDeterminedBeforeTreatment = true,
                        // The exact final bundle exists after compilation but before injection;
                        // consequently this matched estimate never advertises causal eligibility.
                        assignmentBeforeCompileOrInjection = false,
                        fixedOutcomeWindow = true,
                        randomizedAssignment = false,
                        factorialIsolation = false,
                        attributionUnit = if (singlePolicy) {
                            ObservedUtilityAttributionUnit.INDIVIDUAL_POLICY
                        } else {
                            ObservedUtilityAttributionUnit.BUNDLE
                        },
                        targetPolicyId = policy.id.takeIf { singlePolicy },
                    ),
                    cohort = ObservedUtilityCohortIdentity(
                        taskSignature = intent.metadata.taskSignature,
                        taskSignatureVersion = 1,
                        modelIdentity = intent.metadata.modelIdentity,
                        modelVersion = intent.metadata.modelIdentity,
                        providerIdentity = intent.metadata.providerIdentity,
                        providerVersion = intent.metadata.providerGeneration.toString(),
                        toolsetFingerprint = intent.metadata.toolsetFingerprint,
                        toolSchemaVersion = intent.metadata.contextCompilerAbi,
                        producerModelIdentity = policy.producerModelIdentity,
                        producerProviderIdentity = policy.producerProviderIdentity,
                        producerConfigurationIdentity = policy.producerConfigurationIdentity,
                        producerConfigurationGeneration = policy.producerConfigGeneration,
                        outcomeDefinitionVersion = "episode-authority-v1",
                        outcomeWindowIdentity = me.rerere.rikkahub.learning.model
                            .LearningCanonicalId.digest(
                                domainVersion = "observed-utility-window-v1",
                                fields = listOf(
                                    intent.sourceWindowStartMs.toString(),
                                    intent.sourceWindowEndMs.toString(),
                                ),
                            ),
                        providerConfigurationGeneration = intent.metadata.providerGeneration,
                    ),
                    arm = intent.arm,
                    matchKeyDigest = intent.matchKeyDigest,
                    propensity = null,
                    expectedExposureId = intent.reservation.key.reservationId.takeIf {
                        intent.arm == me.rerere.rikkahub.learning.policy.ObservedUtilityArm.EXPOSED
                    },
                    sourceWindowStartMs = intent.sourceWindowStartMs,
                    sourceWindowEndMs = intent.sourceWindowEndMs,
                    eligibilityDeterminedAtMs = intent.eligibilityDeterminedAtMs,
                    assignedAtMs = intent.assignedAtMs,
                )
                result = RoomObservedUtilityLedger(
                    opened,
                    opened.observedUtilityDao(),
                ).reserve(assignment)
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            return ObservedUtilityLedgerWriteResult.Unavailable
        }
        return result ?: when (access) {
            LearningRuntimeAccess.Ready -> ObservedUtilityLedgerWriteResult.Unavailable
            LearningRuntimeAccess.Disabled,
            is LearningRuntimeAccess.Unavailable,
            -> ObservedUtilityLedgerWriteResult.Unavailable
        }
    }

    private suspend fun withExposureStore(
        operation: suspend (RoomPolicyExposureStore) -> PolicyExposureStoreResult,
    ): PolicyExposureStoreResult {
        var result: PolicyExposureStoreResult? = null
        val access = withDatabase {
            result = operation(RoomPolicyExposureStore(checkNotNull(database)))
        }
        return result ?: when (access) {
            LearningRuntimeAccess.Ready -> error("Exposure operation returned no result")
            LearningRuntimeAccess.Disabled,
            is LearningRuntimeAccess.Unavailable,
            -> PolicyExposureStoreResult.Unavailable(
                me.rerere.rikkahub.learning.exposure.PolicyExposureStoreUnavailable.DATABASE_UNAVAILABLE,
            )
        }
    }

    override suspend fun find(candidateId: String): LearnedWorkflowCandidate? {
        var result: LearnedWorkflowCandidate? = null
        withDatabase {
            result = checkNotNull(database).learnedWorkflowCandidateDao().find(candidateId)
                ?.toDomainOrNull()
        }
        return result
    }

    /**
     * Cross-database source fence for learned workflows. The primary descriptor is sampled on
     * both sides of one LearningDatabase read transaction; any stream/head movement, checkpoint
     * lag, unfinished invalidation job, candidate drift, or Policy/evidence drift denies use.
     */
    override suspend fun isCurrent(candidate: LearnedWorkflowCandidate): Boolean {
        val reader = outboxReader ?: return false
        val before = try {
            reader.inspect()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            return false
        }
        var exact = false
        val access = try {
            withDatabase {
                val opened = checkNotNull(database)
                opened.withTransaction {
                    val checkpoint = opened.checkpointDao().listAll().singleOrNull()
                        ?: return@withTransaction
                    val streamId = before.streamId.toString()
                    if (checkpoint.streamId != streamId ||
                        checkpoint.bootstrapState != "COMPLETE" ||
                        checkpoint.lastContiguousSeq < before.headSequence
                    ) return@withTransaction
                    val candidateDao = opened.learnedWorkflowCandidateDao()
                    if (candidateDao.countBlockingSourceInvalidationJobs(
                            streamId = streamId,
                            replayGeneration = checkpoint.replayGeneration,
                        ) != 0L
                    ) return@withTransaction
                    val storedCandidate = candidateDao.find(candidate.id)?.toDomainOrNull()
                    if (storedCandidate != candidate) return@withTransaction
                    val policyDao = opened.policyDao()
                    val policy = policyDao.findPolicy(candidate.sourcePolicyId)
                        ?: return@withTransaction
                    if (policy.scopeKind != candidate.policyScope.kind.name ||
                        policy.scopeId != candidate.policyScope.storageId ||
                        policy.contentRevision != candidate.sourcePolicyRevision ||
                        policy.artifactSha256 != candidate.sourcePolicyArtifactSha256 ||
                        policy.status != LearningPolicyStatus.ACTIVE.name ||
                        !policy.sourceValid || !policy.schemaValid || policy.staleReason != null
                    ) return@withTransaction
                    exact = policyDao.findExactGrantedActivePolicy(
                        streamId = streamId,
                        scopeKind = policy.scopeKind,
                        scopeId = policy.scopeId,
                        taskSignature = policy.taskSignature,
                        policyId = policy.id,
                        contentRevision = candidate.sourcePolicyRevision,
                        artifactSha256 = candidate.sourcePolicyArtifactSha256,
                    )?.stateVersion == policy.stateVersion
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            return false
        }
        if (access != LearningRuntimeAccess.Ready || !exact) return false
        return try {
            reader.inspect() == before
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            false
        }
    }

    override suspend fun transitionExact(
        expected: LearnedWorkflowCandidate,
        nextState: LearnedWorkflowCandidateState,
        nowMs: Long,
    ): Boolean {
        var applied = false
        withDatabase {
            applied = RoomWorkflowPromotionCandidateStore(
                checkNotNull(database).learnedWorkflowCandidateDao(),
            ).transitionExact(expected, nextState, nowMs)
        }
        return applied
    }

    /**
     * Runs one structured Learning operation. The callback and result are deliberately Unit-only:
     * neither a session nor a Room/DAO handle can be returned through this API.
     */
    suspend fun withDatabase(
        operation: suspend (LearningRuntimeSession) -> Unit,
    ): LearningRuntimeAccess = mutex.withLock {
        if (restoreLatched.get()) {
            publishLatchedRestoreState()
            return@withLock LearningRuntimeAccess.Unavailable(
                latchedRestoreErrorCode(),
            )
        }
        val mainProcess = try {
            isMainProcess()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            false
        }
        if (!mainProcess) {
            closeLocked()
            mutableState.value = LearningRuntimeState.DISABLED
            return@withLock LearningRuntimeAccess.Unavailable(LearningRuntimeErrorCode.WRONG_PROCESS)
        }
        val enabled = try {
            isEnabled()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            closeLocked()
            mutableState.value = LearningRuntimeState.DEGRADED
            return@withLock LearningRuntimeAccess.Unavailable(
                LearningRuntimeErrorCode.FLAG_SOURCE_FAILED,
            )
        }
        if (!enabled) {
            closeLocked()
            mutableState.value = LearningRuntimeState.DISABLED
            return@withLock LearningRuntimeAccess.Disabled
        }
        val nowMs = try {
            clock()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            closeLocked()
            mutableState.value = LearningRuntimeState.DEGRADED
            return@withLock LearningRuntimeAccess.Unavailable(
                LearningRuntimeErrorCode.DATABASE_OPERATION_FAILED,
            )
        }.coerceAtLeast(0L)
        if (nowMs < nextOpenAttemptAtMs) {
            mutableState.value = LearningRuntimeState.DEGRADED
            return@withLock LearningRuntimeAccess.Unavailable(
                LearningRuntimeErrorCode.DATABASE_RETRY_BACKOFF,
            )
        }

        val opened = database ?: try {
            val candidate = newDatabaseCandidate()
            try {
                // Room build is lazy. Force schema validation before publishing READY.
                candidate.openHelper.writableDatabase
                database = candidate
                candidate
            } catch (cancelled: CancellationException) {
                closeCandidateBestEffort(candidate)
                throw cancelled
            } catch (failure: Exception) {
                closeCandidateBestEffort(candidate)
                throw failure
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            markFatalLocked(nowMs)
            return@withLock LearningRuntimeAccess.Unavailable(
                if (restoreLatched.get()) {
                    latchedRestoreErrorCode()
                } else {
                    LearningRuntimeErrorCode.DATABASE_OPEN_FAILED
                },
            )
        }

        if (initializedDatabase !== opened) {
            try {
                // Fence work owned by a dead process before any phase-specific initializer can
                // inspect or execute jobs. P0 still has no job handler and therefore never claims
                // work; this is recovery only, not a fabricated successful execution path.
                val recovery = LearningJobCoordinator(
                    database = opened,
                    processSessionId = processSessionId,
                    clock = LearningJobClock { clock().coerceAtLeast(0L) },
                ).recoverOnStartup()
                when (recovery) {
                    is LearningJobStartupRecoveryResult.ClockRollback -> {
                        diagnosticsStore?.record(
                            LearningDiagnosticSample(
                                recordedAtMs = nowMs,
                                code = LearningDiagnosticCode.JOB_RETRY,
                                state = LearningDiagnosticState.CLOCK_ROLLBACK,
                            ),
                        )
                        throw LearningJobClockRollbackException()
                    }

                    is LearningJobStartupRecoveryResult.Recovered -> {
                        val lostLeases = recovery.otherProcessSessions + recovery.expiredLeases
                        if (lostLeases > 0) {
                            diagnosticsStore?.record(
                                LearningDiagnosticSample(
                                    recordedAtMs = nowMs,
                                    code = LearningDiagnosticCode.LEASE_LOST,
                                    state = LearningDiagnosticState.RETRY,
                                    primaryValue = lostLeases.toLong(),
                                ),
                            )
                        }
                        if (recovery.exhaustedAttempts > 0) {
                            diagnosticsStore?.record(
                                LearningDiagnosticSample(
                                    recordedAtMs = nowMs,
                                    code = LearningDiagnosticCode.DEAD_LETTER,
                                    state = LearningDiagnosticState.DEAD_LETTER,
                                    primaryValue = recovery.exhaustedAttempts.toLong(),
                                ),
                            )
                        }
                    }
                }
                initializer.initialize(opened, runtimeGeneration.get(), nowMs)
                val p1Dependencies = p1RuntimeDependencyFactory.create(opened)
                initializedJobHandlerRegistry = jobHandlerRegistry ?: P1LearningRuntimeBindings
                    .createRegistry(opened, p1Dependencies)
                initializedP1CatchUp = p1Dependencies.catchUp
                initializedPolicyGrantRebindCatchUp = policyGrantAuthority?.let {
                    PolicyGrantRebindCatchUp(it)
                }
                initializedDatabase = opened
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: SQLiteException) {
                markFatalLocked(nowMs)
                return@withLock LearningRuntimeAccess.Unavailable(
                    LearningRuntimeErrorCode.DATABASE_OPERATION_FAILED,
                )
            }
            // Lost leases, checkpoint contention and programming errors are not evidence that the
            // derived database is corrupt. Let their typed/domain failure reach the caller instead
            // of closing the database and entering an expensive retry loop.
        }
        if (restoreLatched.get()) {
            publishLatchedRestoreState()
            return@withLock LearningRuntimeAccess.Unavailable(
                latchedRestoreErrorCode(),
            )
        }
        val sessionGeneration = runtimeGeneration.get()
        mutableState.value = LearningRuntimeState.READY
        if (restoreLatched.get()) {
            publishLatchedRestoreState()
            return@withLock LearningRuntimeAccess.Unavailable(
                latchedRestoreErrorCode(),
            )
        }
        val session = LearningRuntimeSession(
            generation = sessionGeneration,
            currentGeneration = runtimeGeneration::get,
            restoreLatched = restoreLatched::get,
        )
        try {
            coroutineScope { operation(session) }
            if (session.isCurrent()) {
                LearningRuntimeAccess.Ready
            } else {
                publishLatchedRestoreState()
                LearningRuntimeAccess.Unavailable(latchedRestoreErrorCode())
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: SQLiteException) {
            val failureNowMs = try {
                clock().coerceAtLeast(nowMs)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                nowMs
            }
            markFatalLocked(failureNowMs)
            LearningRuntimeAccess.Unavailable(
                if (restoreLatched.get()) {
                    latchedRestoreErrorCode()
                } else {
                    LearningRuntimeErrorCode.DATABASE_OPERATION_FAILED
                },
            )
        } finally {
            session.expire()
        }
        // Domain conflicts and programmer errors deliberately propagate. They are not corruption.
    }

    /** Strict persisted consent and cleanup share the reset/restore/maintenance lane. */
    suspend fun finalizeColdRestore(settings: me.rerere.rikkahub.data.datastore.SettingsStore): Boolean = mutex.withLock {
        if (restoreLatched.get() || !isMainProcess()) return@withLock false
        // COMPLETE already contains durable authorization. An unrelated settings read failure
        // or a later opt-in must not strand a partially deleted quarantine on restart.
        if (me.rerere.rikkahub.learning.storage.restore.ColdRestoreStartupCoordinator
                .finalizeDisabledDerivedState(applicationContext, null)) return@withLock true
        settings.withPersistedLearningPreferences { persisted ->
            me.rerere.rikkahub.learning.storage.restore.ColdRestoreStartupCoordinator
                .finalizeDisabledDerivedState(applicationContext, persisted)
        }
    }

    /** Stops new work, waits for the current operation, then permanently fences this process. */
    suspend fun beginRestore(): Long {
        val nextGeneration = establishRestoreFence()
        // Establish the process fence before waiting for the currently structured operation.
        // New entrants now fail closed even while the restore caller is queued on [mutex].
        publishLatchedRestoreState()
        return mutex.withLock {
            closeLocked()
            nextGeneration
        }
    }

    /** A successful or failed main-database restore still requires process restart. */
    suspend fun remainClosedAfterRestore() {
        establishRestoreFence()
        if (latchedRestoreState != LearningRuntimeState.DEGRADED) {
            latchedRestoreState = LearningRuntimeState.RESTORING
        }
        publishLatchedRestoreState()
        mutex.withLock {
            closeLocked()
            publishLatchedRestoreState()
        }
    }

    /** Irreversible restore failure: remain fenced and visibly degraded until process restart. */
    suspend fun remainDegradedAfterRestore() {
        establishRestoreFence()
        latchedRestoreState = LearningRuntimeState.DEGRADED
        publishLatchedRestoreState()
        mutex.withLock {
            closeLocked()
            publishLatchedRestoreState()
        }
    }

    suspend fun close() {
        mutex.withLock {
            closeLocked()
            mutableState.value = if (restoreLatched.get()) {
                latchedRestoreState
            } else {
                runtimeGeneration.incrementAndGet()
                LearningRuntimeState.CLOSED
            }
        }
    }

    /**
     * User-confirmed exact-scope erasure under the same mutex that fences maintenance and restore.
     * This path intentionally works while rollout flags are disabled, so turning Learning off can
     * never make already-derived data impossible to erase. No Room object escapes this call.
     */
    suspend fun eraseDerivedScope(
        scope: LearningScope,
        frozenNowMs: Long,
        ephemeralEraser: LearningEphemeralScopeEraser,
        learnedWorkflowErasePort: ExactScopeLearnedWorkflowErasePort,
        durableLearnedWorkflowPrivacyPort: DurableLearnedWorkflowPrivacyPort,
    ): LearningScopeEraseResult = mutex.withLock {
        require(frozenNowMs >= 0L)
        if (restoreLatched.get()) {
            throw LearningDerivedEraseUnavailableException(
                LearningDerivedEraseFailureCode.RESTORE_IN_PROGRESS,
            )
        }
        val mainProcess = try {
            isMainProcess()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            false
        }
        if (!mainProcess) {
            throw LearningDerivedEraseUnavailableException(
                LearningDerivedEraseFailureCode.WRONG_PROCESS,
            )
        }
        val published = database
        val opened = published ?: try {
            newDatabaseCandidate().also { candidate ->
                try {
                    candidate.openHelper.writableDatabase
                } catch (cancelled: CancellationException) {
                    closeCandidateBestEffort(candidate)
                    throw cancelled
                } catch (failure: Exception) {
                    closeCandidateBestEffort(candidate)
                    throw failure
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            throw LearningDerivedEraseUnavailableException(
                LearningDerivedEraseFailureCode.DATABASE_OPEN_FAILED,
            )
        }
        val temporary = published == null
        try {
            if (!ephemeralEraser.clearForScope(scope)) {
                throw LearningDerivedEraseUnavailableException(
                    LearningDerivedEraseFailureCode.DATABASE_OPERATION_FAILED,
                )
            }
            LearningDerivedDataEraseStore(
                database = opened,
                learnedWorkflowErasePort = learnedWorkflowErasePort,
                durableLearnedWorkflowPrivacyPort = durableLearnedWorkflowPrivacyPort,
            ).eraseScope(scope, frozenNowMs)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (typed: LearningDerivedEraseUnavailableException) {
            throw typed
        } catch (_: Exception) {
            if (!temporary) markFatalLocked(frozenNowMs)
            throw LearningDerivedEraseUnavailableException(
                LearningDerivedEraseFailureCode.DATABASE_OPERATION_FAILED,
            )
        } finally {
            if (temporary) closeCandidateBestEffort(opened)
        }
    }

    private fun markFatalLocked(nowMs: Long) {
        closeLocked()
        if (restoreLatched.get()) {
            publishLatchedRestoreState()
            return
        }
        runtimeGeneration.incrementAndGet()
        nextOpenAttemptAtMs = try {
            Math.addExact(nowMs, retryBackoffMs)
        } catch (_: ArithmeticException) {
            Long.MAX_VALUE
        }
        mutableState.value = LearningRuntimeState.DEGRADED
    }

    private fun closeLocked() {
        val closing = database
        database = null
        initializedDatabase = null
        initializedJobHandlerRegistry = null
        initializedP1CatchUp = NoOpP1DerivedJobCatchUp
        initializedPolicyGrantRebindCatchUp = null
        try {
            closing?.close()
        } catch (_: Exception) {
            // Derived-state shutdown is best effort; the generation/restore fence is authoritative.
        }
    }

    private fun closeCandidateBestEffort(candidate: LearningDatabase) {
        try {
            candidate.close()
        } catch (_: Exception) {
            // The candidate was never published; its original open failure remains authoritative.
        }
    }

    private fun newDatabaseCandidate(): LearningDatabase = Room.databaseBuilder(
        applicationContext,
        LearningDatabase::class.java,
        LearningDatabase.FILE_NAME,
    ).setJournalMode(RoomDatabase.JournalMode.WRITE_AHEAD_LOGGING)
        .also { builder ->
            // The `simple` tokenizer is provided by the bundled SQLite build. Framework-SQLite
            // fixtures intentionally omit the derived FTS projection instead of failing DB open.
            sqliteOpenHelperFactory?.let { factory ->
                builder.openHelperFactory(factory)
                builder.addCallback(
                    object : RoomDatabase.Callback() {
                        override fun onOpen(db: androidx.sqlite.db.SupportSQLiteDatabase) {
                            me.rerere.rikkahub.learning.retrieval.initializePolicyFtsRuntime(
                                applicationContext,
                                db,
                            )
                        }
                    },
                )
            }
        }
        .addMigrations(
            LEARNING_MIGRATION_1_2,
            LEARNING_MIGRATION_2_3,
            LEARNING_MIGRATION_3_4,
            LEARNING_MIGRATION_4_5,
            LEARNING_MIGRATION_5_6,
            LEARNING_MIGRATION_6_7,
            LEARNING_MIGRATION_7_8,
            LEARNING_MIGRATION_8_9,
        )
        .build()

    private fun publishLatchedRestoreState() {
        mutableState.value = latchedRestoreState
    }

    private fun establishRestoreFence(): Long = synchronized(restoreFenceLock) {
        if (restoreLatched.compareAndSet(false, true)) {
            latchedRestoreState = LearningRuntimeState.RESTORING
            runtimeGeneration.incrementAndGet()
        } else {
            runtimeGeneration.get()
        }
    }

    private fun latchedRestoreErrorCode(): LearningRuntimeErrorCode =
        if (latchedRestoreState == LearningRuntimeState.DEGRADED) {
            LearningRuntimeErrorCode.RESTORE_FAILED_RESTART_REQUIRED
        } else {
            LearningRuntimeErrorCode.RESTORE_IN_PROGRESS
        }
}

/**
 * Privacy/correctness authority-loss jobs remain runnable when ordinary job rollout is disabled.
 * A caller-provided allow-list cannot accidentally exclude them.
 */
internal fun LearningRuntimeMaintenanceRequest.withMandatoryAuthorityInvalidationLane():
    LearningRuntimeMaintenanceRequest = when {
    processJobs && eligibleJobTypes == null -> this
    processJobs -> copy(
        eligibleJobTypes = checkNotNull(eligibleJobTypes) +
            MANDATORY_AUTHORITY_INVALIDATION_JOB_TYPES,
    )
    else -> copy(
        processJobs = true,
        eligibleJobTypes = MANDATORY_AUTHORITY_INVALIDATION_JOB_TYPES,
    )
}

private suspend fun LearningDatabase.hasNonDoneAuthorityInvalidationBarrier(
    streamId: String,
    replayGeneration: Long,
): Boolean {
    val count = jobDao().countNonDoneAuthorityInvalidationBarrier(streamId, replayGeneration)
    check(count in 0..1) { "Unbounded authority invalidation barrier result" }
    return count != 0
}

private val MANDATORY_AUTHORITY_INVALIDATION_JOB_TYPES = setOf(
    LearningJobType.INVALIDATE_SOURCE_V1,
    LearningJobType.APPLY_REWARD_AUTHORITY_V1,
)

private data class GrantedActivePolicyMatch(
    val grant: PolicyGrantAuthoritySnapshot,
    val policy: LearningPolicyEntity,
)

private fun PolicyGrantAuthoritySnapshot.toDispatchReceipt(
    taskSignature: me.rerere.rikkahub.learning.task.TaskSignatureV1,
): LearnedPolicyGrantReceipt =
    LearnedPolicyGrantReceipt(authority = this, taskSignature = taskSignature)

/** Data-only intermediate; keeping rank out of this type allows revocation drops to close gaps. */
internal data class ActivePolicyProjection(
    val policyId: String,
    val policyRevision: Long,
    val artifactSha256: String,
    val renderedFragment: String,
    val estimatedTokens: Int,
    val priority: Int,
    val confidence: Double,
    val distinctEpisodeSupport: Long,
    val updatedAtMs: Long,
    val applicableToolSchemaFingerprints: Set<String>,
    val applicableModelIdentity: String,
    val applicableProviderIdentity: String,
    val applicableTemplateIdentity: String,
    val applicableConfigurationIdentity: String,
    val applicableConfigurationGeneration: Long,
    val applicableCapabilityDigest: String?,
    val applicableAuthorityDigest: String?,
) {
    fun toContextItem(scope: LearningScope, rank: Int): LearnedPolicyContextItem =
        LearnedPolicyContextItem(
            policyId = policyId,
            policyRevision = policyRevision,
            scope = scope,
            artifactSha256 = artifactSha256,
            renderedFragment = renderedFragment,
            estimatedTokens = estimatedTokens,
            priority = priority,
            rank = rank,
            policyCompilerRevision = ACTIVE_POLICY_COMPILER_REVISION,
            applicableToolSchemaFingerprints = applicableToolSchemaFingerprints,
            applicableModelIdentity = applicableModelIdentity,
            applicableProviderIdentity = applicableProviderIdentity,
            applicableTemplateIdentity = applicableTemplateIdentity,
            applicableConfigurationIdentity = applicableConfigurationIdentity,
            applicableConfigurationGeneration = applicableConfigurationGeneration,
            applicableCapabilityDigest = applicableCapabilityDigest,
            applicableAuthorityDigest = applicableAuthorityDigest,
            trust = LearnedPolicyContextTrust.UNTRUSTED_CONTEXT_ONLY,
        )
}

/**
 * Pure final mapper used by the runtime and JVM architecture tests. Every field is checked again
 * after Room materialization; unsafe text rejects the complete Policy instead of being truncated.
 */
internal fun projectExactGrantedActivePolicyOrNull(
    input: LearnedPolicyQuery,
    grant: PolicyGrantAuthoritySnapshot,
    policy: LearningPolicyEntity,
): ActivePolicyProjection? {
    if (grant.scope != input.scope || policy.scopeKind != input.scope.kind.name ||
        policy.scopeId != input.scope.storageId
    ) {
        return null
    }
    if (grant.policyId != policy.id || grant.contentRevision != policy.contentRevision ||
        grant.artifactSha256 != policy.artifactSha256
    ) {
        return null
    }
    if (policy.taskSignature != input.taskSignature.value || policy.status != "ACTIVE" ||
        !policy.sourceValid || !policy.schemaValid || policy.staleReason != null
    ) {
        return null
    }
    if (runCatching { policyArtifactSha256(policy) }.getOrNull() != policy.artifactSha256) {
        return null
    }
    val summaries = listOf(
        "trigger_summary" to policy.triggerSummary,
        "procedure_summary" to policy.procedureSummary,
        "verification_summary" to policy.verificationSummary,
        "boundary_summary" to policy.boundarySummary,
        "failure_mode_summary" to policy.failureModeSummary,
    ).map { (label, raw) ->
        val sanitized = TraceSanitizer.sanitize(raw) as? TraceSanitizationResult.Accepted
            ?: return null
        label to sanitized.summary.value
    }
    val rendered = summaries.joinToString(separator = "\n") { (label, value) ->
        "$label: $value"
    }
    if (rendered.isBlank() || rendered.length > MAX_LEARNED_POLICY_CONTEXT_ITEM_CHARS) return null
    val estimatedTokens = ((rendered.toByteArray(Charsets.UTF_8).size + 3) / 4)
        .coerceAtLeast(1)
    val priority = (
        (policy.confidence * 1_000.0).toInt() +
            policy.distinctEpisodeSupport.coerceIn(0L, 100L).toInt() * 10
        ).coerceIn(-10_000, 10_000)
    val applicableTools = PolicyApplicabilityWire.decodeToolSchemasOrNull(
        policy.applicableToolSchemasWire,
    ) ?: return null
    val applicableModel = when (
        val decoded = PolicyApplicabilityWire.decodeIdentity(policy.applicableModelIdentityWire)
    ) {
        PolicyIdentityApplicability.Any -> return null
        is PolicyIdentityApplicability.Exact -> "EXACT_V1:${decoded.identity}"
    }
    val applicableProvider = when (
        val decoded = PolicyApplicabilityWire.decodeIdentity(policy.applicableProviderIdentityWire)
    ) {
        PolicyIdentityApplicability.Any -> return null
        is PolicyIdentityApplicability.Exact -> "EXACT_V1:${decoded.identity}"
    }
    val applicableTemplate = policy.applicableTemplateIdentity ?: return null
    val applicableConfiguration = policy.applicableConfigurationIdentity ?: return null
    val applicableConfigurationGeneration =
        policy.applicableConfigurationGeneration ?: return null
    return ActivePolicyProjection(
        policyId = policy.id,
        policyRevision = policy.contentRevision,
        artifactSha256 = policy.artifactSha256,
        renderedFragment = rendered,
        estimatedTokens = estimatedTokens,
        priority = priority,
        confidence = policy.confidence,
        distinctEpisodeSupport = policy.distinctEpisodeSupport,
        updatedAtMs = policy.updatedAtMs,
        applicableToolSchemaFingerprints = applicableTools,
        applicableModelIdentity = applicableModel,
        applicableProviderIdentity = applicableProvider,
        applicableTemplateIdentity = applicableTemplate,
        applicableConfigurationIdentity = applicableConfiguration,
        applicableConfigurationGeneration = applicableConfigurationGeneration,
        applicableCapabilityDigest = policy.applicableCapabilityDigest,
        applicableAuthorityDigest = policy.applicableAuthorityDigest,
    )
}

private fun LearningFeatureFlagSource.policyInjectionEnabledFailClosed(): Boolean = try {
    current().let { resolved -> resolved.isValid && resolved.effective.policyInjection }
} catch (cancelled: CancellationException) {
    throw cancelled
} catch (_: Throwable) {
    false
}

private const val MAX_ACTIVE_POLICY_GRANT_SCAN = 1_024
private const val ACTIVE_POLICY_RETRIEVAL_REVISION = "active-policy-exact-v1"
private const val ACTIVE_POLICY_COMPILER_REVISION = "active-policy-context-v1"
private const val POLICY_EXPOSURE_ANCHOR_CATCH_UP_BUDGET_MS = 1_500L
private const val POLICY_EXPOSURE_ANCHOR_CATCH_UP_MAX_JOBS = 4

private suspend fun recordMaintenanceHealthBestEffort(
    database: LearningDatabase,
    outboxReader: LearningOutboxReader,
    store: LearningDiagnosticsStore,
    recordedAtMs: Long,
) {
    try {
        val descriptor = outboxReader.inspect()
        val checkpoint = database.checkpointDao().listAll().singleOrNull()
        val lag = checkpoint?.takeIf { it.streamId == descriptor.streamId.toString() }
            ?.let { (descriptor.headSequence - it.lastContiguousSeq).coerceAtLeast(0L) }
            ?: descriptor.headSequence.coerceAtLeast(0L)
        store.record(
            LearningDiagnosticSample(
                recordedAtMs = recordedAtMs,
                code = LearningDiagnosticCode.OUTBOX_BACKLOG,
                state = if (lag == 0L) LearningDiagnosticState.IDLE else LearningDiagnosticState.RETRY,
                primaryValue = lag,
            ),
        )
        store.record(
            LearningDiagnosticSample(
                recordedAtMs = recordedAtMs,
                code = LearningDiagnosticCode.CHECKPOINT_LAG,
                state = if (lag == 0L) LearningDiagnosticState.READY else LearningDiagnosticState.REQUIRED,
                primaryValue = lag,
            ),
        )
        val jobDao = database.jobDao()
        val active = jobDao.countActive()
        val retries = jobDao.countRetry()
        val deadLetters = jobDao.countDeadLetter()
        if (retries > 0L) {
            store.record(
                LearningDiagnosticSample(
                    recordedAtMs = recordedAtMs,
                    code = LearningDiagnosticCode.JOB_RETRY,
                    state = LearningDiagnosticState.RETRY,
                    primaryValue = retries,
                ),
            )
        }
        store.record(
            LearningDiagnosticSample(
                recordedAtMs = recordedAtMs,
                code = LearningDiagnosticCode.JOB_STATE,
                state = if (active == 0L) LearningDiagnosticState.IDLE else LearningDiagnosticState.RUNNING,
                primaryValue = active,
                secondaryValue = retries,
            ),
        )
        if (deadLetters > 0L) {
            store.record(
                LearningDiagnosticSample(
                    recordedAtMs = recordedAtMs,
                    code = LearningDiagnosticCode.DEAD_LETTER,
                    state = LearningDiagnosticState.DEAD_LETTER,
                    primaryValue = deadLetters,
                ),
            )
        }
        val episodeDao = database.episodeDao()
        store.record(
            LearningDiagnosticSample(
                recordedAtMs = recordedAtMs,
                code = LearningDiagnosticCode.EPISODE_STATE,
                state = LearningDiagnosticState.READY,
                primaryValue = episodeDao.countEpisodes(),
            ),
        )
        store.record(
            LearningDiagnosticSample(
                recordedAtMs = recordedAtMs,
                code = LearningDiagnosticCode.REFLECTION_STATE,
                state = LearningDiagnosticState.READY,
                primaryValue = episodeDao.countValidLessons(),
            ),
        )
        store.record(
            LearningDiagnosticSample(
                recordedAtMs = recordedAtMs,
                code = LearningDiagnosticCode.REWARD_STATE,
                state = LearningDiagnosticState.READY,
                primaryValue = episodeDao.countKnownAuthorityRewardWindows(),
            ),
        )
        store.record(
            LearningDiagnosticSample(
                recordedAtMs = recordedAtMs,
                code = LearningDiagnosticCode.POLICY_CANDIDATE_STATE,
                state = LearningDiagnosticState.READY,
                primaryValue = database.policyDao().countEligibleShadowPolicies(),
            ),
        )
        store.record(
            LearningDiagnosticSample(
                recordedAtMs = recordedAtMs,
                code = LearningDiagnosticCode.DATABASE_STATE,
                state = LearningDiagnosticState.READY,
            ),
        )
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (_: Exception) {
        // Health collection is intentionally non-authoritative and never changes drain outcome.
    }
}

private fun LearningDrainResult.withP1Maintenance(
    result: P1DerivedJobCatchUpResult,
): LearningDrainResult = when {
    result !is P1DerivedJobCatchUpResult.Completed -> this
    this == LearningDrainResult.RETRY || this == LearningDrainResult.DISABLED -> this
    result.workMayRemain -> LearningDrainResult.WORK_REMAINS
    result.didWork && this == LearningDrainResult.IDLE -> LearningDrainResult.DID_WORK
    else -> this
}

private fun LearningDrainResult.withObservedUtilityMaintenance(
    result: ObservedUtilityMaintenancePageResult?,
): LearningDrainResult = when {
    result == null || this == LearningDrainResult.RETRY || this == LearningDrainResult.DISABLED ->
        this
    result is ObservedUtilityMaintenancePageResult.Unavailable -> LearningDrainResult.RETRY
    result is ObservedUtilityMaintenancePageResult.Processed && !result.complete ->
        LearningDrainResult.WORK_REMAINS
    result is ObservedUtilityMaintenancePageResult.Processed &&
        result.estimatedCount + result.abstainedCount > 0 &&
        this == LearningDrainResult.IDLE -> LearningDrainResult.DID_WORK
    else -> this
}

private fun LearningRetentionResult.toMaintenanceReceipt(
    batchSize: Int,
    outboxResult: LearningOutboxRetentionResult?,
): LearningRetentionMaintenanceReceipt {
    val counts = listOf(
        censoredOpenEpisodes,
        archivedWorkflowCandidates,
        archivedCuratorCandidates,
        archivedPolicies,
        deletedPolicyRevisions,
        deletedWorkflowRevisions,
        deletedLessons,
        deletedTraceFeatures,
        deletedRewardWindows,
        deletedEpisodes,
        deletedSourceValidityRows,
        deletedSettledPolicyExposures,
        deletedObservedUtilityEvaluationReceipts,
        deletedObservedUtilityAssignments,
        deletedShadowObservations,
        deletedUnreferencedRewardSignals,
        deletedDoneJobs,
        deletedProviderConfigCohorts,
        deletedInboxEvents,
    )
    require(counts.all { it in 0..batchSize }) { "Retention store exceeded its bounded page" }
    val outboxCompleted = outboxResult as? LearningOutboxRetentionResult.Completed
    val outboxDeleted = outboxCompleted?.deletedRows ?: 0
    val outboxWorkMayRemain = outboxCompleted?.workMayRemain == true
    val derivedMutations = counts.fold(0) { total, count -> Math.addExact(total, count) }
    return LearningRetentionMaintenanceReceipt(
        mutationCount = Math.addExact(derivedMutations, outboxDeleted),
        workMayRemain = counts.any { it == batchSize } || outboxWorkMayRemain,
        deletedPrimaryOutboxRows = outboxDeleted,
        primaryOutboxWorkMayRemain = outboxWorkMayRemain,
    )
}

private suspend fun runPolicyGrantRebindCatchUp(
    database: LearningDatabase,
    session: LearningRuntimeSession,
    catchUp: PolicyGrantRebindCatchUp?,
    outboxReader: LearningOutboxReader,
    monotonicDeadlineMs: Long,
    monotonicMs: () -> Long,
): PolicyGrantRebindCatchUpResult? {
    if (catchUp == null) return null
    if (!session.isCurrent() || monotonicMs() >= monotonicDeadlineMs) {
        return PolicyGrantRebindCatchUpResult.Retry
    }
    val descriptor = try {
        outboxReader.inspect()
    } catch (cancelled: CancellationException) {
        throw cancelled
    } catch (_: Throwable) {
        return PolicyGrantRebindCatchUpResult.Retry
    }
    val checkpoint = database.checkpointDao().listAll().singleOrNull()
    val exactStream = exactCompletePolicyGrantRebindStreamOrNull(
        checkpoint = checkpoint,
        authorityStreamId = descriptor.streamId.toString(),
        authorityHeadSequence = descriptor.headSequence,
    ) ?: return null
    return catchUp.catchUp(
        expectedStreamId = exactStream,
        expectedReplayGeneration = checkNotNull(checkpoint).replayGeneration,
        isRuntimeCurrent = {
            session.isCurrent() && monotonicMs() < monotonicDeadlineMs
        },
        projector = PolicyGrantLifecycleProjector { snapshot ->
            projectPolicyGrantInOpenRuntime(database, snapshot)
        },
    )
}

private fun LearningDrainResult.withPolicyGrantRebind(
    result: PolicyGrantRebindCatchUpResult?,
): LearningDrainResult = when {
    result == null -> this
    result == PolicyGrantRebindCatchUpResult.Retry && this != LearningDrainResult.DISABLED ->
        LearningDrainResult.RETRY
    result !is PolicyGrantRebindCatchUpResult.Completed -> this
    this == LearningDrainResult.RETRY || this == LearningDrainResult.DISABLED -> this
    result.workMayRemain -> LearningDrainResult.WORK_REMAINS
    result.didWork && this == LearningDrainResult.IDLE -> LearningDrainResult.DID_WORK
    else -> this
}

private suspend fun projectPolicyGrantInOpenRuntime(
    database: LearningDatabase,
    snapshot: PolicyGrantAuthoritySnapshot,
): PolicyGrantLifecycleProjectionResult {
    val checkpoint = database.checkpointDao().listAll().singleOrNull()
        ?: return PolicyGrantLifecycleProjectionResult.Pending(
            PolicyGrantLifecyclePendingReason.EXACT_POLICY_MISMATCH,
        )
    if (checkpoint.streamId != snapshot.sourceStreamId || checkpoint.bootstrapState != "COMPLETE") {
        return PolicyGrantLifecycleProjectionResult.Pending(
            PolicyGrantLifecyclePendingReason.EXACT_POLICY_MISMATCH,
        )
    }
    var policy = database.policyDao().findPolicy(snapshot.policyId)
        ?: return PolicyGrantLifecycleProjectionResult.Pending(
            PolicyGrantLifecyclePendingReason.POLICY_MISSING,
        )
    if (!policy.matchesExactGrant(snapshot)) {
        return PolicyGrantLifecycleProjectionResult.Pending(
            PolicyGrantLifecyclePendingReason.EXACT_POLICY_MISMATCH,
        )
    }
    if (snapshot.state == PolicyGrantAuthorityState.GRANTED &&
        (!policy.sourceValid || !policy.schemaValid)
    ) {
        return PolicyGrantLifecycleProjectionResult.Pending(
            PolicyGrantLifecyclePendingReason.EXACT_POLICY_MISMATCH,
        )
    }
    if (snapshot.state == PolicyGrantAuthorityState.REVOKED) {
        // Revocation is scoped to snapshot.consumingAssistantId. The shared Policy lifecycle must
        // not be suspended or made stale because another Assistant can hold an independent grant.
        // Provider eligibility is the exact per-consumer grant join, so a revoked grant is inert.
        return PolicyGrantLifecycleProjectionResult.AlreadySatisfied(
            policy.id,
            policy.stateVersion,
        )
    }

    val store = RoomPolicyLifecycleMutationStore(database)
    var didApply = false
    if (
        nextPolicyGrantLifecycleProjectionStep(snapshot.state, policy.lifecycleStatusOrNull()) ==
        PolicyGrantLifecycleProjectionStep.ADMIT_PROBATION
    ) {
        when (
            store.mutate(
                policy.grantTransition(
                    snapshot = snapshot,
                    target = LearningPolicyStatus.PROBATION,
                    actor = PolicyMutationActor.USER,
                ),
            )
        ) {
            is PolicyMutationResult.Applied -> didApply = true
            is PolicyMutationResult.Duplicate -> Unit
            is PolicyMutationResult.Conflict -> return pendingLifecycleConflict()
        }
        // Crash/retry boundary: reload and verify the exact content tuple before activation.
        policy = database.policyDao().findPolicy(snapshot.policyId)
            ?: return pendingLifecycleConflict()
        if (!policy.matchesExactGrant(snapshot)) {
            return PolicyGrantLifecycleProjectionResult.Pending(
                PolicyGrantLifecyclePendingReason.EXACT_POLICY_MISMATCH,
            )
        }
    }
    if (
        nextPolicyGrantLifecycleProjectionStep(snapshot.state, policy.lifecycleStatusOrNull()) ==
        PolicyGrantLifecycleProjectionStep.ACTIVATE
    ) {
        when (
            store.mutate(
                policy.grantTransition(
                    snapshot = snapshot,
                    target = LearningPolicyStatus.ACTIVE,
                    actor = PolicyMutationActor.GRANT_BINDER,
                ),
            )
        ) {
            is PolicyMutationResult.Applied -> didApply = true
            is PolicyMutationResult.Duplicate -> Unit
            is PolicyMutationResult.Conflict -> return pendingLifecycleConflict()
        }
        policy = database.policyDao().findPolicy(snapshot.policyId)
            ?: return pendingLifecycleConflict()
        if (!policy.matchesExactGrant(snapshot)) {
            return PolicyGrantLifecycleProjectionResult.Pending(
                PolicyGrantLifecyclePendingReason.EXACT_POLICY_MISMATCH,
            )
        }
    }
    return if (
        nextPolicyGrantLifecycleProjectionStep(snapshot.state, policy.lifecycleStatusOrNull()) ==
        PolicyGrantLifecycleProjectionStep.ALREADY_SATISFIED
    ) {
        if (didApply) {
            PolicyGrantLifecycleProjectionResult.Applied(policy.id, policy.stateVersion)
        } else {
            PolicyGrantLifecycleProjectionResult.AlreadySatisfied(policy.id, policy.stateVersion)
        }
    } else {
        PolicyGrantLifecycleProjectionResult.Pending(
            PolicyGrantLifecyclePendingReason.POLICY_NOT_TRANSITIONABLE,
        )
    }
}

private suspend fun projectRevokedPolicyGrant(
    database: LearningDatabase,
    snapshot: PolicyGrantAuthoritySnapshot,
    policy: LearningPolicyEntity,
): PolicyGrantLifecycleProjectionResult {
    val transition = when (
        nextPolicyGrantLifecycleProjectionStep(snapshot.state, policy.lifecycleStatusOrNull())
    ) {
        PolicyGrantLifecycleProjectionStep.SUSPEND -> policy.revocationTransition(
            snapshot,
            target = LearningPolicyStatus.SUSPENDED,
            reason = PolicyLifecycleReason.USER_SUSPENDED,
            actor = PolicyMutationActor.USER,
        )
        PolicyGrantLifecycleProjectionStep.STALE_AUTHORITY -> policy.revocationTransition(
            snapshot,
            target = LearningPolicyStatus.STALE_AUTHORITY,
            reason = PolicyLifecycleReason.AUTHORITY_CHANGED,
            actor = PolicyMutationActor.AUTHORITY_RECONCILER,
        )
        PolicyGrantLifecycleProjectionStep.ALREADY_SATISFIED ->
            return PolicyGrantLifecycleProjectionResult.AlreadySatisfied(
                policy.id,
                policy.stateVersion,
            )
        PolicyGrantLifecycleProjectionStep.ADMIT_PROBATION,
        PolicyGrantLifecycleProjectionStep.ACTIVATE,
        PolicyGrantLifecycleProjectionStep.BLOCKED,
        -> return PolicyGrantLifecycleProjectionResult.Pending(
            PolicyGrantLifecyclePendingReason.POLICY_NOT_TRANSITIONABLE,
        )
    }
    return when (val mutation = RoomPolicyLifecycleMutationStore(database).mutate(transition)) {
        is PolicyMutationResult.Applied -> PolicyGrantLifecycleProjectionResult.Applied(
            mutation.policyId,
            mutation.revision,
        )
        is PolicyMutationResult.Duplicate -> PolicyGrantLifecycleProjectionResult.Duplicate(
            mutation.policyId,
            mutation.revision,
        )
        is PolicyMutationResult.Conflict -> pendingLifecycleConflict()
    }
}

private fun LearningPolicyEntity.matchesExactGrant(
    snapshot: PolicyGrantAuthoritySnapshot,
): Boolean = scopeKind == snapshot.scope.kind.name &&
    scopeId == snapshot.scope.storageId &&
    contentRevision == snapshot.contentRevision &&
    artifactSha256 == snapshot.artifactSha256

private fun LearningPolicyEntity.lifecycleStatusOrNull(): LearningPolicyStatus? =
    LearningPolicyStatus.entries.firstOrNull { it.name == status }

private fun LearningPolicyEntity.grantTransition(
    snapshot: PolicyGrantAuthoritySnapshot,
    target: LearningPolicyStatus,
    actor: PolicyMutationActor,
): PolicyMutationRequest.Transition = PolicyMutationRequest.Transition(
    fence = lifecycleFence(snapshot.scope),
    target = target,
    reason = PolicyLifecycleReason.USER_APPROVED_CONTEXTUAL_ADVICE,
    frozenNowMs = snapshot.updatedAtEpochMs.coerceAtLeast(updatedAtMs),
    actor = actor,
    grantBindingProof = snapshot.toBindingProof(),
)

private fun LearningPolicyEntity.revocationTransition(
    snapshot: PolicyGrantAuthoritySnapshot,
    target: LearningPolicyStatus,
    reason: PolicyLifecycleReason,
    actor: PolicyMutationActor,
): PolicyMutationRequest.Transition = PolicyMutationRequest.Transition(
    fence = lifecycleFence(snapshot.scope),
    target = target,
    reason = reason,
    frozenNowMs = snapshot.updatedAtEpochMs.coerceAtLeast(updatedAtMs),
    actor = actor,
)

private fun pendingLifecycleConflict(): PolicyGrantLifecycleProjectionResult.Pending =
    PolicyGrantLifecycleProjectionResult.Pending(
        PolicyGrantLifecyclePendingReason.LIFECYCLE_CONFLICT,
    )

private fun LearningPolicyEntity.lifecycleFence(scope: LearningScope): PolicyMutationFence =
    PolicyMutationFence(
        policyId = id,
        scope = scope,
        expectedRevision = stateVersion,
        expectedContentRevision = contentRevision,
        expectedArtifactHash = artifactSha256,
    )

private fun PolicyGrantAuthoritySnapshot.toBindingProof(): PolicyGrantBindingProof =
    PolicyGrantBindingProof(
        grantId = grantId,
        sourceStreamId = sourceStreamId,
        scope = scope,
        consumingAssistantId = consumingAssistantId,
        policyId = policyId,
        contentRevision = contentRevision,
        artifactSha256 = artifactSha256,
        grantStateVersion = stateVersion,
    )

private suspend fun LearningDatabase.currentReadyReviewStreamId(): String? =
    checkpointDao().listAll().singleOrNull()?.takeIf { checkpoint ->
        checkpoint.bootstrapState == "COMPLETE" &&
            checkpoint.bootstrapHeadSeq != null &&
            checkpoint.lastContiguousSeq >= checkpoint.bootstrapHeadSeq
    }?.streamId

private suspend fun LearningDatabase.readExactReviewedPolicyWorkflowSourceInTransaction(
    request: ReviewedPolicyWorkflowProposalRequest,
    expectedStreamId: String,
): ReviewedPolicyWorkflowSourceResult {
    fun rejected(reason: ReviewedPolicyWorkflowProposalRejection) =
        ReviewedPolicyWorkflowSourceResult.Rejected(reason)

    val currentStreamId = currentReadyReviewStreamId()
    if (currentStreamId == null || currentStreamId != expectedStreamId) {
        return rejected(ReviewedPolicyWorkflowProposalRejection.POLICY_FENCE_CONFLICT)
    }
    val fence = request.fence
    val policy = policyDao().findPolicy(fence.policyId)
        ?: return rejected(ReviewedPolicyWorkflowProposalRejection.POLICY_FENCE_CONFLICT)
    val storedScope = LearningScope.parseOrNull(policy.scopeKind, policy.scopeId)
    if (policy.id != fence.policyId || policy.stateVersion != fence.stateVersion ||
        policy.contentRevision != fence.contentRevision ||
        policy.artifactSha256 != fence.artifactSha256 || storedScope != fence.scope
    ) {
        return rejected(ReviewedPolicyWorkflowProposalRejection.POLICY_FENCE_CONFLICT)
    }
    if (policy.status != LearningPolicyStatus.ACTIVE.name ||
        !policy.sourceValid || !policy.schemaValid || policy.staleReason != null
    ) {
        return rejected(ReviewedPolicyWorkflowProposalRejection.POLICY_NOT_ACTIVE)
    }
    val exactPolicy = policyDao().findExactGrantedActivePolicy(
        streamId = currentStreamId,
        scopeKind = fence.scope.kind.name,
        scopeId = fence.scope.storageId,
        taskSignature = policy.taskSignature,
        policyId = fence.policyId,
        contentRevision = fence.contentRevision,
        artifactSha256 = fence.artifactSha256,
    ) ?: return rejected(ReviewedPolicyWorkflowProposalRejection.POLICY_EVIDENCE_INVALID)
    if (exactPolicy.stateVersion != fence.stateVersion) {
        return rejected(ReviewedPolicyWorkflowProposalRejection.POLICY_FENCE_CONFLICT)
    }
    val validityRows = policyDao().listEvidenceValidity(
        exactPolicy.id,
        MAX_PROPOSAL_EVIDENCE + 1,
    )
    if (validityRows.isEmpty() || validityRows.size > MAX_PROPOSAL_EVIDENCE) {
        return rejected(ReviewedPolicyWorkflowProposalRejection.POLICY_EVIDENCE_INVALID)
    }
    val evidenceRecords = mutableListOf<ReviewedPolicyWorkflowEvidenceRecord>()
    for (validity in validityRows) {
        val edge = policyDao().findEvidence(validity.policyId, validity.episodeId)
            ?: return rejected(
                ReviewedPolicyWorkflowProposalRejection.POLICY_EVIDENCE_INVALID,
            )
        if (edge.policyId != exactPolicy.id || edge.episodeId != validity.episodeId ||
            edge.polarity != validity.polarity
        ) {
            return rejected(
                ReviewedPolicyWorkflowProposalRejection.POLICY_EVIDENCE_INVALID,
            )
        }
        evidenceRecords += ReviewedPolicyWorkflowEvidenceRecord(
            evidenceId = edge.episodeId,
            polarity = edge.polarity,
            sourceRevision = edge.sourceRevision,
            sourceIntegritySha256 = edge.sourceIntegritySha256,
            sourceValid = validity.sourceValid,
        )
    }
    return projectExactReviewedPolicyWorkflowSource(
        request = request,
        currentStreamId = currentStreamId,
        policy = exactPolicy,
        evidence = evidenceRecords,
    )
}

private suspend fun LearningDatabase.toPolicyReviewListItem(
    policy: LearningPolicyEntity,
    sourceStreamId: String?,
): PolicyReviewListItem {
    val scope = checkNotNull(LearningScope.parseOrNull(policy.scopeKind, policy.scopeId))
    val shadow = policyShadowObservationDao().aggregateForPolicyReview(policy.id)
    val exposure = policyExposureDao().aggregateForPolicyReview(policy.id)
    val dropReasons = policyExposureDao().listDropReasonsForPolicy(policy.id, 16)
    return PolicyReviewListItem(
        fence = PolicyReviewFence(
            policyId = policy.id,
            scope = scope,
            stateVersion = policy.stateVersion,
            contentRevision = policy.contentRevision,
            artifactSha256 = policy.artifactSha256,
            sourceStreamId = sourceStreamId,
        ),
        status = LearningPolicyStatus.valueOf(policy.status),
        triggerSummary = policy.triggerSummary,
        distinctEpisodeSupport = policy.distinctEpisodeSupport,
        positiveEpisodeCount = policy.positiveEpisodeCount,
        negativeEpisodeCount = policy.negativeEpisodeCount,
        confidence = policy.confidence,
        observedUtilityDelta = policy.observedUtilityDelta,
        utilityUncertainty = policy.utilityUncertainty,
        staleReason = policy.staleReason,
        exposure = PolicyReviewExposureSummary(
            shadowRecallCount = shadow.recallCount,
            shadowExactTaskRecallCount = shadow.exactTaskRecallCount,
            shadowEstimatedTokenCost = shadow.estimatedTokenCost,
            shadowLastObservedAtMs = shadow.lastObservedAtMs,
            actualRetrievedCount = exposure.actualRetrievedCount,
            injectedHitCount = exposure.injectedHitCount,
            hostDispatchedHitCount = exposure.hostDispatchedHitCount,
            droppedItemCount = exposure.droppedItemCount,
            dropReasons = dropReasons,
            estimatedTokenCost = exposure.estimatedTokenCost,
        ),
        updatedAtMs = policy.updatedAtMs,
    )
}

private suspend fun LearningDatabase.restoreHistoricPolicyRevision(
    command: PolicyReviewLifecycleCommand,
): PolicyReviewRuntimeMutationResult {
    val current = policyDao().findPolicy(command.fence.policyId)
        ?: return PolicyReviewRuntimeMutationResult.Conflict
    if (current.stateVersion != command.fence.stateVersion ||
        current.contentRevision != command.fence.contentRevision ||
        current.artifactSha256 != command.fence.artifactSha256 ||
        current.status != LearningPolicyStatus.ARCHIVED.name ||
        current.scopeKind != command.fence.scope.kind.name ||
        current.scopeId != command.fence.scope.storageId ||
        !current.sourceValid || !current.schemaValid
    ) return PolicyReviewRuntimeMutationResult.Conflict
    val historic = policyDao().findRevision(current.id, command.selectedRevision)
        ?: return PolicyReviewRuntimeMutationResult.Unavailable(
            PolicyReviewUnavailableReason.HISTORIC_CONTENT_RESTORE_NOT_SUPPORTED,
        )
    val parsed = historic.afterSnapshot.parseHistoricPolicyContentOrNull()
        ?: return PolicyReviewRuntimeMutationResult.Unavailable(
            PolicyReviewUnavailableReason.HISTORIC_CONTENT_RESTORE_NOT_SUPPORTED,
        )
    val restoredType = runCatching { PolicyCandidateType.valueOf(parsed.policyType) }
        .getOrNull() ?: return PolicyReviewRuntimeMutationResult.Unavailable(
        PolicyReviewUnavailableReason.HISTORIC_CONTENT_RESTORE_NOT_SUPPORTED,
    )
    val restoredArtifact = policyArtifactSha256(
        type = restoredType,
        trigger = parsed.trigger,
        procedure = parsed.procedure,
        verification = parsed.verification,
        boundary = parsed.boundary,
        failureMode = parsed.failure,
        applicableToolSchemas = parsed.toolSchemas,
        applicableModelIdentity = parsed.applicableModelIdentity,
        applicableProviderIdentity = parsed.applicableProviderIdentity,
        applicableTemplateIdentity = parsed.applicableTemplateIdentity,
        applicableConfigurationIdentity = parsed.applicableConfigurationIdentity,
        applicableConfigurationGeneration = parsed.applicableConfigurationGeneration,
        applicableCapabilityDigest = parsed.applicableCapabilityDigest,
        applicableAuthorityDigest = parsed.applicableAuthorityDigest,
    )
    if (restoredArtifact != historic.afterArtifactSha256 ||
        parsed.artifactSha256 != historic.afterArtifactSha256
    ) {
        return PolicyReviewRuntimeMutationResult.Unavailable(
            PolicyReviewUnavailableReason.HISTORIC_CONTENT_RESTORE_NOT_SUPPORTED,
        )
    }
    val restoredTools = PolicyApplicabilityWire.encodeToolSchemas(parsed.toolSchemas)
    // Parse-and-reencode every applicability wire before the content CAS. Historic restore is a
    // content mutation, so keeping today's task/model/provider fences would silently produce a
    // hybrid revision that never existed.
    PolicyApplicabilityWire.decodeIdentity(parsed.applicableModelIdentityWire)
    PolicyApplicabilityWire.decodeIdentity(parsed.applicableProviderIdentityWire)
    val changed = policyDao().restoreHistoricPolicyContentIfCurrent(
        policyId = current.id,
        expectedStateVersion = current.stateVersion,
        expectedContentRevision = current.contentRevision,
        expectedArtifactSha256 = current.artifactSha256,
        taskSignature = parsed.taskSignature,
        policyType = parsed.policyType,
        triggerSummary = parsed.trigger,
        procedureSummary = parsed.procedure,
        verificationSummary = parsed.verification,
        boundarySummary = parsed.boundary,
        failureModeSummary = parsed.failure,
        restoredArtifactSha256 = restoredArtifact,
        applicableToolSchemasWire = restoredTools,
        applicableModelIdentityWire = parsed.applicableModelIdentityWire,
        applicableProviderIdentityWire = parsed.applicableProviderIdentityWire,
        applicableTemplateIdentity = parsed.applicableTemplateIdentity,
        applicableConfigurationIdentity = parsed.applicableConfigurationIdentity,
        applicableConfigurationGeneration = parsed.applicableConfigurationGeneration,
        applicableCapabilityDigest = parsed.applicableCapabilityDigest,
        applicableAuthorityDigest = parsed.applicableAuthorityDigest,
        updatedAtMs = command.frozenNowMs,
    )
    if (changed != 1) return PolicyReviewRuntimeMutationResult.Conflict
    val restored = policyDao().findPolicy(current.id)
        ?: return PolicyReviewRuntimeMutationResult.Conflict
    val afterSnapshot = listOf(
        "policy-candidate-snapshot-v3",
        "type=${restored.policyType}",
        "task=${restored.taskSignature}",
        "trigger=${restored.triggerSummary}",
        "procedure=${restored.procedureSummary}",
        "verification=${restored.verificationSummary}",
        "boundary=${restored.boundarySummary}",
        "failure=${restored.failureModeSummary}",
        "applicable_tools=${restored.applicableToolSchemasWire}",
        "applicable_model=${restored.applicableModelIdentityWire}",
        "applicable_provider=${restored.applicableProviderIdentityWire}",
        "applicable_template=${restored.applicableTemplateIdentity}",
        "applicable_configuration=${restored.applicableConfigurationIdentity}",
        "applicable_configuration_generation=${restored.applicableConfigurationGeneration}",
        "applicable_capability=${restored.applicableCapabilityDigest ?: "UNKNOWN"}",
        "applicable_authority=${restored.applicableAuthorityDigest ?: "UNKNOWN"}",
        "artifact=${restored.artifactSha256}",
        "support=${restored.distinctEpisodeSupport}",
        "positive=${restored.positiveEpisodeCount}",
        "negative=${restored.negativeEpisodeCount}",
    ).joinToString("\n")
    policyDao().insertRevision(
        PolicyRevisionEntity(
            policyId = restored.id,
            revision = restored.stateVersion,
            beforeSnapshot = current.toHistoricLifecycleSnapshot(),
            afterSnapshot = afterSnapshot,
            beforeArtifactSha256 = current.artifactSha256,
            afterArtifactSha256 = restored.artifactSha256,
            reasonCode = LearningPolicyRevisionReason.USER_RESTORED_REVISION.name,
            actor = LearningPolicyRevisionActor.USER.name,
            createdAtMs = restored.updatedAtMs,
        ),
    )
    return PolicyReviewRuntimeMutationResult.Applied(
        restored.stateVersion,
        LearningPolicyStatus.SHADOW,
    )
}

private data class HistoricPolicyContent(
    val policyType: String,
    val taskSignature: String,
    val trigger: String,
    val procedure: String,
    val verification: String,
    val boundary: String,
    val failure: String,
    val toolSchemas: Set<String>,
    val applicableModelIdentityWire: String,
    val applicableProviderIdentityWire: String,
    val applicableModelIdentity: String,
    val applicableProviderIdentity: String,
    val applicableTemplateIdentity: String,
    val applicableConfigurationIdentity: String,
    val applicableConfigurationGeneration: Long,
    val applicableCapabilityDigest: String?,
    val applicableAuthorityDigest: String?,
    val artifactSha256: String,
)

private fun String.parseHistoricPolicyContentOrNull(): HistoricPolicyContent? = runCatching {
    val lines = lineSequence().toList()
    require(lines.firstOrNull() == "policy-candidate-snapshot-v3")
    val entries = lines.drop(1).map { line ->
        val split = line.indexOf('=')
        require(split > 0)
        line.substring(0, split) to line.substring(split + 1)
    }
    require(entries.map { it.first }.distinct().size == entries.size)
    val values = entries.toMap()
    require(values.keys == HISTORIC_POLICY_SNAPSHOT_KEYS)
    val tools = checkNotNull(values["applicable_tools"])
        .let(PolicyApplicabilityWire::decodeToolSchemasOrNull)
        ?: error("Historic applicability is unproven")
    val modelWire = checkNotNull(values["applicable_model"])
    val providerWire = checkNotNull(values["applicable_provider"])
    val modelIdentity = (PolicyApplicabilityWire.decodeIdentity(modelWire) as
        PolicyIdentityApplicability.Exact).identity
    val providerIdentity = (PolicyApplicabilityWire.decodeIdentity(providerWire) as
        PolicyIdentityApplicability.Exact).identity
    HistoricPolicyContent(
        policyType = checkNotNull(values["type"]),
        taskSignature = checkNotNull(values["task"]),
        trigger = checkNotNull(values["trigger"]),
        procedure = checkNotNull(values["procedure"]),
        verification = checkNotNull(values["verification"]),
        boundary = checkNotNull(values["boundary"]),
        failure = checkNotNull(values["failure"]),
        toolSchemas = tools,
        applicableModelIdentityWire = modelWire,
        applicableProviderIdentityWire = providerWire,
        applicableModelIdentity = modelIdentity,
        applicableProviderIdentity = providerIdentity,
        applicableTemplateIdentity = checkNotNull(values["applicable_template"]),
        applicableConfigurationIdentity = checkNotNull(values["applicable_configuration"]),
        applicableConfigurationGeneration =
            checkNotNull(values["applicable_configuration_generation"]).toLong(),
        applicableCapabilityDigest = checkNotNull(values["applicable_capability"])
            .takeUnless { it == "UNKNOWN" },
        applicableAuthorityDigest = checkNotNull(values["applicable_authority"])
            .takeUnless { it == "UNKNOWN" },
        artifactSha256 = checkNotNull(values["artifact"]),
    )
}.getOrNull()

private val HISTORIC_POLICY_SNAPSHOT_KEYS = setOf(
    "type",
    "task",
    "trigger",
    "procedure",
    "verification",
    "boundary",
    "failure",
    "applicable_tools",
    "applicable_model",
    "applicable_provider",
    "applicable_template",
    "applicable_configuration",
    "applicable_configuration_generation",
    "applicable_capability",
    "applicable_authority",
    "artifact",
    "support",
    "positive",
    "negative",
)

private fun LearningPolicyEntity.toHistoricLifecycleSnapshot(): String = listOf(
    "policy-lifecycle-snapshot-v2",
    "state_version=$stateVersion",
    "content_revision=$contentRevision",
    "status=$status",
    "source_valid=$sourceValid",
    "schema_valid=$schemaValid",
    "applicable_tools=$applicableToolSchemasWire",
    "applicable_model=$applicableModelIdentityWire",
    "applicable_provider=$applicableProviderIdentityWire",
    "applicable_template=${applicableTemplateIdentity ?: "UNPROVEN"}",
    "applicable_configuration=${applicableConfigurationIdentity ?: "UNPROVEN"}",
    "applicable_configuration_generation=${applicableConfigurationGeneration ?: "UNPROVEN"}",
    "applicable_capability=${applicableCapabilityDigest ?: "UNKNOWN"}",
    "applicable_authority=${applicableAuthorityDigest ?: "UNKNOWN"}",
    "stale_reason=${staleReason ?: "NONE"}",
    "artifact=$artifactSha256",
    "support=$distinctEpisodeSupport",
    "positive=$positiveEpisodeCount",
    "negative=$negativeEpisodeCount",
    "usage_count=$usageCount",
    "last_used_at_ms=${lastUsedAtMs ?: "UNKNOWN"}",
    "confidence=$confidence",
    "observed_utility_delta=${observedUtilityDelta ?: "UNKNOWN"}",
    "utility_uncertainty=${utilityUncertainty ?: "UNKNOWN"}",
).joinToString("\n")

private fun LearningRuntimeAccess.toPolicyReviewUnavailableReason():
    PolicyReviewUnavailableReason = when (this) {
    LearningRuntimeAccess.Ready -> PolicyReviewUnavailableReason.RUNTIME_NOT_READY
    LearningRuntimeAccess.Disabled -> PolicyReviewUnavailableReason.FEATURE_DISABLED
    is LearningRuntimeAccess.Unavailable -> when (errorCode) {
        LearningRuntimeErrorCode.WRONG_PROCESS -> PolicyReviewUnavailableReason.WRONG_PROCESS
        LearningRuntimeErrorCode.RESTORE_IN_PROGRESS,
        LearningRuntimeErrorCode.RESTORE_FAILED_RESTART_REQUIRED,
        -> PolicyReviewUnavailableReason.RESTORE_IN_PROGRESS
        LearningRuntimeErrorCode.DATABASE_OPERATION_FAILED,
        LearningRuntimeErrorCode.DATABASE_OPEN_FAILED,
        -> PolicyReviewUnavailableReason.STORAGE_FAILURE
        LearningRuntimeErrorCode.DATABASE_RETRY_BACKOFF,
        LearningRuntimeErrorCode.FLAG_SOURCE_FAILED,
        LearningRuntimeErrorCode.RUNTIME_NOT_CONFIGURED,
        -> PolicyReviewUnavailableReason.RUNTIME_NOT_READY
    }
}

private fun LearningRuntimeAccess.toWorkflowReviewUnavailableReason():
    WorkflowReviewUnavailableReason = when (this) {
    LearningRuntimeAccess.Ready -> WorkflowReviewUnavailableReason.RUNTIME_NOT_READY
    LearningRuntimeAccess.Disabled -> WorkflowReviewUnavailableReason.FEATURE_DISABLED
    is LearningRuntimeAccess.Unavailable -> when (errorCode) {
        LearningRuntimeErrorCode.WRONG_PROCESS -> WorkflowReviewUnavailableReason.WRONG_PROCESS
        LearningRuntimeErrorCode.RESTORE_IN_PROGRESS,
        LearningRuntimeErrorCode.RESTORE_FAILED_RESTART_REQUIRED,
        -> WorkflowReviewUnavailableReason.RESTORE_IN_PROGRESS
        LearningRuntimeErrorCode.DATABASE_OPERATION_FAILED,
        LearningRuntimeErrorCode.DATABASE_OPEN_FAILED,
        -> WorkflowReviewUnavailableReason.STORAGE_FAILURE
        LearningRuntimeErrorCode.DATABASE_RETRY_BACKOFF,
        LearningRuntimeErrorCode.FLAG_SOURCE_FAILED,
        LearningRuntimeErrorCode.RUNTIME_NOT_CONFIGURED,
        -> WorkflowReviewUnavailableReason.RUNTIME_NOT_READY
    }
}

/** Exact deterministic replay recognition; only lifecycle receipt fields may have advanced. */
private fun LearnedWorkflowCandidate.sameCompiledSubmission(
    compiled: LearnedWorkflowCandidate,
): Boolean = copy(
    stateVersion = compiled.stateVersion,
    state = compiled.state,
    verificationReport = compiled.verificationReport,
    verifiedAtMs = compiled.verifiedAtMs,
    archivedAtMs = compiled.archivedAtMs,
    updatedAtMs = compiled.updatedAtMs,
) == compiled

/** Narrow adapter used by the restore coordinator; it never exposes Room or a DAO. */
class LearningRuntimeFacadeRestorePort(
    private val facade: LearningRuntimeFacade,
) : LearningRuntimeRestorePort {
    override suspend fun beginIrreversibleRestore(): LearningRestoreRuntimeFence =
        LearningRestoreRuntimeFence(facade.beginRestore())

    override suspend fun remainClosedUntilProcessRestart(fence: LearningRestoreRuntimeFence) {
        requireCurrentFence(fence)
        facade.remainClosedAfterRestore()
    }

    override suspend fun remainDegradedUntilProcessRestart(
        fence: LearningRestoreRuntimeFence,
        reason: LearningRestoreFailureReason,
    ) {
        requireCurrentFence(fence)
        // The allowlisted reason belongs to the restore result/diagnostics, never to this latch.
        facade.remainDegradedAfterRestore()
    }

    private fun requireCurrentFence(fence: LearningRestoreRuntimeFence) {
        check(fence.generation == facade.currentGeneration()) {
            "Restore runtime fence is stale"
        }
    }
}

private fun isCurrentMainProcess(context: Context): Boolean {
    val processName = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
        Application.getProcessName()
    } else {
        val activityManager = context.getSystemService(Context.ACTIVITY_SERVICE) as? ActivityManager
        activityManager?.runningAppProcesses
            ?.firstOrNull { it.pid == Process.myPid() }
            ?.processName
    }
    return processName == context.packageName
}
