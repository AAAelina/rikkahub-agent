package me.rerere.rikkahub.data.execution

import androidx.room.Room
import androidx.room.withTransaction
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import me.rerere.rikkahub.data.scope.AgentScope
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.ai.ToolCallOrigin
import me.rerere.rikkahub.data.ai.tools.ToolExecutionContext
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.uuid.Uuid

@RunWith(AndroidJUnit4::class)
class ExecutionRecordDaoTest {
    private lateinit var db: AppDatabase
    private lateinit var dao: ExecutionRecordDao

    @Before
    fun setUp() {
        db = Room.inMemoryDatabaseBuilder(
            ApplicationProvider.getApplicationContext(),
            AppDatabase::class.java,
        ).build()
        dao = db.executionRecordDao()
    }

    @After
    fun tearDown() {
        db.close()
    }

    @Test
    fun getInFlight_excludesEveryTerminalStatus() = runBlocking {
        ExecutionStatus.entries.forEachIndexed { index, status ->
            dao.insertIgnore(row(status.name, status, index.toLong()))
        }

        assertEquals(
            ExecutionStatus.IN_FLIGHT.map { it.name }.toSet(),
            dao.getInFlight().map { it.status }.toSet(),
        )
    }

    @Test
    fun pendingApproval_resolutionUsesVersionCasAndIsIdempotent() = runBlocking {
        val approvals = db.pendingToolApprovalDao()
        val pending = PendingToolApprovalRecord(
            approvalId = "approval-1",
            executionId = "tool:run:call",
            traceId = "run",
            toolCallId = "call",
            conversationId = "conversation",
            subjectId = "assistant:conversation",
            subjectType = "LOCAL_SECOND_USER",
            origin = "SystemAssistant",
            capabilityKey = "linux.execute",
            resourceCategory = "workspace",
            requestedAtMs = 10,
            stateVersion = 1,
        )
        approvals.insertIgnore(pending)

        assertEquals(0, approvals.resolveCas(
            approvalId = pending.approvalId,
            expectedVersion = 0,
            nextVersion = 1,
            status = ApprovalStatus.APPROVED.name,
            resolvedAtMs = 20,
            resolutionReason = "approval_granted",
            resolutionRequestId = "request-1",
        ))
        assertEquals(1, approvals.resolveCas(
            approvalId = pending.approvalId,
            expectedVersion = 1,
            nextVersion = 2,
            status = ApprovalStatus.APPROVED.name,
            resolvedAtMs = 20,
            resolutionReason = "approval_granted",
            resolutionRequestId = "request-1",
        ))
        assertEquals(0, approvals.resolveCas(
            approvalId = pending.approvalId,
            expectedVersion = 1,
            nextVersion = 2,
            status = ApprovalStatus.APPROVED.name,
            resolvedAtMs = 20,
            resolutionReason = "approval_granted",
            resolutionRequestId = "request-1",
        ))
        assertEquals(ApprovalStatus.APPROVED.name, approvals.getById("approval-1")?.status)
    }

    @Test
    fun pendingApproval_exactLookupCannotCrossExecutionConversationOrToolCall() = runBlocking {
        val approvals = db.pendingToolApprovalDao()
        val record = PendingToolApprovalRecord(
            approvalId = "approval-exact",
            executionId = "tool:run:exact",
            traceId = "run",
            toolCallId = "call-exact",
            conversationId = "conversation-exact",
            subjectId = "subject-exact",
            subjectType = "LOCAL_SECOND_USER",
            origin = "SystemAssistant",
            capabilityKey = "linux.execute",
            resourceCategory = "workspace",
            requestedAtMs = 10,
            stateVersion = 1,
        )
        approvals.insertIgnore(record)

        assertEquals(
            record,
            approvals.getExact(
                record.approvalId,
                record.executionId,
                record.conversationId,
                record.toolCallId,
            ),
        )
        assertEquals(null, approvals.getExact(record.approvalId, "other-execution", record.conversationId, record.toolCallId))
        assertEquals(null, approvals.getExact(record.approvalId, record.executionId, "other-conversation", record.toolCallId))
        assertEquals(null, approvals.getExact(record.approvalId, record.executionId, record.conversationId, "other-call"))
    }

    @Test
    fun managedRegistration_createsARealChildBeforeRuntimeStart() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val transaction = ExecutionStateTransaction(
                database = db,
                recordDao = dao,
                eventDao = db.executionEventDao(),
            )
            val repository = ExecutionRepository(
                dao = dao,
                transaction = transaction,
                retention = ExecutionRetentionManager(
                    recordDao = dao,
                    eventDao = db.executionEventDao(),
                    approvalDao = db.pendingToolApprovalDao(),
                    scope = scope,
                ),
            )
            val registration = ManagedExecutionRegistration(repository)
            val runId = Uuid.random()

            val record = registration.reserve(
                context = ToolExecutionContext(
                    runId = runId,
                    conversationId = Uuid.random(),
                    assistantId = "00000000-0000-0000-0000-000000000013",
                    callOrigin = ToolCallOrigin.SystemAssistant,
                    toolCallId = "call-1",
                ),
                reservation = ManagedExecutionReservation(
                    executionId = "workspace:wp_12345678",
                    runtime = ExecutionRuntime.WORKSPACE,
                    completionPolicy = CompletionPolicy.DETACH_BACKGROUND,
                ),
            )

            assertEquals("workspace:wp_12345678", record.id)
            assertEquals(ExecutionRecordIds.tool(runId.toString(), "call-1"), record.parentExecutionId)
            assertEquals(ExecutionKind.MANAGED_PROCESS.name, record.executionKind)
            assertEquals(ExecutionStatus.starting.name, record.status)
            assertEquals(1, db.executionEventDao().getEvents(record.id, 10).size)
        } finally {
            scope.cancel()
        }
    }

    @Test
    fun reconciler_discardsOldProbeEvidenceAfterVersionConflict() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        try {
            val repository = repository(scope)
            val opened = repository.open(
                ExecutionRecordDraft(
                    id = "workspace:wp_12345678",
                    traceId = "run",
                    conversationId = "conversation",
                    learningScope = AgentScope.Assistant(
                        Uuid.parse("00000000-0000-0000-0000-000000000001"),
                    ),
                    subjectId = "assistant",
                    subjectType = "LOCAL_SECOND_USER",
                    origin = ToolCallOrigin.SystemAssistant.name,
                    capabilityKeys = "linux.background",
                    resourceSummary = "workspace",
                    runtime = ExecutionRuntime.WORKSPACE,
                    initialStatus = ExecutionStatus.running,
                    executionKind = ExecutionKind.MANAGED_PROCESS,
                    completionPolicy = CompletionPolicy.SERVICE_EXPECTED_TO_STAY_ALIVE,
                    verificationState = VerificationState.RECONCILING,
                    runtimeHandleSummary = "workspace:wp_12345678",
                    runtimeInstanceMarker = "generation:old",
                )
            )
            var probes = 0
            val reconciler = ExecutionReconciler(
                repository = repository,
                probe = ExecutionRuntimeProbe {
                    probes++
                    if (probes == 1) {
                        repository.transition(
                            id = opened.id,
                            target = ExecutionStatus.running,
                            verificationState = VerificationState.RECONCILING,
                            mutationId = "concurrent-runtime-update",
                            reasonCode = "runtime_changed_while_probing",
                        )
                        RuntimeProbeResult.Alive("generation:stale")
                    } else {
                        RuntimeProbeResult.Alive("generation:fresh")
                    }
                },
            )

            val update = reconciler.reconcile(opened.id)
            val final = requireNotNull(repository.get(opened.id))

            assertEquals(2, probes)
            assertEquals("generation:fresh", final.runtimeInstanceMarker)
            assertEquals(RuntimeContinuity.RESTARTED, update.continuity)
            assertEquals(
                0,
                db.executionEventDao().getEvents(opened.id, 20).count {
                    it.eventId.contains("generation:stale")
                },
            )
        } finally {
            scope.cancel()
        }
    }

    private fun repository(scope: CoroutineScope): ExecutionRepository {
        val transaction = ExecutionStateTransaction(
            database = db,
            recordDao = dao,
            eventDao = db.executionEventDao(),
        )
        return ExecutionRepository(
            dao = dao,
            transaction = transaction,
            retention = ExecutionRetentionManager(
                recordDao = dao,
                eventDao = db.executionEventDao(),
                approvalDao = db.pendingToolApprovalDao(),
                scope = scope,
            ),
        )
    }


    private fun row(id: String, status: ExecutionStatus, timestamp: Long) = ExecutionRecord(
        id = id,
        traceId = "trace",
        subjectId = "subject",
        subjectType = "LOCAL_SECOND_USER",
        origin = "APP_UI",
        capabilityKeys = "safe.read",
        resourceSummary = "generic",
        runtime = ExecutionRuntime.LOCAL_TOOL.name,
        status = status.name,
        createdAtMs = timestamp,
        updatedAtMs = timestamp,
        finishedAtMs = timestamp.takeIf { status.isTerminal },
    )
}

