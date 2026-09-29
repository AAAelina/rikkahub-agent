package me.rerere.rikkahub.correctness

import android.content.BroadcastReceiver
import android.content.Context
import android.content.ContextWrapper
import android.content.Intent
import android.content.IntentFilter
import androidx.core.content.ContextCompat
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.first
import me.rerere.ai.provider.Model
import me.rerere.ai.provider.ProviderSetting
import me.rerere.rikkahub.AppScope
import me.rerere.rikkahub.automation.*
import me.rerere.rikkahub.data.agentrun.*
import me.rerere.rikkahub.data.ai.tools.HeadlessConversations
import me.rerere.rikkahub.data.ai.tools.ToolNameSnapshot
import me.rerere.rikkahub.data.datastore.Settings
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.model.Assistant
import me.rerere.rikkahub.data.repository.ConversationRepository
import me.rerere.rikkahub.service.ChatService
import me.rerere.rikkahub.service.ChatServiceProbePoint
import me.rerere.rikkahub.service.chat.CommandOrigin
import me.rerere.rikkahub.service.chat.DurableCommandQueue
import me.rerere.rikkahub.subagent.*
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.koin.core.context.loadKoinModules
import org.koin.dsl.module
import org.koin.java.KoinJavaComponent.getKoin
import kotlin.uuid.Uuid

/** Real manifest receiver, production ChatService/engine and Room ledger. No provider is called:
 * execution is held or failed at an internal debug boundary, in the disposable test application. */
@RunWith(AndroidJUnit4::class)
class HeadlessProductionFaultAndroidTest {
    private val context: Context get() = ApplicationProvider.getApplicationContext()
    private val koin get() = getKoin()
    private lateinit var database: AppDatabase
    private lateinit var chat: ChatService
    private lateinit var settings: SettingsStore
    private lateinit var originalSettings: Settings
    private lateinit var originalDispatcher: ExternalAutomationDispatcher
    private lateinit var config: ExternalAutomationConfig
    private lateinit var scope: AppScope
    private lateinit var assistant: Assistant
    private lateinit var model: Model
    private val callbacks = Channel<Intent>(Channel.UNLIMITED)
    private val attempts = CopyOnWriteArrayList<String>()
    private var failTerminalBroadcast = false
    private val conversations = CopyOnWriteArrayList<Uuid>()
    private val callbackAction = "me.rerere.rikkahub.GBAO_GATE_${Uuid.random()}"
    private val receiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context, intent: Intent) { callbacks.trySend(intent) }
    }

    @Before fun setUp() = runBlocking {
        settings = koin.get()
        originalSettings = settings.settingsFlow.first { !it.init }
        database = koin.get()
        chat = koin.get()
        originalDispatcher = koin.get()
        config = koin.get()
        scope = AppScope()
        model = Model(modelId = "offline-correctness-fixture")
        assistant = Assistant(name = "Synthetic correctness gate", chatModelId = model.id)
        settings.update(Settings(assistantId = assistant.id, assistants = listOf(assistant), chatModelId = model.id,
            providers = listOf(ProviderSetting.OpenAI(models = listOf(model), baseUrl = "http://127.0.0.1:1/v1"))))
        config.setEnabled(true)
        config.addTrustedPackage("<adb>")
        val callbacksContext = object : ContextWrapper(context) {
            override fun sendBroadcast(intent: Intent) {
                val status = intent.getStringExtra(ExternalAutomationDispatcher.EXTRA_STATUS).orEmpty()
                attempts += status
                if (status != "accepted" && failTerminalBroadcast) error("injected callback transport failure")
                super.sendBroadcast(intent)
            }
        }
        val dispatcher = ExternalAutomationDispatcher(callbacksContext, config, chat, koin.get(), settings, scope, koin.get())
        loadKoinModules(module { single<ExternalAutomationDispatcher> { dispatcher } })
        ContextCompat.registerReceiver(context, receiver, IntentFilter(callbackAction), ContextCompat.RECEIVER_EXPORTED)
        chat.correctnessProbe = { stage, envelope ->
            if (envelope.conversationId !in conversations) conversations += envelope.conversationId
            if (stage == ChatServiceProbePoint.BEFORE_EXECUTION) error("offline generation fixture")
        }
    }

    @After fun tearDown() = runBlocking {
        chat.correctnessProbe = null
        conversations.forEach { chat.stopAndAwaitQuiescence(it, null, 5_000L) }
        scope.coroutineContext[Job]?.cancelAndJoin()
        database.openHelper.writableDatabase.execSQL("DROP TRIGGER IF EXISTS gbao_gate_setup")
        database.openHelper.writableDatabase.execSQL("DROP TRIGGER IF EXISTS gbao_gate_terminal")
        context.unregisterReceiver(receiver)
        config.removeTrustedPackage("<adb>")
        config.setEnabled(false)
        settings.update(originalSettings)
        loadKoinModules(module { single<ExternalAutomationDispatcher> { originalDispatcher } })
        Unit
    }

    @Test fun receiverSetupFailureReportsOneTerminalAndLeavesNoConversation() = runBlocking {
        database.openHelper.writableDatabase.execSQL(
            "CREATE TRIGGER gbao_gate_setup BEFORE INSERT ON ConversationEntity WHEN NEW.assistant_id='${assistant.id}' BEGIN SELECT RAISE(ABORT,'injected setup failure'); END",
        )
        val request = send()
        assertEquals("accepted", callback(request))
        assertEquals("failed", callback(request))
        assertEquals(listOf("accepted", "failed"), attempts.toList())
        assertTrue(koin.get<ConversationRepository>().getConversationIdsOfAssistant(assistant.id).isEmpty())
    }

    @Test fun receiverPartialEnqueueFailureStopsActualChatRuntimeAndClosesRoomLedger() = runBlocking {
        val executionStarted = CompletableDeferred<Unit>()
        val admitted = CompletableDeferred<Uuid>()
        chat.correctnessProbe = { stage, envelope ->
            conversations.addIfAbsent(envelope.conversationId)
            if (stage == ChatServiceProbePoint.BEFORE_EXECUTION) {
                executionStarted.complete(Unit)
                awaitCancellation()
            } else if (stage == ChatServiceProbePoint.AFTER_ENQUEUE) {
                admitted.complete(envelope.id)
                executionStarted.await()
                error("injected failure after durable enqueue before handle return")
            }
        }
        val request = send()
        assertEquals("accepted", callback(request))
        assertEquals("failed", callback(request))
        val id = withTimeout(10_000L) { admitted.await() }
        assertNotNull(koin.get<DurableCommandQueue>().findAuthorityRow(id))
        val row = ledger(request)
        assertEquals("failed", row.status)
        assertNotNull(row.finishedAtMs)
        withTimeout(10_000L) { while (HeadlessConversations.isHeadless(conversations.single())) delay(10) }
        assertTrue(chat.stopAndAwaitQuiescence(conversations.single(), id, 5_000L))
        assertEquals(listOf("accepted", "failed"), attempts.toList())
    }

    @Test fun receiverQueuedAdmissionFailureCancelsTheDurableCommandBeforeHandleReturn() = runBlocking {
        val probe = failWhileQueued()
        val request = send()
        assertEquals("accepted", callback(request))
        assertEquals("failed", callback(request))
        assertEquals("failed", ledger(request).status)
        assertQueuedAdmissionCancelled(probe)
        assertEquals(listOf("accepted", "failed"), attempts.toList())
    }

    @Test fun terminalRoomWriteFailureStillBroadcastsAndBootRecoveryClosesStrandedLedger() = runBlocking {
        database.openHelper.writableDatabase.execSQL(
            "CREATE TRIGGER gbao_gate_terminal BEFORE UPDATE ON agent_runs WHEN OLD.kind='external_automation' BEGIN SELECT RAISE(ABORT,'injected terminal write failure'); END",
        )
        val request = send()
        assertEquals("accepted", callback(request))
        assertEquals("failed", callback(request))
        val row = ledger(request)
        assertEquals("running", row.status)
        database.openHelper.writableDatabase.execSQL("DROP TRIGGER gbao_gate_terminal")
        val repository = koin.get<AgentRunRepository>()
        assertEquals(1, repository.markAllProcessLost(listOf(row.id)))
        assertEquals("process_lost", repository.getById(row.id)?.status)
        assertEquals(listOf("accepted", "failed"), attempts.toList())
    }

    @Test fun terminalBroadcastFailureDoesNotUndoDurableLedgerOrRepeatCallback() = runBlocking {
        failTerminalBroadcast = true
        val request = send()
        assertEquals("accepted", callback(request))
        withTimeout(15_000L) { while (attempts.size < 2) delay(10) }
        assertEquals("failed", ledger(request).status)
        assertEquals(listOf("accepted", "failed"), attempts.toList())
    }

    @Test fun cancelledDispatcherScopeStillTerminatesAcceptedReceiverRequest() = runBlocking {
        scope.cancel()
        val request = send()
        assertEquals("accepted", callback(request))
        assertEquals("cancelled", callback(request))
        assertEquals(listOf("accepted", "cancelled"), attempts.toList())
    }

    @Test fun cancelledLazyEngineClosesTheRealRoomQueuedLedger() = runBlocking {
        val registry = SubAgentRegistry()
        scope.cancel()
        val engine = engine(registry, koin.get())
        val result = withTimeout(10_000L) { engine.dispatch(caller(), SubAgentRequest("synthetic cancelled launch")) }
        val run = (result as SubAgentEngine.DispatchResult.Ok).run
        assertEquals(SubAgentStatus.CANCELLED, run.status)
        assertEquals("cancelled", koin.get<AgentRunRepository>().getByDomainId(AgentRunKind.SubAgent, run.id).single().status)
        assertEquals(0, registry.runs.value.values.count { it.status == SubAgentStatus.PENDING || it.status == SubAgentStatus.RUNNING })
    }

    @Test fun cancellationAfterLedgerInsertBeforeLaunchCannotLeaveQueuedAuthority() = runBlocking {
        val registry = SubAgentRegistry()
        val actual = database.agentRunDao()
        val repository = AgentRunRepository(object : AgentRunDao by actual {
            override suspend fun insert(row: AgentRun) {
                actual.insert(row)
                currentCoroutineContext().cancel(CancellationException("cancel after durable insert"))
            }
        })
        val dispatch = async(SupervisorJob()) { engine(registry, repository).dispatch(caller(), SubAgentRequest("synthetic prelaunch cancel")) }
        assertTrue(runCatching { withTimeout(10_000L) { dispatch.await() } }.exceptionOrNull() is CancellationException)
        val run = registry.runs.value.values.single()
        assertEquals(SubAgentStatus.CANCELLED, run.status)
        assertEquals("cancelled", repository.getByDomainId(AgentRunKind.SubAgent, run.id).single().status)
    }

    @Test fun engineSetupFailureAndActiveCancellationAreDurablyTerminal() = runBlocking {
        val registry = SubAgentRegistry()
        database.openHelper.writableDatabase.execSQL(
            "CREATE TRIGGER gbao_gate_setup BEFORE INSERT ON ConversationEntity WHEN NEW.assistant_id='${assistant.id}' BEGIN SELECT RAISE(ABORT,'injected engine setup failure'); END",
        )
        val failed = engine(registry, koin.get()).dispatch(caller(), SubAgentRequest("synthetic setup failure")) as SubAgentEngine.DispatchResult.Ok
        assertEquals(SubAgentStatus.FAILED, failed.run.status)
        assertEquals("failed", koin.get<AgentRunRepository>().getByDomainId(AgentRunKind.SubAgent, failed.run.id).single().status)
        database.openHelper.writableDatabase.execSQL("DROP TRIGGER gbao_gate_setup")
        val executing = CompletableDeferred<Uuid>()
        chat.correctnessProbe = { stage, envelope ->
            conversations.addIfAbsent(envelope.conversationId)
            if (stage == ChatServiceProbePoint.BEFORE_EXECUTION) { executing.complete(envelope.conversationId); awaitCancellation() }
            else if (stage == ChatServiceProbePoint.AFTER_ENQUEUE) { executing.await(); awaitCancellation() }
        }
        val engine = engine(registry, koin.get())
        val result = engine.dispatch(caller(), SubAgentRequest("synthetic active cancellation", runInBackground = true)) as SubAgentEngine.DispatchResult.Ok
        val conversation = withTimeout(15_000L) { executing.await() }
        assertTrue(HeadlessConversations.isHeadless(conversation))
        registry.requestCancel(result.run.id)
        withTimeout(15_000L) {
            while (koin.get<AgentRunRepository>().getByDomainId(AgentRunKind.SubAgent, result.run.id).single().status != "cancelled") delay(10)
            while (HeadlessConversations.isHeadless(conversation)) delay(10)
        }
        assertTrue(chat.stopAndAwaitQuiescence(conversation, null, 5_000L))
        assertNull(koin.get<SubAgentExecutionProfileRegistry>().get(conversation))
    }

    @Test fun engineQueuedAdmissionFailureCancelsTheDurableCommandBeforeHandleReturn() = runBlocking {
        val probe = failWhileQueued()
        val result = engine(SubAgentRegistry(), koin.get()).dispatch(
            caller(), SubAgentRequest("synthetic queued admission failure"),
        ) as SubAgentEngine.DispatchResult.Ok
        assertEquals(SubAgentStatus.FAILED, result.run.status)
        assertEquals("failed", koin.get<AgentRunRepository>()
            .getByDomainId(AgentRunKind.SubAgent, result.run.id).single().status)
        val conversation = assertQueuedAdmissionCancelled(probe)
        assertNull(koin.get<SubAgentExecutionProfileRegistry>().get(conversation))
    }

    private data class QueuedProbe(
        val admitted: CompletableDeferred<Pair<Uuid, Uuid>> = CompletableDeferred(),
        val executions: AtomicInteger = AtomicInteger(),
    )

    private fun failWhileQueued(): QueuedProbe = QueuedProbe().also { probe ->
        chat.correctnessProbe = { stage, envelope ->
            conversations.addIfAbsent(envelope.conversationId)
            when (stage) {
                ChatServiceProbePoint.BEFORE_ENQUEUE -> {
                    // Create the real runtime, then await its emergency pause before admission.
                    chat.getRuntimeStateFlow(envelope.conversationId)
                    check(chat.stopAndAwaitQuiescence(envelope.conversationId, null, 5_000L))
                }
                ChatServiceProbePoint.AFTER_ENQUEUE -> {
                    check(koin.get<DurableCommandQueue>().findAuthorityRow(envelope.id)?.state == "PENDING")
                    probe.admitted.complete(envelope.conversationId to envelope.id)
                    error("injected queued admission failure before handle return")
                }
                ChatServiceProbePoint.BEFORE_EXECUTION -> {
                    probe.executions.incrementAndGet()
                    error("paused command must never execute")
                }
            }
        }
    }

    private suspend fun assertQueuedAdmissionCancelled(probe: QueuedProbe): Uuid {
        val (conversation, command) = withTimeout(10_000L) { probe.admitted.await() }
        val queue = koin.get<DurableCommandQueue>()
        withTimeout(10_000L) {
            while (queue.countActive(conversation) != 0 || HeadlessConversations.isHeadless(conversation)) delay(10)
        }
        val row = requireNotNull(queue.findAuthorityRow(command))
        assertEquals("CANCELLED", row.state)
        assertNotNull(row.finishedAt)
        assertEquals(0, probe.executions.get())
        return conversation
    }

    private fun engine(registry: SubAgentRegistry, repository: AgentRunRepository) =
        SubAgentEngine(registry, koin.get(), koin.get(), settings, scope, repository)
    private fun caller() = SubAgentCallerContext(assistant.id.toString(), null, model.id, ToolNameSnapshot.EMPTY)
    private fun send(): String {
        val request = Uuid.random().toString()
        context.sendBroadcast(Intent(context, ExternalAutomationReceiver::class.java).apply {
            action = ExternalAutomationDispatcher.ACTION_RUN_TASK
            putExtra(ExternalAutomationDispatcher.EXTRA_TASK, "synthetic correctness gate")
            putExtra(ExternalAutomationDispatcher.EXTRA_REQUEST_ID, request)
            putExtra(ExternalAutomationDispatcher.EXTRA_RETURN_ACTION, callbackAction)
            putExtra(ExternalAutomationDispatcher.EXTRA_RETURN_PACKAGE, context.packageName)
        })
        return request
    }
    private suspend fun callback(request: String): String = withTimeout(20_000L) {
        val intent = callbacks.receive()
        assertEquals(request, intent.getStringExtra(ExternalAutomationDispatcher.EXTRA_REQUEST_ID))
        requireNotNull(intent.getStringExtra(ExternalAutomationDispatcher.EXTRA_STATUS))
    }
    private suspend fun ledger(request: String): AgentRun = withTimeout(10_000L) {
        var rows = koin.get<AgentRunRepository>().getByDomainId(AgentRunKind.ExternalAutomation, request)
        while (rows.isEmpty()) { delay(10); rows = koin.get<AgentRunRepository>().getByDomainId(AgentRunKind.ExternalAutomation, request) }
        rows.single()
    }
}
