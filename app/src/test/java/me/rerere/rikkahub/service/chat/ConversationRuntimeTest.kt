package me.rerere.rikkahub.service.chat

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.withContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.datetime.LocalDateTime
import me.rerere.ai.core.MessageRole
import me.rerere.ai.ui.UIMessageAnnotation
import me.rerere.ai.ui.UIMessagePart
import me.rerere.rikkahub.data.ai.SteeringNote
import me.rerere.rikkahub.data.ai.tools.CancelRequestResult
import me.rerere.rikkahub.data.ai.tools.ToolCancelReason
import me.rerere.rikkahub.data.ai.tools.ToolExecutionHandle
import me.rerere.rikkahub.data.ai.tools.ToolTerminationState
import me.rerere.rikkahub.data.model.Conversation
import me.rerere.rikkahub.service.withSteeringAuditMessage
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.time.Clock
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.milliseconds
import kotlin.uuid.Uuid

class ConversationRuntimeTest {
    @Test
    fun `identity mutation waits behind generation and receives its own outcome`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val conversationId = Uuid.random()
        val started = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val editStarted = CompletableDeferred<Unit>()
        val runtime = ConversationRuntime(
            appScope = scope, conversationId = conversationId,
            executor = RuntimeCommandExecutor { envelope, _ ->
                if (envelope.command is SendMessageCommand) {
                    started.complete(Unit); release.await()
                    RunOutcome.Completed()
                } else {
                    editStarted.complete(Unit)
                    RunOutcome.Conflict("Message target disappeared")
                }
            },
        )
        try {
            val send = CommandEnvelope(conversationId = conversationId, command = messageCommand("send"),
                origin = CommandOrigin.APP_UI, sequence = 1L)
            runtime.enqueueEnvelope(send)
            withTimeout(5000) { started.await() }
            val edit = CommandEnvelope(conversationId = conversationId,
                command = MutateMessageCommand(Uuid.random(), Uuid.random()), origin = CommandOrigin.APP_UI, sequence = 2L)
            runtime.enqueueEnvelope(edit)
            assertFalse(editStarted.isCompleted)
            assertFalse(edit.result.isCompleted)
            release.complete(Unit)
            assertEquals(CommandOutcome.Completed, withTimeout(5000) { send.result.await() })
            assertEquals(CommandOutcome.Conflict("Message target disappeared"), withTimeout(5000) { edit.result.await() })
        } finally { runtime.close(); scope.cancel() }
    }

    @Test
    fun `resume queue codec preserves start immediately policy`() {
        val command = ResumeQueueCommand(startNextImmediately = false)
        val encoded = CommandCodec.encode(command)

        assertEquals(command, CommandCodec.decode(encoded.first, encoded.second))
    }

    @Test
    fun `durable command codec preserves cron origin`() {
        val createdAt = LocalDateTime(2026, 8, 17, 16, 0, 0, 123_000_000)
        val command = SendMessageCommand(
            RawUserContent(
                parts = listOf(UIMessagePart.Text("scheduled")),
                createdAt = createdAt,
            ),
        )
        val encoded = CommandCodec.encodeDurable(command, CommandOrigin.CRON)

        assertEquals(CommandOrigin.CRON, CommandCodec.decodeDurableOrigin(encoded.second))
        assertEquals(command, CommandCodec.decode(encoded.first, encoded.second))
    }

    private fun messageCommand(text: String): SendMessageCommand =
        SendMessageCommand(RawUserContent(listOf(UIMessagePart.Text(text))))

    private fun <C : ChatCommand> CommandEnvelope<C>.withRootLineage(
        assistantId: Uuid = Uuid.random(),
        branchAnchorMessageId: Uuid = Uuid.random(),
    ): CommandEnvelope<C> = copy(
        lineage = CommandLineageContext(
            assistantIdSnapshot = assistantId,
            lineageId = id,
            parentCommandId = null,
            branchAnchorMessageId = branchAnchorMessageId,
        ),
    )

    @Test
    fun `waiting checkpoint keeps terminal deferred open and gates ordinary FIFO`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val dao = FakePendingChatCommandDao()
        val queue = DurableCommandQueue(
            dao = dao,
            commandStateTransaction = CommandStateTransaction(dao),
        )
        val conversationId = Uuid.random()
        val assistantId = Uuid.random()
        val firstId = Uuid.random()
        val secondId = Uuid.random()
        val firstResumeId = Uuid.random()
        val executions = java.util.concurrent.atomic.AtomicInteger(0)
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = conversationId,
            durableQueue = queue,
            executor = RuntimeCommandExecutor { envelope, _ ->
                executions.incrementAndGet()
                if (envelope.id == firstId || envelope.id == firstResumeId) {
                    RunOutcome.WaitingApproval(setOf("tool-1"))
                } else {
                    RunOutcome.Completed()
                }
            },
        )
        fun rootEnvelope(id: Uuid, sequence: Long) = CommandEnvelope(
            id = id,
            conversationId = conversationId,
            command = messageCommand("message-$sequence"),
            origin = CommandOrigin.APP_UI,
            sequence = sequence,
            lineage = CommandLineageContext(
                assistantIdSnapshot = assistantId,
                lineageId = id,
                parentCommandId = null,
                branchAnchorMessageId = Uuid.random(),
            ),
        )
        val first = rootEnvelope(firstId, 1L)
        val second = rootEnvelope(secondId, 2L)

        assertEquals(SubmitResult.Accepted(firstId), runtime.enqueueEnvelope(first))
        withTimeout(5_000) {
            while (dao.row(firstId)?.state != DurableCommandState.WAITING_APPROVAL.name) {
                kotlinx.coroutines.delay(10)
            }
        }
        assertFalse(first.result.isCompleted)
        assertEquals(RuntimeState.WaitingApproval, runtime.runtimeState.value)

        assertEquals(SubmitResult.Accepted(secondId), runtime.enqueueEnvelope(second))
        kotlinx.coroutines.delay(200)
        assertEquals(1, executions.get())
        assertFalse(second.result.isCompleted)

        val firstLineage = checkNotNull(first.lineage)
        val resume = CommandEnvelope(
            id = firstResumeId,
            conversationId = conversationId,
            command = ResumeAfterApprovalCommand,
            origin = CommandOrigin.INTERNAL,
            sequence = 3L,
            lineage = firstLineage.copy(parentCommandId = firstId),
        )
        assertEquals(SubmitResult.Accepted(firstResumeId), runtime.enqueueEnvelope(resume))
        withTimeout(5_000) {
            while (
                dao.row(firstResumeId)?.state != DurableCommandState.WAITING_APPROVAL.name ||
                runtime.queueStatus.value.activeCommandId != null
            ) {
                kotlinx.coroutines.delay(10)
            }
        }
        assertFalse(first.result.isCompleted)
        assertFalse(resume.result.isCompleted)
        assertFalse(second.result.isCompleted)
        assertEquals(DurableCommandState.WAITING_APPROVAL.name, dao.row(firstId)?.state)

        val finalResumeId = Uuid.random()
        val finalResume = CommandEnvelope(
            id = finalResumeId,
            conversationId = conversationId,
            command = ResumeAfterApprovalCommand,
            origin = CommandOrigin.INTERNAL,
            sequence = 4L,
            lineage = firstLineage.copy(parentCommandId = firstResumeId),
        )
        assertEquals(SubmitResult.Accepted(finalResumeId), runtime.enqueueEnvelope(finalResume))
        withTimeout(5_000) {
            assertEquals(CommandOutcome.Completed, first.result.await())
            assertEquals(CommandOutcome.Completed, resume.result.await())
            assertEquals(CommandOutcome.Completed, finalResume.result.await())
            assertEquals(CommandOutcome.Completed, second.result.await())
        }
        assertEquals(DurableCommandState.COMPLETED.name, dao.row(firstId)?.state)
        assertEquals(DurableCommandState.COMPLETED.name, dao.row(firstResumeId)?.state)
        assertEquals(DurableCommandState.COMPLETED.name, dao.row(finalResumeId)?.state)
        assertEquals(4, executions.get())
        withTimeout(5_000) {
            while (runtime.runtimeState.value != RuntimeState.Idle) {
                kotlinx.coroutines.delay(10)
            }
        }

        runtime.close()
        scope.cancel()
    }

    @Test
    fun `interrupt with no active run cancels waiting authority and fails closed`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val dao = FakePendingChatCommandDao()
        val queue = DurableCommandQueue(
            dao = dao,
            commandStateTransaction = CommandStateTransaction(dao),
        )
        val conversationId = Uuid.random()
        val rootId = Uuid.random()
        val executions = java.util.concurrent.atomic.AtomicInteger(0)
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = conversationId,
            durableQueue = queue,
            executor = RuntimeCommandExecutor { _, _ ->
                executions.incrementAndGet()
                RunOutcome.WaitingApproval(setOf("tool-1"))
            },
        )
        val root = CommandEnvelope(
            id = rootId,
            conversationId = conversationId,
            command = messageCommand("root"),
            origin = CommandOrigin.APP_UI,
            sequence = 1L,
        ).withRootLineage()
        assertEquals(SubmitResult.Accepted(rootId), runtime.enqueueEnvelope(root))
        withTimeout(5_000) {
            while (
                dao.row(rootId)?.state != DurableCommandState.WAITING_APPROVAL.name ||
                runtime.queueStatus.value.activeCommandId != null
            ) {
                kotlinx.coroutines.delay(10)
            }
        }

        val interrupt = CommandEnvelope(
            conversationId = conversationId,
            command = InterruptCommand(messageCommand("replacement")),
            origin = CommandOrigin.APP_UI,
            sequence = 2L,
        )
        assertEquals(
            SubmitResult.Accepted(interrupt.id),
            runtime.replaceEmergencyEnvelope(interrupt),
        )
        withTimeout(5_000) {
            assertEquals(CommandOutcome.Cancelled, root.result.await())
            assertTrue(interrupt.result.await() is CommandOutcome.Failed)
        }
        assertEquals(DurableCommandState.CANCELLED.name, dao.row(rootId)?.state)
        assertEquals(1, executions.get())
        assertEquals(RuntimeState.Paused, runtime.runtimeState.value)

        runtime.close()
        scope.cancel()
    }

    @Test
    fun `stop with no active run commits waiting cancellation before deferreds`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val dao = FakePendingChatCommandDao()
        val queue = DurableCommandQueue(
            dao = dao,
            commandStateTransaction = CommandStateTransaction(dao),
        )
        val conversationId = Uuid.random()
        val rootId = Uuid.random()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = conversationId,
            durableQueue = queue,
            executor = RuntimeCommandExecutor { _, _ ->
                RunOutcome.WaitingApproval(setOf("tool-1"))
            },
        )
        val root = CommandEnvelope(
            id = rootId,
            conversationId = conversationId,
            command = messageCommand("root"),
            origin = CommandOrigin.APP_UI,
            sequence = 1L,
        ).withRootLineage()
        runtime.enqueueEnvelope(root)
        withTimeout(5_000) {
            while (
                dao.row(rootId)?.state != DurableCommandState.WAITING_APPROVAL.name ||
                runtime.queueStatus.value.activeCommandId != null
            ) {
                kotlinx.coroutines.delay(10)
            }
        }

        val stop = CommandEnvelope(
            conversationId = conversationId,
            command = StopCommand(pauseQueue = false),
            origin = CommandOrigin.APP_UI,
            sequence = 2L,
        )
        runtime.replaceEmergencyEnvelope(stop)
        withTimeout(5_000) {
            assertEquals(CommandOutcome.Cancelled, root.result.await())
            assertEquals(CommandOutcome.Completed, stop.result.await())
        }
        assertEquals(DurableCommandState.CANCELLED.name, dao.row(rootId)?.state)
        assertEquals(RuntimeState.Paused, runtime.runtimeState.value)

        val resumeQueue = CommandEnvelope(
            conversationId = conversationId,
            command = ResumeQueueCommand(),
            origin = CommandOrigin.APP_UI,
            sequence = 3L,
        )
        assertEquals(SubmitResult.Accepted(resumeQueue.id), runtime.enqueueEnvelope(resumeQueue))
        withTimeout(5_000) {
            assertTrue(resumeQueue.result.await() is CommandOutcome.Conflict)
        }
        assertEquals(RuntimeState.Paused, runtime.runtimeState.value)

        runtime.close()
        scope.cancel()
    }

    @Test
    fun `steering audit stores yellow and purple history cards exactly once`() {
        val runId = Uuid.random()
        val persistent = SteeringNote(
            commandId = Uuid.random(),
            runId = runId,
            text = "以后也记住这条补充",
            source = CommandOrigin.APP_UI,
            historyMode = SteeringHistoryMode.PERSISTENT,
        )
        val transient = SteeringNote(
            commandId = Uuid.random(),
            runId = runId,
            text = "只在这次任务里参考",
            source = CommandOrigin.APP_UI,
            historyMode = SteeringHistoryMode.TRANSIENT,
        )
        val original = Conversation(
            assistantId = Uuid.random(),
            messageNodes = emptyList(),
        )

        val stored = original
            .withSteeringAuditMessage(persistent)
            .withSteeringAuditMessage(transient)
        val messages = stored.currentMessages
        assertEquals(2, messages.size)
        assertEquals(MessageRole.USER, messages[0].role)
        assertEquals("以后也记住这条补充", (messages[0].parts.single() as UIMessagePart.Text).text)
        assertEquals("只在这次任务里参考", (messages[1].parts.single() as UIMessagePart.Text).text)
        assertTrue((messages[0].annotations.single() as UIMessageAnnotation.Steering).persistent)
        assertTrue(!(messages[1].annotations.single() as UIMessageAnnotation.Steering).persistent)

        val duplicateAttempt = stored.withSteeringAuditMessage(transient)
        assertTrue(duplicateAttempt === stored)
        assertEquals(2, duplicateAttempt.messageNodes.size)
    }

    @Test
    fun `latest emergency supersedes an older emergency`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val started = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val hydrationGate = CompletableDeferred<Unit>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            dispatchers = DispatcherProvider(
                runtime = Dispatchers.Default,
                io = Dispatchers.Default,
                main = Dispatchers.Default,
            ),
            hydrator = RuntimeHydrator { hydrationGate.await() },
            executor = RuntimeCommandExecutor { _, _ ->
                started.complete(Unit)
                release.await()
                RunOutcome.Completed()
            },
        )
        val first = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = InterruptCommand(messageCommand("first")),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val second = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = InterruptCommand(messageCommand("second")),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )

        assertTrue(runtime.replaceEmergencyEnvelope(first) is SubmitResult.Accepted)
        assertTrue(runtime.replaceEmergencyEnvelope(second) is SubmitResult.Accepted)
        assertEquals(CommandOutcome.Superseded(second.id), first.result.await())
        hydrationGate.complete(Unit)
        withTimeout(5_000) { started.await() }
        release.complete(Unit)
        assertEquals(CommandOutcome.Completed, second.result.await())

        runtime.close()
        scope.cancel()
    }

    @Test
    fun `hydration failure rejects accepted commands`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            hydrator = RuntimeHydrator { error("broken hydration") },
            executor = RuntimeCommandExecutor { _, _ -> RunOutcome.Completed() },
        )
        val command = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("will be rejected"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )

        assertTrue(runtime.enqueueEnvelope(command) is SubmitResult.Accepted)
        val outcome = withTimeout(5_000) { command.result.await() }
        assertTrue(outcome is CommandOutcome.Rejected)
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `ordinary commands execute in strict FIFO order`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val order = java.util.Collections.synchronizedList(mutableListOf<String>())
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { envelope, _ ->
                val text = (envelope.command as SendMessageCommand)
                    .content.parts.filterIsInstance<UIMessagePart.Text>().single().text
                order += text
                RunOutcome.Completed()
            },
        )
        val commands = listOf("A", "B", "C").mapIndexed { index, text ->
            CommandEnvelope(
                conversationId = runtime.conversationId,
                command = messageCommand(text),
                origin = CommandOrigin.APP_UI,
                sequence = index.toLong() + 1,
            )
        }
        commands.forEach { assertTrue(runtime.enqueueEnvelope(it) is SubmitResult.Accepted) }
        commands.forEach { assertEquals(CommandOutcome.Completed, it.result.await()) }
        assertEquals(listOf("A", "B", "C"), order)
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `queued message snapshot exposes latest content attachments and FIFO position`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val firstStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { envelope, _ ->
                if ((envelope.command as? SendMessageCommand)?.content?.parts
                        ?.filterIsInstance<UIMessagePart.Text>()?.firstOrNull()?.text == "A"
                ) {
                    firstStarted.complete(Unit)
                    releaseFirst.await()
                }
                RunOutcome.Completed()
            },
        )
        val first = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("A"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val second = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = SendMessageCommand(
                RawUserContent(
                    listOf(
                        UIMessagePart.Text("B"),
                        UIMessagePart.Image("file:///queued-image.jpg"),
                    )
                )
            ),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )
        val third = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("C"),
            origin = CommandOrigin.APP_UI,
            sequence = 3,
        )

        runtime.enqueueEnvelope(first)
        withTimeout(5_000) { firstStarted.await() }
        runtime.enqueueEnvelope(second)
        runtime.enqueueEnvelope(third)

        withTimeout(5_000) {
            while (runtime.queuedMessages.value.size != 2) kotlinx.coroutines.delay(10)
        }
        val snapshot = runtime.queuedMessages.value
        assertEquals(listOf(second.id, third.id), snapshot.map { it.commandId })
        assertEquals(listOf(1, 2), snapshot.map { it.position })
        assertEquals(second.command.content, snapshot.first().content)

        releaseFirst.complete(Unit)
        first.result.await()
        second.result.await()
        third.result.await()
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `editing a queued message executes only the latest content`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val firstStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val executed = java.util.Collections.synchronizedList(mutableListOf<Pair<String, Boolean>>())
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { envelope, _ ->
                val sendCommand = envelope.command as? SendMessageCommand
                val text = sendCommand?.content?.parts
                    ?.filterIsInstance<UIMessagePart.Text>()?.firstOrNull()?.text
                if (text != null) {
                    executed += text to sendCommand.content.answer
                }
                if (text == "A") {
                    firstStarted.complete(Unit)
                    releaseFirst.await()
                }
                RunOutcome.Completed()
            },
        )
        val first = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("A"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val queued = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = SendMessageCommand(
                RawUserContent(
                    parts = listOf(UIMessagePart.Text("old")),
                    answer = false,
                )
            ),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )
        val update = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = UpdateQueuedMessageCommand(
                targetCommandId = queued.id,
                content = RawUserContent(
                    parts = listOf(UIMessagePart.Text("new")),
                    answer = false,
                ),
            ),
            origin = CommandOrigin.APP_UI,
            sequence = 3,
        )

        runtime.enqueueEnvelope(first)
        withTimeout(5_000) { firstStarted.await() }
        runtime.enqueueEnvelope(queued)
        runtime.enqueueEnvelope(update)
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { update.result.await() })
        assertEquals("new", runtime.queuedMessages.value.single().content.parts
            .filterIsInstance<UIMessagePart.Text>().single().text)

        releaseFirst.complete(Unit)
        assertEquals(CommandOutcome.Completed, first.result.await())
        assertEquals(CommandOutcome.Completed, queued.result.await())
        assertEquals(listOf("A" to true, "new" to false), executed)
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `promoting queued text keeps command id and applies it to the active run`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val runStarted = CompletableDeferred<Unit>()
        val allowCheckpoint = CompletableDeferred<Unit>()
        val deliveredCommandId = CompletableDeferred<Uuid>()
        val releaseRun = CompletableDeferred<Unit>()
        val executionCount = java.util.concurrent.atomic.AtomicInteger(0)
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { _, control ->
                executionCount.incrementAndGet()
                runStarted.complete(Unit)
                allowCheckpoint.await()
                val deliveries = control.takeSteeringForCheckpoint(1)
                control.markSteeringProviderStarted(deliveries)
                deliveredCommandId.complete(deliveries.single().note.commandId)
                releaseRun.await()
                RunOutcome.Completed()
            },
        )
        val active = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val queued = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("现在优先检查日志"),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )
        val promote = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = PromoteQueuedMessageToSteeringCommand(queued.id),
            origin = CommandOrigin.APP_UI,
            sequence = 3,
        )

        runtime.enqueueEnvelope(active)
        withTimeout(5_000) { runStarted.await() }
        runtime.enqueueEnvelope(queued)
        runtime.enqueueEnvelope(promote)
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { promote.result.await() })
        assertTrue(runtime.queuedMessages.value.none { it.commandId == queued.id })

        allowCheckpoint.complete(Unit)
        assertEquals(queued.id, withTimeout(5_000) { deliveredCommandId.await() })
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { queued.result.await() })
        releaseRun.complete(Unit)
        assertEquals(CommandOutcome.Completed, active.result.await())
        assertEquals(1, executionCount.get())
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `attachment only queued message cannot be promoted and remains queued`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val runStarted = CompletableDeferred<Unit>()
        val releaseRun = CompletableDeferred<Unit>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { envelope, _ ->
                if ((envelope.command as? SendMessageCommand)?.content?.parts
                        ?.filterIsInstance<UIMessagePart.Text>()?.singleOrNull()?.text == "active"
                ) {
                    runStarted.complete(Unit)
                    releaseRun.await()
                }
                RunOutcome.Completed()
            },
        )
        val active = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val attachmentContent = RawUserContent(
            listOf(UIMessagePart.Image("file:///attachment-only.jpg"))
        )
        val queued = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = SendMessageCommand(attachmentContent),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )
        val promote = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = PromoteQueuedMessageToSteeringCommand(queued.id),
            origin = CommandOrigin.APP_UI,
            sequence = 3,
        )

        runtime.enqueueEnvelope(active)
        withTimeout(5_000) { runStarted.await() }
        runtime.enqueueEnvelope(queued)
        runtime.enqueueEnvelope(promote)

        val promoteOutcome = withTimeout(5_000) { promote.result.await() }
        assertTrue(promoteOutcome is CommandOutcome.Rejected)
        assertEquals(attachmentContent, runtime.queuedMessages.value.single().content)

        releaseRun.complete(Unit)
        assertEquals(CommandOutcome.Completed, active.result.await())
        assertEquals(CommandOutcome.Completed, queued.result.await())
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `promoted queued message keeps attachments when finished run falls back to FIFO`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val runStarted = CompletableDeferred<Unit>()
        val releaseRun = CompletableDeferred<Unit>()
        val executedContents = java.util.Collections.synchronizedList(mutableListOf<RawUserContent>())
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { envelope, _ ->
                val content = (envelope.command as SendMessageCommand).content
                executedContents += content
                if (content.parts.filterIsInstance<UIMessagePart.Text>().single().text == "active") {
                    runStarted.complete(Unit)
                    releaseRun.await()
                }
                RunOutcome.Completed()
            },
        )
        val active = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val queuedContent = RawUserContent(
            parts = listOf(
                UIMessagePart.Text("下一步参考附件"),
                UIMessagePart.Image("file:///keep-this-image.jpg"),
            ),
            answer = false,
        )
        val queued = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = SendMessageCommand(queuedContent),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )
        val promote = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = PromoteQueuedMessageToSteeringCommand(queued.id),
            origin = CommandOrigin.APP_UI,
            sequence = 3,
        )

        runtime.enqueueEnvelope(active)
        withTimeout(5_000) { runStarted.await() }
        runtime.enqueueEnvelope(queued)
        runtime.enqueueEnvelope(promote)
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { promote.result.await() })

        releaseRun.complete(Unit)
        assertEquals(CommandOutcome.Completed, active.result.await())
        assertEquals(CommandOutcome.Completed, queued.result.await())
        assertEquals(listOf(active.command.content, queuedContent), executedContents)
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `normal queue is bounded while emergency remains available`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val hydrationGate = CompletableDeferred<Unit>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            hydrator = RuntimeHydrator { hydrationGate.await() },
            executor = RuntimeCommandExecutor { _, _ -> RunOutcome.Completed() },
        )
        val commands = (1..32).map { index ->
            CommandEnvelope(
                conversationId = runtime.conversationId,
                command = messageCommand("queued-$index"),
                origin = CommandOrigin.APP_UI,
                sequence = index.toLong(),
            )
        }
        commands.forEach { assertTrue(runtime.enqueueEnvelope(it) is SubmitResult.Accepted) }
        val overflow = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("overflow"),
            origin = CommandOrigin.APP_UI,
            sequence = 33,
        )
        assertEquals(SubmitResult.QueueFull(32), runtime.enqueueEnvelope(overflow))

        val stop = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = StopCommand(),
            origin = CommandOrigin.APP_UI,
            sequence = 34,
        )
        assertTrue(runtime.replaceEmergencyEnvelope(stop) is SubmitResult.Accepted)
        hydrationGate.complete(Unit)
        assertEquals(CommandOutcome.Completed, stop.result.await())
        assertEquals(32, runtime.queueStatus.value.pendingCount)

        runtime.close()
        scope.cancel()
    }

    @Test
    fun `stop pauses queued work until resume`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val firstStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val secondStarted = CompletableDeferred<Unit>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { envelope, _ ->
                val text = (envelope.command as? SendMessageCommand)
                    ?.content?.parts?.filterIsInstance<UIMessagePart.Text>()?.firstOrNull()?.text
                if (text == "A") {
                    firstStarted.complete(Unit)
                    kotlinx.coroutines.awaitCancellation()
                } else if (text == "B") {
                    secondStarted.complete(Unit)
                }
                RunOutcome.Completed()
            },
        )
        val first = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("A"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val second = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("B"),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )

        assertTrue(runtime.enqueueEnvelope(first) is SubmitResult.Accepted)
        withTimeout(5_000) { firstStarted.await() }
        assertTrue(runtime.enqueueEnvelope(second) is SubmitResult.Accepted)
        val stop = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = StopCommand(),
            origin = CommandOrigin.APP_UI,
            sequence = 3,
        )
        assertTrue(runtime.replaceEmergencyEnvelope(stop) is SubmitResult.Accepted)
        releaseFirst.complete(Unit)
        assertEquals(CommandOutcome.Cancelled, first.result.await())
        assertEquals(CommandOutcome.Completed, stop.result.await())
        kotlinx.coroutines.delay(100)
        assertTrue(!secondStarted.isCompleted)

        val resume = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = ResumeQueueCommand(),
            origin = CommandOrigin.APP_UI,
            sequence = 4,
        )
        assertTrue(runtime.enqueueEnvelope(resume) is SubmitResult.Accepted)
        withTimeout(5_000) { secondStarted.await() }
        assertEquals(CommandOutcome.Completed, second.result.await())
        assertEquals(CommandOutcome.Completed, resume.result.await())
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `promoted guidance card disappears when paused queue resumes it`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val firstStarted = CompletableDeferred<Unit>()
        val secondStarted = CompletableDeferred<Unit>()
        val releaseSecond = CompletableDeferred<Unit>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { envelope, _ ->
                val text = (envelope.command as? SendMessageCommand)
                    ?.content?.parts?.filterIsInstance<UIMessagePart.Text>()?.firstOrNull()?.text
                when (text) {
                    "A" -> {
                        firstStarted.complete(Unit)
                        kotlinx.coroutines.awaitCancellation()
                    }
                    "B" -> {
                        secondStarted.complete(Unit)
                        releaseSecond.await()
                    }
                }
                RunOutcome.Completed()
            },
        )
        val first = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("A"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        assertTrue(runtime.enqueueEnvelope(first) is SubmitResult.Accepted)
        withTimeout(5_000) { firstStarted.await() }

        val stop = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = StopCommand(),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )
        assertTrue(runtime.replaceEmergencyEnvelope(stop) is SubmitResult.Accepted)
        assertEquals(CommandOutcome.Cancelled, withTimeout(5_000) { first.result.await() })
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { stop.result.await() })

        val queued = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("B"),
            origin = CommandOrigin.APP_UI,
            sequence = 3,
        )
        assertTrue(runtime.enqueueEnvelope(queued) is SubmitResult.Accepted)
        val promote = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = PromoteQueuedMessageToSteeringCommand(queued.id),
            origin = CommandOrigin.APP_UI,
            sequence = 4,
        )
        assertTrue(runtime.enqueueEnvelope(promote) is SubmitResult.Accepted)
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { promote.result.await() })
        assertEquals(
            me.rerere.rikkahub.data.ai.SteeringState.FALLBACK_QUEUED,
            runtime.steeringEntries.value.getValue(queued.id).state,
        )

        val resume = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = ResumeQueueCommand(),
            origin = CommandOrigin.APP_UI,
            sequence = 5,
        )
        assertTrue(runtime.enqueueEnvelope(resume) is SubmitResult.Accepted)
        withTimeout(5_000) { secondStarted.await() }
        assertTrue(queued.id !in runtime.steeringEntries.value)

        releaseSecond.complete(Unit)
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { queued.result.await() })
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { resume.result.await() })
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `queue status exposes pending command ids in FIFO order`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val hydrationGate = CompletableDeferred<Unit>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            hydrator = RuntimeHydrator { hydrationGate.await() },
            executor = RuntimeCommandExecutor { _, _ -> RunOutcome.Completed() },
        )
        val commands = listOf("A", "B", "C").mapIndexed { index, text ->
            CommandEnvelope(
                conversationId = runtime.conversationId,
                command = messageCommand(text),
                origin = CommandOrigin.APP_UI,
                sequence = index.toLong() + 1,
            )
        }

        commands.forEach { assertTrue(runtime.enqueueEnvelope(it) is SubmitResult.Accepted) }
        assertEquals(commands.map { it.id }, runtime.queueStatus.value.pendingCommandIds)
        assertEquals(3, runtime.queueStatus.value.pendingCount)
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `clear pending queue drains commands still buffered in normal channel`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val hydrationGate = CompletableDeferred<Unit>()
        val executed = java.util.Collections.synchronizedList(mutableListOf<String>())
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            hydrator = RuntimeHydrator { hydrationGate.await() },
            executor = RuntimeCommandExecutor { envelope, _ ->
                executed += (envelope.command as SendMessageCommand)
                    .content.parts.filterIsInstance<UIMessagePart.Text>().single().text
                RunOutcome.Completed()
            },
        )
        val commands = listOf("A", "B", "C").mapIndexed { index, text ->
            CommandEnvelope(
                conversationId = runtime.conversationId,
                command = messageCommand(text),
                origin = CommandOrigin.APP_UI,
                sequence = index.toLong() + 1,
            )
        }
        val clear = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = ClearPendingQueueCommand(),
            origin = CommandOrigin.APP_UI,
            sequence = 4,
        )

        commands.forEach { assertTrue(runtime.enqueueEnvelope(it) is SubmitResult.Accepted) }
        assertTrue(runtime.enqueueEnvelope(clear) is SubmitResult.Accepted)
        hydrationGate.complete(Unit)

        commands.forEach { assertEquals(CommandOutcome.Cancelled, it.result.await()) }
        assertEquals(CommandOutcome.Completed, clear.result.await())
        assertTrue(executed.isEmpty())
        assertEquals(0, runtime.queueStatus.value.pendingCount)
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `cancel queued command can remove a command still buffered in normal channel`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val hydrationGate = CompletableDeferred<Unit>()
        val order = java.util.Collections.synchronizedList(mutableListOf<String>())
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            hydrator = RuntimeHydrator { hydrationGate.await() },
            executor = RuntimeCommandExecutor { envelope, _ ->
                order += (envelope.command as SendMessageCommand)
                    .content.parts.filterIsInstance<UIMessagePart.Text>().single().text
                RunOutcome.Completed()
            },
        )
        val first = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("A"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val second = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("B"),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )
        val third = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("C"),
            origin = CommandOrigin.APP_UI,
            sequence = 3,
        )
        val cancelSecond = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = CancelQueuedCommand(second.id),
            origin = CommandOrigin.APP_UI,
            sequence = 4,
        )

        listOf(first, second, third).forEach { assertTrue(runtime.enqueueEnvelope(it) is SubmitResult.Accepted) }
        assertTrue(runtime.enqueueEnvelope(cancelSecond) is SubmitResult.Accepted)
        hydrationGate.complete(Unit)

        assertEquals(CommandOutcome.Completed, cancelSecond.result.await())
        assertEquals(CommandOutcome.Cancelled, second.result.await())
        assertEquals(CommandOutcome.Completed, first.result.await())
        assertEquals(CommandOutcome.Completed, third.result.await())
        assertEquals(listOf("A", "C"), order)
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `duplicate dedupe key is idempotently coalesced while original command is pending`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val hydrationGate = CompletableDeferred<Unit>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            hydrator = RuntimeHydrator { hydrationGate.await() },
            executor = RuntimeCommandExecutor { _, _ -> RunOutcome.Completed() },
        )
        val first = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("A"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
            dedupeKey = "same-turn",
        )
        val duplicate = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("A again"),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
            dedupeKey = "same-turn",
        )

        assertTrue(runtime.enqueueEnvelope(first) is SubmitResult.Accepted)
        assertEquals(
            SubmitResult.Accepted(first.id),
            runtime.enqueueEnvelope(duplicate),
        )
        assertEquals(CommandOutcome.Superseded(first.id), duplicate.result.await())
        hydrationGate.complete(Unit)
        assertEquals(CommandOutcome.Completed, first.result.await())
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `duplicate command id mirrors the original outcome`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val hydrationGate = CompletableDeferred<Unit>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            hydrator = RuntimeHydrator { hydrationGate.await() },
            executor = RuntimeCommandExecutor { _, _ -> RunOutcome.Completed() },
        )
        val first = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("A"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val duplicate = first.copy(result = CompletableDeferred())

        assertEquals(SubmitResult.Accepted(first.id), runtime.enqueueEnvelope(first))
        assertEquals(SubmitResult.Accepted(first.id), runtime.enqueueEnvelope(duplicate))
        hydrationGate.complete(Unit)

        assertEquals(CommandOutcome.Completed, first.result.await())
        assertEquals(CommandOutcome.Completed, duplicate.result.await())
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `simultaneous retries with one command id execute only once`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val hydrationGate = CompletableDeferred<Unit>()
        val executions = java.util.concurrent.atomic.AtomicInteger(0)
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            hydrator = RuntimeHydrator { hydrationGate.await() },
            executor = RuntimeCommandExecutor { _, _ ->
                executions.incrementAndGet()
                RunOutcome.Completed()
            },
        )
        val original = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("A"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
            dedupeKey = "same-command-and-turn",
        )
        val retries = List(32) { original.copy(result = CompletableDeferred()) }

        val submitResults = retries.map { retry ->
            async(Dispatchers.Default) { runtime.enqueueEnvelope(retry) }
        }.awaitAll()
        assertTrue(submitResults.all { it == SubmitResult.Accepted(original.id) })
        hydrationGate.complete(Unit)

        retries.forEach { retry ->
            assertEquals(CommandOutcome.Completed, withTimeout(5_000) { retry.result.await() })
        }
        assertEquals(1, executions.get())
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `dedupe follower cannot release the original reservation`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val hydrationGate = CompletableDeferred<Unit>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            hydrator = RuntimeHydrator { hydrationGate.await() },
            executor = RuntimeCommandExecutor { _, _ -> RunOutcome.Completed() },
        )
        val first = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("A"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
            dedupeKey = "reserved-turn",
        )
        val second = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("B"),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
            dedupeKey = "reserved-turn",
        )
        val third = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("C"),
            origin = CommandOrigin.APP_UI,
            sequence = 3,
            dedupeKey = "reserved-turn",
        )

        assertEquals(SubmitResult.Accepted(first.id), runtime.enqueueEnvelope(first))
        assertEquals(SubmitResult.Accepted(first.id), runtime.enqueueEnvelope(second))
        assertEquals(CommandOutcome.Superseded(first.id), second.result.await())
        assertEquals(SubmitResult.Accepted(first.id), runtime.enqueueEnvelope(third))
        assertEquals(CommandOutcome.Superseded(first.id), third.result.await())

        hydrationGate.complete(Unit)
        assertEquals(CommandOutcome.Completed, first.result.await())
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `completed command id replays outcome without executing again`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val executions = java.util.concurrent.atomic.AtomicInteger(0)
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { _, _ ->
                executions.incrementAndGet()
                RunOutcome.Completed()
            },
        )
        val first = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("A"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        assertEquals(SubmitResult.Accepted(first.id), runtime.enqueueEnvelope(first))
        assertEquals(CommandOutcome.Completed, first.result.await())

        val replay = first.copy(result = CompletableDeferred())
        assertEquals(SubmitResult.Accepted(first.id), runtime.enqueueEnvelope(replay))
        assertEquals(CommandOutcome.Completed, replay.result.await())
        assertEquals(1, executions.get())

        runtime.close()
        scope.cancel()
    }

    @Test
    fun `expired normal command is rejected before execution`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val executed = CompletableDeferred<Unit>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { _, _ ->
                executed.complete(Unit)
                RunOutcome.Completed()
            },
        )
        val expired = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("expired"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
            expiresAt = Clock.System.now() - 1.seconds,
        )

        assertTrue(runtime.enqueueEnvelope(expired) is SubmitResult.Accepted)
        val outcome = expired.result.await()
        assertTrue(outcome is CommandOutcome.Rejected)
        assertTrue(!executed.isCompleted)
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `dependency failure skips dependent command but does not cancel unrelated queued command`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val order = java.util.Collections.synchronizedList(mutableListOf<String>())
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { envelope, _ ->
                val text = (envelope.command as SendMessageCommand)
                    .content.parts.filterIsInstance<UIMessagePart.Text>().single().text
                order += text
                if (text == "A") RunOutcome.Rejected("boom") else RunOutcome.Completed()
            },
        )
        val first = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("A"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val dependent = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("B"),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
            dependencies = listOf(CommandDependency(first.id, RequiredOutcome.COMPLETED)),
        )
        val unrelated = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("C"),
            origin = CommandOrigin.APP_UI,
            sequence = 3,
        )

        listOf(first, dependent, unrelated).forEach { assertTrue(runtime.enqueueEnvelope(it) is SubmitResult.Accepted) }
        assertTrue(first.result.await() is CommandOutcome.Rejected)
        assertEquals(CommandOutcome.SkippedDependencyFailed(first.id), dependent.result.await())
        assertEquals(CommandOutcome.Completed, unrelated.result.await())
        assertEquals(listOf("A", "C"), order)
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `interrupt cancels active run repairs it and starts latest replacement`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val firstStarted = CompletableDeferred<Unit>()
        val replacementStarted = CompletableDeferred<Unit>()
        val repairReason = CompletableDeferred<me.rerere.rikkahub.data.ai.tools.ToolCancelReason>()
        val order = java.util.Collections.synchronizedList(mutableListOf<String>())
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            repairer = RuntimeRepairer { _, reason ->
                repairReason.complete(reason)
                InterruptCleanupResult.Completed
            },
            executor = RuntimeCommandExecutor { envelope, _ ->
                val text = when (val command = envelope.command) {
                    is SendMessageCommand -> command.content.parts.filterIsInstance<UIMessagePart.Text>().single().text
                    is InterruptCommand -> command.replacement.content.parts.filterIsInstance<UIMessagePart.Text>().single().text
                    else -> "other"
                }
                order += text
                if (text == "A") {
                    firstStarted.complete(Unit)
                    kotlinx.coroutines.awaitCancellation()
                } else {
                    replacementStarted.complete(Unit)
                    RunOutcome.Completed()
                }
            },
        )
        val first = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("A"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val replacement = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = InterruptCommand(messageCommand("B")),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )

        assertTrue(runtime.enqueueEnvelope(first) is SubmitResult.Accepted)
        withTimeout(5_000) { firstStarted.await() }
        assertTrue(runtime.replaceEmergencyEnvelope(replacement) is SubmitResult.Accepted)

        assertEquals(CommandOutcome.Cancelled, first.result.await())
        assertEquals(CommandOutcome.Completed, replacement.result.await())
        assertEquals(me.rerere.rikkahub.data.ai.tools.ToolCancelReason.USER_INTERRUPTED, repairReason.await())
        assertTrue(replacementStarted.isCompleted)
        assertEquals(listOf("A", "B"), order)
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `interrupt regenerate cancels active run before regeneration starts`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val firstStarted = CompletableDeferred<Unit>()
        val regenerateStarted = CompletableDeferred<Unit>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { envelope, _ ->
                when (envelope.command) {
                    is SendMessageCommand -> {
                        firstStarted.complete(Unit)
                        kotlinx.coroutines.awaitCancellation()
                    }
                    is InterruptRegenerateCommand -> regenerateStarted.complete(Unit)
                    else -> Unit
                }
                RunOutcome.Completed()
            },
        )
        val active = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        assertTrue(runtime.enqueueEnvelope(active) is SubmitResult.Accepted)
        withTimeout(5_000) { firstStarted.await() }

        val targetMessageId = Uuid.random()
        val regenerate = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = RegenerateCommand(
                targetMessageId = targetMessageId,
                expectedTargetVersion = 0L,
                expectedBranchHeadMessageId = targetMessageId,
            ),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )
        assertTrue(runtime.enqueueEnvelope(regenerate) is SubmitResult.Accepted)

        assertEquals(CommandOutcome.Cancelled, withTimeout(5_000) { active.result.await() })
        withTimeout(5_000) { regenerateStarted.await() }
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { regenerate.result.await() })

        runtime.close()
        scope.cancel()
    }

    @Test
    fun `interrupt regenerate claims and finalizes its persisted durable row`() = runBlocking {
        // INTERRUPT_CURRENT is persisted before it is rewritten into the emergency envelope.
        // The rewritten envelope must retain lineage, claim that exact row, and fence completion.
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val regenerateStarted = CompletableDeferred<Unit>()
        val dao = FakePendingChatCommandDao()
        val durableQueue = DurableCommandQueue(
            dao,
            workerId = "regenerate-claim-worker",
            commandStateTransaction = CommandStateTransaction(dao),
        )
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            durableQueue = durableQueue,
            executor = RuntimeCommandExecutor { envelope, _ ->
                when (envelope.command) {
                    is InterruptRegenerateCommand -> regenerateStarted.complete(Unit)
                    else -> Unit
                }
                RunOutcome.Completed()
            },
        )

        val targetMessageId = Uuid.random()
        val regenerate = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = RegenerateCommand(
                targetMessageId = targetMessageId,
                expectedTargetVersion = 0L,
                expectedBranchHeadMessageId = targetMessageId,
            ),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        ).withRootLineage()
        assertTrue(runtime.enqueueEnvelope(regenerate) is SubmitResult.Accepted)
        withTimeout(5_000) { regenerateStarted.await() }
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { regenerate.result.await() })
        val durableRow = checkNotNull(dao.row(regenerate.id))
        assertEquals(DurableCommandState.COMPLETED.name, durableRow.state)
        assertEquals(1, durableRow.attempt)
        assertTrue(durableRow.stateVersion >= 3L)

        runtime.close()
        scope.cancel()
    }

    @Test
    fun `interrupt regenerate fences non cooperative provider and continues`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val firstStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val regenerateStarted = CompletableDeferred<Unit>()
        val surfacedError = CompletableDeferred<Throwable>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            cancellationGracePeriod = 50.milliseconds,
            onCancellationTimeout = { _, error -> surfacedError.complete(error) },
            executor = RuntimeCommandExecutor { envelope, _ ->
                when (envelope.command) {
                    is SendMessageCommand -> {
                        firstStarted.complete(Unit)
                        withContext(NonCancellable) { releaseFirst.await() }
                    }
                    is InterruptRegenerateCommand -> regenerateStarted.complete(Unit)
                    else -> Unit
                }
                RunOutcome.Completed()
            },
        )
        val active = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        assertTrue(runtime.enqueueEnvelope(active) is SubmitResult.Accepted)
        withTimeout(5_000) { firstStarted.await() }

        val targetMessageId = Uuid.random()
        val regenerate = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = RegenerateCommand(
                targetMessageId = targetMessageId,
                expectedTargetVersion = 0L,
                expectedBranchHeadMessageId = targetMessageId,
            ),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )
        assertTrue(runtime.enqueueEnvelope(regenerate) is SubmitResult.Accepted)

        withTimeout(2_000) { regenerateStarted.await() }
        assertEquals(CommandOutcome.Completed, withTimeout(2_000) { regenerate.result.await() })
        assertTrue(!surfacedError.isCompleted)

        releaseFirst.complete(Unit)
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `interrupt regenerate fails when a non cooperative tool is still in flight`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val firstStarted = CompletableDeferred<Unit>()
        val releaseFirst = CompletableDeferred<Unit>()
        val regenerateStarted = CompletableDeferred<Unit>()
        val surfacedError = CompletableDeferred<Throwable>()
        val handle = object : ToolExecutionHandle {
            override val executionId: String = "in-flight"
            override suspend fun awaitResult(): List<UIMessagePart> = emptyList()
            override fun requestCancel(reason: ToolCancelReason): CancelRequestResult =
                CancelRequestResult.LocalWaitCancelledOnly
            override suspend fun awaitTermination(gracePeriod: kotlin.time.Duration): ToolTerminationState =
                ToolTerminationState.Unknown
        }
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            cancellationGracePeriod = 50.milliseconds,
            onCancellationTimeout = { _, error -> surfacedError.complete(error) },
            executor = RuntimeCommandExecutor { envelope, control ->
                when (envelope.command) {
                    is SendMessageCommand -> {
                        control.registerTool("call-1", handle)
                        firstStarted.complete(Unit)
                        withContext(NonCancellable) { releaseFirst.await() }
                    }
                    is InterruptRegenerateCommand -> regenerateStarted.complete(Unit)
                    else -> Unit
                }
                RunOutcome.Completed()
            },
        )
        val active = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        assertTrue(runtime.enqueueEnvelope(active) is SubmitResult.Accepted)
        withTimeout(5_000) { firstStarted.await() }

        val targetMessageId = Uuid.random()
        val regenerate = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = RegenerateCommand(
                targetMessageId = targetMessageId,
                expectedTargetVersion = 0L,
                expectedBranchHeadMessageId = targetMessageId,
            ),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )
        assertTrue(runtime.enqueueEnvelope(regenerate) is SubmitResult.Accepted)

        val outcome = withTimeout(2_000) { regenerate.result.await() }
        assertTrue(outcome is CommandOutcome.Failed)
        assertEquals((outcome as CommandOutcome.Failed).error, withTimeout(2_000) { surfacedError.await() })
        assertTrue(!regenerateStarted.isCompleted)

        releaseFirst.complete(Unit)
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `next queued command waits for persistence completion barrier`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val barrierEntered = CompletableDeferred<Unit>()
        val releaseBarrier = CompletableDeferred<Unit>()
        val bStarted = CompletableDeferred<Unit>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            persistenceCoordinator = object : PersistenceCoordinator {
                override suspend fun persistIfNewer(snapshot: ConversationSnapshot): PersistResult =
                    PersistResult.Persisted

                override suspend fun flushThrough(revision: Long): PersistResult {
                    barrierEntered.complete(Unit)
                    releaseBarrier.await()
                    return PersistResult.Persisted
                }
            },
            executor = RuntimeCommandExecutor { envelope, _ ->
                val text = (envelope.command as SendMessageCommand)
                    .content.parts.filterIsInstance<UIMessagePart.Text>().single().text
                if (text == "B") bStarted.complete(Unit)
                RunOutcome.Completed(finalRevision = if (text == "A") 1L else 2L)
            },
        )
        val first = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("A"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val second = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("B"),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )

        assertTrue(runtime.enqueueEnvelope(first) is SubmitResult.Accepted)
        assertTrue(runtime.enqueueEnvelope(second) is SubmitResult.Accepted)
        withTimeout(5_000) { barrierEntered.await() }
        assertTrue(!bStarted.isCompleted)
        assertTrue(!first.result.isCompleted)

        releaseBarrier.complete(Unit)
        assertEquals(CommandOutcome.Completed, first.result.await())
        withTimeout(5_000) { bStarted.await() }
        assertEquals(CommandOutcome.Completed, second.result.await())
        runtime.close()
        scope.cancel()
    }
    @Test
    fun `interrupt passes per-tool cancellation results into repairer`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val started = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val repaired = CompletableDeferred<Map<String, CancelRequestResult>>()
        val handle = object : ToolExecutionHandle {
            override val executionId: String = "tool-execution"
            override suspend fun awaitResult(): List<UIMessagePart> = emptyList()
            override fun requestCancel(reason: ToolCancelReason): CancelRequestResult =
                CancelRequestResult.LocalWaitCancelledOnly
            override suspend fun awaitTermination(gracePeriod: kotlin.time.Duration): ToolTerminationState =
                ToolTerminationState.StoppedConfirmed
        }
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            repairer = object : RuntimeRepairer {
                override suspend fun repair(runId: Uuid, reason: ToolCancelReason): InterruptCleanupResult =
                    InterruptCleanupResult.Completed
                override suspend fun repair(
                    runId: Uuid,
                    reason: ToolCancelReason,
                    toolCancellationResults: Map<String, CancelRequestResult>,
                ): InterruptCleanupResult {
                    repaired.complete(toolCancellationResults)
                    return InterruptCleanupResult.Completed
                }
            },
            executor = RuntimeCommandExecutor { _, control ->
                control.registerTool("call-1", handle)
                started.complete(Unit)
                kotlinx.coroutines.awaitCancellation()
                RunOutcome.Completed()
            },
        )
        val first = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val stop = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = StopCommand(),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )
        assertTrue(runtime.enqueueEnvelope(first) is SubmitResult.Accepted)
        withTimeout(5_000) { started.await() }
        assertTrue(runtime.replaceEmergencyEnvelope(stop) is SubmitResult.Accepted)
        release.complete(Unit)
        assertEquals(CommandOutcome.Cancelled, first.result.await())
        assertEquals(CommandOutcome.Completed, stop.result.await())
        assertEquals(
            CancelRequestResult.LocalWaitCancelledOnly,
            withTimeout(5_000) { repaired.await()["call-1"] },
        )
        runtime.close()
        scope.cancel()
    }
    @Test
    fun `accepted soft steering is visible at the immediate next checkpoint without cancelling work`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val toolStarted = CompletableDeferred<Unit>()
        val releaseTool = CompletableDeferred<Unit>()
        val releaseRun = CompletableDeferred<Unit>()
        val observed = CompletableDeferred<List<me.rerere.rikkahub.data.ai.SteeringNote>>()
        val runId = CompletableDeferred<Uuid>()
        val toolExecutions = java.util.concurrent.atomic.AtomicInteger(0)
        val toolCancelRequests = java.util.concurrent.atomic.AtomicInteger(0)
        val providerCancelRequests = java.util.concurrent.atomic.AtomicInteger(0)
        val handle = object : ToolExecutionHandle {
            override val executionId: String = "soft-steer-tool"
            override suspend fun awaitResult(): List<UIMessagePart> = emptyList()
            override fun requestCancel(reason: ToolCancelReason): CancelRequestResult {
                toolCancelRequests.incrementAndGet()
                return CancelRequestResult.Requested
            }
            override suspend fun awaitTermination(gracePeriod: kotlin.time.Duration): ToolTerminationState =
                ToolTerminationState.StoppedConfirmed
        }
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { _, control ->
                runId.complete(control.runId)
                val providerRegistration = control.registerProviderCancel {
                    providerCancelRequests.incrementAndGet()
                }
                control.registerTool("call-1", handle)
                toolExecutions.incrementAndGet()
                toolStarted.complete(Unit)
                releaseTool.await()
                control.unregisterTool("call-1", handle)
                providerRegistration.close()
                val deliveries = control.takeSteeringForCheckpoint(1)
                control.markSteeringProviderStarted(deliveries)
                observed.complete(deliveries.map { it.note })
                releaseRun.await()
                RunOutcome.Completed()
            },
        )
        val message = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val steer = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = SteerCommand("focus on recovery", SteeringScope.NEXT_MODEL_CALL),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )

        assertTrue(runtime.enqueueEnvelope(message) is SubmitResult.Accepted)
        withTimeout(5_000) { toolStarted.await() }
        assertTrue(runtime.enqueueEnvelope(steer) is SubmitResult.Accepted)
        // Deliberately release immediately: do not wait for pendingSteering() or any
        // internal signal. The registration/checkpoint boundary must be linearizable.
        releaseTool.complete(Unit)

        assertEquals(CommandOutcome.Completed, steer.result.await())
        val delivered = withTimeout(5_000) { observed.await().single() }
        assertEquals("focus on recovery", delivered.text)
        assertEquals(runId.await(), delivered.runId)
        val appliedEntry = runtime.steeringEntries.value.getValue(steer.id)
        assertEquals(me.rerere.rikkahub.data.ai.SteeringState.APPLIED, appliedEntry.state)
        assertTrue(appliedEntry.editable)
        assertEquals(1, toolExecutions.get())
        assertEquals(0, toolCancelRequests.get())
        assertEquals(0, providerCancelRequests.get())
        releaseRun.complete(Unit)
        assertEquals(CommandOutcome.Completed, message.result.await())
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `pending steering is not queued after an explicit stop`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val terminalGate = CompletableDeferred<Unit>()
        val dao = FakePendingChatCommandDao(resolvePendingGate = terminalGate)
        val durableQueue = DurableCommandQueue(
            dao,
            workerId = "stop-steering-worker",
            commandStateTransaction = CommandStateTransaction(dao),
        )
        val runStarted = CompletableDeferred<Unit>()
        val neverRelease = CompletableDeferred<Unit>()
        val executions = java.util.concurrent.atomic.AtomicInteger(0)
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            durableQueue = durableQueue,
            executor = RuntimeCommandExecutor { _, _ ->
                executions.incrementAndGet()
                runStarted.complete(Unit)
                neverRelease.await()
                RunOutcome.Completed()
            },
        )
        val message = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        ).withRootLineage()
        val steer = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = SteerCommand("obsolete after stop"),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        ).withRootLineage()
        val stop = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = StopCommand(),
            origin = CommandOrigin.APP_UI,
            sequence = 3,
        )

        assertEquals(SubmitResult.Accepted(message.id), runtime.enqueueEnvelope(message))
        withTimeout(5_000) { runStarted.await() }
        assertEquals(SubmitResult.Accepted(steer.id), runtime.enqueueEnvelope(steer))
        assertEquals("steer", dao.row(steer.id)?.type)
        assertEquals(SubmitResult.Accepted(stop.id), runtime.replaceEmergencyEnvelope(stop))

        assertEquals(CommandOutcome.Cancelled, withTimeout(5_000) { message.result.await() })
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { stop.result.await() })
        // In-memory completion is deliberately held behind the durable terminal CAS. While
        // that transition is blocked, a scan must not recover the steer as send_message work.
        kotlinx.coroutines.delay(150)
        assertFalse(steer.result.isCompleted)
        assertEquals("steer", dao.row(steer.id)?.type)
        assertEquals(DurableCommandState.PENDING.name, dao.row(steer.id)?.state)
        assertEquals(1, executions.get())

        terminalGate.complete(Unit)
        assertTrue(withTimeout(5_000) { steer.result.await() } is CommandOutcome.NotApplied)
        assertEquals(
            me.rerere.rikkahub.data.ai.SteeringState.NOT_APPLIED_RUN_FINISHED,
            runtime.steeringEntries.value.getValue(steer.id).state,
        )
        withTimeout(5_000) {
            while (dao.row(steer.id)?.state != DurableCommandState.COMPLETED.name) {
                kotlinx.coroutines.delay(10)
            }
        }
        assertEquals("steer", dao.row(steer.id)?.type)
        assertEquals(1, executions.get())

        runtime.close()
        scope.cancel()
    }

    @Test
    fun `applied steering remains editable until the active run finishes`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val runStarted = CompletableDeferred<Unit>()
        val allowCheckpoint = CompletableDeferred<Unit>()
        val checkpointApplied = CompletableDeferred<Unit>()
        val releaseRun = CompletableDeferred<Unit>()
        val activeRunId = CompletableDeferred<Uuid>()
        val persisted = CompletableDeferred<me.rerere.rikkahub.data.ai.SteeringNote>()
        val persistedCount = java.util.concurrent.atomic.AtomicInteger(0)
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { _, control ->
                activeRunId.complete(control.runId)
                runStarted.complete(Unit)
                allowCheckpoint.await()
                control.takeSteeringForCheckpoint(1).also(control::markSteeringProviderStarted).single()
                checkpointApplied.complete(Unit)
                releaseRun.await()
                RunOutcome.Completed()
            },
            onPersistSteering = { note ->
                persistedCount.incrementAndGet()
                persisted.complete(note)
            },
        )
        val message = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val steer = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = SteerCommand(
                text = "keep this after the task",
                scope = SteeringScope.NEXT_MODEL_CALL,
                historyMode = SteeringHistoryMode.TRANSIENT,
            ),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )

        assertTrue(runtime.enqueueEnvelope(message) is SubmitResult.Accepted)
        withTimeout(5_000) { runStarted.await() }
        assertTrue(runtime.enqueueEnvelope(steer) is SubmitResult.Accepted)
        allowCheckpoint.complete(Unit)
        withTimeout(5_000) { checkpointApplied.await() }
        assertEquals(CommandOutcome.Completed, steer.result.await())

        val appliedEntry = runtime.steeringEntries.value.getValue(steer.id)
        assertEquals(activeRunId.await(), appliedEntry.runId)
        assertEquals(me.rerere.rikkahub.data.ai.SteeringState.APPLIED, appliedEntry.state)
        assertTrue(appliedEntry.editable)
        assertTrue(runtime.updateSteeringHistoryMode(steer.id, SteeringHistoryMode.PERSISTENT))
        assertEquals(
            SteeringHistoryMode.PERSISTENT,
            runtime.steeringEntries.value.getValue(steer.id).historyMode,
        )
        assertTrue(runtime.updateSteeringHistoryMode(steer.id, SteeringHistoryMode.TRANSIENT))
        assertEquals(
            SteeringHistoryMode.TRANSIENT,
            runtime.steeringEntries.value.getValue(steer.id).historyMode,
        )
        assertTrue(runtime.updateSteeringHistoryMode(steer.id, SteeringHistoryMode.PERSISTENT))

        releaseRun.complete(Unit)
        assertEquals(CommandOutcome.Completed, message.result.await())
        assertEquals(steer.id, withTimeout(5_000) { persisted.await() }.commandId)
        assertEquals(1, persistedCount.get())
        assertTrue(steer.id !in runtime.steeringEntries.value)
        assertTrue(!runtime.updateSteeringHistoryMode(steer.id, SteeringHistoryMode.TRANSIENT))

        runtime.close()
        scope.cancel()
    }

    @Test
    fun `cancelling applied steering removes its card and prevents persistence`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val runStarted = CompletableDeferred<Unit>()
        val allowCheckpoint = CompletableDeferred<Unit>()
        val checkpointApplied = CompletableDeferred<Unit>()
        val releaseRun = CompletableDeferred<Unit>()
        val persistedCount = java.util.concurrent.atomic.AtomicInteger(0)
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { _, control ->
                runStarted.complete(Unit)
                allowCheckpoint.await()
                control.takeSteeringForCheckpoint(1).also(control::markSteeringProviderStarted)
                checkpointApplied.complete(Unit)
                releaseRun.await()
                RunOutcome.Completed()
            },
            onPersistSteering = { persistedCount.incrementAndGet() },
        )
        val message = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val steer = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = SteerCommand("forget this guidance"),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )
        val cancel = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = CancelSteeringCommand(steer.id),
            origin = CommandOrigin.APP_UI,
            sequence = 3,
        )

        assertTrue(runtime.enqueueEnvelope(message) is SubmitResult.Accepted)
        withTimeout(5_000) { runStarted.await() }
        assertTrue(runtime.enqueueEnvelope(steer) is SubmitResult.Accepted)
        allowCheckpoint.complete(Unit)
        withTimeout(5_000) { checkpointApplied.await() }
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { steer.result.await() })
        assertTrue(steer.id in runtime.steeringEntries.value)

        assertTrue(runtime.enqueueEnvelope(cancel) is SubmitResult.Accepted)
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { cancel.result.await() })
        assertTrue(steer.id !in runtime.steeringEntries.value)
        assertTrue(steer.id !in runtime.steeringStatus.value)

        releaseRun.complete(Unit)
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { message.result.await() })
        assertEquals(0, persistedCount.get())
        runtime.close()
        scope.cancel()
    }

    @Test
    fun `transient applied steering is written as audit history and removed from temporary UI`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val runStarted = CompletableDeferred<Unit>()
        val allowCheckpoint = CompletableDeferred<Unit>()
        val releaseRun = CompletableDeferred<Unit>()
        val persistedCount = java.util.concurrent.atomic.AtomicInteger(0)
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = Uuid.random(),
            executor = RuntimeCommandExecutor { _, control ->
                runStarted.complete(Unit)
                allowCheckpoint.await()
                control.takeSteeringForCheckpoint(1).also(control::markSteeringProviderStarted).single()
                releaseRun.await()
                RunOutcome.Completed()
            },
            onPersistSteering = { persistedCount.incrementAndGet() },
        )
        val message = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        )
        val steer = CommandEnvelope(
            conversationId = runtime.conversationId,
            command = SteerCommand(
                text = "only for this task",
                scope = SteeringScope.NEXT_MODEL_CALL,
                historyMode = SteeringHistoryMode.TRANSIENT,
            ),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        )

        assertTrue(runtime.enqueueEnvelope(message) is SubmitResult.Accepted)
        withTimeout(5_000) { runStarted.await() }
        assertTrue(runtime.enqueueEnvelope(steer) is SubmitResult.Accepted)
        allowCheckpoint.complete(Unit)
        assertEquals(CommandOutcome.Completed, steer.result.await())
        releaseRun.complete(Unit)
        assertEquals(CommandOutcome.Completed, message.result.await())
        assertEquals(1, persistedCount.get())
        assertTrue(steer.id !in runtime.steeringEntries.value)

        runtime.close()
        scope.cancel()
    }

    @Test
    fun `restored soft steering rewrites the same durable row into FIFO`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val dao = FakePendingChatCommandDao()
        val durableQueue = DurableCommandQueue(
            dao,
            workerId = "restore-steering-worker",
            commandStateTransaction = CommandStateTransaction(dao),
        )
        val conversationId = Uuid.random()
        val commandId = Uuid.random()
        val assistantId = Uuid.random()
        val branchAnchorMessageId = Uuid.random()
        val restoredText = "continue with this after restart"
        val encoded = CommandCodec.encode(
            SteerCommand(
                text = restoredText,
                scope = SteeringScope.REMAINDER_OF_RUN,
                applyPolicy = SteeringApplyPolicy.AFTER_CHECKPOINT,
                historyMode = SteeringHistoryMode.TRANSIENT,
            )
        )
        val restoredRow = me.rerere.rikkahub.data.db.entity.PendingChatCommandEntity(
            id = commandId.toString(),
            schemaVersion = 1,
            conversationId = conversationId.toString(),
            type = encoded.first,
            payloadJson = encoded.second,
            state = DurableCommandState.PENDING.name,
            priority = 0,
            sequence = 1,
            expectedTargetVersion = null,
            expectedBranchHeadMessageId = null,
            dedupeKey = null,
            idempotencyKey = commandId.toString(),
            attempt = 0,
            claimedBy = null,
            leaseUntil = null,
            createdAt = 0L,
            startedAt = null,
            finishedAt = null,
            expiresAt = null,
            lastErrorCode = null,
            lastErrorMessage = null,
            assistantIdSnapshot = assistantId.toString(),
            lineageId = commandId.toString(),
            parentCommandId = null,
            branchAnchorMessageId = branchAnchorMessageId.toString(),
            stateVersion = 0L,
        )
        assertEquals(DurableSubmitResult.Inserted(commandId), durableQueue.submitDurable(restoredRow))

        val executed = CompletableDeferred<String>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = conversationId,
            durableQueue = durableQueue,
            executor = RuntimeCommandExecutor { envelope, _ ->
                val text = (envelope.command as SendMessageCommand).content.parts
                    .filterIsInstance<UIMessagePart.Text>()
                    .single()
                    .text
                executed.complete(text)
                RunOutcome.Completed()
            },
        )

        assertEquals(restoredText, withTimeout(5_000) { executed.await() })
        withTimeout(5_000) {
            while (dao.row(commandId)?.state != DurableCommandState.COMPLETED.name) {
                kotlinx.coroutines.delay(10)
            }
        }
        val completedRow = dao.row(commandId)!!
        assertEquals(commandId.toString(), completedRow.id)
        assertEquals(commandId.toString(), completedRow.idempotencyKey)
        assertEquals("send_message", completedRow.type)
        assertEquals(1, dao.allRows().size)
        assertTrue(durableQueue.scanPending().isEmpty())
        assertTrue(commandId !in runtime.steeringEntries.value)

        runtime.close()
        scope.cancel()
    }

    @Test
    fun `steering without a later checkpoint rewrites the same durable row and runs once`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val started = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val executed = java.util.Collections.synchronizedList(mutableListOf<String>())
        val dao = FakePendingChatCommandDao()
        val durableQueue = DurableCommandQueue(
            dao,
            workerId = "fallback-worker",
            commandStateTransaction = CommandStateTransaction(dao),
        )
        val conversationId = Uuid.random()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = conversationId,
            durableQueue = durableQueue,
            executor = RuntimeCommandExecutor { envelope, _ ->
                val text = (envelope.command as SendMessageCommand).content.parts
                    .filterIsInstance<UIMessagePart.Text>()
                    .single()
                    .text
                executed += text
                if (text == "active") {
                    started.complete(Unit)
                    release.await()
                }
                RunOutcome.Completed()
            },
        )
        val message = CommandEnvelope(
            conversationId = conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        ).withRootLineage()
        val steer = CommandEnvelope(
            conversationId = conversationId,
            command = SteerCommand("too late"),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        ).withRootLineage()
        assertTrue(runtime.enqueueEnvelope(message) is SubmitResult.Accepted)
        withTimeout(5_000) { started.await() }
        assertTrue(runtime.enqueueEnvelope(steer) is SubmitResult.Accepted)
        assertEquals("steer", dao.row(steer.id)?.type)
        release.complete(Unit)
        assertEquals(CommandOutcome.Completed, steer.result.await())
        assertEquals(listOf("active", "too late"), executed)
        assertTrue(steer.id !in runtime.steeringEntries.value)

        withTimeout(5_000) {
            while (dao.row(steer.id)?.state != DurableCommandState.COMPLETED.name) {
                kotlinx.coroutines.delay(10)
            }
        }
        val fallbackRow = dao.row(steer.id)!!
        assertEquals(steer.id.toString(), fallbackRow.id)
        assertEquals(steer.id.toString(), fallbackRow.idempotencyKey)
        assertEquals("send_message", fallbackRow.type)
        assertEquals(2, dao.allRows().size)
        assertTrue(durableQueue.scanPending().isEmpty())

        runtime.close()
        scope.cancel()

        val restartScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val restarted = ConversationRuntime(
            appScope = restartScope,
            conversationId = conversationId,
            durableQueue = durableQueue,
            executor = RuntimeCommandExecutor { envelope, _ ->
                val text = (envelope.command as SendMessageCommand).content.parts
                    .filterIsInstance<UIMessagePart.Text>().single().text
                executed += "replayed:$text"
                RunOutcome.Completed()
            },
        )
        kotlinx.coroutines.delay(200)
        assertEquals(listOf("active", "too late"), executed)
        restarted.close()
        restartScope.cancel()
    }

    @Test
    fun `fallback rewrite failure is reported without crashing the runtime`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val started = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val dao = FakePendingChatCommandDao(
            rewriteFailure = IllegalStateException("rewrite unavailable"),
        )
        val durableQueue = DurableCommandQueue(
            dao,
            workerId = "fallback-failure-worker",
            commandStateTransaction = CommandStateTransaction(dao),
        )
        val conversationId = Uuid.random()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = conversationId,
            durableQueue = durableQueue,
            executor = RuntimeCommandExecutor { envelope, _ ->
                if (envelope.command is SendMessageCommand) {
                    started.complete(Unit)
                    release.await()
                }
                RunOutcome.Completed()
            },
        )
        val message = CommandEnvelope(
            conversationId = conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        ).withRootLineage()
        val steer = CommandEnvelope(
            conversationId = conversationId,
            command = SteerCommand("keep this recoverable"),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        ).withRootLineage()

        assertTrue(runtime.enqueueEnvelope(message) is SubmitResult.Accepted)
        withTimeout(5_000) { started.await() }
        assertTrue(runtime.enqueueEnvelope(steer) is SubmitResult.Accepted)
        release.complete(Unit)

        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { message.result.await() })
        assertTrue(withTimeout(5_000) { steer.result.await() } is CommandOutcome.Failed)
        withTimeout(5_000) {
            while (dao.row(steer.id)?.state != DurableCommandState.MANUAL_CONFIRMATION.name) {
                kotlinx.coroutines.delay(10)
            }
        }
        val failedEntry = runtime.steeringEntries.value.getValue(steer.id)
        assertEquals(
            me.rerere.rikkahub.data.ai.SteeringState.REJECTED_NOT_STEERABLE,
            failedEntry.state,
        )
        assertTrue(!failedEntry.editable)
        assertTrue(runtime.runtimeState.value !is RuntimeState.Fatal)

        runtime.close()
        scope.cancel()
    }

    @Test
    fun `fallback rewrite conflict pauses without publishing a false terminal result`() = runBlocking {
        assertRewriteFailureTerminalizationIsFenced(
            finishUnclaimedResultOverride = 0,
        )
    }

    @Test
    fun `fallback rewrite terminal exception pauses without publishing a false terminal result`() = runBlocking {
        assertRewriteFailureTerminalizationIsFenced(
            finishUnclaimedFailure = IllegalStateException("terminal unavailable"),
        )
    }

    private suspend fun assertRewriteFailureTerminalizationIsFenced(
        finishUnclaimedFailure: Throwable? = null,
        finishUnclaimedResultOverride: Int? = null,
    ) {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val started = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val executions = java.util.concurrent.atomic.AtomicInteger(0)
        val dao = FakePendingChatCommandDao(
            rewriteFailure = IllegalStateException("rewrite unavailable"),
            finishUnclaimedFailure = finishUnclaimedFailure,
            finishUnclaimedResultOverride = finishUnclaimedResultOverride,
        )
        val transaction = CommandStateTransaction(dao)
        val durableQueue = DurableCommandQueue(
            dao,
            workerId = "fallback-terminal-fence-worker",
            commandStateTransaction = transaction,
        )
        val conversationId = Uuid.random()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = conversationId,
            durableQueue = durableQueue,
            executor = RuntimeCommandExecutor { envelope, _ ->
                executions.incrementAndGet()
                if (envelope.command is SendMessageCommand) {
                    started.complete(Unit)
                    release.await()
                }
                RunOutcome.Completed()
            },
        )
        val message = CommandEnvelope(
            conversationId = conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        ).withRootLineage()
        val steer = CommandEnvelope(
            conversationId = conversationId,
            command = SteerCommand("keep the durable authority"),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        ).withRootLineage()

        assertTrue(runtime.enqueueEnvelope(message) is SubmitResult.Accepted)
        withTimeout(5_000) { started.await() }
        assertTrue(runtime.enqueueEnvelope(steer) is SubmitResult.Accepted)
        release.complete(Unit)
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { message.result.await() })
        withTimeout(5_000) {
            while (runtime.runtimeState.value != RuntimeState.Paused) {
                kotlinx.coroutines.delay(10)
            }
        }

        assertEquals(DurableCommandState.PENDING.name, dao.row(steer.id)?.state)
        assertFalse(steer.result.isCompleted)
        assertEquals(1, executions.get())

        runtime.close()
        scope.cancel()
    }

    @Test
    fun `fallback rewrite exact manual confirmation duplicate completes idempotently`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val started = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val dao = FakePendingChatCommandDao(
            rewriteFailure = IllegalStateException("rewrite unavailable"),
        )
        val transaction = CommandStateTransaction(dao)
        val durableQueue = DurableCommandQueue(
            dao,
            workerId = "fallback-terminal-duplicate-worker",
            commandStateTransaction = transaction,
        )
        val conversationId = Uuid.random()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = conversationId,
            durableQueue = durableQueue,
            executor = RuntimeCommandExecutor { envelope, _ ->
                if (envelope.command is SendMessageCommand) {
                    started.complete(Unit)
                    release.await()
                }
                RunOutcome.Completed()
            },
        )
        val message = CommandEnvelope(
            conversationId = conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        ).withRootLineage()
        val steer = CommandEnvelope(
            conversationId = conversationId,
            command = SteerCommand("already parked"),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        ).withRootLineage()

        assertTrue(runtime.enqueueEnvelope(message) is SubmitResult.Accepted)
        withTimeout(5_000) { started.await() }
        assertTrue(runtime.enqueueEnvelope(steer) is SubmitResult.Accepted)
        assertTrue(
            transaction.finishUnclaimed(
                id = steer.id,
                terminal = DurableCommandState.MANUAL_CONFIRMATION,
                errorCode = "STEERING_REWRITE_FAILED",
            ) is CommandTransitionResult.Applied,
        )
        release.complete(Unit)

        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { message.result.await() })
        assertTrue(withTimeout(5_000) { steer.result.await() } is CommandOutcome.Failed)
        assertEquals(DurableCommandState.MANUAL_CONFIRMATION.name, dao.row(steer.id)?.state)
        assertTrue(runtime.runtimeState.value !is RuntimeState.Fatal)

        runtime.close()
        scope.cancel()
    }

    @Test
    fun `applied steering durable row is terminal and is not restored`() = runBlocking {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val dao = FakePendingChatCommandDao()
        val durableQueue = DurableCommandQueue(
            dao,
            workerId = "applied-steering-worker",
            commandStateTransaction = CommandStateTransaction(dao),
        )
        val conversationId = Uuid.random()
        val runStarted = CompletableDeferred<Unit>()
        val applyCheckpoint = CompletableDeferred<Unit>()
        val releaseRun = CompletableDeferred<Unit>()
        val runtime = ConversationRuntime(
            appScope = scope,
            conversationId = conversationId,
            durableQueue = durableQueue,
            executor = RuntimeCommandExecutor { envelope, control ->
                if (envelope.command is SendMessageCommand) {
                    runStarted.complete(Unit)
                    applyCheckpoint.await()
                    control.takeSteeringForCheckpoint(1).also(control::markSteeringProviderStarted).single()
                    releaseRun.await()
                }
                RunOutcome.Completed()
            },
        )
        val message = CommandEnvelope(
            conversationId = conversationId,
            command = messageCommand("active"),
            origin = CommandOrigin.APP_UI,
            sequence = 1,
        ).withRootLineage()
        val steer = CommandEnvelope(
            conversationId = conversationId,
            command = SteerCommand("apply exactly once"),
            origin = CommandOrigin.APP_UI,
            sequence = 2,
        ).withRootLineage()

        assertTrue(runtime.enqueueEnvelope(message) is SubmitResult.Accepted)
        withTimeout(5_000) { runStarted.await() }
        assertTrue(runtime.enqueueEnvelope(steer) is SubmitResult.Accepted)
        applyCheckpoint.complete(Unit)
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { steer.result.await() })
        withTimeout(5_000) {
            while (dao.row(steer.id)?.state != DurableCommandState.COMPLETED.name) {
                kotlinx.coroutines.delay(10)
            }
        }
        releaseRun.complete(Unit)
        assertEquals(CommandOutcome.Completed, withTimeout(5_000) { message.result.await() })
        assertTrue(durableQueue.scanPending().isEmpty())
        runtime.close()
        scope.cancel()

        val restartScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val replayed = java.util.concurrent.atomic.AtomicInteger(0)
        val restarted = ConversationRuntime(
            appScope = restartScope,
            conversationId = conversationId,
            durableQueue = durableQueue,
            executor = RuntimeCommandExecutor { _, _ ->
                replayed.incrementAndGet()
                RunOutcome.Completed()
            },
        )
        kotlinx.coroutines.delay(200)
        assertEquals(0, replayed.get())
        restarted.close()
        restartScope.cancel()
    }
}
