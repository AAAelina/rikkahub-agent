package me.rerere.rikkahub.skills

import android.util.Log
import androidx.core.net.toUri
import me.rerere.rikkahub.data.files.FilesManager
import me.rerere.rikkahub.data.files.FileFolders
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.coroutines.Deferred
import me.rerere.rikkahub.service.chat.CommandOutcome
import me.rerere.rikkahub.service.chat.CommandOrigin
import me.rerere.rikkahub.service.chat.SubmitResult
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withTimeoutOrNull
import me.rerere.ai.ui.UIMessagePart
import me.rerere.rikkahub.data.ai.tools.HeadlessConversations
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.data.datastore.getCurrentAssistant
import me.rerere.rikkahub.data.files.SkillManager
import me.rerere.rikkahub.data.model.Conversation
import me.rerere.rikkahub.data.repository.ConversationRepository
import me.rerere.rikkahub.data.repository.ConversationDeletionResult
import me.rerere.rikkahub.service.ChatService
import java.io.IOException
import kotlin.uuid.Uuid

/**
 * Phase 19B — run a single skill against a user-supplied prompt in isolation, harvest the
 * model's final text reply, return.
 *
 * The runner:
 *  1. reads the skill's body via [SkillManager.readSkillBody] (or the test-injected
 *     [skillBodyReader] seam)
 *  2. creates a fresh, ephemeral conversation under the user's currently-selected assistant
 *  3. registers the conversation id in [HeadlessConversations] BEFORE tracked submission
 *     fires (so the sub-agent recursion guard sees this run as headless and per-tool approval
 *     auto-grants for tools the user has Always-Allowed)
 *  4. dispatches the test prompt + the skill body inlined as the user message
 *  5. waits up to [timeoutMs] (default 2 min) for the authoritative command outcome
 *  6. harvests the last assistant message's text + image parts
 *  7. after quiescence, drops the session, deletes it from Room, then unregisters it
 *
 * The skill body is inlined directly into the test prompt (rather than relying on the
 * `use_skill` tool surface) so the tester works regardless of whether the skill is in
 * `assistant.enabledSkills` — it's a pure "what would this prompt + this body produce"
 * smoke test.
 *
 * Hard timeout: 2 minutes by default (overridable for tests). If the generation hasn't
 * settled by then, the run returns [TestRunState.Error] with code `tester_timeout` and
 * requests stop and waits for quiescence. Unconfirmed termination retains the conversation.
 *
 * Testability seams: the [Driver] interface abstracts everything the runner needs from
 * the `ChatService` + `ConversationRepository` + `SettingsStore` triplet. JVM tests
 * supply a fake driver; production wires through [defaultDriver] which delegates to the
 * Koin-provided real services. This avoids pulling Robolectric or a mocking framework
 * into the test module just for tester coverage.
 */
class SkillTestRunner(
    private val driver: Driver,
    private val skillBodyReader: (String) -> String?,
    private val timeoutMs: Long = DEFAULT_TIMEOUT_MS,
    private val cleanupTimeoutMs: Long = 5_000L,
) {
    init {
        require(cleanupTimeoutMs > 0) { "Cleanup timeout must be positive" }
    }

    /** Production Koin convenience constructor — equivalent to passing [defaultDriver]. */
    constructor(
        chatService: ChatService,
        skillManager: SkillManager,
        conversationRepo: ConversationRepository,
        settingsStore: SettingsStore,
        filesManager: FilesManager,
        timeoutMs: Long = DEFAULT_TIMEOUT_MS,
    ) : this(
        driver = defaultDriver(chatService, conversationRepo, settingsStore, filesManager),
        skillBodyReader = { name -> skillManager.readSkillBody(name) },
        timeoutMs = timeoutMs,
    )

    companion object {
        private const val TAG = "SkillTestRunner"
        const val DEFAULT_TIMEOUT_MS: Long = 2 * 60 * 1_000L

        /** Wire the production [Driver] over the real ChatService + Repository + Settings. */
        fun defaultDriver(
            chatService: ChatService,
            conversationRepo: ConversationRepository,
            settingsStore: SettingsStore,
            filesManager: FilesManager,
        ): Driver = object : Driver {
            override suspend fun currentAssistantId(): Uuid =
                settingsStore.settingsFlow.first().getCurrentAssistant().id

            override suspend fun startConversation(conv: Conversation) {
                conversationRepo.insertConversation(conv)
                chatService.initializeConversation(conv.id)
            }

            override suspend fun submit(conv: Conversation, parts: List<UIMessagePart>): Submission {
                val tracked = chatService.submitUserMessageTracked(conv.id, parts, origin = CommandOrigin.APP_UI)
                return Submission((tracked.submission as? SubmitResult.Accepted)?.commandId, tracked.outcome)
            }

            override suspend fun stopAndAwaitQuiescence(conversationId: Uuid, commandId: Uuid?): Boolean =
                chatService.stopAndAwaitQuiescence(conversationId, commandId, 5_000L)

            override suspend fun harvest(conversationId: Uuid): HarvestResult {
                val conv = conversationRepo.getConversationById(conversationId)
                    ?: return HarvestResult("", emptyList())
                val selected = conv.messageNodes.mapNotNull { node ->
                    node.messages.getOrNull(node.selectIndex)
                }
                val lastAssistant = selected.lastOrNull {
                    it.role.name.equals("assistant", ignoreCase = true)
                } ?: return HarvestResult("", emptyList())
                val text = lastAssistant.parts
                    .filterIsInstance<UIMessagePart.Text>()
                    .joinToString("\n") { it.text }
                    .trim()
                val images = lastAssistant.parts
                    .filterIsInstance<UIMessagePart.Image>()
                    .map { it.url }
                val retained = retainSkillResultImages(images) { url ->
                    val entity = if (url.startsWith("file:", ignoreCase = true)) {
                        filesManager.saveManagedFromFile(FileFolders.TOOL_OUTPUTS, java.io.File(java.net.URI(url)))
                    } else {
                        filesManager.saveManagedFromUri(FileFolders.TOOL_OUTPUTS, url.toUri())
                    }
                    val file = filesManager.getFile(entity)
                    check(file.isFile && file.canRead()) { "Skill artifact retention failed" }
                    file.toURI().toString()
                }
                return HarvestResult(text, retained)
            }

            override suspend fun cleanup(conv: Conversation) {
                chatService.dropSession(conv.id)
                when (conversationRepo.deleteConversation(conv)) {
                    is ConversationDeletionResult.Deleted, is ConversationDeletionResult.Missing -> Unit
                    is ConversationDeletionResult.RetainedSecondUser -> error("Skill conversation deletion protected")
                }
            }
        }
    }

    /**
     * Narrow seam over ChatService + ConversationRepository + SettingsStore. Tests supply a
     * fake; production uses [defaultDriver]. Allows pure-JVM coverage without Robolectric.
     */
    interface Driver {
        suspend fun currentAssistantId(): Uuid
        suspend fun startConversation(conv: Conversation)
        suspend fun submit(conv: Conversation, parts: List<UIMessagePart>): Submission
        suspend fun stopAndAwaitQuiescence(conversationId: Uuid, commandId: Uuid?): Boolean
        suspend fun harvest(conversationId: Uuid): HarvestResult
        /** Return only after deletion is confirmed; throw on failure or policy retention. */
        suspend fun cleanup(conv: Conversation)
    }

    data class Submission(val commandId: Uuid?, val outcome: Deferred<CommandOutcome>)

    data class HarvestResult(val text: String, val imageUrls: List<String>)

    sealed class TestRunState {
        data object Idle : TestRunState()
        data class Running(val elapsedMs: Long) : TestRunState()
        data class Done(val text: String, val imageUrls: List<String>) : TestRunState()
        data class Error(val error: String, val detail: String?) : TestRunState()
    }

    /**
     * Run the skill once. Returns a cold [Flow] that emits [TestRunState.Running] when the
     * generation kicks off, then exactly one terminal state ([TestRunState.Done] or
     * [TestRunState.Error]). The flow terminates after the terminal state is emitted.
     */
    fun runOnce(skillName: String, prompt: String): Flow<TestRunState> = flow {
        // readSkillBody now enforces a size cap and throws SkillFileTooLargeException (an
        // IOException) for oversized files; reads can also fail with a plain IOException.
        // Catch both here so the body read surfaces a clean terminal Error instead of an
        // unhandled exception escaping the flow.
        val skillBody = try {
            skillBodyReader(skillName)
        } catch (e: SkillManager.SkillFileTooLargeException) {
            emit(TestRunState.Error("skill_too_large", "skill body exceeds the size cap (${e.lengthBytes} bytes)"))
            return@flow
        } catch (e: IOException) {
            emit(TestRunState.Error("read_failed", e.message ?: "skill body could not be read"))
            return@flow
        }
        if (skillBody.isNullOrBlank()) {
            emit(TestRunState.Error("missing_skill", "skill body could not be read"))
            return@flow
        }
        if (prompt.isBlank()) {
            emit(TestRunState.Error("empty_prompt", "prompt is empty"))
            return@flow
        }

        emit(TestRunState.Running(0L))

        val assistantId = driver.currentAssistantId()
        val conv = Conversation.ofId(
            id = Uuid.random(),
            assistantId = assistantId,
            newConversation = true,
        ).copy(title = "[Skill test] $skillName")

        // Record recovery ownership before any setup side effect. Insertion may succeed
        // even when session initialization fails; mark() may also partially succeed.
        var registered = false
        var submission: Submission? = null
        var quiescent = false
        var harvestIncomplete = false
        try {
            registered = true
            HeadlessConversations.mark(conv.id)
            driver.startConversation(conv)

            val composed = buildString {
                appendLine("You are running the skill below in test mode. Apply it to the user prompt and respond as the skill instructs.")
                appendLine()
                appendLine("---- SKILL ($skillName) ----")
                appendLine(skillBody)
                appendLine("---- END SKILL ----")
                appendLine()
                appendLine("User prompt:")
                append(prompt)
            }
            val tracked = driver.submit(conv, listOf(UIMessagePart.Text(composed)))
            submission = tracked
            val outcome = withTimeoutOrNull(timeoutMs) { tracked.outcome.await() }
            if (outcome == null) {
                quiescent = driver.stopAndAwaitQuiescence(conv.id, tracked.commandId)
                emit(TestRunState.Error("tester_timeout", "exceeded ${timeoutMs / 1_000}s cap; quiescent=$quiescent"))
                return@flow
            }
            if (outcome != CommandOutcome.Completed) {
                val code = when (outcome) {
                    CommandOutcome.Cancelled, is CommandOutcome.Superseded -> "tester_cancelled"
                    else -> "tester_failed"
                }
                emit(TestRunState.Error(code, outcome.toString()))
                return@flow
            }
            quiescent = driver.stopAndAwaitQuiescence(conv.id, tracked.commandId)
            if (!quiescent) {
                emit(TestRunState.Error("termination_unconfirmed", "conversation retained"))
                return@flow
            }

            harvestIncomplete = true
            val harvested = driver.harvest(conv.id)
            harvestIncomplete = false
            if (harvested.text.isBlank() && harvested.imageUrls.isEmpty()) {
                emit(TestRunState.Error("no_response", "the model returned no text or image parts"))
            } else {
                emit(TestRunState.Done(harvested.text, harvested.imageUrls))
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (t: Exception) {
            // Log via a runCatching so JVM tests (which don't stub android.util.Log) don't
            // explode. The error envelope below carries the same info to the UI.
            runCatching { Log.w(TAG, "runOnce failed for $skillName", t) }
            emit(TestRunState.Error(t::class.simpleName ?: "unknown", t.message))
        } finally {
            withContext(NonCancellable) {
                try {
                    val cleaned = withTimeoutOrNull(cleanupTimeoutMs) {
                        if (!quiescent) {
                            quiescent = driver.stopAndAwaitQuiescence(conv.id, submission?.commandId)
                        }
                        if (quiescent && !harvestIncomplete) {
                            driver.cleanup(conv)
                            if (registered) HeadlessConversations.unmark(conv.id)
                            true
                        } else false
                    } ?: false
                    if (!cleaned) runCatching { Log.w(TAG, "Retaining skill conversation for recovery: ${conv.id}") }
                } catch (failure: Exception) {
                    // Cleanup must not replace cancellation or an already delivered result.
                    // Leave the recovery marker until deletion has actually succeeded.
                    runCatching { Log.w(TAG, "Skill conversation cleanup failed: ${conv.id}", failure) }
                }
                // Retention is intentional: dropSession is cancellation, never a join.
            }
        }
    }
}

/** Local results gain an independent managed-file owner before ephemeral conversation deletion. */
internal suspend fun retainSkillResultImages(
    urls: List<String>,
    retain: suspend (String) -> String,
): List<String> = urls.map { url ->
    if (url.startsWith("file:", ignoreCase = true) || url.startsWith("content:", ignoreCase = true)) retain(url)
    else url
}
