package me.rerere.rikkahub.automation

import android.content.Context
import android.content.Intent
import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import me.rerere.ai.ui.UIMessagePart
import me.rerere.rikkahub.AppScope
import me.rerere.rikkahub.data.agentrun.AgentRunKind
import me.rerere.rikkahub.data.agentrun.AgentRunRepository
import me.rerere.rikkahub.data.agentrun.AgentRunStatus
import me.rerere.rikkahub.data.ai.tools.HeadlessConversations
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.data.datastore.getCurrentAssistant
import me.rerere.rikkahub.data.model.Conversation
import me.rerere.rikkahub.data.repository.ConversationRepository
import me.rerere.rikkahub.service.ChatService
import me.rerere.rikkahub.service.chat.CommandOrigin
import me.rerere.rikkahub.service.chat.CommandOutcome
import me.rerere.rikkahub.service.chat.SubmitResult
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.uuid.Uuid

/**
 * Single dispatch point shared by [ExternalAutomationActivity] (for app-callers via Activity
 * target) and [ExternalAutomationReceiver] (for ADB callers).
 *
 * Two intent actions are recognised:
 *  - `me.rerere.rikkahub.RUN_TASK` — fire-and-forget headless run. The provided prompt
 *    runs against the user's current assistant in a fresh conversation marked headless
 *    (auto-approves tools at fire time, HARDLINE still applies). If the caller provided
 *    `return_action` + `return_package`, the dispatcher posts callback broadcasts:
 *    `accepted` immediately and a terminal `completed` / `failed` / `cancelled` /
 *    `blocked` / `rejected` broadcast when the run finishes.
 *  - `me.rerere.rikkahub.RUN_CHAT` — interactive. The activity opens the in-app chat with
 *    the prompt pre-filled but does NOT auto-send it. (Activity handles this directly —
 *    the dispatcher only handles RUN_TASK semantics.)
 *
 * Trust model:
 *  1. If `enabled` flag is OFF → reject every call with `rejected:disabled`.
 *  2. If caller package is in `trustedPackages` → run.
 *  3. Otherwise → return `pending_user_approval`. (For v1, the Activity surface shows an
 *     in-app dialog before continuing; the broadcast-receiver path silently rejects with
 *     `rejected:untrusted_caller` because we cannot show UI from a manifest-registered
 *     receiver running cold.)
 *
 * Extras keys (case-sensitive, matching spec):
 *   - `task` (string) or `task_b64` (Base64-NO_WRAP) — the prompt for RUN_TASK
 *   - `chat` (string) or `chat_b64` — the prompt for RUN_CHAT
 *   - `request_id` (string) — caller correlation id, echoed in callback
 *   - `return_action` (string) — broadcast action for callback
 *   - `return_package` (string) — package to deliver callback to
 *
 * Result codes (in callback broadcast `extra("status", String)`):
 *   - `accepted` — dispatcher acknowledgement (runtime admission may still reject)
 *   - `completed` — generation finished without error
 *   - `failed` — generation threw or returned an error message
 *   - `cancelled` — user cancelled mid-run
 *   - `blocked` — HARDLINE blocked a tool call inside the headless run
 *   - `rejected` — caller failed the trust gate
 */
class ExternalAutomationDispatcher(
    private val context: Context,
    private val config: ExternalAutomationConfig,
    private val chatService: ChatService,
    private val conversationRepo: ConversationRepository,
    private val settingsStore: SettingsStore,
    private val appScope: AppScope,
    /**
     * Phase 24 — unified AgentRun ledger writer. Every external-automation RUN_TASK is an
     * autonomous run that survives only as long as the process: writing it to the ledger
     * means a kill mid-run is reconciled to `process_lost` on next start. No DI-cycle risk.
     */
    private val agentRunRepo: AgentRunRepository,
) {

    sealed class TrustResult {
        data object Trusted : TrustResult()
        data object PendingUserApproval : TrustResult()
        data object Disabled : TrustResult()
    }

    suspend fun classifyCaller(callerPackage: String?): TrustResult {
        if (!config.isEnabled()) return TrustResult.Disabled
        if (callerPackage.isNullOrBlank()) return TrustResult.PendingUserApproval
        return if (callerPackage in config.trustedPackages()) TrustResult.Trusted
        else TrustResult.PendingUserApproval
    }

    /**
     * Dispatch a RUN_TASK intent. Pre-condition: caller is trusted (or user just approved
     * via the activity dialog). Logs the invocation, kicks off the generation in a
     * background coroutine, and posts callback broadcasts if [returnAction]/[returnPackage]
     * were provided.
     *
     * Returns a stable status string the caller can show / log immediately. The actual
     * generation runs async and produces its terminal status via the callback path.
     */
    suspend fun dispatchTask(
        prompt: String,
        callerPackage: String,
        requestId: String?,
        returnAction: String?,
        returnPackage: String?,
    ): String {
        val parsedPrompt = prompt.trim()
        if (parsedPrompt.isEmpty()) {
            log(callerPackage, "RUN_TASK", "rejected:empty_prompt", requestId)
            sendCallback(returnAction, returnPackage, requestId, "rejected", "empty prompt")
            return "rejected:empty_prompt"
        }

        // Dedup: Tasker / MacroDroid retries can re-fire the same intent multiple times in
        // quick succession (e.g. on display-on triggers that fire twice during the wake
        // settle). Without this, every retry would spawn a fresh conversation + LLM run.
        // Keyed by (callerPackage, requestId) — a blank requestId opts out of dedup since
        // the caller did not tag the invocation. Window is conservative (60 s) and entries
        // are pruned lazily on every dispatchTask call.
        if (!requestId.isNullOrBlank()) {
            val dedupKey = callerPackage + "\u0000" + requestId
            val nowMs = android.os.SystemClock.elapsedRealtime()
            pruneRecentRequestIds(nowMs)
            val seenAt = recentRequestIds[dedupKey]
            if (seenAt != null && nowMs - seenAt < REQUEST_ID_DEDUP_WINDOW_MS) {
                log(callerPackage, "RUN_TASK", "deduped:request_id_replay", requestId)
                sendCallback(returnAction, returnPackage, requestId, "rejected", "duplicate request_id within ${REQUEST_ID_DEDUP_WINDOW_MS / 1000}s window")
                return "rejected:duplicate_request_id"
            }
            recentRequestIds[dedupKey] = nowMs
        }

        sendCallback(returnAction, returnPackage, requestId, "accepted", null)
        // Telemetry must never strand an already-accepted task before dispatch.
        try {
            log(callerPackage, "RUN_TASK", "accepted", requestId)
        } catch (cancelled: CancellationException) {
            withContext(NonCancellable) {
                sendCallback(returnAction, returnPackage, requestId, "cancelled", "dispatch cancelled")
            }
            throw cancelled
        } catch (error: Exception) {
            Log.w(TAG, "External task accepted but audit log failed", error)
        }

        val entered = java.util.concurrent.atomic.AtomicBoolean(false)
        appScope.launch(Dispatchers.IO) {
            entered.set(true)
            runHeadless(parsedPrompt, requestId, returnAction, returnPackage)
        }.invokeOnCompletion {
            if (!entered.get()) sendCallback(returnAction, returnPackage, requestId, "cancelled", "dispatch scope unavailable")
        }

        return "accepted"
    }

    private suspend fun runHeadless(
        prompt: String,
        requestId: String?,
        returnAction: String?,
        returnPackage: String?,
    ) {
        // Even settings lookup, DB insertion, session initialization, headless marking and
        // ledger opening can throw AFTER the dispatcher has acknowledged "accepted".
        // The whole setup must be inside the terminal catch/finally, not just generation.
        val setup = ExternalAutomationSetupGuard()
        var ledgerId: String? = null
        var tracked: me.rerere.rikkahub.service.TrackedCommandSubmission? = null
        // Own the identity before admission: a durable enqueue can outlive a failed return.
        val submittedCommandId = Uuid.random()
        var submissionAttempted = false
        var quiescent = false
        var inserted = false
        var createdConversation: Conversation? = null
        val terminalReporter = ExternalAutomationTerminalReporter()
        suspend fun reportTerminalOnce(
            status: String,
            ledgerStatus: AgentRunStatus,
            ledgerDetail: String?,
            callbackDetail: String?,
        ) {
            withContext(NonCancellable) {
                terminalReporter.reportOnce(
                    status = status,
                    ledgerStatus = ledgerStatus,
                    ledgerDetail = ledgerDetail,
                    callbackDetail = callbackDetail,
                    persist = { outcome, detail ->
                        ledgerId?.let { agentRunRepo.markTerminal(it, outcome, detail) }
                    },
                    callback = { phase, detail ->
                        sendCallback(returnAction, returnPackage, requestId, phase, detail)
                    },
                    onLedgerFailure = { error ->
                        Log.w(TAG, "External automation terminal ledger failed; preserving callback", error)
                    },
                )
            }
        }
        try {
            ledgerId = setup.prepare(
                allocate = {
                    val assistant = withContext(Dispatchers.IO) {
                        settingsStore.settingsFlow.first { !it.init }.getCurrentAssistant()
                    }
                    createdConversation = Conversation.ofId(
                        id = Uuid.random(), assistantId = assistant.id, newConversation = true,
                    ).copy(title = "[External] ${prompt.take(40).ifBlank { "(empty)" }}")
                    requireNotNull(createdConversation).id
                },
                persist = {
                    // Insert may commit before a later post-commit projection throws.
                    inserted = true
                    conversationRepo.insertConversation(requireNotNull(createdConversation))
                },
                initialize = { chatService.initializeConversation(it) },
                mark = { HeadlessConversations.mark(it) },
                openLedger = { id ->
                    agentRunRepo.open(
                        kind = AgentRunKind.ExternalAutomation,
                        domainId = requestId?.takeIf { it.isNotBlank() } ?: id.toString(),
                        metadata = buildJsonObject {
                            put("conversation_id", id.toString())
                            if (!requestId.isNullOrBlank()) put("request_id", requestId)
                        },
                    )
                },
            )
            val conversationId = requireNotNull(setup.conversationId)
            // enqueueEnvelope may commit before submitUserMessageTracked returns (or throws).
            // A null tracked handle alone cannot prove that there is no live command.
            submissionAttempted = true
            val submission = chatService.submitUserMessageTracked(
                conversationId, listOf(UIMessagePart.Text(prompt)), origin = CommandOrigin.EXTERNAL_AUTOMATION,
                commandId = submittedCommandId,
            )
            tracked = submission
            val outcome = awaitExternalCommandOutcome(submission.outcome, 15L * 60_000L) {
                quiescent = chatService.stopAndAwaitQuiescence(
                    conversationId, (submission.submission as? SubmitResult.Accepted)?.commandId ?: submittedCommandId, 5_000L)
            }
            val terminal = outcome?.externalTerminal()
            reportTerminalOnce(
                terminal?.first ?: "failed", terminal?.second ?: AgentRunStatus.failed,
                if (outcome == null) "timeout; quiescent=$quiescent" else null,
                if (outcome == null) "generation timeout; termination may be unconfirmed" else null,
            )
        } catch (cancelled: CancellationException) {
            withContext(NonCancellable) {
                quiescent = try {
                    setup.finish(!submissionAttempted,
                        stop = { chatService.stopAndAwaitQuiescence(it,
                            (tracked?.submission as? SubmitResult.Accepted)?.commandId ?: submittedCommandId, 5_000L) },
                        unmark = HeadlessConversations::unmark)
                } catch (cleanupFailure: Exception) {
                    Log.w(TAG, "external automation cancellation cleanup failed", cleanupFailure)
                    false
                }
                val terminal = withTimeoutOrNull(1_000L) { tracked?.outcome?.await() }?.externalTerminal()
                reportTerminalOnce(
                    terminal?.first ?: "failed", terminal?.second ?: AgentRunStatus.failed,
                    if (terminal == null) "caller cancelled; command termination unconfirmed" else null,
                    if (terminal == null) "command termination unconfirmed" else null,
                )
            }
            throw cancelled
        } catch (t: Exception) {
            Log.w(TAG, "external automation run failed", t)
            withContext(NonCancellable) {
                quiescent = try {
                    setup.finish(!submissionAttempted,
                        stop = { chatService.stopAndAwaitQuiescence(it,
                            (tracked?.submission as? SubmitResult.Accepted)?.commandId ?: submittedCommandId, 5_000L) },
                        unmark = HeadlessConversations::unmark)
                } catch (cleanupFailure: Exception) {
                    Log.w(TAG, "external automation exception cleanup failed", cleanupFailure)
                    false
                }
                reportTerminalOnce("failed", AgentRunStatus.failed,
                    "${t::class.simpleName}: ${t.message.orEmpty()}", t.message)
            }
        } finally {
            withContext(NonCancellable) {
                try {
                    quiescent = setup.finish(quiescent || !submissionAttempted,
                        stop = { chatService.stopAndAwaitQuiescence(it,
                            (tracked?.submission as? SubmitResult.Accepted)?.commandId ?: submittedCommandId, 5_000L) },
                        unmark = HeadlessConversations::unmark)
                } catch (cleanupFailure: Exception) {
                    Log.w(TAG, "external automation final cleanup failed", cleanupFailure)
                }
                // An unconfirmed active run must retain its recursion/security marker.
                if (!quiescent && setup.marked) {
                    Log.w(TAG, "Retaining headless marker: termination unconfirmed for ${setup.conversationId}")
                }
                // No command was submitted. This is exclusively our newly created empty
                // conversation; never remove a session whose termination is unconfirmed.
                if (quiescent && inserted && !submissionAttempted) {
                    try {
                        val orphan = requireNotNull(createdConversation)
                        chatService.dropSession(orphan.id)
                        conversationRepo.deleteConversation(orphan)
                    } catch (cleanupFailure: Exception) {
                        Log.w(TAG, "external automation failed-setup conversation cleanup failed", cleanupFailure)
                    }
                }
            }
        }
    }

    /** Convenience for the receiver path that already has classified rejection status. */
    suspend fun rejectAndCallback(
        callerPackage: String,
        action: String,
        requestId: String?,
        returnAction: String?,
        returnPackage: String?,
        reason: String,
    ) {
        log(callerPackage, action, "rejected:$reason", requestId)
        sendCallback(returnAction, returnPackage, requestId, "rejected", reason)
    }

    private fun sendCallback(
        returnAction: String?,
        returnPackage: String?,
        requestId: String?,
        status: String,
        message: String?,
    ) {
        if (returnAction.isNullOrBlank() || returnPackage.isNullOrBlank()) return
        val intent = Intent(returnAction).apply {
            setPackage(returnPackage)
            putExtra(EXTRA_STATUS, status)
            putExtra(EXTRA_REQUEST_ID, requestId.orEmpty())
            if (!message.isNullOrBlank()) putExtra(EXTRA_MESSAGE, message)
            // FLAG_INCLUDE_STOPPED_PACKAGES so the callback reaches Tasker even when it's
            // currently background-frozen (Tasker has the manifest-receiver path enabled,
            // but stopped-package state can still drop the broadcast otherwise).
            addFlags(Intent.FLAG_INCLUDE_STOPPED_PACKAGES)
        }
        runCatching { context.sendBroadcast(intent) }.onFailure {
            Log.w(TAG, "callback broadcast failed", it)
        }
    }

    private suspend fun log(
        callerPackage: String,
        action: String,
        status: String,
        requestId: String?,
    ) {
        config.logInvocation(
            ExternalAutomationConfig.InvocationLog(
                timestampMs = System.currentTimeMillis(),
                callerPackage = callerPackage,
                action = action,
                status = status,
                requestId = requestId,
            )
        )
    }

    // ---- Request-id dedup state ----

    private val recentRequestIds = java.util.concurrent.ConcurrentHashMap<String, Long>()

    /** Drop dedup entries older than the window. Called on every dispatchTask so a
     *  one-shot caller doesn't leave entries piling up forever. */
    private fun pruneRecentRequestIds(nowMs: Long) {
        if (recentRequestIds.isEmpty()) return
        val cutoff = nowMs - REQUEST_ID_DEDUP_WINDOW_MS
        val it = recentRequestIds.entries.iterator()
        while (it.hasNext()) {
            if (it.next().value < cutoff) it.remove()
        }
    }

    companion object {
        const val ACTION_RUN_TASK = "me.rerere.rikkahub.RUN_TASK"
        const val ACTION_RUN_CHAT = "me.rerere.rikkahub.RUN_CHAT"
        private const val REQUEST_ID_DEDUP_WINDOW_MS = 60_000L
        const val EXTRA_TASK = "task"
        const val EXTRA_TASK_B64 = "task_b64"
        const val EXTRA_CHAT = "chat"
        const val EXTRA_CHAT_B64 = "chat_b64"
        const val EXTRA_REQUEST_ID = "request_id"
        const val EXTRA_RETURN_ACTION = "return_action"
        const val EXTRA_RETURN_PACKAGE = "return_package"
        const val EXTRA_STATUS = "status"
        const val EXTRA_MESSAGE = "message"

        private const val TAG = "ExtAutomation"

        /**
         * Pull the prompt out of an intent. Spec mandates support for both raw string and
         * Base64-NO_WRAP encoded forms (Tasker has historically corrupted unicode strings,
         * so the b64 form is the safe path).
         */
        fun extractPrompt(intent: Intent, rawKey: String, b64Key: String): String? =
            extractPromptStrings(
                raw = intent.getStringExtra(rawKey),
                base64Encoded = intent.getStringExtra(b64Key),
            )

        /**
         * Pure-logic version of [extractPrompt] taking the two string values directly. Lets
         * us unit-test the precedence + base64-decode logic on the JVM without an Intent.
         * Public-internal so the extractor stays a single source of truth across the
         * Activity, Receiver, and tests.
         */
        @JvmStatic
        fun extractPromptStrings(raw: String?, base64Encoded: String?): String? {
            raw?.takeIf { it.isNotBlank() }?.let { return it }
            base64Encoded?.takeIf { it.isNotBlank() }?.let { encoded ->
                return runCatching {
                    String(java.util.Base64.getUrlDecoder().decode(encoded))
                }.recoverCatching {
                    String(java.util.Base64.getDecoder().decode(encoded))
                }.getOrNull()
            }
            return null
        }
    }
}

internal fun CommandOutcome.externalTerminal(): Pair<String, AgentRunStatus> = when (this) {
    CommandOutcome.Completed -> "completed" to AgentRunStatus.succeeded
    CommandOutcome.Cancelled, is CommandOutcome.Superseded -> "cancelled" to AgentRunStatus.cancelled
    is CommandOutcome.Rejected -> "rejected" to AgentRunStatus.failed
    is CommandOutcome.Conflict, is CommandOutcome.NotApplied, is CommandOutcome.Failed,
    is CommandOutcome.SkippedDependencyFailed -> "failed" to AgentRunStatus.failed
}
