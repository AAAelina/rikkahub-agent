package me.rerere.rikkahub.context

import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.withTimeoutOrNull
import me.rerere.rikkahub.data.ai.ToolCallOrigin
import me.rerere.rikkahub.service.chat.CommandOrigin

private const val OCR_TIMEOUT_MS = 20_000L
private const val MAX_FROZEN_RUNS = 32
private const val MAX_NOTIFICATION_CONVERSATIONS = 64
private const val MAX_NOTIFICATION_HASHES_PER_CONVERSATION = 128

class DefaultContextBroker(
    private val readers: Map<ContextSource, ContextSourceReader>,
    private val nowMs: () -> Long = System::currentTimeMillis,
    private val elapsedRealtimeNanos: () -> Long = System::nanoTime,
) : ContextBroker {
    private val frozen = ConcurrentHashMap<String, CompletableDeferred<ContextSnapshot>>()
    private val insertionOrder = java.util.concurrent.ConcurrentLinkedQueue<String>()
    private val seenNotifications = ConcurrentHashMap<String, NotificationHistory>()
    private val notificationConversationOrder = java.util.concurrent.ConcurrentLinkedQueue<String>()

    override suspend fun collect(request: ContextRequest): ContextSnapshot {
        val created = CompletableDeferred<ContextSnapshot>()
        val existing = frozen.putIfAbsent(request.runId, created)
        if (existing != null) return existing.await()
        insertionOrder.add(request.runId)
        trimFrozenRuns()
        return try {
            val snapshot = collectFresh(request)
            created.complete(snapshot)
            snapshot
        } catch (cancelled: CancellationException) {
            frozen.remove(request.runId, created)
            insertionOrder.remove(request.runId)
            created.completeExceptionally(cancelled)
            throw cancelled
        } catch (failure: Throwable) {
            val snapshot = ContextSnapshot(
                runId = request.runId,
                fragments = emptyList(),
                omissions = request.allowedSources.map { source ->
                    ContextOmission(source, ContextOmissionReason.FAILED, "broker_failure")
                },
                collectedAtMs = nowMs(),
            )
            created.complete(snapshot)
            snapshot
        }
    }

    fun release(runId: String) {
        frozen.remove(runId)
        insertionOrder.remove(runId)
    }

    private suspend fun collectFresh(request: ContextRequest): ContextSnapshot {
        val omissions = mutableListOf<ContextOmission>()
        if (!request.settings.enabled) {
            return emptySnapshot(request, ContextOmissionReason.DISABLED)
        }
        if (!isLocalEligible(request)) {
            return emptySnapshot(request, ContextOmissionReason.ORIGIN_BLOCKED)
        }

        val enabled = enabledSources(request.settings)
        ContextSource.entries.forEach { source ->
            when {
                source !in request.allowedSources -> omissions += ContextOmission(
                    source,
                    ContextOmissionReason.SOURCE_NOT_ALLOWED,
                )
                source !in enabled -> omissions += ContextOmission(
                    source,
                    ContextOmissionReason.DISABLED,
                )
            }
        }
        val planned = request.allowedSources intersect enabled
        val collected = mutableListOf<ContextFragment>()
        val sourceTimings = mutableListOf<ContextSourceTiming>()

        for (source in PRIMARY_ORDER) {
            if (source !in planned) continue
            val startedAtNs = elapsedRealtimeNanos()
            val fragment = readSource(request, source, omissions)
            sourceTimings += ContextSourceTiming(
                source = source,
                durationNs = (elapsedRealtimeNanos() - startedAtNs).coerceAtLeast(0L),
            )
            fragment?.let { fragment ->
                val filtered = if (source == ContextSource.NOTIFICATIONS) {
                    filterSeenNotifications(request, fragment, omissions)
                } else {
                    fragment
                }
                filtered?.let(collected::add)
            }
        }

        val uiTree = collected.firstOrNull { it.source == ContextSource.UI_TREE }
        val uiSufficient = uiTree != null && (
            uiTree.validNodeCount >= 3 || uiTree.nonSensitiveCharacterCount >= 80
        )
        if (ContextSource.OCR_FALLBACK in planned) {
            if (uiSufficient) {
                omissions += ContextOmission(
                    ContextSource.OCR_FALLBACK,
                    ContextOmissionReason.UI_TREE_SUFFICIENT,
                )
            } else {
                val startedAtNs = elapsedRealtimeNanos()
                val attempt = withTimeoutOrNull(OCR_TIMEOUT_MS) {
                    OcrAttempt(
                        readSource(
                            request,
                            ContextSource.OCR_FALLBACK,
                            omissions,
                            recordUnavailable = true,
                        )
                    )
                }
                sourceTimings += ContextSourceTiming(
                    source = ContextSource.OCR_FALLBACK,
                    durationNs = (elapsedRealtimeNanos() - startedAtNs).coerceAtLeast(0L),
                )
                if (attempt == null) {
                    omissions += ContextOmission(
                        ContextSource.OCR_FALLBACK,
                        ContextOmissionReason.TIMED_OUT,
                    )
                } else {
                    attempt.fragment?.let(collected::add)
                }
            }
        }

        val budgeted = applyBudget(
            fragments = collected,
            maxChars = request.settings.maxChars.coerceAtLeast(0),
            omissions = omissions,
        )
        return ContextSnapshot(
            runId = request.runId,
            fragments = budgeted,
            omissions = omissions.distinct(),
            collectedAtMs = nowMs(),
            sourceTimings = sourceTimings,
        )
    }

    private suspend fun readSource(
        request: ContextRequest,
        source: ContextSource,
        omissions: MutableList<ContextOmission>,
        recordUnavailable: Boolean = true,
    ): ContextFragment? {
        val reader = readers[source]
        if (reader == null) {
            if (recordUnavailable) omissions += ContextOmission(
                source,
                ContextOmissionReason.UNAVAILABLE,
                "reader_missing",
            )
            return null
        }
        return try {
            when (val result = reader.read(request, source)) {
                is ContextReadResult.Available -> {
                    val sanitized = ContextTextSanitizer.sanitize(result.fragment.text).trim()
                    if (sanitized.isEmpty()) {
                        omissions += ContextOmission(source, ContextOmissionReason.EMPTY)
                        null
                    } else {
                        result.fragment.copy(
                            source = source,
                            text = sanitized,
                            nonSensitiveCharacterCount = minOf(
                                result.fragment.nonSensitiveCharacterCount,
                                sanitized.length,
                            ),
                        )
                    }
                }
                is ContextReadResult.Unavailable -> {
                    if (recordUnavailable) omissions += ContextOmission(
                        source,
                        ContextOmissionReason.UNAVAILABLE,
                        result.detailCode,
                    )
                    null
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Throwable) {
            omissions += ContextOmission(source, ContextOmissionReason.FAILED, "reader_failed")
            null
        }
    }

    private fun applyBudget(
        fragments: List<ContextFragment>,
        maxChars: Int,
        omissions: MutableList<ContextOmission>,
    ): List<ContextFragment> {
        var remaining = maxChars
        return buildList {
            fragments.forEach { fragment ->
                if (remaining <= 0) {
                    omissions += ContextOmission(
                        fragment.source,
                        ContextOmissionReason.BUDGET_TRUNCATED,
                    )
                    return@forEach
                }
                val text = fragment.text.take(remaining)
                if (text.length < fragment.text.length) {
                    omissions += ContextOmission(
                        fragment.source,
                        ContextOmissionReason.BUDGET_TRUNCATED,
                    )
                }
                if (text.isNotEmpty()) add(fragment.copy(text = text))
                remaining -= text.length
            }
        }
    }

    private fun emptySnapshot(
        request: ContextRequest,
        reason: ContextOmissionReason,
    ) = ContextSnapshot(
        runId = request.runId,
        fragments = emptyList(),
        omissions = request.allowedSources.map { source -> ContextOmission(source, reason) },
        collectedAtMs = nowMs(),
    )

    private fun isLocalEligible(request: ContextRequest): Boolean {
        if (request.isHeadless || request.isSubAgent) return false
        return when (request.invocationSurface) {
            ContextInvocationSurface.LOCAL_CHAT ->
                request.commandOrigin == CommandOrigin.APP_UI &&
                    request.toolCallOrigin == ToolCallOrigin.LocalChat
            ContextInvocationSurface.SYSTEM_ASSISTANT ->
                request.commandOrigin == CommandOrigin.SYSTEM_ASSISTANT &&
                    request.toolCallOrigin == ToolCallOrigin.SystemAssistant
            // A QuickCapture already contributes a screenshot to this run. Keep automatic
            // context disabled until a future design can guarantee no duplicate OCR/capture.
            ContextInvocationSurface.QUICK_CAPTURE -> false
            ContextInvocationSurface.TELEGRAM,
            ContextInvocationSurface.WEB,
            ContextInvocationSurface.WORKFLOW,
            ContextInvocationSurface.CRON,
            ContextInvocationSurface.SUBAGENT,
            ContextInvocationSurface.KEYGUARD,
            ContextInvocationSurface.MCP,
            ContextInvocationSurface.EXTERNAL_AUTOMATION,
            -> false
        }
    }

    private fun enabledSources(settings: AssistantContextSettings): Set<ContextSource> = buildSet {
        if (settings.foregroundWindow) add(ContextSource.FOREGROUND_WINDOW)
        if (settings.uiTree) add(ContextSource.UI_TREE)
        if (settings.deviceStatus) add(ContextSource.DEVICE_STATUS)
        if (settings.ocrFallback) add(ContextSource.OCR_FALLBACK)
        if (settings.usageStats) add(ContextSource.USAGE_STATS)
        if (settings.notifications) add(ContextSource.NOTIFICATIONS)
    }

    private fun trimFrozenRuns() {
        while (frozen.size > MAX_FROZEN_RUNS) {
            val oldest = insertionOrder.poll() ?: return
            frozen.remove(oldest)
        }
    }

    /**
     * Active Android notifications remain visible for many turns. Inject each exact notification
     * line once per conversation, while allowing changed/new notification content through.
     * Only hashes are retained in memory; notification text is not persisted.
     */
    private fun filterSeenNotifications(
        request: ContextRequest,
        fragment: ContextFragment,
        omissions: MutableList<ContextOmission>,
    ): ContextFragment? {
        val scope = "${request.assistantId}:${request.conversationId}"
        val created = NotificationHistory()
        val history = seenNotifications.putIfAbsent(scope, created) ?: created.also {
            notificationConversationOrder.add(scope)
            trimNotificationConversations()
        }
        val unseenLines = synchronized(history) {
            fragment.text.lineSequence()
                .map(String::trim)
                .filter(String::isNotEmpty)
                .filter { line -> history.hashes.add(line.sha256()) }
                .toList()
                .also {
                    while (history.hashes.size > MAX_NOTIFICATION_HASHES_PER_CONVERSATION) {
                        val iterator = history.hashes.iterator()
                        if (iterator.hasNext()) {
                            iterator.next()
                            iterator.remove()
                        }
                    }
                }
        }
        if (unseenLines.isEmpty()) {
            omissions += ContextOmission(
                ContextSource.NOTIFICATIONS,
                ContextOmissionReason.EMPTY,
                "notifications_already_seen",
            )
            return null
        }
        val text = unseenLines.joinToString("\n")
        return fragment.copy(
            text = text,
            nonSensitiveCharacterCount = minOf(fragment.nonSensitiveCharacterCount, text.length),
        )
    }

    private fun trimNotificationConversations() {
        while (seenNotifications.size > MAX_NOTIFICATION_CONVERSATIONS) {
            val oldest = notificationConversationOrder.poll() ?: return
            seenNotifications.remove(oldest)
        }
    }

    private companion object {
        data class OcrAttempt(val fragment: ContextFragment?)

        class NotificationHistory {
            val hashes = LinkedHashSet<String>()
        }

        val PRIMARY_ORDER = listOf(
            ContextSource.FOREGROUND_WINDOW,
            ContextSource.UI_TREE,
            ContextSource.DEVICE_STATUS,
            ContextSource.USAGE_STATS,
            ContextSource.NOTIFICATIONS,
        )
    }
}

private fun String.sha256(): String = MessageDigest.getInstance("SHA-256")
    .digest(toByteArray(Charsets.UTF_8))
    .joinToString("") { byte -> "%02x".format(byte) }

internal object ContextTextSanitizer {
    private val secretAssignment = Regex(
        "(?i)\\b(password|passwd|token|api[_ -]?key|secret|otp|verification code|验证码)" +
            "\\s*[:=]\\s*([^\\s,;]{3,})",
    )
    private val bearer = Regex("(?i)\\bBearer\\s+[A-Za-z0-9._~+/-]{6,}")
    private val verificationCode = Regex(
        "(?i)(验证码|校验码|动态码|verification\\s*code|one[- ]?time\\s*(?:code|password)|otp)" +
            "([^0-9]{0,16})[0-9]{4,8}",
    )
    private val longFinancialNumber = Regex("(?<![0-9])[0-9][0-9 -]{11,21}[0-9](?![0-9])")

    fun sanitize(text: String): String = text
        .replace(secretAssignment) { match -> "${match.groupValues[1]}=[REDACTED]" }
        .replace(bearer, "Bearer [REDACTED]")
        .replace(verificationCode) { match ->
            "${match.groupValues[1]}${match.groupValues[2]}[REDACTED]"
        }
        .replace(longFinancialNumber, "[REDACTED_NUMBER]")
        .replace('\u0000', ' ')
}
