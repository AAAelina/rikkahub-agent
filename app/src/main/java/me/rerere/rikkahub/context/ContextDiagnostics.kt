package me.rerere.rikkahub.context

import java.security.MessageDigest
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

data class ContextSourceDiagnostic(
    val source: ContextSource,
    val characterCount: Int,
    val provider: String?,
)

data class ContextOmissionDiagnostic(
    val source: ContextSource,
    val reason: ContextOmissionReason,
    val detailCode: String?,
)

data class ContextSourceTimingDiagnostic(
    val source: ContextSource,
    val durationNs: Long,
)

/**
 * Bounded, process-local diagnostics. It deliberately stores no observed screen,
 * notification, application, or OCR text.
 */
data class ContextRunDiagnostic(
    val opaqueRunId: String,
    val invocationSurface: ContextInvocationSurface,
    val sources: List<ContextSourceDiagnostic>,
    val omissions: List<ContextOmissionDiagnostic>,
    val sourceTimings: List<ContextSourceTimingDiagnostic> = emptyList(),
    val totalCharacters: Int,
    val collectedAtMs: Long,
)

class ContextDiagnosticsStore(
    private val maxEntries: Int = 32,
) {
    private val mutableEntries = MutableStateFlow<List<ContextRunDiagnostic>>(emptyList())
    val entries: StateFlow<List<ContextRunDiagnostic>> = mutableEntries.asStateFlow()

    @Synchronized
    fun record(request: ContextRequest, snapshot: ContextSnapshot) {
        val diagnostic = ContextRunDiagnostic(
            opaqueRunId = opaqueId(snapshot.runId),
            invocationSurface = request.invocationSurface,
            sources = snapshot.fragments.map { fragment ->
                ContextSourceDiagnostic(
                    source = fragment.source,
                    characterCount = fragment.text.length,
                    provider = fragment.provider?.take(80),
                )
            },
            omissions = snapshot.omissions.map { omission ->
                ContextOmissionDiagnostic(
                    source = omission.source,
                    reason = omission.reason,
                    detailCode = omission.detailCode?.take(80),
                )
            },
            sourceTimings = snapshot.sourceTimings.map { timing ->
                ContextSourceTimingDiagnostic(
                    source = timing.source,
                    durationNs = timing.durationNs,
                )
            },
            totalCharacters = snapshot.totalCharacters,
            collectedAtMs = snapshot.collectedAtMs,
        )
        mutableEntries.value = (listOf(diagnostic) + mutableEntries.value)
            .distinctBy(ContextRunDiagnostic::opaqueRunId)
            .take(maxEntries.coerceAtLeast(1))
    }

    private fun opaqueId(value: String): String = MessageDigest.getInstance("SHA-256")
        .digest(value.toByteArray(Charsets.UTF_8))
        .take(8)
        .joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }
}
