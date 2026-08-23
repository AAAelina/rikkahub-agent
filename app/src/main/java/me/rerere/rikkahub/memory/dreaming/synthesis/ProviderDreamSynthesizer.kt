package me.rerere.rikkahub.memory.dreaming.synthesis

import java.net.URI
import java.security.MessageDigest
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withTimeout
import me.rerere.ai.context.ProviderContextWindowResolver
import me.rerere.ai.context.ProviderRequestTokenEstimator
import me.rerere.ai.provider.Model
import me.rerere.ai.provider.ModelAbility
import me.rerere.ai.provider.ProviderManager
import me.rerere.ai.provider.ProviderSetting
import me.rerere.ai.provider.TextGenerationParams
import me.rerere.ai.ui.FinishCategory
import me.rerere.ai.ui.UIMessage
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.data.datastore.findModelById
import me.rerere.rikkahub.data.datastore.findProvider
import me.rerere.rikkahub.memory.memoryExtractionReasoningLevel
import me.rerere.rikkahub.memory.resolveMemoryExtractionModel
import me.rerere.rikkahub.memory.dreaming.model.DreamSha256

/**
 * Real provider adapter for M4 shadow synthesis. Production flags remain all-off, so this class is
 * dormant until the runtime has explicitly admitted a run; it never selects an implicit fallback
 * beyond the existing, user-owned memory extraction model policy.
 */
class ProviderDreamSynthesizer(
    private val settingsStore: SettingsStore,
    private val providerManager: ProviderManager,
    private val tokenEstimator: ProviderRequestTokenEstimator = ProviderRequestTokenEstimator(),
) : DreamSynthesizer {
    override suspend fun synthesize(request: DreamSynthesizeRequest): DreamSynthesizeResult = try {
        if (request.promptContractVersion != DREAM_PROMPT_CONTRACT_VERSION ||
            request.validatorVersion != DREAM_VALIDATOR_VERSION
        ) {
            return DreamSynthesizeResult.Failure(
                DreamSynthesizeFailure.INVALID_CONFIGURATION,
                retryable = false,
            )
        }
        val settings = settingsStore.settingsFlow.first { !it.init }
        val model = settings.dreamModelId?.let(settings.providers::findModelById)
            ?: settings.resolveMemoryExtractionModel()
            ?: return DreamSynthesizeResult.Failure(
                DreamSynthesizeFailure.MODEL_UNAVAILABLE,
                retryable = false,
            )
        val providerSetting = model.findProvider(settings.providers)
            ?: return DreamSynthesizeResult.Failure(
                DreamSynthesizeFailure.PROVIDER_UNAVAILABLE,
                retryable = false,
            )
        if (!providerSetting.enabled) {
            return DreamSynthesizeResult.Failure(
                DreamSynthesizeFailure.INVALID_CONFIGURATION,
                retryable = false,
            )
        }
        val provider = providerManager.getProviderByType(providerSetting)
        val messages = listOf(
            UIMessage.system(request.input.systemContract),
            UIMessage.user(request.input.payloadJson),
        )
        val inputUtf8Bytes = request.input.systemContract.toByteArray(Charsets.UTF_8).size.toLong() +
            request.input.payloadJson.toByteArray(Charsets.UTF_8).size.toLong()
        val trustedWindow = provider.resolveTrustedContextWindowTokens(providerSetting, model)
        val enforcedWindow = ProviderContextWindowResolver.resolve(
            configuredPolicyTokens = model.userContextWindowTokens,
            trustedCapabilityTokens = trustedWindow,
            advertisedTokens = model.contextLength,
        ).effectiveTokens
        val estimatedInputTokens = tokenEstimator.estimate(messages).totalInputTokens
        val effectiveOutputTokens = dreamProviderOutputTokenBudget(
            providerSetting = providerSetting,
            model = model,
            requestedOutputTokens = request.maxOutputTokens,
        )
        val synthesisTimeoutMs = dreamProviderTimeoutMs(
            providerSetting = providerSetting,
            model = model,
        )
        val response = when (val admitted = withDreamProviderAdmission(
            inputUtf8Bytes = inputUtf8Bytes,
            estimatedInputTokens = estimatedInputTokens,
            requestedOutputTokens = effectiveOutputTokens,
            enforcedWindowTokens = enforcedWindow,
        ) {
            withTimeout(synthesisTimeoutMs) {
                provider.generateText(
                    providerSetting = providerSetting,
                    messages = messages,
                    params = TextGenerationParams(
                        model = model,
                        temperature = 0.1f,
                        maxTokens = effectiveOutputTokens,
                        tools = emptyList(),
                        reasoningLevel = memoryExtractionReasoningLevel(model),
                        omitReasoningConfigurationWhenOff = true,
                        customHeaders = model.customHeaders,
                        customBody = model.customBodies,
                    ),
                )
            }
        }) {
            is DreamProviderAdmissionResult.Admitted -> admitted.value
            DreamProviderAdmissionResult.Rejected -> return DreamSynthesizeResult.Failure(
                DreamSynthesizeFailure.OUTPUT_LIMIT,
                retryable = false,
            )
        }
        when (response.resolvedTerminal()?.category) {
            FinishCategory.LENGTH -> DreamSynthesizeResult.Failure(
                DreamSynthesizeFailure.OUTPUT_LIMIT,
                retryable = false,
            )

            FinishCategory.SAFETY -> DreamSynthesizeResult.Failure(
                DreamSynthesizeFailure.SAFETY_REJECTION,
                retryable = false,
            )

            FinishCategory.CANCELLED -> DreamSynthesizeResult.Failure(
                DreamSynthesizeFailure.CANCELLED_BY_PROVIDER,
                retryable = true,
            )

            FinishCategory.FAILED,
            FinishCategory.INCOMPLETE,
            FinishCategory.EOF,
            FinishCategory.UNKNOWN,
            FinishCategory.TOOL_CALLS,
            null,
            -> DreamSynthesizeResult.Failure(
                DreamSynthesizeFailure.PROVIDER_UNAVAILABLE,
                retryable = true,
            )

            FinishCategory.STOP -> {
                val output = response.choices.firstOrNull()?.message?.toText().orEmpty().trim()
                if (output.toByteArray(Charsets.UTF_8).size > MAX_DREAM_PROVIDER_OUTPUT_UTF8_BYTES) {
                    DreamSynthesizeResult.Failure(
                        DreamSynthesizeFailure.OUTPUT_LIMIT,
                        retryable = false,
                    )
                } else if (output.isEmpty()) {
                    DreamSynthesizeResult.Failure(
                        DreamSynthesizeFailure.PROVIDER_UNAVAILABLE,
                        retryable = true,
                    )
                } else {
                    DreamSynthesizeResult.Success(
                        rawOutput = output,
                        audit = DreamModelAudit(
                            providerKind = providerSetting.dreamProviderKind(),
                            modelIdentityDigest = dreamModelIdentityDigest(
                                providerSetting = providerSetting,
                                modelId = model.id.toString(),
                                providerModelId = model.modelId,
                            ),
                            promptContractVersion = request.promptContractVersion,
                            validatorVersion = request.validatorVersion,
                            inputTokens = response.usage?.promptTokens,
                            outputTokens = response.usage?.completionTokens,
                        ),
                    )
                }
            }
        }
    } catch (cancelled: CancellationException) {
        if (cancelled is TimeoutCancellationException) {
            DreamSynthesizeResult.Failure(DreamSynthesizeFailure.TIMEOUT, retryable = true)
        } else {
            throw cancelled
        }
    } catch (_: Exception) {
        DreamSynthesizeResult.Failure(DreamSynthesizeFailure.PROVIDER_UNAVAILABLE, retryable = true)
    }
}

internal sealed interface DreamProviderAdmissionResult<out T> {
    data class Admitted<T>(val value: T) : DreamProviderAdmissionResult<T>
    data object Rejected : DreamProviderAdmissionResult<Nothing>
}

/** The generation block is unreachable for every locally detectable input/output overflow. */
internal suspend fun <T> withDreamProviderAdmission(
    inputUtf8Bytes: Long,
    estimatedInputTokens: Int,
    requestedOutputTokens: Int,
    enforcedWindowTokens: Int,
    generate: suspend () -> T,
): DreamProviderAdmissionResult<T> {
    val tokenTotal = estimatedInputTokens.toLong() + requestedOutputTokens.toLong() +
        DREAM_PROVIDER_SAFETY_TOKENS
    if (inputUtf8Bytes < 0L || inputUtf8Bytes > MAX_DREAM_PROVIDER_INPUT_UTF8_BYTES ||
        estimatedInputTokens < 0 || requestedOutputTokens !in 1..MAX_DREAM_PROVIDER_OUTPUT_TOKENS ||
        enforcedWindowTokens <= 0 || tokenTotal > enforcedWindowTokens.toLong()
    ) {
        return DreamProviderAdmissionResult.Rejected
    }
    return DreamProviderAdmissionResult.Admitted(generate())
}

/**
 * DeepSeek V4 on OpenCode exposes only high/max reasoning effort, so even the app's LOW/OFF
 * structured-task policy is normalized to the provider's high reasoning mode. A small completion
 * budget can therefore be exhausted by hidden reasoning before the compact Dream JSON is emitted.
 * Promote only that known provider/model pairing to the larger local Dream ceiling; every other
 * model keeps the caller-owned baseline unchanged.
 */
internal fun dreamProviderOutputTokenBudget(
    providerSetting: ProviderSetting,
    model: Model,
    requestedOutputTokens: Int,
): Int {
    return if (isOpenCodeDeepSeekV4Reasoning(providerSetting, model)) {
        OPEN_CODE_DEEPSEEK_V4_DREAM_OUTPUT_TOKENS
    } else {
        requestedOutputTokens
    }
}

internal fun dreamProviderTimeoutMs(
    providerSetting: ProviderSetting,
    model: Model,
): Long = if (isOpenCodeDeepSeekV4Reasoning(providerSetting, model)) {
    OPEN_CODE_DEEPSEEK_V4_DREAM_TIMEOUT_MS
} else {
    DREAM_SYNTHESIS_TIMEOUT_MS
}

private fun isOpenCodeDeepSeekV4Reasoning(
    providerSetting: ProviderSetting,
    model: Model,
): Boolean = providerSetting is ProviderSetting.OpenAI &&
    providerSetting.baseUrl.isOpenCodeHost() &&
    ModelAbility.REASONING in model.abilities &&
    "deepseek-v4-" in model.modelId.lowercase()

private fun String.isOpenCodeHost(): Boolean = runCatching {
    URI(this).host?.lowercase()?.let { host ->
        host == "opencode.ai" || host.endsWith(".opencode.ai")
    } == true
}.getOrDefault(false)

private fun ProviderSetting.dreamProviderKind(): String = when (this) {
    is ProviderSetting.OpenAI -> "openai"
    is ProviderSetting.Google -> "google"
    is ProviderSetting.Claude -> "claude"
    is ProviderSetting.AICore -> "aicore"
    is ProviderSetting.LiteRtLocal -> "litert"
    is ProviderSetting.Codex -> "codex"
}

private fun dreamModelIdentityDigest(
    providerSetting: ProviderSetting,
    modelId: String,
    providerModelId: String,
): DreamSha256 {
    val digest = MessageDigest.getInstance("SHA-256")
    listOf(
        DREAM_MODEL_IDENTITY_DOMAIN,
        providerSetting.dreamProviderKind(),
        providerSetting.id.toString(),
        modelId,
        providerModelId,
    ).forEach { value ->
        val bytes = value.toByteArray(Charsets.UTF_8)
        digest.update(byteArrayOf(
            (bytes.size ushr 24).toByte(),
            (bytes.size ushr 16).toByte(),
            (bytes.size ushr 8).toByte(),
            bytes.size.toByte(),
        ))
        digest.update(bytes)
    }
    return DreamSha256(
        digest.digest().joinToString("") { byte ->
            (byte.toInt() and 0xff).toString(16).padStart(2, '0')
        },
    )
}

private const val DREAM_SYNTHESIS_TIMEOUT_MS = 2L * 60_000L
private const val OPEN_CODE_DEEPSEEK_V4_DREAM_TIMEOUT_MS = 12L * 60_000L
private const val DREAM_MODEL_IDENTITY_DOMAIN = "rikkahub.dream-model-identity.v1"
private const val MAX_DREAM_PROVIDER_INPUT_UTF8_BYTES = 128_000L
private const val MAX_DREAM_PROVIDER_OUTPUT_UTF8_BYTES = 128_000
private const val OPEN_CODE_DEEPSEEK_V4_DREAM_OUTPUT_TOKENS = 81_920
private const val MAX_DREAM_PROVIDER_OUTPUT_TOKENS = 81_920
private const val DREAM_PROVIDER_SAFETY_TOKENS = 256
