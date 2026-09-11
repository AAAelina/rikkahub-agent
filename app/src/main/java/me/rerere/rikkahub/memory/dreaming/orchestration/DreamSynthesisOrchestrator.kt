package me.rerere.rikkahub.memory.dreaming.orchestration

import android.util.Log
import java.util.concurrent.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import me.rerere.rikkahub.memory.dreaming.input.DreamInputBuilder
import me.rerere.rikkahub.memory.dreaming.input.DreamInputBundle
import me.rerere.rikkahub.memory.dreaming.input.DreamModelInput
import me.rerere.rikkahub.memory.dreaming.model.DREAM_AUTHORITY_PIN_ORDER
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotCompileRequest
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotCompiler
import me.rerere.rikkahub.memory.dreaming.store.BeginDreamSynthesisRequest
import me.rerere.rikkahub.memory.dreaming.store.BeginDreamSynthesisResult
import me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisCommitRejection
import me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisCommitRequest
import me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisCommitResult
import me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisFailure
import me.rerere.rikkahub.memory.dreaming.store.DreamProviderDispatchRequest
import me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisStore
import me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisStoreRejection
import me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisStoreResult
import me.rerere.rikkahub.memory.dreaming.store.ReadDreamInputSeedResult
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamModelAudit
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamProposalParseResult
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamProposalParser
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamProposalValidationRequest
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamProposalValidationResult
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamProposalValidator
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamSynthesizeRequest
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamSynthesizeFailure
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamSynthesizeResult
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamSynthesizer
import me.rerere.rikkahub.memory.dreaming.synthesis.DREAM_PROMPT_CONTRACT_VERSION
import me.rerere.rikkahub.memory.dreaming.synthesis.DREAM_VALIDATOR_VERSION
import me.rerere.rikkahub.memory.dreaming.synthesis.forValidationRepair
import me.rerere.rikkahub.memory.dreaming.runtime.DreamBudgetAdmissionRequest
import me.rerere.rikkahub.memory.dreaming.runtime.DreamBudgetDenialReason
import me.rerere.rikkahub.memory.dreaming.runtime.DreamBudgetGate
import me.rerere.rikkahub.memory.dreaming.runtime.DreamBudgetPermitResult

data class DreamSynthesisOrchestratorConfig(
    val compilerRevision: String,
    val maxOutputTokens: Int,
    val promptContractVersion: String = DREAM_PROMPT_CONTRACT_VERSION,
    val validatorVersion: String = DREAM_VALIDATOR_VERSION,
    val leaseDurationMs: Long,
    val heartbeatIntervalMs: Long,
) {
    init {
        require(compilerRevision.matches(Regex("^[A-Za-z0-9._-]{1,64}$")))
        require(maxOutputTokens in 1..65_536)
        require(promptContractVersion == DREAM_PROMPT_CONTRACT_VERSION)
        require(validatorVersion == DREAM_VALIDATOR_VERSION)
        require(leaseDurationMs in 3L..me.rerere.rikkahub.memory.dreaming.model.MAX_DREAM_RUN_LEASE_DURATION_MS)
        require(heartbeatIntervalMs >= 1L && heartbeatIntervalMs <= leaseDurationMs / 3L)
    }
}

fun interface DreamEpochClock {
    fun nowEpochMs(): Long
}

sealed interface DreamSynthesisRunResult {
    data class Completed(
        val snapshotId: String,
        val committedDreamRevision: Long,
    ) : DreamSynthesisRunResult

    data class AlreadyTerminal(val succeeded: Boolean) : DreamSynthesisRunResult
    data object Disabled : DreamSynthesisRunResult
    data class PolicyDeferred(
        val reason: DreamBudgetDenialReason,
        val retryAtEpochMs: Long?,
    ) : DreamSynthesisRunResult
    data class Retry(val reason: DreamSynthesisRetryReason) : DreamSynthesisRunResult
    data class Failed(val reason: DreamSynthesisFailure) : DreamSynthesisRunResult
}

enum class DreamSynthesisRetryReason {
    STORE_TEMPORARY_FAILURE,
    MODEL_TEMPORARY_FAILURE,
    COMMIT_CONFLICT,
    LEASE_CONFLICT,
}

/**
 * The call chain is deliberately split by store method returns:
 * short begin/read transactions -> source reads + model + pure logic -> short dual-CAS commit.
 */
class DreamSynthesisOrchestrator(
    private val store: DreamSynthesisStore,
    private val inputBuilder: DreamInputBuilder,
    private val synthesizer: DreamSynthesizer,
    private val validator: DreamProposalValidator,
    private val clock: DreamEpochClock,
    private val config: DreamSynthesisOrchestratorConfig,
    private val budgetGate: DreamBudgetGate? = null,
) {
    suspend fun run(
        request: BeginDreamSynthesisRequest,
        firstProviderAttempt: Boolean = true,
        terminalizeRetryableModelFailure: Boolean = false,
    ): DreamSynthesisRunResult {
        val begin = try {
            store.begin(request)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return DreamSynthesisRunResult.Retry(DreamSynthesisRetryReason.STORE_TEMPORARY_FAILURE)
        }
        val fence = when (begin) {
            is BeginDreamSynthesisResult.Ready -> begin.fence
            is BeginDreamSynthesisResult.Terminal -> return DreamSynthesisRunResult.AlreadyTerminal(begin.succeeded)
            is BeginDreamSynthesisResult.Rejected -> {
                if (begin.reason == DreamSynthesisStoreRejection.FEATURE_DISABLED) {
                    return DreamSynthesisRunResult.Disabled
                }
                return DreamSynthesisRunResult.Retry(begin.reason.toRetryReason())
            }
        }
        if (
            fence.scopeId != request.scopeId || fence.runId != request.runId ||
            fence.leaseOwner != request.leaseOwner || !fence.mode.isValidPromotionFrom(request.mode, fence.baseLastAppliedMemoryEpoch) ||
            fence.frozenNowEpochMs > request.attemptNowEpochMs ||
            fence.sourceTimezoneId != request.sourceTimezoneId
        ) {
            return failOrRetry(fence, DreamSynthesisFailure.INPUT_REJECTED)
        }

        val seed = try {
            store.readInputSeed(
                fence = fence,
                attemptNowEpochMs = maxOf(request.attemptNowEpochMs, requireClockAtLeast(fence.frozenNowEpochMs)),
            )
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Exception) {
            Log.w(DREAM_SYNTHESIS_LOG_TAG, "Dream input seed read failed for run ${fence.runId}", error)
            return DreamSynthesisRunResult.Retry(DreamSynthesisRetryReason.STORE_TEMPORARY_FAILURE)
        }
        val inputRequest = when (seed) {
            is ReadDreamInputSeedResult.Ready -> seed.request
            is ReadDreamInputSeedResult.Rejected -> return handleReadRejection(fence, seed.reason)
        }
        if (inputRequest.fence != fence) {
            return failOrRetry(fence, DreamSynthesisFailure.INPUT_REJECTED)
        }

        val input = try {
            withLeaseHeartbeat(fence) { inputBuilder.build(inputRequest) }
        } catch (_: LeaseLost) {
            return DreamSynthesisRunResult.Retry(DreamSynthesisRetryReason.LEASE_CONFLICT)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return failOrRetry(fence, DreamSynthesisFailure.INPUT_REJECTED)
        }
        val gate = budgetGate ?: return synthesizeValidateAndCommit(
            fence,
            input,
            terminalizeRetryableModelFailure,
        )
        val estimatedInputTokens = conservativeDreamModelInputTokens(input.modelInput)
            ?: return DreamSynthesisRunResult.PolicyDeferred(
                DreamBudgetDenialReason.TOKEN_ARITHMETIC_OVERFLOW,
                retryAtEpochMs = null,
            )
        val permit = gate.withPermit(
            request = DreamBudgetAdmissionRequest(
                scopeId = fence.scopeId,
                runId = fence.runId,
                nowEpochMs = requireClockAtLeast(fence.frozenNowEpochMs),
                firstProviderAttempt = firstProviderAttempt,
                estimatedInputTokens = estimatedInputTokens,
                maxOutputTokens = config.maxOutputTokens.toLong(),
            ),
        ) {
            // The global permit remains held until commit has durably stored nullable provider
            // usage. A failed/rolled-back call leaves NULL, so a capped retry fails closed.
            synthesizeValidateAndCommit(fence, input, terminalizeRetryableModelFailure)
        }
        return when (permit) {
            is DreamBudgetPermitResult.Granted -> permit.value
            is DreamBudgetPermitResult.Denied -> DreamSynthesisRunResult.PolicyDeferred(
                reason = permit.denial.reason,
                retryAtEpochMs = permit.denial.window?.endExclusiveEpochMs,
            )
        }
    }

    private suspend fun synthesizeValidateAndCommit(
        fence: me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisFence,
        input: DreamInputBundle,
        terminalizeRetryableModelFailure: Boolean,
    ): DreamSynthesisRunResult {
        val dispatchMarker = try {
            store.markProviderDispatch(
                DreamProviderDispatchRequest(
                    fence = fence,
                    promptContractVersion = config.promptContractVersion,
                    validatorVersion = config.validatorVersion,
                    inputMemoryCount = input.allowedMemories.size,
                    inputManifestHash = input.inputManifestHash,
                    markedAtEpochMs = requireClockAtLeast(fence.frozenNowEpochMs),
                ),
            )
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return DreamSynthesisRunResult.Retry(DreamSynthesisRetryReason.STORE_TEMPORARY_FAILURE)
        }
        if (dispatchMarker is DreamSynthesisStoreResult.Rejected) {
            return when (dispatchMarker.reason) {
                DreamSynthesisStoreRejection.FEATURE_DISABLED -> DreamSynthesisRunResult.Disabled
                DreamSynthesisStoreRejection.STORE_CORRUPTION ->
                    failOrRetry(fence, DreamSynthesisFailure.STORE_FAILURE)
                else -> DreamSynthesisRunResult.Retry(dispatchMarker.reason.toRetryReason())
            }
        }
        val synthesized = try {
            withLeaseHeartbeat(fence) {
                synthesizer.synthesize(
                    DreamSynthesizeRequest(
                        input = input.modelInput,
                        maxOutputTokens = config.maxOutputTokens,
                        promptContractVersion = config.promptContractVersion,
                        validatorVersion = config.validatorVersion,
                    ),
                )
            }
        } catch (_: LeaseLost) {
            return DreamSynthesisRunResult.Retry(DreamSynthesisRetryReason.LEASE_CONFLICT)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            return handleRetryableModelFailure(
                fence,
                DreamSynthesizeFailure.PROVIDER_UNAVAILABLE,
                terminalizeRetryableModelFailure,
            )
        }
        var success = when (synthesized) {
            is DreamSynthesizeResult.Success -> synthesized
            is DreamSynthesizeResult.Failure -> {
                if (synthesized.retryable) {
                    return handleRetryableModelFailure(
                        fence,
                        synthesized.reason,
                        terminalizeRetryableModelFailure,
                    )
                }
                return failOrRetry(fence, synthesized.reason.toDurableSynthesisFailure())
            }
        }
        if (
            success.audit.promptContractVersion != config.promptContractVersion ||
            success.audit.validatorVersion != config.validatorVersion
        ) {
            return failOrRetry(fence, DreamSynthesisFailure.MODEL_AUDIT_MISMATCH)
        }
        var parsed = when (val result = DreamProposalParser.parse(success.rawOutput)) {
            is DreamProposalParseResult.Parsed -> result.proposal
            is DreamProposalParseResult.Rejected -> {
                return failOrRetry(fence, DreamSynthesisFailure.MODEL_OUTPUT_PARSE_REJECTED)
            }
        }
        val initialValidation = validator.validate(DreamProposalValidationRequest(input, parsed))
        val plan = when (
            val result = initialValidation
        ) {
            is DreamProposalValidationResult.Valid -> result.plan
            is DreamProposalValidationResult.Rejected -> {
                logValidationRejection(fence.runId, result, repairAttempt = false)
                val repaired = try {
                    withLeaseHeartbeat(fence) {
                        synthesizer.synthesize(
                            DreamSynthesizeRequest(
                                input = input.modelInput.forValidationRepair(result.failure),
                                maxOutputTokens = config.maxOutputTokens,
                                promptContractVersion = config.promptContractVersion,
                                validatorVersion = config.validatorVersion,
                            ),
                        )
                    }
                } catch (_: LeaseLost) {
                    return DreamSynthesisRunResult.Retry(DreamSynthesisRetryReason.LEASE_CONFLICT)
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (_: Exception) {
                    return handleRetryableModelFailure(
                        fence,
                        DreamSynthesizeFailure.PROVIDER_UNAVAILABLE,
                        terminalizeRetryableModelFailure,
                    )
                }
                val repairSuccess = when (repaired) {
                    is DreamSynthesizeResult.Success -> repaired
                    is DreamSynthesizeResult.Failure -> {
                        if (repaired.retryable) {
                            return handleRetryableModelFailure(
                                fence,
                                repaired.reason,
                                terminalizeRetryableModelFailure,
                            )
                        }
                        return failOrRetry(fence, repaired.reason.toDurableSynthesisFailure())
                    }
                }
                val combinedAudit = combineDreamModelAudits(success.audit, repairSuccess.audit)
                    ?: return failOrRetry(fence, DreamSynthesisFailure.MODEL_AUDIT_MISMATCH)
                success = repairSuccess.copy(audit = combinedAudit)
                parsed = when (val repairParse = DreamProposalParser.parse(repairSuccess.rawOutput)) {
                    is DreamProposalParseResult.Parsed -> repairParse.proposal
                    is DreamProposalParseResult.Rejected -> {
                        return failOrRetry(fence, DreamSynthesisFailure.MODEL_OUTPUT_PARSE_REJECTED)
                    }
                }
                when (
                    val repairedValidation = validator.validate(DreamProposalValidationRequest(input, parsed))
                ) {
                    is DreamProposalValidationResult.Valid -> repairedValidation.plan
                    is DreamProposalValidationResult.Rejected -> {
                        logValidationRejection(fence.runId, repairedValidation, repairAttempt = true)
                        return failOrRetry(fence, DreamSynthesisFailure.MODEL_OUTPUT_VALIDATION_REJECTED)
                    }
                }
            }
        }
        val snapshot = try {
            DreamSnapshotCompiler.compile(
                DreamSnapshotCompileRequest(fence.scopeId, config.compilerRevision, plan.resultingClaims),
            )
        } catch (_: Exception) {
            return failOrRetry(fence, DreamSynthesisFailure.SNAPSHOT_COMPILATION_FAILED)
        }
        val liveAuthorityPins = (
            plan.resultingClaims
                .filter { it.state == me.rerere.rikkahub.memory.dreaming.model.DreamClaimState.ACTIVE_CONTEXTUAL }
                .flatMap { it.sources.filter { source -> source.directAuthority }.map { source -> source.authority } } +
                plan.modelEvidencePins
            ).distinct()
            .sortedWith(DREAM_AUTHORITY_PIN_ORDER)
        val historicalTransitionPins = plan.transitions
            .flatMap { transition ->
                transition.nextVersion.sources
                    .filter { source -> source.directAuthority }
                    .map { source -> source.authority }
            }
            .distinct()
            .filterNot(liveAuthorityPins.toSet()::contains)
            .sortedWith(DREAM_AUTHORITY_PIN_ORDER)
        val commitRequest = DreamSynthesisCommitRequest(
            fence = fence,
            plan = plan,
            snapshot = snapshot,
            liveAuthorityPins = liveAuthorityPins,
            historicalTransitionPins = historicalTransitionPins,
            inputManifestHash = input.inputManifestHash,
            outputManifestHash = snapshot.manifestHash,
            modelAudit = success.audit,
            inputMemoryCount = input.allowedMemories.size,
            outputOperationCount = parsed.operations.size,
            committedAtEpochMs = maxOf(fence.frozenNowEpochMs, requireClock()),
        )
        val commitResult = try {
            store.commit(commitRequest)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            // The store transaction has rolled back. A transport/DB exception is not evidence of a
            // CAS conflict and must leave the durable run retryable rather than terminalizing it.
            return DreamSynthesisRunResult.Retry(DreamSynthesisRetryReason.STORE_TEMPORARY_FAILURE)
        }
        return when (val result = commitResult) {
            is DreamSynthesisCommitResult.Committed -> DreamSynthesisRunResult.Completed(
                result.snapshotId,
                result.committedDreamRevision,
            )
            is DreamSynthesisCommitResult.Rejected -> {
                try {
                    store.terminalizeConflict(fence, result.reason, requireClockAtLeast(fence.frozenNowEpochMs))
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (_: Exception) {
                    return DreamSynthesisRunResult.Retry(DreamSynthesisRetryReason.STORE_TEMPORARY_FAILURE)
                }
                DreamSynthesisRunResult.Retry(
                    if (result.reason.isLeaseConflict) {
                        DreamSynthesisRetryReason.LEASE_CONFLICT
                    } else {
                        DreamSynthesisRetryReason.COMMIT_CONFLICT
                    },
                )
            }
        }
    }

    private suspend fun handleRetryableModelFailure(
        fence: me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisFence,
        failure: DreamSynthesizeFailure,
        terminalize: Boolean,
    ): DreamSynthesisRunResult {
        if (!terminalize) {
            return DreamSynthesisRunResult.Retry(DreamSynthesisRetryReason.MODEL_TEMPORARY_FAILURE)
        }
        return failOrRetry(fence, failure.toDurableSynthesisFailure())
    }

    private suspend fun handleReadRejection(
        fence: me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisFence,
        reason: DreamSynthesisStoreRejection,
    ): DreamSynthesisRunResult {
        if (reason == DreamSynthesisStoreRejection.FEATURE_DISABLED) return DreamSynthesisRunResult.Disabled
        if (reason == DreamSynthesisStoreRejection.STORE_CORRUPTION) {
            // This is a deterministic authority/data-contract failure, not a transient database
            // transport error. Retrying the same immutable seed only creates exponential backoff
            // forever. Record a durable terminal failure; failOrRetry keeps the Work retryable only
            // if that terminal transition itself cannot be persisted.
            return failOrRetry(fence, DreamSynthesisFailure.STORE_FAILURE)
        }
        if (reason == DreamSynthesisStoreRejection.FENCE_CONFLICT) {
            try {
                store.terminalizeConflict(
                    fence,
                    DreamSynthesisCommitRejection.FENCE_CONFLICT,
                    requireClockAtLeast(fence.frozenNowEpochMs),
                )
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                return DreamSynthesisRunResult.Retry(DreamSynthesisRetryReason.STORE_TEMPORARY_FAILURE)
            }
            return DreamSynthesisRunResult.Retry(DreamSynthesisRetryReason.COMMIT_CONFLICT)
        }
        return DreamSynthesisRunResult.Retry(reason.toRetryReason())
    }

    private suspend fun <T> withLeaseHeartbeat(
        fence: me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisFence,
        operation: suspend () -> T,
    ): T = coroutineScope {
        val lost = CompletableDeferred<Unit>()
        val work = async { operation() }
        val heartbeat = launch {
            while (isActive) {
                delay(config.heartbeatIntervalMs)
                val result = try {
                    store.heartbeat(
                        fence = fence,
                        nowMs = requireClockAtLeast(fence.frozenNowEpochMs),
                        leaseDurationMs = config.leaseDurationMs,
                    )
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (_: Exception) {
                    DreamSynthesisStoreResult.Rejected(DreamSynthesisStoreRejection.STORE_CORRUPTION)
                }
                if (result is DreamSynthesisStoreResult.Rejected) {
                    lost.complete(Unit)
                    return@launch
                }
            }
        }
        try {
            when (
                val guarded = select<LeaseGuardResult<T>> {
                    work.onAwait { LeaseGuardResult.Value(it) }
                    lost.onAwait { LeaseGuardResult.Lost }
                }
            ) {
                is LeaseGuardResult.Value -> guarded.value
                LeaseGuardResult.Lost -> {
                    work.cancelAndJoin()
                    throw LeaseLost()
                }
            }
        } finally {
            heartbeat.cancelAndJoin()
        }
    }

    private suspend fun failOrRetry(
        fence: me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisFence,
        failure: DreamSynthesisFailure,
    ): DreamSynthesisRunResult {
        val terminalized = try {
            store.fail(fence, failure, requireClockAtLeast(fence.frozenNowEpochMs)) is
                DreamSynthesisStoreResult.Accepted
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            false
        }
        return if (terminalized) {
            DreamSynthesisRunResult.Failed(failure)
        } else {
            // Returning a terminal Worker failure without a matching durable run transition
            // strands the active scope until lease recovery. Keep Work retryable instead.
            DreamSynthesisRunResult.Retry(DreamSynthesisRetryReason.STORE_TEMPORARY_FAILURE)
        }
    }

    private fun requireClock(): Long = clock.nowEpochMs().also { require(it >= 0L) }

    private fun requireClockAtLeast(floor: Long): Long = maxOf(floor, requireClock())
}

private sealed interface LeaseGuardResult<out T> {
    data class Value<T>(val value: T) : LeaseGuardResult<T>
    data object Lost : LeaseGuardResult<Nothing>
}

private class LeaseLost : RuntimeException()

private const val DREAM_SYNTHESIS_LOG_TAG = "DreamSynthesis"

private fun logValidationRejection(
    runId: String,
    rejection: DreamProposalValidationResult.Rejected,
    repairAttempt: Boolean,
) {
    // Local JVM tests do not provide an Android Log implementation. Diagnostics must never alter
    // the Dream state machine merely because logging is unavailable.
    runCatching {
        Log.w(
            DREAM_SYNTHESIS_LOG_TAG,
            "Dream proposal rejected for run $runId: ${rejection.failure}; repairAttempt=$repairAttempt",
        )
    }
}

private fun combineDreamModelAudits(
    first: DreamModelAudit,
    second: DreamModelAudit,
): DreamModelAudit? {
    if (first.providerKind != second.providerKind ||
        first.modelIdentityDigest != second.modelIdentityDigest ||
        first.promptContractVersion != second.promptContractVersion ||
        first.validatorVersion != second.validatorVersion
    ) {
        return null
    }
    return second.copy(
        inputTokens = sumKnownTokenUsage(first.inputTokens, second.inputTokens),
        outputTokens = sumKnownTokenUsage(first.outputTokens, second.outputTokens),
    )
}

private fun sumKnownTokenUsage(first: Int?, second: Int?): Int? {
    if (first == null || second == null) return null
    return try {
        Math.addExact(first, second)
    } catch (_: ArithmeticException) {
        null
    }
}

private fun DreamSynthesizeFailure.toDurableSynthesisFailure(): DreamSynthesisFailure = when (this) {
    DreamSynthesizeFailure.PROVIDER_UNAVAILABLE -> DreamSynthesisFailure.MODEL_PROVIDER_UNAVAILABLE
    DreamSynthesizeFailure.MODEL_UNAVAILABLE -> DreamSynthesisFailure.MODEL_UNAVAILABLE
    DreamSynthesizeFailure.TIMEOUT -> DreamSynthesisFailure.MODEL_TIMEOUT
    DreamSynthesizeFailure.CANCELLED_BY_PROVIDER -> DreamSynthesisFailure.MODEL_CANCELLED_BY_PROVIDER
    DreamSynthesizeFailure.OUTPUT_LIMIT -> DreamSynthesisFailure.MODEL_OUTPUT_LIMIT
    DreamSynthesizeFailure.SAFETY_REJECTION -> DreamSynthesisFailure.MODEL_SAFETY_REJECTION
    DreamSynthesizeFailure.INVALID_CONFIGURATION -> DreamSynthesisFailure.MODEL_INVALID_CONFIGURATION
}

/**
 * UTF-8 bytes are a provider-independent upper bound for byte-fallback tokenizers. The fixed
 * framing allowance covers the two message roles without trusting a provider-advertised count.
 */
internal fun conservativeDreamModelInputTokens(input: DreamModelInput): Long? = try {
    val systemBytes = input.systemContract.toByteArray(Charsets.UTF_8).size.toLong()
    val payloadBytes = input.payloadJson.toByteArray(Charsets.UTF_8).size.toLong()
    Math.addExact(Math.addExact(systemBytes, payloadBytes), DREAM_MODEL_INPUT_FRAMING_TOKENS)
} catch (_: ArithmeticException) {
    null
}

private const val DREAM_MODEL_INPUT_FRAMING_TOKENS = 64L

private fun DreamSynthesisStoreRejection.toRetryReason(): DreamSynthesisRetryReason = when (this) {
    DreamSynthesisStoreRejection.OWNER_MISMATCH,
    DreamSynthesisStoreRejection.LEASE_EXPIRED,
    -> DreamSynthesisRetryReason.LEASE_CONFLICT
    else -> DreamSynthesisRetryReason.STORE_TEMPORARY_FAILURE
}

private val DreamSynthesisCommitRejection.isLeaseConflict: Boolean
    get() = this in setOf(
        DreamSynthesisCommitRejection.LEASE_MISSING,
        DreamSynthesisCommitRejection.LEASE_OWNER_MISMATCH,
        DreamSynthesisCommitRejection.LEASE_EXPIRED,
        DreamSynthesisCommitRejection.RUN_NOT_RUNNING,
    )

private fun me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisMode.isValidPromotionFrom(
    requested: me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisMode,
    baseLastAppliedMemoryEpoch: Long,
): Boolean = when {
    baseLastAppliedMemoryEpoch == 0L ->
        this == me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisMode.FULL &&
            requested in setOf(
                me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisMode.INCREMENTAL,
                me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisMode.FULL,
            )
    else -> this == requested
}
