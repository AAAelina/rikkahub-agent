package me.rerere.rikkahub.memory.dreaming.orchestration

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.delay
import me.rerere.rikkahub.memory.dreaming.DreamingTestFixtures
import me.rerere.rikkahub.memory.dreaming.input.DreamInputBuildRequest
import me.rerere.rikkahub.memory.dreaming.input.DreamInputBuilder
import me.rerere.rikkahub.memory.dreaming.input.DreamDeterministicInvalidation
import me.rerere.rikkahub.memory.dreaming.input.DreamDeterministicInvalidationReason
import me.rerere.rikkahub.memory.dreaming.input.DreamModelInput
import me.rerere.rikkahub.memory.dreaming.model.DreamSha256
import me.rerere.rikkahub.memory.dreaming.source.DreamSourceReadResult
import me.rerere.rikkahub.memory.dreaming.source.DreamSourceReader
import me.rerere.rikkahub.memory.dreaming.store.BeginDreamSynthesisRequest
import me.rerere.rikkahub.memory.dreaming.store.BeginDreamSynthesisResult
import me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisCommitRejection
import me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisCommitResult
import me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisStoreRejection
import me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisStoreResult
import me.rerere.rikkahub.memory.dreaming.store.FakeDreamSynthesisStore
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamClaimIdFactory
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamModelAudit
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamProposalValidator
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamSynthesizeResult
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamSynthesizer
import me.rerere.rikkahub.memory.dreaming.runtime.DreamBudgetDenialReason
import me.rerere.rikkahub.memory.dreaming.runtime.DreamBudgetGate
import me.rerere.rikkahub.memory.dreaming.runtime.DreamDailyUsage
import me.rerere.rikkahub.memory.dreaming.runtime.DreamDailyUsageStore
import me.rerere.rikkahub.memory.dreaming.runtime.DreamingCostPolicy
import me.rerere.rikkahub.memory.dreaming.runtime.DreamingCostPolicySource
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class DreamSynthesisOrchestratorTest {
    @Test
    fun `model runs outside store transaction and no-op rechecks all active provenance`() = runBlocking {
        val fixture = fixture()
        var modelCalled = false
        val orchestrator = fixture.orchestrator(
            DreamSynthesizer {
                assertFalse(fixture.store.transactionOpen)
                assertEquals(1, fixture.store.dispatches.size)
                modelCalled = true
                DreamSynthesizeResult.Success(noOpJson(), audit())
            },
        )

        val result = orchestrator.run(beginRequest())

        assertTrue(modelCalled)
        assertTrue(result is DreamSynthesisRunResult.Completed)
        assertEquals(1, fixture.store.commits.size)
        assertEquals(1, fixture.store.commits.single().liveAuthorityPins.size)
        assertTrue(fixture.store.commits.single().historicalTransitionPins.isEmpty())
        assertEquals(1, fixture.store.commits.single().inputMemoryCount)
        assertEquals(1, fixture.store.commits.single().outputOperationCount)
        assertEquals(
            fixture.store.commits.single().snapshot.manifestHash,
            fixture.store.commits.single().outputManifestHash,
        )
        assertFalse(fixture.store.transactionOpen)
    }

    @Test
    fun `provider is never called unless durable dispatch accounting succeeds`() = runBlocking {
        val fixture = fixture()
        fixture.store.dispatchResult = DreamSynthesisStoreResult.Rejected(
            DreamSynthesisStoreRejection.STORE_CORRUPTION,
        )
        var providerCalled = false

        val result = fixture.orchestrator(
            DreamSynthesizer {
                providerCalled = true
                DreamSynthesizeResult.Success(noOpJson(), audit())
            },
        ).run(beginRequest())

        assertFalse(providerCalled)
        assertEquals(1, fixture.store.dispatches.size)
        assertEquals(
            me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisFailure.STORE_FAILURE,
            (result as DreamSynthesisRunResult.Failed).reason,
        )
        assertEquals(
            listOf(me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisFailure.STORE_FAILURE),
            fixture.store.failures,
        )
    }

    @Test
    fun `host invalidation keeps historical pins without weakening the live set`() = runBlocking {
        val fixture = fixture()
        val claim = fixture.store.seed.currentClaims.single()
        fixture.store.seed = fixture.store.seed.copy(
            deterministicInvalidations = listOf(
                DreamDeterministicInvalidation(
                    claimId = claim.claimId,
                    expectedRevision = claim.revision,
                    reason = DreamDeterministicInvalidationReason.SOURCE_EXPIRED,
                ),
            ),
        )

        val result = fixture.orchestrator(
            DreamSynthesizer { DreamSynthesizeResult.Success(noOpJson(), audit()) },
        ).run(beginRequest())

        assertTrue(result is DreamSynthesisRunResult.Completed)
        val commit = fixture.store.commits.single()
        assertTrue(commit.liveAuthorityPins.isEmpty())
        assertEquals(listOf(claim.sources.single().authority), commit.historicalTransitionPins)
    }

    @Test
    fun `resume attempt may advance while persisted semantic clock remains frozen`() = runBlocking {
        val fixture = fixture()

        val result = fixture.orchestrator(
            DreamSynthesizer { DreamSynthesizeResult.Success(noOpJson(), audit()) },
        ).run(beginRequest(attemptNowEpochMs = DreamingTestFixtures.NOW + 60_000))

        assertTrue(result is DreamSynthesisRunResult.Completed)
        assertEquals(DreamingTestFixtures.NOW, fixture.store.commits.single().fence.frozenNowEpochMs)
    }

    @Test
    fun `dual CAS rejection rolls back then terminalizes conflict separately`() = runBlocking {
        val fixture = fixture()
        fixture.store.commitResult = DreamSynthesisCommitResult.Rejected(
            DreamSynthesisCommitRejection.MEMORY_EPOCH_CONFLICT,
        )
        val result = fixture.orchestrator(
            DreamSynthesizer { DreamSynthesizeResult.Success(noOpJson(), audit()) },
        ).run(beginRequest())

        assertEquals(DreamSynthesisRetryReason.COMMIT_CONFLICT, (result as DreamSynthesisRunResult.Retry).reason)
        assertEquals(listOf(DreamSynthesisCommitRejection.MEMORY_EPOCH_CONFLICT), fixture.store.conflicts)
        assertFalse(fixture.store.transactionOpen)
    }

    @Test
    fun `commit exception rolls back and remains retryable without false conflict`() = runBlocking {
        val fixture = fixture()
        fixture.store.commitException = IllegalStateException("transient database failure")

        val result = fixture.orchestrator(
            DreamSynthesizer { DreamSynthesizeResult.Success(noOpJson(), audit()) },
        ).run(beginRequest())

        assertEquals(
            DreamSynthesisRetryReason.STORE_TEMPORARY_FAILURE,
            (result as DreamSynthesisRunResult.Retry).reason,
        )
        assertTrue(fixture.store.commits.isEmpty())
        assertTrue(fixture.store.conflicts.isEmpty())
        assertFalse(fixture.store.transactionOpen)
    }

    @Test
    fun `retryable model failure leaves durable run recoverable and commits nothing`() = runBlocking {
        val fixture = fixture()
        val result = fixture.orchestrator(
            DreamSynthesizer {
                DreamSynthesizeResult.Failure(
                    me.rerere.rikkahub.memory.dreaming.synthesis.DreamSynthesizeFailure.TIMEOUT,
                    retryable = true,
                )
            },
        ).run(beginRequest())

        assertEquals(DreamSynthesisRetryReason.MODEL_TEMPORARY_FAILURE, (result as DreamSynthesisRunResult.Retry).reason)
        assertTrue(fixture.store.commits.isEmpty())
        assertTrue(fixture.store.failures.isEmpty())
    }

    @Test
    fun `final retryable model failure terminalizes run with stable provider code`() = runBlocking {
        val fixture = fixture()
        val result = fixture.orchestrator(
            DreamSynthesizer {
                DreamSynthesizeResult.Failure(
                    me.rerere.rikkahub.memory.dreaming.synthesis.DreamSynthesizeFailure.PROVIDER_UNAVAILABLE,
                    retryable = true,
                )
            },
        ).run(
            request = beginRequest(),
            terminalizeRetryableModelFailure = true,
        )

        assertEquals(
            me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisFailure.MODEL_PROVIDER_UNAVAILABLE,
            (result as DreamSynthesisRunResult.Failed).reason,
        )
        assertEquals(
            listOf(me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisFailure.MODEL_PROVIDER_UNAVAILABLE),
            fixture.store.failures,
        )
        assertTrue(fixture.store.commits.isEmpty())
    }

    @Test
    fun `permanent model failures retain their specific durable reason`() = runBlocking {
        val cases = listOf(
            me.rerere.rikkahub.memory.dreaming.synthesis.DreamSynthesizeFailure.MODEL_UNAVAILABLE to
                me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisFailure.MODEL_UNAVAILABLE,
            me.rerere.rikkahub.memory.dreaming.synthesis.DreamSynthesizeFailure.OUTPUT_LIMIT to
                me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisFailure.MODEL_OUTPUT_LIMIT,
            me.rerere.rikkahub.memory.dreaming.synthesis.DreamSynthesizeFailure.SAFETY_REJECTION to
                me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisFailure.MODEL_SAFETY_REJECTION,
            me.rerere.rikkahub.memory.dreaming.synthesis.DreamSynthesizeFailure.INVALID_CONFIGURATION to
                me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisFailure.MODEL_INVALID_CONFIGURATION,
        )

        cases.forEach { (providerFailure, durableFailure) ->
            val fixture = fixture()
            val result = fixture.orchestrator(
                DreamSynthesizer {
                    DreamSynthesizeResult.Failure(providerFailure, retryable = false)
                },
            ).run(beginRequest())

            assertEquals(durableFailure, (result as DreamSynthesisRunResult.Failed).reason)
            assertEquals(listOf(durableFailure), fixture.store.failures)
            assertTrue(fixture.store.commits.isEmpty())
        }
    }

    @Test
    fun `failed terminal transition remains retryable instead of stranding running lease`() = runBlocking {
        val fixture = fixture()
        fixture.store.failResult = DreamSynthesisStoreResult.Rejected(
            DreamSynthesisStoreRejection.LEASE_EXPIRED,
        )

        val result = fixture.orchestrator(
            DreamSynthesizer {
                DreamSynthesizeResult.Failure(
                    me.rerere.rikkahub.memory.dreaming.synthesis.DreamSynthesizeFailure.PROVIDER_UNAVAILABLE,
                    retryable = true,
                )
            },
        ).run(
            request = beginRequest(),
            terminalizeRetryableModelFailure = true,
        )

        assertEquals(
            DreamSynthesisRetryReason.STORE_TEMPORARY_FAILURE,
            (result as DreamSynthesisRunResult.Retry).reason,
        )
        assertEquals(
            listOf(me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisFailure.MODEL_PROVIDER_UNAVAILABLE),
            fixture.store.failures,
        )
    }

    @Test
    fun `long model stage renews lease and heartbeat rejection cancels before commit`() = runBlocking {
        val healthy = fixture()
        healthy.orchestrator(
            DreamSynthesizer {
                delay(20)
                DreamSynthesizeResult.Success(noOpJson(), audit())
            },
        ).run(beginRequest())
        assertTrue(healthy.store.heartbeatCount > 0)

        val lost = fixture()
        lost.store.heartbeatResult = DreamSynthesisStoreResult.Rejected(
            DreamSynthesisStoreRejection.LEASE_EXPIRED,
        )
        val result = lost.orchestrator(
            DreamSynthesizer {
                delay(50)
                DreamSynthesizeResult.Success(noOpJson(), audit())
            },
        ).run(beginRequest())

        assertEquals(DreamSynthesisRetryReason.LEASE_CONFLICT, (result as DreamSynthesisRunResult.Retry).reason)
        assertTrue(lost.store.commits.isEmpty())
    }

    @Test
    fun `feature disabled is terminal skip and read fence conflict is rescan`() = runBlocking {
        val disabled = fixture()
        disabled.store.beginOverride = BeginDreamSynthesisResult.Rejected(
            DreamSynthesisStoreRejection.FEATURE_DISABLED,
        )
        assertTrue(
            disabled.orchestrator(DreamSynthesizer { error("must not call") }).run(beginRequest()) ===
                DreamSynthesisRunResult.Disabled,
        )

        val conflicted = fixture()
        conflicted.store.readRejection = DreamSynthesisStoreRejection.FENCE_CONFLICT
        val result = conflicted.orchestrator(DreamSynthesizer { error("must not call") }).run(beginRequest())
        assertEquals(DreamSynthesisRetryReason.COMMIT_CONFLICT, (result as DreamSynthesisRunResult.Retry).reason)
        assertEquals(listOf(DreamSynthesisCommitRejection.FENCE_CONFLICT), conflicted.store.conflicts)
    }

    @Test
    fun `read store corruption terminalizes instead of retrying immutable seed forever`() = runBlocking {
        val fixture = fixture()
        fixture.store.readRejection = DreamSynthesisStoreRejection.STORE_CORRUPTION

        val result = fixture.orchestrator(DreamSynthesizer { error("must not call") }).run(beginRequest())

        assertEquals(
            me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisFailure.STORE_FAILURE,
            (result as DreamSynthesisRunResult.Failed).reason,
        )
        assertEquals(
            listOf(me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisFailure.STORE_FAILURE),
            fixture.store.failures,
        )
    }

    @Test
    fun `provider cannot self-report a different prompt ABI`() = runBlocking {
        val fixture = fixture()
        val mismatched = audit().copy(validatorVersion = "other-validator")
        val result = fixture.orchestrator(
            DreamSynthesizer { DreamSynthesizeResult.Success(noOpJson(), mismatched) },
        ).run(beginRequest())

        assertEquals(
            me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisFailure.MODEL_AUDIT_MISMATCH,
            (result as DreamSynthesisRunResult.Failed).reason,
        )
        assertTrue(fixture.store.commits.isEmpty())
    }

    @Test
    fun `real built input estimate is budgeted before provider and denial is not model failure`() = runBlocking {
        val fixture = fixture()
        var providerCalled = false
        val gate = DreamBudgetGate(
            policySource = DreamingCostPolicySource {
                DreamingCostPolicy(
                    dailyInputTokenLimit = 1L,
                    dailyOutputTokenLimit = null,
                )
            },
            usageStore = DreamDailyUsageStore {
                DreamDailyUsage(0, 0L, 0L, 0, 0)
            },
        )

        val result = fixture.orchestrator(
            synthesizer = DreamSynthesizer {
                providerCalled = true
                DreamSynthesizeResult.Success(noOpJson(), audit())
            },
            budgetGate = gate,
        ).run(beginRequest())

        assertFalse(providerCalled)
        assertEquals(
            DreamBudgetDenialReason.INPUT_TOKEN_LIMIT,
            (result as DreamSynthesisRunResult.PolicyDeferred).reason,
        )
        assertTrue(fixture.store.commits.isEmpty())
        assertTrue(fixture.store.failures.isEmpty())
    }

    @Test
    fun `conservative estimator counts UTF8 bytes and fixed framing`() {
        assertEquals(
            69L,
            conservativeDreamModelInputTokens(DreamModelInput(systemContract = "你", payloadJson = "{}")),
        )
    }

    private fun fixture(): Fixture {
        val fence = DreamingTestFixtures.fence()
        val memory = DreamingTestFixtures.memory()
        val claim = DreamingTestFixtures.claim()
        val seed = DreamInputBuildRequest(
            fence = fence,
            candidates = listOf(DreamingTestFixtures.candidate(memory)),
            currentClaims = listOf(claim),
        )
        val store = FakeDreamSynthesisStore(fence, seed)
        val builder = DreamInputBuilder(
            sourceReader = DreamSourceReader { request ->
                request.locators.map { locator ->
                    DreamSourceReadResult.Found(
                        locator,
                        "source text",
                        DreamingTestFixtures.NOW - 100,
                        locator.expectedConsumedTextDigest,
                    )
                }
            },
            tokenFactory = DreamingTestFixtures.DeterministicTokenFactory(),
        )
        return Fixture(store, builder)
    }

    private fun Fixture.orchestrator(
        synthesizer: DreamSynthesizer,
        budgetGate: DreamBudgetGate? = null,
    ) = DreamSynthesisOrchestrator(
        store = store,
        inputBuilder = builder,
        synthesizer = synthesizer,
        validator = DreamProposalValidator(DreamClaimIdFactory { DreamingTestFixtures.NEW_CLAIM_ID }),
        clock = DreamEpochClock { DreamingTestFixtures.NOW + 1_000 },
        config = DreamSynthesisOrchestratorConfig(
            compilerRevision = "compiler-v1",
            maxOutputTokens = 2_048,
            leaseDurationMs = 30,
            heartbeatIntervalMs = 5,
        ),
        budgetGate = budgetGate,
    )

    private fun beginRequest(
        attemptNowEpochMs: Long = DreamingTestFixtures.NOW,
    ) = BeginDreamSynthesisRequest(
        scopeId = DreamingTestFixtures.scope,
        runId = DreamingTestFixtures.RUN_ID,
        leaseOwner = "unit-worker",
        attemptNowEpochMs = attemptNowEpochMs,
        sourceTimezoneId = "Asia/Shanghai",
        mode = me.rerere.rikkahub.memory.dreaming.model.DreamSynthesisMode.INCREMENTAL,
    )

    private fun noOpJson() =
        """{"schema_version":2,"proposal_nonce":"p_${"N".repeat(43)}","base_experience_epoch":7,"base_dream_revision":3,"mode":"INCREMENTAL","operations":[{"op":"NO_OP"}]}"""

    private fun audit() = DreamModelAudit(
        providerKind = "fake",
        modelIdentityDigest = DreamSha256("9".repeat(64)),
        promptContractVersion = me.rerere.rikkahub.memory.dreaming.synthesis.DREAM_PROMPT_CONTRACT_VERSION,
        validatorVersion = me.rerere.rikkahub.memory.dreaming.synthesis.DREAM_VALIDATOR_VERSION,
        inputTokens = 10,
        outputTokens = 5,
    )

    private data class Fixture(
        val store: FakeDreamSynthesisStore,
        val builder: DreamInputBuilder,
    )
}
