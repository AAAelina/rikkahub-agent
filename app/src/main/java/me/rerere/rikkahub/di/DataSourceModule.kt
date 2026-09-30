package me.rerere.rikkahub.di

import androidx.room.Room
import androidx.room.RoomDatabase
import androidx.sqlite.db.SupportSQLiteDatabase
import android.content.Context
import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.http.HttpHeaders
import io.pebbletemplates.pebble.PebbleEngine
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import me.rerere.ai.provider.ProviderManager
import me.rerere.common.http.AcceptLanguageBuilder
import me.rerere.rikkahub.BuildConfig
import me.rerere.rikkahub.AppScope
import me.rerere.rikkahub.data.ai.AIRequestInterceptor
import me.rerere.rikkahub.data.ai.BoundedDreamRuntimeTelemetryStore
import me.rerere.rikkahub.data.ai.DreamRuntimeDiagnosticsSink
import me.rerere.rikkahub.data.ai.DreamRuntimeUsageRecorder
import me.rerere.rikkahub.data.ai.RequestLoggingInterceptor
import me.rerere.rikkahub.data.ai.transformers.AssistantTemplateLoader
import me.rerere.rikkahub.data.ai.GenerationHandler
import me.rerere.rikkahub.data.ai.transformers.TemplateTransformer
import me.rerere.rikkahub.data.api.RikkaHubAPI
import me.rerere.rikkahub.data.api.SponsorAPI
import me.rerere.rikkahub.data.codex.CodexAccountRepository
import me.rerere.rikkahub.data.codex.CodexCredentialStore
import me.rerere.rikkahub.data.codex.CodexOAuthManager
import me.rerere.rikkahub.data.codex.CodexProvider
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.createAppSQLiteOpenHelperFactory
import me.rerere.rikkahub.data.db.fts.MessageFtsManager
import me.rerere.rikkahub.data.db.fts.MemoryFtsManager
import me.rerere.rikkahub.data.db.fts.SimpleDictManager
import me.rerere.rikkahub.data.db.fts.ensureMemoryFtsSchema
import me.rerere.rikkahub.data.db.migrations.Migration_6_7
import me.rerere.rikkahub.data.db.migrations.Migration_11_12
import me.rerere.rikkahub.data.db.migrations.Migration_13_14
import me.rerere.rikkahub.data.db.migrations.Migration_14_15
import me.rerere.rikkahub.data.db.migrations.Migration_15_16
import me.rerere.rikkahub.data.db.migrations.Migration_23_24
import me.rerere.rikkahub.data.db.migrations.MIGRATION_26_27
import me.rerere.rikkahub.data.db.migrations.MIGRATION_27_28
import me.rerere.rikkahub.data.db.migrations.MIGRATION_28_29
import me.rerere.rikkahub.data.db.migrations.MIGRATION_29_30
import me.rerere.rikkahub.data.db.migrations.MIGRATION_30_31
import me.rerere.rikkahub.data.db.migrations.MIGRATION_31_32
import me.rerere.rikkahub.data.db.migrations.MIGRATION_32_33
import me.rerere.rikkahub.data.db.migrations.MIGRATION_33_34
import me.rerere.rikkahub.data.db.migrations.MIGRATION_34_35
import me.rerere.rikkahub.data.db.migrations.MIGRATION_35_36
import me.rerere.rikkahub.data.db.migrations.MIGRATION_36_37
import me.rerere.rikkahub.data.db.migrations.MIGRATION_37_38
import me.rerere.rikkahub.data.db.migrations.MIGRATION_38_39
import me.rerere.rikkahub.data.db.migrations.MIGRATION_39_40
import me.rerere.rikkahub.data.db.migrations.MIGRATION_40_41
import me.rerere.rikkahub.data.db.migrations.MIGRATION_41_42
import me.rerere.rikkahub.data.db.migrations.MIGRATION_42_43
import me.rerere.rikkahub.data.db.migrations.MIGRATION_43_44
import me.rerere.rikkahub.data.db.migrations.MIGRATION_44_45
import me.rerere.rikkahub.data.db.migrations.MIGRATION_45_46
import me.rerere.rikkahub.data.db.migrations.MIGRATION_46_47
import me.rerere.rikkahub.data.db.migrations.MIGRATION_47_48
import me.rerere.rikkahub.data.db.migrations.MIGRATION_48_49
import me.rerere.rikkahub.data.db.migrations.MIGRATION_49_50
import me.rerere.rikkahub.data.repository.MemorySearchIndex
import me.rerere.rikkahub.data.repository.MemoryRetriever
import me.rerere.rikkahub.memory.AndroidMemoryWorkScheduler
import me.rerere.rikkahub.memory.DefaultMemoryV2Coordinator
import me.rerere.rikkahub.memory.MemoryCaptureStore
import me.rerere.rikkahub.memory.MemoryEmergencyGate
import me.rerere.rikkahub.memory.MemoryExtractor
import me.rerere.rikkahub.memory.MemoryProcessingStore
import me.rerere.rikkahub.memory.MemoryMetadataReconciler
import me.rerere.rikkahub.memory.DefaultMemoryMutationCoordinator
import me.rerere.rikkahub.memory.MemoryMutationCoordinator
import me.rerere.rikkahub.memory.MemoryV2Coordinator
import me.rerere.rikkahub.memory.MemoryWorkScheduler
import me.rerere.rikkahub.memory.ProviderMemoryExtractor
import me.rerere.rikkahub.memory.RoomMemoryCaptureStore
import me.rerere.rikkahub.memory.RoomMemoryProcessingStore
import me.rerere.rikkahub.memory.dreaming.store.DreamObserverStore
import me.rerere.rikkahub.memory.dreaming.store.DreamPrivacyScrubber
import me.rerere.rikkahub.memory.dreaming.store.DreamSynthesisStore
import me.rerere.rikkahub.memory.dreaming.store.RoomDreamObserverStore
import me.rerere.rikkahub.memory.dreaming.store.RoomDreamPrivacyScrubber
import me.rerere.rikkahub.memory.dreaming.store.RoomDreamSynthesisStore
import me.rerere.rikkahub.memory.dreaming.diagnostics.DreamObserverDiagnostics
import me.rerere.rikkahub.memory.dreaming.diagnostics.StoreDreamObserverDiagnostics
import me.rerere.rikkahub.memory.dreaming.input.DreamInputBuilder
import me.rerere.rikkahub.memory.dreaming.orchestration.DreamEpochClock
import me.rerere.rikkahub.memory.dreaming.orchestration.DreamSynthesisOrchestrator
import me.rerere.rikkahub.memory.dreaming.orchestration.DreamSynthesisOrchestratorConfig
import me.rerere.rikkahub.memory.dreaming.runtime.DreamObserverRuntime
import me.rerere.rikkahub.memory.dreaming.runtime.DreamAppIdleTracker
import me.rerere.rikkahub.memory.dreaming.runtime.DreamBudgetGate
import me.rerere.rikkahub.memory.dreaming.runtime.DreamDailyUsageStore
import me.rerere.rikkahub.memory.dreaming.runtime.DreamInitialSourceTimezoneSource
import me.rerere.rikkahub.memory.dreaming.runtime.DreamSnapshotProjectionReader
import me.rerere.rikkahub.memory.dreaming.runtime.DreamingCostPolicySource
import me.rerere.rikkahub.memory.dreaming.runtime.DreamingFeatureFlagSource
import me.rerere.rikkahub.memory.dreaming.runtime.DreamingPreferencesSource
import me.rerere.rikkahub.memory.dreaming.runtime.DreamSynthesisCoordinator
import me.rerere.rikkahub.memory.dreaming.runtime.DreamSynthesisSchedulingStore
import me.rerere.rikkahub.memory.dreaming.runtime.DreamSynthesisRuntime
import me.rerere.rikkahub.memory.dreaming.runtime.DeviceDreamInitialSourceTimezoneSource
import me.rerere.rikkahub.memory.dreaming.runtime.ProcessLifecycleDreamAppIdleTracker
import me.rerere.rikkahub.memory.dreaming.runtime.SharedPreferencesDreamAppIdleStateStore
import me.rerere.rikkahub.memory.dreaming.runtime.RoomDreamSnapshotProjectionReader
import me.rerere.rikkahub.memory.dreaming.runtime.SettingsDreamingPreferencesSource
import me.rerere.rikkahub.memory.dreaming.store.RoomDreamSynthesisSchedulingStore
import me.rerere.rikkahub.memory.dreaming.review.DefaultDreamReviewRepository
import me.rerere.rikkahub.memory.dreaming.review.DreamAuthorityCorrectionPort
import me.rerere.rikkahub.memory.dreaming.review.DreamReviewRepository
import me.rerere.rikkahub.memory.dreaming.review.DreamReviewStore
import me.rerere.rikkahub.memory.dreaming.review.MemoryMutationDreamAuthorityCorrectionPort
import me.rerere.rikkahub.memory.dreaming.review.RoomDreamReviewStore
import me.rerere.rikkahub.memory.dreaming.source.DreamSourceReader
import me.rerere.rikkahub.memory.dreaming.source.RoomDreamSourceReader
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamProposalValidator
import me.rerere.rikkahub.memory.dreaming.synthesis.DreamSynthesizer
import me.rerere.rikkahub.memory.dreaming.synthesis.ProviderDreamSynthesizer
import me.rerere.rikkahub.memory.dreaming.work.AndroidDreamObserverWorkScheduler
import me.rerere.rikkahub.memory.dreaming.work.AndroidDreamSynthesisWorkScheduler
import me.rerere.rikkahub.memory.dreaming.work.DreamObserverCommitSignal
import me.rerere.rikkahub.memory.dreaming.work.DreamObserverWorkScheduler
import me.rerere.rikkahub.memory.dreaming.work.DreamSynthesisWorkScheduler
import me.rerere.rikkahub.pet.PetDialogueRepository
import me.rerere.rikkahub.pet.PetHandoffCoordinator
import me.rerere.rikkahub.pet.PetDiaryToolProvider
import me.rerere.rikkahub.pet.PetDialogueGenerator
import me.rerere.rikkahub.pet.PetPersonaSource
import me.rerere.rikkahub.pet.PetSummaryScheduler
import me.rerere.rikkahub.pet.AndroidPetSummaryScheduler
import me.rerere.rikkahub.pet.PetDiarySummarizer
import me.rerere.rikkahub.pet.PetHandoffRecovery
import me.rerere.rikkahub.pet.PetDiagnostics
import me.rerere.rikkahub.pet.behavior.PetActionTraceStore
import me.rerere.rikkahub.pet.behavior.PetRuntimeDiagnostics
import me.rerere.rikkahub.data.db.RoomCommandTransactionRunner
import me.rerere.rikkahub.service.chat.CommandAuthorityEventPort
import me.rerere.rikkahub.service.chat.NoOpCommandAuthorityEventPort
import me.rerere.rikkahub.service.chat.CommandStateTransaction
import me.rerere.rikkahub.service.chat.CommandTransactionRunner
import me.rerere.rikkahub.service.chat.DurableCommandQueue
import me.rerere.rikkahub.data.ai.mcp.McpManager
import me.rerere.rikkahub.data.agentrun.AgentRunBootRecovery
import me.rerere.rikkahub.data.agentrun.AgentRunRepository
import me.rerere.rikkahub.data.sync.webdav.WebDavSync
import me.rerere.search.SearchService
import me.rerere.rikkahub.data.sync.S3Sync
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.logging.HttpLoggingInterceptor
import org.koin.dsl.module
import org.koin.core.qualifier.named
import me.rerere.rikkahub.data.alarm.AlarmRepository
import me.rerere.rikkahub.data.alarm.AlarmScheduler
import retrofit2.Retrofit
import retrofit2.converter.kotlinx.serialization.asConverterFactory
import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.UUID

val dataSourceModule = module {
    single {
        SettingsStore(context = get(), scope = get())
    }

    single {
        val context: Context = get()
        Room.databaseBuilder(context, AppDatabase::class.java, "rikka_hub")
            .setJournalMode(RoomDatabase.JournalMode.WRITE_AHEAD_LOGGING)
            .addMigrations(
                Migration_6_7,
                Migration_11_12,
                Migration_13_14,
                Migration_14_15,
                Migration_15_16,
                Migration_23_24,
                MIGRATION_26_27,
                MIGRATION_27_28,
                MIGRATION_28_29,
                MIGRATION_29_30,
                MIGRATION_30_31,
                MIGRATION_31_32,
                MIGRATION_32_33,
                MIGRATION_33_34,
                MIGRATION_34_35,
                MIGRATION_35_36,
                MIGRATION_36_37,
                MIGRATION_37_38,
                MIGRATION_38_39,
                MIGRATION_39_40,
                MIGRATION_40_41,
                MIGRATION_41_42,
                MIGRATION_42_43,
                MIGRATION_43_44,
                MIGRATION_44_45,
                MIGRATION_45_46,
                MIGRATION_46_47,
                MIGRATION_47_48,
                MIGRATION_48_49,
                MIGRATION_49_50,
            )
            .addCallback(object : RoomDatabase.Callback() {
                override fun onCreate(db: SupportSQLiteDatabase) {
                    me.rerere.rikkahub.data.db.migrations.ensureLearningOutboxStreamSentinel(
                        db = db,
                        streamId = UUID.randomUUID().toString(),
                        createdAtMs = System.currentTimeMillis(),
                    )
                    me.rerere.rikkahub.data.db.migrations.requireHealthyLearningOutboxV47(db)
                }

                override fun onOpen(db: SupportSQLiteDatabase) {
                    me.rerere.rikkahub.data.db.migrations.recoverOrphanedDreamSynthesisRuns(db)
                    val dictDir = SimpleDictManager.extractDict(context)
                    val cursor = db.query("SELECT jieba_dict(?)", arrayOf(dictDir.absolutePath))
                    cursor.use {
                        if (it.moveToFirst()) {
                            val result = it.getString(0)
                            val success = result?.trimEnd('/') == dictDir.absolutePath.trimEnd('/')
                            if (!success) {
                                android.util.Log.e(
                                    "DataSourceModule",
                                    "jieba_dict failed: $result, path=${dictDir.absolutePath}"
                                )
                            }
                        }
                    }
                    db.execSQL(me.rerere.rikkahub.data.db.fts.MESSAGE_FTS_CREATE_SQL.trimIndent())
                    ensureMemoryFtsSchema(db)
                }
            })
            .openHelperFactory(createAppSQLiteOpenHelperFactory(context))
            .build()
    }

    // Command authority state commits atomically in the primary Room database.
    single<CommandTransactionRunner> { RoomCommandTransactionRunner(database = get()) }
    single<CommandAuthorityEventPort> { NoOpCommandAuthorityEventPort }
    single {
        CommandStateTransaction(
            dao = get<AppDatabase>().pendingChatCommandDao(),
            transactions = get(),
            events = get(),
        )
    }
    single {
        DurableCommandQueue(
            dao = get<AppDatabase>().pendingChatCommandDao(),
            commandStateTransaction = get(),
        )
    }
    single { get<AppDatabase>().learningSourceAuthorityDao() }
    single<me.rerere.rikkahub.data.authority.source.MessageSourceTransitionInvalidationPort> {
        me.rerere.rikkahub.data.authority.source.NoOpMessageSourceTransitionInvalidationPort
    }
    single {
        me.rerere.rikkahub.data.authority.source.RoomConversationSourceAuthorityStore(
            dao = get(),
            isInAuthorityTransaction = { get<AppDatabase>().inTransaction() },
        )
    }
    single<me.rerere.rikkahub.data.authority.source.ConversationSourceInitialCaptureGate> {
        me.rerere.rikkahub.data.authority.source.DenyConversationSourceInitialCapture
    }
    single<me.rerere.rikkahub.data.authority.source.SourceInvalidationAuthorityEventPort> {
        me.rerere.rikkahub.data.authority.source.NoOpSourceInvalidationAuthorityEventPort
    }
    single {
        me.rerere.rikkahub.data.authority.source.ConversationSourceAuthorityWriter(
            store = get<me.rerere.rikkahub.data.authority.source.RoomConversationSourceAuthorityStore>(),
            events = get(),
            transitionInvalidations = get(),
            initialCaptureGate = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.authority.transaction
            .TransientConversationFinalizationAuthorityCoordinator(
                transactions = get(),
                sources = get(),
            )
    }
    single { me.rerere.rikkahub.data.authority.transaction.CommandStateAdmissionAuthorityAdapter(get()) }
    single<me.rerere.rikkahub.data.authority.transaction.CommandCompletionAuthorityPort> {
        me.rerere.rikkahub.data.authority.transaction.CommandStateCompletionAuthorityAdapter(get())
    }
    single {
        me.rerere.rikkahub.data.authority.transaction.CommandAdmissionAuthorityCoordinator(
            transactions = get(),
            sources = get(),
            commands = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.authority.transaction.WaitingApprovalAuthorityCoordinator(
            transactions = get(),
            sources = get(),
            commands = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.authority.transaction.FinalConversationAuthorityCoordinator(
            transactions = get(),
            sources = get(),
            commands = get(),
        )
    }
    single { me.rerere.rikkahub.owner.OwnerOperationBootRecovery(get<AppDatabase>().hostOperationDao()) }

    single {
        AssistantTemplateLoader(settingsStore = get())
    }

    single {
        PebbleEngine.Builder()
            .loader(get<AssistantTemplateLoader>())
            .defaultLocale(Locale.getDefault())
            .autoEscaping(false)
            .build()
    }

    single { TemplateTransformer(engine = get(), settingsStore = get()) }

    single {
        get<AppDatabase>().conversationDao()
    }

    single {
        get<AppDatabase>().memoryDao()
    }

    single {
        get<AppDatabase>().memoryV2Dao()
    }

    single {
        get<AppDatabase>().dreamDao()
    }

    single {
        get<AppDatabase>().dreamExperienceDao()
    }

    single {
        me.rerere.rikkahub.memory.dreaming.experience.RoomDreamExperienceStore(
            database = get(),
            dao = get(),
            legacyDreamDao = get(),
        )
    }

    single {
        me.rerere.rikkahub.memory.dreaming.experience.ConversationEpisodeAdapter(store = get())
    }

    single {
        me.rerere.rikkahub.memory.dreaming.experience.DreamMemoryAdapter(
            memoryDao = get(),
            store = get(),
        )
    }

    single {
        me.rerere.rikkahub.memory.dreaming.experience.DreamExperienceIngestor(
            conversationAdapter = get(),
            memoryAdapter = get(),
            store = get(),
            conversationRepository = get(),
            synthesisCoordinator = get(),
        )
    }

    // Dormant M4 persistence primitives only; no synthesizer, Worker, or runtime consumer is
    // registered by the schema migration.
    single {
        get<AppDatabase>().dreamSynthesisDao()
    }

    single {
        me.rerere.rikkahub.memory.dreaming.runtime.DreamIntrospectionToolProvider(
            experienceDao = get(),
            synthesisDao = get(),
            dreamDao = get(),
        )
    }

    // M2 Observer only replays payload-free epochs locally. It has no model, prompt, provider, or
    // GenerationHandler dependency; all Dream generation/use flags remain disabled.
    single<DreamObserverStore> {
        RoomDreamObserverStore(database = get(), dreamDao = get())
    }
    single<DreamObserverWorkScheduler>(createdAtStart = true) {
        AndroidDreamObserverWorkScheduler(context = get())
    }
    single<DreamSynthesisWorkScheduler> { AndroidDreamSynthesisWorkScheduler(context = get()) }
    single(createdAtStart = true) {
        DreamObserverCommitSignal(
            database = get<AppDatabase>(),
            scheduler = get<DreamObserverWorkScheduler>(),
        )
    }
    single {
        DreamObserverRuntime(store = get(), scheduler = get())
    }
    single<DreamObserverDiagnostics> {
        StoreDreamObserverDiagnostics(store = get())
    }

    single {
        SettingsDreamingPreferencesSource(
            settingsStore = get(),
            trustedSchemaReady = true,
        )
    }
    single<DreamingPreferencesSource> { get<SettingsDreamingPreferencesSource>() }
    single<DreamingFeatureFlagSource> { get<SettingsDreamingPreferencesSource>() }
    single<DreamingCostPolicySource> { get<SettingsDreamingPreferencesSource>() }
    single<DreamSnapshotProjectionReader> {
        RoomDreamSnapshotProjectionReader(
            database = get(),
            synthesisDao = get(),
            experienceDao = get(),
        )
    }
    single<DreamInitialSourceTimezoneSource> { DeviceDreamInitialSourceTimezoneSource }
    single<DreamAppIdleTracker>(createdAtStart = true) {
        ProcessLifecycleDreamAppIdleTracker(
            clock = get(),
            stateStore = SharedPreferencesDreamAppIdleStateStore(context = get()),
        )
    }
    single<DreamSourceReader> { RoomDreamSourceReader(database = get()) }
    single { DreamInputBuilder(sourceReader = get()) }
    single<DreamSynthesizer> { ProviderDreamSynthesizer(settingsStore = get(), providerManager = get()) }
    single { DreamProposalValidator() }
    single<DreamEpochClock> { DreamEpochClock { System.currentTimeMillis() } }
    single {
        RoomDreamSynthesisSchedulingStore(
            database = get(),
            dreamDao = get(),
            experienceDao = get(),
        )
    }
    single<DreamSynthesisSchedulingStore> { get<RoomDreamSynthesisSchedulingStore>() }
    single<DreamDailyUsageStore> { get<RoomDreamSynthesisSchedulingStore>() }
    single {
        DreamBudgetGate(
            policySource = get(),
            usageStore = get(),
        )
    }
    single<DreamPrivacyScrubber> {
        RoomDreamPrivacyScrubber(
            database = get(),
            dreamDao = get(),
            synthesisDao = get(),
            memoryDao = get(),
            memoryV2Dao = get(),
            json = get(),
        )
    }
    single<DreamSynthesisStore> {
        RoomDreamSynthesisStore(
            database = get(),
            dreamDao = get(),
            experienceDao = get(),
            synthesisDao = get(),
            memoryDao = get(),
            memoryV2Dao = get(),
            observerStore = get(),
            featureFlags = get(),
            json = get(),
        )
    }
    single {
        DreamSynthesisOrchestrator(
            store = get(),
            inputBuilder = get(),
            synthesizer = get(),
            validator = get(),
            clock = get(),
            config = DreamSynthesisOrchestratorConfig(
                compilerRevision = "dream-snapshot-compiler-v2",
                // Dream output is strict JSON, but reasoning-capable OpenCode/DeepSeek V4 models
                // cannot actually disable reasoning: LOW/OFF normalize to the provider's `high`
                // effort. 2k can therefore be exhausted by reasoning before a JSON answer is
                // emitted. Keep the larger allowance scoped to Dream rather than changing normal
                // chat or Memory extraction behavior.
                maxOutputTokens = 4_096,
                leaseDurationMs = 15L * 60_000L,
                heartbeatIntervalMs = 2L * 60_000L,
            ),
            budgetGate = get(),
        )
    }
    single {
        DreamSynthesisRuntime(
            dreamDao = get(),
            featureFlags = get(),
            timezoneSource = get(),
            orchestrator = get(),
            clock = get(),
            policySource = get(),
            idleTracker = get(),
        )
    }
    single(createdAtStart = true) {
        val coordinator = DreamSynthesisCoordinator(
            store = get(),
            featureFlags = get(),
            policySource = get(),
            scheduler = get(),
            clock = get(),
        )
        get<AppScope>().launch(Dispatchers.IO) {
            coordinator.armStartupAndPeriodicRecovery()
        }
        coordinator
    }

    single<DreamReviewStore> {
        RoomDreamReviewStore(
            database = get(),
            dreamDao = get(),
            experienceDao = get(),
            synthesisDao = get(),
            memoryDao = get(),
            memoryV2Dao = get(),
            featureFlags = get(),
            json = get(),
        )
    }
    single<DreamAuthorityCorrectionPort> {
        MemoryMutationDreamAuthorityCorrectionPort(
            mutationCoordinator = get(),
            observerStore = get(),
        )
    }
    single<DreamReviewRepository> {
        DefaultDreamReviewRepository(
            store = get(),
            authority = get(),
            experienceStore = get(),
            synthesisCoordinator = get(),
        )
    }

    single {
        get<AppDatabase>().genMediaDao()
    }

    single {
        get<AppDatabase>().messageNodeDao()
    }

    single {
        get<AppDatabase>().managedFileDao()
    }

    single {
        get<AppDatabase>().favoriteDao()
    }

    single {
        get<AppDatabase>().workspaceDao()
    }

    single {
        MessageFtsManager(get())
    }
    single<MemorySearchIndex> { MemoryFtsManager(get()) }
    single { MemoryRetriever(get()) }
    single {
        me.rerere.rikkahub.data.repository.MemoryRetrievalDiagnosticsStore(
            filesDir = get<Context>().filesDir,
        )
    }
    single { MemoryMetadataReconciler(get(), get()) }
    single<MemoryWorkScheduler>(createdAtStart = true) { AndroidMemoryWorkScheduler(get()) }
    single<MemoryCaptureStore> { RoomMemoryCaptureStore(get()) }
    single<MemoryProcessingStore> {
        RoomMemoryProcessingStore(
            database = get(),
            memoryDao = get(),
            memoryV2Dao = get(),
            retriever = get(),
            json = get(),
            dreamObserverStore = get(),
            dreamPrivacyScrubber = get(),
        )
    }
    single<MemoryMutationCoordinator> { DefaultMemoryMutationCoordinator(get()) }
    single<MemoryExtractor> { ProviderMemoryExtractor(get(), get()) }
    single<MemoryEmergencyGate> {
        val safetySettings = get<me.rerere.rikkahub.data.ai.AgentSafetySettings>()
        MemoryEmergencyGate { safetySettings.emergencyStopFlow.first() }
    }
    single<MemoryV2Coordinator> {
        DefaultMemoryV2Coordinator(
            captureStore = get(),
            workScheduler = get(),
            processingStore = get(),
            extractor = get(),
            emergencyGate = get(),
            idGenerator = { kotlin.uuid.Uuid.random().toString() },
        )
    }

    // Phase 24 — unified AgentRun ledger. DAO + the single shared writer/reader + the
    // boot-recovery sweep. AgentRunRepository has no cross-dependencies (only the DAO), so
    // there is no DI-cycle risk here.
    single { get<AppDatabase>().agentRunDao() }
    single { AgentRunRepository(get()) }
    single { AgentRunBootRecovery(context = get(), repository = get()) }

    // Authoritative per-execution ledger. This is intentionally distinct from AgentRun: one
    // AgentRun may own many runtime handles, each with its own cancellation/recovery outcome.
    single { get<AppDatabase>().executionRecordDao() }
    single { get<AppDatabase>().executionEventDao() }
    single { get<AppDatabase>().pendingToolApprovalDao() }
    single { get<AppDatabase>().pendingChatCommandDao() }
    single { get<AppDatabase>().petDialogueDao() }
    single<PetSummaryScheduler> { AndroidPetSummaryScheduler(context = get()) }
    single { PetDialogueRepository(database = get(), dao = get(), summaryScheduler = get()) }
    single { get<AppDatabase>().toolExperienceDao() }
    single { me.rerere.rikkahub.toolcatalog.ToolExperienceRepository(database = get(), dao = get()) }
    single { get<AppDatabase>().toolShortcutDao() }
    single { me.rerere.rikkahub.toolcatalog.ToolShortcutRepository(database = get(), dao = get()) }
    single {
        me.rerere.rikkahub.diagnostics.ToolCatalogDiagnostics(
            experiences = get(),
            shortcuts = get(),
        )
    }
    single {
        PetHandoffCoordinator(
            database = get(),
            dao = get(),
            pendingCommandDao = get(),
            dialogueRepository = get(),
            conversationRepository = get(),
            chatService = get(),
            authority = get(),
            appScope = get<AppScope>(),
        )
    }
    single { PetHandoffRecovery(dao = get(), coordinator = get()) }
    single { PetDiaryToolProvider(dao = get(), repository = get()) }
    single { PetPersonaSource(settingsStore = get()) }
    single { PetDialogueGenerator(settingsStore = get(), providerManager = get(), secretVault = get()) }
    single { PetDiarySummarizer(settingsStore = get(), providerManager = get()) }
    single { PetActionTraceStore() }
    single { PetRuntimeDiagnostics() }
    single {
        PetDiagnostics(
            context = get(),
            dao = get(),
            settingsStore = get(),
            summaryScheduler = get(),
            actionTraceStore = get(),
            runtimeDiagnosticsStore = get(),
        )
    }
    single { me.rerere.rikkahub.data.execution.ExecutionConsistencyMetrics() }
    single {
        me.rerere.rikkahub.data.execution.ExecutionStateTransaction(
            database = get(),
            recordDao = get(),
            eventDao = get(),
            metrics = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.execution.ExecutionRetentionManager(
            recordDao = get(),
            eventDao = get(),
            approvalDao = get(),
            scope = get<AppScope>(),
        )
    }
    single {
        me.rerere.rikkahub.data.execution.ExecutionRepository(
            dao = get(),
            transaction = get(),
            retention = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.execution.ExecutionMessageAuthorityBinder(
            database = get(),
            dao = get(),
        )
    }
    single {
        me.rerere.rikkahub.toolcatalog.ToolExperienceRecorder(
            executionRepository = get(),
            experiences = get(),
            shortcuts = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.execution.ManagedExecutionRegistration(
            repository = get(),
            trackingHealth = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.execution.SecondUserApprovalLifecycle(
            database = get(),
            conversationRepository = get(),
            approvalDao = get(),
            executionRepository = get(),
            retentionManager = get(),
            messageAuthorityBinder = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.execution.SecondUserApprovalRecovery(
            settingsStore = get(),
            conversationRepository = get(),
            approvalDao = get(),
            lifecycle = get(),
            authorityService = get(),
        )
    }
    single { get<AppDatabase>().capabilityGrantDao() }
    single { me.rerere.rikkahub.data.capability.CapabilityGrantRepository(get()) }
    single<me.rerere.rikkahub.data.capability.CapabilityPolicyEngine> {
        me.rerere.rikkahub.data.capability.DefaultCapabilityPolicyEngine(
            grants = { get<me.rerere.rikkahub.data.capability.CapabilityGrantRepository>().current() },
        )
    }
    single<me.rerere.rikkahub.data.execution.ManagedExecutionVerifier> {
        me.rerere.rikkahub.data.execution.LiveManagedExecutionVerifier(
            ledgerVerifier = me.rerere.rikkahub.data.execution.LedgerManagedExecutionVerifier(get()),
            termuxSupervisor = get(),
            tokenProvider = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.execution.ExecutionBootRecovery(
            repository = get(),
            approvalDao = get(),
            reconciler = get(),
            cancellationCoordinator = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.execution.ManagedExecutionCallerResolver(
            ledger = get(),
            workspaceManager = get(),
        )
    }
    single<me.rerere.rikkahub.data.execution.ExecutionRuntimeProbe> {
        me.rerere.rikkahub.data.execution.DefaultExecutionRuntimeProbe(
            workspaceManager = get(),
            coordinator = get(),
            callerResolver = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.execution.ExecutionReconciler(
            repository = get(),
            probe = get(),
            metrics = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.execution.ExecutionProbeScheduler(
            context = get(),
            scope = get<AppScope>(),
            repository = get(),
            reconciler = get(),
            workspaceManager = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.execution.CancellationCoordinator(
            scope = get<AppScope>(),
            repository = get(),
            runtimeProbe = get(),
            callerResolver = get(),
            managedCoordinator = get(),
        )
    }
    single {
        me.rerere.rikkahub.diagnostics.ExecutionConsistencyDoctor(
            repository = get(),
            eventDao = get(),
            approvalDao = get(),
            conversationRepository = get(),
            workspaceManager = get(),
            reconciler = get(),
            approvalRecovery = get(),
            retentionManager = get(),
            trackingHealth = get(),
            metrics = get(),
        )
    }
    single<me.rerere.rikkahub.data.ai.execution.CriticalToolLifecycleSink> {
        me.rerere.rikkahub.data.execution.ExecutionRecordCriticalToolLifecycleSink(get())
    }
    single { me.rerere.rikkahub.data.ai.execution.ExecutionTrackingHealth() }

    // Alarm
    single { get<AppDatabase>().alarmDao() }
    single { AlarmRepository(get()) }
    single { AlarmScheduler(context = get(), repository = get()) }

    single {
        McpManager(
            context = get(),
            settingsStore = get(),
            appScope = get(),
            filesManager = get(),
            secretVault = get(),
        )
    }

    single { BoundedDreamRuntimeTelemetryStore() }
    single<DreamRuntimeDiagnosticsSink> { get<BoundedDreamRuntimeTelemetryStore>() }
    single<DreamRuntimeUsageRecorder> { get<BoundedDreamRuntimeTelemetryStore>() }

    single {
        GenerationHandler(
            context = get(),
            providerManager = get(),
            json = get(),
            memoryRepo = get(),
            conversationRepo = get(),
            aiLoggingManager = get(),
            systemPromptBuilder = get(),
            toolExecutionGate = get(),
            toolRuntime = get(),
            toolStartableResolver = get(),
            toolExecutionBatchCoordinator = get(),
            contextBroker = get(),
            contextDiagnosticsStore = get(),
            secondUserSecretVault = get(),
            secretPlaintextSessions = get(),
            ephemeralToolResults = get(),
            runtimeSecretRedactor = get(),
            toolExperienceRecorder = get(),
            dreamingFeatureFlags = get(),
            dreamSnapshotProjectionReader = get(),
            dreamRuntimeUsageRecorder = get(),
            dreamRuntimeDiagnosticsSink = get(),
        )
    }

    single { me.rerere.rikkahub.data.ai.SystemPromptBuilder() }

    single<OkHttpClient> {
        val acceptLang = AcceptLanguageBuilder.fromAndroid(get())
            .build()
        OkHttpClient.Builder()
            .connectTimeout(20, TimeUnit.SECONDS)
            .readTimeout(10, TimeUnit.MINUTES)
            .writeTimeout(120, TimeUnit.SECONDS)
            .followSslRedirects(true)
            .followRedirects(true)
            .retryOnConnectionFailure(true)
            .addInterceptor { chain ->
                val originalRequest = chain.request()
                val requestBuilder = originalRequest.newBuilder()
                    .addHeader(HttpHeaders.AcceptLanguage, acceptLang)

                if (originalRequest.header(HttpHeaders.UserAgent) == null) {
                    requestBuilder.addHeader(HttpHeaders.UserAgent, "RikkaHub-Android/${BuildConfig.VERSION_NAME}")
                }

                chain.proceed(requestBuilder.build())
            }
            .addNetworkInterceptor { chain ->
                val request = chain.request()
                val contentTypeHeader = request.header("Content-Type")
                if (
                    contentTypeHeader != null &&
                    contentTypeHeader.contains(";") &&
                    contentTypeHeader.substringBefore(";").trim().equals("application/json", ignoreCase = true)
                ) {
                    chain.proceed(
                        request.newBuilder()
                            .header("Content-Type", contentTypeHeader.substringBefore(";").trim())
                            .build()
                    )
                } else {
                    chain.proceed(request)
                }
            }
            .addNetworkInterceptor(RequestLoggingInterceptor())
            .addInterceptor(AIRequestInterceptor())
            .apply {
                // HEADERS-level logging prints Authorization: Bearer <api-key> to logcat.
                // Debug-only so release builds never leak provider keys to logcat.
                if (BuildConfig.DEBUG) {
                    addInterceptor(HttpLoggingInterceptor().apply {
                        level = HttpLoggingInterceptor.Level.HEADERS
                    })
                }
            }
            .build().also { SearchService.init(it, get()) }
    }

    single<OkHttpClient>(named("codex")) {
        OkHttpClient.Builder()
            .connectTimeout(20, TimeUnit.SECONDS)
            .readTimeout(10, TimeUnit.MINUTES)
            .writeTimeout(120, TimeUnit.SECONDS)
            .followSslRedirects(true)
            .followRedirects(true)
            .retryOnConnectionFailure(true)
            .build()
    }

    single {
        SponsorAPI.create(get())
    }

    single {
        CodexAccountRepository(
            store = CodexCredentialStore(context = get(), json = get()),
            client = get(named("codex")),
            json = get(),
        )
    }

    single {
        CodexOAuthManager(
            context = get(),
            scope = get<AppScope>(),
            client = get(named("codex")),
            repository = get(),
        )
    }

    single {
        val settingsStore: me.rerere.rikkahub.data.datastore.SettingsStore = get()
        val codexRepository: CodexAccountRepository = get()
        val json: Json = get()
        ProviderManager(client = get(), context = get()).also { pm ->
            pm.registerProvider(
                "local_litert",
                me.rerere.locallm.litert.LiteRtProvider(
                    context = get(),
                    runtime = get(),
                    prefs = get(),
                    settingsUpdater = { transform ->
                        settingsStore.update { old -> old.copy(providers = transform(old.providers)) }
                    },
                ),
            )
            pm.registerProvider(
                "codex",
                CodexProvider(
                    context = get(),
                    client = get(named("codex")),
                    repository = codexRepository,
                    json = json,
                )
            )
        }
    }

    single {
        WebDavSync(
            settingsStore = get(),
            json = get(),
            context = get(),
            httpClient = get(),
            appDatabase = get()
        )
    }

    single { me.rerere.rikkahub.data.sync.LocalBackupFacade(get(), get(), get()) }

    single<HttpClient> {
        HttpClient(OkHttp) {
            engine {
                config {
                    connectTimeout(20, TimeUnit.SECONDS)
                    readTimeout(10, TimeUnit.MINUTES)
                    writeTimeout(120, TimeUnit.SECONDS)
                    followSslRedirects(true)
                    followRedirects(true)
                    retryOnConnectionFailure(true)
                }
            }
        }
    }

    single {
        S3Sync(
            settingsStore = get(),
            json = get(),
            context = get(),
            httpClient = get(),
            appDatabase = get()
        )
    }

    single<Retrofit> {
        Retrofit.Builder()
            .baseUrl("https://api.rikka-ai.com")
            .addConverterFactory(get<Json>().asConverterFactory("application/json; charset=UTF8".toMediaType()))
            .build()
    }

    single<RikkaHubAPI> {
        get<Retrofit>().create(RikkaHubAPI::class.java)
    }
}
