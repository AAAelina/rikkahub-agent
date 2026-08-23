package me.rerere.rikkahub.di

import kotlinx.coroutines.flow.first
import kotlinx.serialization.json.Json
import me.rerere.rikkahub.plugin.containsApprovedModelTool
import me.rerere.highlight.Highlighter
import me.rerere.rikkahub.AppScope
import me.rerere.rikkahub.data.ai.AgentSafetySettings
import me.rerere.rikkahub.data.ai.AILoggingManager
import me.rerere.rikkahub.data.ai.ToolExecutionGate
import me.rerere.rikkahub.data.ai.tools.LocalTools
import me.rerere.rikkahub.data.ai.tools.local.BiometricResultBuffer
import me.rerere.rikkahub.data.ai.tools.local.CameraResultBuffer
import me.rerere.rikkahub.data.event.AppEventBus
import me.rerere.rikkahub.data.ai.tools.local.InteractiveToolStreamer
import me.rerere.rikkahub.data.repository.ScheduledJobRepository
import me.rerere.rikkahub.data.repository.SshHostRepository
import me.rerere.rikkahub.data.repository.TelegramChatRepository
import me.rerere.rikkahub.data.notifications.NotificationListenerPreferences
import me.rerere.rikkahub.data.telegram.TelegramBotClient
import me.rerere.rikkahub.data.telegram.TelegramBotPreferences
import me.rerere.rikkahub.data.telegram.TelegramCredentialResolver
import me.rerere.rikkahub.service.ChatService
import me.rerere.rikkahub.service.CronJobScheduler
import me.rerere.rikkahub.utils.EmojiData
import me.rerere.rikkahub.utils.EmojiUtils
import me.rerere.rikkahub.utils.JsonInstant
import me.rerere.rikkahub.utils.SoundEffectPlayer
import me.rerere.rikkahub.utils.UpdateChecker
import me.rerere.rikkahub.web.WebServerManager
import me.rerere.tts.provider.TTSManager
import org.koin.dsl.module

val appModule = module {
    single<Json> { JsonInstant }
    single<me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingClock> {
        me.rerere.rikkahub.diagnostics.agenttiming.AndroidAgentTimingClock
    }
    single {
        me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingStore(clock = get())
    }
    // Learning sees only Dreaming's bounded public projection. The adapter never opens Dream DAOs
    // and rejects every Learning scope that has no exact private Memory authority counterpart.
    single<me.rerere.rikkahub.learning.api.IdentityContextProvider> {
        me.rerere.rikkahub.learning.adapters.DreamingIdentityAdapter(
            featureFlags = get(),
            projectionReader = get(),
        )
    }
    single {
        me.rerere.rikkahub.learning.adapters.AgentTimingLearningAdapter(
            store = get(),
        )
    }
    single {
        Highlighter(get())
    }

    single {
        AppEventBus()
    }

    single { CameraResultBuffer() }
    single { BiometricResultBuffer() }
    // Phase 25 — NFC reader-mode + SAF directory-picker Activity bridges, and the SAF
    // tree-grant store backing the ExternalStorage tools.
    single { me.rerere.rikkahub.data.ai.tools.local.NfcResultBuffer() }
    single { me.rerere.rikkahub.data.ai.tools.local.SafPickerResultBuffer() }
    single { me.rerere.rikkahub.data.storage.StorageVolumeGrantStore(get()) }

    single { ScheduledJobRepository(get<me.rerere.rikkahub.data.db.AppDatabase>().scheduledJobDao()) }
    single { me.rerere.rikkahub.data.repository.ScheduledJobRunRepository(get<me.rerere.rikkahub.data.db.AppDatabase>().scheduledJobRunDao()) }
    single {
        me.rerere.rikkahub.service.DirectModeActionRunner(
            toolRuntime = get(),
            toolStartableResolver = get(),
            preflight = get(),
        )
    }
    single { CronJobScheduler(get(), get()) }
    single {
        me.rerere.rikkahub.automation.AutomationControlFacade(
            scheduledJobs = get(),
            scheduledJobRuns = get(),
            cronScheduler = get(),
            alarms = get(),
            alarmScheduler = get(),
        )
    }
    single { SshHostRepository(get<me.rerere.rikkahub.data.db.AppDatabase>().sshHostDao()) }
    single { TelegramChatRepository(get<me.rerere.rikkahub.data.db.AppDatabase>().telegramChatDao()) }
    single { TelegramBotPreferences(get()) }
    single { me.rerere.rikkahub.browser.BrowserPreferences(get()) }
    single { me.rerere.rikkahub.data.preferences.TermuxPreferences(get()) }
    single<me.rerere.rikkahub.data.ai.tools.local.TermuxSessionEmergencyController> {
        me.rerere.rikkahub.data.ai.tools.local.AndroidTermuxSessionEmergencyController(get())
    }
    single<me.rerere.rikkahub.data.phone.PhoneCallPreferences> {
        me.rerere.rikkahub.data.phone.DataStorePhoneCallPreferences(get())
    }
    single<me.rerere.rikkahub.data.phone.PhoneCallPlatform> {
        me.rerere.rikkahub.data.phone.AndroidPhoneCallPlatform(get())
    }
    single<me.rerere.rikkahub.data.phone.PhoneCallController> {
        me.rerere.rikkahub.data.phone.DefaultPhoneCallController(get(), get())
    }
    single<me.rerere.rikkahub.data.packageinstaller.ApkInstallController> {
        me.rerere.rikkahub.data.packageinstaller.AndroidApkInstallController(get())
    }
    // Pass 3: Telegram-bound screenshot streamer for headless browser mode. Bound to the
    // [BrowserScreenshotStreamer] interface so [BrowserController.streamScreenshotIfHeadless]
    // can resolve it lazily via Koin without taking a constructor dep — avoids a cycle
    // through TelegramBotClient → TelegramBotPreferences → ... → LocalTools → controller.
    single<me.rerere.rikkahub.browser.BrowserScreenshotStreamer> {
        me.rerere.rikkahub.data.telegram.TelegramBrowserScreenshotStreamer(get(), get(), get())
    }
    // Interactive-tool post-action screenshot streamer for headless mode (Telegram bot /
    // cron / sub-agent). Resolves lazily inside each interactive tool's execute lambda so
    // there's no DI cycle through LocalTools → ChatService → ... → TelegramBotClient.
    single<InteractiveToolStreamer> {
        me.rerere.rikkahub.data.telegram.TelegramInteractiveToolStreamer(get(), get(), get(), get())
    }
    single { me.rerere.rikkahub.data.preferences.ToolApprovalPreferences(get()) }
    single { TelegramCredentialResolver(get(), get()) }
    single { TelegramBotClient { runCatching { kotlinx.coroutines.runBlocking { get<TelegramCredentialResolver>().currentToken() } }.getOrDefault("") } }
    // Phase 24 — Telegram long-poll stall tracker. Shared singleton: TelegramBotService's
    // poll loop calls markUpdate() on every getUpdates; the in-service stall checker and
    // DoctorChecks read it. No cross-dependencies, so no DI-cycle risk.
    single { me.rerere.rikkahub.data.telegram.TelegramPollStallTracker() }
    single { NotificationListenerPreferences(get()) }

    // Phase 13: External Automation Intent API
    single { me.rerere.rikkahub.automation.ExternalAutomationConfig(get()) }
    single {
        me.rerere.rikkahub.automation.ExternalAutomationDispatcher(
            context = get(),
            config = get(),
            chatService = get(),
            conversationRepo = get(),
            settingsStore = get(),
            appScope = get(),
            // Phase 24 — unified AgentRun ledger writer.
            agentRunRepo = get(),
        )
    }

    // Phase 14: Reliability bundle
    single { me.rerere.rikkahub.reliability.GitHubReleaseChecker(get()) }
    single { me.rerere.rikkahub.reliability.BugReportBuilder(get()) }

    // Phase 11: Sub-agents
    single { me.rerere.rikkahub.subagent.SubAgentRegistry() }
    single { me.rerere.rikkahub.subagent.SubAgentExecutionProfileRegistry() }
    single {
        me.rerere.rikkahub.subagent.SubAgentEngine(
            registry = get(),
            executionProfileRegistry = get(),
            // chatService is resolved lazily inside SubAgentEngine to break the
            // ChatService→LocalTools→SubAgentEngine→ChatService cycle. See SubAgentEngine kdoc.
            conversationRepo = get(),
            settingsStore = get(),
            appScope = get(),
            // Phase 24 — unified AgentRun ledger writer. No DI cycle: AgentRunRepository
            // depends only on its DAO.
            agentRunRepo = get(),
        )
    }

    // Phase 16: Skill URL-import
    single {
        me.rerere.rikkahub.skills.SkillUrlImporter(
            skillManager = get<me.rerere.rikkahub.data.files.SkillManager>(),
        )
    }

    // Phase 19B: Skill isolation tester. Eager construction is safe here — ChatService
    // doesn't reach back into SkillTestRunner anywhere, so no DI cycle.
    single {
        me.rerere.rikkahub.skills.SkillTestRunner(
            chatService = get(),
            skillManager = get(),
            conversationRepo = get(),
            settingsStore = get(),
        )
    }

    // Phase 18: JS skills (run_js + secrets store)
    single { me.rerere.rikkahub.skills.js.JsSkillRunner(get()) }
    single { me.rerere.rikkahub.skills.js.SkillSecretsStore(get()) }

    // Phase 12: Workflows
    single {
        val safetySettings = get<AgentSafetySettings>()
        me.rerere.rikkahub.workflow.execution.WorkflowEmergencyController(
            persistedEmergencyStop = safetySettings::isEmergencyStop,
        )
    }
    single<me.rerere.rikkahub.research.ResearchChildGateway> {
        me.rerere.rikkahub.research.SubAgentResearchChildGateway(get(), get())
    }
    single<me.rerere.rikkahub.research.ResearchCompletionNotifier> {
        me.rerere.rikkahub.research.ChatResearchCompletionNotifier()
    }
    single {
        me.rerere.rikkahub.research.ResearchCoordinator(
            childGateway = get(),
            scope = get<AppScope>(),
            completionNotifier = get(),
        )
    }
    single<me.rerere.rikkahub.setup.SetupConfigurationStore> {
        me.rerere.rikkahub.setup.SettingsStoreSetupConfigurationStore(get())
    }
    single<me.rerere.rikkahub.setup.SetupResourceCatalog> {
        me.rerere.rikkahub.setup.RepositorySetupResourceCatalog(
            workspaceRepository = get(),
            skillManager = get(),
        )
    }
    single<me.rerere.rikkahub.setup.SetupTransactionBackend> {
        me.rerere.rikkahub.setup.SettingsSetupTransactionBackend(
            configurationStore = get(),
            resources = get(),
        )
    }
    single<me.rerere.rikkahub.setup.SetupAuditLedger> {
        me.rerere.rikkahub.setup.AgentRunSetupAuditLedger(get())
    }
    single {
        me.rerere.rikkahub.setup.SetupTransactionCoordinator(
            backend = get(),
            auditLedger = get(),
        )
    }
    single {
        me.rerere.rikkahub.workflow.repository.WorkflowRepository(
            workflowDao = get<me.rerere.rikkahub.data.db.AppDatabase>().workflowDao(),
            workflowRunDao = get<me.rerere.rikkahub.data.db.AppDatabase>().workflowRunDao(),
        )
    }
    single { me.rerere.rikkahub.workflow.condition.ContextProvider(get()) }
    single {
        me.rerere.rikkahub.workflow.execution.WorkflowActionRunner(
            toolRuntime = get(),
            toolStartableResolver = get(),
            preflight = get(),
        )
    }
    single {
        me.rerere.rikkahub.workflow.execution.WorkflowEngine(
            repository = get(),
            settingsStore = get(),
            contextProvider = get(),
            actionRunner = get(),
            emergencyController = get(),
        ).also { engine ->
            // Bridge for the repo to notify the engine on delete so the engine's per-workflow
            // lock map doesn't leak. Lazy because both singletons have to exist first.
            get<me.rerere.rikkahub.workflow.repository.WorkflowRepository>().bindEngine(engine)
        }
    }
    single {
        me.rerere.rikkahub.workflow.trigger.TriggerRegistry(
            context = get(),
            appScope = get(),
            workflowRepository = get(),
        )
    }

    single { me.rerere.rikkahub.data.keyboard.KeyboardApiClient(get()) }

    single { me.rerere.rikkahub.assistant.SystemAssistantRoleController(get()) }
    single { me.rerere.rikkahub.security.SecondUserSecretVault(get()) }
    single { me.rerere.rikkahub.security.RuntimeSecretRedactor() }
    single {
        me.rerere.rikkahub.security.SecretPlaintextSessionManager(
            context = get(),
            settingsStore = get(),
            vault = get(),
            safetySettings = get(),
            redactor = get(),
            appScope = get(),
        )
    }
    single { me.rerere.rikkahub.security.SecretEgressGuard(get()) }
    single {
        me.rerere.rikkahub.security.EphemeralToolResultStore(
            egressGuard = get<me.rerere.rikkahub.security.SecretEgressGuard>(),
        ).also { store ->
            get<me.rerere.rikkahub.security.SecretPlaintextSessionManager>()
                .addCloseListener(store::clear)
        }
    }
    single {
        me.rerere.rikkahub.owner.OwnerLocalServiceSupervisor(
            dao = get<me.rerere.rikkahub.data.db.AppDatabase>().hostLocalServiceDao(),
            manager = get(),
            safety = get(),
            httpClient = get(),
            scope = get(),
            specStore = get(),
            termux = get(),
        )
    }
    single { me.rerere.rikkahub.security.SecondUserLegacySecretMigration(get(), get(), get()) }
    single { me.rerere.rikkahub.security.StrongBiometricAuthenticator(get(), get()) }
    single { me.rerere.rikkahub.owner.OwnerServiceSpecStore(get()) }
    single {
        me.rerere.rikkahub.owner.OwnerTermuxServiceLauncher(
            context = get(),
            factory = get(),
            coordinator = get(),
        )
    }
    single {
        me.rerere.rikkahub.assistant.SecondUserAuthorityService(
            settingsStore = get(),
            conversations = me.rerere.rikkahub.assistant.SecondUserAuthorityConversationReader { id ->
                get<me.rerere.rikkahub.data.db.AppDatabase>().conversationDao()
                    .getAssistantIdByConversationId(id.toString())
                    ?.let { raw -> runCatching { kotlin.uuid.Uuid.parse(raw) }.getOrNull() }
            },
            appScope = get(),
        )
    }
    single { me.rerere.rikkahub.assistant.AppStartDestinationResolver(get()) }
    single {
        val settingsStore = get<me.rerere.rikkahub.data.datastore.SettingsStore>()
        val conversationRepository = get<me.rerere.rikkahub.data.repository.ConversationRepository>()
        me.rerere.rikkahub.assistant.SecondUserTargetResolver(
            settingsReader = me.rerere.rikkahub.assistant.SecondUserTargetSettingsReader {
                settingsStore.settingsFlow.first { settings -> !settings.init }
            },
            conversationReader = me.rerere.rikkahub.assistant.SecondUserTargetConversationReader { conversationId ->
                conversationRepository.getAssistantIdOfConversation(conversationId)
            },
            conversationTitleReader = me.rerere.rikkahub.assistant.SecondUserTargetConversationTitleReader {
                    conversationId ->
                conversationRepository.getConversationById(conversationId)?.title
            },
            authorityService = get(),
        )
    }
    single {
        val settingsStore = get<me.rerere.rikkahub.data.datastore.SettingsStore>()
        me.rerere.rikkahub.quickcapture.QuickCaptureTargetResolver(
            settingsReader = me.rerere.rikkahub.quickcapture.QuickCaptureSettingsReader {
                settingsStore.settingsFlow.first { settings -> !settings.init }
            },
            secondUserResolver = get(),
        )
    }
    single<me.rerere.rikkahub.quickcapture.QuickCaptureAccessState> {
        me.rerere.rikkahub.quickcapture.AndroidQuickCaptureAccessState(get())
    }
    single<me.rerere.rikkahub.quickcapture.QuickCaptureNavigator> {
        me.rerere.rikkahub.quickcapture.AndroidQuickCaptureNavigator(get())
    }
    single { me.rerere.rikkahub.quickcapture.ScreenCaptureManager() }
    single {
        val database = get<me.rerere.rikkahub.data.db.AppDatabase>()
        me.rerere.rikkahub.quickcapture.QuickCaptureFileCleaner(
            filesManager = get(),
            conversationDao = database.conversationDao(),
            messageNodeDao = database.messageNodeDao(),
            pendingCommandDao = database.pendingChatCommandDao(),
        )
    }
    single {
        me.rerere.rikkahub.quickcapture.QuickCaptureCoordinator(
            context = get(),
            settingsStore = get(),
            targetResolver = get(),
            captureManager = get(),
            filesManager = get(),
            chatService = get(),
            safetySettings = get(),
            accessState = get(),
            navigator = get(),
            parentScope = get(),
        )
    }
    single<me.rerere.rikkahub.assistant.SystemAssistantChatBackend> {
        me.rerere.rikkahub.assistant.ChatServiceSystemAssistantBackend(get())
    }
    single<me.rerere.rikkahub.assistant.SystemAssistantAccessState> {
        me.rerere.rikkahub.assistant.AndroidSystemAssistantAccessState(get())
    }
    single<me.rerere.rikkahub.assistant.SystemAssistantEmergencyStopState> {
        me.rerere.rikkahub.assistant.AndroidSystemAssistantEmergencyStopState(get())
    }
    single<me.rerere.rikkahub.assistant.SecondUserPresentationSource> {
        me.rerere.rikkahub.assistant.DefaultSecondUserPresentationSource(
            chatService = get(),
            executionRepository = get(),
            approvalDao = get(),
            subAgentRegistry = get(),
            safetySettings = get(),
            probeScheduler = get(),
        )
    }
    single<me.rerere.rikkahub.assistant.SystemAssistantSessionControllerFactory> {
        me.rerere.rikkahub.assistant.DefaultSystemAssistantSessionControllerFactory(
            targetResolver = get(),
            chatBackend = get(),
            accessState = get(),
            emergencyStopState = get(),
            parentScope = get<AppScope>(),
            presentationSource = get(),
        )
    }
    single {
        me.rerere.rikkahub.assistant.AndroidSystemAssistantSessionAdapter(
            context = get(),
            controllerFactory = get(),
        )
    }

    single { me.rerere.rikkahub.privilege.ShizukuBridgeManager(get()) }
    single<me.rerere.rikkahub.privilege.PrivilegedPackageMetadataReader> {
        me.rerere.rikkahub.privilege.AndroidPrivilegedPackageMetadataReader(get())
    }
    single<me.rerere.rikkahub.privilege.PrivilegedRuntimeStatusProvider> {
        me.rerere.rikkahub.privilege.AndroidPrivilegedRuntimeStatusProvider(
            context = get(),
            bridge = get<me.rerere.rikkahub.privilege.ShizukuBridgeManager>(),
            workspaceProcessManager = get(),
        )
    }
    single {
        val safetySettings = get<AgentSafetySettings>()
        me.rerere.rikkahub.privilege.StructuredPrivilegedCommandExecutor(
            bridge = get<me.rerere.rikkahub.privilege.ShizukuBridgeManager>(),
            scope = get<AppScope>(),
            packageMetadataReader = get(),
            runtimeStatusProvider = get(),
            protectedPackages = me.rerere.rikkahub.privilege.defaultStructuredProtectedPackages(get()),
            criticalSystemPackages =
                me.rerere.rikkahub.privilege.defaultStructuredCriticalSystemPackages(get()),
            isEmergencyStopActive = safetySettings::isEmergencyStop,
        )
    }
    single {
        me.rerere.rikkahub.diagnostics.RuntimeDiagnosticsProvider(
            context = get(),
            settingsStore = get(),
            conversationRepository = get(),
            safetySettings = get(),
            shizukuBridgeManager = get(),
            workspaceProcessManager = get(),
            keyboardApiClient = get(),
            contextDiagnosticsStore = get(),
            displayAutomationRuntime = get(),
            managedExecutionCoordinator = get(),
            pluginRegistryStore = get(),
            toolSecurityDescriptorResolver = get(),
            toolExecutionPolicyResolver = get(),
            executionConsistencyDoctor = get(),
            toolCatalogDiagnostics = get(),
            reverseGeocodeRuntimeDiagnostics = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.ai.tools.local.AndroidGnssObservationSource(get())
    }
    single<me.rerere.rikkahub.data.ai.tools.local.AndroidGeocoderClient> {
        me.rerere.rikkahub.data.ai.tools.local.PlatformAndroidGeocoderClient(get())
    }
    single { me.rerere.rikkahub.data.ai.tools.local.ReverseGeocodeCache() }
    single { me.rerere.rikkahub.data.ai.tools.local.ReverseGeocodeDiagnosticsStore() }
    single {
        me.rerere.rikkahub.data.ai.tools.local.ReverseGeocodeHttpClient(
            baseClient = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.ai.tools.local.ReverseGeocodeRuntimeDiagnosticsSource(
            androidClient = get(),
            cache = get(),
            diagnostics = get(),
        )
    }
    single<me.rerere.rikkahub.data.ai.tools.local.ReverseGeocodeCoordinator> {
        me.rerere.rikkahub.data.ai.tools.local.ReverseGeocodeCoordinatorImpl(
            androidBackend = me.rerere.rikkahub.data.ai.tools.local.AndroidReverseGeocoder(get()),
            cache = get(),
            diagnostics = get(),
            settingsStore = get(),
            vault = get(),
            http = get(),
        )
    }
    single {
        me.rerere.rikkahub.data.ai.tools.local.ReverseGeocodeProviderTestGateway(get())
    }
    single {
        me.rerere.rikkahub.data.ai.tools.local.ReverseGeocodeToolProvider(get())
    }

    single {
        LocalTools(
            context = get(),
            eventBus = get(),
            cameraResultBuffer = get(),
            biometricResultBuffer = get(),
            scheduledJobRepository = get(),
            scheduledJobRunRepository = get(),
            cronJobScheduler = get(),
            settingsStore = get(),
            sshHostRepository = get(),
            telegramBotPreferences = get(),
            telegramBotClient = get(),
            notificationListenerPreferences = get(),
            mcpManager = get(),
            externalAutomationConfig = get(),
            gitHubReleaseChecker = get(),
            bugReportBuilder = get(),
            subAgentEngine = get(),
            subAgentRegistry = get(),
            researchCoordinator = get(),
            conversationRepo = get(),
            workflowRepository = get(),
            workflowEngine = get(),
            skillUrlImporter = get(),
            skillManager = get(),
            jsSkillRunner = get(),
            skillSecretsStore = get(),
            browserPreferences = get(),
            termuxPreferences = get(),
            interactiveToolStreamer = get(),
            nfcResultBuffer = get(),
            safPickerResultBuffer = get(),
            storageVolumeGrantStore = get(),
            okHttpClient = get(),
            keyboardApiClient = get(),
            shizukuBridgeManager = get(),
            phoneCallController = get(),
            apkInstallController = get(),
            managedExecutionCoordinator = get(),
            displayAutomationRuntime = get(),
            capabilityGrantRepository = get(),
            workspaceRepository = get(),
            workspaceProcessManager = get(),
            termuxSessionEmergencyController = get(),
            executionTokenProvider = get(),
            cancellationCoordinator = get(),
            petDiaryToolProvider = get(),
            dreamIntrospectionToolProvider = get(),
            persistentTtsLibrary = get(),
            ttsLibraryToolProvider = get(),
            reverseGeocodeToolProvider = get(),
        )
    }

    single {
        UpdateChecker(get())
    }

    single {
        AppScope()
    }

    single<EmojiData> {
        EmojiUtils.loadEmoji(get())
    }

    single {
        TTSManager(get())
    }

    single {
        me.rerere.rikkahub.tts.PersistentTtsLibrary(
            context = get(),
            settingsStore = get(),
            ttsManager = get(),
            appScope = get(),
            secretVault = get(),
        )
    }
    single { me.rerere.rikkahub.tts.TtsLibraryToolProvider(get()) }

    single {
        SoundEffectPlayer(get())
    }

    single {
        AILoggingManager(get(), get())
    }

    // P0: Agent safety and security gate
    single { AgentSafetySettings(context = get()) }
    single {
        ToolExecutionGate(
            context = get(),
            safetySettings = get(),
            capabilityPolicyEngine = get(),
        )
    }
    single<me.rerere.rikkahub.data.ai.execution.ToolRunPreflight> {
        me.rerere.rikkahub.data.ai.execution.DefaultToolRunPreflight(get())
    }
    // P0 Plugin Runtime Lite. The registry and package root are app-private; its installation
    // marker deliberately lives under noBackupFilesDir so restored plugins always require a new
    // review before their tools or hooks can run.
    single<me.rerere.rikkahub.plugin.PluginRegistryStore> {
        val context = get<android.content.Context>()
        me.rerere.rikkahub.plugin.FilePluginRegistryStore(
            root = me.rerere.rikkahub.plugin.PluginRuntimePaths.root(context),
            markerDirectory = me.rerere.rikkahub.plugin.PluginRuntimePaths
                .installationMarkerDirectory(context),
        )
    }
    single<me.rerere.rikkahub.plugin.PluginNetworkGateway> {
        me.rerere.rikkahub.plugin.AndroidPluginNetworkGateway(get())
    }
    single {
        val context = get<android.content.Context>()
        me.rerere.rikkahub.plugin.PluginHostRpcGateway(
            storageRoot = me.rerere.rikkahub.plugin.PluginRuntimePaths.storageRoot(context),
            networkGateway = get(),
        )
    }
    single<me.rerere.rikkahub.plugin.PluginRuntimeTransport> {
        me.rerere.rikkahub.plugin.AndroidPluginRuntimeTransport(
            context = get(),
            hostRpcGateway = get(),
        )
    }
    single { me.rerere.rikkahub.plugin.PluginAuditStore() }
    single {
        val settingsStore = get<me.rerere.rikkahub.data.datastore.SettingsStore>()
        me.rerere.rikkahub.plugin.PluginRuntimeCoordinator(
            registry = get(),
            transport = get(),
            hostRpcGateway = get(),
            isRuntimeEnabled = { settingsStore.settingsFlow.value.pluginRuntimeEnabled },
            auditStore = get(),
        )
    }
    single<me.rerere.rikkahub.plugin.PluginInvocationRunner> { get<me.rerere.rikkahub.plugin.PluginRuntimeCoordinator>() }
    single {
        val context = get<android.content.Context>()
        me.rerere.rikkahub.plugin.PluginPackageInstaller(
            root = me.rerere.rikkahub.plugin.PluginRuntimePaths.root(context),
            registry = get(),
        )
    }
    single {
        me.rerere.rikkahub.plugin.PluginBuiltInExampleInstaller(
            context = get(),
            installer = get(),
        )
    }
    single {
        val settingsStore = get<me.rerere.rikkahub.data.datastore.SettingsStore>()
        me.rerere.rikkahub.plugin.PluginHookBridge(
            registry = get(),
            invoker = get(),
            isRuntimeEnabled = { settingsStore.settingsFlow.value.pluginRuntimeEnabled },
            enabledPluginsForAssistant = { assistantId ->
                settingsStore.settingsFlow.value.assistants
                    .firstOrNull { it.id.toString() == assistantId }
                    ?.enabledPluginIds
                    .orEmpty()
            },
        )
    }
    single {
        val settingsStore = get<me.rerere.rikkahub.data.datastore.SettingsStore>()
        me.rerere.rikkahub.plugin.PluginToolCatalog(
            registry = get(),
            invoker = get(),
            isRuntimeEnabled = { settingsStore.settingsFlow.value.pluginRuntimeEnabled },
            executionScope = get<me.rerere.rikkahub.AppScope>(),
        )
    }
    // P0 ToolRuntime: every production tool call goes through the shared policy,
    // descriptor and cancellation seam.  These are deliberately registered here
    // rather than constructed by GenerationHandler so Fast Path and future callers
    // can resolve the same runtime without creating a second policy boundary.
    single<me.rerere.rikkahub.data.ai.execution.ToolExecutionPolicyResolver> {
        me.rerere.rikkahub.data.ai.execution.DefaultToolExecutionPolicyResolver()
    }
    single {
        me.rerere.rikkahub.data.ai.execution.ToolExecutionBatchCoordinator(
            me.rerere.rikkahub.data.ai.execution.ToolExecutionBatchPlanner(get()),
        )
    }
    single<me.rerere.rikkahub.data.ai.execution.ToolSecurityDescriptorResolver> {
        val registry = get<me.rerere.rikkahub.plugin.PluginRegistryStore>()
        me.rerere.rikkahub.data.ai.execution.DefaultToolSecurityDescriptorResolver(
            pluginToolKnown = { toolName -> registry.containsApprovedModelTool(toolName) },
        )
    }
    single<me.rerere.rikkahub.execution.ManagedExecutionLedger> {
        me.rerere.rikkahub.execution.AtomicFileManagedExecutionLedger(get())
    }
    single<me.rerere.rikkahub.execution.ExecutionTokenProvider> {
        me.rerere.rikkahub.execution.AndroidKeystoreExecutionTokenProvider()
    }
    single<me.rerere.rikkahub.owner.OwnerOperationFingerprinter> {
        me.rerere.rikkahub.owner.KeystoreOwnerOperationFingerprinter(get())
    }
    single<me.rerere.rikkahub.execution.TermuxManagedSupervisor> {
        me.rerere.rikkahub.execution.AndroidTermuxManagedSupervisor(get())
    }
    single {
        me.rerere.rikkahub.execution.WorkspaceManagedProcessStarter(
            manager = get(),
            registration = get(),
            scope = get<me.rerere.rikkahub.AppScope>(),
        )
    }
    single {
        me.rerere.rikkahub.execution.WorkspaceProcessStartableFactory(
            starter = get(),
            workspaceRepository = get(),
            scope = get<me.rerere.rikkahub.AppScope>(),
        )
    }
    single {
        me.rerere.rikkahub.execution.TermuxManagedStartableFactory(
            supervisor = get(),
            ledger = get(),
            tokenProvider = get(),
            scope = get<me.rerere.rikkahub.AppScope>(),
            registration = get(),
        )
    }
    single {
        me.rerere.rikkahub.execution.LinuxManagedStartableFactory(
            appContext = get(),
            termuxFactory = get(),
            workspaceRepository = get(),
            settingsStore = get(),
            scope = get<me.rerere.rikkahub.AppScope>(),
            workspaceStarter = get(),
        )
    }
    single {
        me.rerere.rikkahub.execution.SshUnmanagedExecutionRegistry(
            scope = get<me.rerere.rikkahub.AppScope>(),
        )
    }
    single {
        me.rerere.rikkahub.execution.SshManagedStartableFactory(
            context = get(),
            repository = get(),
            scope = get<me.rerere.rikkahub.AppScope>(),
            ledger = get(),
            tokenProvider = get(),
            registration = get(),
            unmanagedRegistry = get(),
        )
    }
    single<me.rerere.rikkahub.data.ai.execution.ToolStartableResolver> {
        me.rerere.rikkahub.data.ai.execution.DefaultToolStartableResolver(
            termuxFactory = get(),
            sshFactory = get(),
            linuxFactory = get(),
            workspaceFactory = get(),
        )
    }
    single<me.rerere.rikkahub.data.ai.execution.ToolRuntime> {
        val pluginHooks = get<me.rerere.rikkahub.plugin.PluginHookBridge>()
        me.rerere.rikkahub.data.ai.execution.DefaultToolRuntime(
            policyResolver = get(),
            securityDescriptorResolver = get(),
            criticalSink = get(),
            trackingHealth = get(),
            interceptors = listOf(pluginHooks),
            observers = listOf(pluginHooks),
        )
    }
    single<me.rerere.rikkahub.execution.ManagedExecutionCoordinator> {
        val ledger = get<me.rerere.rikkahub.execution.ManagedExecutionLedger>()
        val tokenProvider = get<me.rerere.rikkahub.execution.ExecutionTokenProvider>()
        me.rerere.rikkahub.execution.DefaultManagedExecutionCoordinator(
            adapters = listOf(
                me.rerere.rikkahub.execution.WorkspaceManagedExecutionAdapter(
                    me.rerere.rikkahub.execution.WorkspaceProcessManagerPort(get()),
                ),
                me.rerere.rikkahub.execution.TermuxManagedExecutionAdapter(
                    ledger = ledger,
                    supervisor = get(),
                    tokenProvider = tokenProvider,
                ),
                me.rerere.rikkahub.execution.SshManagedExecutionAdapter(
                    ledger = ledger,
                    supervisor = me.rerere.rikkahub.execution.AndroidSshManagedSupervisor(get()),
                    profileResolver = me.rerere.rikkahub.execution
                        .RepositorySshSavedConnectionResolver(get()),
                    tokenProvider = tokenProvider,
                    unmanagedRegistry = get(),
                ),
            ),
        )
    }
    single<me.rerere.rikkahub.display.PrivilegedDisplayBridgePort> {
        me.rerere.rikkahub.privilege.ShizukuDisplayBridgePort(
            get<me.rerere.rikkahub.privilege.ShizukuBridgeManager>(),
        )
    }
    single<me.rerere.rikkahub.display.DisplayPublicCapabilityProbe> {
        me.rerere.rikkahub.display.AndroidDisplayPublicCapabilityProbe()
    }
    // A missing Shizuku bridge or an unavailable public adapter fails closed inside this
    // provisioner. It never fabricates Display 0 or remaps an operation to the primary phone.
    single<me.rerere.rikkahub.display.DisplayAutomationRuntime> {
        me.rerere.rikkahub.display.DefaultDisplayAutomationRuntime(
            me.rerere.rikkahub.display.ShizukuDisplayProvisioner(
                bridge = get(),
                publicCapabilityProbe = get(),
            ),
        )
    }
    // P0 Context Broker. The broker is opt-in per assistant and rejects every remote,
    // keyguard, and sub-agent surface before a platform reader can inspect device state.
    single<me.rerere.rikkahub.context.VisionDescriptionClient> {
        me.rerere.rikkahub.context.ProviderVisionDescriptionClient(get(), get())
    }
    single { me.rerere.rikkahub.context.ContextDiagnosticsStore() }
    single<me.rerere.rikkahub.context.ContextBroker> {
        val accessibilityReader = me.rerere.rikkahub.context.AndroidAccessibilityContextReader(get())
        val visionClient = get<me.rerere.rikkahub.context.VisionDescriptionClient>()
        me.rerere.rikkahub.context.DefaultContextBroker(
            readers = mapOf(
                me.rerere.rikkahub.context.ContextSource.FOREGROUND_WINDOW to accessibilityReader,
                me.rerere.rikkahub.context.ContextSource.UI_TREE to accessibilityReader,
                me.rerere.rikkahub.context.ContextSource.DEVICE_STATUS to
                    me.rerere.rikkahub.context.AndroidDeviceStatusContextReader(get()),
                me.rerere.rikkahub.context.ContextSource.OCR_FALLBACK to
                    me.rerere.rikkahub.context.AndroidOcrContextReader(get(), visionClient),
                me.rerere.rikkahub.context.ContextSource.USAGE_STATS to
                    me.rerere.rikkahub.context.AndroidUsageStatsContextReader(get()),
                me.rerere.rikkahub.context.ContextSource.NOTIFICATIONS to
                    me.rerere.rikkahub.context.AndroidNotificationContextReader(),
            ),
        )
    }
    single {
        me.rerere.rikkahub.data.ai.EmergencyStopCoordinator(
            safetySettings = get(),
            externalPrivilegeBridge = get<me.rerere.rikkahub.privilege.ShizukuBridgeManager>(),
            workspaceProcessManager = get(),
            workspaceRepository = get(),
            chatService = get(),
            termuxSessionController = get(),
            subAgentRegistry = get(),
            researchCoordinator = get(),
            workflowEmergencyController = get(),
            managedExecutionCoordinator = get(),
            displayAutomationRuntime = get(),
            executionProbeScheduler = get(),
            executionRepository = get(),
        )
    }

    // Phase 22A: Local-LLM on-device providers
    single { me.rerere.locallm.LocalRuntimePreferences(get()) }
    single { me.rerere.locallm.litert.LiteRtRuntime(get()) }

    single {
        ChatService(
            context = get(),
            appScope = get(),
            settingsStore = get(),
            conversationRepo = get(),
            memoryRepository = get(),
            memoryRetrievalDiagnostics = get(),
            agentTimingStore = get(),
            memoryV2Coordinator = get(),
            dreamExperienceIngestor = get(),
            generationHandler = get(),
            templateTransformer = get(),
            providerManager = get(),
            localTools = get(),
            mcpManager = get(),
            filesManager = get(),
            skillManager = get(),
            toolApprovalPreferences = get(),
            capabilityGrantRepository = get(),
            workspaceRepository = get(),
            workflowRepository = get(),
            conversationDeletionPolicy = get(),
            secondUserSecretVault = get(),
            durableCommandQueue = get(),
            secondUserApprovalLifecycle = get(),
            toolExecutionGate = get(),
            toolRuntime = get(),
            pluginToolCatalog = get(),
            pluginHookBridge = get(),
            pluginRegistryStore = get(),
            pluginPackageInstaller = get(),
            agentSafetySettings = get(),
            shizukuBridgeManager = get(),
            workspaceProcessManager = get(),
            structuredPrivilegedCommandExecutor = get(),
            subAgentExecutionProfileRegistry = get(),
            setupTransactionCoordinator = get(),
            displayAutomationRuntime = get(),
            toolExperienceRepository = get(),
            toolShortcutRepository = get(),
            secondUserAuthorityService = get(),
            hostOperationDao = get<me.rerere.rikkahub.data.db.AppDatabase>().hostOperationDao(),
            secretPlaintextSessions = get(),
            ephemeralToolResults = get(),
            runtimeSecretRedactor = get(),
            assistantRemovalService = get(),
            persistentTtsLibrary = get(),
            workspaceManagedProcessStarter = get(),
            hostLocalServiceDao = get<me.rerere.rikkahub.data.db.AppDatabase>().hostLocalServiceDao(),
            ownerHttpClient = get(),
            workflowActionRunner = get(),
            automationControlFacade = get(),
            doctorChecks = get(),
            executionConsistencyDoctor = get(),
            ownerLocalServiceSupervisor = get(),
            agentRunRepository = get(),
            ownerServiceSpecStore = get(),
            ownerTermuxServiceLauncher = get(),
            ownerOperationFingerprinter = get(),
            localBackupFacade = get(),
            petDialogueRepository = get(),
            telegramBotPreferences = get(),
            telegramCredentialResolver = get(),
            reverseGeocodeProviderTestGateway = get(),
            dreamReviewRepository = get(),
            learningForegroundRegistry = get(),
            commandAdmissionAuthority = get(),
            commandAdmissionAuthorityAdapter = get(),
            waitingApprovalAuthority = get(),
            finalConversationAuthority = get(),
            executionMessageAuthorityBinder = get(),
        )
    }
    single {
        me.rerere.rikkahub.assistant.SecondUserAuthorityRevocationCoordinator(
            authority = get(),
            queue = get(),
            grants = get(),
            approvalDao = get(),
            approvalLifecycle = get(),
            conversations = get(),
            executions = get(),
            cancellation = get(),
            chatService = get(),
            toolExperiences = get(),
            toolShortcuts = get(),
            learningAuthorityRevocation = get(),
        )
    }

    single {
        WebServerManager(
            context = get(),
            appScope = get(),
            chatService = get(),
            conversationRepo = get(),
            settingsStore = get(),
            filesManager = get()
        )
    }

    single {
        me.rerere.rikkahub.ui.pages.setting.doctor.DoctorChecks(
            context = get(),
            settingsStore = get(),
            telegramPrefs = get(),
            workflowRepository = get(),
            scheduledJobRepository = get(),
            scheduledJobRunRepository = get(),
            conversationRepository = get(),
            database = get(),
            // Pass 3: surface the browser write-tools-enabled INFO row + profile-dir AutoFix.
            browserPreferences = get(),
            // Phase 25: surface the SAF granted-directories live count.
            storageVolumeGrantStore = get(),
            // LiteRT accelerator status row in the Doctor: shows the persisted backend
            // decision so a silent GPU -> CPU fallback is visible.
            localRuntimePreferences = get(),
            runtimeDiagnosticsProvider = get(),
            workspaceRepository = get(),
            capabilityGrantRepository = get(),
            executionConsistencyDoctor = get(),
            petDiagnostics = get(),
        )
    }
}
