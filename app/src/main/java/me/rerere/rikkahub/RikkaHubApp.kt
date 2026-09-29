package me.rerere.rikkahub

import android.app.Application
import android.app.ActivityManager
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Process
import android.util.Log
import androidx.compose.foundation.ComposeFoundationFlags
import androidx.compose.runtime.Composer
import androidx.compose.runtime.tooling.ComposeStackTraceMode
import androidx.core.app.NotificationChannelCompat
import androidx.core.app.NotificationManagerCompat
import androidx.core.content.ContextCompat
import androidx.lifecycle.DefaultLifecycleObserver
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.ProcessLifecycleOwner
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import me.rerere.rikkahub.data.db.ImportedDatabaseReconciler
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreStartupCoordinator
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreStartupResult
import java.io.File
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.drop
import kotlinx.coroutines.flow.collect
import me.rerere.common.android.appTempFolder
import com.whl.quickjs.android.QuickJSLoader
import me.rerere.rikkahub.di.appModule
import me.rerere.rikkahub.di.dataSourceModule
import me.rerere.rikkahub.di.repositoryModule
import me.rerere.rikkahub.di.viewModelModule
import me.rerere.rikkahub.data.files.FilesManager
import me.rerere.rikkahub.data.files.FileFolders
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.data.ai.tools.HeadlessConversations
import me.rerere.rikkahub.pet.resolvePetOverlaySelection
import me.rerere.rikkahub.service.WebServerService
import me.rerere.rikkahub.utils.CrashHandler
import me.rerere.rikkahub.utils.DatabaseUtil
import me.rerere.rikkahub.data.repository.WorkspaceRepository
import me.rerere.workspace.WorkspaceManager
import org.koin.android.ext.android.get
import org.koin.android.ext.koin.androidContext
import org.koin.android.ext.koin.androidLogger
import org.koin.androidx.workmanager.koin.workManagerFactory
import org.koin.core.context.startKoin

private const val TAG = "RikkaHubApp"
internal const val VOICE_INTERACTOR_PROCESS_SUFFIX = ":voice_interactor"
internal const val PLUGIN_RUNTIME_PROCESS_SUFFIX = ":plugin_runtime"

const val CHAT_COMPLETED_NOTIFICATION_CHANNEL_ID = "chat_completed"
const val CHAT_LIVE_UPDATE_NOTIFICATION_CHANNEL_ID = "chat_live_update"
const val WEB_SERVER_NOTIFICATION_CHANNEL_ID = "web_server"
const val WORKSPACE_PROCESS_NOTIFICATION_CHANNEL_ID = "workspace_process"
const val ACCESSIBILITY_KEEP_ALIVE_NOTIFICATION_CHANNEL_ID = "accessibility_keep_alive"

class RikkaHubApp : Application() {
    private var dependencyGraphStarted = false

    override fun onCreate() {
        super.onCreate()
        if (isVoiceInteractorProcess() || isPluginRuntimeProcess()) {
            Log.i(TAG, "Skipping full app initialization in a lightweight runtime process")
            return
        }
        // A database restore is prepared and swapped only here, before Room/Koin/WorkManager can
        // obtain either database. Ambiguous or post-boundary failure deliberately leaves the app
        // in a closed DEGRADED process instead of reopening an old Learning timeline.
        val coldRestore = ColdRestoreStartupCoordinator.run(this)
        when (coldRestore) {
            ColdRestoreStartupResult.NoPendingRestore,
            ColdRestoreStartupResult.Complete,
            -> Unit
            ColdRestoreStartupResult.RebuildRequired ->
                Log.i(TAG, "Cold restore committed; Learning rebuild is required")
            is ColdRestoreStartupResult.LiveDatabaseUnchanged ->
                Log.w(TAG, "Cold restore preparation refused: ${coldRestore.reasonCode}")
            ColdRestoreStartupResult.Busy -> {
                Log.e(TAG, "Cold restore lock is busy; dependency graph will remain closed")
                return
            }
            is ColdRestoreStartupResult.DegradedRestartRequired -> {
                Log.e(
                    TAG,
                    "Cold restore is degraded; dependency graph will remain closed: " +
                        coldRestore.reasonCode,
                )
                return
            }
        }
        // Reconcile the database before Room or any DI/DB layer opens it.
        // This creates fork-only tables and stamps the correct identity hash so
        // a restored official RikkaHub backup (which lacks agent tables) can be
        // opened by Room without crashing on "no such table" or hash mismatch.
        ImportedDatabaseReconciler.reconcile(this)

        startKoin {
            androidLogger()
            androidContext(this@RikkaHubApp)
            workManagerFactory()
            modules(appModule, viewModelModule, dataSourceModule, repositoryModule)
        }
        dependencyGraphStarted = true
        // Privacy maintenance is content-free and may be armed immediately. Persisted Learning
        // rollout flags, however, are unavailable until DataStore replaces Settings.dummy(). If
        // the flag-gated scheduler samples that dummy value it cancels every drain/recovery chain
        // and a previously enabled installation remains stuck until the user toggles the stage.
        runCatching {
            get<me.rerere.rikkahub.learning.jobs.LearningWorkScheduler>()
                .scheduleMaintenance()
        }.onFailure { error ->
            Log.w(TAG, "Learning maintenance scheduling unavailable", error)
        }
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                val settings = get<SettingsStore>()
                settings.settingsFlow.first { value -> !value.init }
                if (coldRestore == ColdRestoreStartupResult.RebuildRequired || coldRestore == ColdRestoreStartupResult.Complete) {
                    if (!get<me.rerere.rikkahub.learning.runtime.LearningRuntimeFacade>().finalizeColdRestore(settings)) {
                        Log.i(TAG, "Cold restore cleanup retained until persisted consent or completed rebuild authorizes it")
                    }
                }
                get<me.rerere.rikkahub.learning.jobs.LearningWorkScheduler>()
                    .scheduleStartupAndRecovery()
            }.onFailure { error ->
                Log.w(TAG, "Learning startup/recovery scheduling unavailable", error)
            }
        }
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                get<me.rerere.rikkahub.assistant.SecondUserAuthorityService>()
                    .initializeLegacyMigration()
            }.onFailure { error ->
                Log.e(TAG, "Second-user authority migration failed", error)
            }
        }
        runCatching {
            me.rerere.rikkahub.assistant.SystemAssistantSessionAdapterRegistry.install(
                get<me.rerere.rikkahub.assistant.AndroidSystemAssistantSessionAdapter>()
            )
        }.onFailure { error ->
            Log.e(TAG, "Unable to install the system-assistant session adapter", error)
        }
        this.createNotificationChannel()
        val keepAccessibilityAlive =
            me.rerere.rikkahub.service.AccessibilityKeepAliveState
                .initializeFromSystemAuthorization(
                    context = this,
                    authorized = me.rerere.rikkahub.data.ai.tools.local.AccessibilityServiceHandle
                        .isEnabledInSettings(this),
                )
        if (keepAccessibilityAlive) {
            me.rerere.rikkahub.service.AccessibilityKeepAliveService.start(this)
        }
        me.rerere.rikkahub.pet.PetDailyArchiveScheduler.schedule(this)

        // Restore any headless conversation IDs that survived a process kill; must run
        // before any cron worker fires so mark/unmark are consistent.
        HeadlessConversations.init(this)

        // Sweep orphan headless conversations created by workers that were killed mid-execute.
        sweepOrphanHeadlessConversations()

        // set cursor window size to 32MB
        DatabaseUtil.setCursorWindowSize(32 * 1024 * 1024)

        // install crash handler
        CrashHandler.install(this)

        // Init QuickJS native library
        QuickJSLoader.init()

        // delete temp files
        deleteTempFiles()

        // cleanup stale tool output files
        cleanupToolOutputs()

        // cleanup workspace temp dirs (proot + rootfs /tmp)
        cleanupWorkspaceTempDirs()

        // check workspace integrity (remove orphaned DB records after backup restore)
        checkWorkspaceIntegrity()

        // Android 12+ only permits the first foreground-service launch while the app is
        // user-visible. Existing START_STICKY services restore independently.
        restoreWorkspaceProcessesWhenForegrounded()
        restoreQuickCaptureWhenForegrounded()
        restoreDesktopPetWhenForegrounded()

        // sync upload files to DB
        syncManagedFiles()
        cleanupQuickCaptureFiles()

        // Start WebServer if enabled in settings
        startWebServerIfEnabled()

        // Eagerly construct ChatService on the main thread. Its constructor calls
        // LifecycleRegistry.addObserver which throws if it runs off-main, and the Telegram
        // bot service runs on Dispatchers.IO — without this priming, the first inbound bot
        // message after a fresh app start crashes the bot's handleIncoming with
        // "addObserver must be called on the main thread" because Koin's lazy factory
        // builds ChatService on the IO thread.
        eagerlyInitChatService()
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching { get<me.rerere.rikkahub.pet.PetHandoffRecovery>().reconcile() }
                .onFailure { Log.w(TAG, "Pet handoff recovery failed", it) }
        }

        // Start Telegram bot if previously enabled — service is START_NOT_STICKY so OS won't
        // auto-revive it after a process kill; we need to bring it back ourselves.
        startTelegramBotIfEnabled()

        // Initialise the agent's `~` workspace at /data/data/<pkg>/files/workspace/.
        // Tools resolve `~` and `~/foo` paths to this dir, giving the LLM a stable
        // sandbox for `.learnings/`, scratch files, and skill state without scoped-
        // storage friction. Termux-style: private, persistent, OS-blessed.
        me.rerere.rikkahub.data.ai.tools.local.AgentWorkspace.init(this)

        // Best-effort shared hand-off path for RikkaHub, Termux, and workspace proot.
        // Android 11+ requires the existing All Files Access special permission; startup
        // never prompts or crashes when it is absent, and the permission diagnostics page
        // remains the user-controlled way to grant it.
        when (val exchange = me.rerere.rikkahub.data.files.SharedExchangeDirectory.ensure(this)) {
            is me.rerere.rikkahub.data.files.SharedExchangeDirectory.Status.Ready ->
                Log.i(TAG, "Shared exchange directory ready: ${exchange.directory}")
            is me.rerere.rikkahub.data.files.SharedExchangeDirectory.Status.PermissionRequired ->
                Log.i(TAG, "Shared exchange directory awaits All Files Access")
            is me.rerere.rikkahub.data.files.SharedExchangeDirectory.Status.Unavailable ->
                Log.w(TAG, "Shared exchange directory unavailable: ${exchange.directory}")
        }

        // Copy any default skills bundled in assets/default-skills/* into the user's skills
        // dir on first launch. SkillManager guards via a per-skill .seeded sentinel so this
        // is a one-time install — user edits / deletes are respected on subsequent launches.
        seedDefaultSkillsIfNeeded()

        // Increment launch count
        incrementLaunchCount()

        // Phase 12: kick off the workflow trigger registry. It subscribes to the workflows
        // table and reconciles broadcast receivers / geofences / time_cron schedules with
        // every change. With zero enabled workflows, no receivers are registered.
        startWorkflowRegistry()

        // Phase-17 stability — register a network-change monitor that evicts OkHttp's
        // connection pool on every default-network transition. Fixes the post-Termux-
        // interactive-session "Unable to resolve host …" bug: when the user opens
        // Termux's terminal for `htop` the app backgrounds, Android may flip the
        // network into a restricted state, and the JVM's negative DNS cache plus
        // OkHttp's idle sockets keep the failure sticky after return. Eviction on the
        // next onAvailable forces a fresh DNS lookup + new socket on the next request.
        startNetworkChangeMonitor()

        // Phase 24 — unified AgentRun ledger boot recovery. Walk the ledger once per
        // process start: any autonomous run (cron / workflow / sub-agent / Telegram /
        // external automation) left in flight by a killed process is flipped to
        // `process_lost` and a single aggregate notification is fired. This is the
        // cross-pillar generalisation of the Phase 9.5 cron stranded-row sweep and is what
        // makes background sub-agents survivable across process death.
        runAgentRunBootRecovery()
        get<me.rerere.rikkahub.data.execution.ExecutionRetentionManager>().requestCleanup()
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching { get<me.rerere.rikkahub.toolcatalog.ToolExperienceRepository>().purgeDeleted() }
                .onFailure { Log.w(TAG, "tool experience retention cleanup failed", it) }
        }
        runExecutionBootRecovery()
        get<me.rerere.rikkahub.owner.OwnerLocalServiceSupervisor>().start()
        refreshCapabilityPolicyGrants()
        reconcileMemoryV2Metadata()

        // Auto-recover from a prior native crash inside a local-runtime JNI lib
        // (LiteRT-LM 0.11.0 has known SIGSEGVs on the GPU/NNAPI backend during
        // inference on Pixel Tensor-G). If we detect one, force the runtime to
        // CPU on the next load and stamp a recovery banner the LiteRT settings
        // page picks up — so users see "Recovered: switched to CPU" instead of
        // a silent re-crash.
        sweepLocalLlmNativeCrashes()

        // Clear stale per-device decisions (cached accelerator, vision-unavailable set,
        // crash-recovery banner) when the bundled LiteRT-LM SDK has been bumped since
        // the last app start. An older SDK's "GPU is broken on Adreno 7xx" / "vision
        // encoder unavailable" decisions can mask a fix shipped in the new SDK; without
        // this sweep, a 0.11→0.12 bump would silently stay on CPU even though 0.12 may
        // have fixed the GPU path. Decisions are re-inferred from a fresh probe on the
        // next inference / re-detect tap. User-set knobs (force-CPU toggle, max-context
        // override) are NOT touched.
        invalidateLocalLlmDecisionsOnSdkUpgrade()

        // Composer.setDiagnosticStackTraceMode(ComposeStackTraceMode.Auto)
    }

    /**
     * Inspect the package's recent ApplicationExitInfo records for a native crash whose
     * stack/description points at a local-runtime JNI library. When one is found, set the
     * matching runtime's force-CPU flag so the next inference runs on CPU, and record the
     * crashed accelerator label so the settings UI can surface a "switched to CPU" notice.
     *
     * Best-effort: errors are logged, never thrown — a stuck app start is worse than a
     * skipped recovery sweep.
     */
    private fun sweepLocalLlmNativeCrashes() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return  // ApplicationExitInfo is API 30+
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                val am = getSystemService(android.app.ActivityManager::class.java) ?: return@runCatching
                // Look at the last ~5 exits: more than enough to spot a recent crash even if
                // the user opened the app a few times since (each open = one exit record).
                val recentExits = am.getHistoricalProcessExitReasons(packageName, 0, 5)
                val nativeCrash = recentExits.firstOrNull { exit ->
                    exit.reason == android.app.ApplicationExitInfo.REASON_CRASH_NATIVE &&
                        // ApplicationExitInfo.description includes the offending shared library
                        // for native crashes. Match the JNI sidekick of each runtime.
                        (exit.description?.contains("liblitertlm", ignoreCase = true) == true)
                } ?: return@runCatching
                val prefs = get<me.rerere.locallm.LocalRuntimePreferences>()
                val runtime = me.rerere.locallm.LocalRuntime.LiteRT
                // Don't double-stamp if the user has already seen and dismissed an earlier
                // crash banner — the prior dismiss cleared the recovery key, but if a NEW
                // crash happened after, we want a fresh notice.
                val crashedAccel = prefs.acceleratorFlow(runtime).first() ?: "GPU/NPU"
                if (!prefs.forceCpu(runtime)) {
                    prefs.setForceCpu(runtime, true)
                    prefs.clearAccelerator(runtime)
                }
                prefs.setCrashRecovery(runtime, crashedAccel)
                Log.w(
                    TAG,
                    "sweepLocalLlmNativeCrashes: detected native crash in liblitertlm at " +
                        "${nativeCrash.timestamp} (accel=$crashedAccel) — forcing CPU + stamping recovery banner"
                )
            }.onFailure {
                Log.w(TAG, "sweepLocalLlmNativeCrashes failed", it)
            }
        }
    }

    /**
     * Fire-and-forget: clear stale SDK-coupled decisions (accelerator, vision-unavailable
     * set, crash-recovery banner) whenever the compiled-in LiteRT-LM version differs from
     * the last-persisted one. Best-effort — failure is logged and ignored so a slow or
     * broken DataStore read can never block app start. Idempotent across multiple calls
     * within the same process (the version write makes the second call a no-op).
     */
    private fun invalidateLocalLlmDecisionsOnSdkUpgrade() {
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                val prefs = get<me.rerere.locallm.LocalRuntimePreferences>()
                val invalidated = prefs.maybeInvalidateOnSdkUpgrade(me.rerere.locallm.LocalRuntime.LiteRT)
                if (invalidated) {
                    Log.i(
                        TAG,
                        "invalidateLocalLlmDecisionsOnSdkUpgrade: SDK version changed — cleared " +
                            "accelerator + vision-unavailable + crash-recovery for LiteRT (new=${prefs.currentSdkVersion})",
                    )
                }
                // Unconditionally wipe the visionUnavailable set on every app start. Stale
                // flags can be left behind by transient failures the SDK has since
                // recovered from (most notably the 0.12.0 -> 0.11.0 downgrade where the
                // SDK-version key may already match because the device ran 0.11.0 first).
                // If GPU vision really is broken on this device, [LiteRtRuntime.ensureLoaded]
                // re-stamps the flag the moment it observes a fresh failure — so the wipe
                // never strands the app in a crash loop, it just ensures we re-test on
                // every launch.
                val wipedVision = prefs.clearAllVisionUnavailable(me.rerere.locallm.LocalRuntime.LiteRT)
                if (wipedVision > 0) {
                    Log.i(
                        TAG,
                        "invalidateLocalLlmDecisionsOnSdkUpgrade: wiped $wipedVision stale " +
                            "visionUnavailable entries (forcing fresh attempt next inference)",
                    )
                }
            }.onFailure {
                Log.w(TAG, "invalidateLocalLlmDecisionsOnSdkUpgrade failed", it)
            }
        }
    }

    /**
     * Phase 24 — run the unified AgentRun ledger boot-recovery sweep once per process
     * start. Best-effort: a slow or failed sweep must never block app start, so it runs on
     * the IO dispatcher off [AppScope] and swallows its own failures.
     */
    private fun runAgentRunBootRecovery() {
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                get<me.rerere.rikkahub.data.agentrun.AgentRunBootRecovery>().runRecovery()
            }.onFailure {
                Log.w(TAG, "runAgentRunBootRecovery failed", it)
            }
        }
    }

    /**
     * Per-handle recovery never retries a model action. It only classifies managed runtimes that
     * can still be verified from their durable PID/process ledger; every other in-flight record
     * becomes an honest orphan for later review.
     */
    private fun runExecutionBootRecovery() {
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                // Authority migration/revocation deliberately precedes every approval and
                // runtime sweep. A killed reassignment therefore continues revocation rather
                // than giving an old queue, approval, or child process one recovery turn.
                get<me.rerere.rikkahub.assistant.SecondUserAuthorityService>()
                    .initializeLegacyMigration()
                get<me.rerere.rikkahub.assistant.SecondUserAuthorityRevocationCoordinator>()
                    .resumeIfNeeded()
                get<me.rerere.rikkahub.data.execution.SecondUserApprovalRecovery>().runRecovery()
                get<me.rerere.rikkahub.owner.OwnerOperationBootRecovery>().recover()
                get<me.rerere.rikkahub.data.execution.ExecutionBootRecovery>().runRecovery()
                startExecutionProbeScheduler()
            }.onFailure {
                Log.w(TAG, "runExecutionBootRecovery failed", it)
            }
        }
    }

    private fun startExecutionProbeScheduler() {
        runCatching {
            val scheduler = get<me.rerere.rikkahub.data.execution.ExecutionProbeScheduler>()
            scheduler.start()
            get<AppScope>().launch {
                get<me.rerere.rikkahub.privilege.ShizukuBridgeManager>()
                    .statusFlow
                    .drop(1)
                    .collect { scheduler.requestProbe() }
            }
        }.onFailure {
            Log.w(TAG, "startExecutionProbeScheduler failed", it)
        }
    }

    /** Load durable remote/plugin/workflow grants before new inbound work is admitted. */
    private fun refreshCapabilityPolicyGrants() {
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                get<me.rerere.rikkahub.data.capability.CapabilityGrantRepository>().refresh()
            }.onFailure {
                Log.w(TAG, "refreshCapabilityPolicyGrants failed; policy remains fail-closed", it)
            }
        }
    }

    private fun startWorkflowRegistry() {
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                val registry = get<me.rerere.rikkahub.workflow.trigger.TriggerRegistry>()
                val engine = get<me.rerere.rikkahub.workflow.execution.WorkflowEngine>()
                registry.setEngineCallback(engine.triggerCallback)
                registry.start()
            }.onFailure {
                Log.e(TAG, "startWorkflowRegistry failed", it)
            }
        }
    }

    private fun startNetworkChangeMonitor() {
        runCatching {
            val client = get<okhttp3.OkHttpClient>()
            me.rerere.rikkahub.utils.NetworkChangeMonitor.start(this, client)
        }.onFailure {
            Log.w(TAG, "startNetworkChangeMonitor failed", it)
        }
    }

    /**
     * Cleans up orphan conversations left by cron workers that were killed mid-execute.
     *
     * When a worker is killed between HeadlessConversations.mark() and unmark(), the
     * conversation ID remains in SharedPreferences. On the next app start we detect these
     * IDs and delete the corresponding "[Scheduled]" conversations from the DB so they
     * don't pollute the chat list.
     *
     * We clear the persisted set at the end regardless — if a conversation doesn't exist in
     * DB there's nothing to clean up, and stale IDs only confuse future sweeps.
     */
    private fun sweepOrphanHeadlessConversations() {
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                val orphanIds = HeadlessConversations.activeIds()
                if (orphanIds.isEmpty()) return@runCatching
                Log.i(TAG, "sweepOrphanHeadlessConversations: found ${orphanIds.size} candidate(s)")
                val convRepo = get<me.rerere.rikkahub.data.repository.ConversationRepository>()
                for (id in orphanIds) {
                    runCatching {
                        val conv = convRepo.getConversationById(id)
                        if (conv != null && conv.title.startsWith("[Scheduled]")) {
                            Log.i(TAG, "sweepOrphanHeadlessConversations: deleting orphan conv $id")
                            convRepo.deleteConversation(conv)
                        }
                    }.onFailure { Log.w(TAG, "sweepOrphanHeadlessConversations: error for $id", it) }
                }
                HeadlessConversations.clearAll()
                Log.i(TAG, "sweepOrphanHeadlessConversations: sweep complete")
            }.onFailure {
                Log.e(TAG, "sweepOrphanHeadlessConversations failed", it)
            }
        }
    }

    private fun incrementLaunchCount() {
        get<AppScope>().launch {
            runCatching {
                val store = get<SettingsStore>()
                val current = store.settingsFlowRaw.first()
                store.update(current.copy(launchCount = current.launchCount + 1))
                Log.i(TAG, "incrementLaunchCount: ${store.settingsFlowRaw.first().launchCount}")
            }.onFailure {
                Log.e(TAG, "incrementLaunchCount failed", it)
            }
        }
    }

    private fun eagerlyInitChatService() {
        try {
            // Just resolving the singleton triggers Koin's factory; the side effect we care
            // about is the LifecycleRegistry.addObserver call inside ChatService.<init>,
            // which Android requires to happen on the main thread.
            get<me.rerere.rikkahub.service.ChatService>()
            // Installs the one-way Owner Emergency Stop bridge after ChatService exists,
            // avoiding a DI cycle while keeping deactivation on the trusted user page only.
            get<me.rerere.rikkahub.data.ai.EmergencyStopCoordinator>()
        } catch (t: Throwable) {
            Log.e(TAG, "eagerlyInitChatService failed", t)
        }
    }

    private fun startTelegramBotIfEnabled() {
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                val cfg = get<me.rerere.rikkahub.data.telegram.TelegramBotPreferences>().current()
                if (cfg.isUsable) {
                    Log.i(TAG, "startTelegramBotIfEnabled: re-starting bot service")
                    me.rerere.rikkahub.service.TelegramBotService.start(this@RikkaHubApp)
                    // Defense-in-depth against OEM aggressive task-killing: a 30-min
                    // periodic health probe re-starts the service if anything killed it
                    // outside our control. Idempotent — uses ExistingPeriodicWorkPolicy.KEEP.
                    me.rerere.rikkahub.service.TelegramBotHealthWorker.schedule(this@RikkaHubApp)
                } else {
                    me.rerere.rikkahub.service.TelegramBotHealthWorker.cancel(this@RikkaHubApp)
                }
            }.onFailure {
                Log.e(TAG, "startTelegramBotIfEnabled failed", it)
            }
        }
    }

    private fun cleanupWorkspaceTempDirs() {
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                get<WorkspaceManager>().cleanupAllTempDirs()
            }.onFailure {
                Log.e(TAG, "cleanupWorkspaceTempDirs failed", it)
            }
        }
    }

    private fun reconcileMemoryV2Metadata() {
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                val count = get<me.rerere.rikkahub.memory.MemoryMetadataReconciler>().reconcile()
                if (count > 0) Log.i(TAG, "Memory V2 metadata reconciled for $count rows")
            }.onFailure { error ->
                Log.w(TAG, "Memory V2 metadata reconciliation failed", error)
            }
        }
    }

    private fun restoreWorkspaceProcessesWhenForegrounded() {
        ProcessLifecycleOwner.get().lifecycle.addObserver(object : DefaultLifecycleObserver {
            override fun onStart(owner: LifecycleOwner) {
                get<AppScope>().launch(Dispatchers.IO) {
                    runCatching {
                        val repository = get<WorkspaceRepository>()
                        repository.checkIntegrity()
                        val validWorkspaces = repository.getAll().associate { it.id to it.root }
                        val manager = get<me.rerere.workspace.WorkspaceProcessManager>()
                        if (manager.hasDesiredProcesses(validWorkspaces)) {
                            ContextCompat.startForegroundService(
                                this@RikkaHubApp,
                                me.rerere.rikkahub.service.WorkspaceProcessService
                                    .startIntent(this@RikkaHubApp),
                            )
                        }
                    }.onFailure {
                        Log.w(TAG, "restoreWorkspaceProcessesWhenForegrounded failed", it)
                    }
                }
            }
        })
    }

    private fun restoreQuickCaptureWhenForegrounded() {
        ProcessLifecycleOwner.get().lifecycle.addObserver(object : DefaultLifecycleObserver {
            override fun onStart(owner: LifecycleOwner) {
                get<AppScope>().launch(Dispatchers.IO) {
                    runCatching {
                        val settings = get<SettingsStore>().settingsFlowRaw.first()
                        if (settings.quickCaptureSettings.enabled &&
                            android.provider.Settings.canDrawOverlays(this@RikkaHubApp)
                        ) {
                            ContextCompat.startForegroundService(
                                this@RikkaHubApp,
                                me.rerere.rikkahub.quickcapture.QuickCaptureOverlayService
                                    .startIntent(this@RikkaHubApp),
                            )
                        }
                    }.onFailure {
                        Log.w(TAG, "restoreQuickCaptureWhenForegrounded failed", it)
                    }
                }
            }
        })
    }

    /**
     * Unlike boot restore, this runs only after the user explicitly brings RikkaHub to the
     * foreground. This makes an enabled pet return after Android kills the app process without
     * turning the pet into an unsolicited boot-time foreground service.
     */
    private fun restoreDesktopPetWhenForegrounded() {
        ProcessLifecycleOwner.get().lifecycle.addObserver(object : DefaultLifecycleObserver {
            override fun onStart(owner: LifecycleOwner) {
                get<AppScope>().launch(Dispatchers.IO) {
                    runCatching {
                        val settings = get<SettingsStore>().settingsFlow.first { !it.init }
                        if (me.rerere.rikkahub.pet.overlay.PetOverlayRestorePolicy
                                .shouldRestoreOnAppForeground(
                                    selection = settings.resolvePetOverlaySelection()?.selection,
                                    overlayPermissionGranted = android.provider.Settings.canDrawOverlays(this@RikkaHubApp),
                                )
                        ) {
                            me.rerere.rikkahub.pet.overlay.DesktopPetService.start(this@RikkaHubApp)
                        }
                    }.onFailure {
                        Log.w(TAG, "restoreDesktopPetWhenForegrounded failed", it)
                    }
                }
            }
        })
    }

    private fun seedDefaultSkillsIfNeeded() {
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                get<me.rerere.rikkahub.data.files.SkillManager>().seedDefaultSkillsIfNeeded()
            }.onFailure {
                Log.e(TAG, "seedDefaultSkillsIfNeeded failed", it)
            }
        }
    }

    private fun checkWorkspaceIntegrity() {
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                get<WorkspaceRepository>().checkIntegrity()
            }.onFailure {
                Log.e(TAG, "checkWorkspaceIntegrity failed", it)
            }
        }
    }

    private fun deleteTempFiles() {
        get<AppScope>().launch(Dispatchers.IO) {
            val dir = appTempFolder
            if (dir.exists()) {
                dir.deleteRecursively()
            }
        }
    }

    private fun cleanupToolOutputs() {
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                val dir = File(filesDir, FileFolders.TOOL_OUTPUTS)
                if (!dir.exists()) return@runCatching
                val cutoff = System.currentTimeMillis() - TOOL_ARTIFACT_RETENTION_MS
                dir.walkBottomUp()
                    .filter { it != dir && it.lastModified() < cutoff }
                    .forEach { file -> if (file.isDirectory) file.delete() else file.delete() }
                val remaining = dir.walkTopDown().filter(File::isFile)
                    .sortedBy(File::lastModified).toMutableList()
                var total = remaining.sumOf(File::length)
                for (file in remaining) {
                    if (total <= TOOL_ARTIFACT_MAX_BYTES) break
                    val length = file.length()
                    if (file.delete()) total -= length
                }
            }
        }
    }

    private companion object {
        const val TOOL_ARTIFACT_RETENTION_MS = 7L * 24 * 60 * 60 * 1_000
        const val TOOL_ARTIFACT_MAX_BYTES = 512L * 1024 * 1024
    }

    private fun syncManagedFiles() {
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                get<FilesManager>().syncFolder()
            }.onFailure {
                Log.e(TAG, "syncManagedFiles failed", it)
            }
        }
    }

    private fun cleanupQuickCaptureFiles() {
        get<AppScope>().launch(Dispatchers.IO) {
            runCatching {
                get<me.rerere.rikkahub.quickcapture.QuickCaptureFileCleaner>().cleanup()
            }.onFailure {
                Log.w(TAG, "cleanupQuickCaptureFiles failed", it)
            }
        }
    }

    private fun startWebServerIfEnabled() {
        get<AppScope>().launch {
            runCatching {
                delay(500)
                val settings = get<SettingsStore>().settingsFlowRaw.first()
                if (settings.webServerEnabled) {
                    if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU &&
                        ContextCompat.checkSelfPermission(
                            this@RikkaHubApp,
                            android.Manifest.permission.POST_NOTIFICATIONS
                        ) != PackageManager.PERMISSION_GRANTED
                    ) {
                        Log.w(TAG, "startWebServerIfEnabled: notification permission not granted, skipping")
                        return@launch
                    }
                    val intent = Intent(this@RikkaHubApp, WebServerService::class.java).apply {
                        action = WebServerService.ACTION_START
                        putExtra(WebServerService.EXTRA_PORT, settings.webServerPort)
                        putExtra(WebServerService.EXTRA_LOCALHOST_ONLY, true)
                    }
                    startForegroundService(intent)
                }
            }.onFailure {
                Log.e(TAG, "startWebServerIfEnabled failed", it)
            }
        }
    }

    private fun createNotificationChannel() {
        val notificationManager = NotificationManagerCompat.from(this)
        val chatCompletedChannel = NotificationChannelCompat
            .Builder(
                CHAT_COMPLETED_NOTIFICATION_CHANNEL_ID,
                NotificationManagerCompat.IMPORTANCE_HIGH
            )
            .setName(getString(R.string.notification_channel_chat_completed))
            .setVibrationEnabled(true)
            .build()
        notificationManager.createNotificationChannel(chatCompletedChannel)

        val chatLiveUpdateChannel = NotificationChannelCompat
            .Builder(
                CHAT_LIVE_UPDATE_NOTIFICATION_CHANNEL_ID,
                NotificationManagerCompat.IMPORTANCE_LOW
            )
            .setName(getString(R.string.notification_channel_chat_live_update))
            .setVibrationEnabled(false)
            .build()
        notificationManager.createNotificationChannel(chatLiveUpdateChannel)

        val webServerChannel = NotificationChannelCompat
            .Builder(WEB_SERVER_NOTIFICATION_CHANNEL_ID, NotificationManagerCompat.IMPORTANCE_LOW)
            .setName(getString(R.string.notification_channel_web_server))
            .setVibrationEnabled(false)
            .setShowBadge(false)
            .build()
        notificationManager.createNotificationChannel(webServerChannel)

        val workspaceProcessChannel = NotificationChannelCompat
            .Builder(
                WORKSPACE_PROCESS_NOTIFICATION_CHANNEL_ID,
                NotificationManagerCompat.IMPORTANCE_LOW,
            )
            .setName(getString(R.string.notification_channel_workspace_process))
            .setVibrationEnabled(false)
            .setShowBadge(false)
            .build()
        notificationManager.createNotificationChannel(workspaceProcessChannel)

        val accessibilityKeepAliveChannel = NotificationChannelCompat
            .Builder(
                ACCESSIBILITY_KEEP_ALIVE_NOTIFICATION_CHANNEL_ID,
                NotificationManagerCompat.IMPORTANCE_LOW,
            )
            .setName(getString(R.string.notification_channel_accessibility_keep_alive))
            .setVibrationEnabled(false)
            .setShowBadge(false)
            .build()
        notificationManager.createNotificationChannel(accessibilityKeepAliveChannel)

        val alarmChannel = NotificationChannelCompat
            .Builder("alarm", NotificationManagerCompat.IMPORTANCE_HIGH)
            .setName("Alarm")
            .setVibrationEnabled(true)
            .setShowBadge(true)
            .build()
        notificationManager.createNotificationChannel(alarmChannel)
    }

    override fun onTerminate() {
        me.rerere.rikkahub.assistant.SystemAssistantSessionAdapterRegistry.reset()
        if (dependencyGraphStarted) {
            get<AppScope>().cancel()
            stopService(Intent(this, WebServerService::class.java))
        }
        super.onTerminate()
    }

    private fun isVoiceInteractorProcess(): Boolean = isVoiceInteractorProcess(
        packageName = packageName,
        processName = currentProcessNameCompat(),
    )

    private fun isPluginRuntimeProcess(): Boolean = isPluginRuntimeProcess(
        packageName = packageName,
        processName = currentProcessNameCompat(),
    )

    @Suppress("DEPRECATION")
    private fun currentProcessNameCompat(): String? = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
        getProcessName()
    } else {
        val processId = Process.myPid()
        getSystemService(ActivityManager::class.java)
            ?.runningAppProcesses
            ?.firstOrNull { it.pid == processId }
            ?.processName
            ?: runCatching {
                File("/proc/self/cmdline").inputStream().bufferedReader().use { reader ->
                    reader.readLine()?.trimEnd('\u0000')
                }
            }.getOrNull()
    }
}

internal fun isVoiceInteractorProcess(packageName: String, processName: String?): Boolean =
    processName == packageName + VOICE_INTERACTOR_PROCESS_SUFFIX

internal fun isPluginRuntimeProcess(packageName: String, processName: String?): Boolean =
    processName == packageName + PLUGIN_RUNTIME_PROCESS_SUFFIX

class AppScope : CoroutineScope by CoroutineScope(
    SupervisorJob()
        + Dispatchers.Main
        + CoroutineName("AppScope")
        + CoroutineExceptionHandler { _, e ->
        Log.e(TAG, "AppScope exception", e)
    }
)
