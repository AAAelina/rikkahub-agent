package me.rerere.rikkahub.ui.pages.setting

import android.content.Context
import android.net.Uri
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import java.io.File
import java.io.FileOutputStream
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import me.rerere.rikkahub.data.datastore.Settings
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.data.model.Assistant
import me.rerere.rikkahub.plugin.InstalledPluginRecord
import me.rerere.rikkahub.plugin.PluginBuiltInExampleInstaller
import me.rerere.rikkahub.plugin.PluginPackageInstaller
import me.rerere.rikkahub.plugin.PluginRegistryStore
import me.rerere.rikkahub.plugin.PluginReviewStatus

sealed interface PluginSettingsNotice {
    data class Installed(val name: String, val addedPermissionCount: Int) : PluginSettingsNotice
    data class Failed(val code: String) : PluginSettingsNotice
    data object Updated : PluginSettingsNotice
}

class AgentRuntimeSettingsViewModel(
    context: Context,
    private val settingsStore: SettingsStore,
    private val registry: PluginRegistryStore,
    private val installer: PluginPackageInstaller,
    private val builtInInstaller: PluginBuiltInExampleInstaller,
) : ViewModel() {
    private val appContext = context.applicationContext

    val settings: StateFlow<Settings> = settingsStore.settingsFlow.stateIn(
        viewModelScope,
        SharingStarted.Eagerly,
        Settings.dummy(),
    )
    private val _plugins = MutableStateFlow(registry.snapshot())
    val plugins: StateFlow<List<InstalledPluginRecord>> = _plugins.asStateFlow()
    private val _busy = MutableStateFlow(false)
    val busy: StateFlow<Boolean> = _busy.asStateFlow()
    private val _notice = MutableStateFlow<PluginSettingsNotice?>(null)
    val notice: StateFlow<PluginSettingsNotice?> = _notice.asStateFlow()

    fun updateSettings(transform: (Settings) -> Settings) {
        viewModelScope.launch { settingsStore.update(transform) }
    }

    fun updateCurrentAssistant(transform: (Assistant) -> Assistant) {
        viewModelScope.launch {
            settingsStore.update { current ->
                current.copy(
                    assistants = current.assistants.map { assistant ->
                        if (assistant.id == current.assistantId) transform(assistant) else assistant
                    }
                )
            }
        }
    }

    fun install(uri: Uri) {
        runPluginOperation {
            val importDir = File(appContext.cacheDir, "plugin-imports").apply { mkdirs() }
            val archive = File.createTempFile("plugin-import-", ".zip", importDir)
            try {
                appContext.contentResolver.openInputStream(uri)?.use { input ->
                    FileOutputStream(archive).use { output ->
                        val buffer = ByteArray(16 * 1024)
                        var total = 0L
                        while (true) {
                            val count = input.read(buffer)
                            if (count < 0) break
                            total += count
                            require(total <= MAX_IMPORT_BYTES) { "plugin_archive_size_invalid" }
                            output.write(buffer, 0, count)
                        }
                        output.fd.sync()
                    }
                } ?: error("plugin_archive_unreadable")
                installer.install(archive).getOrThrow()
            } finally {
                archive.delete()
            }
        }
    }

    fun installBuiltInExample() {
        runPluginOperation { builtInInstaller.install().getOrThrow() }
    }

    fun approve(pluginId: String) {
        updatePlugin(pluginId) { record ->
            record.copy(
                enabled = false,
                reviewStatus = PluginReviewStatus.APPROVED,
                failureTimestampsMs = emptyList(),
                pendingAddedPermissions = emptySet(),
            )
        }
    }

    fun setPluginEnabled(pluginId: String, enabled: Boolean) {
        updatePlugin(pluginId) { record ->
            require(!enabled || record.reviewStatus == PluginReviewStatus.APPROVED) {
                "plugin_not_approved"
            }
            record.copy(enabled = enabled)
        }
    }

    fun setPluginEnabledForCurrentAssistant(pluginId: String, enabled: Boolean) {
        updateCurrentAssistant { assistant ->
            assistant.copy(
                enabledPluginIds = if (enabled) {
                    assistant.enabledPluginIds + pluginId
                } else {
                    assistant.enabledPluginIds - pluginId
                }
            )
        }
    }

    fun clearNotice() {
        _notice.value = null
    }

    private fun updatePlugin(
        pluginId: String,
        transform: (InstalledPluginRecord) -> InstalledPluginRecord,
    ) {
        viewModelScope.launch(Dispatchers.IO) {
            runCatching { registry.update(pluginId, transform) }
                .onSuccess {
                    refreshPlugins()
                    _notice.value = PluginSettingsNotice.Updated
                }
                .onFailure { failure ->
                    _notice.value = PluginSettingsNotice.Failed(failure.toPluginCode())
                }
        }
    }

    private fun runPluginOperation(
        operation: suspend () -> me.rerere.rikkahub.plugin.PluginInstallResult,
    ) {
        if (_busy.value) return
        viewModelScope.launch {
            _busy.value = true
            try {
                val result = withContext(Dispatchers.IO) { operation() }
                refreshPlugins()
                _notice.value = PluginSettingsNotice.Installed(
                    result.record.name,
                    result.addedPermissions.size,
                )
            } catch (failure: Throwable) {
                _notice.value = PluginSettingsNotice.Failed(failure.toPluginCode())
            } finally {
                _busy.value = false
            }
        }
    }

    private fun refreshPlugins() {
        _plugins.value = registry.snapshot()
    }

    private fun Throwable.toPluginCode(): String = message
        ?.takeIf { it.matches(Regex("[a-z0-9_]{3,80}")) }
        ?: "plugin_operation_failed"

    private companion object {
        const val MAX_IMPORT_BYTES = 8L * 1024 * 1024
    }
}
