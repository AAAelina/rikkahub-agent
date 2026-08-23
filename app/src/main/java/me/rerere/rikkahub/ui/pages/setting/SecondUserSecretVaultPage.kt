package me.rerere.rikkahub.ui.pages.setting

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.LargeFlexibleTopAppBar
import androidx.compose.material3.ListItem
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TopAppBarDefaults
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.nestedscroll.nestedScroll
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.unit.dp
import kotlinx.coroutines.launch
import me.rerere.rikkahub.R
import me.rerere.rikkahub.assistant.SecondUserAuthorityRegistry
import me.rerere.rikkahub.security.SecretLeaseResult
import me.rerere.rikkahub.security.SecondUserLegacySecretMigration
import me.rerere.rikkahub.security.SecondUserLegacySecretMigrationResult
import me.rerere.rikkahub.security.SecretSlotMetadata
import me.rerere.rikkahub.security.SecondUserSecretVault
import me.rerere.rikkahub.security.SecondUserSecretAccessMode
import me.rerere.rikkahub.security.SecretPlaintextSessionManager
import me.rerere.rikkahub.security.SecretPlaintextSessionState
import me.rerere.rikkahub.security.StrongBiometricAuthenticator
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.ui.components.nav.BackButton
import me.rerere.rikkahub.ui.theme.CustomColors
import me.rerere.rikkahub.utils.getActivity
import org.koin.compose.koinInject

/**
 * User-facing vault surface. Secret values are intentionally unavailable to the model-facing
 * management tools; only this BIOMETRIC_STRONG-gated page ever turns a value into visible text.
 */
@Composable
fun SecondUserSecretVaultPage(
    vault: SecondUserSecretVault = koinInject(),
    legacyMigration: SecondUserLegacySecretMigration = koinInject(),
    biometric: StrongBiometricAuthenticator = koinInject(),
    settingsStore: SettingsStore = koinInject(),
    plaintextSessions: SecretPlaintextSessionManager = koinInject(),
) {
    val context = LocalContext.current
    val settings by settingsStore.settingsFlow.collectAsState()
    val plaintextState by plaintextSessions.state.collectAsState()
    val scope = rememberCoroutineScope()
    val scrollBehavior = TopAppBarDefaults.exitUntilCollapsedScrollBehavior()
    var slots by remember { mutableStateOf(emptyList<SecretSlotMetadata>()) }
    var busy by remember { mutableStateOf(false) }
    var slotIdInput by remember { mutableStateOf("") }
    var secretInput by remember { mutableStateOf("") }
    var editingSlotId by remember { mutableStateOf<String?>(null) }
    var migrationResult by remember { mutableStateOf<SecondUserLegacySecretMigrationResult?>(null) }
    var confirmRemoteMode by remember { mutableStateOf(false) }

    val activity = context.getActivity()
    DisposableEffect(activity) {
        activity?.window?.addFlags(android.view.WindowManager.LayoutParams.FLAG_SECURE)
        onDispose {
            activity?.window?.clearFlags(android.view.WindowManager.LayoutParams.FLAG_SECURE)
        }
    }

    if (confirmRemoteMode) {
        androidx.compose.material3.AlertDialog(
            onDismissRequest = { confirmRemoteMode = false },
            title = { Text(stringResource(R.string.second_user_plaintext_mode_title)) },
            text = { Text(stringResource(R.string.second_user_plaintext_mode_risk)) },
            confirmButton = {
                TextButton(onClick = {
                    confirmRemoteMode = false
                    scope.launch {
                        settingsStore.update {
                            it.copy(secondUserSecretAccessMode = SecondUserSecretAccessMode.PLAINTEXT_REMOTE_SESSION)
                        }
                    }
                }) { Text(stringResource(R.string.second_user_plaintext_mode_confirm)) }
            },
            dismissButton = {
                TextButton(onClick = { confirmRemoteMode = false }) {
                    Text(stringResource(android.R.string.cancel))
                }
            },
        )
    }

    fun withUserAuthorization(action: suspend (me.rerere.rikkahub.security.SecretVaultUserAuthorization) -> Unit) {
        scope.launch {
            if (busy) return@launch
            busy = true
            try {
                biometric.authorizeSecretVault(
                    title = context.getString(R.string.second_user_vault_title),
                    subtitle = context.getString(R.string.second_user_vault_desc),
                )?.let { authorization ->
                    action(authorization)
                    slots = vault.listMetadataForUser(authorization)
                }
            } finally {
                busy = false
            }
        }
    }

    Scaffold(
        modifier = Modifier.nestedScroll(scrollBehavior.nestedScrollConnection),
        topBar = {
            LargeFlexibleTopAppBar(
                title = { Text(stringResource(R.string.second_user_vault_title)) },
                navigationIcon = { BackButton() },
                scrollBehavior = scrollBehavior,
                colors = CustomColors.topBarColors,
            )
        },
        containerColor = CustomColors.topBarColors.containerColor,
    ) { innerPadding ->
        LazyColumn(
            modifier = Modifier.padding(innerPadding),
            contentPadding = PaddingValues(12.dp),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            item {
                Text(
                    stringResource(R.string.second_user_vault_desc),
                    style = MaterialTheme.typography.bodyMedium,
                )
            }
            item {
                Card(colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceContainer)) {
                    Column(
                        modifier = Modifier.padding(16.dp),
                        verticalArrangement = Arrangement.spacedBy(10.dp),
                    ) {
                        Text(
                            stringResource(R.string.second_user_plaintext_mode_title),
                            style = MaterialTheme.typography.titleMedium,
                        )
                        androidx.compose.foundation.layout.Row(
                            modifier = Modifier.fillMaxWidth(),
                            horizontalArrangement = Arrangement.spacedBy(12.dp),
                            verticalAlignment = androidx.compose.ui.Alignment.CenterVertically,
                        ) {
                            Text(
                                if (settings.secondUserSecretAccessMode == SecondUserSecretAccessMode.PLAINTEXT_REMOTE_SESSION) {
                                    stringResource(R.string.second_user_plaintext_mode_remote)
                                } else {
                                    stringResource(R.string.second_user_plaintext_mode_use_only)
                                },
                                modifier = Modifier.weight(1f),
                                style = MaterialTheme.typography.bodySmall,
                            )
                            Switch(
                                checked = settings.secondUserSecretAccessMode == SecondUserSecretAccessMode.PLAINTEXT_REMOTE_SESSION,
                                onCheckedChange = { enabled ->
                                    if (enabled) {
                                        confirmRemoteMode = true
                                    } else {
                                        plaintextSessions.close()
                                        scope.launch {
                                            settingsStore.update {
                                                it.copy(secondUserSecretAccessMode = SecondUserSecretAccessMode.USE_ONLY)
                                            }
                                        }
                                    }
                                },
                            )
                        }
                        when (val state = plaintextState) {
                            is SecretPlaintextSessionState.Open -> {
                                val formatted = remember(state.expiresAtMs) {
                                    java.text.DateFormat.getTimeInstance(java.text.DateFormat.SHORT)
                                        .format(java.util.Date(state.expiresAtMs))
                                }
                                Text(stringResource(R.string.second_user_plaintext_session_active, formatted))
                                Button(onClick = { plaintextSessions.close() }) {
                                    Text(stringResource(R.string.second_user_plaintext_session_close))
                                }
                            }
                            SecretPlaintextSessionState.Closed -> {
                                Text(stringResource(R.string.second_user_plaintext_session_closed))
                                Button(
                                    enabled = !busy && settings.secondUserSecretAccessMode == SecondUserSecretAccessMode.PLAINTEXT_REMOTE_SESSION,
                                    onClick = {
                                        scope.launch {
                                            if (busy) return@launch
                                            busy = true
                                            try {
                                                biometric.authorizeSecretPlaintextSession(
                                                    title = context.getString(R.string.second_user_plaintext_mode_title),
                                                    subtitle = context.getString(R.string.second_user_plaintext_mode_risk),
                                                )?.let { plaintextSessions.openForCurrent(it) }
                                            } finally {
                                                busy = false
                                            }
                                        }
                                    },
                                ) {
                                    Text(stringResource(R.string.second_user_plaintext_session_open))
                                }
                            }
                        }
                    }
                }
            }
            item {
                Button(
                    enabled = !busy,
                    onClick = { withUserAuthorization { } },
                ) {
                    Text(stringResource(R.string.second_user_vault_open))
                }
            }
            item {
                Card(colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceContainer)) {
                    Column(
                        modifier = Modifier.padding(16.dp),
                        verticalArrangement = Arrangement.spacedBy(8.dp),
                    ) {
                        Button(
                            enabled = !busy,
                            onClick = {
                                withUserAuthorization { authorization ->
                                    migrationResult = legacyMigration.migrateForUser(authorization)
                                }
                            },
                        ) {
                            Text(stringResource(R.string.second_user_vault_migrate_legacy))
                        }
                        migrationResult?.let { result ->
                            Text(
                                stringResource(
                                    R.string.second_user_vault_migration_result,
                                    result.migratedTotal,
                                    result.pendingEntries,
                                ),
                                style = MaterialTheme.typography.bodySmall,
                            )
                        }
                    }
                }
            }
            item {
                Card(colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceContainer)) {
                    Column(
                        modifier = Modifier.padding(16.dp),
                        verticalArrangement = Arrangement.spacedBy(8.dp),
                    ) {
                        OutlinedTextField(
                            value = slotIdInput,
                            onValueChange = { slotIdInput = it.take(96) },
                            modifier = Modifier.fillMaxWidth(),
                            label = { Text(stringResource(R.string.second_user_vault_slot_label)) },
                            singleLine = true,
                        )
                        Button(
                            enabled = !busy && slotIdInput.isNotBlank(),
                            onClick = {
                                val requestedSlotId = slotIdInput.trim()
                                withUserAuthorization { authorization ->
                                    val active = SecondUserAuthorityRegistry.current() ?: return@withUserAuthorization
                                    if (vault.createEmptySlot(
                                            metadata = SecretSlotMetadata(
                                                slotId = requestedSlotId,
                                                label = requestedSlotId,
                                                purpose = "user-created",
                                                authoritySubjectId = active.subjectId,
                                                createdAtMs = System.currentTimeMillis(),
                                                updatedAtMs = System.currentTimeMillis(),
                                            ),
                                            subjectId = active.subjectId,
                                        )
                                    ) {
                                        slotIdInput = ""
                                    }
                                }
                            },
                        ) {
                            Text(stringResource(R.string.second_user_vault_add))
                        }
                    }
                }
            }
            if (slots.isEmpty()) {
                item { Text(stringResource(R.string.second_user_vault_empty)) }
            }
            items(slots, key = { it.slotId }) { slot ->
                Card(colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceContainer)) {
                    ListItem(
                        headlineContent = { Text(slot.label.ifBlank { slot.slotId }) },
                        supportingContent = {
                            Text(
                                buildString {
                                    append(slot.purpose)
                                    if (slot.bindings.isNotEmpty()) {
                                        append(" • ")
                                        append(slot.bindings.joinToString { it.kind.name })
                                    }
                                },
                            )
                        },
                        trailingContent = {
                            Column {
                                TextButton(
                                    enabled = !busy,
                                    onClick = {
                                        withUserAuthorization { authorization ->
                                            when (val result = vault.withUserSecret(authorization, slot.slotId) {
                                                it.concatToString()
                                            }) {
                                                is SecretLeaseResult.Success -> {
                                                    editingSlotId = slot.slotId
                                                    secretInput = result.value
                                                }
                                                SecretLeaseResult.SlotMissing -> {
                                                    // Empty Vault slots are valid: they are created first and
                                                    // populated afterwards. Treat a missing value file as an empty
                                                    // secret so the user can enter the initial credential instead of
                                                    // silently returning from the biometric prompt with no editor.
                                                    editingSlotId = slot.slotId
                                                    secretInput = ""
                                                }
                                                else -> Unit
                                            }
                                        }
                                    },
                                ) { Text(stringResource(R.string.second_user_vault_secret)) }
                                TextButton(
                                    enabled = !busy,
                                    onClick = {
                                        withUserAuthorization { authorization ->
                                            vault.deleteForUser(authorization, slot.slotId)
                                            if (editingSlotId == slot.slotId) {
                                                editingSlotId = null
                                                secretInput = ""
                                            }
                                        }
                                    },
                                ) { Text(stringResource(R.string.second_user_vault_delete)) }
                            }
                        },
                    )
                }
            }
            editingSlotId?.let { slotId ->
                item {
                    Card(colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceContainer)) {
                        Column(
                            modifier = Modifier.padding(16.dp),
                            verticalArrangement = Arrangement.spacedBy(8.dp),
                        ) {
                            OutlinedTextField(
                                value = secretInput,
                                onValueChange = { secretInput = it },
                                modifier = Modifier.fillMaxWidth(),
                                label = { Text(stringResource(R.string.second_user_vault_secret)) },
                                visualTransformation = PasswordVisualTransformation(),
                            )
                            Button(
                                enabled = !busy,
                                onClick = {
                                    withUserAuthorization { authorization ->
                                        // Do not materialize a mutable secret before the user has
                                        // completed BIOMETRIC_STRONG. The vault clears this buffer
                                        // in all success and failure paths.
                                        val replacement = secretInput.toCharArray()
                                        if (vault.storeForUser(authorization, slotId, replacement)) {
                                            secretInput = ""
                                        }
                                    }
                                },
                            ) { Text(stringResource(R.string.second_user_vault_save)) }
                            TextButton(
                                enabled = !busy,
                                onClick = {
                                    withUserAuthorization { authorization ->
                                        val active = SecondUserAuthorityRegistry.current() ?: return@withUserAuthorization
                                        val slot = slots.firstOrNull { it.slotId == slotId }
                                            ?: return@withUserAuthorization
                                        vault.rebindForUser(
                                            authorization = authorization,
                                            slotId = slotId,
                                            newSubjectId = active.subjectId,
                                            bindings = slot.bindings,
                                        )
                                    }
                                },
                            ) { Text(stringResource(R.string.second_user_vault_rebind)) }
                        }
                    }
                }
            }
        }
    }
}
