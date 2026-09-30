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
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TopAppBarDefaults
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableIntStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.nestedscroll.nestedScroll
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.launch
import me.rerere.rikkahub.R
import me.rerere.rikkahub.Screen
import me.rerere.rikkahub.assistant.SecondUserAuthorityResolution
import me.rerere.rikkahub.assistant.SecondUserAuthorityRevocationCoordinator
import me.rerere.rikkahub.assistant.SecondUserAuthorityService
import me.rerere.rikkahub.assistant.SecondUserAuthorityState
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.data.repository.ConversationRepository
import me.rerere.rikkahub.security.StrongBiometricAuthenticator
import me.rerere.rikkahub.security.SecondUserAuthorityUserAuthorization
import me.rerere.rikkahub.ui.components.nav.BackButton
import me.rerere.rikkahub.ui.components.ui.Select
import me.rerere.rikkahub.ui.context.LocalNavController
import me.rerere.rikkahub.ui.theme.CustomColors
import org.koin.compose.koinInject
import kotlin.uuid.Uuid

/**
 * The only app UI which may activate, replace, or unassign global second-user authority.
 * Every state-changing action is preceded by a BIOMETRIC_STRONG prompt; no model tool reaches
 * this page or its service methods.
 */
@Composable
fun SecondUserAuthorityRecoveryPage(
    settingsStore: SettingsStore = koinInject(),
    conversations: ConversationRepository = koinInject(),
    authority: SecondUserAuthorityService = koinInject(),
    revocation: SecondUserAuthorityRevocationCoordinator = koinInject(),
    biometric: StrongBiometricAuthenticator = koinInject(),
) {
    val context = LocalContext.current
    val navigator = LocalNavController.current
    val scope = rememberCoroutineScope()
    val settings by settingsStore.settingsFlow.collectAsStateWithLifecycle()
    val scrollBehavior = TopAppBarDefaults.exitUntilCollapsedScrollBehavior()
    var refresh by remember { mutableIntStateOf(0) }
    var resolution by remember { mutableStateOf<SecondUserAuthorityResolution?>(null) }
    var selectedAssistantId by remember { mutableStateOf<Uuid?>(null) }
    var busy by remember { mutableStateOf(false) }

    LaunchedEffect(refresh) {
        resolution = authority.resolve()
        val configuredId = authority.currentConfig().assistantId
        if (selectedAssistantId == null || settings.assistants.none { it.id == selectedAssistantId }) {
            selectedAssistantId = configuredId ?: settings.assistants.firstOrNull()?.id
        }
    }
    LaunchedEffect(settings.assistants) {
        if (selectedAssistantId == null) selectedAssistantId = settings.assistants.firstOrNull()?.id
    }

    val candidateConversations by remember(selectedAssistantId) {
        selectedAssistantId?.let(conversations::getConversationsOfAssistant) ?: flowOf(emptyList())
    }.collectAsStateWithLifecycle(initialValue = emptyList())

    fun runStrong(action: suspend (SecondUserAuthorityUserAuthorization) -> Unit) {
        scope.launch {
            if (busy) return@launch
            busy = true
            try {
                val authorization = biometric.authorizeSecondUser(
                    title = context.getString(R.string.second_user_authority_recovery_title),
                    subtitle = context.getString(R.string.second_user_authority_recovery_desc),
                )
                if (authorization != null) action(authorization)
            } finally {
                busy = false
                refresh++
            }
        }
    }

    val configuredSnapshot = (resolution as? SecondUserAuthorityResolution.Active)?.snapshot
    val pendingConfig = (resolution as? SecondUserAuthorityResolution.Pending)?.config
    val currentConversationId = configuredSnapshot?.conversationId ?: pendingConfig?.conversationId

    Scaffold(
        modifier = Modifier.nestedScroll(scrollBehavior.nestedScrollConnection),
        topBar = {
            LargeFlexibleTopAppBar(
                title = { Text(stringResource(R.string.second_user_authority_recovery_title)) },
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
                    text = stringResource(R.string.second_user_authority_recovery_desc),
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
                            text = when (resolution) {
                                is SecondUserAuthorityResolution.Active ->
                                    stringResource(R.string.assistant_privileged_conversation)
                                is SecondUserAuthorityResolution.Pending ->
                                    stringResource(R.string.second_user_authority_pending)
                                is SecondUserAuthorityResolution.Invalid ->
                                    stringResource(R.string.second_user_authority_invalid)
                                else -> stringResource(R.string.assistant_privileged_conversation_none)
                            },
                            style = MaterialTheme.typography.titleMedium,
                        )
                        currentConversationId?.let {
                            Text(
                                text = it.toString().take(18),
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                            )
                        }
                        if (pendingConfig?.let {
                                it.state == SecondUserAuthorityState.PENDING_CONFIRMATION &&
                                    it.assistantId != null &&
                                    it.conversationId != null
                            } == true
                        ) {
                            Button(
                                onClick = {
                                    runStrong { authorization ->
                                        authority.confirmActive(authorization)
                                        val active = authority.resolve() as? SecondUserAuthorityResolution.Active
                                        active?.snapshot?.conversationId?.let { id ->
                                            navigator.clearAndNavigate(Screen.Chat(id.toString()))
                                        }
                                    }
                                },
                                enabled = !busy,
                            ) {
                                Text(stringResource(R.string.second_user_authority_confirm))
                            }
                        }
                        if (currentConversationId != null) {
                            TextButton(
                                enabled = !busy,
                                onClick = {
                                    runStrong { authorization ->
                                        authority.beginRevocation(authorization)
                                        revocation.resumeIfNeeded()
                                        navigator.clearAndNavigate(Screen.Assistant)
                                    }
                                },
                            ) {
                                Text(stringResource(R.string.assistant_privileged_conversation_none))
                            }
                        }
                        TextButton(
                            enabled = !busy,
                            onClick = { navigator.navigate(Screen.SecondUserSecretVault) },
                        ) {
                            Text(stringResource(R.string.second_user_vault_open))
                        }
                        if (configuredSnapshot != null) {
                            TextButton(
                                enabled = !busy,
                                onClick = { navigator.navigate(Screen.SecondUserToolLibrary) },
                            ) {
                                Text(stringResource(R.string.second_user_tool_library_title))
                            }
                        }
                    }
                }
            }
            item {
                Text(
                    text = stringResource(R.string.assistant_privileged_conversation),
                    style = MaterialTheme.typography.titleMedium,
                )
            }
            item {
                val selectedAssistant = settings.assistants.firstOrNull { it.id == selectedAssistantId }
                if (selectedAssistant != null) {
                    Select(
                        options = settings.assistants,
                        selectedOption = selectedAssistant,
                        onOptionSelected = { selectedAssistantId = it.id },
                        modifier = Modifier.fillMaxWidth(),
                        optionToString = { assistant -> assistant.name.ifBlank { assistant.id.toString() } },
                    )
                }
            }
            items(candidateConversations.size, key = { index -> candidateConversations[index].id.toString() }) { index ->
                val conversation = candidateConversations[index]
                Card(colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceContainer)) {
                    ListItem(
                        headlineContent = {
                            Text(conversation.title.ifBlank { conversation.id.toString().take(12) })
                        },
                        supportingContent = {
                            Text(conversation.id.toString().take(18))
                        },
                        trailingContent = {
                            TextButton(
                                enabled = !busy,
                                onClick = {
                                    runStrong { authorization ->
                                        val current = authority.currentConfig()
                                        if (current.state != SecondUserAuthorityState.UNCONFIGURED) {
                                            authority.beginRevocation(authorization)
                                            revocation.resumeIfNeeded()
                                        }
                                        // Revocation may include a real process stop and take
                                        // longer than a short-lived biometric grant. Re-auth
                                        // before binding the replacement so expiry never causes
                                        // an ambiguous half-reassignment.
                                        val replacementAuthorization = biometric.authorizeSecondUser(
                                            title = context.getString(R.string.second_user_authority_recovery_title),
                                            subtitle = context.getString(R.string.second_user_authority_recovery_desc),
                                        ) ?: return@runStrong
                                        authority.stageReassignment(
                                            authorization = replacementAuthorization,
                                            assistantId = conversation.assistantId,
                                            conversationId = conversation.id,
                                            auditId = "user-biometric-selection",
                                        )
                                        authority.confirmActive(replacementAuthorization)
                                        navigator.clearAndNavigate(Screen.Chat(conversation.id.toString()))
                                    }
                                },
                            ) {
                                Text(stringResource(R.string.second_user_authority_confirm))
                            }
                        },
                    )
                }
            }
        }
    }
}
