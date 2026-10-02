package me.rerere.rikkahub.ui.pages.assistant.detail

import me.rerere.hugeicons.HugeIcons
import me.rerere.hugeicons.stroke.BookOpen01
import me.rerere.hugeicons.stroke.Brain02
import me.rerere.hugeicons.stroke.ArrowRight01
import me.rerere.hugeicons.stroke.Code
import me.rerere.hugeicons.stroke.Message02
import me.rerere.hugeicons.stroke.Settings03
import me.rerere.hugeicons.stroke.Puzzle
import me.rerere.hugeicons.stroke.Wrench01
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.material3.Icon
import androidx.compose.material3.LargeFlexibleTopAppBar
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBarDefaults
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.input.nestedscroll.nestedScroll
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import me.rerere.rikkahub.R
import me.rerere.rikkahub.Screen
import me.rerere.rikkahub.data.model.Assistant
import me.rerere.rikkahub.ui.components.nav.BackButton
import me.rerere.rikkahub.ui.components.ui.CardGroup
import me.rerere.rikkahub.ui.components.ui.UIAvatar
import me.rerere.rikkahub.ui.context.LocalNavController
import me.rerere.rikkahub.ui.hooks.heroAnimation
import me.rerere.rikkahub.ui.theme.CustomColors
import me.rerere.rikkahub.utils.plus
import org.koin.androidx.compose.koinViewModel
import org.koin.core.parameter.parametersOf

@Composable
fun AssistantDetailPage(id: String) {
    val vm: AssistantDetailVM = koinViewModel(
        parameters = {
            parametersOf(id)
        }
    )
    val assistant by vm.assistant.collectAsStateWithLifecycle()
    val settings by vm.settings.collectAsStateWithLifecycle()
    val pendingReviewCount by vm.pendingReviewCount.collectAsStateWithLifecycle()
    val navController = LocalNavController.current
    val scrollBehavior = TopAppBarDefaults.exitUntilCollapsedScrollBehavior()
    var showPetSettings by remember { mutableStateOf(false) }

    Scaffold(
        topBar = {
            LargeFlexibleTopAppBar(
                title = {
                    Text(
                        text = assistant.name.ifBlank {
                            stringResource(R.string.assistant_page_default_assistant)
                        },
                        maxLines = 1,
                    )
                },
                navigationIcon = {
                    BackButton()
                },
                scrollBehavior = scrollBehavior,
                colors = CustomColors.topBarColors
            )
        },
        modifier = Modifier.nestedScroll(scrollBehavior.nestedScrollConnection),
        containerColor = CustomColors.topBarColors.containerColor
    ) { innerPadding ->
        LazyColumn(
            modifier = Modifier.fillMaxSize(),
            contentPadding = innerPadding + PaddingValues(8.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            item {
                AssistantHeader(
                    assistant = assistant,
                    modifier = Modifier.padding(horizontal = 8.dp, vertical = 16.dp)
                )
            }

            item {
                CardGroup(
                    modifier = Modifier.padding(horizontal = 8.dp),
                ) {
                    item(
                        onClick = { navController.navigate(Screen.AssistantBasic(id)) },
                        leadingContent = { Icon(HugeIcons.Settings03, null) },
                        supportingContent = { Text(stringResource(R.string.assistant_detail_basic_desc)) },
                        headlineContent = { Text(stringResource(R.string.assistant_page_tab_basic)) },
                        trailingContent = { Icon(HugeIcons.ArrowRight01, null) },
                    )
                    item(
                        onClick = { navController.navigate(Screen.AssistantPrompt(id)) },
                        leadingContent = { Icon(HugeIcons.Message02, null) },
                        supportingContent = { Text(stringResource(R.string.assistant_detail_prompt_desc)) },
                        headlineContent = { Text(stringResource(R.string.assistant_page_tab_prompt)) },
                        trailingContent = { Icon(HugeIcons.ArrowRight01, null) },
                    )
                    item(
                        onClick = { navController.navigate(Screen.AssistantInjections(id)) },
                        leadingContent = { Icon(HugeIcons.Puzzle, null) },
                        supportingContent = { Text(stringResource(R.string.assistant_detail_extensions_desc)) },
                        headlineContent = { Text(stringResource(R.string.assistant_page_tab_extensions)) },
                        trailingContent = { Icon(HugeIcons.ArrowRight01, null) },
                    )
                    item(
                        onClick = { navController.navigate(Screen.AssistantMemory(id)) },
                        leadingContent = { Icon(HugeIcons.Brain02, null) },
                        supportingContent = {
                            Column {
                                Text(stringResource(R.string.assistant_detail_memory_desc))
                                if (pendingReviewCount > 0) {
                                    Text(
                                        text = stringResource(
                                            R.string.memory_v2_pending_review_count,
                                            pendingReviewCount,
                                        ),
                                        color = MaterialTheme.colorScheme.primary,
                                        style = MaterialTheme.typography.labelMedium,
                                    )
                                }
                            }
                        },
                        headlineContent = { Text(stringResource(R.string.assistant_page_tab_memory)) },
                        trailingContent = { Icon(HugeIcons.ArrowRight01, null) },
                    )
                    item(
                        onClick = { navController.navigate(Screen.AssistantRequest(id)) },
                        leadingContent = { Icon(HugeIcons.Code, null) },
                        supportingContent = { Text(stringResource(R.string.assistant_detail_request_desc)) },
                        headlineContent = { Text(stringResource(R.string.assistant_page_tab_request)) },
                        trailingContent = { Icon(HugeIcons.ArrowRight01, null) },
                    )
                    item(
                        onClick = { navController.navigate(Screen.AssistantMcp(id)) },
                        leadingContent = { Icon(HugeIcons.Wrench01, null) },
                        supportingContent = { Text(stringResource(R.string.assistant_detail_mcp_desc)) },
                        headlineContent = { Text(stringResource(R.string.assistant_page_tab_mcp)) },
                        trailingContent = { Icon(HugeIcons.ArrowRight01, null) },
                    )
                    item(
                        onClick = { navController.navigate(Screen.AssistantLocalTool(id)) },
                        leadingContent = { Icon(HugeIcons.BookOpen01, null) },
                        supportingContent = { Text(stringResource(R.string.assistant_detail_local_tools_desc)) },
                        headlineContent = { Text(stringResource(R.string.assistant_page_tab_local_tools)) },
                        trailingContent = { Icon(HugeIcons.ArrowRight01, null) },
                    )
                    item(
                        onClick = { showPetSettings = true },
                        leadingContent = { Icon(HugeIcons.Puzzle, null) },
                        supportingContent = {
                            Text(
                                if (assistant.privilegedConversationId == null) {
                                    "请先配置第二用户会话"
                                } else if (assistant.petEnabled) {
                                    "已启用 · ${assistant.petPackageId ?: "应用占位图"}"
                                } else {
                                    "导入 Codex Pet，并绑定第二用户短会话"
                                },
                            )
                        },
                        headlineContent = { Text("第二用户桌宠") },
                        trailingContent = { Icon(HugeIcons.ArrowRight01, null) },
                    )
                }
            }
        }
    }
    if (showPetSettings) {
        val authoritativePet = settings.petOverlaySelection?.takeIf { selection ->
            selection.ownerAssistantId == assistant.id &&
                selection.privilegedConversationId == assistant.privilegedConversationId
        }
        val petDraft = authoritativePet?.let { selection ->
            assistant.copy(
                petEnabled = selection.enabled,
                petPackageId = selection.packageId,
                petScale = selection.scale,
                petAnimationFps = selection.animationFps,
                petHeadBoundary = selection.headBoundary,
                petBodyBoundary = selection.bodyBoundary,
                petIdlePoolEnabled = selection.idlePoolEnabled,
            )
        } ?: assistant
        PetSettingsDialog(
            assistant = petDraft,
            onDismiss = { showPetSettings = false },
            onUpdate = { next, afterUpdate ->
                vm.updatePetSettingsAfter(
                    draft = next,
                    afterUpdate = afterUpdate,
                )
            },
        )
    }
}

@Composable
private fun AssistantHeader(
    assistant: Assistant,
    modifier: Modifier = Modifier
) {
    Column(
        modifier = modifier.fillMaxWidth(),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        UIAvatar(
            value = assistant.avatar,
            name = assistant.name.ifBlank { stringResource(R.string.assistant_page_default_assistant) },
            onUpdate = null,
            modifier = Modifier
                .size(100.dp)
                .heroAnimation("assistant_${assistant.id}")
        )

        Text(
            text = assistant.name.ifBlank { stringResource(R.string.assistant_page_default_assistant) },
            style = MaterialTheme.typography.headlineSmall,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis
        )

        if (assistant.systemPrompt.isNotBlank()) {
            Text(
                text = assistant.systemPrompt.take(100) + if (assistant.systemPrompt.length > 100) "..." else "",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                maxLines = 2,
                overflow = TextOverflow.Ellipsis
            )
        }
    }
}
