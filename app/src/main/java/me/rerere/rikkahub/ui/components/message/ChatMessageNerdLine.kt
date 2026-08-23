package me.rerere.rikkahub.ui.components.message

import androidx.annotation.VisibleForTesting
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.navigationBarsPadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.Icon
import androidx.compose.material3.LocalContentColor
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.ProvideTextStyle
import androidx.compose.material3.SheetValue
import androidx.compose.material3.Text
import androidx.compose.material3.rememberBottomSheetState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.CompositionLocalProvider
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.unit.dp
import kotlinx.datetime.toJavaLocalDateTime
import me.rerere.ai.core.currentCachedTokens
import me.rerere.ai.core.currentFreshPromptTokens
import me.rerere.ai.core.currentPromptTokens
import me.rerere.ai.ui.UIMessage
import me.rerere.hugeicons.HugeIcons
import me.rerere.hugeicons.stroke.Clock02
import me.rerere.hugeicons.stroke.CoinsDollar
import me.rerere.hugeicons.stroke.Download04
import me.rerere.hugeicons.stroke.Upload02
import me.rerere.hugeicons.stroke.Zap
import me.rerere.rikkahub.R
import me.rerere.rikkahub.diagnostics.agenttiming.AgentTimingTraceSnapshot
import me.rerere.rikkahub.ui.context.LocalSettings
import me.rerere.rikkahub.utils.formatNumber
import me.rerere.rikkahub.utils.toFixed
import java.time.Duration

/**
 * 显示消息的技术统计信息（如 token 使用量）
 */
@Composable
fun ChatMessageNerdLine(
    message: UIMessage,
    agentTiming: AgentTimingTraceSnapshot? = null,
    modifier: Modifier = Modifier,
    color: Color = MaterialTheme.colorScheme.secondary.copy(alpha = 0.5f),
) {
    val settings = LocalSettings.current.displaySetting

    ProvideTextStyle(MaterialTheme.typography.labelSmall.copy(color = color)) {
        CompositionLocalProvider(LocalContentColor provides color) {
            Column(
                verticalArrangement = Arrangement.spacedBy(2.dp),
                modifier = modifier.padding(horizontal = 4.dp),
            ) {
                val usage = message.usage
                if (settings.showTokenUsage && usage != null) {
                    // Input tokens
                    StatsItem(
                        icon = {
                            Icon(
                                imageVector = HugeIcons.Upload02,
                                contentDescription = "Input",
                                tint = color,
                                modifier = Modifier.size(12.dp)
                            )
                        },
                        content = {
                            val latestInput = usage.currentPromptTokens
                            Text(
                                text = stringResource(
                                    R.string.chat_token_usage_latest,
                                    latestInput.formatNumber(),
                                    usage.currentFreshPromptTokens.formatNumber(),
                                    formatCacheRate(latestInput, usage.currentCachedTokens),
                                ),
                            )
                        }
                    )
                    // Output tokens
                    StatsItem(
                        icon = {
                            Icon(
                                imageVector = HugeIcons.Download04,
                                contentDescription = "Output",
                                modifier = Modifier.size(12.dp)
                            )
                        },
                        content = {
                            Text(
                                text = stringResource(
                                    R.string.chat_token_usage_turn,
                                    usage.promptTokens.formatNumber(),
                                    usage.providerCallCount.coerceAtLeast(1),
                                    usage.completionTokens.formatNumber(),
                                ),
                            )
                        }
                    )
                    // Cost (USD) — shown when the provider reports it (e.g. OpenRouter usage.cost)
                    FlowRow(
                        horizontalArrangement = Arrangement.spacedBy(4.dp),
                        itemVerticalAlignment = Alignment.CenterVertically,
                    ) {
                        val cost = usage.cost
                        if (cost != null && cost > 0.0) {
                            StatsItem(
                                icon = {
                                    Icon(
                                        imageVector = HugeIcons.CoinsDollar,
                                        contentDescription = "Cost",
                                        tint = color,
                                        modifier = Modifier.size(12.dp)
                                    )
                                },
                                content = {
                                    Text(text = formatCost(cost))
                                }
                            )
                        }
                        // TPS
                        if (message.finishedAt != null) {
                            val duration = Duration.between(
                                message.createdAt.toJavaLocalDateTime(),
                                message.finishedAt!!.toJavaLocalDateTime()
                            )
                            val tps = usage.completionTokens.toFloat() / duration.toMillis() * 1000
                            val seconds = (duration.toMillis() / 1000f).toFixed(1)
                            StatsItem(
                                icon = {
                                    Icon(
                                        imageVector = HugeIcons.Zap,
                                        contentDescription = "Speed",
                                        modifier = Modifier.size(12.dp)
                                    )
                                },
                                content = {
                                    Text(text = "${tps.toFixed(1)} tok/s")
                                }
                            )

                            StatsItem(
                                icon = {
                                    Icon(
                                        imageVector = HugeIcons.Clock02,
                                        contentDescription = "Duration",
                                        modifier = Modifier.size(12.dp)
                                    )
                                },
                                content = {
                                    Text(text = "${seconds}s")
                                }
                            )
                        }
                    }
                }
                if (settings.showAgentTiming && agentTiming != null) {
                    AgentTimingSummaryLine(trace = agentTiming)
                }
            }
        }
    }
}

@Composable
private fun AgentTimingSummaryLine(trace: AgentTimingTraceSnapshot) {
    val summary = remember(trace) { buildAgentTimingSummary(trace) }
    var showDetails by remember(trace.traceSequence) { mutableStateOf(false) }
    val responseLabel = when (summary.firstResponseKind) {
        AgentTimingFirstResponseKind.FIRST_PROGRESS -> stringResource(R.string.agent_timing_first_progress)
        AgentTimingFirstResponseKind.FULL_RESPONSE -> stringResource(R.string.agent_timing_full_response)
    }
    val text = stringResource(
        R.string.agent_timing_summary,
        formatAgentTimingDuration(summary.totalNs),
        formatAgentTimingDuration(summary.excludingHumanWaitNs),
        responseLabel,
        formatAgentTimingDuration(summary.firstResponseNs),
        summary.roundCount,
        summary.toolCount,
    )
    StatsItem(
        icon = {
            Icon(
                imageVector = HugeIcons.Clock02,
                contentDescription = null,
                modifier = Modifier.size(12.dp),
            )
        },
        content = {
            Text(
                text = text,
                modifier = Modifier.clickable(onClick = { showDetails = true }),
            )
        },
    )

    if (showDetails) {
        AgentTimingDetailSheet(
            trace = trace,
            onDismiss = { showDetails = false },
        )
    }
}

@Composable
internal fun AgentTimingToolExtra(presentation: AgentToolTimingPresentation) {
    val parts = buildList {
        presentation.executionNs?.let {
            add(stringResource(R.string.agent_timing_tool_execution, formatAgentTimingDuration(it)))
        }
        presentation.postProcessingNs?.let {
            add(stringResource(R.string.agent_timing_tool_post_processing, formatAgentTimingDuration(it)))
        }
        presentation.batchReadyNs?.let {
            add(stringResource(R.string.agent_timing_tool_batch_wait, formatAgentTimingDuration(it)))
        }
        presentation.handoffNs?.let {
            add(stringResource(R.string.agent_timing_tool_handoff, formatAgentTimingDuration(it)))
        }
        presentation.nextResponseNs?.let {
            add(stringResource(R.string.agent_timing_tool_next_response, formatAgentTimingDuration(it)))
        }
    }
    if (parts.isEmpty()) return
    Text(
        text = parts.joinToString(" \u00b7 "),
        style = MaterialTheme.typography.labelSmall,
        color = if (presentation.hasLongMetric) {
            MaterialTheme.colorScheme.tertiary
        } else {
            MaterialTheme.colorScheme.onSurfaceVariant
        },
        maxLines = 2,
    )
}

@Composable
private fun AgentTimingDetailSheet(
    trace: AgentTimingTraceSnapshot,
    onDismiss: () -> Unit,
) {
    val detail = remember(trace) { buildAgentTimingDetail(trace) }
    ModalBottomSheet(
        sheetState = rememberBottomSheetState(
            initialValue = SheetValue.Hidden,
            enabledValues = setOf(SheetValue.Hidden, SheetValue.Expanded),
        ),
        onDismissRequest = onDismiss,
    ) {
        LazyColumn(
            modifier = Modifier
                .fillMaxWidth()
                .navigationBarsPadding(),
            contentPadding = PaddingValues(horizontal = 20.dp, vertical = 8.dp),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            item(key = "title") {
                Text(
                    text = stringResource(R.string.agent_timing_details_title),
                    style = MaterialTheme.typography.titleLarge,
                )
            }
            items(detail.overview, key = { "overview-${it.kind}" }) { section ->
                AgentTimingSection(section)
            }
            item(key = "response-layers") {
                AgentTimingResponseLayers(detail.responseLayers)
            }
            items(
                detail.rounds,
                key = { "round-${it.snapshotOrdinal}-${it.logicalRoundNumber}-${it.attemptIndex}" },
            ) { round ->
                Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                    Text(
                        text = stringResource(
                            R.string.agent_timing_round_title,
                            round.logicalRoundNumber,
                            round.attemptIndex + 1,
                        ),
                        style = MaterialTheme.typography.titleMedium,
                    )
                    round.sections.forEach { section ->
                        AgentTimingSection(section)
                        if (section.kind == AgentTimingSectionKind.PROVIDER) {
                            round.providerStats?.let { stats ->
                                AgentTimingValueRow(
                                    label = stringResource(R.string.agent_timing_metric_provider_output_tokens),
                                    value = stats.outputTokens.formatNumber(),
                                    isLong = false,
                                )
                                stats.decodeTps?.let { tps ->
                                    AgentTimingValueRow(
                                        label = stringResource(R.string.agent_timing_metric_provider_decode_tps),
                                        value = "${tps.toFixed(1)} tok/s",
                                        isLong = false,
                                    )
                                }
                                stats.providerVisible?.let { stream ->
                                    AgentTimingValueRow(
                                        label = stringResource(R.string.agent_timing_metric_provider_visible_stream),
                                        value = streamThroughputValue(stream),
                                        isLong = false,
                                    )
                                }
                                stats.sessionVisible?.let { stream ->
                                    AgentTimingValueRow(
                                        label = stringResource(R.string.agent_timing_metric_session_visible_stream),
                                        value = streamThroughputValue(stream),
                                        isLong = false,
                                    )
                                }
                                stats.uiVisible?.let { stream ->
                                    AgentTimingValueRow(
                                        label = stringResource(R.string.agent_timing_metric_ui_visible_stream),
                                        value = streamThroughputValue(stream),
                                        isLong = false,
                                    )
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

private fun streamThroughputValue(stats: AgentTimingStreamStatsPresentation): String {
    val rate = stats.tps?.let { "~${it.toFixed(1)} tok/s" } ?: "~? tok/s"
    return "$rate · ${stats.sampleCount} samples"
}

@Composable
private fun AgentTimingResponseLayers(layers: AgentTimingResponseLayersPresentation) {
    Column(verticalArrangement = Arrangement.spacedBy(4.dp)) {
        Text(
            text = stringResource(R.string.agent_timing_section_response_layers),
            style = MaterialTheme.typography.labelLarge,
            color = MaterialTheme.colorScheme.primary,
        )
        layers.sessionContentFromDispatchNs?.let { duration ->
            AgentTimingValueRow(
                label = stringResource(R.string.agent_timing_metric_session_content),
                value = formatAgentTimingDuration(duration),
                isLong = duration > AGENT_TIMING_LONG_STAGE_NS,
            )
        }
        val drawValue = when (layers.visibleDrawState) {
            AgentTimingVisibleDrawState.OBSERVED ->
                formatAgentTimingDuration(layers.visibleDrawFromSessionContentNs)
            AgentTimingVisibleDrawState.NOT_OBSERVED ->
                stringResource(R.string.agent_timing_visible_not_observed)
            AgentTimingVisibleDrawState.PENDING ->
                stringResource(R.string.agent_timing_visible_pending)
        }
        AgentTimingValueRow(
            label = stringResource(R.string.agent_timing_metric_first_visible_draw),
            value = drawValue,
            isLong = layers.visibleDrawFromSessionContentNs?.let {
                it > AGENT_TIMING_LONG_STAGE_NS
            } == true,
        )
    }
}

@Composable
private fun AgentTimingValueRow(
    label: String,
    value: String,
    isLong: Boolean,
) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.spacedBy(12.dp),
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Text(
            text = label,
            style = MaterialTheme.typography.bodySmall,
            modifier = Modifier.weight(1f),
        )
        if (isLong) {
            Text(
                text = stringResource(R.string.agent_timing_long_stage),
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.tertiary,
            )
        }
        Text(
            text = value,
            style = MaterialTheme.typography.labelMedium,
            color = if (isLong) {
                MaterialTheme.colorScheme.tertiary
            } else {
                MaterialTheme.colorScheme.onSurfaceVariant
            },
        )
    }
}

@Composable
private fun AgentTimingSection(section: AgentTimingSectionPresentation) {
    Column(verticalArrangement = Arrangement.spacedBy(4.dp)) {
        Text(
            text = section.kind.label(),
            style = MaterialTheme.typography.labelLarge,
            color = MaterialTheme.colorScheme.primary,
        )
        section.metrics.forEach { metric ->
            AgentTimingValueRow(
                label = metric.label(),
                value = formatAgentTimingDuration(metric.durationNs),
                isLong = metric.isLong,
            )
        }
    }
}

@Composable
private fun AgentTimingSectionKind.label(): String = stringResource(
    when (this) {
        AgentTimingSectionKind.OVERVIEW -> R.string.agent_timing_section_overview
        AgentTimingSectionKind.CONTEXT -> R.string.agent_timing_section_context
        AgentTimingSectionKind.DIAGNOSTICS -> R.string.agent_timing_section_diagnostics
        AgentTimingSectionKind.PROVIDER -> R.string.agent_timing_section_provider
        AgentTimingSectionKind.TOOLS -> R.string.agent_timing_section_tools
        AgentTimingSectionKind.HANDOFF -> R.string.agent_timing_section_handoff
        AgentTimingSectionKind.APPROVAL -> R.string.agent_timing_section_approval
    }
)

@Composable
private fun AgentTimingMetricPresentation.label(): String {
    val resource = when (kind) {
        AgentTimingMetricKind.ADMISSION -> R.string.agent_timing_metric_admission
        AgentTimingMetricKind.RUN_PREFLIGHT -> R.string.agent_timing_metric_run_preflight
        AgentTimingMetricKind.MEMORY_RETRIEVAL -> R.string.agent_timing_metric_memory_retrieval
        AgentTimingMetricKind.AUTO_CONTEXT -> R.string.agent_timing_metric_auto_context
        AgentTimingMetricKind.TOOL_SURFACE -> R.string.agent_timing_metric_tool_surface
        AgentTimingMetricKind.MCP_DISCOVERY -> R.string.agent_timing_metric_mcp_discovery
        AgentTimingMetricKind.CONTEXT -> R.string.agent_timing_metric_context
        AgentTimingMetricKind.RECENT_CHATS -> R.string.agent_timing_metric_recent_chats
        AgentTimingMetricKind.MEMORY_PROMPT -> R.string.agent_timing_metric_memory_prompt
        AgentTimingMetricKind.TOOL_PROMPT -> R.string.agent_timing_metric_tool_prompt
        AgentTimingMetricKind.SYSTEM_PROMPT -> R.string.agent_timing_metric_system_prompt
        AgentTimingMetricKind.CONTEXT_GATE_INITIAL -> R.string.agent_timing_metric_context_gate_initial
        AgentTimingMetricKind.INPUT_TRANSFORM -> R.string.agent_timing_metric_input_transform
        AgentTimingMetricKind.CONTEXT_GATE_FINAL -> R.string.agent_timing_metric_context_gate_final
        AgentTimingMetricKind.CONTEXT_COMPRESSION -> R.string.agent_timing_metric_context_compression
        AgentTimingMetricKind.TOKEN_COUNT -> R.string.agent_timing_metric_token_count
        AgentTimingMetricKind.REQUEST_BUILD -> R.string.agent_timing_metric_request_build
        AgentTimingMetricKind.DIAGNOSTICS -> R.string.agent_timing_metric_diagnostics
        AgentTimingMetricKind.REQUEST_BREAKDOWN_BUILD -> R.string.agent_timing_metric_request_breakdown_build
        AgentTimingMetricKind.REQUEST_BREAKDOWN_WRITE -> R.string.agent_timing_metric_request_breakdown_write
        AgentTimingMetricKind.MEMORY_LAST_ACCESS -> R.string.agent_timing_metric_memory_last_access
        AgentTimingMetricKind.PROVIDER_PREPARE -> R.string.agent_timing_metric_provider_prepare
        AgentTimingMetricKind.FIRST_PROGRESS -> R.string.agent_timing_first_progress
        AgentTimingMetricKind.FIRST_TEXT -> R.string.agent_timing_first_text
        AgentTimingMetricKind.FULL_RESPONSE -> R.string.agent_timing_full_response
        AgentTimingMetricKind.PROVIDER_TOTAL -> R.string.agent_timing_metric_provider_total
        AgentTimingMetricKind.TOOL_BATCH -> R.string.agent_timing_metric_tool_batch
        AgentTimingMetricKind.TOOL_EXECUTION -> R.string.agent_timing_metric_tool_execution
        AgentTimingMetricKind.TOOL_POST_PROCESSING -> R.string.agent_timing_metric_tool_post_processing
        AgentTimingMetricKind.TOOL_BATCH_WAIT -> R.string.agent_timing_metric_tool_batch_wait
        AgentTimingMetricKind.HANDOFF -> R.string.agent_timing_metric_handoff
        AgentTimingMetricKind.HUMAN_WAIT -> R.string.agent_timing_metric_human_wait
        AgentTimingMetricKind.APPROVAL_RESOLUTION -> R.string.agent_timing_metric_approval_resolution
        AgentTimingMetricKind.FINAL_SAVE -> R.string.agent_timing_metric_final_save
        AgentTimingMetricKind.GENERATION_DONE_NOTIFY -> R.string.agent_timing_metric_generation_done_notify
        AgentTimingMetricKind.POST_PROVIDER_PROCESSING -> R.string.agent_timing_metric_post_provider_processing
        AgentTimingMetricKind.RUN_COMPLETION_DRAIN -> R.string.agent_timing_metric_run_completion_drain
        AgentTimingMetricKind.RUNTIME_FINALIZATION -> R.string.agent_timing_metric_runtime_finalization
    }
    return if (ordinal != null && (
            kind == AgentTimingMetricKind.TOOL_EXECUTION ||
                kind == AgentTimingMetricKind.TOOL_POST_PROCESSING ||
                kind == AgentTimingMetricKind.TOOL_BATCH_WAIT
            )
    ) {
        stringResource(R.string.agent_timing_metric_tool_numbered, ordinal + 1, stringResource(resource))
    } else {
        stringResource(resource)
    }
}

// Generation cost is often a tiny fraction of a cent, so a fixed decimal count would show
// "$0.0000". Render up to 6 decimals and trim trailing zeros (e.g. "$0.0123", "$0.000045").
// A positive cost smaller than 1e-6 would round to zero at 6dp and read as "$0" (free), which
// is misleading; clamp those to a "<$0.000001" form so a real charge never displays as free.
@VisibleForTesting
internal fun formatCost(cost: Double): String {
    val rounded = java.math.BigDecimal(cost)
        .setScale(6, java.math.RoundingMode.HALF_UP)
    if (cost > 0.0 && rounded.signum() == 0) {
        return "<$0.000001"
    }
    val s = rounded.stripTrailingZeros().toPlainString()
    return "$" + s
}

@VisibleForTesting
internal fun formatCacheRate(promptTokens: Int, cachedTokens: Int): String {
    if (promptTokens <= 0) return "0%"
    val percent = cachedTokens.coerceIn(0, promptTokens).toDouble() * 100.0 / promptTokens
    return "${percent.toFixed(2)}%"
}

@Composable
fun StatsItem(
    icon: @Composable () -> Unit,
    content: @Composable () -> Unit
) {
    Row(
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(1.dp),
    ) {
        icon()
        content()
    }
}
