package me.rerere.rikkahub.ui.pages.memory

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.AssistChip
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import me.rerere.hugeicons.HugeIcons
import me.rerere.hugeicons.stroke.Moon02
import me.rerere.rikkahub.R
import me.rerere.rikkahub.memory.MemoryKind
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimState
import me.rerere.rikkahub.memory.dreaming.review.DreamClaimDetail
import me.rerere.rikkahub.memory.dreaming.review.DreamClaimMutationTarget
import me.rerere.rikkahub.memory.dreaming.review.DreamClaimSummary
import me.rerere.rikkahub.memory.dreaming.review.DreamCorrectionDraft
import me.rerere.rikkahub.memory.dreaming.review.DreamDerivedStatus
import me.rerere.rikkahub.memory.dreaming.review.DreamEvidenceSummary
import me.rerere.rikkahub.memory.dreaming.review.DreamEvidenceValidity
import me.rerere.rikkahub.memory.dreaming.review.DreamReviewProjection
import me.rerere.rikkahub.memory.dreaming.review.DreamSnapshotChange
import me.rerere.rikkahub.memory.dreaming.review.DreamSnapshotChangeType
import me.rerere.rikkahub.memory.dreaming.review.DreamSnapshotDiffResult
import me.rerere.rikkahub.memory.dreaming.review.DreamUsageMode
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotSection

@Composable
fun MemoryDreamTab(
    projection: DreamReviewProjection?,
    detailState: MemoryDreamDetailState,
    onOpenClaim: (DreamClaimMutationTarget) -> Unit,
    onCloseClaim: () -> Unit,
    onRevealEvidence: (DreamEvidenceSummary) -> Unit,
    onRejectClaim: (DreamClaimMutationTarget) -> Unit,
    onCorrectClaim: (DreamCorrectionDraft) -> Unit,
    onClearDerived: (me.rerere.rikkahub.memory.dreaming.review.DreamReviewFence) -> Unit,
) {
    var clearConfirmation by remember { mutableStateOf(false) }
    LazyColumn(
        contentPadding = PaddingValues(12.dp),
        verticalArrangement = Arrangement.spacedBy(10.dp),
    ) {
        item {
            DreamStatusCard(projection)
        }
        if (projection != null) {
            PAIR_PORTRAIT_SECTIONS.forEach { section ->
                val sectionClaims = projection.claims.filter {
                    it.section == section && it.state in CURRENT_PORTRAIT_STATES
                }
                item(key = "portrait_${section.name}") {
                    DreamPortraitCard(
                        section = section,
                        claims = sectionClaims,
                        projection = projection,
                        onOpenClaim = onOpenClaim,
                    )
                }
            }
            DreamSnapshotSection.entries.filterNot(PAIR_PORTRAIT_SECTIONS::contains).forEach { section ->
                val sectionClaims = projection.claims.filter { it.section == section }
                if (sectionClaims.isNotEmpty()) {
                    item(key = "section_${section.name}") {
                        Text(
                            text = section.title(),
                            style = MaterialTheme.typography.titleMedium,
                            fontWeight = FontWeight.SemiBold,
                        )
                    }
                    items(
                        items = sectionClaims,
                        key = { claim -> "${claim.claimId}:${claim.revision}" },
                    ) { claim ->
                        val target = remember(projection.fence, claim.claimId, claim.revision) {
                            DreamClaimMutationTarget(projection.fence, claim.claimId, claim.revision)
                        }
                        DreamClaimCard(claim = claim, onClick = { onOpenClaim(target) })
                    }
                }
            }
            item {
                DreamDiffCard(projection.snapshotDiff)
            }
            item {
                DreamRunsCard(projection)
            }
            item {
                OutlinedButton(
                    onClick = { clearConfirmation = true },
                    enabled = projection.claims.isNotEmpty() || projection.activeSnapshot != null,
                    modifier = Modifier.fillMaxWidth(),
                ) {
                    Text(stringResource(R.string.memory_dream_clear_derived))
                }
                Text(
                    text = stringResource(R.string.memory_dream_clear_derived_desc),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.padding(top = 6.dp),
                )
            }
        }
    }

    if (clearConfirmation && projection != null) {
        AlertDialog(
            onDismissRequest = { clearConfirmation = false },
            title = { Text(stringResource(R.string.memory_dream_clear_confirm_title)) },
            text = { Text(stringResource(R.string.memory_dream_clear_confirm_body)) },
            confirmButton = {
                TextButton(onClick = {
                    clearConfirmation = false
                    onClearDerived(projection.fence)
                }) { Text(stringResource(R.string.memory_dream_clear_confirm_action)) }
            },
            dismissButton = {
                TextButton(onClick = { clearConfirmation = false }) {
                    Text(stringResource(R.string.cancel))
                }
            },
        )
    }

    when (detailState) {
        MemoryDreamDetailState.Closed -> Unit
        is MemoryDreamDetailState.Loading -> DreamClaimSheet(
            detail = null,
            loading = true,
            failedReason = null,
            revealed = emptyMap(),
            revealing = emptySet(),
            onDismiss = onCloseClaim,
            onRevealEvidence = onRevealEvidence,
            onRejectClaim = onRejectClaim,
            onCorrectClaim = onCorrectClaim,
        )
        is MemoryDreamDetailState.Failed -> DreamClaimSheet(
            detail = null,
            loading = false,
            failedReason = detailState.reasonCode,
            revealed = emptyMap(),
            revealing = emptySet(),
            onDismiss = onCloseClaim,
            onRevealEvidence = onRevealEvidence,
            onRejectClaim = onRejectClaim,
            onCorrectClaim = onCorrectClaim,
        )
        is MemoryDreamDetailState.Ready -> DreamClaimSheet(
            detail = detailState.detail,
            loading = false,
            failedReason = null,
            revealed = detailState.revealedEvidence,
            revealing = detailState.revealingEvidence,
            onDismiss = onCloseClaim,
            onRevealEvidence = onRevealEvidence,
            onRejectClaim = onRejectClaim,
            onCorrectClaim = onCorrectClaim,
        )
    }
}

@Composable
private fun DreamPortraitCard(
    section: DreamSnapshotSection,
    claims: List<DreamClaimSummary>,
    projection: DreamReviewProjection,
    onOpenClaim: (DreamClaimMutationTarget) -> Unit,
) {
    Card(modifier = Modifier.fillMaxWidth()) {
        Column(
            modifier = Modifier.padding(14.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            Text(
                text = section.title(),
                style = MaterialTheme.typography.titleMedium,
                fontWeight = FontWeight.SemiBold,
            )
            if (claims.isEmpty()) {
                Text(
                    text = stringResource(R.string.memory_dream_portrait_empty),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            } else {
                claims.forEach { claim ->
                    val target = remember(projection.fence, claim.claimId, claim.revision) {
                        DreamClaimMutationTarget(projection.fence, claim.claimId, claim.revision)
                    }
                    DreamClaimCard(claim = claim, onClick = { onOpenClaim(target) })
                }
            }
        }
    }
}

@Composable
private fun DreamStatusCard(projection: DreamReviewProjection?) {
    Card(modifier = Modifier.fillMaxWidth()) {
        Column(
            modifier = Modifier.padding(16.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            Row(
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.spacedBy(10.dp),
            ) {
                Icon(
                    imageVector = HugeIcons.Moon02,
                    contentDescription = null,
                    modifier = Modifier.size(28.dp),
                )
                Column(modifier = Modifier.weight(1f)) {
                    Text(
                        text = stringResource(R.string.memory_dream_summary_title),
                        style = MaterialTheme.typography.titleLarge,
                    )
                    Text(
                        text = stringResource(R.string.memory_dream_summary_desc),
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                }
            }
            if (projection == null) {
                Row(
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(10.dp),
                ) {
                    CircularProgressIndicator(modifier = Modifier.size(20.dp), strokeWidth = 2.dp)
                    Text(stringResource(R.string.memory_dream_loading))
                }
            } else {
                FlowRow(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    AssistChip(onClick = {}, label = { Text(projection.derivedStatus.title()) })
                    AssistChip(onClick = {}, label = { Text(projection.usageMode.title()) })
                }
                Text(
                    text = stringResource(
                        R.string.memory_dream_experience_summary,
                        projection.fence.expectedMemoryEpoch,
                        projection.fence.expectedLastAppliedMemoryEpoch,
                        projection.fence.expectedDreamRevision,
                        projection.pendingExperienceCount,
                        projection.experienceDebt,
                    ),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
                projection.activeSnapshot?.let { snapshot ->
                    Text(
                        text = stringResource(
                            R.string.memory_dream_snapshot_summary,
                            snapshot.claimCount,
                            snapshot.estimatedTokens,
                        ),
                        style = MaterialTheme.typography.bodySmall,
                    )
                }
            }
        }
    }
}

@Composable
private fun DreamClaimCard(claim: DreamClaimSummary, onClick: () -> Unit) {
    Card(onClick = onClick, modifier = Modifier.fillMaxWidth()) {
        Column(
            modifier = Modifier.padding(14.dp),
            verticalArrangement = Arrangement.spacedBy(6.dp),
        ) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(
                    text = claim.title,
                    style = MaterialTheme.typography.titleMedium,
                    modifier = Modifier.weight(1f),
                    maxLines = 2,
                    overflow = TextOverflow.Ellipsis,
                )
                Text(
                    text = stringResource(R.string.memory_dream_confidence, claim.confidencePermille / 10),
                    style = MaterialTheme.typography.labelMedium,
                )
            }
            Text(
                text = claim.statement,
                style = MaterialTheme.typography.bodyMedium,
                maxLines = 4,
                overflow = TextOverflow.Ellipsis,
            )
            Text(
                text = stringResource(
                    R.string.memory_dream_claim_meta,
                    claim.state.name,
                    claim.temporalState.name,
                    claim.evidenceCount,
                ),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
    }
}

@Composable
private fun DreamDiffCard(diff: DreamSnapshotDiffResult) {
    Card(modifier = Modifier.fillMaxWidth()) {
        Column(
            modifier = Modifier.padding(14.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            Text(stringResource(R.string.memory_dream_changes_title), style = MaterialTheme.typography.titleMedium)
            when (diff) {
                is DreamSnapshotDiffResult.Unavailable -> Text(
                    text = stringResource(R.string.memory_dream_changes_unavailable, diff.failure.name),
                    color = MaterialTheme.colorScheme.error,
                    style = MaterialTheme.typography.bodySmall,
                )
                is DreamSnapshotDiffResult.Available -> if (diff.changes.isEmpty()) {
                    Text(
                        text = stringResource(R.string.memory_dream_changes_empty),
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                } else {
                    diff.changes.take(MAX_VISIBLE_DIFF_ROWS).forEachIndexed { index, change ->
                        if (index > 0) HorizontalDivider()
                        DreamChangeRow(change)
                    }
                    if (diff.changes.size > MAX_VISIBLE_DIFF_ROWS) {
                        Text(
                            text = stringResource(
                                R.string.memory_dream_more_items,
                                diff.changes.size - MAX_VISIBLE_DIFF_ROWS,
                            ),
                            style = MaterialTheme.typography.bodySmall,
                        )
                    }
                }
            }
        }
    }
}

@Composable
private fun DreamChangeRow(change: DreamSnapshotChange) {
    Column(verticalArrangement = Arrangement.spacedBy(2.dp)) {
        Text(
            text = "${change.type.title()} · ${change.title}",
            style = MaterialTheme.typography.bodyMedium,
        )
        val flags = buildList {
            if (change.confidenceChanged) add(stringResource(R.string.memory_dream_confidence_changed))
            if (change.temporalChanged) add(stringResource(R.string.memory_dream_time_changed))
        }
        if (flags.isNotEmpty()) {
            Text(
                text = flags.joinToString(" · "),
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
    }
}

@Composable
private fun DreamRunsCard(projection: DreamReviewProjection) {
    Card(modifier = Modifier.fillMaxWidth()) {
        Column(
            modifier = Modifier.padding(14.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            Text(stringResource(R.string.memory_dream_runs_title), style = MaterialTheme.typography.titleMedium)
            if (projection.recentRuns.isEmpty()) {
                Text(
                    text = stringResource(R.string.memory_dream_runs_empty),
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            } else {
                projection.recentRuns.take(MAX_VISIBLE_RUNS).forEach { run ->
                    Text(
                        text = stringResource(
                            R.string.memory_dream_run_row,
                            run.statusCode,
                            run.inputTokens?.toString() ?: stringResource(R.string.memory_dream_unmeasured),
                            run.outputTokens?.toString() ?: stringResource(R.string.memory_dream_unmeasured),
                        ),
                        style = MaterialTheme.typography.bodySmall,
                    )
                }
                Text(
                    text = stringResource(R.string.memory_dream_cost_unavailable),
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                )
            }
        }
    }
}

@Composable
private fun DreamClaimSheet(
    detail: DreamClaimDetail?,
    loading: Boolean,
    failedReason: String?,
    revealed: Map<me.rerere.rikkahub.memory.dreaming.review.DreamEvidenceReference,
        me.rerere.rikkahub.memory.dreaming.review.DreamEvidenceExcerpt>,
    revealing: Set<me.rerere.rikkahub.memory.dreaming.review.DreamEvidenceReference>,
    onDismiss: () -> Unit,
    onRevealEvidence: (DreamEvidenceSummary) -> Unit,
    onRejectClaim: (DreamClaimMutationTarget) -> Unit,
    onCorrectClaim: (DreamCorrectionDraft) -> Unit,
) {
    var rejectConfirmation by remember(detail?.target) { mutableStateOf(false) }
    var correctionOpen by remember(detail?.target) { mutableStateOf(false) }
    ModalBottomSheet(onDismissRequest = onDismiss) {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 18.dp, vertical = 8.dp),
            verticalArrangement = Arrangement.spacedBy(12.dp),
        ) {
            Text(stringResource(R.string.memory_dream_claim_detail_title), style = MaterialTheme.typography.titleLarge)
            when {
                loading -> CircularProgressIndicator(modifier = Modifier.align(Alignment.CenterHorizontally))
                failedReason != null -> Text(
                    text = stringResource(R.string.memory_dream_claim_load_failed, failedReason),
                    color = MaterialTheme.colorScheme.error,
                )
                detail != null -> {
                    Text(detail.summary.title, style = MaterialTheme.typography.titleMedium)
                    Text(detail.summary.statement)
                    Text(
                        text = stringResource(
                            R.string.memory_dream_claim_detail_meta,
                            detail.summary.state.name,
                            detail.storageClass.name,
                            detail.epistemicType.name,
                            detail.summary.confidencePermille / 10,
                        ),
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant,
                    )
                    HorizontalDivider()
                    Text(stringResource(R.string.memory_dream_evidence_title), style = MaterialTheme.typography.titleMedium)
                    if (detail.evidence.isEmpty()) {
                        Text(stringResource(R.string.memory_dream_evidence_empty))
                    } else {
                        detail.evidence.forEach { evidence ->
                            DreamEvidenceRow(
                                evidence = evidence,
                                excerpt = revealed[evidence.reference]?.text,
                                revealing = evidence.reference in revealing,
                                onReveal = { onRevealEvidence(evidence) },
                            )
                        }
                    }
                    HorizontalDivider()
                    Text(stringResource(R.string.memory_dream_versions_title), style = MaterialTheme.typography.titleMedium)
                    detail.versions.takeLast(MAX_VISIBLE_VERSIONS).asReversed().forEach { version ->
                        Text(
                            text = stringResource(
                                R.string.memory_dream_version_row,
                                version.revision,
                                version.state.name,
                                version.reasonCode,
                            ),
                            style = MaterialTheme.typography.bodySmall,
                        )
                    }
                    Row(
                        modifier = Modifier.fillMaxWidth(),
                        horizontalArrangement = Arrangement.spacedBy(8.dp, Alignment.End),
                    ) {
                        OutlinedButton(onClick = { rejectConfirmation = true }) {
                            Text(stringResource(R.string.memory_dream_reject))
                        }
                        Button(onClick = { correctionOpen = true }) {
                            Text(stringResource(R.string.memory_dream_correct))
                        }
                    }
                }
            }
            Spacer(Modifier.height(24.dp))
        }
    }
    if (rejectConfirmation && detail != null) {
        AlertDialog(
            onDismissRequest = { rejectConfirmation = false },
            title = { Text(stringResource(R.string.memory_dream_reject_confirm_title)) },
            text = { Text(stringResource(R.string.memory_dream_reject_confirm_body)) },
            confirmButton = {
                TextButton(onClick = {
                    rejectConfirmation = false
                    onRejectClaim(detail.target)
                }) { Text(stringResource(R.string.memory_dream_reject)) }
            },
            dismissButton = {
                TextButton(onClick = { rejectConfirmation = false }) {
                    Text(stringResource(R.string.cancel))
                }
            },
        )
    }
    if (correctionOpen && detail != null) {
        DreamCorrectionDialog(
            detail = detail,
            onDismiss = { correctionOpen = false },
            onSubmit = { draft ->
                correctionOpen = false
                onCorrectClaim(draft)
            },
        )
    }
}

@Composable
private fun DreamEvidenceRow(
    evidence: DreamEvidenceSummary,
    excerpt: String?,
    revealing: Boolean,
    onReveal: () -> Unit,
) {
    Column(
        modifier = Modifier.fillMaxWidth(),
        verticalArrangement = Arrangement.spacedBy(4.dp),
    ) {
        Text(
            text = stringResource(
                R.string.memory_dream_evidence_meta,
                evidence.reference.supportType.name,
                evidence.validity.name,
                evidence.sourceKind ?: stringResource(R.string.memory_dream_unknown),
            ),
            style = MaterialTheme.typography.bodySmall,
        )
        excerpt?.let {
            Text(
                text = it,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
            )
        }
        if (evidence.excerptAvailable && evidence.validity == DreamEvidenceValidity.VALID && excerpt == null) {
            TextButton(onClick = onReveal, enabled = !revealing) {
                if (revealing) {
                    CircularProgressIndicator(modifier = Modifier.size(16.dp), strokeWidth = 2.dp)
                } else {
                    Text(stringResource(R.string.memory_dream_reveal_sensitive_evidence))
                }
            }
        }
    }
}

@Composable
private fun DreamCorrectionDialog(
    detail: DreamClaimDetail,
    onDismiss: () -> Unit,
    onSubmit: (DreamCorrectionDraft) -> Unit,
) {
    var title by remember(detail.target) { mutableStateOf(detail.summary.title) }
    var content by remember(detail.target) { mutableStateOf(detail.summary.statement) }
    var tags by remember(detail.target) { mutableStateOf("") }
    var expiry by remember(detail.target) { mutableStateOf("") }
    val parsedExpiry = expiry.takeIf(String::isNotBlank)?.toLongOrNull()
    val valid = content.isNotBlank() && (expiry.isBlank() || parsedExpiry != null)
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text(stringResource(R.string.memory_dream_correct_title)) },
        text = {
            Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                Text(stringResource(R.string.memory_dream_correct_desc), style = MaterialTheme.typography.bodySmall)
                OutlinedTextField(
                    value = title,
                    onValueChange = { title = it.take(4_096) },
                    label = { Text(stringResource(R.string.memory_dream_correct_title_field)) },
                    modifier = Modifier.fillMaxWidth(),
                )
                OutlinedTextField(
                    value = content,
                    onValueChange = { content = it.take(64_000) },
                    label = { Text(stringResource(R.string.memory_dream_correct_content_field)) },
                    minLines = 3,
                    modifier = Modifier.fillMaxWidth(),
                )
                OutlinedTextField(
                    value = tags,
                    onValueChange = { tags = it.take(4_096) },
                    label = { Text(stringResource(R.string.memory_dream_correct_tags_field)) },
                    supportingText = { Text(stringResource(R.string.memory_dream_correct_tags_hint)) },
                    modifier = Modifier.fillMaxWidth(),
                )
                OutlinedTextField(
                    value = expiry,
                    onValueChange = { expiry = it.filter(Char::isDigit).take(19) },
                    label = { Text(stringResource(R.string.memory_dream_correct_expiry_field)) },
                    supportingText = { Text(stringResource(R.string.memory_dream_correct_expiry_hint)) },
                    isError = expiry.isNotBlank() && parsedExpiry == null,
                    modifier = Modifier.fillMaxWidth(),
                )
            }
        },
        confirmButton = {
            TextButton(
                onClick = {
                    onSubmit(
                        DreamCorrectionDraft(
                            target = detail.target,
                            title = title.trim().ifBlank { null },
                            content = content.trim(),
                            kind = MemoryKind.OTHER,
                            tags = tags.split(',').map(String::trim).filter(String::isNotEmpty).distinct().take(256),
                            expiresAtEpochMs = parsedExpiry,
                        ),
                    )
                },
                enabled = valid,
            ) { Text(stringResource(R.string.memory_dream_save_correction)) }
        },
        dismissButton = {
            TextButton(onClick = onDismiss) { Text(stringResource(R.string.cancel)) }
        },
    )
}

@Composable
private fun DreamSnapshotSection.title(): String = when (this) {
    DreamSnapshotSection.ABOUT_USER -> "斯啾伊"
    DreamSnapshotSection.ABOUT_ASSISTANT -> "七七"
    DreamSnapshotSection.ABOUT_RELATIONSHIP -> "我们"
    DreamSnapshotSection.PROFILE -> stringResource(R.string.memory_dream_section_profile)
    DreamSnapshotSection.CURRENT_PROJECTS -> stringResource(R.string.memory_dream_section_projects)
    DreamSnapshotSection.ACTIVE_PLANS -> stringResource(R.string.memory_dream_section_plans)
    DreamSnapshotSection.ACTIVE_CONSTRAINTS -> stringResource(R.string.memory_dream_section_constraints)
    DreamSnapshotSection.OTHER_CONTEXT -> stringResource(R.string.memory_dream_section_other)
}

@Composable
private fun DreamDerivedStatus.title(): String = when (this) {
    DreamDerivedStatus.EMPTY -> stringResource(R.string.memory_dream_status_empty)
    DreamDerivedStatus.RUNNING -> stringResource(R.string.memory_dream_status_running)
    DreamDerivedStatus.DIRTY -> stringResource(R.string.memory_dream_status_dirty)
    DreamDerivedStatus.READY -> stringResource(R.string.memory_dream_status_ready)
    DreamDerivedStatus.DEGRADED -> stringResource(R.string.memory_dream_status_degraded)
    DreamDerivedStatus.INVALID -> stringResource(R.string.memory_dream_status_invalid)
}

@Composable
private fun DreamUsageMode.title(): String = when (this) {
    DreamUsageMode.OFF -> stringResource(R.string.memory_dream_mode_off)
    DreamUsageMode.GENERATED_ONLY -> stringResource(R.string.memory_dream_mode_generated)
    DreamUsageMode.SHADOW -> stringResource(R.string.memory_dream_mode_shadow)
    DreamUsageMode.ACTIVE -> stringResource(R.string.memory_dream_mode_active)
}

@Composable
private fun DreamSnapshotChangeType.title(): String = when (this) {
    DreamSnapshotChangeType.ADDED -> stringResource(R.string.memory_dream_change_added)
    DreamSnapshotChangeType.UPDATED -> stringResource(R.string.memory_dream_change_updated)
    DreamSnapshotChangeType.RETIRED -> stringResource(R.string.memory_dream_change_retired)
}

private val PAIR_PORTRAIT_SECTIONS = listOf(
    DreamSnapshotSection.ABOUT_USER,
    DreamSnapshotSection.ABOUT_ASSISTANT,
    DreamSnapshotSection.ABOUT_RELATIONSHIP,
)

private val CURRENT_PORTRAIT_STATES = setOf(
    DreamClaimState.ACTIVE_CONTEXTUAL,
    DreamClaimState.PENDING_REVIEW,
)

private const val MAX_VISIBLE_DIFF_ROWS = 20
private const val MAX_VISIBLE_RUNS = 10
private const val MAX_VISIBLE_VERSIONS = 20
