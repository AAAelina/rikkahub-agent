package me.rerere.rikkahub.data.ai

import me.rerere.ai.ui.UIMessage
import me.rerere.ai.ui.UIMessagePart
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.runtime.DreamRuntimeCompileStatus

/** Pure preparation helpers for Dream Recall. */
internal fun DreamGenerationContext.toRecallDreamItems(
    scopeId: DreamScopeId,
): List<RecallDreamContextItem> {
    val compiled = compileResult
        ?.takeIf { it.status == DreamRuntimeCompileStatus.COMPILED }
        ?: return emptyList()
    if (compiled.renderedSection.isBlank() || compiled.actualClaimRefs.isEmpty()) return emptyList()
    return listOf(
        RecallDreamContextItem(
            scopeId = scopeId.value,
            claims = compiled.actualClaimRefs.map { ref ->
                RecallDreamClaimIdentity(ref.claimId, ref.claimRevision)
            },
            renderedFragment = compiled.renderedSection,
            compilerRevision = compiled.compilerRevision,
        ),
    )
}

internal fun RecallPromptCompileResult.requirePresentOnFinalWire(
    messages: List<UIMessage>,
): Boolean {
    if (text.isEmpty()) return false
    val present = messages.asSequence()
        .flatMap { it.parts.asSequence() }
        .filterIsInstance<UIMessagePart.Text>()
        .any { text in it.text }
    check(present) { "Compiled Recall projection was not preserved by the final provider gate" }
    return true
}
