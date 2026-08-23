package me.rerere.rikkahub.memory.dreaming.runtime

import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import me.rerere.ai.core.InputSchema
import me.rerere.ai.core.Tool
import me.rerere.ai.ui.UIMessagePart
import me.rerere.rikkahub.data.ai.tools.ToolInvocationContext
import me.rerere.rikkahub.data.db.dao.DreamDao
import me.rerere.rikkahub.data.db.dao.DreamExperienceDao
import me.rerere.rikkahub.data.db.dao.DreamSynthesisDao
import me.rerere.rikkahub.memory.dreaming.model.DreamPairScopeId
import kotlin.uuid.Uuid

/** Read-only self-introspection for the currently calling assistant's pair Dream. */
class DreamIntrospectionToolProvider(
    private val experienceDao: DreamExperienceDao,
    private val synthesisDao: DreamSynthesisDao,
    private val dreamDao: DreamDao,
) {
    fun tools(context: ToolInvocationContext): List<Tool> {
        val assistantId = context.callerAssistantId
            ?.let { raw -> runCatching { Uuid.parse(raw) }.getOrNull() }
            ?: return emptyList()
        return listOf(dreamViewTool(DreamPairScopeId.forAssistant(assistantId).value))
    }

    private fun dreamViewTool(scopeId: String) = Tool(
        name = "dream_view",
        description = "Read your own current Dream portrait, Dream claims, recent Dream runs, or the experiences supporting one claim. Read-only and automatically scoped to the calling assistant.",
        parameters = {
            InputSchema.Obj(
                properties = buildJsonObject {
                    put("view", type("string", "summary | claims | recent | sources. Defaults to summary."))
                    put("claim_id", type("string", "Required only for sources; use an ID returned by claims."))
                },
            )
        },
        execute = { args ->
            val input = args.jsonObject
            val view = input["view"]?.jsonPrimitive?.contentOrNull?.lowercase() ?: "summary"
            val payload = when (view) {
                "summary" -> summary(scopeId)
                "claims" -> claims(scopeId)
                "recent" -> recent(scopeId)
                "sources" -> sources(scopeId, input["claim_id"]?.jsonPrimitive?.contentOrNull)
                else -> error("DREAM_VIEW_INVALID_VIEW")
            }
            response(payload)
        },
    )

    private suspend fun summary(scopeId: String) = buildJsonObject {
        val state = experienceDao.getState(scopeId)
        val claims = synthesisDao.listRuntimeActiveClaimHeads(scopeId, MAX_TOOL_CLAIMS)
        val recent = dreamDao.listRecentRuns(scopeId, 10).firstOrNull { it.status == "SUCCEEDED" }
        put("status", if (state?.activeSnapshotId == null) "no_active_dream" else "active")
        put("profile_revision", state?.profileRevision ?: 0L)
        put("experience_epoch", state?.experienceEpoch ?: 0L)
        put("applied_experience_epoch", state?.appliedExperienceEpoch ?: 0L)
        put("lag", state?.let { it.experienceEpoch - it.appliedExperienceEpoch } ?: 0L)
        put("pending_experience_count", state?.let {
            experienceDao.countPending(scopeId, it.appliedExperienceEpoch)
        } ?: 0)
        put("experience_debt", state?.experienceDebt ?: 0.0)
        put("last_dream_at_ms", recent?.finishedAtMs ?: recent?.startedAtMs ?: recent?.createdAtMs ?: 0L)
        put("about_user", portraitClaims(claims, "USER"))
        put("about_assistant", portraitClaims(claims, "ASSISTANT"))
        put("about_relationship", portraitClaims(claims, "RELATIONSHIP"))
    }

    private suspend fun claims(scopeId: String) = buildJsonObject {
        put("claims", buildJsonArray {
            synthesisDao.listRuntimeActiveClaimHeads(scopeId, MAX_TOOL_CLAIMS).forEach { claim ->
                add(buildJsonObject {
                    put("claim_id", claim.claimId)
                    put("revision", claim.claimRevision)
                    put("subject", claim.subjectKind)
                    put("profile_section", claim.profileSection)
                    put("epistemic_origin", claim.epistemicOrigin)
                    put("content_type", claim.contentType)
                    put("title", claim.title)
                    put("statement", claim.statement)
                    put("confidence", claim.confidence)
                })
            }
        })
    }

    private suspend fun recent(scopeId: String) = buildJsonObject {
        val state = experienceDao.getState(scopeId)
        val recentRuns = dreamDao.listRecentRuns(scopeId, 10)
        val latestSucceeded = recentRuns.firstOrNull { it.status == "SUCCEEDED" }
        put("profile_revision", state?.profileRevision ?: 0L)
        put("lag", state?.let { it.experienceEpoch - it.appliedExperienceEpoch } ?: 0L)
        put("runs", buildJsonArray {
            recentRuns.forEach { run ->
                add(buildJsonObject {
                    put("mode", run.mode)
                    put("status", run.status)
                    put("base_experience_epoch", run.baseMemoryEpoch)
                    put("base_profile_revision", run.baseDreamRevision)
                    put("created_at_ms", run.createdAtMs)
                    put("started_at_ms", run.startedAtMs ?: 0L)
                    put("finished_at_ms", run.finishedAtMs ?: 0L)
                    put("failure_code", run.failureCode ?: "")
                    put("input_experience_count", run.inputMemoryCount ?: 0)
                    put("output_claim_count", run.outputClaimCount ?: 0)
                })
            }
        })
        put("recent_absorbed_experiences", buildJsonArray {
            latestSucceeded?.let { run ->
                val through = run.baseMemoryEpoch
                experienceDao.listExperiences(
                    pairScopeId = scopeId,
                    afterExclusiveEpoch = (through - MAX_RECENT_EXPERIENCES).coerceAtLeast(0L),
                    throughInclusiveEpoch = through,
                    limit = MAX_RECENT_EXPERIENCES,
                ).forEach { experience ->
                    add(buildJsonObject {
                        put("experience_id", experience.experienceId)
                        put("experience_epoch", experience.experienceEpoch)
                        put("experience_kind", experience.experienceKind)
                        put("actor", experience.actor)
                        put("occurred_at_ms", experience.occurredAtMs)
                        put("summary", experience.summary)
                    })
                }
            }
        })
    }

    private suspend fun sources(scopeId: String, claimId: String?) = buildJsonObject {
        val id = claimId?.takeIf(String::isNotBlank)
        if (id == null) {
            put("error", "DREAM_VIEW_CLAIM_ID_REQUIRED")
            return@buildJsonObject
        }
        val claim = synthesisDao.getClaim(id, scopeId)
        if (claim == null) {
            put("error", "DREAM_VIEW_CLAIM_NOT_FOUND")
            return@buildJsonObject
        }
        put("claim_id", claim.claimId)
        put("title", claim.title)
        put("statement", claim.statement)
        put("sources", buildJsonArray {
            synthesisDao.listClaimExperienceSources(claim.claimId, claim.claimRevision).forEach { source ->
                val experience = experienceDao.getExperience(source.experienceId, scopeId)
                    ?: return@forEach
                add(buildJsonObject {
                    put("experience_id", experience.experienceId)
                    put("experience_epoch", experience.experienceEpoch)
                    put("source_kind", experience.sourceKind)
                    put("source_ref", experience.sourceRef)
                    put("experience_kind", experience.experienceKind)
                    put("conversation_id", experience.conversationId ?: "")
                    put("occurred_at_ms", experience.occurredAtMs)
                    put("summary", experience.summary)
                    put("source_manifest_json", experience.sourceManifestJson)
                    put("support_type", source.supportType)
                })
            }
        })
    }

    private fun portraitClaims(
        claims: List<me.rerere.rikkahub.data.db.entity.DreamClaimEntity>,
        subject: String,
    ) = buildJsonArray {
        claims.filter { it.subjectKind == subject }.forEach { claim ->
            add(buildJsonObject {
                put("title", claim.title)
                put("statement", claim.statement)
                put("profile_section", claim.profileSection)
                put("confidence", claim.confidence)
            })
        }
    }

    private fun type(type: String, description: String) = buildJsonObject {
        put("type", type)
        put("description", description)
    }

    private fun error(code: String) = buildJsonObject { put("error", code) }
    private fun response(value: kotlinx.serialization.json.JsonObject) = listOf(UIMessagePart.Text(value.toString()))

    private companion object {
        const val MAX_TOOL_CLAIMS = 256
        const val MAX_RECENT_EXPERIENCES = 20
    }
}
