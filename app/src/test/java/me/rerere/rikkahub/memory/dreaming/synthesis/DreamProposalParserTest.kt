package me.rerere.rikkahub.memory.dreaming.synthesis

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class DreamProposalParserTest {
    @Test
    fun `accepts exactly one strict proposal object`() {
        val result = DreamProposalParser.parse(validNoOp())
        assertTrue(result is DreamProposalParseResult.Parsed)
        assertTrue(
            DreamProposalParser.parse("```json\n${validNoOp()}\n```") is
                DreamProposalParseResult.Parsed,
        )
    }

    @Test
    fun `rejects prose fences unknown keys enums and semantic duplicate keys`() {
        val cases = listOf(
            "prefix ${validNoOp()}" to DreamProposalParseFailure.INVALID_JSON,
            "```json\n${validNoOp()}\n```\ntrailing" to DreamProposalParseFailure.INVALID_JSON,
            "```json\n```json\n${validNoOp()}\n```\n```" to DreamProposalParseFailure.INVALID_JSON,
            validNoOp().replaceFirst("{", "{\"extra\":1,") to DreamProposalParseFailure.UNKNOWN_FIELD,
            validNoOp().replace("\"INCREMENTAL\"", "\"incremental\"") to DreamProposalParseFailure.UNKNOWN_ENUM,
            validNoOp().replace("\"schema_version\":2", "\"schema_version\":2,\"schema_\\u0076ersion\":2") to
                DreamProposalParseFailure.DUPLICATE_KEY,
        )

        cases.forEach { (raw, failure) ->
            assertEquals(failure, (DreamProposalParser.parse(raw) as DreamProposalParseResult.Rejected).failure)
        }
    }

    @Test
    fun `escaped nul in claim text fails closed`() {
        val raw = validUpsert().replace("safe statement", "unsafe\\u0000statement")
        assertEquals(
            DreamProposalParseFailure.INVALID_VALUE,
            (DreamProposalParser.parse(raw) as DreamProposalParseResult.Rejected).failure,
        )
    }

    @Test
    fun `empty operation array is rejected instead of becoming an implicit no-op`() {
        val raw = validNoOp().replace("[{\"op\":\"NO_OP\"}]", "[]")

        assertEquals(
            DreamProposalParseFailure.INVALID_VALUE,
            (DreamProposalParser.parse(raw) as DreamProposalParseResult.Rejected).failure,
        )
    }

    @Test
    fun `published prompt ABI describes a gold output that strict parser accepts`() {
        val contract = me.rerere.rikkahub.memory.dreaming.input.DreamInputBuilder.SYSTEM_CONTRACT
        assertTrue(contract.contains(DREAM_PROMPT_CONTRACT_VERSION))
        assertTrue(contract.contains("UPSERT_CLAIM"))
        assertTrue(contract.contains("SUPERSEDE_CLAIM"))
        assertTrue(contract.contains("INVALIDATE_CLAIM"))
        assertTrue(contract.contains("NO_OP"))
        assertTrue(DreamProposalParser.parse(validUpsert()) is DreamProposalParseResult.Parsed)
    }

    companion object {
        fun validNoOp(): String =
            """{"schema_version":2,"proposal_nonce":"p_${"N".repeat(43)}","base_experience_epoch":7,"base_dream_revision":3,"mode":"INCREMENTAL","operations":[{"op":"NO_OP"}]}"""

        fun validUpsert(
            experienceToken: String = "m_${"A".repeat(22)}",
            subjectKind: String = "USER",
            epistemicOrigin: String = "EXPLICIT",
            contentType: String = "PROJECT_STATE",
            temporalExpression: String = "null",
        ): String =
            """{"schema_version":2,"proposal_nonce":"p_${"N".repeat(43)}","base_experience_epoch":7,"base_dream_revision":3,"mode":"INCREMENTAL","operations":[{"op":"UPSERT_CLAIM","target_claim_token":null,"expected_claim_revision":null,"claim":{"claim_key_hint":"project.offline","subject_kind":"$subjectKind","profile_section":"project","epistemic_origin":"$epistemicOrigin","content_type":"$contentType","title":"Safe title","statement":"safe statement","temporal_expression":$temporalExpression,"evidence":[{"experience_token":"$experienceToken","expected_epoch":2,"support_type":"SUPPORTS"}]}}]}"""
    }
}
