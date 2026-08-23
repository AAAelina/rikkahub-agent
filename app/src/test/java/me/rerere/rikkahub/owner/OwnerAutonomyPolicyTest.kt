package me.rerere.rikkahub.owner

import me.rerere.rikkahub.assistant.SecondUserAdmissionSnapshot
import me.rerere.rikkahub.assistant.SecondUserAuthorityRegistry
import me.rerere.rikkahub.data.ai.ToolCallOrigin
import me.rerere.rikkahub.privilege.PrivilegedSessionContext
import org.junit.After
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.uuid.Uuid

class OwnerAutonomyPolicyTest {
    private val assistantId = Uuid.parse("10000000-0000-0000-0000-000000000001")
    private val conversationId = Uuid.parse("20000000-0000-0000-0000-000000000002")
    private val snapshot = SecondUserAdmissionSnapshot.create(
        assistantId = assistantId,
        conversationId = conversationId,
        authorityEpoch = 7,
        origin = ToolCallOrigin.LocalChat,
    )

    @After
    fun clearRegistry() {
        SecondUserAuthorityRegistry.install(null)
    }

    @Test
    fun `live local owner auto approves plugin and linux grant tools`() {
        SecondUserAuthorityRegistry.install(snapshot)
        val context = context(ToolCallOrigin.LocalChat)

        assertTrue(OwnerAutonomyPolicy.canAutoApprove(context, context.origin, "plugin__abc__write"))
        assertTrue(OwnerAutonomyPolicy.canAutoApprove(context, context.origin, "mcp__gbao__exec_command"))
        assertTrue(OwnerAutonomyPolicy.canAutoApprove(context, context.origin, "linux_grant_request"))
        assertTrue(OwnerAutonomyPolicy.canAutoApprove(context, context.origin, "linux_grant_revoke"))
        OwnerToolFamily.entries.forEach { family ->
            assertTrue(OwnerAutonomyPolicy.canAutoApprove(context, context.origin, family.toolName))
        }
    }

    @Test
    fun `owner authority never turns ask user into an automatic answer`() {
        SecondUserAuthorityRegistry.install(snapshot)
        val context = context(ToolCallOrigin.LocalChat)
        assertFalse(OwnerAutonomyPolicy.canAutoApprove(context, context.origin, "ask_user"))
    }

    @Test
    fun `remote automatic handoff and stale epoch do not inherit owner autonomy`() {
        SecondUserAuthorityRegistry.install(snapshot)
        val remote = context(ToolCallOrigin.Telegram)
        val automaticPet = context(ToolCallOrigin.PetHandoffAuto)
        val stale = context(ToolCallOrigin.LocalChat).copy(authoritySubjectId =
            SecondUserAdmissionSnapshot.subjectId(assistantId, conversationId, 6))

        assertFalse(OwnerAutonomyPolicy.canAutoApprove(remote, remote.origin, "plugin__abc__write"))
        assertFalse(OwnerAutonomyPolicy.canAutoApprove(automaticPet, automaticPet.origin, "linux_grant_request"))
        assertFalse(OwnerAutonomyPolicy.canAutoApprove(stale, stale.origin, "linux_grant_request"))
    }

    private fun context(origin: ToolCallOrigin) = PrivilegedSessionContext(
        assistantId = assistantId,
        conversationId = conversationId,
        origin = origin,
        privilegedConversationId = conversationId,
        identityName = "owner",
        isPrivileged = true,
        expandLocalTools = true,
        autoApproveTools = true,
        unrestrictedOverride = false,
        authoritySubjectId = snapshot.subjectId,
        authorityEpoch = snapshot.authorityEpoch,
    )
}
