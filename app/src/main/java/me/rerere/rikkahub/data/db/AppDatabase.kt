package me.rerere.rikkahub.data.db

import androidx.room.AutoMigration
import androidx.room.Database
import androidx.room.RoomDatabase
import androidx.room.TypeConverter
import androidx.room.TypeConverters
import me.rerere.ai.core.TokenUsage
import me.rerere.rikkahub.data.agentrun.AgentRun
import me.rerere.rikkahub.data.agentrun.AgentRunDao
import me.rerere.rikkahub.data.execution.ExecutionRecord
import me.rerere.rikkahub.data.execution.ExecutionRecordDao
import me.rerere.rikkahub.data.execution.ExecutionEventDao
import me.rerere.rikkahub.data.execution.ExecutionEventRecord
import me.rerere.rikkahub.data.execution.PendingToolApprovalDao
import me.rerere.rikkahub.data.execution.PendingToolApprovalRecord
import me.rerere.rikkahub.data.capability.CapabilityGrantDao
import me.rerere.rikkahub.data.capability.CapabilityGrantEntity
import me.rerere.rikkahub.data.db.dao.AlarmDao
import me.rerere.rikkahub.data.db.dao.ConversationDAO
import me.rerere.rikkahub.data.db.dao.DreamDao
import me.rerere.rikkahub.data.db.dao.DreamExperienceDao
import me.rerere.rikkahub.data.db.dao.DreamSynthesisDao
import me.rerere.rikkahub.data.db.dao.BrowserLibraryDao
import me.rerere.rikkahub.data.db.dao.FavoriteDAO
import me.rerere.rikkahub.data.db.dao.GenMediaDAO
import me.rerere.rikkahub.data.db.dao.ManagedFileDAO
import me.rerere.rikkahub.data.db.dao.MemoryDAO
import me.rerere.rikkahub.data.db.dao.MemoryV2Dao
import me.rerere.rikkahub.data.db.dao.MessageNodeDAO
import me.rerere.rikkahub.data.db.dao.LearningOutboxDao
import me.rerere.rikkahub.data.db.dao.LearningReconciliationAuthorityDao
import me.rerere.rikkahub.data.db.dao.LearningSourceAuthorityDao
import me.rerere.rikkahub.data.db.dao.LearningPolicyGrantDao
import me.rerere.rikkahub.data.db.dao.PendingChatCommandDao
import me.rerere.rikkahub.data.db.dao.RewardFeedbackAuthorityDao
import me.rerere.rikkahub.data.db.dao.PetDialogueDao
import me.rerere.rikkahub.data.db.dao.ScheduledJobDao
import me.rerere.rikkahub.data.db.dao.ScheduledJobRunDao
import me.rerere.rikkahub.data.db.dao.SshHostDao
import me.rerere.rikkahub.data.db.dao.TelegramChatDao
import me.rerere.rikkahub.data.db.dao.WorkspaceDAO
import me.rerere.rikkahub.toolcatalog.ToolExperienceDao
import me.rerere.rikkahub.toolcatalog.ToolExperienceEntity
import me.rerere.rikkahub.toolcatalog.ToolExperienceEvidenceEntity
import me.rerere.rikkahub.toolcatalog.ToolExperienceRevisionEntity
import me.rerere.rikkahub.toolcatalog.ToolShortcutDao
import me.rerere.rikkahub.toolcatalog.ToolShortcutEntity
import me.rerere.rikkahub.data.db.entity.AlarmEntity
import me.rerere.rikkahub.data.db.entity.ConversationEntity
import me.rerere.rikkahub.data.db.entity.DreamRunEntity
import me.rerere.rikkahub.data.db.entity.DreamExperienceEntity
import me.rerere.rikkahub.data.db.entity.DreamExperienceStateEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimExperienceSourceEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimVersionEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimVersionSourceEntity
import me.rerere.rikkahub.data.db.entity.DreamSnapshotEntity
import me.rerere.rikkahub.data.db.entity.BrowserBookmarkEntity
import me.rerere.rikkahub.data.db.entity.BrowserHistoryEntity
import me.rerere.rikkahub.data.db.entity.FavoriteEntity
import me.rerere.rikkahub.data.db.entity.GenMediaEntity
import me.rerere.rikkahub.data.db.entity.ManagedFileEntity
import me.rerere.rikkahub.data.db.entity.MemoryEntity
import me.rerere.rikkahub.data.db.entity.MemoryCandidateEntity
import me.rerere.rikkahub.data.db.entity.MemoryCaptureEntity
import me.rerere.rikkahub.data.db.entity.MemoryRevisionEntity
import me.rerere.rikkahub.data.db.entity.MemoryEvidenceEntity
import me.rerere.rikkahub.data.db.entity.MemoryLinkEntity
import me.rerere.rikkahub.data.db.entity.MemoryLinkRevisionEntity
import me.rerere.rikkahub.data.db.entity.MemoryRelationCandidateEntity
import me.rerere.rikkahub.data.db.entity.MemoryBackfillRunEntity
import me.rerere.rikkahub.data.db.entity.MemorySourceTombstoneEntity
import me.rerere.rikkahub.data.db.entity.MemoryScopeChangeEntity
import me.rerere.rikkahub.data.db.entity.MemoryScopeStateEntity
import me.rerere.rikkahub.data.db.entity.MessageNodeEntity
import me.rerere.rikkahub.data.db.entity.LearningOutboxEntity
import me.rerere.rikkahub.data.db.entity.LearningConversationSourceAuthorityEntity
import me.rerere.rikkahub.data.db.entity.LearningMessageSourceAuthorityEntity
import me.rerere.rikkahub.data.db.entity.LearningPolicyGrantEntity
import me.rerere.rikkahub.data.db.entity.LearningPolicyGrantRevisionEntity
import me.rerere.rikkahub.data.db.entity.PendingChatCommandEntity
import me.rerere.rikkahub.data.db.entity.RewardFeedbackAuthorityEntity
import me.rerere.rikkahub.data.db.entity.RewardFeedbackAuthorityRevisionEntity
import me.rerere.rikkahub.data.db.entity.PetDialogueRevisionEntity
import me.rerere.rikkahub.data.db.entity.PetDialogueSessionEntity
import me.rerere.rikkahub.data.db.entity.PetDialogueTurnEntity
import me.rerere.rikkahub.data.db.entity.PetHandoffRequestEntity
import me.rerere.rikkahub.data.db.entity.ScheduledJobEntity
import me.rerere.rikkahub.data.db.entity.ScheduledJobRunEntity
import me.rerere.rikkahub.data.db.entity.SshHostEntity
import me.rerere.rikkahub.data.db.entity.TelegramChatEntity
import me.rerere.rikkahub.data.db.entity.WorkspaceEntity
import me.rerere.rikkahub.data.db.migrations.Migration_16_17
import me.rerere.rikkahub.data.db.migrations.Migration_20_21
import me.rerere.rikkahub.data.db.migrations.Migration_21_22
import me.rerere.rikkahub.data.db.migrations.Migration_22_23
import me.rerere.rikkahub.data.db.migrations.Migration_8_9
import me.rerere.rikkahub.data.db.migrations.MIGRATION_26_27
import me.rerere.rikkahub.data.db.migrations.MIGRATION_27_28
import me.rerere.rikkahub.utils.JsonInstant
import me.rerere.rikkahub.workflow.db.WorkflowDao
import me.rerere.rikkahub.workflow.db.WorkflowEntity
import me.rerere.rikkahub.workflow.db.WorkflowRunDao
import me.rerere.rikkahub.workflow.db.WorkflowRunEntity
import me.rerere.rikkahub.owner.db.HostLocalServiceDao
import me.rerere.rikkahub.owner.db.HostLocalServiceEntity
import me.rerere.rikkahub.owner.db.HostOperationDao
import me.rerere.rikkahub.owner.db.HostOperationEntity
import me.rerere.rikkahub.owner.db.HostOperationEventEntity

@Database(
    entities = [
        AlarmEntity::class,
        BrowserBookmarkEntity::class,
        BrowserHistoryEntity::class,
        ConversationEntity::class,
        MemoryEntity::class,
        MemoryCaptureEntity::class,
        MemoryCandidateEntity::class,
        MemoryRevisionEntity::class,
        MemoryEvidenceEntity::class,
        MemoryLinkEntity::class,
        MemoryLinkRevisionEntity::class,
        MemoryRelationCandidateEntity::class,
        MemoryBackfillRunEntity::class,
        MemorySourceTombstoneEntity::class,
        MemoryScopeStateEntity::class,
        MemoryScopeChangeEntity::class,
        DreamRunEntity::class,
        DreamExperienceEntity::class,
        DreamExperienceStateEntity::class,
        DreamClaimEntity::class,
        DreamClaimExperienceSourceEntity::class,
        DreamClaimVersionEntity::class,
        DreamClaimVersionSourceEntity::class,
        DreamSnapshotEntity::class,
        GenMediaEntity::class,
        MessageNodeEntity::class,
        ManagedFileEntity::class,
        FavoriteEntity::class,
        ScheduledJobEntity::class,
        ScheduledJobRunEntity::class,
        SshHostEntity::class,
        TelegramChatEntity::class,
        WorkflowEntity::class,
        WorkflowRunEntity::class,
        AgentRun::class,
        ExecutionRecord::class,
        ExecutionEventRecord::class,
        PendingToolApprovalRecord::class,
        CapabilityGrantEntity::class,
        WorkspaceEntity::class,
        PendingChatCommandEntity::class,
        PetDialogueSessionEntity::class,
        PetDialogueTurnEntity::class,
        PetHandoffRequestEntity::class,
        PetDialogueRevisionEntity::class,
        ToolExperienceEntity::class,
        ToolExperienceEvidenceEntity::class,
        ToolExperienceRevisionEntity::class,
        ToolShortcutEntity::class,
        HostOperationEntity::class,
        HostOperationEventEntity::class,
        HostLocalServiceEntity::class,
        LearningOutboxEntity::class,
        LearningConversationSourceAuthorityEntity::class,
        LearningMessageSourceAuthorityEntity::class,
        LearningPolicyGrantEntity::class,
        LearningPolicyGrantRevisionEntity::class,
        RewardFeedbackAuthorityEntity::class,
        RewardFeedbackAuthorityRevisionEntity::class,
    ],
    // v49 makes workflow capability/provenance authority durable. Learned artifacts remain
    // disabled until an explicit cross-database promotion completes.
    version = 50,
    autoMigrations = [
        AutoMigration(from = 1, to = 2),
        AutoMigration(from = 2, to = 3),
        AutoMigration(from = 3, to = 4),
        AutoMigration(from = 4, to = 5),
        AutoMigration(from = 5, to = 6),
        AutoMigration(from = 7, to = 8),
        AutoMigration(from = 8, to = 9, spec = Migration_8_9::class),
        AutoMigration(from = 9, to = 10),
        AutoMigration(from = 10, to = 11),
        AutoMigration(from = 12, to = 13),
        AutoMigration(from = 16, to = 17, spec = Migration_16_17::class),
        AutoMigration(from = 17, to = 18),
        AutoMigration(from = 18, to = 19),
        AutoMigration(from = 19, to = 20),
        AutoMigration(from = 20, to = 21, spec = Migration_20_21::class),
        AutoMigration(from = 21, to = 22, spec = Migration_21_22::class),
        AutoMigration(from = 22, to = 23, spec = Migration_22_23::class),
        // v25: upstream 2.2.6 added conversation-level custom_system_prompt / mode_injection_ids
        // / lorebook_ids columns (all carry defaultValue, so a plain auto-migration suffices).
        AutoMigration(from = 24, to = 25),
        // v26: the 2.3.1 merge brings upstream's workspaces table (WorkspaceEntity). Existing
        // fork users never had it, so Room auto-creates the table on this step.
        AutoMigration(from = 25, to = 26),
    ]
)
@TypeConverters(TokenUsageConverter::class)
abstract class AppDatabase : RoomDatabase() {
    abstract fun alarmDao(): AlarmDao

    abstract fun browserLibraryDao(): BrowserLibraryDao

    abstract fun conversationDao(): ConversationDAO

    abstract fun memoryDao(): MemoryDAO

    abstract fun memoryV2Dao(): MemoryV2Dao

    abstract fun dreamDao(): DreamDao

    abstract fun dreamExperienceDao(): DreamExperienceDao

    abstract fun dreamSynthesisDao(): DreamSynthesisDao

    abstract fun genMediaDao(): GenMediaDAO

    abstract fun messageNodeDao(): MessageNodeDAO

    abstract fun managedFileDao(): ManagedFileDAO

    abstract fun favoriteDao(): FavoriteDAO

    abstract fun scheduledJobDao(): ScheduledJobDao

    abstract fun scheduledJobRunDao(): ScheduledJobRunDao

    abstract fun sshHostDao(): SshHostDao

    abstract fun telegramChatDao(): TelegramChatDao

    abstract fun workflowDao(): WorkflowDao

    abstract fun workflowRunDao(): WorkflowRunDao

    abstract fun agentRunDao(): AgentRunDao

    abstract fun executionRecordDao(): ExecutionRecordDao

    abstract fun executionEventDao(): ExecutionEventDao

    abstract fun pendingToolApprovalDao(): PendingToolApprovalDao

    abstract fun capabilityGrantDao(): CapabilityGrantDao

    abstract fun workspaceDao(): WorkspaceDAO

    abstract fun pendingChatCommandDao(): PendingChatCommandDao

    abstract fun petDialogueDao(): PetDialogueDao

    abstract fun toolExperienceDao(): ToolExperienceDao

    abstract fun toolShortcutDao(): ToolShortcutDao

    abstract fun hostOperationDao(): HostOperationDao

    abstract fun hostLocalServiceDao(): HostLocalServiceDao

    abstract fun learningOutboxDao(): LearningOutboxDao

    abstract fun learningReconciliationAuthorityDao(): LearningReconciliationAuthorityDao

    abstract fun learningSourceAuthorityDao(): LearningSourceAuthorityDao

    abstract fun learningPolicyGrantDao(): LearningPolicyGrantDao

    abstract fun rewardFeedbackAuthorityDao(): RewardFeedbackAuthorityDao
}

object TokenUsageConverter {
    @TypeConverter
    fun fromTokenUsage(usage: TokenUsage?): String {
        return JsonInstant.encodeToString(usage)
    }

    @TypeConverter
    fun toTokenUsage(usage: String): TokenUsage? {
        return JsonInstant.decodeFromString(usage)
    }
}
