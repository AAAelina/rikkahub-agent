package me.rerere.rikkahub.di

import android.content.Context
import me.rerere.rikkahub.data.db.dao.MemoryV2Dao
import me.rerere.rikkahub.data.files.FileFolders
import me.rerere.rikkahub.data.files.FilesManager
import me.rerere.rikkahub.data.files.SkillManager
import me.rerere.rikkahub.data.repository.ConversationRepository
import me.rerere.rikkahub.data.repository.FavoriteRepository
import me.rerere.rikkahub.data.repository.FilesRepository
import me.rerere.rikkahub.data.repository.GenMediaRepository
import me.rerere.rikkahub.data.repository.MemoryRepository
import me.rerere.rikkahub.data.repository.WorkspaceRepository
import me.rerere.rikkahub.data.files.SharedExchangeDirectory
import me.rerere.workspace.ProotShellRunner
import me.rerere.workspace.RootfsInstaller
import me.rerere.workspace.WorkspaceBindMount
import me.rerere.workspace.WorkspaceManager
import me.rerere.workspace.WorkspaceMountResolver
import me.rerere.workspace.WorkspaceProcessHost
import me.rerere.workspace.WorkspaceProcessManager
import me.rerere.workspace.WorkspaceProcessPersistence
import me.rerere.rikkahub.service.AndroidWorkspaceProcessHost
import me.rerere.rikkahub.AppScope
import org.koin.dsl.module
import java.io.File

val repositoryModule = module {
    single {
        ConversationRepository(get(), get(), get(), get(), get(), get(), get(), get(), get(), get(), get())
    }

    single { me.rerere.rikkahub.data.repository.ConversationDeletionPolicy(get()) }
    single {
        me.rerere.rikkahub.data.repository.AssistantRemovalService(
            settingsStore = get(),
            memoryRepository = get(),
            conversations = get(),
            filesManager = get(),
            authority = get(),
        )
    }

    single {
        MemoryRepository(
            memoryDAO = get(),
            retriever = get(),
            mutationCoordinator = get(),
            memoryV2Dao = get<MemoryV2Dao>(),
        )
    }

    single {
        GenMediaRepository(get())
    }

    single {
        FilesRepository(get())
    }

    single {
        FavoriteRepository(get())
    }

    single {
        val context: Context = get()
        WorkspaceMountResolver(
            sharedStorageBindMount = WorkspaceBindMount(
                source = android.os.Environment.getExternalStorageDirectory(),
                target = "/sdcard",
            ),
            extraBindMounts = listOf(
                WorkspaceBindMount(
                    source = File(context.filesDir, FileFolders.SKILLS).apply { mkdirs() },
                    target = "/skills",
                ),
                WorkspaceBindMount(
                    source = File(context.filesDir, FileFolders.TOOL_OUTPUTS).apply { mkdirs() },
                    target = "/tool_outputs",
                ),
            ),
        )
    }

    single {
        val context: Context = get()
        ProotShellRunner(
            nativeLibraryDir = File(context.applicationInfo.nativeLibraryDir),
            mountResolver = get(),
        )
    }

    single {
        val context: Context = get()
        WorkspaceManager(
            baseDir = File(context.filesDir, "workspaces"),
            sharedFilesBaseDir = File(
                android.os.Environment.getExternalStorageDirectory(),
                "${SharedExchangeDirectory.DIRECTORY_NAME}/workspaces",
            ),
            shellRunner = get<ProotShellRunner>(),
            mountResolver = get(),
        )
    }

    single { WorkspaceProcessPersistence(get()) }
    single<WorkspaceProcessHost> { AndroidWorkspaceProcessHost(get()) }
    single {
        WorkspaceProcessManager(
            workspaceManager = get(),
            launcher = get<ProotShellRunner>(),
            persistence = get(),
            host = get(),
            scope = get<AppScope>(),
        )
    }

    single {
        RootfsInstaller(get())
    }

    single {
        WorkspaceRepository(get(), get(), get(), get(), get())
    }

    single {
        FilesManager(get(), get(), get())
    }

    single {
        SkillManager(get(), get())
    }
}
