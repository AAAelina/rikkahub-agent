import com.android.build.api.dsl.Packaging
import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.gradle.api.tasks.testing.Test
import java.io.FileInputStream
import java.util.Properties

plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.compose)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.ksp)
}

android {
    namespace = "me.rerere.rikkahub"
    compileSdk = 37

    defaultConfig {
        applicationId = "me.rerere.rikkahub"
        minSdk = 26
        targetSdk = 37
        versionCode = 191
        versionName = "2.3.1-agent-up244.7"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        // AGP 9.2 UTP copies PlatformTestStorage output to
        // build/outputs/managed_device_android_test_additional_output.
        testInstrumentationRunnerArguments["useTestStorageService"] = "true"

        ndk {
            abiFilters += listOf("arm64-v8a", "x86_64")
        }
    }

    splits {
        abi {
            // AppBundle tasks usually contain "bundle" in their name
            //noinspection WrongGradleMethod
            val isBuildingBundle = gradle.startParameter.taskNames.any { it.lowercase().contains("bundle") }
            isEnable = !isBuildingBundle
            reset()
            include("arm64-v8a", "x86_64")
            isUniversalApk = true
        }
    }

    val localProperties = Properties().apply {
        val localPropertiesFile = rootProject.file("local.properties")
        if (localPropertiesFile.exists()) {
            load(FileInputStream(localPropertiesFile))
        }
    }

    signingConfigs {
        val configuredDebugStore = localProperties.getProperty("debugStoreFile")
            ?: System.getenv("RIKKAHUB_DEBUG_KEYSTORE")
        val debugStoreFile = configuredDebugStore?.let(::file)
            ?: file("${System.getProperty("user.home")}/.android/debug.keystore")
        if (debugStoreFile.isFile) {
            create("legacyDebug") {
                // Keep updates installable over an existing personal build while allowing a
                // clean workstation to fall back to AGP's generated debug signing config.
                storeFile = debugStoreFile
                storePassword = localProperties.getProperty("debugStorePassword", "android")
                keyAlias = localProperties.getProperty("debugKeyAlias", "androiddebugkey")
                keyPassword = localProperties.getProperty("debugKeyPassword", "android")
            }
        }

        create("release") {
            val storeFilePath = localProperties.getProperty("storeFile")
            val storePasswordValue = localProperties.getProperty("storePassword")
            val keyAliasValue = localProperties.getProperty("keyAlias")
            val keyPasswordValue = localProperties.getProperty("keyPassword")

            if (storeFilePath != null && storePasswordValue != null &&
                keyAliasValue != null && keyPasswordValue != null
            ) {
                storeFile = file(storeFilePath)
                storePassword = storePasswordValue
                keyAlias = keyAliasValue
                keyPassword = keyPasswordValue
            }
        }
    }

    buildTypes {
        release {
            signingConfig = signingConfigs.getByName("release")
            isMinifyEnabled = true
            isShrinkResources = true
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
            buildConfigField("String", "VERSION_NAME", "\"${android.defaultConfig.versionName}\"")
            buildConfigField("String", "VERSION_CODE", "\"${android.defaultConfig.versionCode}\"")
            buildConfigField("String", "UPDATE_API_URL", "\"\"")
        }
        debug {
            signingConfig = signingConfigs.findByName("legacyDebug")
                ?: signingConfigs.getByName("debug")
            buildConfigField("String", "VERSION_NAME", "\"${android.defaultConfig.versionName}\"")
            buildConfigField("String", "VERSION_CODE", "\"${android.defaultConfig.versionCode}\"")
            buildConfigField("String", "UPDATE_API_URL", "\"\"")
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    buildFeatures {
        compose = true
        buildConfig = true
        // agent-keyboard IPC: IKeyboardApi.aidl + EditorInfoBundle.aidl in src/main/aidl.
        aidl = true
    }
    sourceSets {
        getByName("androidTest").assets.directories.add("$projectDir/schemas")
    }
    androidResources {
        generateLocaleConfig = true
    }
    packaging {
        jniLibs {
            useLegacyPackaging = true
            pickFirsts += "lib/*/libtermux.so"
        }
    }
    lint {
        // FullBackupContent insists every <exclude> path lives under a previously
        // <include>'d root. Our backup_rules.xml + data_extraction_rules.xml use
        // include="upload/" + explicit excludes for databases / sharedpref /
        // datastore/ / known_hosts / browser-profile/ / local-models/ as
        // belt-and-suspenders defence: if anyone later adds a broader <include>
        // (e.g. domain="root"), the excludes still keep credentials and
        // multi-GB local LLM weights off the cloud-backup path. Lint reads that
        // pattern as redundant; the runtime accepts it. Keep the rules; mute
        // the check.
        disable.add("FullBackupContent")
    }
    testOptions {
        managedDevices {
            localDevices {
                // Disposable emulator-only P5 Room/FTS/migration/process-recovery gate.
                // Configuration and source compilation do not download the image. Running the
                // generated task may download it and must never be replaced with connected tests
                // against the user's Honor AAK-AN00 primary phone.
                create("p5DisposablePixel6Api35") {
                    device = "Pixel 6"
                    apiLevel = 35
                    systemImageSource = "aosp-atd"
                    testedAbi = "x86_64"
                }
            }
        }
    }
    tasks.withType<KotlinCompile>().configureEach {
        compilerOptions.optIn.add("androidx.compose.material3.ExperimentalMaterial3Api")
        compilerOptions.optIn.add("androidx.compose.material3.ExperimentalMaterial3ExpressiveApi")
        compilerOptions.optIn.add("androidx.compose.material3.adaptive.ExperimentalMaterial3AdaptiveApi")
        compilerOptions.optIn.add("androidx.compose.animation.ExperimentalAnimationApi")
        compilerOptions.optIn.add("androidx.compose.animation.ExperimentalSharedTransitionApi")
        compilerOptions.optIn.add("androidx.compose.foundation.ExperimentalFoundationApi")
        compilerOptions.optIn.add("androidx.compose.foundation.layout.ExperimentalLayoutApi")
        compilerOptions.optIn.add("kotlin.uuid.ExperimentalUuidApi")
        compilerOptions.optIn.add("kotlin.time.ExperimentalTime")
        compilerOptions.optIn.add("kotlinx.coroutines.ExperimentalCoroutinesApi")
        // ExperimentalNavigation3Api was renamed/removed in newer navigation3 — opt-in is
        // no longer required and the marker class no longer exists in the runtime artifact.
    }
}

composeCompiler {
    stabilityConfigurationFiles.add(
        project.layout.projectDirectory.file("compose_compiler_config.conf")
    )
}

tasks.register("buildAll") {
    dependsOn("assembleRelease", "bundleRelease")
    description = "Build both APK and AAB"
}

tasks.register("compileP5ManagedDeviceVerificationSources") {
    group = "verification"
    description = "Compile P5 disposable-emulator test sources without downloading/running a GMD or assembling a final APK"
    dependsOn("compileDebugAndroidTestKotlin")
}

tasks.register("p5DisposableManagedDeviceInstructions") {
    group = "verification"
    description = "Print the explicit disposable-emulator P5 test entry; never targets connected/Honor devices"
    doLast {
        logger.lifecycle("Disposable emulator task: p5DisposablePixel6Api35DebugAndroidTest")
        logger.lifecycle(
            "Host artifact: build/outputs/managed_device_android_test_additional_output/" +
                "debug/p5DisposablePixel6Api35/p5-production-eval-redacted.txt",
        )
        logger.lifecycle("Honor AAK-AN00 / connectedAndroidTest: PROHIBITED")
    }
}

// Dedicated JVM gate: it compiles/tests the fixed offline production-component adapters only.
// It has no assemble/bundle/connected/GMD dependency and publishes one bounded redacted report.
afterEvaluate {
    tasks.register<Test>("p5ProductionEvaluationGate") {
        group = "verification"
        description = "Run frozen P5 component regression and publish the fail-closed decision"
        dependsOn("compileDebugUnitTestKotlin", "processDebugUnitTestJavaRes")
        val debugUnitTest = tasks.named<Test>("testDebugUnitTest")
        testClassesDirs = debugUnitTest.get().testClassesDirs
        classpath = debugUnitTest.get().classpath
        filter {
            includeTestsMatching(
                "me.rerere.rikkahub.learning.eval.ProductionLearningEvaluationCiTest",
            )
        }
        maxParallelForks = 1
        maxHeapSize = "192m"
        jvmArgs("-XX:+UseSerialGC")
        val redactedOutput = layout.buildDirectory.file(
            "reports/agent-learning/p5-production-eval-redacted.txt",
        )
        outputs.file(redactedOutput)
        systemProperty("rikkahub.p5.eval.output", redactedOutput.get().asFile.absolutePath)
    }
}

tasks.withType<Test>().configureEach {
    // This repository's Android/KSP graph already occupies most of the Windows commit limit.
    // Keep unit tests deterministic on low-pagefile machines instead of allowing Gradle to
    // fork multiple 512 MiB G1 heaps that fail before a single test executes (errno 1455).
    maxParallelForks = 1
    maxHeapSize = "192m"
    jvmArgs("-XX:+UseSerialGC")
}

ksp {
    arg("room.schemaLocation", "$projectDir/schemas")
}

kotlin {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

dependencies {
    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.lifecycle.runtime.ktx)
    implementation(libs.androidx.lifecycle.process)
    implementation(libs.androidx.work.runtime.ktx)
    implementation(libs.androidx.browser)
    implementation(libs.androidx.webkit)
    implementation(libs.androidx.profileinstaller)
    implementation(libs.termux.terminal.view)
    implementation(libs.guava.listenablefuture)

    // Compose
    implementation(libs.androidx.activity.compose)
    implementation(platform(libs.androidx.compose.bom))
    implementation(libs.androidx.ui)
    implementation(libs.androidx.ui.graphics)
    implementation(libs.androidx.ui.tooling.preview)
    implementation(libs.androidx.material3)
    implementation(libs.androidx.material3.adaptive)
    implementation(libs.androidx.material3.adaptive.layout)

    // Navigation 3
    implementation(libs.androidx.navigation3.runtime)
    implementation(libs.androidx.navigation3.ui)
    implementation(libs.androidx.lifecycle.viewmodel.navigation3)
    implementation(libs.androidx.material3.adaptive.navigation3)


    // DataStore
    implementation(libs.androidx.datastore.preferences)

    // Image metadata extractor
    // https://github.com/drewnoakes/metadata-extractor
    implementation(libs.metadata.extractor)

    // Haze (background blur)
    implementation(libs.haze)
    implementation(libs.haze.blur)
    implementation(libs.haze.blur.materials)

    // koin
    implementation(platform(libs.koin.bom))
    implementation(libs.koin.android)
    implementation(libs.koin.compose)
    implementation(libs.koin.androidx.workmanager)

    // jetbrains markdown parser
    implementation(libs.jetbrains.markdown)

    // okhttp
    implementation(libs.okhttp)
    implementation(libs.okhttp.sse)
    implementation(libs.retrofit)
    implementation(libs.retrofit.serialization.json)

    // ktor client
    implementation(libs.ktor.client.core)
    implementation(libs.ktor.client.okhttp)
    implementation(libs.ktor.client.content.negotiation)
    implementation(libs.ktor.serialization.kotlinx.json)

    // ucrop
    implementation(libs.ucrop)

    // pebble (template engine)
    implementation(libs.pebble)

    // java-diff-utils (unified diff)
    implementation(libs.diffutils)

    // coil
    implementation(libs.coil.compose)
    implementation(libs.coil.gif)
    implementation(libs.coil.okhttp)
    implementation(libs.coil.svg)
    implementation(libs.coil.cache.control)

    // serialization
    implementation(libs.kotlinx.serialization.json)

    // zxing
    implementation(libs.zxing.core)

    // quickie (qrcode scanner)
    implementation(libs.quickie.bundled)
    implementation(libs.barcode.scanning)
    implementation(libs.androidx.camera.core)

    // Room
    implementation(libs.androidx.room.runtime)
    implementation(libs.androidx.room.ktx)
    implementation(libs.androidx.room.paging)
    ksp(libs.androidx.room.compiler)

    // Paging3
    implementation(libs.androidx.paging.runtime)
    implementation(libs.androidx.paging.compose)

    // Apache Commons Text
    implementation(libs.commons.text)

    // Toast (Sonner)
    implementation(libs.sonner)

    // Reorderable (https://github.com/Calvin-LL/Reorderable/)
    implementation(libs.reorderable)

    // lucide icons
    implementation(libs.lucide.icons)
    implementation(libs.huge.icons)

    // image viewer
    implementation(libs.image.viewer)

    // JLatexMath
    // https://github.com/rikkahub/jlatexmath-android
    implementation(libs.jlatexmath)
    implementation(libs.jlatexmath.font.greek)
    implementation(libs.jlatexmath.font.cyrillic)

    // Structured shell/root bridge through Shizuku or Sui.
    implementation(libs.shizuku.api)
    implementation(libs.shizuku.provider)

    // mcp
    implementation(libs.modelcontextprotocol.kotlin.sdk)

    // jmDNS (mDNS/Bonjour for .local hostname)
    implementation(libs.jmdns)

    // SLF4J Android binding — routes Ktor/SLF4J logs to logcat
    implementation(libs.slf4j.api)
    implementation(libs.slf4j.android)

    // sqlite-android (requery SQLite for Android)
    implementation(libs.sqlite.android)

    // Google Play Services Location (FusedLocationProvider)
    implementation("com.google.android.gms:play-services-location:21.3.0")
    // kotlinx.coroutines.tasks.await for Task<*> (was previously transitive via Firebase)
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-play-services:1.11.0")

    // AndroidX Biometric (BiometricPrompt)
    implementation("androidx.biometric:biometric:1.2.0-alpha05")

    // AndroidX Media — MediaSessionCompat, MediaButtonReceiver, NotificationCompat.MediaStyle
    implementation("androidx.media:media:1.7.0")

    // AndroidX DocumentFile — Phase 25 SAF tree traversal for the ExternalStorage tools
    // (USB / SD / Downloads / cloud DocumentsProvider access via persisted tree grants).
    implementation("androidx.documentfile:documentfile:1.0.1")

    // modules
    implementation(project(":ai"))
    implementation(project(":local-llm"))
    implementation(project(":web"))
    implementation(project(":document"))
    implementation(project(":highlight"))
    // eval_javascript remains an app capability; syntax highlighting no longer owns QuickJS.
    implementation(libs.quickjs)
    implementation(project(":search"))
    implementation(project(":speech"))
    implementation(project(":common"))
    implementation(project(":material3"))
    implementation(project(":workspace"))
    implementation(fileTree(mapOf("dir" to "libs", "include" to listOf("*.jar", "*.aar"))))
    implementation(kotlin("reflect"))

    // SSH client (Mwiede fork — maintained, Android-friendly)
    implementation("com.github.mwiede:jsch:0.2.21")

    // Cron utilities (expression parsing & validation)
    implementation("com.cronutils:cron-utils:9.2.1")

    // Leak Canary
    // debugImplementation(libs.leakcanary.android)

    // tests
    testImplementation(libs.junit)
    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.androidx.espresso.core)
    androidTestImplementation(platform(libs.androidx.compose.bom))
    androidTestImplementation(libs.androidx.ui.test.junit4)
    androidTestImplementation(libs.androidx.room.testing)
    androidTestUtil("androidx.test.services:test-services:1.6.0")
    debugImplementation(libs.androidx.ui.tooling)
    debugImplementation(libs.androidx.ui.test.manifest)
}
