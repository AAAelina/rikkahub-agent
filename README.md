<div align="center">

<img src="docs/icon.png" width="96" height="96" alt="RikkaHub Agent" />

# RikkaHub Agent

### 让一个真正属于你的 AI 助手常驻 Android

**不只会聊天，而是会陪伴、记忆、学习并操作设备。**

*Your assistant, always present.*

<p>
  <a href="https://github.com/AAAelina/rikkahub-agent/releases"><img src="https://img.shields.io/github/v/release/AAAelina/rikkahub-agent?include_prereleases&style=flat-square&label=release&color=blue" alt="Release" /></a>
  <a href="https://github.com/AAAelina/rikkahub-agent/releases"><img src="https://img.shields.io/github/downloads/AAAelina/rikkahub-agent/total?style=flat-square&color=brightgreen" alt="Downloads" /></a>
  <a href="https://github.com/AAAelina/rikkahub-agent/stargazers"><img src="https://img.shields.io/github/stars/AAAelina/rikkahub-agent?style=flat-square&color=yellow" alt="Stars" /></a>
  <img src="https://img.shields.io/badge/platform-Android%208%2B-3DDC84?style=flat-square&logo=android&logoColor=white" alt="Android 8+" />
</p>

[下载 APK](https://github.com/AAAelina/rikkahub-agent/releases/latest) ·
[新增功能](#这个-fork-增加了什么) ·
[项目网站](https://aaaelina.github.io/rikkahub-agent/) ·
[安装说明](#下载与安装) ·
[构建说明](#从源码构建)

</div>

---

## 这是什么

本项目基于 [ExTV/RikkaHub Agent](https://github.com/ExTV/rikkahub-agent)，底层来自开源 Android AI 客户端 [RikkaHub](https://github.com/rikkahub/rikkahub)。它保留多模型聊天、Android Agent 工具、工作流、定时任务、MCP、Skills、Telegram、Termux、SSH、Codex OAuth 等基础能力。

这个 Fork 的重点不是继续堆叠通用工具，而是把一个助手真正固定在设备上：它可以通过 **系统 AI 键、桌宠和悬浮截屏** 随时出现，拥有受保护的 Owner 身份，并通过 **Dreaming-X** 与 **Agent Learning Runtime** 逐步形成可检查、可纠正的长期理解和任务经验。

> 本项目为独立维护的个人增强分支。Fork 特有问题请提交到本仓库，不要反馈给上游项目。

---

## 这个 Fork 增加了什么

| 新增能力 | 作用 | 当前状态 |
| --- | --- | --- |
| **Owner Assistant** | 固定、受保护的主人助手，拥有独立 Authority、Epoch 和完整应用控制面 | 已实现 |
| **系统 AI 键对话** | 通过 Android 系统助手入口、实体 AI 键或音量键快捷方式呼出悬浮对话 | 已实现，受 OEM 限制 |
| **桌宠伴生助手** | 常驻桌面，支持短对话、动作状态、TTS、日记归档与安全任务转交 | 已实现 |
| **QuickCapture** | 悬浮按钮截取全屏或框选区域，自动发送到固定助手或带草稿打开会话 | 已实现 |
| **Dreaming-X** | 以独立 Experience Ledger 形成用户、助手与关系三画像，支持证据审查、纠正和助手自查 | 实验性 |
| **Agent Learning Runtime** | 从任务结果形成有证据的候选经验，经 Shadow 与人工审核后作为上下文建议 | Shadow / 审核阶段 |
| **Secret Vault** | 使用 Android Keystore、AES-GCM 与强生物识别保护 Provider、TTS、ASR、MCP 凭据 | 已实现 |
| **跨会话与运行控制** | 按需读取其他会话，并在生成过程中追加指令、取消、继续或恢复任务 | 已实现 |

## 实机界面

<p align="center">
  <a href="docs/img/system-assistant-ai-key.jpg"><img src="docs/img/system-assistant-ai-key.jpg" width="31%" alt="系统 AI 键悬浮对话" /></a>
  <a href="docs/img/desktop-pet.jpg"><img src="docs/img/desktop-pet.jpg" width="31%" alt="第二用户桌宠设置" /></a>
  <a href="docs/img/memory-dream.jpg"><img src="docs/img/memory-dream.jpg" width="31%" alt="记忆中心与 Dream" /></a>
</p>

<p align="center">
  <sub>系统 AI 键悬浮对话　·　第二用户桌宠　·　记忆中心与 Dream</sub>
</p>

> 截图来自当前开发构建，具体界面可能随版本继续调整。点击图片可以查看原图。

---

## 不只在聊天页面

### 系统 AI 键对话

RikkaHub Agent 可以注册为 Android 系统助手，把设备的助手入口直接连接到固定 Owner Assistant：

- 从导航助手手势、长按电源键或 OEM AI 键呼出悬浮对话。
- 在 MagicOS 等限制较多的系统上，可使用双音量键无障碍快捷方式。
- 请求进入固定第二用户会话，后台任务继续在原会话中运行。
- 悬浮层可以查看生成、排队、工具执行、授权等待与完成状态。
- 涉及屏幕操作时，悬浮层会先退出，避免遮挡目标应用。
- Emergency Stop、设备锁定状态和既有安全门仍然有效。

这里的 **系统 AI 键** 是本 Fork 的系统助手入口；它与 ExTV 提供的 **Agent Keyboard 输入法工具**不是同一个功能。

### 桌宠伴生助手

桌宠不是贴在屏幕上的普通聊天气泡，而是绑定 Owner Assistant 的受限伴生运行时：

- 独立的短对话与触摸反馈。
- 根据空闲、思考、工具执行、成功、失败等状态切换动作。
- 支持 TTS 播放状态与语音驱动表现。
- 将短对话归档为日记或可回顾记录。
- 复杂任务通过 Handoff 转交给 Owner Assistant。
- 桌宠自身不继承 Owner 工具权限，任务转交也不会绕过原有执行门。
- 支持导入桌宠包、全局选择、缩放、替换与删除。

相关实现位于 [pet](app/src/main/java/me/rerere/rikkahub/pet)。

### 悬浮截屏提问

QuickCapture 用一个常驻悬浮按钮把屏幕内容直接交给固定助手：

1. 点击悬浮按钮截取当前屏幕。
2. 选择全屏或手动框选区域。
3. 自动附带预设问题并发送到当前第二用户会话。
4. 关闭自动发送时，打开会话并保留图片与问题草稿。
5. 长按可以收集多张截图，双击取消批次或打开菜单。
6. 随时跳回当前会话查看任务进度。

预览截图只保存在内存中；安全诊断不会记录截图、问题正文和文件路径。相关实现位于 [quickcapture](app/src/main/java/me/rerere/rikkahub/quickcapture)。

---

## Owner Assistant

Owner Assistant 是这个 Fork 的核心。它不是一个临时获得更多工具的普通助手，而是一组被本机确认、受身份状态保护的固定关系：

- 一个全局固定的 Assistant 与 Conversation。
- Authority 与 Epoch 绑定运行身份、授权、队列和托管执行。
- 删除、清空、重置、助手移除和异常恢复路径不会误删固定身份。
- 重新分配 Owner 前，旧 Epoch 的授权、队列、批准与执行会先被撤销。
- 每个操作执行前重新核验 Authority、Epoch、会话与来源。
- Owner Assistant 不可自行解除 Emergency Stop，也不能删除或转移自己的固定身份。

### 完整应用控制面

Owner 控制面包含 **24 个工具族、167 个 Action**，覆盖：

- Assistant、Conversation、Provider、模型路由与 Secret Vault
- TTS、ASR、Search、Web 与 Telegram Channel
- MCP、Skills、插件、Workflow、定时任务和闹钟
- Workspace、Termux、SSH、Shizuku 与本地服务
- QuickCapture、桌宠、Memory、Prompt、Lorebook 和快捷消息
- App 设置、运行参数、权限、Doctor、备份、任务取消与恢复

一次 Owner 工具调用可以按顺序携带 1–20 个 Action，用较小的工具 Schema 完成组合配置。完整能力表与边界见 [Owner 控制面审计](docs/owner-complete-app-control-audit.md)。

---

## Dreaming-X

Dreaming-X 是独立于普通 Memory 的离线认知层。它不替代原始聊天或已保存记忆，而是围绕“当前用户 × 当前助手”建立稳定的 `DreamPairScope`，把双方共同经历整理成可审查、可纠正、可重建的长期画像。

### 独立 Experience Ledger

Dream 不再由 `useGlobalMemory` 或 Memory Epoch 决定作用域和运行时钟。当前实现使用独立的 `dream_experiences` 与 `dream_experience_state`：

- 连续聊天回合通过 Conversation Episode Adapter 形成经历。
- 已确认 Memory、Narrative Event、Insight 与 Theory 可作为普通来源接入。
- 用户明确纠正或拒绝会立即退出相关 Claim，并写入高权重 Experience。
- Ledger 保存稳定的来源引用、摘要与 Digest，不重复保存大段聊天正文。
- 新经历出现时旧画像仍可使用，同时显示 Pending、Lag 与 Dream Debt。

### 用户、助手与关系三画像

一次 Dream Synthesis 会共同生成三部分 Pair Portrait：

| 画像 | 内容 |
| --- | --- |
| `about_user` | 用户的长期特征、偏好、项目、计划与约束 |
| `about_assistant` | 助手从共同经历中形成的 Learned Self Model，不覆盖用户配置的 Identity Kernel |
| `about_relationship` | 双方的相处方式、共同经历、信任边界与关系变化 |

每条 Claim 都带有 `subjectKind`、`profileSection`、`epistemicOrigin`、`contentType`、置信度、来源与版本信息。用户可以在记忆中心查看三张画像卡、证据与版本差异，并纠正或 Reject 错误理解。

### 助手可以查看自己的 Dream

当前助手会获得只读的 `dream_view` 工具，并自动限定到自己的 Pair Scope：

- `summary`：三画像摘要、Profile Revision 与最近 Dream 时间。
- `claims`：当前用户、助手与关系 Claims。
- `recent`：最近吸收的 Experience 与画像变化。
- `sources`：指定 Claim 的 Episode、Message 与来源引用。

`dream_view` 同时返回 Pending Experience、Dream Debt 与 Lag，让助手知道是否还有未处理的经历。它不能查看其他助手的 Dream，也不能修改、删除或强制生成 Dream。

Dream 使用独立模型入口，并继续复用 WorkManager、Lease、Retry、预算以及空闲/充电/网络条件。当前 Room Schema 为 v50；功能仍处于实验性迭代阶段。相关实现位于 [memory/dreaming](app/src/main/java/me/rerere/rikkahub/memory/dreaming)，只读接口见 [DreamIntrospectionTools.kt](app/src/main/java/me/rerere/rikkahub/memory/dreaming/runtime/DreamIntrospectionTools.kt)。

---

## Agent Learning Runtime

Agent Learning Runtime（ALR）负责学习“以后怎样把任务做得更好”，但不负责改写“用户是谁”。

它与 Dreaming-X 分工明确：

| 系统 | 负责 | 不负责 |
| --- | --- | --- |
| Dreaming-X | 用户画像、助手自我理解与关系上下文 | 自动生成任务执行权限 |
| Agent Learning | 有证据的任务经验与上下文建议 | 改写个人记忆、系统提示词或工具权限 |

ALR 的第一阶段支持：

- Capture Only
- Candidate Shadow
- Retrieval Shadow
- Reviewed Policy Opt-in

学习结果默认不会改变正常回答。候选建议需要经过 Shadow、证据检查和人工审核，批准后也只是助手级或 Authority Subject 级的非可信上下文建议，不会成为系统指令，也不会授予文件、网络、Android 或工具权限。

相关实现位于 [learning](app/src/main/java/me/rerere/rikkahub/learning)，设计边界见 [ALR Authority Scope](docs/adr/2026-08-12-agent-learning-authority-scope.md)。

---

## 跨会话、上下文与运行控制

### 跨会话读取

Owner Assistant 可以在用户启用后列出、读取和搜索其他会话。读取按命令执行，只允许受保护的本地来源，并受设备解锁、调用次数和文本长度限制；结果仅供当前任务使用，不会因此写入长期记忆或额外持久化。

### 实时运行干预

- 生成过程中追加 Steering 消息。
- 稳定保留 Command Identity。
- 取消正在生成或执行工具的任务。
- 对失败和中断任务继续、重试或恢复。
- 在系统助手、桌宠 Handoff、QuickCapture、Telegram 与主聊天之间保持同一权威队列。

### 上下文与 OCR

- 更稳定的上下文回合选择与预算控制。
- OCR 附件和大体积多模态请求处理。
- 请求大小与 Token 使用诊断。
- 导入恢复、全文搜索和定制路径的兼容修复。

---

## Secret Vault 与权限边界

敏感凭据不应被当作普通聊天内容或设置字符串处理：

- Provider、TTS、ASR 与 MCP 凭据可存入本地 Vault。
- 使用 Android Keystore 与 AES-GCM 加密，默认排除在备份之外。
- 查看或编辑明文需要 BIOMETRIC_STRONG。
- 本地适配器只获得作用域受限、短时有效的内存 Lease。
- 明文不会写入 Room、日志、Doctor、备份或普通模型工具结果。
- Owner、桌宠、Dreaming-X 与 Agent Learning 各自保留独立权限边界。
- HARDLINE、系统权限、来源审批与 Emergency Stop 继续生效。

安全能力默认按需启用。Owner Authority 需要在本机完成选择与确认后才会激活。

---

## 与上游的关系

| 项目 | 本项目使用的基础 |
| --- | --- |
| [RikkaHub](https://github.com/rikkahub/rikkahub) | Android 聊天客户端、Provider、多模型、多模态和基础数据结构 |
| [ExTV/RikkaHub Agent](https://github.com/ExTV/rikkahub-agent) | Android Agent 工具、浏览器、Telegram、工作流、定时任务、MCP、Skills、Sub-agent、Codex OAuth 与 Agent Keyboard |
| **本 Fork** | Owner Assistant、系统 AI 键入口、桌宠伴生运行时、QuickCapture、Dreaming-X、Agent Learning、Vault 与定制运行控制 |

需要特别说明：

- **Codex OAuth** 与 ChatGPT 账号登录能力来自 ExTV 上游。
- **Agent Keyboard** 的安全输入、读取和光标操作来自 ExTV 的配套项目。
- 本 Fork 的 **系统 AI 键对话** 是 Android 系统助手悬浮入口，不等同于 Agent Keyboard。
- 通用的 Memory、Skills、MCP、Sub-agent 和 Workflow 并非本 Fork 独有。

<details>
<summary><strong>查看继承的基础能力</strong></summary>

- OpenAI、Google、Anthropic、OpenRouter、Ollama 与 OpenAI-compatible Provider
- Codex ChatGPT OAuth、多账号与本地模型
- Android 点击、滑动、输入、截图、通知、传感器、联系人、短信、NFC 与文件工具
- 内置浏览器、网页提取和浏览器迷你聊天
- Telegram Bot、Termux、SSH/SFTP 与本地服务
- Workflow、定时任务、闹钟与外部 Intent 触发
- MCP、Skills、插件、Sub-agent、Doctor 与诊断
- 音乐媒体、TTS、ASR、备份、导入和多语言界面

这些能力的完整说明请查看对应上游仓库。

</details>

---

## 功能状态与限制

| 项目 | 说明 |
| --- | --- |
| Dreaming-X | 实验性功能；Pair Portrait 与 Experience 派生状态可以重建，不替代原始聊天和记忆 |
| Agent Learning | 当前以 Shadow 与人工审核为主，Policy 注入默认关闭 |
| 系统 AI 键 | 不同 OEM 对系统助手、长按电源键和 AI 键的开放程度不同 |
| MagicOS 快捷入口 | 可使用双音量键无障碍快捷服务；该服务不能读取窗口、执行手势或截图 |
| 桌宠与 QuickCapture | 需要 Android 悬浮窗权限；截屏后端可能需要无障碍或 MediaProjection 授权 |
| Secret Vault | 需要设备支持强生物识别 |
| 完整数据库恢复 | 创建本地备份已支持；需要冷启动替换数据库的完整恢复路径仍需用户操作 |
| Release 签名 | 更换 APK 签名无法覆盖安装，升级前请先备份数据 |

---

## 下载与安装

1. 从 [Releases](https://github.com/AAAelina/rikkahub-agent/releases/latest) 下载最新 APK。
2. 安装并添加至少一个 LLM Provider；也可以通过 ChatGPT OAuth 登录 Codex。
3. 创建或选择一个 Assistant，在本机确认其 Owner / 第二用户身份。
4. 按需启用系统助手、桌宠、QuickCapture、Dream 或 Agent Learning。
5. 只授予实际需要的 Android 权限和工具分类。

### 从旧 Agent 版本迁移

从早于 `2.3.1-agent.0` 的构建升级时请注意：应用 ID 已改为 `excp.rikkahub`，会与上游 RikkaHub 并存，Android 不会把它当作旧版本的覆盖更新。

迁移步骤：

1. 在旧应用中进入 Settings → Backup 创建备份。
2. 安装本 Fork。
3. 在新应用中恢复备份。
4. 确认数据无误后再卸载旧版本。

---

## 运行要求

| 项目 | 要求 |
| --- | --- |
| Android | 8.0+（API 26），目标 API 37 |
| 架构 | arm64 或 x86_64 |
| 存储空间 | 约 80 MB 起，不含模型与缓存 |
| 模型服务 | OpenAI、Google、Anthropic、OpenRouter、Codex、Ollama 或 OpenAI-compatible |
| 可选组件 | Agent Keyboard、Termux、Telegram、MCP Server、本地模型 |

---

## 从源码构建

构建需要 JDK、Android SDK，以及 PATH 中可用的 [Bun](https://bun.sh)。Gradle 会先在 `web-ui/` 中运行 Bun 构建，再打包 APK。

```bash
git clone https://github.com/AAAelina/rikkahub-agent.git
cd rikkahub-agent

./gradlew test
./gradlew :app:assembleDebug
```

默认分支为 `master`。Material Color Utilities 已随 `material3` 模块一同提供，不需要额外初始化子模块。

---

## 致谢

| 项目 | 作用 |
| --- | --- |
| [RikkaHub](https://github.com/rikkahub/rikkahub) | 上游 Android AI 客户端 |
| [ExTV/RikkaHub Agent](https://github.com/ExTV/rikkahub-agent) | Agent 能力与主要 Fork 基础 |
| [Agent Keyboard](https://github.com/ExTV/agent-keyboard) | 基于 FlorisBoard 的安全输入法配套 |
| [Termux](https://github.com/termux/termux-app) | Android Linux 与命令执行环境 |
| [whisper.cpp](https://github.com/ggerganov/whisper.cpp) | 本地语音转写 |
| [JSch](https://github.com/mwiede/jsch) | SSH/SFTP 客户端 |
| [cron-utils](https://github.com/jmrozanec/cron-utils) | Cron 表达式解析 |

本 Fork 与 RikkaHub 官方维护者无隶属关系。底层客户端、Provider 抽象和原始界面设计的权利与贡献归上游项目及其贡献者所有。

---

## 许可证

许可证继承自 [RikkaHub](https://github.com/rikkahub/rikkahub)，详情见 [LICENSE](LICENSE)。
