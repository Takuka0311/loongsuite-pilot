# Rust 发布与测试优先级

## 本轮交付顺序与边界

先更新文档，再交付自动发布 CI，随后分批提交生产代码、脚本和文档，最后迁移独立测试。内嵌 `#[cfg(test)]` 模块及编译必需 fixture 随所属源码提交，不为拆分提交破坏源码闭包。

推送前必须通过 `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`。不降低 lint 等级、不添加全局 allow、不使用 `continue-on-error` 把诊断变成绿色。Git 推送目标为 `origin/loongsuite-pilot-rust`，不推 main，不自动创建 tag 或实际 Release。

## 自动发布方案

- `.github/workflows/release.yml`：`v*` tag 触发正式发布；手动运行仅 dry-run，不创建 tag、Release，也不更新渠道。
- 固定八平台：Linux x64/ARM64 的 GNU、musl，macOS Intel/Apple Silicon，Windows x64/ARM64 MSVC。每个平台 1 个构建任务，加 1 个汇总/发布任务，共 9 个 runner 任务，不因失败自动缩小矩阵。
- Cargo 是版本唯一来源，工具链固定 Rust 1.91.1；tag 必须等于 `v<Cargo version>`，来源提交必须属于 main/master/loongsuite-pilot-rust 中的受信任分支。
- 公开构建不带 proprietary feature。每包仅包含相应平台二进制、白名单资源及 `runtime-manifest.json`。macOS 必须携带对应架构 Swift UI。
- `xtask package` 在对应 OS/架构执行包内 `--version`、`probe --build-edition`、`check --json`；隔离 HOME 和 PATH，核对执行前后文件未被修改。
- 汇总必须检查八个平台集合、版本、提交、edition、归档格式和 SHA-256；拒绝 dirty 候选、缺包、重复包或预检失败，不重新编译已验收的发行包。
- 正式发布使用 `release` Environment，手动使用 `release-dry-run`；写权限仅限汇总/发布 job（GitHub 权限按 job 生效），只有正式发布步骤显式传入 `GH_TOKEN`，dry-run 不调用远端写入。发布前及公开前核对远端 tag 实际指向的提交。先创建 draft，上传并核对完整资产，再公开。已有版本不覆盖；失败 draft 保留供检查，不静默补写可能不一致的文件。
- stable 与 prerelease 分开：预发布不得成为 latest，也不得更新稳定清单。工作流按仓库串行发布，不取消正在上传的版本；旧版本不得覆盖更新的 stable。
- 本次不接入 OSS 上传、不自动 bump Cargo 版本、不自动 commit/tag，不切换用户安装，不恢复真实自动更新。首次 Rust 安装使用显式平台包地址，见[安装指南](installation.md#rust-发行包与平台)。

`deploy/package-opensource.sh` 和 `deploy/release-opensource.sh` 仅转发对应 `xtask` 参数。打包前先构建目标二进制，再传 `--target`、`--binary`、`--commit`；旧 `--skip-build`、`-o <archive>` 不再支持。发布默认只准备本地资产，需要 `--packages`、`--output`、`--commit`、`--repository`；旧自动升版/OSS 参数不再支持，正式发布还需显式 `--tag` 和 `--publish`。

正式资产为八个 `loongsuite-pilot-<version>-<target>` 归档、安装脚本、`SHA256SUMS`、轻门禁摘要，以及 stable 的 `latest-rust.json`（预发布使用独立文件）。清单由当前候选包生成，不接受旧 Node 通用包。

这是一条“构建与包预检”发布门禁，不是完整 Agent 注入或安装生命周期验收。平台工作流尚待 GitHub 实跑；不得把工作流文件存在、Linux 测试通过或无运行时 PATH 预检称作八平台全部验收完成。系统签名、notarization、GUI/真实 Agent 会话不在本轮轻门禁内。

## 日常 CI 预算

日常 PR、main/master/loongsuite-pilot-rust push 和手动 CI：目标为整个工作流 10 分钟内完成，不是每个矩阵任务各跑 10 分钟。使用一个 Linux 质量任务，现有 Secret Scan 独立任务保持不变；总数远低于 10。不在日常 CI 重复八平台 release 编译或新增逐 Agent runner。

日常阻断项：

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked --all-features
```

默认 feature 的构建与现有测试放到发布的 Linux GNU 任务执行；独立测试尚未提交时不能声称它们已在远端运行。后续纳入 Cargo 测试时保持同一个质量任务，不逐测试扩张 runner。

10 分钟是预算与超时上限，不是尚未实测的性能保证。冷缓存、排队及依赖下载可能超出墙钟目标。先记录冷/热缓存实际耗时；超时则 CI 失败，不能直接跳过失败步骤。优先优化 Cargo 缓存、编译配置和重复 feature 编译，超预算仍无法解决时交由维护者决定下面的裁剪项。

发布构建单独计时，不受日常 10 分钟预算限制，但每次最多 9 个 runner 任务；每个构建有有限超时。不能用拆分 workflow 绕过 runner 预算。

## 测试优先级与可裁剪范围

| 优先级 | 内容 | 当前安排 | 裁剪风险 / 决策 |
|---|---|---|---|
| P0 | fmt、严格 clippy、Rust 编译、已有核心单元测试 | 日常阻断；不删已有快测 | 编译失败、类型错误和主要回归；不建议裁剪 |
| P0 | 八平台包完整性、版本/提交/摘要、包内原生预检 | 发布阻断，8 个构建内执行 | 错平台包或损坏包进入更新渠道；不建议裁剪 |
| P0 | 安装/更新事务、配置保护、checkpoint、脱敏、Hook 协议、真实 OTLP 编解码的现有 Rust 断言 | 保留；独立测试随后分批提交 | 数据丢失、破坏用户配置或泄漏；优先迁移，不按数量删减 |
| P1 | 默认与 all-features 的重复全量测试 | 日常 all-features，发布 Linux 跑默认 | 两种构建仍有覆盖；若改为周期运行需维护者确认 |
| P1 | Windows/macOS 专属路径、服务、编码、worker 测试 | 后续按平台归组，不逐 Agent 建 runner | Linux 无法证明平台行为；暂缓不是验收通过 |
| P1 | 全部 Agent 的注入、重装、清理和失败矩阵 | 已同意暂缓新增到 CI | 轻门禁不保证每个 Agent 的跨平台配置注入 |
| P2 | Docker/远程真实会话、宿主插件加载、GUI、长时间稳定性和性能 | 本地或后续手动验收，不进日常 CI | 不能声明真实采集、桌面或长期稳定性已验证 |
| P2 | 历史断言中的重复 fixture、纯脚本文本匹配 | 迁移时归并同一行为，记录映射，不机械逐文件复制 | 仅在确认有等价 Rust 断言后删除冗余 |

最近已记录的历史审计为 391 个文件：6 完整映射、15 部分映射、370 未映射。这是待比对的文件数，不是失败测试或已知 bug 数。最新本地主库 1,249 项通过不等于旧断言全部承接，也不代表 GitHub 耗时。

`tests/fixtures/rust-migration-map.json` 保留覆盖缺口，`migration-audit --require-complete` 在未完整映射时继续失败；本轮暂不把该完整性门禁接入轻发布，也不据此删除旧测试/工具。需要进一步裁剪时，提交实际耗时和行为覆盖变化，由维护者决定，不能仅凭测试数目裁剪。

## 提交批次与验证

1. 文档：README、中英文安装/Trace、项目导航和本文。
2. 原生运行链路：Cargo/lock/toolchain、Rust 生产模块及其内嵌测试、必要 fixture；独立集成测试先留在工作区。
3. 运行脚本与资源：安装、服务、Hook 薄入口及必需资源；不清理尚未承接的历史工具。
4. 发布工具与 CI：Rust 打包/发布校验工具、薄脚本、八平台发布和日常质量工作流。
5. 独立测试：现有 Rust 集成测试与迁移映射分批提交，再按上表补齐旧行为。

推送前检查暂存文件清单、敏感内容、编译必需 `include_str!` / `include_bytes!` 引用及工作区以外依赖；从提交树验证构建和门禁，不能只依赖包含未提交 fixture 的工作区测试结果。提交历史可分批，本轮全部主要批次准备好且严格 clippy 清零后再推送，避免远端收到缺依赖的中间状态。

## 管理员接入与验收记录

需要确认 GitHub-hosted 八种 runner 可用、Actions 权限、`release` Environment 保护以及分支保护检查名。仅工作流代码不能配置仓库管理员设置。正式发布仍需单独批准创建 tag；手动 dry-run 不发布。

验收应记录 GitHub run URL、提交 SHA、冷/热缓存耗时、runner 任务数、八个平台包摘要及明确暂缓项。目前未填写成功运行证据，不宣称 CI 已通过。
