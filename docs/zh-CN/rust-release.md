# Rust 发布与测试优先级

## 本轮交付顺序与边界

先更新文档，再交付自动发布 CI，随后分批提交生产代码、脚本和文档，最后迁移独立测试。内嵌 `#[cfg(test)]` 模块及编译必需 fixture 随所属源码提交，不为拆分提交破坏源码闭包。

推送前必须通过 `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`。不降低 lint 等级、不添加全局 allow、不使用 `continue-on-error` 把诊断变成绿色。Git 推送目标为 `origin/loongsuite-pilot-rust`，不推 main，不自动创建 tag 或实际 Release。

## 自动发布方案

- `.github/workflows/release.yml`：`v*` tag 触发正式发布；手动运行仅 dry-run，不创建 tag、Release，也不更新渠道。
- 固定八平台：Linux x64/ARM64 的 GNU、musl，macOS Intel/Apple Silicon，Windows x64/ARM64 MSVC。每个平台 1 个构建任务，加 1 个汇总/发布任务，共 9 个 runner 任务，不因失败自动缩小矩阵。
- Cargo 是版本唯一来源，工具链固定 Rust 1.91.1；tag 必须等于 `v<Cargo version>`，来源提交必须属于 main/master/loongsuite-pilot-rust 中的受信任分支。
- 公开构建不带 proprietary feature。每包仅包含相应平台二进制、白名单资源及 `runtime-manifest.json`。macOS 必须携带对应架构 Swift UI，Rust、C 依赖与 Swift 的最低系统版本统一为 macOS 13。
- `xtask package` 在对应 OS/架构执行包内 `--version`、`probe --build-edition`、`check --json`；隔离 HOME 和 PATH，核对执行前后文件未被修改。
- CI 候选包、汇总输入与发布输出放在 `RUNNER_TEMP` 下，按 run/attempt/target 隔离，不进入 Cargo `target` 缓存；包工具仍拒绝覆盖旧候选。`.sh` / `.ps1` 通过 `.gitattributes` 统一 LF，保持 Hook 模板逐字节校验。非 Linux 构建复用包、Hook、候选进程及 canonical 配置路径回归，不增加 runner。
- 汇总必须检查八个平台集合、版本、提交、edition、归档格式和 SHA-256；拒绝 dirty 候选、缺包、重复包或预检失败，不重新编译已验收的发行包。
- 正式发布使用 `release` Environment，手动使用 `release-dry-run`；写权限仅限汇总/发布 job（GitHub 权限按 job 生效），只有正式发布步骤显式传入 `GH_TOKEN`，dry-run 不调用远端写入。发布前及公开前核对远端 tag 实际指向的提交。先创建 draft，上传并核对完整资产，再公开。已有版本不覆盖；失败 draft 保留供检查，不静默补写可能不一致的文件。
- stable 与 prerelease 分开：预发布不得成为 latest，也不得更新稳定清单。工作流按仓库串行发布，不取消正在上传的版本；旧版本不得覆盖更新的 stable。
- 本次不接入 OSS 上传、不自动 bump Cargo 版本、不自动 commit/tag，不切换用户安装，不恢复真实自动更新。首次 Rust 安装使用显式平台包地址，见[安装指南](installation.md#rust-发行包与平台)。

`deploy/package-opensource.sh` 和 `deploy/release-opensource.sh` 仅转发对应 `xtask` 参数。打包前先构建目标二进制，再传 `--target`、`--binary`、`--commit`；旧 `--skip-build`、`-o <archive>` 不再支持。发布默认只准备本地资产，需要 `--packages`、`--output`、`--commit`、`--repository`；旧自动升版/OSS 参数不再支持，正式发布还需显式 `--tag` 和 `--publish`。

正式资产为八个 `loongsuite-pilot-<version>-<target>` 归档、安装脚本、`SHA256SUMS`、轻门禁摘要，以及 stable 的 `latest-rust.json`（预发布使用独立文件）。清单由当前候选包生成，不接受旧 Node 通用包。

这是一条“构建与包预检”发布门禁，不是完整 Agent 注入或安装生命周期验收。第五、六轮 GitHub dry-run 的八平台包及汇总均已通过，具体提交、日志与资产摘要见文末；不得把轻门禁通过称作八平台全部行为验收完成。系统签名、notarization、GUI/真实 Agent 会话不在本轮轻门禁内。文件采集与 WorkBuddy 的 inotify 唤醒仅限 Linux，非 Linux 使用既有轮询兜底，不代表已实现 macOS/Windows 原生文件监视。

## 日常 CI 预算

日常 PR、main/master/loongsuite-pilot-rust push 和手动 CI：目标为整个工作流 10 分钟内完成，不是每个矩阵任务各跑 10 分钟。使用一个 Linux 质量任务，现有 Secret Scan 独立任务保持不变；总数远低于 10。不在日常 CI 重复八平台 release 编译或新增逐 Agent runner。

日常阻断项：

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked --all-features
```

默认 feature 的构建与现有测试放到发布的 Linux GNU 任务执行。独立 Rust 测试已分两批提交，并在同一个日常质量任务内执行，不逐测试扩张 runner。

10 分钟是预算与超时上限，不是所有运行的性能保证。实测：独立测试提交前冷缓存为 3 分 22 秒；纳入独立测试后缓存命中为 2 分 45 秒，未命中缓存的完整 1,367 项回归为 2 分 49 秒；最终代码 `2d56ecd` 的 1,369 项回归缓存命中为 2 分 04 秒，均为 1 个 runner。冷缓存、排队及依赖下载可能超出墙钟目标；超时则 CI 失败，不能直接跳过失败步骤。优先优化 Cargo 缓存、编译配置和重复 feature 编译，超预算仍无法解决时交由维护者决定下面的裁剪项。

发布构建单独计时，不受日常 10 分钟预算限制，但每次最多 9 个 runner 任务；每个构建有有限超时。不能用拆分 workflow 绕过 runner 预算。

## 测试优先级与可裁剪范围

| 优先级 | 内容 | 当前安排 | 裁剪风险 / 决策 |
|---|---|---|---|
| P0 | fmt、严格 clippy、Rust 编译、已有核心单元测试 | 日常阻断；不删已有快测 | 编译失败、类型错误和主要回归；不建议裁剪 |
| P0 | 八平台包完整性、版本/提交/摘要、包内原生预检 | 发布阻断，8 个构建内执行 | 错平台包或损坏包进入更新渠道；不建议裁剪 |
| P0 | 安装/更新事务、配置保护、checkpoint、脱敏、Hook 协议、真实 OTLP 编解码的现有 Rust 断言 | 已保留；独立测试已分批提交并通过 Linux CI | 数据丢失、破坏用户配置或泄漏；优先迁移，不按数量删减 |
| P1 | 默认与 all-features 的重复全量测试 | 日常 all-features，发布 Linux 跑默认 | 两种构建仍有覆盖；若改为周期运行需维护者确认 |
| P1 | Windows/macOS 专属路径、服务、编码、worker 测试 | 包、Hook、候选进程与配置路径已实跑；其余后续按平台归组 | 完整平台生命周期仍暂缓；不能把轻回归外推为全部通过 |
| P1 | 全部 Agent 的注入、重装、清理和失败矩阵 | 已同意暂缓新增到 CI | 轻门禁不保证每个 Agent 的跨平台配置注入 |
| P2 | Docker/远程真实会话、宿主插件加载、GUI、长时间稳定性和性能 | 本地或后续手动验收，不进日常 CI | 不能声明真实采集、桌面或长期稳定性已验证 |
| P2 | 历史断言中的重复 fixture、纯脚本文本匹配 | 迁移时归并同一行为，记录映射，不机械逐文件复制 | 仅在确认有等价 Rust 断言后删除冗余 |

最近已记录的历史审计为 391 个文件：9 完整映射、14 部分映射、368 未映射。StateStore / SnapshotStore 两个历史文件的 28 个测试已逐项核对，全部由现有 Rust 断言承接；checkpoint 组 34 项回归耗时 0.04 秒，不新增测试或 runner，旧文件仍保留在提交树中。CP5 的单批/分批四个历史合同已合并为一个参数化 Rust 测试，本地 OTLP 集成组 4 项共耗时 0.27 秒，不新增 runner。这是待比对的文件数，不是失败测试或已知 bug 数。最终代码 `2d56ecd` 的本地、干净提交树及 GitHub Linux 回归已通过主库 1,252 项、xtask 单元 35 项、独立集成 82 项，共 1,369 项。相较早期 1,366 项，新增候选无后代正常退出、canonical 配置路径、HTTP 请求头分段读取三个回归；stdout JSON 诊断并入既有候选失败测试。另有 2 个 doc-tests ignored。Linux 中 Windows 专属测试为 0 项，不计作 Windows 通过；发布中的 Windows 轻回归单独记录。测试通过不等于旧断言全部承接。

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

已记录的 GitHub 实跑（2026-10-27～28）：

| 提交 | 工作流 / 证据 | 缓存、耗时与任务数 | 结论 |
|---|---|---|---|
| `e2c5002` | [日常 CI](https://github.com/Takuka0311/loongsuite-pilot/actions/runs/40482767167) | 冷缓存，3 分 22 秒，1 个任务 | 严格 lint 与 1,284 项单元测试通过；不含后提交的独立集成测试 |
| `7492d43` | [日常 CI](https://github.com/Takuka0311/loongsuite-pilot/actions/runs/40486204539) | 缓存命中，2 分 45 秒，1 个任务 | 严格 lint 与 1,366 项 Linux 测试通过 |
| `e2c5002` | [首次发布 dry-run](https://github.com/Takuka0311/loongsuite-pilot/actions/runs/40482807142) | 9 个任务 | Linux x64 GNU/musl 成功，其余六平台失败，汇总正确拒绝 |
| `f9aa186` | [日常 CI](https://github.com/Takuka0311/loongsuite-pilot/actions/runs/40487786813) | 2 分 21 秒，1 个任务 | 通过；此记录不作为冷缓存证据 |
| `7abac87` | [日常 CI](https://github.com/Takuka0311/loongsuite-pilot/actions/runs/40618596606) | 未命中缓存，2 分 49 秒，1 个任务 | 严格 lint 与 1,367 项 Linux 测试通过；日志明确 `No cache found` |
| `f9aa186` | [第二次发布 dry-run](https://github.com/Takuka0311/loongsuite-pilot/actions/runs/40487786768) | 9 个任务，失败 | 八平台均越过上一轮编译错误；Linux ARM64 GNU/musl 成功。Linux x64 命中缓存中的旧候选目录，Windows Hook 字节校验失败，macOS 打包报 EPERM；汇总正确拒绝 |
| `7abac87` | [第三次发布 dry-run](https://github.com/Takuka0311/loongsuite-pilot/actions/runs/40618597067) | 18 分 56 秒，9 个任务，失败 | 四个 Linux 与两个 macOS 包成功，缓存目录污染和 macOS EPERM 修复已实跑通过；Windows 包/Hook 测试成功，主库测试编译因旧测试无条件使用 Unix 权限 API 失败，汇总正确拒绝 |
| `eb9fcd7` | [日常 CI](https://github.com/Takuka0311/loongsuite-pilot/actions/runs/40620736566) | 缓存命中，2 分 03 秒，1 个任务 | 严格 lint 与 1,367 项 Linux 测试通过 |
| `eb9fcd7` | [第四次发布 dry-run](https://github.com/Takuka0311/loongsuite-pilot/actions/runs/40620737504) | 17 分 20 秒，9 个任务，失败 | Linux/macOS 六包成功；Windows 两架构已通过测试编译及候选进程回归，但实际包内 `check --json` 失败，汇总正确拒绝 |
| `a0c49cd` | [日常 CI](https://github.com/Takuka0311/loongsuite-pilot/actions/runs/40622985965) | 2 分 37 秒，1 个任务，失败 | 严格 lint 通过；Dashboard 测试收到 `ConnectionReset`，主库 1,250 通过、1 失败，未将失败重跑为成功 |
| `a0c49cd` | [第五次发布 dry-run](https://github.com/Takuka0311/loongsuite-pilot/actions/runs/40622987158) | 12 分 58 秒，9 个任务 | 八平台包与汇总首次全部通过；Windows 原生配置路径修复已实跑验证 |
| `2d56ecd` | [日常 CI](https://github.com/Takuka0311/loongsuite-pilot/actions/runs/40624833561) | 缓存命中，2 分 04 秒，1 个任务 | 严格 lint 与 1,369 项 Linux 测试通过，包含 Dashboard 修复 |
| `2d56ecd` | [第六次发布 dry-run](https://github.com/Takuka0311/loongsuite-pilot/actions/runs/40624834661) | 15 分 08 秒，9 个任务 | 最终代码的八平台包、原生预检与汇总全部通过；资产下载后 SHA-256 复核通过 |

修复链路：候选输出移出 Cargo 缓存，脚本通过 Git 属性固定 LF；macOS 仅在 libproc 确认组内没有活进程时处理 XNU 的僵尸组 EPERM，查询不完整或真正权限失败仍报错。`6217343` 限定 Unix 专属权限测试，保留平台无关断言。`a0c49cd` 将内置配置路径改为原生 `Path::join`，避免 Windows `\\?\` 路径混入 `/`；同时保留有界 stdout/stderr 失败诊断，防止丢失 `check --json` 的错误报告。

`2d56ecd` 修复 Dashboard 将一次 TCP `read()` 当成完整请求的问题：累计读取至请求头结束，保留 8 KiB 大小上限和共享两秒总时限；新增分段、EOF、超限及超时断言，包含新增回归的四项 Dashboard 测试本地重复 30 轮通过。该提交的默认/all-features 严格 clippy、本地及干净提交树 1,369 项 all-features 回归均已通过，未跳过失败用例或降低门禁。

第六轮的非 Linux 实跑范围：每个 Windows 架构通过包测试 6 项、Hook 测试 2 项、候选进程测试 6 项及配置路径测试 1 项；每个 macOS 架构通过包测试 6 项、Hook 测试 2 项、候选进程测试 5 项、配置路径测试 1 项及 Swift 测试 28 项。上述项目包含测试 fixture 入口，不等同于同等数量的独立业务场景。每包还执行真实归档解包后的 `--version`、`probe --build-edition`、`check --json`。完整服务/worker 生命周期、真实 Agent、GUI 与系统签名仍未验收。

## 最终八平台候选证明

来源为第六轮的 [release-assets](https://github.com/Takuka0311/loongsuite-pilot/actions/runs/40624834661/artifacts/12789945421)，保留期 90 天。已下载并回读 `release-summary.json`、`latest-rust.json`、`SHA256SUMS`，全部 12 个被校验文件均通过 SHA-256 复核。

- 版本 `1.2.0`，提交 `2d56ecd15ea907b16ff93ca1a1549b05ee247d11`，八包均为 `opensource`、`dirty=false`、`preflight.ok=true`，包内 `gitCommit` 一致。
- `release-summary.json` 的 SHA-256：`17d663cdf44e0993587921b7acfe277b1eab5f494cf9a80b5db4f5f03c1b27b9`。
- 摘要仍明确 `gate=build-and-package-preflight`、`agentInjection=deferred`、`platformLifecycle=not-verified`。

下表归档名统一为 `loongsuite-pilot-1.2.0-<target>`，Windows 后缀为 `.zip`，其他平台为 `.tar.gz`。

| target | 归档 SHA-256 |
|---|---|
| `aarch64-apple-darwin` | `65e54ab6e495c4aea3ce7b8d4d4c1338e5178912fe0a4168693d24578445a5bc` |
| `aarch64-pc-windows-msvc` | `0c9c8bb2dd598d87b1e25235d025db8336225090004394e392c86c0ba5a3bf25` |
| `aarch64-unknown-linux-gnu` | `49e5620b7fb154b64ff5b9bfc0d49442dac111c578cfaaa5e7ec5cae61e18200` |
| `aarch64-unknown-linux-musl` | `632148966da52f01fb809f9cbb1a43fdf5f70a3e0c254bccf76b174f6e2a73dd` |
| `x86_64-apple-darwin` | `83a63243c34ceb54e96c85d7ef8375513b81160d669ad35e358870fcf8ce8f27` |
| `x86_64-pc-windows-msvc` | `d6eeef50b1b9dd12ddf791e991c7ae979d119be9561bb5b0a6b53f198ad85e54` |
| `x86_64-unknown-linux-gnu` | `cd39025c9eacdc93256b39aa0ccaa836000d49ec4af44645751f0cbe42152570` |
| `x86_64-unknown-linux-musl` | `2b81e28f314b504d2e0dbd2a45a66c211a2ff2cdb5650e6b62be55fb73d5ca71` |

这些是 dry-run 候选，不是正式发行。`latest-rust.json` 中的 Release 下载地址仅为预生成地址，未上传为更新渠道；没有创建 tag 或正式 Release。后续仅补充本文的提交不改变上述候选的生产代码，发行候选 SHA 仍以上述完整提交为准。历史测试完整映射与旧工具清理仍按前述门禁暂缓。
