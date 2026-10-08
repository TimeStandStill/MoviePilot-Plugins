# 上游迁移记录

- 来源仓库：https://github.com/DDSRem-Dev/MoviePilot-Plugins
- 来源分支：`main`
- 来源提交：`2d6f93a5a80930e4d8001452439b81039cf2c50b`
- 上游版本：`2.8.74`
- 本仓库 V3 版本：`3.0.1`
- 迁移日期：2026-10-08
- 原作者：DDSRem

## 迁移范围

从上游 `plugins.v2/p115strmhelper` 迁入后，以 `plugins.v2/qbp115strmhelper` 保留后端、数据库迁移、测试、翻译、Rust 源码与随附 wheels；同时迁入 `frontend/qbp115strmhelper` 前端及 `docs/qbp115strmhelper` 文档。在 `plugins.v3/qbp115strmhelper` 维护独立 V3 适配版本，分别登记在 `package.v2.json` 和 `package.v3.json`，不覆盖原有插件条目。

保留上游作者、外部服务地址和使用条款；上游 LICENSE 副本随插件分发。

## 私用标识

- 显示名称：115网盘STRM助手（QB私用）。
- 类名和市场 ID：`QBP115StrmHelper`；模块、前端和文档目录：`qbp115strmhelper`。
- 配置前缀、数据库目录、缓存、任务 ID、API 路径及前端模块均使用独立标识；远程命令和 PluginAction 事件加 `qb_` 前缀。
- 旧版本配置不自动复用；切换时停用旧版，重新配置并更新已有 STRM 的 API 地址。

## V3 适配

- 日志、配置、事件、媒体上下文、网络、缓存和通用工具改用 V3 SDK。
- 新增 `pyproject.toml`，使用 Python 3.14；锁定随附纯 Python wheels 对应的依赖版本，避免间接依赖升级破坏 p115client 导入。
- 整理历史按 `MediaSource.TMDB` 与字符串 `media_id` 查询，路径检索通过宿主管理的查询会话执行。
- 一次性任务与生活事件守护任务重注册改用公开调度 SDK。
- 数据库迁移脚本按插件实际安装位置定位。
- V3 整理使用宿主原生持久化队列，保留任务准入、执行检查点、失败恢复和结算流程；不再启用 V2 替换私有整理方法的批量接管补丁，前端对此显示说明。

验证宿主为 `jxxghp/MoviePilot` 的 `v3` 分支提交 `656e2852f798cac19151ce725daca2c2e6334a3d`。验证范围包括 5 项 V3 集成测试和 86 项上游工具测试，共 91 项通过；原有插件 6 项测试通过。前端生产构建和依赖兼容检查通过。没有连接真实 115 账号验证登录、全量/增量同步及播放。

可用已安装宿主、插件依赖和 pytest 的 Python 3.14 环境复现：

```powershell
python scripts/validate_p115_v3.py --host /path/to/MoviePilot
```

该脚本复用宿主测试引导，使用隔离配置与数据库，不修改真实 MoviePilot 配置或连接真实网盘。

## 构建与发布

```powershell
npm ci --prefix frontend/qbp115strmhelper
npm run build --prefix frontend/qbp115strmhelper
Copy-Item frontend/qbp115strmhelper/dist plugins.v2/qbp115strmhelper/dist -Recurse -Force
Copy-Item frontend/qbp115strmhelper/dist plugins.v3/qbp115strmhelper/dist -Recurse -Force
```

修改版本索引或手动运行 Plugin Release 工作流时，自动构建前端并按各自版本生成 Release：V3 标签 `QBP115StrmHelper_v3.0.1`、资产 `qbp115strmhelper_v3.0.1.zip`；V2 标签 `QBP115StrmHelper_v2.8.75`、资产 `qbp115strmhelper_v2.8.75.zip`。压缩包顶层目录均为 `qbp115strmhelper`。

后续版本更新需同时同步上游后端、前端、随附 wheels、文档和对应索引。勿直接覆盖本仓库完整的 package 文件，以免丢失其他插件条目。
