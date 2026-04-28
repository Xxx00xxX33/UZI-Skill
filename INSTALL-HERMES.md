# Hermes Agent · 安装指南

> **v3.3.1+ 起 main 分支已直接支持 Hermes**（之前需要切换到 `hermes-compat` 分支）·
> 现在 `hermes skills install wbh604/UZI-Skill/skills/<skill>` 默认拉 main 即可工作。

## 最短路径（推荐）

```bash
# 先装依赖 skill（提供脚本 + lib）
hermes skills install wbh604/UZI-Skill/skills/deep-analysis

# 再装 3 个专用 skill（视需要）
hermes skills install wbh604/UZI-Skill/skills/investor-panel
hermes skills install wbh604/UZI-Skill/skills/lhb-analyzer
hermes skills install wbh604/UZI-Skill/skills/trap-detector
```

首次用 `/analyze-stock 600519.SH` 触发 `deep-analysis` 时，skill 会根据自身 SKILL.md 的提示自动让 LLM 跑一次 `pip install -r ~/.hermes/skills/deep-analysis/requirements.txt`，之后永久生效。

## 升级提示（重要）

如果你在 v3.3.1 之前装过 hermes 版本，升级前**先删掉旧的**再装新的：

```bash
hermes skills uninstall deep-analysis investor-panel lhb-analyzer trap-detector
hermes skills install wbh604/UZI-Skill/skills/deep-analysis
hermes skills install wbh604/UZI-Skill/skills/investor-panel
hermes skills install wbh604/UZI-Skill/skills/lhb-analyzer
hermes skills install wbh604/UZI-Skill/skills/trap-detector
```

旧版本（v2.10.8 之前）skill_dir 缺 `run.py` 或 `requirements.txt` · 这是历史报错的根因.

## 手动安装（clone + symlink）

适合开发或想修改源码的用户：

```bash
git clone https://github.com/wbh604/UZI-Skill.git ~/UZI-Skill
mkdir -p ~/.hermes/skills
for s in deep-analysis investor-panel lhb-analyzer trap-detector; do
  ln -sfn ~/UZI-Skill/skills/$s ~/.hermes/skills/$s
done
# 装 Python 依赖到 Hermes venv
"$HOME/.hermes/venv/bin/pip" install -r ~/UZI-Skill/requirements.txt
```

## 验证

```bash
hermes                       # 打开 TUI
/skills                      # 列出已装 skill · 应见 deep-analysis / investor-panel / lhb-analyzer / trap-detector
/analyze-stock 600519.SH     # 触发分析 · lite 模式下 30-60 秒出报告
```

报告生成到：
- `~/.hermes/skills/deep-analysis/scripts/reports/<ticker>_<date>/full-report-standalone.html`
- 手动装的话：`~/UZI-Skill/skills/deep-analysis/scripts/reports/...`

## 可选：环境变量（数据源增强）

写到 `~/.hermes/.env`：

```bash
# 东财妙想官方 API（国内推荐，境外反而更稳）
MX_APIKEY=your_miaoxiang_key

# Tushare（覆盖 baostock 不到的场景）
TUSHARE_TOKEN=your_tushare_token
```

不设也能跑，只是 fallback 数据源多一层。

## 三档思考深度

Hermes 用户推荐默认跑 `lite`（30-60s）或 `medium`（2-4min），触发时传 `--depth` 即可。

```
/analyze-stock 00700.HK --depth lite
/analyze-stock AAPL --depth medium
/analyze-stock 600519.SH --depth deep    # 15-20min，含 Bull-Bear 辩论
```

## 与其他环境的关系

| 环境 | 分支 | 状态 |
|---|---|---|
| Claude Code | `main` | 官方支持 v2.10.7 |
| Codex | `main` | 官方支持 v2.10.7 |
| Cursor | `main` | 官方支持 v2.10.7 |
| **Hermes** | **`hermes-compat`** | **v2.10.8 · 本次适配** |

`hermes-compat` 分支从 `main` 派生，只做 Hermes 兼容改动（SKILL.md 加 tags / 复制 requirements 到 skill dir / run.py 路径兼容），不会回灌到 `main` 影响其他环境用户。

## 故障排查

**问题：`/skills` 没列出 UZI-Skill**
- 检查 `~/.hermes/skills/deep-analysis/SKILL.md` 是否存在
- 跑 `hermes skills list` 看状态

**问题：触发后 `ImportError: No module named akshare`**
- 检查依赖装哪了：`which pip && pip show akshare`
- 重跑：`~/.hermes/venv/bin/pip install -r ~/.hermes/skills/deep-analysis/requirements.txt`

**问题：网络受限跑不完**
- 降到 lite：`/analyze-stock <ticker> --depth lite`
- 设 `MX_APIKEY` 切换到东财妙想主源
- 参考 [AGENTS.md 网络受限章节](./AGENTS.md)

## 反馈

- Issues: https://github.com/wbh604/UZI-Skill/issues
- 本分支 PR: https://github.com/wbh604/UZI-Skill/tree/hermes-compat
