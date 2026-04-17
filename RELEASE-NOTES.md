# Release Notes

## v2.5.0 — 2026-04-17

### 数据源注册表（v2.5 主题）
- **`lib/data_source_registry.py`** — 40+ 个数据源元数据集中管理（新文件，~330 行）
  - 3 个 Tier：HTTP 主源 (22) / Playwright 浏览器源 (7) / 官方披露源 (11)
  - 字段：id / name / url / markets / dims / tier / access / health / notes
  - 辅助函数：`by_dim()` / `by_market()` / `http_sources_for()` / `playwright_sources_for()`
- **`task2.5-qualitative-deep-dive.md` URL 模板扩充**
  - Dim 3 加 华尔街见闻 / 第一财经 / Investing 经济日历 + 商品
  - Dim 4 新加段（A/H 浏览器源）
  - Dim 7 加 问财 / 同花顺 F10
  - Dim 13 加 财联社 7x24 / 华尔街见闻
  - Dim 15 加 财联社 / 第一财经 / 金融界 / 网易财经；HK 段加 HKEXNews + AASTOCKS
  - Dim 16 新加段（云财经龙虎榜 + 东财）

### 港股 5 维实际增强
- **`lib/hk_data_sources.py`** — 包装之前未用到的 50+ akshare HK 函数（新文件，~200 行）
  - `fetch_hk_basic_combined`：XQ basic + EM company profile + EM valuation/scale/growth comparison
  - `fetch_hk_announcements`：HKEXNews 静态 HTML 抓取（基础版）
- **`_fetch_basic_hk` 重写**（data_sources.py）：从 1 个 push2 调用扩展到 4 源 fallback chain
  - 新拿到字段：industry / pe_ttm / pb / market_cap / 主营业务 / chairman / ranks / 港股通标记
  - 实测 00700：industry=软件服务、PE=18.95、PB=3.69、市值=4.13万亿、HK 排名第 1
- **`fetch_peers.py` HK 分支**：rank-in-HK-universe 替代具体同行表（agent 可走 AASTOCKS Playwright 补充）
- **`fetch_capital_flow.py` HK 分支**：港股通资格 (沪/深) + eniu 30日市值历史
- **`fetch_events.py` HK 分支**：HKEXNews 公告 + 中文 web search 兜底

### AGENTS.md
- 新增"数据源速查表"小节：按 dim × 市场列出推荐源优先级
- Python 调用示例（agent 怎么用 registry）

### 维持兼容
- `requirements.txt` 不变（无新 pip 依赖）
- AASTOCKS 仅作 Tier-2 Playwright 源在 registry 出现，不写 fetcher 代码（HTML 是 JS 渲染的）
- 1_financials / 6_research / 16_lhb 等其他 6 个 HK dim 仍 stub，标在 RELEASE-NOTES "已知缺口"

### 已知缺口（next）
- HK price/change_pct（push2 spot 不通；agent 可走 Tencent qt: `qt.gtimg.cn/q=hk{code5}`）
- HK 财报 / HK 研报 / HK 沪深港通持股变动 / HK 同行具体 list（全走 AASTOCKS Playwright）

---

## v2.4.0 — 2026-04-17

### 大佬抓作业完整性
- `fetch_fund_holders.py:limit` 默认从 50 → None，茅台实测 649 家主动权益基金全部收录
- `assemble_report.render_fund_managers` 第 7 位起切换到紧凑行（48px × 滚动）
- 并行计算 fund_stats（默认 3 workers，`UZI_FUND_WORKERS=1` 切串行）

### 6 维定性深度方法论
- 新增 `references/task2.5-qualitative-deep-dive.md`（~400 行）
  - 3 个并行 sub-agent 分工（Macro-Policy / Industry-Events / Cost-Transmission）
  - 每维 4-7 必答问题、6 条跨域因果链
- SKILL.md 新增 HARD-GATE-QUALITATIVE
- pip 国内镜像自动 fallback（清华 → 阿里云 → 中科大 → 豆瓣）
- AGENTS.md 新增"网络受限环境"场景 A/B/C

---

## v2.3.0 — 2026-04-17

### 中文名纠错 + MX 妙想 API 接入
- 新 `lib/name_matcher.py` (Levenshtein + Jaccard fuzzy)
- 新 `lib/mx_api.py` (东财妙想 Skills Hub `mkapi2.dfcfs.com` 客户端)
- 三层解析：MX → akshare 精确 → 本地 fuzzy
- `.env` + `--force-name`

### 数据缺口 agent 接管
- `data_integrity.generate_recovery_tasks` 输出 agent 任务清单
- HTML 报告顶部橙色 banner 标注缺失字段
- HARD-GATE-NAME + HARD-GATE-DATAGAPS

---

## v2.2.0 (develop) — 2026-04-16

### Agent Closed-Loop (核心改动)
- **`agent_analysis.json`**: 新增闭环文件，agent 的定性分析独立存储
- **`generate_synthesis()` 合并机制**: 优先使用 agent 写入的字段，仅对缺失字段生成 stub
- **`stage2()` 自动读取**: 读取 `agent_analysis.json` 并传给 `generate_synthesis` 合并
- **`agent_reviewed` 标记**: synthesis 输出带标记，明确标识是否有 agent 介入
- **HARD-GATE 增强**: 必须写 `agent_analysis.json` + 设置 `agent_reviewed: true` 才能进 stage2
- **合并优先级**: agent dim_commentary > stub，agent punchline > 脚本金句，agent risks > 低分维度生成

### Agent 可覆盖字段
- `dim_commentary` — 每维度定性评语
- `panel_insights` — 评委整体观察
- `great_divide_override.punchline` — 冲突金句
- `great_divide_override.bull_say_rounds` / `bear_say_rounds` — 辩论 3 轮
- `narrative_override.core_conclusion` — 综合结论
- `narrative_override.risks` — 风险列表
- `narrative_override.buy_zones` — 四派买入区间

### Bug Fixes
- Fixed: `main()` 函数 `standalone_path` 不在作用域（NameError）

---

## v2.1.0 — 2026-04-16

### Architecture
- **Two-stage pipeline**: `stage1()` (data + skeleton) → agent analysis → `stage2()` (report)
- **HARD-GATE tags**: Claude cannot skip agent analysis step
- **Multi-platform support**: `.codex/`, `.opencode/`, `.cursor-plugin/`, `GEMINI.md`
- **Session hooks**: `hooks.json` auto-activates on session start
- **Agent template**: `agents/investor-panel.md` for sub-agent role-play

### Investor Intelligence
- **3-layer evaluation**: reality check (market/holdings/affinity) → rule engine → composite
- **Known holdings**: Buffett×Apple=100 bullish (actual holding), 游资×US=skip
- **Market scope**: Only 游资 restricted to A-share; all others evaluate globally

### Bug Fixes
- Fixed: KeyError 'skip' in sig_dist and vote_dist
- Fixed: investor_personas crash on skip signal
- Fixed: Hardcoded risks "苹果订单" appearing for all stocks
- Fixed: Great Divide bull/bear score mismatch with jury seats
- Fixed: build_unit_economics crash when industry is None
- Fixed: Capital flow empty (北向关停 → 主力资金替代)
- Fixed: LHB empty → show sector TOP 5
- Fixed: Governance pledge parsing (list[dict] not string)

---

## v2.0.0 — 2026-04-16

### New Features
- **17 institutional analysis methods** from anthropics/financial-services-plugins
  - DCF (WACC + 2-stage FCF + 5×5 sensitivity)
  - Comps (peer multiples + percentile)
  - 3-Statement projection (5Y IS/BS/CF)
  - Quick LBO (PE buyer IRR test)
  - Initiating Coverage (JPM/GS/MS format)
  - IC Memo (8 chapters + Bull/Base/Bear scenarios)
  - Porter 5 Forces + BCG Matrix
  - Catalyst Calendar, Thesis Tracker, Idea Screen, etc.
- **51 investor panel** with 180 quantified rules
- **Rule engine**: investor_criteria.py + investor_evaluator.py + stock_features.py (108 features)
- **Data integrity validator**: 100% coverage check after Task 1
- **Bloomberg-style HTML report** (~600KB self-contained)
- **14 slash commands**: /dcf, /comps, /lbo, /initiate, /ic-memo, /catalysts, /thesis, /screen, /dd, etc.

### Data Sources
- 22 dimensions, 8+ data sources, multi-layer fallback
- All free, zero API key (akshare/yfinance/ddgs/eastmoney/xueqiu/tencent/sina/baidu)

---

## v1.0.0 — 2026-04-14

- Initial release
- 19 dimensions + 50 investor panel + trap detection
- Basic HTML report
