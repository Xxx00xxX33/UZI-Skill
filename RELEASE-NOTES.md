# Release Notes

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
