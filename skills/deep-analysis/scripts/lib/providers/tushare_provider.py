"""Tushare Pro provider · 需 TUSHARE_TOKEN · 官方级冗余.

Tushare Pro (https://tushare.pro) 是 A 股最专业的官方 API 之一：
  · 财报深度历史 (5-10 年)
  · 机构级龙虎榜 / 北向 / 期货逐笔
  · ISO 数据清洗流程，字段稳定性 >> akshare 爬虫

免费额度：新账号 120 分，常用接口 2000 分（贡献数据 / 充值）。

配置：
  1. 访问 https://tushare.pro 注册
  2. 个人中心 → 接口 TOKEN
  3. export TUSHARE_TOKEN=your_token
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, cast

from . import Provider, register, ProviderError

try:
    import tushare as ts  # type: ignore

    _TS_OK = True
except ImportError:
    ts = None
    _TS_OK = False


_AI_TRADER_ENV_PATH = Path("/opt/ai-trader/.env")


def _strip_env_value(raw: str) -> str:
    value = raw.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]
    return value.strip()


def _read_tushare_token_from_ai_trader_env() -> str:
    try:
        if not _AI_TRADER_ENV_PATH.exists():
            return ""
        for line in _AI_TRADER_ENV_PATH.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, _, value = stripped.partition("=")
            if key.strip() != "TUSHARE_TOKEN":
                continue
            return _strip_env_value(value)
    except Exception:
        return ""
    return ""


def resolve_tushare_token() -> str:
    token = os.environ.get("TUSHARE_TOKEN", "").strip()
    if token:
        return token
    return _read_tushare_token_from_ai_trader_env()


def _first_record(df) -> dict:
    if df is None or getattr(df, "empty", True):
        return {}
    try:
        records = df.to_dict("records")
    except Exception:
        return {}
    return records[0] if records else {}


class _TushareProvider:
    name: str = "tushare"
    requires_key: bool = True
    markets = cast(tuple[str, ...], ("A",))  # Tushare 主要覆盖 A 股

    _pro = None

    def is_available(self) -> bool:
        if not _TS_OK:
            return False
        return bool(resolve_tushare_token())

    def _get_pro(self):
        """懒初始化 tushare.pro_api."""
        if self._pro is not None:
            return self._pro
        if not self.is_available():
            raise ProviderError("Tushare 未启用（pip install tushare + TUSHARE_TOKEN）")
        token = resolve_tushare_token()
        if not token:
            raise ProviderError("Tushare 未启用（pip install tushare + TUSHARE_TOKEN）")
        os.environ.setdefault("TUSHARE_TOKEN", token)
        ts_mod = cast(Any, ts)
        if ts_mod is None:
            raise ProviderError("tushare package not installed")
        self._pro = ts_mod.pro_api(token)
        return self._pro

    def _ts_code(self, code: str) -> str:
        """A 股 6 位码 → Tushare 格式 (600519 → 600519.SH)."""
        code6 = code.split(".")[0].zfill(6)
        if code6.startswith(
            ("60", "68", "90", "50", "51", "52", "56", "58", "10", "11")
        ):
            return f"{code6}.SH"
        if code6.startswith(("83", "87", "88", "92")):
            return f"{code6}.BJ"
        return f"{code6}.SZ"

    def fetch_basic_a(self, code: str) -> dict:
        """stock_basic · 基础信息."""
        try:
            pro = self._get_pro()
            ts_code = self._ts_code(code)
            basic = _first_record(
                pro.stock_basic(
                    ts_code=ts_code,
                    fields="ts_code,symbol,name,industry,market,list_date",
                )
            )
            if not basic:
                raise ProviderError("tushare stock_basic empty")

            daily_basic = _first_record(
                pro.daily_basic(
                    ts_code=ts_code,
                    fields="ts_code,trade_date,close,pe_ttm,pb,total_mv,circ_mv",
                    limit=1,
                )
            )
            daily = _first_record(
                pro.daily(
                    ts_code=ts_code,
                    limit=1,
                )
            )

            raw = {
                **basic,
                **daily_basic,
                **daily,
            }
            return {"ok": True, "raw": raw}
        except ProviderError:
            raise
        except Exception as e:
            raise ProviderError(f"tushare.stock_basic: {e}")

    def fetch_financials_a(self, code: str, years: int = 5) -> dict:
        """利润表 + 资产负债表 + 现金流表 · 3 表 N 年深度历史."""
        try:
            pro = self._get_pro()
            ts_code = self._ts_code(code)
            income = pro.income(ts_code=ts_code, limit=years * 4)
            balance = pro.balancesheet(ts_code=ts_code, limit=years * 4)
            cashflow = pro.cashflow(ts_code=ts_code, limit=years * 4)
            try:
                indicator = pro.fina_indicator(ts_code=ts_code, limit=years * 4)
            except Exception:
                indicator = None
            return {
                "ok": True,
                "income": income.to_dict("records") if income is not None else [],
                "balance": balance.to_dict("records") if balance is not None else [],
                "cashflow": cashflow.to_dict("records") if cashflow is not None else [],
                "indicator": indicator.to_dict("records")
                if indicator is not None
                else [],
            }
        except Exception as e:
            raise ProviderError(f"tushare.financials: {e}")

    def fetch_kline_a(
        self,
        code: str,
        period: str = "daily",
        start: str = "20200101",
        adjust: str = "qfq",
    ) -> list[dict]:
        """A 股日线 · v2.10.6 新增 · 让 kline chain 有 tushare 作为最后一层兜底.

        pro.daily 给未复权价 + adj_factor；我们做后复权/前复权的朴素还原。
        字段统一成 data_sources 链里的中文列名，和 akshare 保持一致。
        """
        try:
            pro = self._get_pro()
            ts_code = self._ts_code(code)
            # 取日线（未复权）
            df = pro.daily(ts_code=ts_code, start_date=start)
            if df is None or df.empty:
                raise ProviderError("tushare.daily empty")
            # qfq 处理：拉 adj_factor 做前复权
            if adjust == "qfq":
                try:
                    adj = pro.adj_factor(ts_code=ts_code, start_date=start)
                    if adj is not None and not adj.empty:
                        latest_factor = float(adj.iloc[0]["adj_factor"])
                        df = df.merge(
                            adj[["trade_date", "adj_factor"]],
                            on="trade_date",
                            how="left",
                        )
                        ratio = df["adj_factor"] / latest_factor
                        for col in ("open", "high", "low", "close", "pre_close"):
                            if col in df.columns:
                                df[col] = df[col] * ratio
                except Exception:
                    pass  # 复权失败就返回未复权
            rows = []
            for _, r in df.sort_values("trade_date").iterrows():
                rows.append(
                    {
                        "日期": str(r["trade_date"])[:4]
                        + "-"
                        + str(r["trade_date"])[4:6]
                        + "-"
                        + str(r["trade_date"])[6:8],
                        "开盘": float(r.get("open", 0) or 0),
                        "收盘": float(r.get("close", 0) or 0),
                        "最高": float(r.get("high", 0) or 0),
                        "最低": float(r.get("low", 0) or 0),
                        "成交量": float(r.get("vol", 0) or 0),
                        "成交额": float(r.get("amount", 0) or 0),
                        "涨跌幅": float(r.get("pct_chg", 0) or 0),
                    }
                )
            return rows
        except ProviderError:
            raise
        except Exception as e:
            raise ProviderError(f"tushare.kline: {e}")

    def fetch_top10_holders(self, code: str) -> list[dict]:
        """前十大流通股东."""
        try:
            pro = self._get_pro()
            df = pro.top10_floatholders(ts_code=self._ts_code(code))
            return df.to_dict("records") if df is not None else []
        except Exception as e:
            raise ProviderError(f"tushare.top10_floatholders: {e}")

    def fetch_top_list(self, code: str, start: str, end: str) -> list[dict]:
        """龙虎榜 · 席位详情（比 akshare 覆盖更深）."""
        try:
            pro = self._get_pro()
            df = pro.top_list(
                ts_code=self._ts_code(code), start_date=start, end_date=end
            )
            return df.to_dict("records") if df is not None else []
        except Exception as e:
            raise ProviderError(f"tushare.top_list: {e}")

    def fetch_hsgt_flow(self, date: str = "") -> list[dict]:
        """北向资金流向（机构级）."""
        try:
            pro = self._get_pro()
            df = pro.moneyflow_hsgt(trade_date=date) if date else pro.moneyflow_hsgt()
            return df.to_dict("records") if df is not None else []
        except Exception as e:
            raise ProviderError(f"tushare.hsgt: {e}")


register(_TushareProvider())
