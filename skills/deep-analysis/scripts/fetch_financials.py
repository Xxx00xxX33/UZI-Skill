"""Dimension 1 · 财报 — 产出 viz 需要的完整 shape.

Output shape (matches report viz expectations):
{
  "roe": "18.7%", "net_margin": "...", "revenue_growth": "...", "fcf": "...",
  "roe_history":        [12.4, 14.1, 15.8, 16.2, 17.5, 18.7],   # 5Y+
  "revenue_history":    [21.5, 25.8, 28.6, 32.1, 38.4, 49.2],   # 亿
  "net_profit_history": [4.2,  5.1,  5.9,  6.8,  8.3,  10.5],   # 亿
  "financial_years":    ["2020", "2021", "2022", "2023", "2024", "25Q1"],
  "dividend_years":     ["2020", ...],
  "dividend_amounts":   [...],   # 元/10 股
  "dividend_yields":    [...],   # %
  "financial_health": {
      "current_ratio": 2.4,
      "debt_ratio":    28.5,
      "fcf_margin":   118.0,
      "roic":          22.3,
  }
}
"""

from __future__ import annotations

import math
import json
import importlib
import sys
import traceback
from typing import Any

from lib import data_sources as ds
from lib.market_router import parse_ticker

ak = ds.ak


def _to_float_or_none(v) -> float | None:
    try:
        if v in (None, "", "--", "-", "—"):
            return None
        parsed = float(str(v).replace(",", "").replace("%", ""))
        if math.isnan(parsed):
            return None
        return parsed
    except (ValueError, TypeError):
        return None


def _to_float(v) -> float:
    return _to_float_or_none(v) or 0.0


def _to_yi(v) -> float:
    """Convert raw (often 元) to 亿."""
    n = _to_float(v)
    return round(n / 1e8, 2)


def _is_meaningful_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not math.isnan(float(value))
        and abs(float(value)) > 1e-9
    )


def _has_meaningful_list(value: Any, *, min_points: int = 1) -> bool:
    if not isinstance(value, list) or not value:
        return False
    numeric_items = [item for item in value if isinstance(item, (int, float))]
    if numeric_items:
        return (
            sum(1 for item in numeric_items if _is_meaningful_number(item))
            >= min_points
        )
    return any(item not in (None, "", "—", "-", "--") for item in value)


def _has_meaningful_scalar(value: Any) -> bool:
    if value in (None, "", "—", "-", "--"):
        return False
    if isinstance(value, (int, float)):
        return _is_meaningful_number(value)
    text = str(value).strip()
    if not text:
        return False
    compact = text.replace(" ", "").lstrip("+-")
    return compact not in {
        "0",
        "0.0",
        "0.00",
        "0%",
        "0.0%",
        "0.00%",
        "0亿",
        "0.0亿",
        "0.00亿",
    }


def _has_meaningful_health(value: Any) -> bool:
    return isinstance(value, dict) and any(
        _is_meaningful_number(item) for item in value.values()
    )


def _pick_first(record: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = record.get(key)
        if value not in (None, "", "--", "-", "—"):
            return value
    return None


def _record_period(record: dict[str, Any]) -> str:
    raw = _pick_first(record, "end_date", "report_date", "trade_date", "ann_date")
    if raw is None:
        return ""
    text = str(raw).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return text


def _sort_period_key(period: str) -> tuple[int, str]:
    digits = "".join(ch for ch in period if ch.isdigit())
    try:
        return (int(digits or 0), period)
    except ValueError:
        return (0, period)


def _records_by_period(
    records: list[dict[str, Any]], *, annual_only: bool = True
) -> dict[str, dict[str, Any]]:
    ordered = sorted(
        records,
        key=lambda rec: (
            _sort_period_key(_record_period(rec)),
            _sort_period_key(str(_pick_first(rec, "ann_date", "f_ann_date") or "")),
        ),
    )
    out: dict[str, dict[str, Any]] = {}
    for record in ordered:
        period = _record_period(record)
        if not period:
            continue
        if annual_only and not period.endswith("1231"):
            continue
        out[period] = record
    return out


def _pick_numeric(record: dict[str, Any], *keys: str) -> float | None:
    return ds._safe_float(_pick_first(record, *keys))


def _fmt_period_label(period: str) -> str:
    digits = "".join(ch for ch in period if ch.isdigit())
    if len(digits) >= 8:
        year = digits[:4]
        month_day = digits[4:8]
        return (
            year
            if month_day == "1231"
            else f"{year}Q{((int(digits[4:6]) - 1) // 3) + 1}"
        )
    return period[:4] if len(period) >= 4 else period


def _compute_roe_from_records(
    income_rec: dict[str, Any], balance_rec: dict[str, Any]
) -> float | None:
    profit = _pick_numeric(income_rec, "n_income_attr_p", "n_income", "net_profit")
    equity = _pick_numeric(
        balance_rec,
        "total_hldr_eqy_exc_min_int",
        "total_hldr_eqy_inc_min_int",
        "total_hldr_eqy",
        "total_owner_equities",
    )
    if equity is None:
        assets = _pick_numeric(balance_rec, "total_assets")
        liab = _pick_numeric(balance_rec, "total_liab")
        if assets is not None and liab is not None and assets > liab:
            equity = assets - liab
    if profit is None or equity is None or equity <= 0:
        return None
    return round(profit / equity * 100, 2)


def _build_tushare_financial_fallback(payload: dict[str, Any]) -> dict[str, Any]:
    income_rows = payload.get("income") if isinstance(payload, dict) else []
    balance_rows = payload.get("balance") if isinstance(payload, dict) else []
    cashflow_rows = payload.get("cashflow") if isinstance(payload, dict) else []
    indicator_rows = payload.get("indicator") if isinstance(payload, dict) else []

    income_by_period = _records_by_period(
        income_rows if isinstance(income_rows, list) else []
    )
    balance_by_period = _records_by_period(
        balance_rows if isinstance(balance_rows, list) else []
    )
    cashflow_by_period = _records_by_period(
        cashflow_rows if isinstance(cashflow_rows, list) else []
    )
    indicator_by_period = _records_by_period(
        indicator_rows if isinstance(indicator_rows, list) else []
    )

    periods = sorted(
        set(income_by_period)
        | set(balance_by_period)
        | set(cashflow_by_period)
        | set(indicator_by_period),
        key=_sort_period_key,
    )[-6:]
    if not periods:
        return {}

    revenue_history: list[float] = []
    net_profit_history: list[float] = []
    roe_history: list[float] = []
    financial_years: list[str] = []

    latest_period = periods[-1]
    latest_income: dict[str, Any] = {}
    latest_balance: dict[str, Any] = {}
    latest_cashflow: dict[str, Any] = {}
    latest_indicator: dict[str, Any] = {}

    for period in periods:
        income_rec = income_by_period.get(period, {})
        balance_rec = balance_by_period.get(period, {})
        cashflow_rec = cashflow_by_period.get(period, {})
        indicator_rec = indicator_by_period.get(period, {})

        revenue = _pick_numeric(
            income_rec, "total_revenue", "revenue", "operate_income"
        )
        net_profit = _pick_numeric(
            income_rec, "n_income_attr_p", "n_income", "net_profit"
        )
        roe = _pick_numeric(indicator_rec, "roe", "roe_yearly", "q_roe", "roe_avg")
        if roe is None:
            roe = _compute_roe_from_records(income_rec, balance_rec)

        if revenue is None or net_profit is None or roe is None:
            continue

        revenue_history.append(round(revenue / 1e8, 2))
        net_profit_history.append(round(net_profit / 1e8, 2))
        roe_history.append(round(roe, 2))
        financial_years.append(_fmt_period_label(period))

        if period == latest_period:
            latest_income = income_rec
            latest_balance = balance_rec
            latest_cashflow = cashflow_rec
            latest_indicator = indicator_rec

    if not revenue_history or not net_profit_history or not roe_history:
        return {}

    if not latest_income:
        latest_income = income_by_period.get(latest_period, {})
    if not latest_balance:
        latest_balance = balance_by_period.get(latest_period, {})
    if not latest_cashflow:
        latest_cashflow = cashflow_by_period.get(latest_period, {})
    if not latest_indicator:
        latest_indicator = indicator_by_period.get(latest_period, {})

    net_margin_value = _pick_numeric(
        latest_indicator,
        "netprofit_margin",
        "net_margin",
    )
    if net_margin_value is None and revenue_history[-1] > 0:
        net_margin_value = round(net_profit_history[-1] / revenue_history[-1] * 100, 2)

    debt_ratio = _pick_numeric(
        latest_indicator,
        "debt_to_assets",
        "debt_to_asset",
        "debt_ratio",
    )
    if debt_ratio is None:
        total_assets = _pick_numeric(latest_balance, "total_assets")
        total_liab = _pick_numeric(latest_balance, "total_liab")
        if total_assets and total_liab is not None and total_assets > 0:
            debt_ratio = round(total_liab / total_assets * 100, 2)

    current_ratio = _pick_numeric(latest_indicator, "current_ratio")
    if current_ratio is None:
        total_cur_assets = _pick_numeric(latest_balance, "total_cur_assets")
        total_cur_liab = _pick_numeric(latest_balance, "total_cur_liab")
        if total_cur_assets is not None and total_cur_liab and total_cur_liab > 0:
            current_ratio = round(total_cur_assets / total_cur_liab, 2)

    roic = _pick_numeric(latest_indicator, "roic", "roa")

    ocf = _pick_numeric(latest_cashflow, "n_cashflow_act")
    out: dict[str, Any] = {
        "revenue_history": revenue_history,
        "net_profit_history": net_profit_history,
        "roe_history": roe_history,
        "financial_years": financial_years,
        "roe": f"{roe_history[-1]:.1f}%",
        "net_margin": f"{net_margin_value:.1f}%"
        if net_margin_value is not None
        else "—",
    }

    if len(revenue_history) >= 2 and revenue_history[-2]:
        growth = (revenue_history[-1] - revenue_history[-2]) / revenue_history[-2] * 100
        out["revenue_growth"] = f"{growth:+.1f}%"

    health: dict[str, float] = {}
    if current_ratio is not None:
        health["current_ratio"] = round(current_ratio, 2)
    if debt_ratio is not None:
        health["debt_ratio"] = round(debt_ratio, 1)
    if roic is not None:
        health["roic"] = round(roic, 1)
    if ocf is not None:
        out["fcf"] = f"{ocf / 1e8:.1f}亿"
        if net_profit_history[-1] > 0:
            health["fcf_margin"] = round(ocf / 1e8 / net_profit_history[-1] * 100, 1)
    if health:
        out["financial_health"] = health

    return out


def _merge_financial_fallback(
    out: dict[str, Any], fallback: dict[str, Any], source: str
) -> bool:
    touched = False
    list_fields = (
        "revenue_history",
        "net_profit_history",
        "roe_history",
        "financial_years",
        "dividend_years",
        "dividend_amounts",
        "dividend_yields",
    )
    for field in list_fields:
        value = fallback.get(field)
        if _has_meaningful_list(value) and not _has_meaningful_list(out.get(field)):
            out[field] = value
            touched = True

    scalar_fields = ("roe", "net_margin", "revenue_growth", "fcf")
    for field in scalar_fields:
        value = fallback.get(field)
        if _has_meaningful_scalar(value) and not _has_meaningful_scalar(out.get(field)):
            out[field] = value
            touched = True

    fallback_health = fallback.get("financial_health")
    if isinstance(fallback_health, dict):
        health = out.setdefault("financial_health", {})
        if isinstance(health, dict):
            for key, value in fallback_health.items():
                if _is_meaningful_number(value) and not _is_meaningful_number(
                    health.get(key)
                ):
                    health[key] = value
                    touched = True

    if touched:
        out["_provider_financial_source"] = source
        out["_used_backup_provider"] = True
    return touched


def _needs_financial_fallback(out: dict[str, Any]) -> bool:
    health: dict[str, Any] = {}
    raw_health = out.get("financial_health")
    if isinstance(raw_health, dict):
        health = raw_health
    return not (
        _has_meaningful_list(out.get("revenue_history"), min_points=2)
        and _has_meaningful_list(out.get("net_profit_history"), min_points=2)
        and _has_meaningful_list(out.get("roe_history"), min_points=2)
        and _is_meaningful_number(health.get("debt_ratio"))
    )


def _has_meaningful_financial_data(data: dict[str, Any]) -> bool:
    return (
        _has_meaningful_list(data.get("revenue_history"))
        or _has_meaningful_list(data.get("net_profit_history"))
        or _has_meaningful_list(data.get("roe_history"))
        or _has_meaningful_scalar(data.get("roe"))
        or _has_meaningful_scalar(data.get("net_margin"))
        or _has_meaningful_health(data.get("financial_health"))
        or _has_meaningful_list(data.get("dividend_years"))
    )


def _fetch_a_share(ti) -> dict:
    out: dict[str, Any] = {}
    code = ti.code
    source_parts: list[str] = []

    # ─── 1. 历年关键指标 (stock_financial_abstract_ths 或 stock_financial_abstract)
    try:
        df_abs = ak.stock_financial_abstract(symbol=code)
        if df_abs is not None and not df_abs.empty:
            # 该接口一列是 "指标", 后面几列是报告期
            period_cols = [c for c in df_abs.columns if c not in ("选项", "指标")]
            # 最近 6 个年报 (按季度倒序)
            period_cols_annual = [c for c in period_cols if str(c).endswith("1231")][:6]
            period_cols_annual = sorted(period_cols_annual)  # 旧 -> 新

            def _row(keyword: str) -> list:
                row = df_abs[
                    df_abs["指标"]
                    .astype(str)
                    .str.contains(keyword, na=False, regex=False)
                ]
                if row.empty:
                    return []
                values = []
                meaningful = False
                for col in period_cols_annual:
                    raw_value = row[col].iloc[0]
                    parsed = _to_float_or_none(raw_value)
                    if parsed is None:
                        values.append(0.0)
                        continue
                    yi_value = round(parsed / 1e8, 2)
                    values.append(yi_value)
                    if _is_meaningful_number(yi_value):
                        meaningful = True
                return values if meaningful else []

            out["revenue_history"] = _row("营业总收入")
            out["net_profit_history"] = _row("归属于母公司所有者的净利润") or _row(
                "净利润"
            )
            out["financial_years"] = [str(c)[:4] for c in period_cols_annual]
            if _has_meaningful_list(out.get("revenue_history")) or _has_meaningful_list(
                out.get("net_profit_history")
            ):
                source_parts.append("akshare")
    except Exception as e:
        out["_abstract_error"] = str(e)

    # ─── 2. 加权 ROE 序列 (stock_financial_analysis_indicator)
    try:
        df_ind = ak.stock_financial_analysis_indicator(symbol=code, start_year="2018")
        if df_ind is not None and not df_ind.empty:
            date_col = "日期" if "日期" in df_ind.columns else df_ind.columns[0]
            df_ind = df_ind.sort_values(date_col)
            # filter to year-end rows (12-31)
            df_annual = df_ind[df_ind[date_col].astype(str).str.endswith("12-31")]
            if len(df_annual) < 3:  # fallback to all rows
                df_annual = df_ind

            for col_key, target in [
                ("加权净资产收益率(%)", "roe_history"),
                ("净资产收益率加权(%)", "roe_history"),
                ("ROE", "roe_history"),
            ]:
                if col_key in df_ind.columns:
                    raw_values = [
                        _to_float_or_none(v)
                        for v in df_annual[col_key].tail(6).tolist()
                    ]
                    if any(_is_meaningful_number(v) for v in raw_values):
                        out[target] = [0.0 if v is None else v for v in raw_values]
                    break

            last = df_ind.iloc[-1]
            # Financial health
            health = {}
            for src_key, dst_key, unit_div in [
                ("流动比率", "current_ratio", 1),
                ("资产负债率(%)", "debt_ratio", 1),
                ("总资产净利率(%)", "roic", 1),
                ("销售净利率(%)", "net_margin_pct", 1),
            ]:
                if src_key in df_ind.columns:
                    v = _to_float_or_none(last.get(src_key))
                    if v is not None and _is_meaningful_number(v):
                        health[dst_key] = v / unit_div
            if _has_meaningful_health(health):
                out["financial_health"] = health

            # Net margin / ROE 汇总 summary strings
            if "加权净资产收益率(%)" in df_ind.columns:
                latest_roe = _to_float_or_none(last["加权净资产收益率(%)"])
                if latest_roe is not None:
                    out["roe"] = f"{latest_roe:.1f}%"
            if "销售净利率(%)" in df_ind.columns:
                latest_margin = _to_float_or_none(last["销售净利率(%)"])
                if latest_margin is not None:
                    out["net_margin"] = f"{latest_margin:.1f}%"
            if "akshare" not in source_parts and (
                _has_meaningful_list(out.get("roe_history"))
                or _has_meaningful_scalar(out.get("roe"))
                or _has_meaningful_scalar(out.get("net_margin"))
                or _has_meaningful_health(out.get("financial_health"))
            ):
                source_parts.append("akshare")
    except Exception as e:
        out["_indicator_error"] = str(e)

    # ─── 3. 营收增速 summary
    try:
        rh = out.get("revenue_history") or []
        if len(rh) >= 2 and rh[-2]:
            growth = (rh[-1] - rh[-2]) / rh[-2] * 100
            out["revenue_growth"] = f"{growth:+.1f}%"
    except Exception:
        pass

    # ─── 4. 现金流 (FCF 占净利比)
    try:
        df_cf = ak.stock_cash_flow_sheet_by_report_em(
            symbol=f"{'SZ' if ti.full.endswith('SZ') else 'SH'}{code}"
        )
        if df_cf is not None and not df_cf.empty:
            # 最近一期 经营性现金流
            if "经营活动产生的现金流量净额" in df_cf.columns:
                ocf = _to_float_or_none(df_cf["经营活动产生的现金流量净额"].iloc[0])
                if ocf is not None:
                    out["fcf"] = f"{ocf / 1e8:.1f}亿"
                    # ocf/np
                    np_latest = (out.get("net_profit_history") or [0])[-1]
                    if np_latest:
                        out.setdefault("financial_health", {})["fcf_margin"] = round(
                            ocf / 1e8 / np_latest * 100, 1
                        )
                    if "akshare" not in source_parts:
                        source_parts.append("akshare")
    except Exception:
        pass

    # ─── 5. 分红历史
    try:
        df_div = ak.stock_history_dividend_detail(symbol=code, indicator="分红")
        if df_div is not None and not df_div.empty:
            # 取近 5 年，按年份聚合（同一年可能多次分红）
            from collections import defaultdict

            by_year: dict[str, float] = defaultdict(float)
            for _, row in df_div.head(30).iterrows():
                date_str = str(row.get("公告日期", row.get("除权除息日", "")))
                year = date_str[:4] if date_str and len(date_str) >= 4 else ""
                amount = _to_float_or_none(
                    row.get("派息", row.get("现金分红-派息(税前)(元/10股)", 0))
                )
                if year and amount:
                    by_year[year] += amount
            if by_year:
                years_sorted = sorted(by_year.keys())[-5:]
                out["dividend_years"] = years_sorted
                out["dividend_amounts"] = [round(by_year[y], 2) for y in years_sorted]
                # dividend yield ~ 自算，暂取占比近似，真实算法需要当年年末价格
                out["dividend_yields"] = [
                    round(by_year[y] / 20, 2) for y in years_sorted
                ]  # 非常粗略，生产环境应该用年末价
                if "akshare" not in source_parts:
                    source_parts.append("akshare")
    except Exception as e:
        out["_dividend_error"] = str(e)

    if _needs_financial_fallback(out):
        try:
            payload, source = ds._call_a_share_provider_fallback(
                "fetch_financials_a",
                code,
                provider_names=("tushare",),
                years=6,
            )
            if source == "tushare":
                fallback = _build_tushare_financial_fallback(
                    payload if isinstance(payload, dict) else {}
                )
                if _merge_financial_fallback(out, fallback, source):
                    source_parts.append(f"providers/{source}")
        except Exception as e:
            out["_provider_financial_error"] = str(e)

    if source_parts:
        deduped: list[str] = []
        for part in source_parts:
            if part not in deduped:
                deduped.append(part)
        out["_source"] = " + ".join(deduped)

    return out


def _fetch_hk(ti) -> dict:
    """v2.7.2 · 港股财报 — 之前 HK 分支直接返回 {}，导致 1_financials 完全空。

    数据源: akshare.stock_financial_hk_analysis_indicator_em
      返回 9 年年度指标，含 ROE_AVG / ROE_YEARLY / ROIC_YEARLY / DEBT_ASSET_RATIO
      / CURRENT_RATIO / GROSS_PROFIT_RATIO / OPERATE_INCOME / HOLDER_PROFIT /
      OPERATE_INCOME_YOY / HOLDER_PROFIT_YOY / NET_PROFIT_RATIO / BASIC_EPS
      / PER_NETCASH_OPERATE.
    """
    code5 = ti.code.zfill(5)
    out: dict = {}
    try:
        df = ak.stock_financial_hk_analysis_indicator_em(symbol=code5, indicator="年度")
        if df is None or df.empty:
            return {}
        # 按年份升序，取最近 6 年
        df = df.sort_values("REPORT_DATE").tail(6).reset_index(drop=True)

        years = [str(d)[:4] for d in df["REPORT_DATE"].tolist()]
        out["financial_years"] = years

        def _col(name, div=1.0, ndigits=2):
            if name not in df.columns:
                return []
            vals = []
            for v in df[name].tolist():
                try:
                    vals.append(round(float(v) / div, ndigits))
                except (TypeError, ValueError):
                    vals.append(None)
            return vals

        # OPERATE_INCOME 和 HOLDER_PROFIT 以 元 为单位，折算亿
        out["revenue_history"] = _col("OPERATE_INCOME", div=1e8, ndigits=2)
        out["net_profit_history"] = _col("HOLDER_PROFIT", div=1e8, ndigits=2)
        out["roe_history"] = _col("ROE_AVG", ndigits=2)
        out["gross_margin_history"] = _col("GROSS_PROFIT_RATIO", ndigits=2)
        out["net_margin_history"] = _col("NET_PROFIT_RATIO", ndigits=2)

        last = df.iloc[-1].to_dict()

        def _last_pct(key, default="—"):
            v = last.get(key)
            parsed = _to_float_or_none(v)
            if parsed is None:
                return default
            return f"{parsed:.1f}%"

        out["roe"] = _last_pct("ROE_AVG")
        out["roic"] = _last_pct("ROIC_YEARLY")
        out["net_margin"] = _last_pct("NET_PROFIT_RATIO")
        out["gross_margin"] = _last_pct("GROSS_PROFIT_RATIO")

        # 营收增速（最后一年 YoY）
        revenue_growth = _to_float_or_none(last.get("OPERATE_INCOME_YOY"))
        out["revenue_growth"] = (
            f"{revenue_growth:.1f}%" if revenue_growth is not None else "—"
        )
        profit_growth = _to_float_or_none(last.get("HOLDER_PROFIT_YOY"))
        out["profit_growth"] = (
            f"{profit_growth:.1f}%" if profit_growth is not None else "—"
        )

        # financial_health 子结构与 A 股保持一致
        try:
            health = {
                "debt_ratio": _to_float_or_none(last.get("DEBT_ASSET_RATIO")),
                "current_ratio": _to_float_or_none(last.get("CURRENT_RATIO")),
                "roic": _to_float_or_none(last.get("ROIC_YEARLY")),
                "fcf_margin": None,  # HK 年报未直接给 FCF margin
            }
            cleaned_health = {
                key: round(value, 2 if key != "debt_ratio" else 1)
                for key, value in health.items()
                if _is_meaningful_number(value)
            }
            if cleaned_health:
                out["financial_health"] = cleaned_health
        except Exception:
            pass

        # EPS / BPS
        try:
            out["eps"] = round(float(last.get("BASIC_EPS") or 0), 3)
        except Exception:
            pass
        try:
            out["bps"] = round(float(last.get("BPS") or 0), 2)
        except Exception:
            pass

        out["currency"] = str(last.get("CURRENCY") or "HKD")
    except Exception as e:
        out["_hk_indicator_error"] = f"{type(e).__name__}: {e}"

    # 港股派息（派息记录需要另一个 API；akshare 覆盖有限，暂不强制）
    return out


def _fetch_us(ti) -> dict:
    try:
        yf = importlib.import_module("yfinance")
    except ImportError:
        return {}
    try:
        t = yf.Ticker(ti.code)
        fin = t.financials  # 最近 4 年
        bs = t.balance_sheet
        cf = t.cashflow
        info = t.info or {}
        out: dict = {}
        if fin is not None and not fin.empty:
            rev_row = next(
                (r for r in ["Total Revenue", "TotalRevenue"] if r in fin.index), None
            )
            np_row = next(
                (
                    r
                    for r in [
                        "Net Income",
                        "NetIncome",
                        "Net Income Common Stockholders",
                    ]
                    if r in fin.index
                ),
                None,
            )
            if rev_row:
                out["revenue_history"] = [
                    round(float(v) / 1e8, 2) for v in fin.loc[rev_row].tolist()[::-1]
                ]
            if np_row:
                out["net_profit_history"] = [
                    round(float(v) / 1e8, 2) for v in fin.loc[np_row].tolist()[::-1]
                ]
            out["financial_years"] = [str(c)[:4] for c in fin.columns[::-1]]
        out["roe"] = (
            f"{info.get('returnOnEquity', 0) * 100:.1f}%"
            if info.get("returnOnEquity")
            else "—"
        )
        out["net_margin"] = (
            f"{info.get('profitMargins', 0) * 100:.1f}%"
            if info.get("profitMargins")
            else "—"
        )
        return out
    except Exception:
        return {}


def main(ticker: str) -> dict:
    ti = parse_ticker(ticker)
    try:
        if ti.market == "A":
            data = _fetch_a_share(ti)
        elif ti.market == "U":
            data = _fetch_us(ti)
        elif ti.market == "H":
            data = _fetch_hk(ti)
        else:
            data = {}
        error = None
    except Exception as e:
        data = {}
        error = f"{type(e).__name__}: {e}"
        traceback.print_exc(file=sys.stderr)

    source = data.pop("_source", None) if isinstance(data, dict) else None
    used_backup_provider = False
    if isinstance(data, dict):
        used_backup_provider = bool(data.pop("_used_backup_provider", False))
        if not used_backup_provider and isinstance(source, str):
            used_backup_provider = "providers/" in source
    if not source:
        if ti.market == "A":
            source = "akshare:stock_financial_abstract + indicator + cash_flow + dividend_detail"
        elif ti.market == "H":
            source = "akshare:stock_financial_hk_analysis_indicator_em"
        elif ti.market == "U":
            source = "yfinance:financials + balance_sheet + cashflow"
        else:
            source = "unknown"

    return {
        "ticker": ti.full,
        "data": data,
        "source": source,
        "used_backup_provider": used_backup_provider,
        "fallback": not _has_meaningful_financial_data(
            data if isinstance(data, dict) else {}
        ),
        "error": error,
    }


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "002273.SZ"
    print(json.dumps(main(arg), ensure_ascii=False, indent=2, default=str))
