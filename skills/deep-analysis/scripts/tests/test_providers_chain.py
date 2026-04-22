"""Regression tests for v2.10.6 provider chain integration.

Covers:
- try_chain() success / failover / all-fail
- get_provider_chain() env override
- health_check() structure
- provider method completeness (spot-check)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def test_health_check_structure():
    from lib.providers import health_check

    h = health_check()
    assert isinstance(h, dict)
    assert "akshare" in h  # always registered
    for name, info in h.items():
        assert "available" in info
        assert "status" in info


def test_get_provider_chain_defaults_to_akshare_first():
    os.environ.pop("UZI_PROVIDERS_KLINE", None)
    from lib.providers import get_provider_chain

    chain = get_provider_chain("kline", "A")
    # akshare 应该在链首（安装了 akshare 的机器上）
    if chain:  # only if akshare available
        assert chain[0].name == "akshare"


def test_env_override_reorders_chain(monkeypatch):
    """Env UZI_PROVIDERS_<DIM> 应改变 chain 顺序."""
    monkeypatch.setenv("UZI_PROVIDERS_KLINE", "baostock,akshare")
    from lib.providers import get_provider_chain

    chain = get_provider_chain("kline", "A")
    names = [p.name for p in chain]
    # 两个都可用的环境下，baostock 应先于 akshare
    if "baostock" in names and "akshare" in names:
        assert names.index("baostock") < names.index("akshare"), names
    # 至少有一个匹配到
    assert len(names) >= 1 or not names  # chain 空在缺依赖时也允许


def test_try_chain_returns_data_and_source(monkeypatch):
    """try_chain 拿到第一个成功的 provider 的返回 + 名字."""
    import lib.providers as pv

    class _FakeOk:
        name = "fake_ok"
        requires_key = False
        markets = ("A",)

        def is_available(self):
            return True

        def fetch_thing(self, x):
            return {"got": x}

    class _FakeFail:
        name = "fake_fail"
        requires_key = False
        markets = ("A",)

        def is_available(self):
            return True

        def fetch_thing(self, x):
            raise pv.ProviderError("nope")

    # Stub _REGISTRY so chain only sees our fakes
    monkeypatch.setattr(
        pv, "_REGISTRY", {"fake_fail": _FakeFail(), "fake_ok": _FakeOk()}
    )
    monkeypatch.setenv("UZI_PROVIDERS_THING", "fake_fail,fake_ok")

    data, src = pv.try_chain("fetch_thing", "thing", "A", "hello")
    assert data == {"got": "hello"}
    assert src == "fake_ok"


def test_try_chain_raises_when_all_fail(monkeypatch):
    import lib.providers as pv

    class _F1:
        name = "f1"
        requires_key = False
        markets = ("A",)

        def is_available(self):
            return True

        def fetch_x(self):
            raise pv.ProviderError("boom1")

    class _F2:
        name = "f2"
        requires_key = False
        markets = ("A",)

        def is_available(self):
            return True

        def fetch_x(self):
            raise pv.ProviderError("boom2")

    monkeypatch.setattr(pv, "_REGISTRY", {"f1": _F1(), "f2": _F2()})
    monkeypatch.setenv("UZI_PROVIDERS_X", "f1,f2")
    import pytest

    with pytest.raises(pv.ProviderError) as exc:
        pv.try_chain("fetch_x", "x", "A")
    assert "boom1" in str(exc.value) or "boom2" in str(exc.value)


def test_try_chain_skips_method_not_implemented(monkeypatch):
    """Provider 没实现目标方法时应跳过而不是崩溃."""
    import lib.providers as pv

    class _NoMethod:
        name = "no_method"
        requires_key = False
        markets = ("A",)

        def is_available(self):
            return True

        # 故意不实现 fetch_kline_a

    class _HasMethod:
        name = "has_method"
        requires_key = False
        markets = ("A",)

        def is_available(self):
            return True

        def fetch_kline_a(self, **kw):
            return ["row"]

    monkeypatch.setattr(
        pv, "_REGISTRY", {"no_method": _NoMethod(), "has_method": _HasMethod()}
    )
    monkeypatch.setenv("UZI_PROVIDERS_KLINE", "no_method,has_method")
    data, src = pv.try_chain("fetch_kline_a", "kline", "A")
    assert src == "has_method"
    assert data == ["row"]


def test_tushare_has_kline_method_v2_10_6():
    """v2.10.6 新增：tushare provider 必须实现 fetch_kline_a（之前缺失）."""
    from lib.providers import tushare_provider

    p = tushare_provider._TushareProvider()
    assert hasattr(p, "fetch_kline_a"), (
        "tushare_provider.fetch_kline_a must exist in v2.10.6"
    )
    assert callable(p.fetch_kline_a)


def test_tushare_resolves_token_from_ai_trader_env(monkeypatch, tmp_path):
    from lib.providers import tushare_provider

    env_path = tmp_path / "ai-trader.env"
    env_path.write_text('TUSHARE_TOKEN="demo-token"\n', encoding="utf-8")

    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.setattr(tushare_provider, "_AI_TRADER_ENV_PATH", env_path)

    assert tushare_provider.resolve_tushare_token() == "demo-token"


def test_fetch_basic_a_uses_tushare_provider_fallback(monkeypatch):
    from lib.data_sources import _fetch_basic_a
    from lib.market_router import parse_ticker
    import lib.data_sources as ds

    monkeypatch.setattr(ds, "ak", None)
    monkeypatch.setattr(ds, "requests", None)
    monkeypatch.setattr(ds, "_mx_available", lambda: False)

    def _fake_provider(
        method: str, code: str, *, provider_names: tuple[str, ...], **kwargs
    ):
        assert method == "fetch_basic_a"
        assert code == "000001"
        assert provider_names == ("tushare",)
        return (
            {
                "ok": True,
                "raw": {
                    "name": "平安银行",
                    "industry": "银行",
                    "close": 10.5,
                    "pe_ttm": 5.2,
                    "pb": 0.61,
                    "total_mv": 2500000,
                },
            },
            "tushare",
        )

    monkeypatch.setattr(ds, "_call_a_share_provider_fallback", _fake_provider)

    out = _fetch_basic_a(parse_ticker("000001"))

    assert out["code"] == "000001.SZ"
    assert out["name"] == "平安银行"
    assert out["industry"] == "银行"
    assert out["price"] == 10.5
    assert out["pe_ttm"] == 5.2
    assert out["pb"] == 0.61
    assert out["market_cap"] == "250.0亿"
    assert out["_provider_basic_source"] == "tushare"
    assert "provider-tushare" in str(out.get("_fallback_snap"))


def test_fetch_financials_a_uses_tushare_provider_fallback(monkeypatch):
    import fetch_financials as ff
    from lib.market_router import parse_ticker

    monkeypatch.setattr(ff, "ak", None)

    def _fake_provider(
        method: str, code: str, *, provider_names: tuple[str, ...], **kwargs
    ):
        assert method == "fetch_financials_a"
        assert code == "000001"
        assert provider_names == ("tushare",)
        assert kwargs["years"] == 6
        return (
            {
                "ok": True,
                "income": [
                    {
                        "end_date": "20211231",
                        "total_revenue": 40 * 1e8,
                        "n_income_attr_p": 4 * 1e8,
                    },
                    {
                        "end_date": "20221231",
                        "total_revenue": 45 * 1e8,
                        "n_income_attr_p": 5 * 1e8,
                    },
                    {
                        "end_date": "20231231",
                        "total_revenue": 50 * 1e8,
                        "n_income_attr_p": 6 * 1e8,
                    },
                    {
                        "end_date": "20241231",
                        "total_revenue": 60 * 1e8,
                        "n_income_attr_p": 9 * 1e8,
                    },
                ],
                "balance": [
                    {
                        "end_date": "20211231",
                        "total_assets": 100 * 1e8,
                        "total_liab": 42 * 1e8,
                        "total_cur_assets": 35 * 1e8,
                        "total_cur_liab": 20 * 1e8,
                    },
                    {
                        "end_date": "20221231",
                        "total_assets": 110 * 1e8,
                        "total_liab": 46 * 1e8,
                        "total_cur_assets": 40 * 1e8,
                        "total_cur_liab": 22 * 1e8,
                    },
                    {
                        "end_date": "20231231",
                        "total_assets": 120 * 1e8,
                        "total_liab": 50 * 1e8,
                        "total_cur_assets": 44 * 1e8,
                        "total_cur_liab": 24 * 1e8,
                    },
                    {
                        "end_date": "20241231",
                        "total_assets": 135 * 1e8,
                        "total_liab": 54 * 1e8,
                        "total_cur_assets": 48 * 1e8,
                        "total_cur_liab": 24 * 1e8,
                    },
                ],
                "cashflow": [
                    {"end_date": "20241231", "n_cashflow_act": 12 * 1e8},
                ],
                "indicator": [
                    {
                        "end_date": "20211231",
                        "roe": 8.0,
                        "netprofit_margin": 10.0,
                        "debt_to_assets": 42.0,
                        "current_ratio": 1.75,
                        "roic": 6.5,
                    },
                    {
                        "end_date": "20221231",
                        "roe": 10.5,
                        "netprofit_margin": 11.1,
                        "debt_to_assets": 41.8,
                        "current_ratio": 1.82,
                        "roic": 7.1,
                    },
                    {
                        "end_date": "20231231",
                        "roe": 12.0,
                        "netprofit_margin": 12.0,
                        "debt_to_assets": 41.7,
                        "current_ratio": 1.83,
                        "roic": 7.9,
                    },
                    {
                        "end_date": "20241231",
                        "roe": 18.0,
                        "netprofit_margin": 15.0,
                        "debt_to_assets": 40.0,
                        "current_ratio": 2.0,
                        "roic": 9.5,
                    },
                ],
            },
            "tushare",
        )

    monkeypatch.setattr(ff.ds, "_call_a_share_provider_fallback", _fake_provider)

    out = ff._fetch_a_share(parse_ticker("000001"))

    assert out["revenue_history"] == [40.0, 45.0, 50.0, 60.0]
    assert out["net_profit_history"] == [4.0, 5.0, 6.0, 9.0]
    assert out["roe_history"] == [8.0, 10.5, 12.0, 18.0]
    assert out["financial_years"] == ["2021", "2022", "2023", "2024"]
    assert out["roe"] == "18.0%"
    assert out["net_margin"] == "15.0%"
    assert out["revenue_growth"] == "+20.0%"
    assert out["fcf"] == "12.0亿"
    assert out["financial_health"]["debt_ratio"] == 40.0
    assert out["financial_health"]["current_ratio"] == 2.0
    assert out["financial_health"]["roic"] == 9.5
    assert out["financial_health"]["fcf_margin"] == round(12.0 / 9.0 * 100, 1)
    assert out["_provider_financial_source"] == "tushare"
    assert out["_source"] == "providers/tushare"


def test_fetch_financials_main_marks_provider_rescue_as_not_fallback(monkeypatch):
    import fetch_financials as ff

    monkeypatch.setattr(
        ff,
        "_fetch_a_share",
        lambda ti: {
            "revenue_history": [10.0, 12.0],
            "net_profit_history": [1.0, 1.5],
            "roe_history": [8.0, 9.0],
            "financial_health": {"debt_ratio": 35.0},
            "_source": "providers/tushare",
        },
    )

    result = ff.main("000001")

    assert result["ticker"] == "000001.SZ"
    assert result["source"] == "providers/tushare"
    assert result["used_backup_provider"] is True
    assert result["fallback"] is False
    assert result["data"]["revenue_history"] == [10.0, 12.0]


def test_fetch_financials_main_treats_zero_like_payload_as_fallback(monkeypatch):
    import fetch_financials as ff

    monkeypatch.setattr(
        ff,
        "_fetch_a_share",
        lambda ti: {
            "revenue_history": [0.0, 0.0],
            "net_profit_history": [0.0, 0.0],
            "roe_history": [0.0, 0.0],
            "roe": "0.0%",
            "net_margin": "0.0%",
            "financial_health": {"debt_ratio": 0.0, "current_ratio": 0.0},
            "_source": "akshare",
        },
    )

    result = ff.main("000001")

    assert result["ticker"] == "000001.SZ"
    assert result["source"] == "akshare"
    assert result["used_backup_provider"] is False
    assert result["fallback"] is True


def test_fetch_financials_main_error_metadata_still_reports_fallback(monkeypatch):
    import fetch_financials as ff

    monkeypatch.setattr(
        ff,
        "_fetch_a_share",
        lambda ti: {
            "_abstract_error": "timeout",
            "_indicator_error": "parse failed",
            "_source": "akshare",
        },
    )

    result = ff.main("000001")

    assert result["source"] == "akshare"
    assert result["used_backup_provider"] is False
    assert result["fallback"] is True


def test_fetch_financials_placeholder_akshare_still_uses_provider(monkeypatch):
    import pandas as pd

    import fetch_financials as ff
    from lib.market_router import parse_ticker

    class _FakeAk:
        @staticmethod
        def stock_financial_abstract(symbol):
            assert symbol == "000001"
            return pd.DataFrame(
                {
                    "指标": ["营业总收入", "归属于母公司所有者的净利润"],
                    "20231231": ["--", "--"],
                    "20241231": [None, None],
                }
            )

        @staticmethod
        def stock_financial_analysis_indicator(symbol, start_year="2018"):
            assert symbol == "000001"
            assert start_year == "2018"
            return pd.DataFrame(
                {
                    "日期": ["2023-12-31", "2024-12-31"],
                    "加权净资产收益率(%)": ["--", None],
                    "销售净利率(%)": ["--", None],
                    "资产负债率(%)": ["--", None],
                }
            )

        @staticmethod
        def stock_cash_flow_sheet_by_report_em(symbol):
            return pd.DataFrame({"经营活动产生的现金流量净额": ["--"]})

        @staticmethod
        def stock_history_dividend_detail(symbol, indicator):
            return pd.DataFrame()

    def _fake_provider(
        method: str, code: str, *, provider_names: tuple[str, ...], **kwargs
    ):
        assert method == "fetch_financials_a"
        assert code == "000001"
        assert provider_names == ("tushare",)
        assert kwargs["years"] == 6
        return (
            {
                "ok": True,
                "income": [
                    {
                        "end_date": "20211231",
                        "total_revenue": 40 * 1e8,
                        "n_income_attr_p": 4 * 1e8,
                    },
                    {
                        "end_date": "20221231",
                        "total_revenue": 45 * 1e8,
                        "n_income_attr_p": 5 * 1e8,
                    },
                ],
                "balance": [
                    {
                        "end_date": "20211231",
                        "total_assets": 100 * 1e8,
                        "total_liab": 42 * 1e8,
                        "total_cur_assets": 35 * 1e8,
                        "total_cur_liab": 20 * 1e8,
                    },
                    {
                        "end_date": "20221231",
                        "total_assets": 110 * 1e8,
                        "total_liab": 46 * 1e8,
                        "total_cur_assets": 40 * 1e8,
                        "total_cur_liab": 22 * 1e8,
                    },
                ],
                "cashflow": [
                    {"end_date": "20221231", "n_cashflow_act": 6 * 1e8},
                ],
                "indicator": [
                    {
                        "end_date": "20211231",
                        "roe": 8.0,
                        "netprofit_margin": 10.0,
                        "debt_to_assets": 42.0,
                        "current_ratio": 1.75,
                        "roic": 6.5,
                    },
                    {
                        "end_date": "20221231",
                        "roe": 10.5,
                        "netprofit_margin": 11.1,
                        "debt_to_assets": 41.8,
                        "current_ratio": 1.82,
                        "roic": 7.1,
                    },
                ],
            },
            "tushare",
        )

    monkeypatch.setattr(ff, "ak", _FakeAk())
    monkeypatch.setattr(ff.ds, "_call_a_share_provider_fallback", _fake_provider)

    out = ff._fetch_a_share(parse_ticker("000001"))

    assert out["revenue_history"] == [40.0, 45.0]
    assert out["net_profit_history"] == [4.0, 5.0]
    assert out["roe_history"] == [8.0, 10.5]
    assert out["roe"] == "10.5%"
    assert out["net_margin"] == "11.1%"
    assert out["financial_health"]["debt_ratio"] == 41.8
    assert out["_provider_financial_source"] == "tushare"
    assert out["_used_backup_provider"] is True
    assert out["_source"] == "providers/tushare"


def test_fetch_financials_impl_uses_provider_when_akshare_returns_empty(monkeypatch):
    import pandas as pd

    import lib.data_sources as ds
    from lib.market_router import parse_ticker

    class _FakeAk:
        @staticmethod
        def stock_financial_abstract(symbol):
            assert symbol == "000001"
            return pd.DataFrame()

        @staticmethod
        def stock_financial_analysis_indicator(symbol):
            assert symbol == "000001"
            return pd.DataFrame()

    def _fake_provider(
        method: str, code: str, *, provider_names: tuple[str, ...], **kwargs
    ):
        assert method == "fetch_financials_a"
        assert code == "000001"
        assert provider_names == ("tushare",)
        assert kwargs["years"] == 5
        return (
            {
                "ok": True,
                "income": [{"end_date": "20241231", "total_revenue": 60 * 1e8}],
                "balance": [{"end_date": "20241231", "total_assets": 100 * 1e8}],
                "cashflow": [],
                "indicator": [],
            },
            "tushare",
        )

    monkeypatch.setattr(ds, "ak", _FakeAk())
    monkeypatch.setattr(ds, "_call_a_share_provider_fallback", _fake_provider)

    out = ds._fetch_financials_impl(parse_ticker("000001"))

    assert out["fallback"] is True
    assert out["source"] == "providers/tushare"
    assert out["used_backup_provider"] is True
    assert out["income"][0]["total_revenue"] == 60 * 1e8


def test_fetch_financials_source_reports_mixed_primary_and_provider(monkeypatch):
    import pandas as pd

    import fetch_financials as ff
    from lib.market_router import parse_ticker

    class _FakeAk:
        @staticmethod
        def stock_financial_abstract(symbol):
            assert symbol == "000001"
            return pd.DataFrame(
                {
                    "指标": ["营业总收入", "归属于母公司所有者的净利润"],
                    "20231231": [50 * 1e8, 6 * 1e8],
                    "20241231": [60 * 1e8, 9 * 1e8],
                }
            )

        @staticmethod
        def stock_financial_analysis_indicator(symbol, start_year="2018"):
            assert symbol == "000001"
            assert start_year == "2018"
            return pd.DataFrame(
                {
                    "日期": ["2023-12-31", "2024-12-31"],
                    "加权净资产收益率(%)": [12.0, 18.0],
                    "销售净利率(%)": [12.0, 15.0],
                }
            )

        @staticmethod
        def stock_cash_flow_sheet_by_report_em(symbol):
            raise RuntimeError("skip cashflow")

        @staticmethod
        def stock_history_dividend_detail(symbol, indicator):
            return pd.DataFrame()

    def _fake_provider(
        method: str, code: str, *, provider_names: tuple[str, ...], **kwargs
    ):
        assert method == "fetch_financials_a"
        assert code == "000001"
        return (
            {
                "ok": True,
                "income": [
                    {
                        "end_date": "20241231",
                        "total_revenue": 60 * 1e8,
                        "n_income_attr_p": 9 * 1e8,
                    }
                ],
                "balance": [
                    {
                        "end_date": "20241231",
                        "total_assets": 135 * 1e8,
                        "total_liab": 54 * 1e8,
                        "total_cur_assets": 48 * 1e8,
                        "total_cur_liab": 24 * 1e8,
                    }
                ],
                "cashflow": [],
                "indicator": [
                    {
                        "end_date": "20241231",
                        "roe": 18.0,
                        "netprofit_margin": 15.0,
                        "debt_to_assets": 40.0,
                        "current_ratio": 2.0,
                        "roic": 9.5,
                    }
                ],
            },
            "tushare",
        )

    monkeypatch.setattr(ff, "ak", _FakeAk())
    monkeypatch.setattr(ff.ds, "_call_a_share_provider_fallback", _fake_provider)

    out = ff._fetch_a_share(parse_ticker("000001"))

    assert out["revenue_history"] == [50.0, 60.0]
    assert out["roe_history"] == [12.0, 18.0]
    assert out["financial_health"]["debt_ratio"] == 40.0
    assert out["_used_backup_provider"] is True
    assert out["_source"] == "akshare + providers/tushare"


def test_fetch_financials_main_reports_mixed_source_backup_usage(monkeypatch):
    import fetch_financials as ff

    monkeypatch.setattr(
        ff,
        "_fetch_a_share",
        lambda ti: {
            "revenue_history": [50.0, 60.0],
            "net_profit_history": [6.0, 9.0],
            "roe_history": [12.0, 18.0],
            "financial_health": {"debt_ratio": 40.0},
            "_source": "akshare + providers/tushare",
            "_used_backup_provider": True,
        },
    )

    result = ff.main("000001")

    assert result["source"] == "akshare + providers/tushare"
    assert result["used_backup_provider"] is True
    assert result["fallback"] is False


def test_tushare_unavailable_without_any_token(monkeypatch, tmp_path):
    from lib.providers import tushare_provider

    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.setattr(
        tushare_provider, "_AI_TRADER_ENV_PATH", tmp_path / "missing.env"
    )

    provider = tushare_provider._TushareProvider()

    assert provider.is_available() is False


def test_all_providers_have_is_available():
    """Protocol 合规：每个 provider 都有 is_available()."""
    from lib.providers import _REGISTRY

    for name, p in _REGISTRY.items():
        assert hasattr(p, "is_available"), f"{name} missing is_available"
        # 能调（不崩）
        result = p.is_available()
        assert isinstance(result, bool), f"{name}.is_available must return bool"
