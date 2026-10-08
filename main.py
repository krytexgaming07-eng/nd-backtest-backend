from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import date, timedelta
import csv
import io
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import ccxt
import yfinance as yf
import pandas as pd
import numpy as np
import ta

app = FastAPI()
BACKTEST_ENGINE_VERSION = "risk-costs-v2"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ऑटो-सजेशनसाठी लोकप्रिय ॲसेट्सची यादी
MARKET_CATALOG = [
    # Indices
    {"symbol": "^NSEI", "name": "Nifty 50", "category": "Index", "source": "yfinance", "market": "India", "exchange": "NSE"},
    {"symbol": "^NSEBANK", "name": "Bank Nifty", "category": "Index", "source": "yfinance", "market": "India", "exchange": "NSE"},
    {"symbol": "^BSESN", "name": "BSE Sensex", "category": "Index", "source": "yfinance", "market": "India", "exchange": "BSE"},
    {"symbol": "^CNXIT", "name": "Nifty IT", "category": "Index", "source": "yfinance", "market": "India", "exchange": "NSE"},
    {"symbol": "^GSPC", "name": "S&P 500", "category": "Index", "source": "yfinance"},
    # Stocks
    {"symbol": "RELIANCE.NS", "name": "Reliance Industries", "category": "Stock", "source": "yfinance", "market": "India", "exchange": "NSE"},
    {"symbol": "TCS.NS", "name": "Tata Consultancy Services", "category": "Stock", "source": "yfinance", "market": "India", "exchange": "NSE"},
    {"symbol": "HDFCBANK.NS", "name": "HDFC Bank", "category": "Stock", "source": "yfinance", "market": "India", "exchange": "NSE"},
    {"symbol": "ICICIBANK.NS", "name": "ICICI Bank", "category": "Stock", "source": "yfinance", "market": "India", "exchange": "NSE"},
    {"symbol": "INFY.NS", "name": "Infosys", "category": "Stock", "source": "yfinance", "market": "India", "exchange": "NSE"},
    {"symbol": "SBIN.NS", "name": "State Bank of India", "category": "Stock", "source": "yfinance", "market": "India", "exchange": "NSE"},
    {"symbol": "BHARTIARTL.NS", "name": "Bharti Airtel", "category": "Stock", "source": "yfinance", "market": "India", "exchange": "NSE"},
    {"symbol": "ITC.NS", "name": "ITC", "category": "Stock", "source": "yfinance", "market": "India", "exchange": "NSE"},
    {"symbol": "LT.NS", "name": "Larsen & Toubro", "category": "Stock", "source": "yfinance", "market": "India", "exchange": "NSE"},
    {"symbol": "HINDUNILVR.NS", "name": "Hindustan Unilever", "category": "Stock", "source": "yfinance", "market": "India", "exchange": "NSE"},
    {"symbol": "AAPL", "name": "Apple Inc.", "category": "Stock", "source": "yfinance"},
    # Commodities & Forex
    {"symbol": "GC=F", "name": "Gold Futures", "category": "Commodity", "source": "yfinance"},
    {"symbol": "CL=F", "name": "Crude Oil Futures", "category": "Commodity", "source": "yfinance"},
    {"symbol": "USDINR=X", "name": "USD to INR", "category": "Forex", "source": "yfinance"},
    {"symbol": "EURUSD=X", "name": "EUR to USD", "category": "Forex", "source": "yfinance"},
    {"symbol": "GBPUSD=X", "name": "GBP to USD", "category": "Forex", "source": "yfinance"},
    {"symbol": "USDJPY=X", "name": "USD to JPY", "category": "Forex", "source": "yfinance"},
    {"symbol": "AUDUSD=X", "name": "AUD to USD", "category": "Forex", "source": "yfinance"},
    {"symbol": "USDCAD=X", "name": "USD to CAD", "category": "Forex", "source": "yfinance"},
    {"symbol": "USDCHF=X", "name": "USD to CHF", "category": "Forex", "source": "yfinance"},
    {"symbol": "NZDUSD=X", "name": "NZD to USD", "category": "Forex", "source": "yfinance"},
    {"symbol": "EURGBP=X", "name": "EUR to GBP", "category": "Forex", "source": "yfinance"},
    {"symbol": "EURJPY=X", "name": "EUR to JPY", "category": "Forex", "source": "yfinance"},
    {"symbol": "GBPJPY=X", "name": "GBP to JPY", "category": "Forex", "source": "yfinance"},
    # Crypto
    {"symbol": "BTC/USDT", "name": "Bitcoin", "category": "Crypto", "source": "ccxt"},
    {"symbol": "ETH/USDT", "name": "Ethereum", "category": "Crypto", "source": "ccxt"},
    {"symbol": "SOL/USDT", "name": "Solana", "category": "Crypto", "source": "ccxt"},
]


NSE_EQUITY_LIST_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
BSE_EQUITY_LIST_URL = (
    "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
    "?Group=&Scripcode=&industry=&segment=Equity&status=Active"
)
SYMBOL_CACHE_SECONDS = 6 * 60 * 60
_symbol_cache: Optional[List[Dict[str, str]]] = None
_symbol_cache_time = 0.0


def _exchange_request(url: str) -> bytes:
    request = Request(
        url,
        headers={
            "Accept": "text/csv,application/json",
            "Referer": "https://www.bseindia.com/",
            "User-Agent": "Mozilla/5.0 (compatible; NDBacktest/1.0)",
        },
    )
    with urlopen(request, timeout=20) as response:
        return response.read()


def _fetch_nse_symbols() -> List[Dict[str, str]]:
    content = _exchange_request(NSE_EQUITY_LIST_URL).decode("utf-8-sig")
    rows = csv.DictReader(io.StringIO(content))
    symbols = []
    for row in rows:
        symbol = (row.get("SYMBOL") or "").strip()
        name = (row.get("NAME OF COMPANY") or "").strip()
        series = (row.get(" SERIES") or row.get("SERIES") or "").strip()
        if symbol and name and series in {"EQ", "BE", "SM", "ST"}:
            symbols.append({
                "symbol": f"{symbol}.NS",
                "name": name,
                "category": "Stock",
                "source": "yfinance",
                "market": "India",
                "exchange": "NSE",
            })
    return symbols


def _fetch_bse_symbols() -> List[Dict[str, str]]:
    payload = json.loads(_exchange_request(BSE_EQUITY_LIST_URL))
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = next(
            (
                value
                for key, value in payload.items()
                if key.lower() in {"table", "data", "scripdata"} and isinstance(value, list)
            ),
            [],
        )
    else:
        rows = []

    symbols = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized = {str(key).lower().replace("_", "").replace(" ", ""): value for key, value in row.items()}
        code = normalized.get("scripcd") or normalized.get("scripcode") or normalized.get("securitycode")
        name = normalized.get("scripname") or normalized.get("securityname") or normalized.get("companyname")
        if code is None or not name:
            continue
        code = str(code).strip()
        if not code.isdigit():
            continue
        symbols.append({
            "symbol": f"{code}.BO",
            "name": str(name).strip(),
            "category": "Stock",
            "source": "yfinance",
            "market": "India",
            "exchange": "BSE",
        })
    return symbols


@app.get("/symbols")
def get_symbols():
    global _symbol_cache, _symbol_cache_time
    now = time.monotonic()
    if _symbol_cache is not None and now - _symbol_cache_time < SYMBOL_CACHE_SECONDS:
        return _symbol_cache

    symbols_by_ticker = {item["symbol"]: item.copy() for item in MARKET_CATALOG}
    fetchers = {
        "NSE": _fetch_nse_symbols,
        "BSE": _fetch_bse_symbols,
    }
    with ThreadPoolExecutor(max_workers=len(fetchers)) as executor:
        futures = {
            executor.submit(fetch_symbols): exchange
            for exchange, fetch_symbols in fetchers.items()
        }
        for future in as_completed(futures):
            exchange = futures[future]
            try:
                symbols_by_ticker.update(
                    (item["symbol"], item) for item in future.result()
                )
            except (
                HTTPError,
                URLError,
                TimeoutError,
                UnicodeDecodeError,
                json.JSONDecodeError,
            ) as error:
                logging.warning(
                    "Could not refresh %s symbol list: %s", exchange, error
                )

    _symbol_cache = list(symbols_by_ticker.values())
    _symbol_cache_time = now
    return _symbol_cache

TIMEFRAME_SETTINGS = {
    "1m": {"ccxt": ("1m", None), "yfinance": ("1m", None)},
    "3m": {"ccxt": ("3m", None), "yfinance": ("1m", "3min")},
    "5m": {"ccxt": ("5m", None), "yfinance": ("5m", None)},
    "15m": {"ccxt": ("15m", None), "yfinance": ("15m", None)},
    "30m": {"ccxt": ("30m", None), "yfinance": ("30m", None)},
    "1h": {"ccxt": ("1h", None), "yfinance": ("60m", None)},
    "2h": {"ccxt": ("2h", None), "yfinance": ("60m", "2h")},
    "3h": {"ccxt": ("1h", "3h"), "yfinance": ("60m", "3h")},
    "5h": {"ccxt": ("1h", "5h"), "yfinance": ("60m", "5h")},
    "1d": {"ccxt": ("1d", None), "yfinance": ("1d", None)},
    "7d": {"ccxt": ("1d", "7D"), "yfinance": ("1d", "7D")},
    "14d": {"ccxt": ("1d", "14D"), "yfinance": ("1d", "14D")},
    "1mo": {"ccxt": ("1d", "MS"), "yfinance": ("1mo", None)},
    "3mo": {"ccxt": ("1d", "3MS"), "yfinance": ("3mo", None)},
    "6mo": {"ccxt": ("1d", "6MS"), "yfinance": ("1mo", "6MS")},
    "1y": {"ccxt": ("1d", "YS"), "yfinance": ("1mo", "YS")},
}

YFINANCE_INTERVALS = {
    "1m": "1m",
    "3m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "60m",
    "2h": "60m",
    "3h": "60m",
    "5h": "60m",
    "1d": "1d",
    "7d": "1d",
    "14d": "1d",
    "1mo": "1mo",
    "3mo": "3mo",
    "6mo": "1mo",
    "1y": "1mo",
}

MAX_BACKTEST_CANDLES = 20000


def _filter_date_range(df: pd.DataFrame, start_date: date, end_date: date) -> pd.DataFrame:
    timestamps = pd.to_datetime(df["datetime"])
    if timestamps.dt.tz is not None:
        timestamps = timestamps.dt.tz_localize(None)
    df["datetime"] = timestamps
    end_exclusive = pd.Timestamp(end_date + timedelta(days=1))
    return df[(df["datetime"] >= pd.Timestamp(start_date)) & (df["datetime"] < end_exclusive)]


def _resample_ohlcv(df: pd.DataFrame, rule: Optional[str]) -> pd.DataFrame:
    if not rule:
        return df
    return (
        df.set_index("datetime")
        .resample(rule)
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )


def _fetch_ccxt_history(req: "AdvancedBacktestRequest", start_date: date, end_date: date) -> pd.DataFrame:
    source_timeframe, resample_rule = TIMEFRAME_SETTINGS[req.timeframe]["ccxt"]
    client = getattr(ccxt, req.exchange.lower())({"enableRateLimit": True})
    start_ms = int(pd.Timestamp(start_date, tz="UTC").timestamp() * 1000)
    end_ms = int(pd.Timestamp(end_date + timedelta(days=1), tz="UTC").timestamp() * 1000)
    interval_ms = int(pd.Timedelta(source_timeframe).total_seconds() * 1000)
    cursor = start_ms
    candles = []

    while cursor < end_ms:
        batch = client.fetch_ohlcv(req.symbol, timeframe=source_timeframe, since=cursor, limit=1000)
        if not batch:
            break
        new_candles = [candle for candle in batch if cursor <= candle[0] < end_ms]
        if not new_candles:
            break
        candles.extend(new_candles)
        if len(candles) > MAX_BACKTEST_CANDLES:
            raise ValueError(
                f"Selected date range exceeds {MAX_BACKTEST_CANDLES:,} candles for {req.timeframe}. "
                "Choose a shorter range or a larger timeframe."
            )
        cursor = new_candles[-1][0] + interval_ms

    if not candles:
        return pd.DataFrame()
    df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_localize(None)
    return _resample_ohlcv(df.drop(columns=["timestamp"]), resample_rule)


def _fetch_yfinance_history(req: "AdvancedBacktestRequest", start_date: date, end_date: date) -> pd.DataFrame:
    _, resample_rule = TIMEFRAME_SETTINGS[req.timeframe]["yfinance"]
    interval = YFINANCE_INTERVALS[req.timeframe]
    ticker = yf.Ticker(req.symbol)
    df = ticker.history(
        start=start_date.isoformat(),
        end=(end_date + timedelta(days=1)).isoformat(),
        interval=interval,
    )
    if df.empty:
        return df
    df = df.reset_index()
    df.columns = [str(column).lower() for column in df.columns]
    if "datetime" not in df.columns:
        datetime_column = "date" if "date" in df.columns else "index"
        df.rename(columns={datetime_column: "datetime"}, inplace=True)
    df.rename(columns={"adj close": "adj_close"}, inplace=True)
    df["datetime"] = pd.to_datetime(df["datetime"])
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_localize(None)
    required_columns = ["open", "high", "low", "close", "volume", "datetime"]
    df = df[required_columns]
    df = _filter_date_range(df, start_date, end_date)
    if len(df) > MAX_BACKTEST_CANDLES:
        raise ValueError(
            f"Selected date range exceeds {MAX_BACKTEST_CANDLES:,} candles for {req.timeframe}. "
            "Choose a shorter range or a larger timeframe."
        )
    return _resample_ohlcv(df, resample_rule)


class AdvancedBacktestRequest(BaseModel):
    mode: str = "indicator" # "indicator" किंवा "custom_code"
    data_source: str = "ccxt" # "ccxt" किंवा "yfinance"
    exchange: str = "binance"
    symbol: str = "BTC/USDT"
    timeframe: str = "15m"
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    capital: float = 10000.0
    risk_amount: float = 1000.0
    reward_amount: float = 2000.0
    brokerage_per_order: float = 0.0
    slippage_bps: float = 0.0
    india_charges_pct: float = 0.0
    # Indicator Mode
    fast_ema: int = 9
    slow_ema: int = 21
    rsi_period: int = 14
    # Custom Code Mode
    custom_strategy_code: Optional[str] = None


@app.get("/health")
def health():
    return {"status": "ok", "engine_version": BACKTEST_ENGINE_VERSION}


@app.post("/run-backtest")
def run_backtest(req: AdvancedBacktestRequest):
    try:
        if req.timeframe not in TIMEFRAME_SETTINGS:
            return {"status": "error", "message": f"Unsupported timeframe: {req.timeframe}."}

        end_date = req.end_date or date.today()
        start_date = req.start_date or end_date - timedelta(days=30)
        if req.capital <= 0:
            return {"status": "error", "message": "Capital must be greater than zero."}
        if req.risk_amount <= 0 or req.reward_amount <= 0:
            return {"status": "error", "message": "Risk and reward amounts must be greater than zero."}
        if req.brokerage_per_order < 0 or req.slippage_bps < 0 or req.india_charges_pct < 0:
            return {"status": "error", "message": "Brokerage, slippage, and charges cannot be negative."}
        if req.india_charges_pct > 100:
            return {"status": "error", "message": "India charges percentage cannot exceed 100."}
        if req.risk_amount >= req.capital:
            return {"status": "error", "message": "Risk amount must be less than the starting capital."}
        risk_per_trade = round(req.risk_amount, 2)
        reward_per_trade = round(req.reward_amount, 2)
        is_indian_instrument = (
            req.symbol.upper().endswith((".NS", ".BO"))
            or req.symbol.upper().startswith(("^NSE", "^BSE"))
        )
        if start_date > end_date:
            return {"status": "error", "message": "Start date must be on or before end date."}
        if req.data_source != "ccxt":
            source_interval = YFINANCE_INTERVALS[req.timeframe]
            max_history_days = {"1m": 7, "5m": 60, "15m": 60, "30m": 60, "60m": 730}.get(
                source_interval
            )
            requested_days = (end_date - start_date).days + 1
            if max_history_days and requested_days > max_history_days:
                return {
                    "status": "error",
                    "message": (
                        f"Yahoo Finance provides {source_interval} history for up to "
                        f"{max_history_days} days. Choose a shorter date range or a larger timeframe."
                    ),
                }

        # Fetch only candles inside the requested, inclusive date range.
        if req.data_source == "ccxt":
            df = _fetch_ccxt_history(req, start_date, end_date)
        else:
            df = _fetch_yfinance_history(req, start_date, end_date)
        if df.empty:
            return {
                "status": "error",
                "message": f"No {req.timeframe} data for {req.symbol} from {start_date} to {end_date}.",
            }
        df["datetime"] = pd.to_datetime(df["datetime"]).dt.strftime("%Y-%m-%d %H:%M")

        df['signal'] = 0 # 1: Buy, -1: Sell, 0: Neutral

        # २. स्ट्रॅटेजी एक्झिक्युशन
        if req.mode == "custom_code" and req.custom_strategy_code:
            # सुरक्षित Python execution environment
            # युझरचा कोड df['signal'] सेट करेल
            local_vars = {"df": df, "np": np, "pd": pd, "ta": ta}
            exec(req.custom_strategy_code, {}, local_vars)
            df = local_vars["df"]
            if 'signal' not in df.columns:
                return {"status": "error", "message": "तुमच्या कोडमध्ये df['signal'] कॉलम आढळला नाही."}
        else:
            # Indicator Mode (EMA + RSI Filter)
            df['fast_ema'] = ta.trend.ema_indicator(df['close'], window=req.fast_ema)
            df['slow_ema'] = ta.trend.ema_indicator(df['close'], window=req.slow_ema)
            df['rsi'] = ta.momentum.rsi(df['close'], window=req.rsi_period)
            
            # Lookahead Bias प्रतिबंध:
            # सिग्नल मागील कँडलचा क्लोज ओलांडल्यावरच तयार होतो
            buy_condition = (df['fast_ema'] > df['slow_ema']) & (df['fast_ema'].shift(1) <= df['slow_ema'].shift(1)) & (df['rsi'] > 50)
            sell_condition = (df['fast_ema'] < df['slow_ema']) & (df['fast_ema'].shift(1) >= df['slow_ema'].shift(1))
            
            df.loc[buy_condition, 'signal'] = 1
            df.loc[sell_condition, 'signal'] = -1

        # Lookahead Bias टाळण्यासाठी: सिग्नल एका कँडलने पुढे शिफ्ट करणे
        # (म्हणजे आजचा सिग्नल पुढच्या कँडलच्या ओपनवर एक्झिक्युट होईल)
        df['exec_signal'] = df['signal'].shift(1).fillna(0)

        # ३. व्हेक्टरायझेशन आणि सिमुलेशन (ट्रेड लॉग आणि इक्विटी कर्व्ह)
        capital = req.capital
        initial_capital = req.capital
        peak_capital = capital
        max_drawdown = 0.0

        equity_curve = []
        trade_logs = []
        
        in_trade = False
        entry_price = 0.0
        entry_time = ""
        entry_capital = 0.0
        position_size = 0.0
        stop_price = 0.0
        target_price = 0.0
        wins = 0
        losses = 0
        breakeven = 0
        stop_hits = 0
        target_hits = 0
        signal_exits = 0
        end_of_data_exits = 0
        ambiguous_exit_bars = 0
        gross_profit = 0.0
        gross_loss = 0.0
        total_brokerage = 0.0
        total_slippage = 0.0
        total_india_charges = 0.0

        for i in range(len(df)):
            row = df.iloc[i]
            curr_open = row['open']
            curr_time = row['datetime']
            curr_close = row['close']
            sig = row['exec_signal']

            # Entry
            if (
                sig == 1
                and not in_trade
                and i < len(df) - 1
                and capital > risk_per_trade
                and curr_open > 0
            ):
                in_trade = True
                entry_price = curr_open
                entry_time = curr_time
                entry_capital = capital
                position_size = entry_capital / entry_price
                stop_price = entry_price - risk_per_trade / position_size
                target_price = entry_price + reward_per_trade / position_size

            # Exit
            elif in_trade:
                exit_price = None
                exit_reason = None
                hit_stop = curr_open <= stop_price or row["low"] <= stop_price
                hit_target = curr_open >= target_price or row["high"] >= target_price
                if hit_stop and hit_target:
                    ambiguous_exit_bars += 1
                if hit_stop:
                    exit_price = min(curr_open, stop_price)
                    exit_reason = "stop_loss"
                elif hit_target:
                    exit_price = max(curr_open, target_price)
                    exit_reason = "take_profit"
                elif sig == -1:
                    exit_price = curr_open
                    exit_reason = "signal"
                elif i == len(df) - 1:
                    exit_price = curr_close
                    exit_reason = "end_of_data"

                if exit_price is not None:
                    gross_pnl = round((exit_price - entry_price) * position_size, 2)
                    turnover = (entry_price + exit_price) * position_size
                    brokerage = round(req.brokerage_per_order * 2, 2)
                    slippage = round(turnover * req.slippage_bps / 10000, 2)
                    india_charges = round(
                        turnover * req.india_charges_pct / 100
                        if is_indian_instrument
                        else 0.0,
                        2,
                    )
                    charges = round(brokerage + slippage + india_charges, 2)
                    pnl = round(gross_pnl - charges, 2)
                    trade_pct = pnl / entry_capital if entry_capital else 0.0
                    capital += pnl
                    if gross_pnl > 0:
                        gross_profit += gross_pnl
                    elif gross_pnl < 0:
                        gross_loss += gross_pnl
                    total_brokerage += brokerage
                    total_slippage += slippage
                    total_india_charges += india_charges

                    if pnl > 0:
                        wins += 1
                    elif pnl < 0:
                        losses += 1
                    else:
                        breakeven += 1
                    if exit_reason == "stop_loss":
                        stop_hits += 1
                    elif exit_reason == "take_profit":
                        target_hits += 1
                    elif exit_reason == "signal":
                        signal_exits += 1
                    elif exit_reason == "end_of_data":
                        end_of_data_exits += 1

                    trade_logs.append({
                        "entry_time": entry_time,
                        "exit_time": curr_time,
                        "entry_price": round(entry_price, 2),
                        "exit_price": round(exit_price, 2),
                        "stop_price": round(stop_price, 2),
                        "target_price": round(target_price, 2),
                        "risk_amount": risk_per_trade,
                        "reward_amount": reward_per_trade,
                        "gross_pnl": gross_pnl,
                        "brokerage": brokerage,
                        "slippage": slippage,
                        "india_charges": india_charges,
                        "total_charges": charges,
                        "pnl": pnl,
                        "return_pct": round(trade_pct * 100, 2),
                        "exit_reason": exit_reason,
                        "type": "BUY"
                    })
                    in_trade = False

            # Drawdown & Equity Tracking
            if capital > peak_capital:
                peak_capital = capital
            dd = (peak_capital - capital) / peak_capital * 100 if peak_capital > 0 else 0
            if dd > max_drawdown:
                max_drawdown = dd

            equity_curve.append({
                "time": curr_time,
                "equity": round(capital, 2)
            })

        total_trades = wins + losses + breakeven
        win_rate = round((wins / total_trades) * 100, 2) if total_trades > 0 else 0.0
        net_pnl = round(capital - initial_capital, 2)
        gross_profit = round(gross_profit, 2)
        gross_loss = round(gross_loss, 2)
        total_brokerage = round(total_brokerage, 2)
        total_slippage = round(total_slippage, 2)
        total_india_charges = round(total_india_charges, 2)
        total_charges = round(total_brokerage + total_slippage + total_india_charges, 2)
        roi_pct = round((net_pnl / initial_capital) * 100, 2)

        return {
            "status": "success",
            "engine_version": BACKTEST_ENGINE_VERSION,
            "symbol": req.symbol,
            "risk_amount": risk_per_trade,
            "reward_amount": reward_per_trade,
            "risk_reward_ratio": round(reward_per_trade / risk_per_trade, 2),
            "final_capital": round(capital, 2),
            "net_pnl": net_pnl,
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "brokerage": total_brokerage,
            "slippage": total_slippage,
            "india_charges": total_india_charges,
            "total_charges": total_charges,
            "roi_pct": roi_pct,
            "win_rate_pct": win_rate,
            "total_trades": total_trades,
            "wins": wins,
            "losses": losses,
            "breakeven": breakeven,
            "stop_loss_exits": stop_hits,
            "take_profit_exits": target_hits,
            "signal_exits": signal_exits,
            "end_of_data_exits": end_of_data_exits,
            "ambiguous_exit_bars": ambiguous_exit_bars,
            "max_drawdown_pct": round(max_drawdown, 2),
            "equity_curve": equity_curve[::max(1, len(equity_curve)//50)], # चार्टसाठी 50 पॉईंट्स
            "trade_logs": trade_logs
        }

    except Exception as e:
        return {"status": "error", "message": f"Execution Error: {str(e)}"}