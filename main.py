from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import date, timedelta
import csv
import io
import json
import logging
import os
import time
import calendar
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import ccxt
import yfinance as yf
import pandas as pd
import numpy as np
import ta
import firebase_admin
from firebase_admin import auth as firebase_auth, credentials as firebase_credentials
from firebase_admin import firestore as firebase_firestore

app = FastAPI()
BACKTEST_ENGINE_VERSION = "risk-costs-v2"
MAX_ASYNC_TRADE_LOGS = 10000
FIRESTORE_TRADE_LOGS_PER_DOCUMENT = 200
_firestore_client = None

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
MAX_BACKTEST_YEARS = 20


def _earliest_supported_start_date(end_date: date) -> date:
    year = end_date.year - MAX_BACKTEST_YEARS
    day = min(end_date.day, calendar.monthrange(year, end_date.month)[1])
    return date(year, end_date.month, day)


def _validate_backtest_date_range(start_date: date, end_date: date) -> Optional[str]:
    earliest_start_date = _earliest_supported_start_date(end_date)
    if start_date < earliest_start_date:
        return (
            f"Backtests are limited to the most recent {MAX_BACKTEST_YEARS} years "
            f"for the selected end date (earliest start: {earliest_start_date})."
        )
    return None


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
                f"This {req.timeframe} backtest requires more than "
                f"{MAX_BACKTEST_CANDLES:,} candles, above the server's safe limit. "
                "Keep the selected timeframe unchanged and choose a shorter date range."
            )
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
            f"This {req.timeframe} backtest requires more than "
            f"{MAX_BACKTEST_CANDLES:,} candles, above the server's safe limit. "
            "Keep the selected timeframe unchanged and choose a shorter date range."
        )
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


def _get_firestore_client():
    global _firestore_client
    if _firestore_client is not None:
        return _firestore_client
    service_account_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
    if not service_account_json:
        raise RuntimeError("FIREBASE_SERVICE_ACCOUNT_JSON is not configured.")
    try:
        firebase_app = firebase_admin.get_app()
    except ValueError:
        service_account = json.loads(service_account_json)
        firebase_app = firebase_admin.initialize_app(
            firebase_credentials.Certificate(service_account)
        )
    _firestore_client = firebase_firestore.client(firebase_app)
    return _firestore_client


def _authenticated_uid(authorization: Optional[str]) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Firebase sign-in is required.")
    try:
        decoded_token = firebase_auth.verify_id_token(authorization[7:])
    except Exception as error:
        raise HTTPException(status_code=401, detail="Invalid or expired sign-in token.") from error
    uid = decoded_token.get("uid")
    if not uid:
        raise HTTPException(status_code=401, detail="The sign-in token has no user ID.")
    return uid


def _dispatch_long_backtest(job_id: str) -> None:
    token = os.environ.get("GITHUB_ACTIONS_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_ACTIONS_TOKEN is not configured.")
    owner = os.environ.get("GITHUB_ACTIONS_OWNER", "krytexgaming07-eng")
    repository = os.environ.get("GITHUB_ACTIONS_REPOSITORY", "nd-backtest-backend")
    request = Request(
        f"https://api.github.com/repos/{owner}/{repository}/dispatches",
        data=json.dumps({
            "event_type": "run-long-backtest",
            "client_payload": {"job_id": job_id},
        }).encode("utf-8"),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
            "User-Agent": "nd-backtest",
        },
        method="POST",
    )
    with urlopen(request, timeout=15) as response:
        if response.status != 204:
            raise RuntimeError(f"GitHub Actions dispatch returned HTTP {response.status}.")


@app.post("/backtest-jobs")
def create_backtest_job(
    req: AdvancedBacktestRequest,
    authorization: Optional[str] = Header(default=None),
):
    uid = _authenticated_uid(authorization)
    if req.mode != "indicator":
        raise HTTPException(
            status_code=400,
            detail="Long Binance archive jobs support indicator mode only; custom code is disabled.",
        )
    if req.data_source != "binance_archive" or req.exchange.lower() != "binance":
        raise HTTPException(
            status_code=400,
            detail="Long jobs require the explicitly selected Binance Spot archive source.",
        )
    if req.symbol.upper().replace("/", "") != "BTCUSDT":
        raise HTTPException(
            status_code=400,
            detail="The free historical archive worker currently supports BTC/USDT only.",
        )
    if req.timeframe != "1m":
        raise HTTPException(
            status_code=400,
            detail="The long Binance archive worker currently accepts 1-minute backtests only.",
        )
    if req.timeframe not in TIMEFRAME_SETTINGS:
        raise HTTPException(status_code=400, detail=f"Unsupported timeframe: {req.timeframe}.")

    end_date = req.end_date or date.today()
    start_date = req.start_date or end_date - timedelta(days=30)
    if end_date > date.today():
        raise HTTPException(status_code=400, detail="Backtest end date cannot be in the future.")
    date_range_error = _validate_backtest_date_range(start_date, end_date)
    if start_date > end_date or date_range_error:
        raise HTTPException(
            status_code=400,
            detail=date_range_error or "Start date must be on or before end date.",
        )

    job_id = uuid.uuid4().hex
    firestore = _get_firestore_client()
    job_reference = firestore.collection("backtest_jobs").document(job_id)
    active_reference = firestore.collection("active_backtest_jobs").document(uid)
    active_snapshot = active_reference.get()
    active_job_id = (active_snapshot.to_dict() or {}).get("job_id") if active_snapshot.exists else None
    if active_job_id:
        active_job = firestore.collection("backtest_jobs").document(active_job_id).get()
        if active_job.exists and (active_job.to_dict() or {}).get("status") in {
            "queued", "downloading", "calculating",
        }:
            raise HTTPException(
                status_code=429,
                detail="You already have a long backtest running. Wait for it to finish.",
            )
    job_reference.set({
        "uid": uid,
        "status": "queued",
        "progress_pct": 0,
        "message": "Queued for free GitHub Actions processing.",
        "request": req.dict(),
        "created_at": firebase_firestore.SERVER_TIMESTAMP,
        "updated_at": firebase_firestore.SERVER_TIMESTAMP,
    })
    active_reference.set({"job_id": job_id, "status": "queued"})
    try:
        _dispatch_long_backtest(job_id)
    except Exception as error:
        job_reference.update({
            "status": "failed",
            "message": f"Could not start the worker: {error}",
            "updated_at": firebase_firestore.SERVER_TIMESTAMP,
        })
        active_reference.set({"job_id": job_id, "status": "failed"})
        raise HTTPException(
            status_code=503,
            detail="Could not start the free backtest worker. Check GitHub Actions configuration.",
        ) from error
    return {"status": "queued", "job_id": job_id}


@app.get("/backtest-jobs/{job_id}")
def get_backtest_job(
    job_id: str,
    authorization: Optional[str] = Header(default=None),
):
    uid = _authenticated_uid(authorization)
    snapshot = _get_firestore_client().collection("backtest_jobs").document(job_id).get()
    if not snapshot.exists:
        raise HTTPException(status_code=404, detail="Backtest job was not found.")
    job = snapshot.to_dict() or {}
    if job.get("uid") != uid:
        raise HTTPException(status_code=403, detail="This backtest job belongs to another user.")
    response = {
        "status": job.get("status"),
        "progress_pct": job.get("progress_pct", 0),
        "message": job.get("message", ""),
    }
    if job.get("status") == "completed":
        response["result"] = job.get("result", {})
        response["trade_log_count"] = job.get("trade_log_count", 0)
        response["trade_logs_truncated"] = job.get("trade_logs_truncated", False)
    return response


@app.get("/backtest-jobs/{job_id}/trades")
def get_backtest_job_trades(
    job_id: str,
    offset: int = 0,
    limit: int = 200,
    authorization: Optional[str] = Header(default=None),
):
    uid = _authenticated_uid(authorization)
    if offset < 0 or limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="Use offset >= 0 and limit between 1 and 500.")
    job_reference = _get_firestore_client().collection("backtest_jobs").document(job_id)
    snapshot = job_reference.get()
    if not snapshot.exists:
        raise HTTPException(status_code=404, detail="Backtest job was not found.")
    job = snapshot.to_dict() or {}
    if job.get("uid") != uid:
        raise HTTPException(status_code=403, detail="This backtest job belongs to another user.")
    if job.get("status") != "completed":
        raise HTTPException(status_code=409, detail="The backtest is not complete.")

    first_chunk = offset // FIRESTORE_TRADE_LOGS_PER_DOCUMENT
    last_chunk = (offset + limit - 1) // FIRESTORE_TRADE_LOGS_PER_DOCUMENT
    trades = []
    for chunk_index in range(first_chunk, last_chunk + 1):
        chunk = (
            job_reference.collection("trades")
            .document(f"{chunk_index:06d}")
            .get()
        )
        if chunk.exists:
            trades.extend((chunk.to_dict() or {}).get("items", []))
    start_in_page = offset % FIRESTORE_TRADE_LOGS_PER_DOCUMENT
    trades = trades[start_in_page : start_in_page + limit]
    total = int(job.get("trade_log_count", 0))
    return {
        "trades": trades,
        "offset": offset,
        "limit": limit,
        "total": total,
        "has_more": offset + len(trades) < total,
    }


@app.post("/run-backtest")
def run_backtest(req: AdvancedBacktestRequest):
    return _run_backtest(req)


def _run_backtest(
    req: AdvancedBacktestRequest,
    data_override: Optional[pd.DataFrame] = None,
    allow_large_dataset: bool = False,
):
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
        date_range_error = _validate_backtest_date_range(start_date, end_date)
        if date_range_error:
            return {"status": "error", "message": date_range_error}
        if req.data_source != "ccxt" and data_override is None:
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
        if data_override is not None:
            df = data_override.copy()
        elif req.data_source == "ccxt":
            df = _fetch_ccxt_history(req, start_date, end_date)
        else:
            df = _fetch_yfinance_history(req, start_date, end_date)
        if df.empty:
            return {
                "status": "error",
                "message": f"No {req.timeframe} data for {req.symbol} from {start_date} to {end_date}.",
            }
        df["datetime"] = pd.to_datetime(df["datetime"])

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
        open_values = df["open"].to_numpy()
        high_values = df["high"].to_numpy()
        low_values = df["low"].to_numpy()
        close_values = df["close"].to_numpy()
        time_values = df["datetime"].to_numpy()
        signal_values = df["exec_signal"].to_numpy()

        # ३. व्हेक्टरायझेशन आणि सिमुलेशन (ट्रेड लॉग आणि इक्विटी कर्व्ह)
        capital = req.capital
        initial_capital = req.capital
        peak_capital = capital
        max_drawdown = 0.0

        equity_curve = []
        equity_stride = max(1, len(df) // 50)
        trade_logs = []
        trade_logs_truncated = False
        
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
            curr_open = open_values[i]
            curr_time = time_values[i]
            curr_close = close_values[i]
            sig = signal_values[i]

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
                hit_stop = curr_open <= stop_price or low_values[i] <= stop_price
                hit_target = curr_open >= target_price or high_values[i] >= target_price
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

                    if (
                        not allow_large_dataset
                        or len(trade_logs) < MAX_ASYNC_TRADE_LOGS
                    ):
                        trade_logs.append({
                            "entry_time": pd.Timestamp(entry_time).strftime("%Y-%m-%d %H:%M"),
                            "exit_time": pd.Timestamp(curr_time).strftime("%Y-%m-%d %H:%M"),
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
                    else:
                        trade_logs_truncated = True
                    in_trade = False

            # Drawdown & Equity Tracking
            if capital > peak_capital:
                peak_capital = capital
            dd = (peak_capital - capital) / peak_capital * 100 if peak_capital > 0 else 0
            if dd > max_drawdown:
                max_drawdown = dd

            if i % equity_stride == 0 or i == len(df) - 1:
                equity_curve.append({
                    "time": pd.Timestamp(curr_time).strftime("%Y-%m-%d %H:%M"),
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
            "equity_curve": equity_curve,
            "trade_logs": trade_logs,
            "trade_logs_truncated": trade_logs_truncated,
        }

    except Exception as e:
        return {"status": "error", "message": f"Execution Error: {str(e)}"}