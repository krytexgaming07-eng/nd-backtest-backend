"""Download and parse public Binance Spot one-minute archive files."""

from __future__ import annotations

import calendar
import io
import zipfile
from datetime import date, datetime, timedelta, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pandas as pd

ARCHIVE_BASE_URL = "https://data.binance.vision/data/spot"
CSV_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_base_volume",
    "taker_quote_volume",
    "ignore",
]
OHLCV_COLUMNS = ["datetime", "open", "high", "low", "close", "volume"]


def _download_zip(url: str) -> bytes:
    request = Request(
        url,
        headers={"User-Agent": "NDBacktest/1.0", "Accept": "application/zip"},
    )
    with urlopen(request, timeout=60) as response:
        return response.read()


def _parse_kline_zip(content: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        csv_files = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(csv_files) != 1:
            raise ValueError("Expected exactly one CSV file in a Binance archive.")
        with archive.open(csv_files[0]) as csv_file:
            frame = pd.read_csv(
                csv_file,
                header=None,
                names=CSV_COLUMNS,
                usecols=range(6),
            )

    frame["open_time"] = pd.to_numeric(frame["open_time"], errors="coerce")
    frame = frame.dropna(subset=["open_time", "open", "high", "low", "close", "volume"])
    if frame.empty:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    timestamp_unit = "us" if frame["open_time"].max() >= 1_000_000_000_000_000 else "ms"
    frame["datetime"] = pd.to_datetime(
        frame["open_time"],
        unit=timestamp_unit,
        utc=True,
    ).dt.tz_localize(None)
    return (
        frame[OHLCV_COLUMNS]
        .drop_duplicates(subset=["datetime"])
        .sort_values("datetime")
        .reset_index(drop=True)
    )


def _iter_archive_urls(start_date: date, end_date: date):
    start_date = max(start_date, date(2017, 8, 17))
    if start_date > end_date:
        return
    current_month = date(start_date.year, start_date.month, 1)
    today_utc = datetime.now(timezone.utc).date()
    last_complete_day = min(end_date, today_utc - timedelta(days=1))
    while current_month <= end_date:
        month_last_day = calendar.monthrange(current_month.year, current_month.month)[1]
        month_end = date(current_month.year, current_month.month, month_last_day)
        if current_month.year == today_utc.year and current_month.month == today_utc.month:
            day = max(start_date, current_month)
            while day <= last_complete_day:
                filename = f"BTCUSDT-1m-{day:%Y-%m-%d}.zip"
                yield f"{ARCHIVE_BASE_URL}/daily/klines/BTCUSDT/1m/{filename}", day, day
                day += timedelta(days=1)
        else:
            filename = f"BTCUSDT-1m-{current_month:%Y-%m}.zip"
            yield (
                f"{ARCHIVE_BASE_URL}/monthly/klines/BTCUSDT/1m/{filename}",
                current_month,
                min(month_end, end_date),
            )
        if current_month.month == 12:
            current_month = date(current_month.year + 1, 1, 1)
        else:
            current_month = date(current_month.year, current_month.month + 1, 1)


def load_btcusdt_1m(
    start_date: date,
    end_date: date,
    progress_callback=None,
) -> pd.DataFrame:
    if start_date > end_date:
        raise ValueError("Start date must be on or before end date.")

    frames = []
    archive_urls = list(_iter_archive_urls(start_date, end_date))
    if not archive_urls:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    for index, (url, archive_start, archive_end) in enumerate(archive_urls, start=1):
        try:
            frame = _parse_kline_zip(_download_zip(url))
        except HTTPError as error:
            if error.code == 404 and archive_start < date(2017, 8, 1):
                continue
            raise RuntimeError(
                f"Binance archive unavailable for {archive_start}: HTTP {error.code}."
            ) from error
        if not frame.empty:
            frames.append(frame)
        if progress_callback is not None and (index % 12 == 0 or index == len(archive_urls)):
            progress_callback(index, len(archive_urls), archive_end.isoformat())

    if not frames:
        return pd.DataFrame(columns=OHLCV_COLUMNS)
    data = (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["datetime"])
        .sort_values("datetime")
        .reset_index(drop=True)
    )
    start = pd.Timestamp(start_date)
    end_exclusive = pd.Timestamp(end_date + timedelta(days=1))
    return data[(data["datetime"] >= start) & (data["datetime"] < end_exclusive)].reset_index(drop=True)


def resample_ohlcv(data: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    if timeframe == "1m":
        return data
    rules = {
        "3m": "3min",
        "5m": "5min",
        "15m": "15min",
        "30m": "30min",
        "1h": "1h",
        "2h": "2h",
        "3h": "3h",
        "5h": "5h",
        "1d": "1D",
        "7d": "7D",
        "14d": "14D",
        "1mo": "MS",
        "3mo": "3MS",
        "6mo": "6MS",
        "1y": "YS",
    }
    rule = rules.get(timeframe)
    if rule is None:
        raise ValueError(f"Unsupported Binance archive timeframe: {timeframe}.")
    return (
        data.set_index("datetime")
        .resample(rule)
        .agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        })
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )
