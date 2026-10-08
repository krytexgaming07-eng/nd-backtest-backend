"""GitHub Actions worker for long Binance Spot archive backtests."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import date

import firebase_admin
from firebase_admin import credentials, firestore

import main
from binance_archive import load_btcusdt_1m, resample_ohlcv

TRADE_LOGS_PER_DOCUMENT = main.FIRESTORE_TRADE_LOGS_PER_DOCUMENT


def _get_firestore():
    try:
        app = firebase_admin.get_app()
    except ValueError:
        account_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")
        if not account_json:
            raise RuntimeError("FIREBASE_SERVICE_ACCOUNT_JSON secret is not configured.")
        app = firebase_admin.initialize_app(
            credentials.Certificate(json.loads(account_json))
        )
    return firestore.client(app)


def _save_trades(job_reference, trades):
    for offset in range(0, len(trades), TRADE_LOGS_PER_DOCUMENT):
        chunk_number = offset // TRADE_LOGS_PER_DOCUMENT
        chunk = trades[offset : offset + TRADE_LOGS_PER_DOCUMENT]
        job_reference.collection("trades").document(f"{chunk_number:06d}").set({
            "items": chunk,
            "offset": offset,
        })


def process_job(job_id: str) -> None:
    database = _get_firestore()
    job_reference = database.collection("backtest_jobs").document(job_id)
    snapshot = job_reference.get()
    if not snapshot.exists:
        raise RuntimeError(f"Backtest job {job_id} does not exist.")
    job = snapshot.to_dict() or {}
    if job.get("status") in {"completed", "failed"}:
        logging.info("Job %s is already %s; skipping.", job_id, job.get("status"))
        return

    request_data = job.get("request")
    if not isinstance(request_data, dict):
        raise RuntimeError("Backtest job does not contain a request.")
    request_data["data_source"] = "binance_archive"
    request_data["exchange"] = "binance"
    request_data["symbol"] = "BTC/USDT"
    request = main.AdvancedBacktestRequest(**request_data)
    if request.mode != "indicator":
        raise RuntimeError("Long archive worker only accepts indicator mode.")
    start_date = request.start_date or date.today()
    active_reference = database.collection("active_backtest_jobs").document(job["uid"])
    active_reference.set({"job_id": job_id, "status": "downloading"})
    end_date = request.end_date or date.today()

    job_reference.update({
        "status": "downloading",
        "progress_pct": 0,
        "message": "Downloading Binance public monthly archive files.",
        "updated_at": firestore.SERVER_TIMESTAMP,
    })

    def update_progress(index, total, through_date):
        job_reference.update({
            "progress_pct": min(70, int(index * 70 / total)),
            "message": f"Downloaded archive data through {through_date}.",
            "updated_at": firestore.SERVER_TIMESTAMP,
        })

    one_minute_data = load_btcusdt_1m(
        start_date,
        end_date,
        progress_callback=update_progress,
    )
    if one_minute_data.empty:
        raise RuntimeError("Binance has no BTC/USDT data for the selected date range.")
    actual_start = one_minute_data["datetime"].min().date().isoformat()
    actual_end = one_minute_data["datetime"].max().date().isoformat()
    data = resample_ohlcv(one_minute_data, request.timeframe)
    job_reference.update({
        "status": "calculating",
        "progress_pct": 75,
        "message": f"Calculating {len(data):,} {request.timeframe} candles.",
        "actual_start_date": actual_start,
        "actual_end_date": actual_end,
        "updated_at": firestore.SERVER_TIMESTAMP,
    })

    result = main._run_backtest(
        request,
        data_override=data,
        allow_large_dataset=True,
    )
    if result.get("status") != "success":
        raise RuntimeError(result.get("message", "Backtest calculation failed."))

    trade_logs = result.pop("trade_logs", [])
    trade_logs_truncated = result.pop("trade_logs_truncated", False)
    _save_trades(job_reference, trade_logs)
    result["trade_logs"] = []
    result["trade_logs_truncated"] = trade_logs_truncated
    result["data_source"] = "Binance Spot public archive"
    result["requested_start_date"] = start_date.isoformat()
    result["requested_end_date"] = end_date.isoformat()
    result["actual_start_date"] = actual_start
    result["actual_end_date"] = actual_end
    if actual_start > start_date.isoformat():
        result["history_warning"] = (
            f"BTC/USDT data is available from {actual_start}; "
            "no earlier market data exists for this Binance Spot pair."
        )

    job_reference.update({
        "status": "completed",
        "progress_pct": 100,
        "message": "Backtest completed.",
        "result": result,
        "trade_log_count": len(trade_logs),
        "trade_logs_truncated": trade_logs_truncated,
        "updated_at": firestore.SERVER_TIMESTAMP,
    })
    active_reference.set({"job_id": job_id, "status": "completed"})


def main_cli() -> int:
    if len(sys.argv) != 2:
        logging.error("Usage: python backtest_worker.py JOB_ID")
        return 2
    job_id = sys.argv[1]
    try:
        process_job(job_id)
    except Exception as error:
        logging.exception("Long backtest job failed: %s", error)
        try:
            reference = _get_firestore().collection("backtest_jobs").document(job_id)
            job_snapshot = reference.get()
            reference.update({
                "status": "failed",
                "message": str(error),
                "updated_at": firestore.SERVER_TIMESTAMP,
            })
            job_data = job_snapshot.to_dict() or {}
            uid = job_data.get("uid")
            if uid:
                _get_firestore().collection("active_backtest_jobs").document(uid).set({
                    "job_id": job_id,
                    "status": "failed",
                })
        except Exception:
            logging.exception("Could not persist worker failure status.")
        return 1
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main_cli())
