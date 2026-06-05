#!/usr/bin/env python3
"""Build the live trader data package for the public dashboard.

This script is intentionally runnable without secrets. In `offline` mode it
rebuilds the live summary from the checked-in CSV files. When BscScan/RPC
credentials and the trader wallet are added, the same state files can be used
for incremental onchain sync and receipt decoding.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlencode
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW = DATA / "raw"
STATE = DATA / "state"


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: Iterable[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def short_addr(value: str) -> str:
    if not value:
        return ""
    return value if len(value) <= 16 else f"{value[:8]}...{value[-6:]}"


def load_env() -> dict[str, str]:
    env = dict(os.environ)
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env.setdefault(key.strip(), value.strip())
    return env


def fetch_bscscan_txs(wallet: str, api_key: str, start_block: int = 0) -> list[dict[str, Any]]:
    """Fetch ordinary transactions from BscScan.

    The decoder still requires receipt/log parsing before these become
    Four.meme trade facts. This function is kept separate so server deployment
    can enable it without touching the report generation code.
    """
    if not wallet or wallet == "0x0000000000000000000000000000000000000000":
        raise ValueError("BSC_TRADER_WALLET is not configured")
    if not api_key:
        raise ValueError("BSCSCAN_API_KEY is not configured")
    query = urlencode(
        {
            "module": "account",
            "action": "txlist",
            "address": wallet,
            "startblock": start_block,
            "endblock": 999999999,
            "sort": "asc",
            "apikey": api_key,
        }
    )
    with urlopen(f"https://api.bscscan.com/api?{query}", timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if payload.get("status") not in {"1", 1}:
        raise RuntimeError(f"BscScan error: {payload.get('message')} {payload.get('result')}")
    return payload.get("result", [])


@dataclass
class LivePackage:
    summary: dict[str, Any]
    recent_buys: list[dict[str, Any]]
    recent_positions: list[dict[str, Any]]
    daily: list[dict[str, Any]]


def build_offline_package() -> LivePackage:
    buys = read_csv(DATA / "buy_detail_with_fdv.csv")
    tokens = read_csv(DATA / "token_trade_summary.csv")
    daily = read_csv(DATA / "daily_trade_summary.csv")

    total_pnl = sum(to_float(row.get("realized_pnl_bnb")) for row in tokens)
    open_positions = sum(1 for row in tokens if str(row.get("open_position_flag", "")).lower() == "true")
    winners = sum(1 for row in tokens if to_float(row.get("realized_pnl_bnb")) > 0)
    bucket_counts = Counter(row.get("first_buy_bucket") or "unknown" for row in tokens)
    last_trade = max((row.get("last_trade_time_utc", "") for row in tokens), default="")
    first_buy = min((row.get("first_buy_time_utc", "") for row in tokens if row.get("first_buy_time_utc")), default="")

    recent_buys = sorted(buys, key=lambda row: row.get("time_utc", ""), reverse=True)[:50]
    recent_buys_view = [
        {
            "time_utc": row.get("time_utc", ""),
            "event": "buy",
            "token": row.get("token", ""),
            "token_short": short_addr(row.get("token", "")),
            "symbol": row.get("symbol") or row.get("name") or short_addr(row.get("token", "")),
            "bnb_amount": to_float(row.get("bnb_amount")),
            "usd_amount": to_float(row.get("usd_amount")),
            "fdv_usd": to_float(row.get("trade_fdv_usd")),
            "bucket": row.get("trade_fdv_bucket", ""),
            "tx_hash": row.get("tx_hash", ""),
            "verification": "verified_from_decoded_csv",
        }
        for row in recent_buys
    ]

    recent_positions = sorted(tokens, key=lambda row: row.get("last_trade_time_utc", ""), reverse=True)[:80]
    recent_positions_view = [
        {
            "last_trade_time_utc": row.get("last_trade_time_utc", ""),
            "first_buy_time_utc": row.get("first_buy_time_utc", ""),
            "token": row.get("token", ""),
            "token_short": short_addr(row.get("token", "")),
            "symbol": row.get("symbol_or_addr") or row.get("name") or short_addr(row.get("token", "")),
            "buy_count": int(to_float(row.get("buy_count"))),
            "sell_count": int(to_float(row.get("sell_count"))),
            "buy_bnb": to_float(row.get("buy_bnb")),
            "sell_bnb": to_float(row.get("sell_bnb")),
            "realized_pnl_bnb": to_float(row.get("realized_pnl_bnb")),
            "realized_roi": to_float(row.get("realized_roi")),
            "first_buy_fdv_usd": to_float(row.get("first_buy_fdv_usd")),
            "first_buy_bucket": row.get("first_buy_bucket", ""),
            "open_position_flag": str(row.get("open_position_flag", "")).lower() == "true",
            "verification": "token_level_reconstruction",
        }
        for row in recent_positions
    ]

    daily_view = [
        {
            "date": row.get("date", ""),
            "buys": int(to_float(row.get("buys"))),
            "sells": int(to_float(row.get("sells"))),
            "buy_bnb": to_float(row.get("buy_bnb")),
            "sell_bnb": to_float(row.get("sell_bnb")),
        }
        for row in daily[-60:]
    ]

    summary = {
        "generated_at_utc": utc_now(),
        "mode": "offline_csv_rebuild",
        "sync_status": "ready_for_server_credentials",
        "tracked_wallet": os.environ.get("BSC_TRADER_WALLET", ""),
        "data_window": {"first_buy_time_utc": first_buy, "last_trade_time_utc": last_trade},
        "token_count": len(tokens),
        "buy_event_count": len(buys),
        "open_position_count": open_positions,
        "realized_pnl_bnb": round(total_pnl, 8),
        "win_rate": round(winners / len(tokens), 6) if tokens else 0,
        "dominant_bucket": bucket_counts.most_common(1)[0][0] if bucket_counts else "",
        "bucket_counts": dict(bucket_counts),
        "next_required_inputs": [
            "BSC_TRADER_WALLET",
            "BSCSCAN_API_KEY",
            "BSC_RPC_URL",
            "Four.meme event decoder contract/topic map",
            "server cron or systemd timer",
        ],
    }
    return LivePackage(summary, recent_buys_view, recent_positions_view, daily_view)


def run_sync(mode: str) -> LivePackage:
    env = load_env()
    os.environ.update(env)
    if mode == "bscscan":
        wallet = env.get("BSC_TRADER_WALLET", "")
        api_key = env.get("BSCSCAN_API_KEY", "")
        last_block_path = STATE / "last_bscscan_block.txt"
        start_block = int(last_block_path.read_text().strip()) if last_block_path.exists() else 0
        txs = fetch_bscscan_txs(wallet, api_key, start_block=start_block)
        write_json(RAW / "bscscan_txlist_latest.json", {"generated_at_utc": utc_now(), "transactions": txs})
        if txs:
            last_block_path.write_text(str(max(int(tx["blockNumber"]) for tx in txs)), encoding="utf-8")
        package = build_offline_package()
        package.summary["mode"] = "bscscan_txlist_fetched_receipt_decode_pending"
        package.summary["bscscan_new_tx_count"] = len(txs)
        package.summary["sync_status"] = "txlist_fetched_receipt_decoder_pending"
        return package
    return build_offline_package()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build live Four.meme trader dashboard data")
    parser.add_argument("--mode", choices=["offline", "bscscan"], default=os.environ.get("FOURMEME_PROGRAM_MODE", "offline"))
    args = parser.parse_args()

    package = run_sync(args.mode)
    write_json(DATA / "live_summary.json", package.summary)
    write_json(
        DATA / "live_trades.json",
        {
            "generated_at_utc": package.summary["generated_at_utc"],
            "recent_buys": package.recent_buys,
            "recent_positions": package.recent_positions,
            "daily": package.daily,
        },
    )
    write_csv(
        DATA / "live_recent_buys.csv",
        package.recent_buys,
        ["time_utc", "event", "token", "symbol", "bnb_amount", "usd_amount", "fdv_usd", "bucket", "tx_hash", "verification"],
    )
    write_csv(
        DATA / "live_recent_positions.csv",
        package.recent_positions,
        [
            "last_trade_time_utc",
            "first_buy_time_utc",
            "token",
            "symbol",
            "buy_count",
            "sell_count",
            "buy_bnb",
            "sell_bnb",
            "realized_pnl_bnb",
            "realized_roi",
            "first_buy_fdv_usd",
            "first_buy_bucket",
            "open_position_flag",
            "verification",
        ],
    )
    print(json.dumps(package.summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

