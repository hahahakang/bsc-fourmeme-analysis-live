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
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.request import Request

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW = DATA / "raw"
STATE = DATA / "state"

DEFAULT_TRADER_WALLET = "0x55976c6818e4794f3e2e7179eea2cc2202811e11"
DEFAULT_RPC_URL = "https://bsc.rpc.blxrbdn.com"
FALLBACK_RPC_URLS = [
    "https://bsc.rpc.blxrbdn.com",
    "https://binance-smart-chain-public.nodies.app",
    "https://rpc-bsc.48.club",
    "https://bnb-mainnet.g.alchemy.com/public",
    "https://bsc-dataseed.binance.org/",
]
FOURMEME_SWAP_CONTRACT = "0x1de460f363af910f51726def188f9004276bf4bc"
FOURMEME_SWAP_TOPIC = "0x8619026a40d38bedb4002fe511cea4bc4a9b336710efe8f21a61869a7ee0f02a"
WBNB_ADDRESS = "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"
DEFAULT_START_BLOCK = 101648164
CONFIRMATION_BLOCKS = 12


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


def topic_address(address: str) -> str:
    clean = address.lower().replace("0x", "")
    return "0x" + clean.rjust(64, "0")


def int_address(value: int) -> str:
    if value == 0:
        return ""
    return "0x" + f"{value:064x}"[-40:]


def iso_from_block_ts(timestamp_hex: str) -> str:
    ts = int(timestamp_hex, 16)
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


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


def rpc_call(rpc_url: str, method: str, params: list[Any], retries: int = 3) -> Any:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode("utf-8")
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            req = Request(
                rpc_url,
                data=payload,
                headers={"Content-Type": "application/json", "User-Agent": "bsc-fourmeme-live-sync"},
            )
            with urlopen(req, timeout=40) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            if data.get("error"):
                raise RuntimeError(data["error"])
            return data.get("result")
        except (URLError, TimeoutError, RuntimeError) as exc:
            last_error = exc
            if attempt < retries - 1:
                time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"RPC {method} failed: {last_error}")


def choose_rpc_url(preferred_url: str, wallet: str) -> str:
    candidates = []
    if preferred_url:
        candidates.append(preferred_url)
    candidates.extend(url for url in FALLBACK_RPC_URLS if url not in candidates)
    topic_wallet = topic_address(wallet)
    for url in candidates:
        try:
            current_block = int(rpc_call(url, "eth_blockNumber", []), 16)
            test_from = max(DEFAULT_START_BLOCK, current_block - 100)
            rpc_call(
                url,
                "eth_getLogs",
                [
                    {
                        "fromBlock": hex(test_from),
                        "toBlock": hex(test_from + 5),
                        "address": FOURMEME_SWAP_CONTRACT,
                        "topics": [FOURMEME_SWAP_TOPIC, topic_wallet],
                    }
                ],
                retries=1,
            )
            return url
        except RuntimeError:
            continue
    raise RuntimeError("No usable BSC RPC endpoint for eth_getLogs")


def get_block_time_cache(rpc_url: str, block_numbers: Iterable[int]) -> dict[int, str]:
    cache_path = STATE / "block_time_cache.json"
    if cache_path.exists():
        cache = {int(k): v for k, v in json.loads(cache_path.read_text(encoding="utf-8")).items()}
    else:
        cache = {}
    for block_number in sorted(set(block_numbers)):
        if block_number in cache:
            continue
        block = rpc_call(rpc_url, "eth_getBlockByNumber", [hex(block_number), False])
        if block:
            cache[block_number] = iso_from_block_ts(block["timestamp"])
    write_json(cache_path, {str(k): v for k, v in sorted(cache.items())})
    return cache


def decode_words(data_hex: str) -> list[int]:
    data_hex = data_hex[2:] if data_hex.startswith("0x") else data_hex
    if not data_hex:
        return []
    return [int(data_hex[i : i + 64], 16) for i in range(0, len(data_hex), 64)]


def decode_swap_log(log: dict[str, Any], block_time: str) -> dict[str, Any] | None:
    topics = [topic.lower() for topic in log.get("topics", [])]
    if not topics or topics[0] != FOURMEME_SWAP_TOPIC:
        return None
    words = decode_words(log.get("data", "0x"))
    if len(words) < 9:
        return None
    asset_in = int_address(words[6])
    asset_out = int_address(words[7])
    if not asset_in and not asset_out:
        return None
    if asset_in == WBNB_ADDRESS:
        event = "buy"
        bnb_raw = words[0]
        token_raw = words[1]
        token_addr = asset_out
    elif asset_out == WBNB_ADDRESS:
        event = "sell"
        token_raw = words[0]
        bnb_raw = words[1]
        token_addr = asset_in
    elif not asset_in:
        event = "buy"
        bnb_raw = words[0]
        token_raw = words[1]
        token_addr = asset_out
    elif not asset_out:
        event = "sell"
        token_raw = words[0]
        bnb_raw = words[1]
        token_addr = asset_in
    else:
        return None
    if not token_addr or token_addr == WBNB_ADDRESS:
        return None
    bnb_amount = bnb_raw / 1e18
    if bnb_amount <= 0 or bnb_amount > 100:
        return None
    block_number = int(log.get("blockNumber", "0x0"), 16)
    log_index = int(log.get("logIndex", "0x0"), 16)
    return {
        "time_utc": block_time,
        "block_number": block_number,
        "tx_hash": log.get("transactionHash", ""),
        "log_index": log_index,
        "event": event,
        "token": token_addr,
        "token_short": short_addr(token_addr),
        "symbol": short_addr(token_addr),
        "bnb_amount": bnb_amount,
        "token_amount": token_raw / 1e18,
        "fdv_usd": "",
        "bucket": "live_pending_fdv",
        "verification": "verified_from_rpc_swap_log",
    }


def fetch_swap_logs(rpc_url: str, wallet: str, start_block: int, end_block: int, chunk_size: int) -> list[dict[str, Any]]:
    if start_block > end_block:
        return []
    logs: list[dict[str, Any]] = []
    topic_wallet = topic_address(wallet)
    current = start_block
    while current <= end_block:
        chunk_end = min(current + chunk_size - 1, end_block)
        params = [
            {
                "fromBlock": hex(current),
                "toBlock": hex(chunk_end),
                "address": FOURMEME_SWAP_CONTRACT,
                "topics": [FOURMEME_SWAP_TOPIC, topic_wallet],
            }
        ]
        try:
            logs.extend(rpc_call(rpc_url, "eth_getLogs", params))
            current = chunk_end + 1
        except RuntimeError:
            if chunk_size <= 500:
                raise
            chunk_size = max(500, chunk_size // 2)
    return logs


def load_existing_live_events() -> list[dict[str, Any]]:
    path = DATA / "live_events.json"
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload.get("events", [])


def dedupe_events(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, int], dict[str, Any]] = {}
    for event in events:
        key = (event.get("tx_hash", ""), int(to_float(event.get("log_index"))))
        if key[0]:
            by_key[key] = event
    return sorted(by_key.values(), key=lambda r: (r.get("block_number", 0), r.get("log_index", 0)), reverse=True)


def summarize_live_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    if not events:
        return {
            "latest_event_time_utc": "",
            "live_event_count": 0,
            "live_buy_count": 0,
            "live_sell_count": 0,
            "live_buy_bnb": 0,
            "live_sell_bnb": 0,
            "live_unique_tokens": 0,
        }
    return {
        "latest_event_time_utc": max(event.get("time_utc", "") for event in events),
        "live_event_count": len(events),
        "live_buy_count": sum(1 for event in events if event.get("event") == "buy"),
        "live_sell_count": sum(1 for event in events if event.get("event") == "sell"),
        "live_buy_bnb": round(sum(to_float(event.get("bnb_amount")) for event in events if event.get("event") == "buy"), 8),
        "live_sell_bnb": round(sum(to_float(event.get("bnb_amount")) for event in events if event.get("event") == "sell"), 8),
        "live_unique_tokens": len({event.get("token") for event in events if event.get("token")}),
    }


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
    live_events: list[dict[str, Any]]


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

    live_events = load_existing_live_events()
    live_event_summary = summarize_live_events(live_events)
    summary = {
        "generated_at_utc": utc_now(),
        "mode": "offline_csv_rebuild",
        "sync_status": "ready_for_server_credentials",
        "tracked_wallet": os.environ.get("BSC_TRADER_WALLET", DEFAULT_TRADER_WALLET),
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
    summary.update(live_event_summary)
    if live_event_summary["latest_event_time_utc"]:
        summary["data_window"]["last_live_event_time_utc"] = live_event_summary["latest_event_time_utc"]
    return LivePackage(summary, recent_buys_view, recent_positions_view, daily_view, live_events[:200])


def run_rpc_sync(env: dict[str, str], start_block_arg: int | None = None) -> LivePackage:
    wallet = env.get("BSC_TRADER_WALLET", DEFAULT_TRADER_WALLET) or DEFAULT_TRADER_WALLET
    rpc_url = choose_rpc_url(env.get("BSC_RPC_URL", DEFAULT_RPC_URL) or DEFAULT_RPC_URL, wallet)
    chunk_size = int(env.get("RPC_LOG_CHUNK_SIZE", "200"))
    max_blocks = int(env.get("RPC_MAX_BLOCKS_PER_RUN", "5000"))
    current_block = int(rpc_call(rpc_url, "eth_blockNumber", []), 16)
    safe_end = max(DEFAULT_START_BLOCK, current_block - CONFIRMATION_BLOCKS)
    last_block_path = STATE / "last_rpc_block.txt"
    if start_block_arg is not None:
        start_block = start_block_arg
    elif last_block_path.exists():
        start_block = max(DEFAULT_START_BLOCK, int(last_block_path.read_text(encoding="utf-8").strip()) + 1)
    else:
        start_block = DEFAULT_START_BLOCK + 1
    scan_end = min(safe_end, start_block + max_blocks - 1)

    package = build_offline_package()
    existing_events = load_existing_live_events()
    fetched_logs: list[dict[str, Any]] = []
    decoded_events: list[dict[str, Any]] = []
    if start_block <= scan_end:
        fetched_logs = fetch_swap_logs(rpc_url, wallet, start_block, scan_end, chunk_size)
        block_times = get_block_time_cache(rpc_url, [int(log.get("blockNumber", "0x0"), 16) for log in fetched_logs])
        for log in fetched_logs:
            block_number = int(log.get("blockNumber", "0x0"), 16)
            event = decode_swap_log(log, block_times.get(block_number, ""))
            if event:
                decoded_events.append(event)
        last_block_path.parent.mkdir(parents=True, exist_ok=True)
        last_block_path.write_text(str(scan_end), encoding="utf-8")

    live_events = dedupe_events([*existing_events, *decoded_events])
    write_json(
        DATA / "live_events.json",
        {
            "generated_at_utc": utc_now(),
            "tracked_wallet": wallet,
            "latest_scanned_block": safe_end,
            "events": live_events,
        },
    )
    event_summary = summarize_live_events(live_events)
    package.live_events = live_events[:200]
    package.summary.update(event_summary)
    package.summary.update(
        {
            "mode": "rpc_auto_sync",
            "sync_status": "rpc_swap_logs_verified",
            "tracked_wallet": wallet,
            "rpc_url": rpc_url,
            "latest_scanned_block": safe_end,
            "latest_completed_block": scan_end,
            "rpc_from_block": start_block,
            "rpc_to_block": scan_end,
            "rpc_safe_tip_block": safe_end,
            "rpc_blocks_remaining": max(0, safe_end - scan_end),
            "rpc_max_blocks_per_run": max_blocks,
            "rpc_new_log_count": len(fetched_logs),
            "rpc_new_event_count": len(decoded_events),
        }
    )
    if event_summary["latest_event_time_utc"]:
        package.summary["data_window"]["last_live_event_time_utc"] = event_summary["latest_event_time_utc"]
    return package


def run_sync(mode: str, start_block: int | None = None) -> LivePackage:
    env = load_env()
    os.environ.update(env)
    os.environ.setdefault("BSC_TRADER_WALLET", DEFAULT_TRADER_WALLET)
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
    if mode == "rpc":
        return run_rpc_sync(env, start_block_arg=start_block)
    return build_offline_package()


def main() -> None:
    parser = argparse.ArgumentParser(description="Build live Four.meme trader dashboard data")
    parser.add_argument("--mode", choices=["offline", "bscscan", "rpc"], default=os.environ.get("FOURMEME_PROGRAM_MODE", "rpc"))
    parser.add_argument("--start-block", type=int, default=None, help="Override RPC start block for a one-off backfill")
    args = parser.parse_args()

    package = run_sync(args.mode, start_block=args.start_block)
    write_json(DATA / "live_summary.json", package.summary)
    write_json(
        DATA / "live_trades.json",
        {
            "generated_at_utc": package.summary["generated_at_utc"],
            "recent_buys": package.recent_buys,
            "recent_positions": package.recent_positions,
            "daily": package.daily,
            "live_events": package.live_events,
        },
    )
    write_csv(
        DATA / "live_events.csv",
        package.live_events,
        [
            "time_utc",
            "block_number",
            "tx_hash",
            "log_index",
            "event",
            "token",
            "symbol",
            "bnb_amount",
            "token_amount",
            "fdv_usd",
            "bucket",
            "verification",
        ],
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
