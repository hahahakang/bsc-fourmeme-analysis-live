#!/usr/bin/env python3
"""Near-real-time trader listener for server deployment.

This process is designed for a VPS or long-running machine. It watches the
tracked wallet's Four.meme Swap logs every few seconds, sends alerts, and
records paper-trade decisions. It never submits real trades.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import smtplib
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from sync_trader import (
    CONFIRMATION_BLOCKS,
    DATA,
    DEFAULT_RPC_URL,
    DEFAULT_START_BLOCK,
    DEFAULT_TRADER_WALLET,
    STATE,
    choose_rpc_url,
    decode_logs_to_events,
    fetch_swap_logs,
    load_existing_live_events,
    load_env,
    rpc_call,
    short_addr,
    utc_now,
    write_csv,
    write_json,
)


LISTENER_STATE = STATE / "server_listener_state.json"
LISTENER_STATUS = DATA / "server_listener_status.json"
LISTENER_ALERTS = DATA / "server_listener_alerts.json"
PAPER_ORDERS = DATA / "server_paper_orders.json"

ALERT_FIELDS = [
    "created_at_utc",
    "event_time_utc",
    "block_number",
    "event",
    "token",
    "token_short",
    "bnb_amount",
    "tx_hash",
    "decision",
    "reason",
]

PAPER_FIELDS = [
    "created_at_utc",
    "event_time_utc",
    "token",
    "event",
    "signal_bnb",
    "paper_bnb",
    "decision",
    "reason",
    "tx_hash",
]


def env_bool(env: dict[str, str], key: str, default: bool = False) -> bool:
    value = str(env.get(key, "")).strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "y", "on"}


def env_float(env: dict[str, str], key: str, default: float) -> float:
    try:
        return float(env.get(key, default))
    except (TypeError, ValueError):
        return default


def env_int(env: dict[str, str], key: str, default: int) -> int:
    try:
        return int(env.get(key, default))
    except (TypeError, ValueError):
        return default


def parse_utc(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return default


def event_key(event: dict[str, Any]) -> str:
    return f"{event.get('tx_hash')}:{event.get('log_index', 0)}"


def trim_list(items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return items[:limit] if len(items) > limit else items


@dataclass
class ListenerConfig:
    wallet: str
    rpc_url: str
    poll_seconds: int
    confirmations: int
    lookback_blocks: int
    max_blocks_per_tick: int
    chunk_size: int
    bootstrap_blocks: int
    alert_limit: int
    notify_channels: set[str]
    dry_run: bool
    first_buy_only: bool
    min_signal_bnb: float
    max_signal_bnb: float
    paper_bnb: float
    max_signal_age_seconds: int
    min_fdv_usd: float
    max_fdv_usd: float
    require_market_data: bool
    require_sell_simulation: bool


def build_config(env: dict[str, str], args: argparse.Namespace) -> ListenerConfig:
    channels = {
        item.strip().lower()
        for item in env.get("LISTENER_NOTIFY_CHANNELS", "console,json,web").split(",")
        if item.strip()
    }
    return ListenerConfig(
        wallet=env.get("BSC_TRADER_WALLET", DEFAULT_TRADER_WALLET) or DEFAULT_TRADER_WALLET,
        rpc_url=choose_rpc_url(env.get("BSC_RPC_URL", DEFAULT_RPC_URL) or DEFAULT_RPC_URL, env.get("BSC_TRADER_WALLET", DEFAULT_TRADER_WALLET)),
        poll_seconds=args.poll_seconds or env_int(env, "LISTENER_POLL_SECONDS", 10),
        confirmations=env_int(env, "LISTENER_CONFIRMATION_BLOCKS", 2),
        lookback_blocks=env_int(env, "LISTENER_LOOKBACK_BLOCKS", 8),
        max_blocks_per_tick=env_int(env, "LISTENER_MAX_BLOCKS_PER_TICK", 5000),
        chunk_size=env_int(env, "LISTENER_LOG_CHUNK_SIZE", 2000),
        bootstrap_blocks=env_int(env, "LISTENER_BOOTSTRAP_BLOCKS", 30),
        alert_limit=env_int(env, "LISTENER_ALERT_LIMIT", 500),
        notify_channels=channels,
        dry_run=env_bool(env, "COPYTRADE_DRY_RUN", True),
        first_buy_only=env_bool(env, "COPYTRADE_FIRST_BUY_ONLY", True),
        min_signal_bnb=env_float(env, "COPYTRADE_MIN_SIGNAL_BNB", 0.05),
        max_signal_bnb=env_float(env, "COPYTRADE_MAX_SIGNAL_BNB", 2.0),
        paper_bnb=env_float(env, "COPYTRADE_PAPER_BNB", 0.02),
        max_signal_age_seconds=env_int(env, "COPYTRADE_MAX_SIGNAL_AGE_SECONDS", 45),
        min_fdv_usd=env_float(env, "COPYTRADE_MIN_FDV_USD", 0),
        max_fdv_usd=env_float(env, "COPYTRADE_MAX_FDV_USD", 50000),
        require_market_data=env_bool(env, "COPYTRADE_REQUIRE_MARKET_DATA", False),
        require_sell_simulation=env_bool(env, "COPYTRADE_REQUIRE_SELL_SIMULATION", True),
    )


def initial_state(config: ListenerConfig, safe_tip: int, args: argparse.Namespace) -> dict[str, Any]:
    start_block = args.start_block
    if start_block is None:
        start_block = max(DEFAULT_START_BLOCK + 1, safe_tip - config.bootstrap_blocks + 1)
    existing_events = load_existing_live_events()
    seen_event_keys = [event_key(event) for event in existing_events if event.get("tx_hash")]
    seen_buy_tokens = [
        str(event.get("token", "")).lower()
        for event in existing_events
        if event.get("event") == "buy" and event.get("token")
    ]
    return {
        "created_at_utc": utc_now(),
        "last_scanned_block": start_block - 1,
        "seen_event_keys": sorted(set(seen_event_keys))[-2000:],
        "seen_buy_tokens": sorted(set(seen_buy_tokens))[-2000:],
        "bootstrap_completed": False,
    }


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, state)


def reset_records() -> None:
    write_json(LISTENER_ALERTS, {"generated_at_utc": utc_now(), "records": []})
    write_json(PAPER_ORDERS, {"generated_at_utc": utc_now(), "records": []})
    write_records_csv(DATA / "server_listener_alerts.csv", [], ALERT_FIELDS)
    write_records_csv(DATA / "server_paper_orders.csv", [], PAPER_FIELDS)


def event_to_alert(event: dict[str, Any], decision: dict[str, Any]) -> dict[str, Any]:
    return {
        "created_at_utc": utc_now(),
        "event_time_utc": event.get("time_utc", ""),
        "block_number": event.get("block_number", ""),
        "event": event.get("event", ""),
        "token": event.get("token", ""),
        "token_short": event.get("token_short") or short_addr(event.get("token", "")),
        "bnb_amount": event.get("bnb_amount", 0),
        "tx_hash": event.get("tx_hash", ""),
        "log_index": event.get("log_index", 0),
        "decision": decision.get("decision", ""),
        "reason": decision.get("reason", ""),
        "paper_bnb": decision.get("paper_bnb", 0),
    }


def append_records(path: Path, new_rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    payload = load_json(path, {"records": []})
    records = payload.get("records", [])
    records = sorted([*new_rows, *records], key=lambda r: r.get("created_at_utc", ""), reverse=True)
    records = trim_list(records, limit)
    write_json(path, {"generated_at_utc": utc_now(), "records": records})
    return records


def write_records_csv(path: Path, records: list[dict[str, Any]], fields: list[str]) -> None:
    write_csv(path, records, fields)


def decide_paper_order(event: dict[str, Any], state: dict[str, Any], config: ListenerConfig) -> dict[str, Any]:
    reasons: list[str] = []
    decision = "alert_only"
    if event.get("event") != "buy":
        return {"decision": "observe_sell", "reason": "sell_event_no_copy", "paper_bnb": 0}

    signal_bnb = float(event.get("bnb_amount") or 0)
    token = str(event.get("token", "")).lower()
    event_time = parse_utc(event.get("time_utc", ""))
    signal_age = (datetime.now(timezone.utc) - event_time).total_seconds() if event_time else 999999

    if signal_bnb < config.min_signal_bnb:
        reasons.append("signal_bnb_below_min")
    if signal_bnb > config.max_signal_bnb:
        reasons.append("signal_bnb_above_max")
    if signal_age > config.max_signal_age_seconds:
        reasons.append("signal_too_old")
    if config.first_buy_only and token in set(state.get("seen_buy_tokens", [])):
        reasons.append("not_first_seen_buy")

    fdv = event.get("fdv_usd")
    if fdv not in {"", None}:
        try:
            fdv_value = float(fdv)
            if fdv_value < config.min_fdv_usd:
                reasons.append("fdv_below_min")
            if fdv_value > config.max_fdv_usd:
                reasons.append("fdv_above_max")
        except (TypeError, ValueError):
            reasons.append("fdv_parse_failed")
    elif config.require_market_data:
        reasons.append("market_data_missing")

    if config.require_sell_simulation:
        reasons.append("sell_simulation_not_connected")

    if reasons:
        decision = "paper_skip"
    elif config.dry_run:
        decision = "paper_follow_candidate"
    else:
        decision = "real_trade_disabled_in_code"

    return {
        "decision": decision,
        "reason": ",".join(reasons) if reasons else "rule_passed_dry_run_only",
        "paper_bnb": config.paper_bnb if decision == "paper_follow_candidate" else 0,
    }


def format_alert(alert: dict[str, Any], config: ListenerConfig) -> str:
    action = "买入" if alert.get("event") == "buy" else "卖出"
    token = alert.get("token_short") or short_addr(alert.get("token", ""))
    return (
        f"BSC Four.meme 交易员{action}提醒\n"
        f"钱包: {config.wallet}\n"
        f"时间UTC: {alert.get('event_time_utc')}\n"
        f"Token: {token}\n"
        f"BNB: {alert.get('bnb_amount')}\n"
        f"Block: {alert.get('block_number')}\n"
        f"决策: {alert.get('decision')} / {alert.get('reason')}\n"
        f"Tx: https://bscscan.com/tx/{alert.get('tx_hash')}"
    )


def post_json(url: str, payload: dict[str, Any]) -> None:
    req = Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": "bsc-fourmeme-server-listener"},
    )
    with urlopen(req, timeout=15) as resp:
        resp.read()


def send_telegram(env: dict[str, str], text: str) -> None:
    token = env.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = env.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        raise RuntimeError("Telegram token/chat_id missing")
    post_json(
        f"https://api.telegram.org/bot{token}/sendMessage",
        {"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
    )


def send_webhook(env: dict[str, str], alert: dict[str, Any], text: str) -> None:
    url = env.get("ALERT_WEBHOOK_URL", "")
    if not url:
        raise RuntimeError("ALERT_WEBHOOK_URL missing")
    post_json(url, {"text": text, "alert": alert})


def send_email(env: dict[str, str], text: str) -> None:
    host = env.get("SMTP_HOST", "")
    to_addr = env.get("ALERT_EMAIL_TO", "")
    from_addr = env.get("ALERT_EMAIL_FROM", env.get("SMTP_USER", ""))
    if not host or not to_addr or not from_addr:
        raise RuntimeError("SMTP/email settings missing")
    port = env_int(env, "SMTP_PORT", 587)
    msg = EmailMessage()
    msg["Subject"] = env.get("ALERT_EMAIL_SUBJECT", "BSC Four.meme trader alert")
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(text)
    with smtplib.SMTP(host, port, timeout=20) as smtp:
        if env_bool(env, "SMTP_STARTTLS", True):
            smtp.starttls()
        user = env.get("SMTP_USER", "")
        password = env.get("SMTP_PASSWORD", "")
        if user and password:
            smtp.login(user, password)
        smtp.send_message(msg)


def notify(env: dict[str, str], config: ListenerConfig, alerts: list[dict[str, Any]], no_alerts: bool) -> list[str]:
    errors: list[str] = []
    if no_alerts:
        return errors
    for alert in alerts:
        text = format_alert(alert, config)
        if "console" in config.notify_channels:
            print(text, flush=True)
        for channel in sorted(config.notify_channels - {"console", "json", "web"}):
            try:
                if channel == "telegram":
                    send_telegram(env, text)
                elif channel == "email":
                    send_email(env, text)
                elif channel == "webhook":
                    send_webhook(env, alert, text)
            except Exception as exc:  # noqa: BLE001 - keep listener alive after alert failures
                errors.append(f"{channel}: {exc}")
    return errors


def scan_once(
    env: dict[str, str],
    config: ListenerConfig,
    state_path: Path,
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    current_block = int(rpc_call(config.rpc_url, "eth_blockNumber", []), 16)
    safe_tip = max(DEFAULT_START_BLOCK, current_block - config.confirmations)
    state = load_json(state_path, None)
    if state is None:
        state = initial_state(config, safe_tip, args)
    start_block = max(DEFAULT_START_BLOCK + 1, int(state.get("last_scanned_block", DEFAULT_START_BLOCK)) + 1 - config.lookback_blocks)
    end_block = min(safe_tip, start_block + config.max_blocks_per_tick - 1)

    seen_keys = set(state.get("seen_event_keys", []))
    logs = fetch_swap_logs(config.rpc_url, config.wallet, start_block, end_block, config.chunk_size) if start_block <= end_block else []
    events = decode_logs_to_events(config.rpc_url, logs)
    events = sorted(events, key=lambda r: (r.get("block_number", 0), r.get("log_index", 0)))

    new_alerts: list[dict[str, Any]] = []
    paper_rows: list[dict[str, Any]] = []
    seen_buy_tokens = set(state.get("seen_buy_tokens", []))
    for event in events:
        key = event_key(event)
        token = str(event.get("token", "")).lower()
        prior_state = {**state, "seen_buy_tokens": sorted(seen_buy_tokens)}
        if key in seen_keys:
            if event.get("event") == "buy":
                seen_buy_tokens.add(token)
            continue
        decision = decide_paper_order(event, prior_state, config)
        alert = event_to_alert(event, decision)
        new_alerts.append(alert)
        paper_rows.append(
            {
                "created_at_utc": alert["created_at_utc"],
                "event_time_utc": event.get("time_utc", ""),
                "token": event.get("token", ""),
                "event": event.get("event", ""),
                "signal_bnb": event.get("bnb_amount", 0),
                "paper_bnb": decision.get("paper_bnb", 0),
                "decision": decision.get("decision", ""),
                "reason": decision.get("reason", ""),
                "tx_hash": event.get("tx_hash", ""),
            }
        )
        seen_keys.add(key)
        if event.get("event") == "buy":
            seen_buy_tokens.add(token)

    if end_block >= int(state.get("last_scanned_block", DEFAULT_START_BLOCK)):
        state["last_scanned_block"] = end_block
    state["updated_at_utc"] = utc_now()
    state["seen_event_keys"] = sorted(seen_keys)[-2000:]
    state["seen_buy_tokens"] = sorted(seen_buy_tokens)[-2000:]
    state["bootstrap_completed"] = True
    save_state(state_path, state)

    alerts = append_records(LISTENER_ALERTS, new_alerts, config.alert_limit)
    paper_orders = append_records(PAPER_ORDERS, paper_rows, config.alert_limit)
    write_records_csv(DATA / "server_listener_alerts.csv", alerts, ALERT_FIELDS)
    write_records_csv(DATA / "server_paper_orders.csv", paper_orders, PAPER_FIELDS)

    errors = notify(env, config, new_alerts, args.no_alerts)
    status = {
        "generated_at_utc": utc_now(),
        "mode": "server_listener",
        "wallet": config.wallet,
        "rpc_url": config.rpc_url,
        "poll_seconds": config.poll_seconds,
        "confirmations": config.confirmations,
        "start_block": start_block,
        "end_block": end_block,
        "safe_tip_block": safe_tip,
        "blocks_behind": max(0, safe_tip - end_block),
        "logs_seen": len(logs),
        "events_decoded": len(events),
        "new_alerts": len(new_alerts),
        "total_alerts": len(alerts),
        "dry_run": config.dry_run,
        "copytrade_status": "paper_only_real_trading_disabled",
        "rule_summary": {
            "first_buy_only": config.first_buy_only,
            "min_signal_bnb": config.min_signal_bnb,
            "max_signal_bnb": config.max_signal_bnb,
            "paper_bnb": config.paper_bnb,
            "max_signal_age_seconds": config.max_signal_age_seconds,
            "min_fdv_usd": config.min_fdv_usd,
            "max_fdv_usd": config.max_fdv_usd,
            "require_market_data": config.require_market_data,
            "require_sell_simulation": config.require_sell_simulation,
        },
        "notify_channels": sorted(config.notify_channels),
        "notify_errors": errors,
    }
    write_json(LISTENER_STATUS, status)
    return new_alerts, status


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a near-real-time Four.meme trader listener")
    parser.add_argument("--once", action="store_true", help="scan one window and exit")
    parser.add_argument("--no-alerts", action="store_true", help="write files but do not send external notifications")
    parser.add_argument("--poll-seconds", type=int, default=0, help="override LISTENER_POLL_SECONDS")
    parser.add_argument("--start-block", type=int, default=None, help="override first scanned block")
    parser.add_argument("--state-file", type=Path, default=LISTENER_STATE, help="state file path")
    parser.add_argument("--reset-records", action="store_true", help="clear alert and paper-order files before scanning")
    args = parser.parse_args()

    env = load_env()
    os.environ.update(env)
    config = build_config(env, args)
    if args.reset_records:
        reset_records()

    while True:
        try:
            alerts, status = scan_once(env, config, args.state_file, args)
            print(json.dumps({"status": status, "alerts": alerts}, ensure_ascii=False), flush=True)
        except KeyboardInterrupt:
            raise
        except Exception as exc:  # noqa: BLE001 - long-running listener should report and retry
            error_status = {
                "generated_at_utc": utc_now(),
                "mode": "server_listener",
                "sync_status": "error",
                "error": str(exc),
            }
            write_json(LISTENER_STATUS, error_status)
            print(json.dumps(error_status, ensure_ascii=False), file=sys.stderr, flush=True)
        if args.once:
            break
        time.sleep(config.poll_seconds)


if __name__ == "__main__":
    main()
