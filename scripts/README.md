# Live Trader Sync

This folder is for the new live-development copy, not the original GitHub report.

## Local Run

```bash
python3 scripts/sync_trader.py --mode rpc
python3 -m http.server 8780
```

Open `http://127.0.0.1:8780/`.

## Server Run Later

1. Copy `.env.example` to `.env`.
2. Keep `BSC_TRADER_WALLET=0x55976c6818e4794f3e2e7179eea2cc2202811e11`.
3. Use a BSC RPC endpoint that supports `eth_getLogs`.
4. Run `python3 scripts/sync_trader.py --mode rpc`.
5. Add cron/systemd:

```cron
*/5 * * * * cd /srv/bsc-fourmeme-analysis-live && python3 scripts/sync_trader.py --mode rpc && git add data/*.json data/*.csv data/state/*.txt data/state/*.json && git commit -m "Update live trader facts" && git push
```

GitHub Actions already runs the same sync every five minutes. It scans Four.meme Swap logs for the tracked wallet, stores progress in `data/state/last_rpc_block.txt`, and publishes new JSON/CSV facts back to the site.

Each run scans two windows:

- a recent window controlled by `RPC_RECENT_BLOCKS_PER_RUN`, so new trades appear quickly;
- a historical backfill window controlled by `RPC_MAX_BLOCKS_PER_RUN`, so older gaps are filled safely without overloading public RPC endpoints.

Important: this watcher covers Four.meme Swap events. If the trader uses another router or CEX/aggregator contract, that requires a transaction indexer/API in addition to raw BSC RPC.

## Near-Real-Time Server Listener

Use `server_listener.py` on a VPS when the goal is alerting or future copy-trade research. It is intentionally separate from GitHub Actions because Actions scheduling is not precise enough for trading.

```bash
cp .env.example .env
python3 scripts/server_listener.py --once --no-alerts
python3 scripts/server_listener.py
```

Default behavior:

- polls every `LISTENER_POLL_SECONDS=10`;
- scans a small moving block window with confirmation delay;
- deduplicates events by tx hash and log index;
- writes `data/server_listener_status.json`;
- writes `data/server_listener_alerts.json` and `.csv`;
- writes `data/server_paper_orders.json` and `.csv`;
- sends console alerts by default;
- can send Telegram, email, or webhook alerts after credentials are added.

Example Telegram setup:

```env
LISTENER_NOTIFY_CHANNELS=console,json,web,telegram
TELEGRAM_BOT_TOKEN=123456:xxxx
TELEGRAM_CHAT_ID=123456789
```

Example email setup:

```env
LISTENER_NOTIFY_CHANNELS=console,json,web,email
ALERT_EMAIL_TO=you@example.com
ALERT_EMAIL_FROM=bot@example.com
SMTP_HOST=smtp.example.com
SMTP_PORT=587
SMTP_USER=bot@example.com
SMTP_PASSWORD=your-app-password
SMTP_STARTTLS=true
```

Example systemd unit:

```ini
[Unit]
Description=BSC Four.meme live trader listener
After=network-online.target

[Service]
WorkingDirectory=/srv/bsc-fourmeme-analysis-live
ExecStart=/usr/bin/python3 scripts/server_listener.py
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

Copy-trade rules are paper-only for now:

- only considers buy events;
- can require first-seen buy only;
- filters by signal BNB size;
- supports FDV bounds once market data is connected;
- defaults to `COPYTRADE_REQUIRE_SELL_SIMULATION=true`, so no paper-follow candidate is emitted until a sell simulation module exists;
- real trading is disabled in code. Add private-key execution only after the sample library is large enough and sell simulation passes.
