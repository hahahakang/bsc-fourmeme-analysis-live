# Live Trader Sync

This folder is for the new live-development copy, not the original GitHub report.

## Local Run

```bash
python3 scripts/sync_trader.py --mode offline
python3 -m http.server 8780
```

Open `http://127.0.0.1:8780/`.

## Server Run Later

1. Copy `.env.example` to `.env`.
2. Fill `BSC_TRADER_WALLET`, `BSCSCAN_API_KEY`, and `BSC_RPC_URL`.
3. Run `python3 scripts/sync_trader.py --mode bscscan`.
4. Add cron/systemd:

```cron
*/10 * * * * cd /srv/bsc-fourmeme-analysis-live && python3 scripts/sync_trader.py --mode bscscan && git add data/*.json data/*.csv && git commit -m "Update live trader facts" && git push
```

The first live version fetches BscScan transaction lists and keeps the receipt/Four.meme event decoder as the next module. Until RPC decoding is enabled, the dashboard clearly labels data as CSV-derived reconstruction rather than new verified Four.meme events.

