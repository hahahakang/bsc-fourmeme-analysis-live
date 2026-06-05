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
