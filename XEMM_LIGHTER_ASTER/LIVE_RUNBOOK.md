# Live Runbook

## Current HYPE Normal Run

Useful monitoring commands:

```bash
tail -f runs/live-hype-normal-20260623T212845Z.log
python3 scripts/check_hedged_trade.py runs/live-hype-normal-20260623T212845Z-journal.jsonl --config config-live-lighter.toml
ps -p 4115966 -o pid,stat,etime,cmd
```

To stop cleanly later:

```bash
kill -INT 4115966
```
