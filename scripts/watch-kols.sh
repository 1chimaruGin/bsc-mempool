#!/usr/bin/env bash
# Clean live KOL trade tape. Tails bsc-runner.service and prints ONE compact
# aligned row per event — GMGN swaps only (approves/transfers suppressed),
# token addresses resolved to symbols via eth_call (cached).
#
#   TIME      KOL SIDE  TOKEN         STAT  DETAIL                 TX
#   13:11:34  O   BUY   FOUR          ⏳    pending                ba0eef27
#   13:11:34  O   BUY   FOUR          ✅    blk 98627904  +29ms    ba0eef27
#
# Usage:
#   scripts/watch-kols.sh            # all GOAT trades, live
#   scripts/watch-kols.sh buy        # BUYs only
#   scripts/watch-kols.sh sell       # SELLs only
#   scripts/watch-kols.sh D          # one wallet
#   scripts/watch-kols.sh --all      # include approves/transfers (noisy)
#   scripts/watch-kols.sh --hist 30  # replay last 30 min then follow
set -uo pipefail
exec python3 "$(dirname "$0")/watch_kols.py" "$@"
