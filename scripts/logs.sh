#!/usr/bin/env bash
# Export / follow bsc-runner logs as flat text files (journald → files).
#
#   scripts/logs.sh dump            one-shot: full history → logs/runner-<ts>.log
#   scripts/logs.sh follow          continuous: append live → logs/runner.log
#   scripts/logs.sh kol|trader|tokflow|priceoracle   filtered one-shot dump
#
# Plain text, ANSI stripped, ISO timestamps. closed_trades.csv (the trade
# record) is already a file at trader/closed_trades.csv — not duplicated here.
set -uo pipefail
DIR=/data/bsc-meme-mev/logs
mkdir -p "$DIR"
U=bsc-runner.service
strip() { sed -u 's/\x1b\[[0-9;]*m//g; s/\x1b]8;;[^\x1b]*\x1b\\//g'; }

case "${1:-dump}" in
  follow)
    f="$DIR/runner.log"
    echo "appending live → $f  (Ctrl-C to stop)"
    journalctl -u "$U" -f -n 0 -o short-iso 2>/dev/null | strip >> "$f" ;;
  dump)
    ts=$(date -u +%Y%m%dT%H%M%SZ); f="$DIR/runner-$ts.log"
    journalctl -u "$U" --no-pager -o short-iso 2>/dev/null | strip > "$f"
    echo "wrote $(wc -l < "$f") lines → $f" ;;
  kol|trader|tokflow|priceoracle)
    ts=$(date -u +%Y%m%dT%H%M%SZ); f="$DIR/$1-$ts.log"
    journalctl -u "$U" --no-pager -o cat 2>/dev/null | strip \
      | grep -E "(^| )$1: |target.*$1" > "$f"
    echo "wrote $(wc -l < "$f") lines → $f" ;;
  *) echo "usage: logs.sh dump|follow|kol|trader|tokflow|priceoracle" ; exit 1 ;;
esac
