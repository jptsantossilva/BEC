#!/usr/bin/env sh
set -eu

mkdir -p /app/persist /app/persist/backtest_results /app/static

touch /app/persist/data.db /app/persist/main.log
ln -sf /app/persist/data.db /app/data.db
ln -sf /app/persist/main.log /app/main.log

if [ -d /app/static/backtest_results ] && [ ! -L /app/static/backtest_results ]; then
  cp -a /app/static/backtest_results/. /app/persist/backtest_results/ 2>/dev/null || true
  rm -rf /app/static/backtest_results
fi
ln -sfn /app/persist/backtest_results /app/static/backtest_results

exec "$@"
