#!/usr/bin/env sh
set -eu

mkdir -p /app/persist

touch /app/persist/data.db /app/persist/main.log
ln -sf /app/persist/data.db /app/data.db
ln -sf /app/persist/main.log /app/main.log

exec "$@"
