# Trading and Persistence Safety Guide

Use this before changing trading execution, strategy selection, Binance access,
database behavior, scheduler jobs, or Telegram/OpenAI integrations.

## Non-Negotiables
- Never commit `.env`, API keys, tokens, database files, logs, or generated
  runtime artifacts.
- Never delete persistent Docker volumes with `docker compose down -v` unless
  the user explicitly requests data deletion.
- Do not make live-order behavior broader or more aggressive without calling out
  the risk and adding focused validation.
- Keep defaults conservative when behavior affects position sizing, balances,
  symbol selection, stop loss, take profit, or scheduled execution.

## Areas That Need Extra Care
- Binance client calls, order creation, balance reads, and symbol filters.
- Scheduler behavior in `jobs_runner.py` and `bec/jobs_runner.py`.
- Settings loaded from the database through `bec/utils/config.py`.
- Persistent paths such as `data.db`, `/app/persist`, `main.log`, and generated
  backtesting reports.
- Telegram error/reporting paths that users may rely on during unattended runs.

## Implementation Guidance
- Prefer pure functions and focused helpers for trading calculations so they can
  be tested without live exchange access.
- Mock or isolate Binance, Telegram, OpenAI, and filesystem side effects in
  tests where practical.
- Preserve backward compatibility for existing database settings unless a
  migration or fallback is explicitly part of the task.
- When changing risk controls, document the before/after behavior in the PR or
  final response.

## Validation
- Run `python -m pytest tests` for relevant behavior.
- Add or update unit tests for calculations, parsing, settings, and decision
  logic touched by the change.
- For scheduler or integration changes, run the relevant command locally when it
  is safe: `python jobs_runner.py`, `python main.py 1d`, or the dashboard command.
- If a live-network or credential-dependent check cannot be run, state that
  clearly and describe the safer validation that was performed.
