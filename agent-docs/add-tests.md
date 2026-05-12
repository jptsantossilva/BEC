# Add Tests Command Guide

Use this guide when the user asks to add tests, improve coverage, or create a
custom `/add-tests` style workflow.

## Prompt
Add focused tests for the files changed in this branch. Follow this guide, keep
diffs minimal, mock external services, and run the narrowest relevant tests plus
the full suite when practical.

## Standard Ask
Generate unit tests for the touched files. Use the project's existing test
runner and conventions. Keep diffs minimal and runnable locally.

## Workflow
1. Inspect the touched files and nearby tests before writing new tests.
2. Prefer focused unit tests for changed behavior over broad snapshot or
   integration tests.
3. Reuse existing fixtures, helper functions, naming style, and import patterns.
4. Isolate Binance, Telegram, OpenAI, filesystem, scheduler, and network side
   effects with mocks or pure helper boundaries.
5. Cover edge cases that are realistic for trading, backtesting, settings, or
   persistence logic.
6. Run the narrowest relevant test first, then the full test suite when
   practical.

## BEC Test Conventions
- Put unit tests under `tests/unit/`.
- Put integration-style checks under `tests/integration/` only when side effects
  are mocked or safely contained.
- Use deterministic inputs for Monte Carlo, backtesting, and strategy logic.
- Keep tests independent from local `data.db`, `.env`, live Binance access, and
  real Telegram/OpenAI credentials.
- Prefer testing `bec/` package modules directly instead of root compatibility
  wrappers unless wrapper behavior is the target.

## Validation Commands
- Full suite: `.venv/bin/python -m pytest tests`
- System Python fallback: `python3 -m pytest tests`
- Narrow test run: `.venv/bin/python -m pytest tests/unit/test_name.py`
- Syntax check:
  `.venv/bin/python -m py_compile bec/*.py bec/utils/*.py bec/exchanges/*.py bec/signals/*.py`

If a command cannot run because dependencies or credentials are missing, report
the exact failure and what was validated instead.
