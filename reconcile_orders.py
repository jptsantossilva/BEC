"""Compatibility wrapper for scheduled Kraken reconciliation."""

from bec.reconcile_orders import main


if __name__ == "__main__":
    raise SystemExit(main())
