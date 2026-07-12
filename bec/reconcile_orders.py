"""Reconcile unsettled exchange order intents without submitting new orders."""

from bec.exchanges.live_execution import reconcile_unsettled_orders


def main() -> int:
    stats = reconcile_unsettled_orders()
    print(f"Exchange reconciliation: {stats}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
