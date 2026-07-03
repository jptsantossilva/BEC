"""Exchange-neutral contracts and adapter access."""

from bec.exchanges.base import ExchangeAdapter
from bec.exchanges.registry import get_default_adapter

__all__ = ["ExchangeAdapter", "get_default_adapter"]
