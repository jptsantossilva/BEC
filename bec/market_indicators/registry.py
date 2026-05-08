from dataclasses import dataclass
from typing import Callable, Dict, Optional


@dataclass
class IndicatorMeta:
    key: str
    name: str
    short: str
    description: str
    source_url: Optional[str]
    compute_fn: Callable # signature: compute_fn(ohlc_df, **kwargs) -> dict


# --- Example registry entry for PI Cycle Top ---


def get_registry() -> Dict[str, IndicatorMeta]:
    from .data import compute_pi_cycle_top
    return {
        "pi_cycle_top": IndicatorMeta(
        key="pi_cycle_top",
        name="Pi Cycle Top",
        short="PICT",
        description=(
            "Uses crossover of 111D and 350D (x2) moving averages on BTC price to flag cycle tops."
        ),
        source_url="https://www.coinglass.com/pro/i/pi-cycle-top-indicator",
        compute_fn=compute_pi_cycle_top,
    ),
    # Add more indicators here as you implement them...
    # "mvrv_z": IndicatorMeta(...),
    # "puell_multiple": IndicatorMeta(...),
}