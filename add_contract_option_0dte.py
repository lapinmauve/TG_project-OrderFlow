"""
Utility helpers to append 0DTE option contracts to the TG streaming matrix.

The main entry point, `add_0dte_option_contracts`, is designed to be called from
`TG_Reference_code.py` once the underlying universe is streaming. It locates the
current price of the chosen underlying (SPY by default), builds four contracts
(ATM/OTM Call and Put), allocates free rows in `streaming_STK_OPT_TRADE`, and
starts market data streaming for each contract.
"""

from __future__ import annotations

import datetime as dt
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence

import numpy as np
import pytz
from ibapi.contract import Contract
from loguru import logger


@dataclass(frozen=True)
class OptionSpec:
    label: str
    right: str  # 'C' or 'P'
    strike: float
    target_delta: float


def _build_option_contract(
    symbol: str,
    expiry: str,
    strike: float,
    right: str,
    trading_class: Optional[str] = None,
) -> Contract:
    """Create a US option contract with sensible defaults."""
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "OPT"
    contract.currency = "USD"
    contract.exchange = "SMART"
    contract.lastTradeDateOrContractMonth = expiry
    contract.strike = float(strike)
    contract.right = right
    contract.multiplier = "100"
    if trading_class:
        contract.tradingClass = trading_class
    if symbol.upper() == "SPX":
        contract.primaryExchange = "CBOE"
    return contract


def _next_trading_day(base: dt.datetime) -> dt.datetime:
    """Return the same day if it is a weekday, otherwise advance to the next weekday."""
    current = base
    while current.weekday() >= 5:  # 5=Saturday, 6=Sunday
        current += dt.timedelta(days=1)
    return current


def _find_free_rows(streaming_table: np.ndarray, start_index: int, count: int) -> List[int]:
    """Locate `count` empty rows (all zeros) starting at or after `start_index`."""
    free_rows: List[int] = []
    total_rows = streaming_table.shape[0]
    for idx in range(start_index, total_rows):
        if np.allclose(streaming_table[idx], 0.0):
            free_rows.append(idx)
        if len(free_rows) == count:
            break
    return free_rows


def add_0dte_option_contracts(
    streaming_table: np.ndarray,
    stock_symbols: Sequence[str],
    option_contract_dates: Optional[List[str]] = None,
    *,
    underlying: str = "SPY",
    tz: str = "US/Eastern",
    strike_step: float = 1.0,
    otm_offset: float = 5.0,
    trading_class: Optional[str] = None,
    price_timeout: float = 10.0,
    price_poll_interval: float = 0.5,
) -> List[dict]:
    """
    Append four 0DTE option contracts (ATM/OTM Calls and Puts) for `underlying`
    to the `streaming_STK_OPT_TRADE` matrix.

    Parameters
    ----------
    streaming_table : numpy.ndarray
        Global streaming matrix defined in TG_Reference_code.py. It is mutated in place.
    stock_symbols : Sequence[str]
        Ordered list of stock tickers corresponding to the first rows of the matrix.
    option_contract_dates : list[str], optional
        Optional list to append the expiry strings for downstream bookkeeping.
    underlying : str
        Underlying symbol (e.g., "SPY" or "SPX").
    tz : str
        Timezone used to determine the trading day.
    strike_step : float
        Rounding increment for strikes (adjust for instruments like SPX if needed).
    otm_offset : float
        Distance from ATM strike (in dollars) used as a proxy for ~0.25 delta.

    Returns
    -------
    List[dict]
        Metadata for the created contracts: reqId, label, strike, right, expiry.
    """

    if underlying not in stock_symbols:
        raise ValueError(f"{underlying} must be present in stock_symbols to seed 0DTE options.")

    under_index = stock_symbols.index(underlying)
    deadline = time.time() + price_timeout
    under_price = float(streaming_table[under_index, 0])
    while under_price <= 0.0 and time.time() < deadline:
        time.sleep(price_poll_interval)
        under_price = float(streaming_table[under_index, 0])
    if under_price <= 0.0:
        raise ValueError(
            f"{underlying} price is not populated in streaming table after waiting {price_timeout} seconds."
        )

    timezone = pytz.timezone(tz)
    eastern_now = dt.datetime.now(timezone)
    expiry_dt = _next_trading_day(eastern_now)
    expiry_str = expiry_dt.strftime("%Y%m%d")

    atm_strike = round(round(under_price / strike_step) * strike_step, 2)
    call_otm_strike = round(atm_strike + otm_offset, 2)
    put_otm_strike = round(max(atm_strike - otm_offset, strike_step), 2)

    specs: Iterable[OptionSpec] = (
        OptionSpec(f"{underlying}_CALL_ATM", "C", atm_strike, 0.50),
        OptionSpec(f"{underlying}_CALL_OTM", "C", call_otm_strike, 0.25),
        OptionSpec(f"{underlying}_PUT_ATM", "P", atm_strike, -0.50),
        OptionSpec(f"{underlying}_PUT_OTM", "P", put_otm_strike, -0.25),
    )

    start_index = len(stock_symbols)
    free_rows = _find_free_rows(streaming_table, start_index, count=4)
    if len(free_rows) < 4:
        raise RuntimeError(
            f"Unable to allocate 4 free rows in streaming table (found {len(free_rows)}). "
            "Increase TRADE_nbMAX_slots or release unused option slots."
        )

    trading_class_value = trading_class or ("SPXW" if underlying.upper() == "SPX" else underlying)

    metadata: List[dict] = []
    for spec, row_index in zip(specs, free_rows):
        contract = _build_option_contract(
            underlying,
            expiry_str,
            spec.strike,
            spec.right,
            trading_class=trading_class_value,
        )

        logger.info(
            "Associating {label} ({right}) strike {strike} exp {expiry} to reqId={req}",
            label=spec.label,
            right=spec.right,
            strike=spec.strike,
            expiry=expiry_str,
            req=row_index,
        )

        streaming_table[row_index, :] = 0.0
        streaming_table[row_index, 0] = spec.strike

        if option_contract_dates is not None:
            option_contract_dates.append(expiry_str)

        metadata.append(
            {
                "reqId": row_index,
                "label": spec.label,
                "strike": spec.strike,
                "right": spec.right,
                "expiry": expiry_str,
                "target_delta": spec.target_delta,
                "tradingClass": trading_class_value,
                "contract": contract,
            }
        )

    return metadata
