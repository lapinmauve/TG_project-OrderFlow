"""
Flexible option contract preparation utilities.

This module reads symbolic option configuration entries (e.g.
``OPT_SPY_0DTE_CALL_ATM``) and prepares corresponding IBKR option contracts.
Each configuration will allocate one row in the shared streaming array, store
the strike, and return metadata ready for streaming.
"""

from __future__ import annotations

import datetime as dt
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

import numpy as np
import pytz
from ibapi.contract import Contract
from loguru import logger


@dataclass(frozen=True)
class OptionRequest:
    label: str
    symbol: str
    days_to_expiry: int
    right: str  # 'C' or 'P'
    moneyness: str  # 'ATM' or 'OTM'


def _build_option_contract(
    symbol: str,
    expiry: str,
    strike: float,
    right: str,
    trading_class: Optional[str] = None,
) -> Contract:
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
    current = base
    while current.weekday() >= 5:
        current += dt.timedelta(days=1)
    return current


def _add_trading_days(base: dt.datetime, days: int) -> dt.datetime:
    current = _next_trading_day(base)
    added = 0
    while added < days:
        current += dt.timedelta(days=1)
        if current.weekday() < 5:
            added += 1
    return current


def _find_free_rows(streaming_table: np.ndarray, start_index: int, count: int) -> List[int]:
    free_rows: List[int] = []
    total_rows = streaming_table.shape[0]
    for idx in range(start_index, total_rows):
        if np.allclose(streaming_table[idx], 0.0):
            free_rows.append(idx)
        if len(free_rows) == count:
            break
    return free_rows


def parse_option_config_line(line: str) -> OptionRequest:
    """
    Convert strings like ``OPT_SPY_0DTE_CALL_ATM`` into OptionRequest objects.
    """
    parts = line.strip().split("_")
    if len(parts) != 5 or parts[0] != "OPT":
        raise ValueError(f"Invalid option configuration entry: {line}")

    symbol = parts[1]
    dte_part = parts[2]
    if not dte_part.endswith("DTE"):
        raise ValueError(f"Invalid DTE token in entry: {line}")
    days_to_expiry = int(dte_part[:-3])

    right_part = parts[3].upper()
    if right_part not in ("CALL", "PUT"):
        raise ValueError(f"Invalid option right in entry: {line}")
    right = "C" if right_part == "CALL" else "P"

    moneyness = parts[4].upper()
    if moneyness not in ("ATM", "OTM"):
        raise ValueError(f"Invalid moneyness in entry: {line}")

    return OptionRequest(
        label=line.strip(),
        symbol=symbol.upper(),
        days_to_expiry=days_to_expiry,
        right=right,
        moneyness=moneyness,
    )


def load_option_requests(config_lines: Iterable[str]) -> List[OptionRequest]:
    requests: List[OptionRequest] = []
    for line in config_lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        requests.append(parse_option_config_line(line))
    return requests


def prepare_option_contracts(
    streaming_table: np.ndarray,
    stock_symbols: Sequence[str],
    option_requests: Sequence[OptionRequest],
    *,
    option_contract_dates: Optional[List[str]] = None,
    timezone: str = "US/Eastern",
    price_timeout: float = 10.0,
    price_poll_interval: float = 0.5,
    strike_steps: Optional[Dict[str, float]] = None,
    otm_offsets: Optional[Dict[str, float]] = None,
    trading_classes: Optional[Dict[str, str]] = None,
) -> List[dict]:
    """
    Prepare option contracts for the given requests and fill the streaming table.

    Returns a metadata list where each entry contains reqId, label, contract, etc.
    """
    strike_steps = strike_steps or {}
    otm_offsets = otm_offsets or {}
    trading_classes = trading_classes or {}

    if not option_requests:
        return []

    timezone_obj = pytz.timezone(timezone)
    now = dt.datetime.now(timezone_obj)

    start_index = len(stock_symbols)
    free_rows = _find_free_rows(streaming_table, start_index, len(option_requests))
    if len(free_rows) < len(option_requests):
        raise RuntimeError("Not enough free rows to allocate option contracts.")

    metadata: List[dict] = []

    for request, row_index in zip(option_requests, free_rows):
        if request.symbol not in stock_symbols:
            raise ValueError(f"{request.symbol} not present in stock symbol list; cannot price options.")

        underlying_idx = stock_symbols.index(request.symbol)
        deadline = time.time() + price_timeout
        underlying_price = float(streaming_table[underlying_idx, 0])
        while underlying_price <= 0.0 and time.time() < deadline:
            time.sleep(price_poll_interval)
            underlying_price = float(streaming_table[underlying_idx, 0])
        if underlying_price <= 0.0:
            raise ValueError(
                f"{request.symbol} price unavailable after waiting {price_timeout} seconds."
            )

        if request.days_to_expiry == 0:
            expiry_dt = _next_trading_day(now)
        else:
            expiry_dt = _add_trading_days(now, request.days_to_expiry)
        expiry_str = expiry_dt.strftime("%Y%m%d")

        step = strike_steps.get(request.symbol, 1.0)
        atm_strike = round(round(underlying_price / step) * step, 2)

        offset = otm_offsets.get(request.symbol, step)
        if request.moneyness == "ATM":
            strike = atm_strike
        else:
            if request.right == "C":
                strike = round(atm_strike + offset, 2)
            else:
                strike = round(max(step, atm_strike - offset), 2)

        trading_class = trading_classes.get(request.symbol)
        contract = _build_option_contract(
            request.symbol,
            expiry_str,
            strike,
            request.right,
            trading_class=trading_class,
        )

        streaming_table[row_index, :] = 0.0
        streaming_table[row_index, 0] = strike

        if option_contract_dates is not None:
            option_contract_dates.append(expiry_str)

        metadata.append(
            {
                "reqId": row_index,
                "label": request.label,
                "symbol": request.symbol,
                "expiry": expiry_str,
                "strike": strike,
                "right": request.right,
                "contract": contract,
                "type": "OPT_CFG",
            }
        )

        logger.info(
            "Prepared option contract {} -> {} strike {} expiry {} row {}",
            request.label,
            contract.symbol,
            strike,
            expiry_str,
            row_index,
        )

    return metadata
