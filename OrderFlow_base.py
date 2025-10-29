"""
OrderFlow_base.py
------------------
Connects to the IBKR API, subscribes to market data, and maintains a
rolling numpy-backed table of the latest tick snapshots. Each incoming
tick for price or size (bid/ask/last/volume) updates a per-symbol
snapshot and appends a full row with a Unix timestamp to the buffer.
"""

import argparse
import math
import os
import sys
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
from dotenv import load_dotenv
from ibapi.client import EClient
from ibapi.common import TickAttrib
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper
from loguru import logger


# -----------------------------------------------------------------------------
# Logging setup
# -----------------------------------------------------------------------------

logger.remove()
logger.add(sys.stdout, colorize=True,
           format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                  "<level>{level: <8}</level> | "
                  "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                  "<level>{message}</level>")
logger.add(sys.stderr, format="{time:MMMM D, YYYY > HH:mm:ss} | {level} | {message} | {extra}")


# -----------------------------------------------------------------------------
# Buffer implementation
# -----------------------------------------------------------------------------

@dataclass
class Snapshot:
    """Holds the latest tick values for a given ticker id."""

    bid_size: float = np.nan
    bid_price: float = np.nan
    ask_price: float = np.nan
    ask_size: float = np.nan
    last_price: float = np.nan
    last_size: float = np.nan
    volume: float = np.nan


class OrderFlowBuffer:
    """
    Circular buffer that stores the latest tick snapshots in a structured numpy
    array. Each row captures the full state after a tick arrives.
    """

    dtype = np.dtype([
        ("timestamp", "float64"),
        ("ticker_id", "int32"),
        ("tick_type", "int32"),
        ("bid_size", "float64"),
        ("bid_price", "float64"),
        ("ask_price", "float64"),
        ("ask_size", "float64"),
        ("last_price", "float64"),
        ("last_size", "float64"),
        ("volume", "float64"),
    ])

    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._data = np.full(capacity, np.nan, dtype=self.dtype)
        self._write_idx = 0
        self.size = 0
        logger.debug("OrderFlowBuffer initialized with capacity {}", capacity)

    def append(self, ticker_id: int, tick_type: int, snapshot: Snapshot) -> None:
        now = time.time()
        row = (
            now,
            ticker_id,
            tick_type,
            snapshot.bid_size,
            snapshot.bid_price,
            snapshot.ask_price,
            snapshot.ask_size,
            snapshot.last_price,
            snapshot.last_size,
            snapshot.volume,
        )

        self._data[self._write_idx] = row
        self._write_idx = (self._write_idx + 1) % self.capacity
        if self.size < self.capacity:
            self.size += 1

    def to_array(self) -> np.ndarray:
        if self.size == 0:
            return np.empty(0, dtype=self.dtype)
        idx = (self._write_idx - self.size) % self.capacity
        if idx + self.size <= self.capacity:
            return self._data[idx: idx + self.size].copy()
        first_part = self._data[idx:].copy()
        second_part = self._data[:self._write_idx].copy()
        return np.concatenate((first_part, second_part))

    def tail(self, n: int) -> np.ndarray:
        if n <= 0 or self.size == 0:
            return np.empty(0, dtype=self.dtype)
        n = min(n, self.size)
        indices = (self._write_idx - np.arange(1, n + 1)) % self.capacity
        return self._data[indices][::-1].copy()


class SlidingHistogram:
    """
    Maintains a sliding window histogram for trade volume per price bin.
    """

    def __init__(self, window_seconds: float, bin_size: float, name: str):
        self.window_seconds = window_seconds
        self.bin_size = bin_size
        self.name = name
        self.events: deque[Tuple[float, float, float]] = deque()
        self.hist: Dict[float, float] = defaultdict(float)

    def _bin_key(self, price: float) -> float:
        if math.isnan(price):
            raise ValueError("Price cannot be NaN for histogram binning")
        return math.floor(price / self.bin_size) * self.bin_size

    def add(self, timestamp: float, price: float, value: float) -> None:
        if math.isnan(price) or math.isnan(value):
            return
        if value == 0.0:
            return
        bin_key = self._bin_key(price)
        self.events.append((timestamp, bin_key, value))
        self.hist[bin_key] += value
        self._expire(timestamp)

    def _expire(self, current_time: float) -> None:
        threshold = current_time - self.window_seconds
        while self.events and self.events[0][0] < threshold:
            ts, bin_key, val = self.events.popleft()
            self.hist[bin_key] -= val
            if abs(self.hist[bin_key]) < 1e-9:
                del self.hist[bin_key]

    def snapshot(self) -> Dict[float, float]:
        return dict(self.hist)

    def top_bins(self, n: int = 5) -> List[Tuple[float, float]]:
        return sorted(self.hist.items(), key=lambda item: item[1], reverse=True)[:n]


class BookWindow:
    """
    Tracks the latest best bid/ask liquidity per price bin within a
    sliding time window (level-1 order book).
    """

    def __init__(self, window_seconds: float, bin_size: float, name: str):
        self.window_seconds = window_seconds
        self.bin_size = bin_size
        self.name = name
        self.events: deque[Tuple[float, Tuple[str, float], float]] = deque()
        self.latest: Dict[Tuple[str, float], Tuple[float, float]] = {}

    def _bin_key(self, price: float) -> float:
        if math.isnan(price):
            raise ValueError("Price cannot be NaN for book binning")
        return math.floor(price / self.bin_size) * self.bin_size

    def add(self, timestamp: float, side: str, price: float, size: float) -> None:
        if math.isnan(price) or math.isnan(size):
            return
        if size < 0.0:
            return
        bin_key = self._bin_key(price)
        key = (side, bin_key)
        self.events.append((timestamp, key, size))
        self.latest[key] = (timestamp, size)
        self._expire(timestamp)

    def _expire(self, current_time: float) -> None:
        threshold = current_time - self.window_seconds
        while self.events and self.events[0][0] < threshold:
            ts, key, _size = self.events.popleft()
            latest_entry = self.latest.get(key)
            if latest_entry and latest_entry[0] == ts:
                del self.latest[key]

    def snapshot(self) -> Dict[Tuple[str, float], float]:
        return {key: size for key, (ts, size) in self.latest.items()}

# -----------------------------------------------------------------------------
# IB API application
# -----------------------------------------------------------------------------

PRICE_TICK_TYPES = {
    1: "BID",
    2: "ASK",
    4: "LAST",
}

SIZE_TICK_TYPES = {
    0: "BID_SIZE",
    3: "ASK_SIZE",
    5: "LAST_SIZE",
    8: "VOLUME",
}


class OrderFlowApp(EWrapper, EClient):
    def __init__(self, host: str, port: int, client_id: int,
                 buffer_capacity: int, bin_size: float,
                 log_tail_size: int = 5):
        EClient.__init__(self, wrapper=self)
        self.host = host
        self.port = port
        self.client_id = client_id
        self._connected = threading.Event()
        self.buffer = OrderFlowBuffer(buffer_capacity)
        self.snapshots: Dict[int, Snapshot] = defaultdict(Snapshot)
        self._log_tail_size = log_tail_size
        self._last_log = 0.0
        self._log_interval = 5.0
        self.bin_size = bin_size
        self.orderflow_windows: Dict[str, SlidingHistogram] = {
            "fast": SlidingHistogram(60.0, bin_size, "OrderFlow_fast"),
            "mid": SlidingHistogram(5 * 60.0, bin_size, "OrderFlow_mid"),
            "slow": SlidingHistogram(30 * 60.0, bin_size, "OrderFlow_slow"),
        }
        self.book_windows: Dict[str, BookWindow] = {
            "fast": BookWindow(60.0, bin_size, "Book_fast"),
            "mid": BookWindow(5 * 60.0, bin_size, "Book_mid"),
            "slow": BookWindow(30 * 60.0, bin_size, "Book_slow"),
        }

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect_and_start(self) -> None:
        logger.info("Connecting to IBKR on {}:{} with client_id={}",
                    self.host, self.port, self.client_id)
        self.connect(self.host, self.port, self.client_id)
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        logger.debug("Background reader thread started")
        # Wait for nextValidId to be received as proof of connection
        if not self._connected.wait(timeout=10.0):
            raise RuntimeError("Failed to receive nextValidId within timeout")

    def nextValidId(self, orderId: int):
        logger.info("Connection confirmed. Next valid order id: {}", orderId)
        self._connected.set()

    def error(self, reqId: int, errorCode: int, errorString: str):
        logger.error("Error. reqId={} code={} msg={}", reqId, errorCode, errorString)

    # ------------------------------------------------------------------
    # Market data handlers
    # ------------------------------------------------------------------

    def tickPrice(self, reqId: int, tickType: int, price: float,
                  attrib: TickAttrib):
        if tickType not in PRICE_TICK_TYPES:
            return
        snapshot = self.snapshots[reqId]
        if tickType == 1:  # BID
            snapshot.bid_price = price
        elif tickType == 2:  # ASK
            snapshot.ask_price = price
        elif tickType == 4:  # LAST
            snapshot.last_price = price

        self._record_tick(reqId, tickType, snapshot)

    def tickSize(self, reqId: int, tickType: int, size: float):
        if tickType not in SIZE_TICK_TYPES:
            return
        snapshot = self.snapshots[reqId]
        if tickType == 0:  # BID_SIZE
            snapshot.bid_size = size
        elif tickType == 3:  # ASK_SIZE
            snapshot.ask_size = size
        elif tickType == 5:  # LAST_SIZE
            snapshot.last_size = size
        elif tickType == 8:  # VOLUME
            snapshot.volume = size

        self._record_tick(reqId, tickType, snapshot)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _record_tick(self, req_id: int, tick_type: int, snapshot: Snapshot):
        now = time.time()
        self.buffer.append(req_id, tick_type, snapshot)
        self._handle_aggregations(now, tick_type, snapshot)
        if now - self._last_log >= self._log_interval:
            self._log_state(now)

    def request_market_data(self, contracts: Iterable[Contract]) -> None:
        for req_id, contract in enumerate(contracts, start=1):
            logger.info("Requesting market data for {} (req_id={})",
                        contract.symbol, req_id)
            self.reqMktData(req_id, contract, "", False, False, [])

    def _handle_aggregations(self, timestamp: float, tick_type: int,
                              snapshot: Snapshot) -> None:
        if tick_type == 5:  # LAST_SIZE
            price = snapshot.last_price
            size = snapshot.last_size
            for hist in self.orderflow_windows.values():
                hist.add(timestamp, price, size)
        elif tick_type in (0, 3):  # BID_SIZE or ASK_SIZE
            side = "bid" if tick_type == 0 else "ask"
            price = snapshot.bid_price if side == "bid" else snapshot.ask_price
            size = snapshot.bid_size if side == "bid" else snapshot.ask_size
            for book in self.book_windows.values():
                book.add(timestamp, side, price, size)

    def _log_state(self, timestamp: float) -> None:
        tail = self.buffer.tail(self._log_tail_size)
        if tail.size > 0:
            logger.info("Latest {} ticks:\n{}", self._log_tail_size, tail)
        else:
            logger.info("No data recorded yet")

        for hist in self.orderflow_windows.values():
            logger.info("{} top bins: {}", hist.name, hist.top_bins())

        for book in self.book_windows.values():
            logger.info("{} snapshot: {}", book.name, book.snapshot())

        self._last_log = timestamp


# -----------------------------------------------------------------------------
# Contract helpers
# -----------------------------------------------------------------------------

def create_stock_contract(symbol: str) -> Contract:
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.currency = "USD"
    contract.exchange = "SMART"
    return contract


# -----------------------------------------------------------------------------
# CLI handling
# -----------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="OrderFlow base data collector for IBKR market data."
    )
    parser.add_argument("--symbol", "-s", action="append", required=True,
                        help="Ticker symbol to subscribe to (repeatable).")
    parser.add_argument("--buffer-capacity", type=int, default=180_000,
                        help="Maximum number of rows retained in the buffer.")
    parser.add_argument("--price-bin-size", type=float, default=0.01,
                        help="Price bin size used for OrderFlow histograms.")
    parser.add_argument("--client-id", type=int,
                        default=int(os.getenv("IB_CLIENT_ID", 1)),
                        help="IBKR client id to use for the session.")
    parser.add_argument("--host", type=str,
                        default=os.getenv("IB_HOST", "127.0.0.1"),
                        help="TWS / IB Gateway host.")
    parser.add_argument("--port", type=int,
                        default=int(os.getenv("IB_PORT", 7497)),
                        help="TWS (paper) port or IB Gateway port.")
    return parser.parse_args(argv)


# -----------------------------------------------------------------------------
# Entry point
# -----------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv()
    args = parse_args(argv)

    symbols = args.symbol
    contracts = [create_stock_contract(sym) for sym in symbols]

    app = OrderFlowApp(
        host=args.host,
        port=args.port,
        client_id=args.client_id,
        buffer_capacity=args.buffer_capacity,
        bin_size=args.price_bin_size,
    )

    try:
        app.connect_and_start()
    except Exception:
        logger.exception("Unable to establish connection with IBKR")
        return 1

    app.request_market_data(contracts)

    logger.info("Streaming market data. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received. Disconnecting...")
    finally:
        app.disconnect()
        time.sleep(1.0)  # give the reader thread time to exit
        logger.info("Disconnected.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
