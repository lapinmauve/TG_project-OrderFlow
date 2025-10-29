"""
OrdreFLow_base_vis.py
----------------------
Real-time Order Flow and Level 1 book visualization for UVXY using the IBKR API.

Launch this script (e.g. in Spyder) to connect to TWS/Gateway, stream ticks,
accumulate sliding-window order-flow histograms (60s / 5min / 30min) and
display rolling volume profiles alongside best bid/ask liquidity bins.
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

import matplotlib.pyplot as plt
import numpy as np
from dotenv import load_dotenv
from ibapi.client import EClient
from ibapi.common import TickAttrib
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper
from loguru import logger
from matplotlib.animation import FuncAnimation


logger.remove()
logger.add(sys.stdout, colorize=True,
           format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                  "<level>{level: <8}</level> | "
                  "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                  "<level>{message}</level>")
logger.add(sys.stderr, format="{time:MMMM D, YYYY > HH:mm:ss} | {level} | {message} | {extra}")


@dataclass
class Snapshot:
    bid_size: float = np.nan
    bid_price: float = np.nan
    ask_price: float = np.nan
    ask_size: float = np.nan
    last_price: float = np.nan
    last_size: float = np.nan
    volume: float = np.nan


class OrderFlowBuffer:
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

    def append(self, ticker_id: int, tick_type: int, snapshot: Snapshot) -> None:
        now = time.time()
        row = (
            now,
            ticker_id,
            tick_type,
            float(snapshot.bid_size),
            float(snapshot.bid_price),
            float(snapshot.ask_price),
            float(snapshot.ask_size),
            float(snapshot.last_price),
            float(snapshot.last_size),
            float(snapshot.volume),
        )
        self._data[self._write_idx] = row
        self._write_idx = (self._write_idx + 1) % self.capacity
        if self.size < self.capacity:
            self.size += 1

    def tail(self, n: int) -> np.ndarray:
        if n <= 0 or self.size == 0:
            return np.empty(0, dtype=self.dtype)
        n = min(n, self.size)
        indices = (self._write_idx - np.arange(1, n + 1)) % self.capacity
        return self._data[indices][::-1].copy()


class SlidingHistogram:
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
        if math.isnan(price) or math.isnan(value) or value == 0.0:
            return
        bin_key = self._bin_key(float(price))
        value_float = float(value)
        self.events.append((timestamp, bin_key, value_float))
        self.hist[bin_key] += value_float
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


class BookWindow:
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
        if math.isnan(price) or math.isnan(size) or size < 0.0:
            return
        bin_key = self._bin_key(float(price))
        key = (side, bin_key)
        size_float = float(size)
        self.events.append((timestamp, key, size_float))
        self.latest[key] = (timestamp, size_float)
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


PRICE_TICK_TYPES = {1: "BID", 2: "ASK", 4: "LAST"}
SIZE_TICK_TYPES = {0: "BID_SIZE", 3: "ASK_SIZE", 5: "LAST_SIZE", 8: "VOLUME"}


class OrderFlowApp(EWrapper, EClient):
    def __init__(self, host: str, port: int, client_id: int,
                 buffer_capacity: int, bin_size: float):
        EClient.__init__(self, wrapper=self)
        self.host = host
        self.port = port
        self.client_id = client_id
        self._connected = threading.Event()
        self.buffer = OrderFlowBuffer(buffer_capacity)
        self.snapshots: Dict[int, Snapshot] = defaultdict(Snapshot)
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
        self._data_lock = threading.Lock()

    def connect_and_start(self) -> None:
        logger.info("Connecting to IBKR on {}:{} with client_id={}",
                    self.host, self.port, self.client_id)
        self.connect(self.host, self.port, self.client_id)
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        if not self._connected.wait(timeout=10.0):
            raise RuntimeError("Failed to receive nextValidId within timeout")

    def nextValidId(self, orderId: int):
        logger.info("Connection confirmed. Next valid order id: {}", orderId)
        self._connected.set()

    def error(self, reqId: int, errorCode: int, errorString: str):
        logger.error("Error. reqId={} code={} msg={}", reqId, errorCode, errorString)

    def tickPrice(self, reqId: int, tickType: int, price: float,
                  attrib: TickAttrib):
        if tickType not in PRICE_TICK_TYPES:
            return
        snapshot = self.snapshots[reqId]
        if tickType == 1:
            snapshot.bid_price = float(price)
        elif tickType == 2:
            snapshot.ask_price = float(price)
        elif tickType == 4:
            snapshot.last_price = float(price)
        self._record_tick(reqId, tickType, snapshot)

    def tickSize(self, reqId: int, tickType: int, size: float):
        if tickType not in SIZE_TICK_TYPES:
            return
        snapshot = self.snapshots[reqId]
        if tickType == 0:
            snapshot.bid_size = float(size)
        elif tickType == 3:
            snapshot.ask_size = float(size)
        elif tickType == 5:
            snapshot.last_size = float(size)
        elif tickType == 8:
            snapshot.volume = float(size)
        self._record_tick(reqId, tickType, snapshot)

    def _record_tick(self, req_id: int, tick_type: int, snapshot: Snapshot):
        now = time.time()
        with self._data_lock:
            self.buffer.append(req_id, tick_type, snapshot)
            self._handle_aggregations(now, tick_type, snapshot)

    def _handle_aggregations(self, timestamp: float, tick_type: int,
                              snapshot: Snapshot) -> None:
        if tick_type == 5:
            price = snapshot.last_price
            size = snapshot.last_size
            for hist in self.orderflow_windows.values():
                hist.add(timestamp, price, size)
        elif tick_type in (0, 3):
            side = "bid" if tick_type == 0 else "ask"
            price = snapshot.bid_price if side == "bid" else snapshot.ask_price
            size = snapshot.bid_size if side == "bid" else snapshot.ask_size
            for book in self.book_windows.values():
                book.add(timestamp, side, price, size)

    def request_market_data(self, contracts: Iterable[Contract]) -> None:
        for req_id, contract in enumerate(contracts, start=1):
            logger.info("Requesting market data for {} (req_id={})",
                        contract.symbol, req_id)
            self.reqMktData(req_id, contract, "", False, False, [])

    def get_orderflow_snapshot(self) -> Dict[str, Dict[float, float]]:
        with self._data_lock:
            return {name: hist.snapshot() for name, hist in self.orderflow_windows.items()}

    def get_book_snapshot(self) -> Dict[str, Dict[Tuple[str, float], float]]:
        with self._data_lock:
            return {name: book.snapshot() for name, book in self.book_windows.items()}

    def get_latest_price(self) -> Optional[float]:
        with self._data_lock:
            # Using the first snapshot available
            if not self.snapshots:
                return None
            any_snapshot = next(iter(self.snapshots.values()))
            return any_snapshot.last_price


def create_stock_contract(symbol: str) -> Contract:
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.currency = "USD"
    contract.exchange = "SMART"
    return contract


class OrderFlowVisualizer:
    def __init__(self, app: OrderFlowApp, symbol: str,
                 update_interval_ms: int = 1000):
        self.app = app
        self.symbol = symbol
        self.update_interval_ms = update_interval_ms

        plt.style.use("dark_background")
        self.fig, (self.ax_profile, self.ax_book) = plt.subplots(
            nrows=1, ncols=2, sharey=True, figsize=(12, 6),
            gridspec_kw={"width_ratios": [2, 1]}
        )
        self.fig.suptitle(f"Order Flow & L1 Book — {symbol}", fontsize=16)
        self.ax_profile.set_xlabel("Volume")
        self.ax_profile.set_ylabel("Price")
        self.ax_profile.set_title("Order Flow")

        self.ax_book.set_title("Best Bid/Ask Liquidity")
        self.ax_book.set_xlabel("Size (Bid ←   → Ask)")

        self.animation = FuncAnimation(
            self.fig, self._update_plot, interval=self.update_interval_ms, blit=False
        )

    def run(self):
        plt.tight_layout()
        plt.show()

    def _update_plot(self, _frame):
        orderflow = self.app.get_orderflow_snapshot()
        book = self.app.get_book_snapshot()
        last_price = self.app.get_latest_price()

        # Determine the sorted list of price bins to plot
        price_bins = sorted({
            bin_key
            for hist in orderflow.values()
            for bin_key in hist.keys()
        } | {
            bin_key
            for book_snap in book.values()
            for (_side, bin_key) in book_snap.keys()
        })

        if not price_bins:
            self.ax_profile.clear()
            self.ax_book.clear()
            self.ax_profile.set_title("Order Flow (waiting for data)")
            self.ax_book.set_title("Best Bid/Ask Liquidity")
            return

        # Build bar arrays for each horizon (side-by-side grouped bars)
        horizons = [("fast", "#1f77b4"), ("mid", "#ff7f0e"), ("slow", "#2ca02c")]
        self.ax_profile.clear()
        self.ax_profile.set_title("Order Flow")
        self.ax_profile.set_xlabel("Volume")
        self.ax_profile.set_ylabel("Price")

        bar_height = self.app.bin_size * 0.28
        offset_increment = self.app.bin_size / (len(horizons) + 1)
        offsets = {
            name: (idx - (len(horizons) - 1) / 2.0) * offset_increment
            for idx, (name, _color) in enumerate(horizons)
        }

        for name, color in horizons:
            if name not in orderflow:
                continue
            offset = float(offsets[name])
            price_positions = [float(bin_key) + offset for bin_key in price_bins]
            volumes = [float(orderflow[name].get(bin_key, 0.0)) for bin_key in price_bins]
            self.ax_profile.barh(
                price_positions, volumes, height=bar_height,
                color=color, alpha=0.6, label=name
            )

        handles, labels = self.ax_profile.get_legend_handles_labels()
        unique = dict(zip(labels, handles))
        self.ax_profile.legend(unique.values(), unique.keys(), loc="lower right")
        if last_price:
            self.ax_profile.axhline(last_price, color="white", linestyle="--", linewidth=1)
            self.ax_profile.text(
                self.ax_profile.get_xlim()[1], last_price,
                f" Last: {last_price:.2f}", va="bottom", ha="right", color="white"
            )

        # Book plot
        self.ax_book.clear()
        self.ax_book.set_title("Best Bid/Ask Liquidity")
        self.ax_book.set_xlabel("Size (Bid ←   → Ask)")
        color_map = {
            "fast": ("#1f77b4", "#d62728"),
            "mid": ("#9467bd", "#ff9896"),
            "slow": ("#2ca02c", "#8c564b"),
        }

        book_max = 0.0
        for name in [n for n, _c in horizons]:
            if name not in book:
                continue
            offset = float(offsets[name])
            price_positions = [float(bin_key) + offset for bin_key in price_bins]
            bid_sizes = [-float(book[name].get(("bid", bin_key), 0.0)) for bin_key in price_bins]
            ask_sizes = [float(book[name].get(("ask", bin_key), 0.0)) for bin_key in price_bins]
            local_max = max([abs(val) for val in bid_sizes + ask_sizes] or [0.0])
            book_max = max(book_max, local_max)
            bid_color, ask_color = color_map[name]
            self.ax_book.barh(
                price_positions, bid_sizes, height=bar_height,
                color=bid_color, alpha=0.6, label=f"{name} bid"
            )
            self.ax_book.barh(
                price_positions, ask_sizes, height=bar_height,
                color=ask_color, alpha=0.6, label=f"{name} ask"
            )

        self.ax_book.axvline(0, color="white", linewidth=0.8)
        if book_max > 0.0:
            self.ax_book.set_xlim(-book_max * 1.2, book_max * 1.2)
        book_handles, book_labels = self.ax_book.get_legend_handles_labels()
        unique_book = dict(zip(book_labels, book_handles))
        self.ax_book.legend(unique_book.values(), unique_book.keys(), loc="lower right")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Real-time Order Flow visualizer for IBKR market data."
    )
    parser.add_argument("--symbol", default="UVXY",
                        help="Ticker symbol to subscribe to (default: UVXY).")
    parser.add_argument("--buffer-capacity", type=int, default=180_000,
                        help="Maximum number of tick rows retained in memory.")
    parser.add_argument("--price-bin-size", type=float, default=0.02,
                        help="Price bin size used for histograms.")
    parser.add_argument("--client-id", type=int,
                        default=int(os.getenv("IB_CLIENT_ID", 1)),
                        help="IBKR client id to use for the session.")
    parser.add_argument("--host", type=str,
                        default=os.getenv("IB_HOST", "127.0.0.1"),
                        help="TWS / IB Gateway host.")
    parser.add_argument("--port", type=int,
                        default=int(os.getenv("IB_PORT", 7497)),
                        help="TWS paper port or IB Gateway port.")
    parser.add_argument("--update-interval", type=int, default=1000,
                        help="Plot update interval in milliseconds.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    load_dotenv()
    args = parse_args(argv)

    symbol = args.symbol
    contract = create_stock_contract(symbol)

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

    app.request_market_data([contract])
    logger.info("Streaming market data. Close the figure window to stop.")

    visualizer = OrderFlowVisualizer(app, symbol, args.update_interval)
    try:
        visualizer.run()
    finally:
        logger.info("Disconnecting from IBKR...")
        app.disconnect()
        time.sleep(1.0)
        logger.info("Disconnected.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
