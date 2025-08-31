import os
import shutil
import logging
import sys

import matplotlib.pyplot as plt
import pandas as pd
import yfinance as yf
from alpaca_trade_api.rest import REST
from dotenv import load_dotenv

from sqlalchemy import create_engine


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler("app.log", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

if not logger.hasHandlers():
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def load_data(symbol: str, start: str, end: str) -> pd.DataFrame:
    logger.info(f"load_data({symbol}, {start}, {end})")
    data = yf.download(symbol, start=start, end=end, interval="1d", auto_adjust=True)
    data["Signal"] = 0
    return data


def apply_strategy(data: pd.DataFrame, start_cash: float) -> tuple:
    logger.info("apply_strategy()")
    cash = start_cash
    position = 0
    buy_price = None

    for i in range(1, len(data)):
        price = data["Close"].iloc[i].item()
        yesterday_price = data["Close"].iloc[i - 1].item()

        if position == 0:
            drop = (price - yesterday_price) / yesterday_price
            if drop < -0.02:
                position = cash // price
                buy_price = price
                cash -= position * price
                data.at[data.index[i], "Signal"] = 1

        elif position > 0:
            gain = (price - buy_price) / buy_price
            if gain > 0.03:
                cash += position * price
                position = 0
                buy_price = None
                data.at[data.index[i], "Signal"] = -1

    final_value = cash + position * data["Close"].iloc[-1].item()
    return final_value, data


def calculate_buy_and_hold(data: pd.DataFrame, start_cash: float) -> float:
    start_price = data["Close"].iloc[0].item()
    end_price = data["Close"].iloc[-1].item()
    return start_cash * (end_price / start_price)


def plot_results(
    data: pd.DataFrame,
    start_cash: float,
    final_value: float,
    buy_and_hold_value: float,
    symbol: str,
):
    plt.figure(figsize=(12, 6))
    plt.plot(data["Close"], label="Close Price")
    plt.plot(
        data[data["Signal"] == 1].index,
        data[data["Signal"] == 1]["Close"],
        "^",
        color="g",
        label="Buy",
        markersize=10,
    )
    plt.plot(
        data[data["Signal"] == -1].index,
        data[data["Signal"] == -1]["Close"],
        "v",
        color="r",
        label="Sell",
        markersize=10,
    )

    plt.plot(
        data.index,
        start_cash * (data["Close"] / data["Close"].iloc[0]),
        label="Buy and Hold",
        linestyle="--",
        color="blue",
    )
    plt.title(f"{symbol} Backtest: ${start_cash} -> ${final_value:.2f}")
    plt.legend()
    plt.savefig("backtest_plot.png")

    if shutil.which("xdg-open"):
        os.system("xdg-open backtest_plot.png")
    else:
        logger.info("Plot saved as backtest_plot.png. Please open it manually.")


def main():
    load_dotenv()

    API_KEY = os.getenv("ALPACA_API_KEY")
    API_SECRET = os.getenv("ALPACA_SECRET_KEY")
    BASE_URL = os.getenv("ALPACA_BASE_URL")

    api = REST(API_KEY, API_SECRET, BASE_URL)

    if API_KEY:
        logger.info(f"API Key loaded: {API_KEY[:5]}...")
    else:
        logger.info("Not loaded")

    PGUSER = os.getenv("POSTGRES_USER")
    PGPASSWORD = os.getenv("POSTGRES_PASSWORD")

    engine = create_engine(f'postgresql+psycopg2://{PGUSER}:{PGPASSWORD}@db/pguser')



    symbol = "AAPL"
    start_cash = 1000
    start_data = "2024-01-01"
    end_date = "2025-01-01"

    data = load_data(symbol, start_data, end_date)
    final_value, data_with_signals = apply_strategy(data, start_cash)
    buy_and_hold_value = calculate_buy_and_hold(data, start_cash)

    logger.info(
        f"Start: ${start_cash:.2f}, End: ${final_value:.2f}, Profit: ${
            final_value - start_cash:.2f}"
    )
    logger.info(
        f"Buy and Hold Returns: ${buy_and_hold_value:.2f}, Profit: ${
            buy_and_hold_value - start_cash:.2f}"
    )

    plot_results(data_with_signals, start_cash, final_value, buy_and_hold_value, symbol)


if __name__ == "__main__":
    main()
