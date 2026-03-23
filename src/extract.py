import yfinance as yf
import logging
import pandas as pd
from datetime import date

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def load_data(symbol: str, start: str, end: str = str(date.today())) -> pd.DataFrame:
    logger.info(f"load_data({symbol}, {start}, {end})")
    data = yf.download(
        symbol, start=start, end=end, interval="1d", auto_adjust=True
    )

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = [col[0].lower() for col in data.columns]
    else:
        data.columns = [col.lower() for col in data.columns]

    data["symbol"] = symbol
    data = data.reset_index()
    data.columns = [col.lower() for col in data.columns]
    data = data.dropna(subset=["close"])

    return data


def extract(symbols: list[str], start: str) -> pd.DataFrame:
    frames = []
    for symbol in symbols:
        logger.info(f"Extracting {symbol}")
        frames.append(load_data(symbol, start))

    return pd.concat(frames, ignore_index=True)
