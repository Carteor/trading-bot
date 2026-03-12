import pandas as pd
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("date")

    df["daily_return"] = df["close"].pct_change()
    df["ma_7"] = df["close"].rolling(window=7).mean()
    df["ma_21"] = df["close"].rolling(window=21).mean()
    df["volatility_21"] = df["daily_return"].rolling(window=21).std()

    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df["rsi_14"] = 100 - (100 / (1 + rs))

    return df


def transform(engine) -> pd.DataFrame:
    logger.info("Reading from raw.prices")
    df = pd.read_sql("SELECT * FROM raw.prices", engine)

    logger.info("Computing indicators per symbol")
    df_enriched = (
        df.groupby("symbol", group_keys=False)
            .apply(compute_indicators)
    )
    df_enriched = df_enriched.drop(columns=["id"])

    mart_columns = ["symbol", "date", "close", "ma_7", "ma_21", "rsi_14", "daily_return", "volatility_21"]
    logger.info(f"Transform complete, {len(df_enriched)} rows")

    return df_enriched[mart_columns]
