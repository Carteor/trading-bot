import pandas as pd
import logging
from sqlalchemy import text

logger = logging.getLogger(__name__)


def load_raw_prices(df: pd.DataFrame, engine) -> None:
    logger.info(f"Loading {len(df)} rows to raw.prices")
    symbols = df["symbol"].unique().tolist()
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM raw.prices WHERE symbol = ANY(:symbols)"),
            {"symbols": symbols}
        )
    df.to_sql(
        name="prices",
        schema="raw",
        con=engine,
        if_exists="append",
        index=False
    )
    logger.info("Done loading raw.prices")

def load_mart(df: pd.DataFrame, engine) -> None:
    logger.info(f"Loading {len(df)} rows to mart.daily_market_summary")
    symbols = df["symbol"].unique().tolist()
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM mart.daily_market_summary WHERE symbol = ANY(:symbols)"),
            {"symbols": symbols}
        )
    df.to_sql(
        name="daily_market_summary",
        schema="mart",
        con=engine,
        if_exists="append",
        index=False
    )
    logger.info("Done loading mart.daily_market_summary")
