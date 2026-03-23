import requests
import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

FRED_SERIES = {
    "FEDFUNDS": "federal_funds_rate",
    "CPIAUCSL": "cpi_inflation",
    "DCOILWTICO": "crude_oil_price",
}

def fetch_series(series_id: str, start: str, api_key: str) -> pd.DataFrame:
    logger.info(f"Fetching FRED series {series_id}")
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "observation_start": start,
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    observations = response.json()["observations"]
    df = pd.DataFrame(observations)[["date", "value"]]
    df["series_id"] = series_id
    df["indicator_name"] = FRED_SERIES[series_id]
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])
    return df

def extract_fred(series_ids: list[str], start: str) -> pd.DataFrame:
    api_key = os.getenv("FRED_API_KEY")
    frames = []
    for series_id in series_ids:
        frames.append(fetch_series(series_id, start, api_key))
    return pd.concat(frames, ignore_index=True)
