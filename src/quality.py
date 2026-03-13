import pandas as pd
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def check_nulls(df):
    # close, symbol, date
    null_columns = df.columns[df.isna().any()].tolist()
    for null_column in null_columns:
        if null_column in ['close', 'symbol', 'date']:
            return False
    return True

def check_duplicates(df):
    # same symbol + same date
    duplicates = df.duplicated(subset=['symbol', 'date']).any()
    if duplicates:
        return False
    return True

def check_prices(df):
    # daily_return is not more than 50% in a day
    anomaly = (df['daily_return'].abs() > 0.5).any()
    if anomaly:
        return False
    return True

def run_quality_checks(engine) -> None:
    df = pd.read_sql("SELECT * FROM mart.daily_market_summary", engine)

    checks = [check_nulls, check_duplicates, check_prices]

    for check in checks:
        if not check(df):
            raise Exception(f"Quality check failed: {check.__name__}")
