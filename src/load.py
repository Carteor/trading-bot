import pandas as pd


def load_raw(df: pd.DataFrame, engine) -> None:
    df.to_sql(
        name="prices",
        schema="raw",
        con=engine,
        if_exists="append",
        index=False
    )
