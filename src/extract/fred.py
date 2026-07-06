import os
from dotenv import load_dotenv
import pandas as pd
from fredapi import Fred

load_dotenv()

def fetch_series(series_id: str, start: str | None = None) -> pd.DataFrame:
    fred = Fred(api_key=os.environ["FRED_API_KEY"])
    s = fred.get_series(series_id, observation_start=start)
    df = s.rename("value").rename_axis("obs_date").reset_index()
    df["obs_date"] = pd.to_datetime(df["obs_date"]).dt.date
    return df.dropna(subset=["value"]).reset_index(drop=True)  # FRED use NaN as วันหยุด

