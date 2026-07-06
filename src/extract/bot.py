import os
from datetime import datetime

import pandas as pd
import requests

URL = "https://gateway.api.bot.or.th/PolicyRate/v3/policy_rate"
STAT_OBS_URL = "https://gateway.api.bot.or.th/observations"


def fetch_bot_series(series_code: str, start: str = "2000-01-01", end: str | None = None) -> pd.DataFrame:
    end = end or datetime.today().strftime("%Y-%m-%d")
    r = requests.get(STAT_OBS_URL, headers={"Authorization": os.environ["BOT_API_STAT_KEY"]},
                     params={"series_code": series_code, "start_period": start, "end_period": end}, timeout=30)
    r.raise_for_status()
    obs = r.json()["result"]["series"][0]["observations"]
    df = pd.DataFrame(obs)
    df["obs_date"] = pd.to_datetime(df["period_start"], format="%Y-%m").dt.date  # yyyy-mm → เดือนที่ 1
    df["value"] = df["value"].astype(float)
    return df[["obs_date", "value"]].dropna().reset_index(drop=True)


def fetch_policy_rate() -> dict:
    r = requests.get(URL, headers={"Authorization": os.environ["BOT_API_KEY"]}, timeout=30)
    r.raise_for_status()
    res = r.json()["result"]
    eff = datetime.strptime(res["effective_datetime"], "%Y-%m-%d %H:%M:%S").date()
    return {
        "obs_date": str(eff),
        "value": float(res["data"]),
        "news_th": res.get("news_text_th"),
        "news_en": res.get("news_text_en"),
        "raw": res,
    }
