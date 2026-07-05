import pandas as pd
import yfinance as yf


def fetch_set_raw(period: str = "max") -> pd.DataFrame:
    return yf.download("^SET.BK", period=period, interval="1d", auto_adjust=False)


def fetch_set(period: str = "max") -> pd.DataFrame:
    df = yf.download("^SET.BK", period=period, interval="1d", auto_adjust=False)[["Close", "Volume"]]
    df.columns = df.columns.droplevel("Ticker")
    df = df.reset_index().rename(columns={"Date": "obs_date", "Close": "close", "Volume": "volume"})
    df["obs_date"] = pd.to_datetime(df["obs_date"]).dt.date
    return df[df["volume"] > 0].reset_index(drop=True)

if __name__=="__main__":
    print(fetch_set_raw())
