"""SET extract — ดึง ^SET.BK คืน DataFrame สะอาด (obs_date, close, volume). ไม่แตะ DB."""
import pandas as pd
import yfinance as yf


def fetch_set(period: str = "5y") -> pd.DataFrame:
    df = yf.download("^SET.BK", period=period, interval="1d", auto_adjust=False)[["Close", "Volume"]]
    df.columns = df.columns.droplevel("Ticker")        # แบน MultiIndex -> Close, Volume
    df = df.reset_index().rename(columns={"Date": "obs_date", "Close": "close", "Volume": "volume"})
    df["obs_date"] = pd.to_datetime(df["obs_date"]).dt.date
    return df[df["volume"] > 0].reset_index(drop=True)  # ตัดวันตลาดยังไม่ปิด (volume=0)


if __name__ == "__main__":  # self-check: รัน python -m src.extract.set
    out = fetch_set()
    assert {"obs_date", "close", "volume"} == set(out.columns), "คอลัมน์ผิด"
    assert (out["volume"] > 0).all(), "ยังมี volume=0 หลุดมา"
    assert out["close"].gt(0).all(), "มี close <= 0"
    print(f"OK: {len(out)} แถว, {out.obs_date.min()} → {out.obs_date.max()}")
