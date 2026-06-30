"""Serving spine — FastAPI อ่าน gold สดคืน JSON + เสิร์ฟหน้าเว็บ 1 หน้า.
รัน: uv run uvicorn web.app:app --reload   (ต่อ Postgres host 127.0.0.1:5433)
docs ฟรีที่ /docs. อ่านอย่างเดียว ไม่มี auth (portfolio/local)."""
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, text

ENGINE = create_engine("postgresql+psycopg2://warehouse:warehouse@127.0.0.1:5433/warehouse")
HERE = Path(__file__).resolve().parent
app = FastAPI(title="Thai Macro & Market Data")


def rows(sql: str, **params) -> list[dict]:
    """query gold -> list of dict (date/Decimal ให้ FastAPI encode เอง)."""
    with ENGINE.connect() as c:
        return [dict(r) for r in c.execute(text(sql), params).mappings()]


@app.get("/")
def index():
    return FileResponse(HERE / "index.html")


@app.get("/api/dashboard")
def dashboard():
    """ทุก mart รายเดือนรวม FULL JOIN (SET/FX/policy/cpi/real_rate)."""
    return rows("SELECT * FROM gold.dashboard_monthly ORDER BY obs_month")


@app.get("/api/forecast")
def forecast(indicator: str = "TH.SET_INDEX"):
    """actual + forecast ของ indicator เดียว (is_forecast แยกเส้น)."""
    return rows("""
        SELECT obs_month, yhat, yhat_lower, yhat_upper, is_forecast
        FROM gold.forecast_monthly WHERE indicator = :i ORDER BY obs_month
    """, i=indicator)


@app.get("/api/correlation")
def correlation():
    return rows("SELECT * FROM gold.correlation_matrix ORDER BY abs(correlation) DESC")
