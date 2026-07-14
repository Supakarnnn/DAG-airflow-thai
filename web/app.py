"""Serving spine — FastAPI อ่าน gold สดคืน JSON + เสิร์ฟหน้าเว็บ 1 หน้า.
รัน: uv run uvicorn web.app:app --reload   (ต่อ Postgres host 127.0.0.1:5433)
docs ฟรีที่ /docs. อ่านอย่างเดียว ไม่มี auth (portfolio/local)."""
import os
import time
from collections import defaultdict, deque
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from fastapi.responses import FileResponse

from src.llm.summary import generate_summary
from src.llm.text_to_llm import is_safe_select, route_question

load_dotenv()  # OPENROUTER_API_KEY (DAG/Airflow ได้ผ่าน docker-compose env แทน)

READ_ONLY = {"options": "-c default_transaction_read_only=on -c statement_timeout=10000"}
ENGINE = create_engine(os.environ["DATABASE_URL"], connect_args=READ_ONLY, pool_pre_ping=True)
LLM_ENGINE = create_engine(os.environ["LLM_DATABASE_URL"], connect_args=READ_ONLY, pool_pre_ping=True)
HERE = Path(__file__).resolve().parent
app = FastAPI(title="Thai Macro & Market Data", docs_url=None, redoc_url=None, openapi_url=None)

# ponytail: single-process limiter is enough for one portfolio VPS; use Redis if it scales out.
_REQUESTS: dict[str, deque[float]] = defaultdict(deque)


class Question(BaseModel):
    question: str = Field(min_length=1, max_length=500)


def rate_limit(request: Request, limit: int) -> None:
    now = time.monotonic()
    key = request.client.host if request.client else "unknown"
    hits = _REQUESTS[key]
    while hits and hits[0] < now - 60:
        hits.popleft()
    if len(hits) >= limit:
        raise HTTPException(429, "ลองใหม่ในอีกสักครู่")
    hits.append(now)


def rows(sql: str, **params) -> list[dict]:
    """query gold -> list of dict (date/Decimal ให้ FastAPI encode เอง)."""
    with ENGINE.connect() as c:
        return [dict(r) for r in c.execute(text(sql), params).mappings()]


@app.get("/")
def index():
    return FileResponse(HERE / "index.html")


@app.get("/health")
def health():
    with ENGINE.connect() as c:
        c.execute(text("SELECT 1"))
    return {"status": "ok"}


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


@app.post("/api/ask")
def ask(body: Question, request: Request):
    """chatbot-style: LLM ตัดสินใจเอง (tool calling) ว่าต้อง query gold หรือตอบเป็น chat ธรรมดา
    (ทักทาย/นอกเรื่อง). ถ้าเลือก query -> is_safe_select() กัน -> รันจริงผ่าน role llm_readonly."""
    rate_limit(request, 10)
    question = body.question
    decision = route_question(question)
    if decision["type"] == "chat":
        return {"question": question, "type": "chat", "answer": decision["text"]}

    sql = decision["sql"]
    if not is_safe_select(sql):
        raise HTTPException(400, "คำถามนี้ไม่สามารถประมวลผลได้")
    try:
        with LLM_ENGINE.connect() as c:
            result = [dict(r) for r in c.execute(text(sql)).mappings()]
    except SQLAlchemyError:
        raise HTTPException(400, "คำถามนี้ไม่สามารถประมวลผลได้") from None
    return {"question": question, "type": "sql", "result": result[:100]}


@app.get("/api/summary")
def summary(request: Request):
    """สรุปภาษาไทยของเดือนล่าสุดใน gold.dashboard_monthly (ground บน source_rows ที่ส่งกลับมาด้วย)."""
    rate_limit(request, 5)
    source_rows = rows("SELECT * FROM gold.dashboard_monthly ORDER BY obs_month DESC LIMIT 3")
    return {"narrative": generate_summary(source_rows), "source_rows": source_rows}
