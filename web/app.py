from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from fastapi.responses import FileResponse

from src.llm.summary import generate_summary
from src.llm.text_to_llm import is_safe_select, route_question

load_dotenv()

ENGINE = create_engine("postgresql+psycopg2://warehouse:warehouse@127.0.0.1:5440/warehouse")
LLM_ENGINE = create_engine("postgresql+psycopg2://llm_readonly:llm_readonly@127.0.0.1:5440/warehouse")
HERE = Path(__file__).resolve().parent
app = FastAPI(title="Thai Macro & Market Data")


def rows(sql: str, **params) -> list[dict]:
    with ENGINE.connect() as c:
        return [dict(r) for r in c.execute(text(sql), params).mappings()]


@app.get("/")
def index():
    return FileResponse(HERE / "index.html")


@app.get("/api/dashboard")
def dashboard():
    return rows("SELECT * FROM gold.dashboard_monthly ORDER BY obs_month")


@app.get("/api/forecast")
def forecast(indicator: str = "TH.SET_INDEX"):
    return rows("""
        SELECT obs_month, yhat, yhat_lower, yhat_upper, is_forecast
        FROM gold.forecast_monthly WHERE indicator = :i ORDER BY obs_month
    """, i=indicator)


@app.get("/api/correlation")
def correlation():
    return rows("SELECT * FROM gold.correlation_matrix ORDER BY abs(correlation) DESC")


@app.get("/api/ask")
def ask(question: str):
    decision = route_question(question)
    if decision["type"] == "chat":
        return {"question": question, "type": "chat", "answer": decision["text"]}

    sql = decision["sql"]
    if not is_safe_select(sql):
        raise HTTPException(400, f"SQL ที่ gen ไม่ผ่านการตรวจสอบ: {sql}")
    try:
        with LLM_ENGINE.connect() as c:
            result = [dict(r) for r in c.execute(text(sql)).mappings()]
    except SQLAlchemyError as e:
        raise HTTPException(400, f"รัน SQL ไม่สำเร็จ: {e}") from e
    return {"question": question, "type": "sql", "sql": sql, "result": result}


@app.get("/api/summary")
def summary():
    source_rows = rows("SELECT * FROM gold.dashboard_monthly ORDER BY obs_month DESC LIMIT 3")
    return {"narrative": generate_summary(source_rows), "source_rows": source_rows}
