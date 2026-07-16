import os
import re

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.tools import tool
from langchain_openrouter import ChatOpenRouter

MODEL = os.environ.get("OPENROUTER_MODEL", "nvidia/nemotron-3-ultra-550b-a55b:free")

SCHEMA = """Read-only Postgres schema:

gold.dashboard_monthly(
obs_month date, set_close numeric, set_mom numeric, set_volume numeric,
usdthb numeric, usdthb_mom numeric, policy_rate numeric, cpi_yoy numeric,
bond_10y numeric, real_rate numeric
)

gold.forecast_monthly(
obs_month date, indicator text, yhat numeric, yhat_lower numeric,
yhat_upper numeric, is_forecast boolean, model text, generated_at timestamptz
)
indicator: 'TH.SET_INDEX', 'FX.USDTHB'
is_forecast=false: actual, true: forecast

gold.correlation_matrix(
var1 text, var2 text, correlation numeric, n bigint
)
"""

SYSTEM_PROMPT = f"""You are the assistant embedded in a Thai macro/market data dashboard.

Schema:
{SCHEMA}

Rules:

If the question needs real data from the schema above, call the query_gold_data tool with one
valid PostgreSQL SELECT query (no Markdown, no explanations, gold-qualified tables only, no other
schemas/system tables/functions/multiple statements, no invented tables/columns/indicator values).

SQL best practices (dashboard_monthly is a FULL JOIN of several sources, so every column has NULLs
in different month ranges):
- "Latest value of X": always filter WHERE <column> IS NOT NULL before ORDER BY obs_month DESC,
  otherwise you return a row where the asked column is still NULL.
- Same rule for MIN/MAX/"highest"/"lowest" questions: exclude NULLs of the asked column.
- Select only the columns the question asks about (plus obs_month), not SELECT *.
- Always ORDER BY, and LIMIT to what the question needs (e.g. LIMIT 1 for "latest", LIMIT 6 for
  "last 6 months").
- Use is_forecast=true for forecasts and false for actual values in forecast_monthly.

Otherwise (greeting, small talk, general knowledge, or anything the schema above cannot answer),
do NOT answer from your own knowledge. Reply briefly in Thai that you can only answer questions
about this dashboard's data, and mention what kind of question you can actually help with.
Never use the em dash character (—)."""


@tool
def query_gold_data(sql: str) -> str:
    return sql


_FORBIDDEN = re.compile(r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|call)\b", re.I)


def route_question(question: str) -> dict:
    llm = ChatOpenRouter(model=MODEL, temperature=0, max_tokens=512).bind_tools([query_gold_data])
    resp = llm.invoke([SystemMessage(SYSTEM_PROMPT), HumanMessage(question)])
    if resp.tool_calls:
        sql = resp.tool_calls[0]["args"]["sql"].strip()
        sql = re.sub(r"^```sql\s*|```$", "", sql, flags=re.I | re.M).strip()
        return {"type": "sql", "sql": sql}
    return {"type": "chat", "text": resp.content.strip().replace("—", ",")}


def is_safe_select(sql: str) -> bool:
    body = sql.strip().rstrip(";")
    if ";" in body or not re.match(r"^(select|with)\b", body, re.I):
        return False
    return not _FORBIDDEN.search(body)


if __name__ == "__main__":
    print("ok")

