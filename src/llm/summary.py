import os

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openrouter import ChatOpenRouter

MODEL = os.environ.get("OPENROUTER_MODEL", "nvidia/nemotron-3-ultra-550b-a55b:free")

SYSTEM_PROMPT = """You are an analyst specializing in the Thai economy.

Based on the provided JSON data, write a concise summary in Thai using 2 to 4 sentences.

Requirements:

* Use only the facts and numerical values explicitly provided in the JSON data.
* Do not infer, estimate, calculate, or invent any missing information.
* Do not introduce external knowledge or unsupported conclusions.
* If the data is insufficient, clearly state that there is not enough information to provide a complete analysis.
* Do not use the em dash character (—).
* Return only the final Thai summary without headings, bullet points, explanations, or additional commentary.
"""


def generate_summary(source_rows: list[dict]) -> str:
    """source_rows = gold rows ที่ใช้ ground คำตอบ (ส่งกลับไว้คู่กับ narrative เพื่อ provenance)."""
    llm = ChatOpenRouter(model=MODEL, temperature=0.3, max_tokens=512)
    resp = llm.invoke([SystemMessage(SYSTEM_PROMPT), HumanMessage(f"ข้อมูล: {source_rows}")])
    return resp.content.strip().replace("—", ",")  # กันโมเดลไม่ทำตาม prompt


if __name__ == "__main__":
    print("ok")
