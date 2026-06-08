"""
Tower Health — Cortex Analyst Chat App
EC2 deployment | v7.0 — JWT key-pair auth
Run: streamlit run tower_health_streamlit.py --server.port 8501
Requires: ~/.streamlit/secrets.toml + ~/.streamlit/rsa_key.p8
"""

import streamlit as st
import json
import re
import hashlib
import base64
import datetime
import requests
import jwt
import snowflake.connector
from cryptography.hazmat.primitives import serialization

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Tower Health — NOC Intelligence",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS ────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
.stApp { background: #0D1821; color: #E8EDF2; }
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 1.5rem 2rem 2rem 2rem; max-width: 1100px; }

.th-header {
    display: flex; align-items: center; gap: 14px;
    padding: 18px 24px; background: #1B2A3A;
    border-radius: 10px; border-left: 4px solid #C9A86C; margin-bottom: 1.5rem;
}
.th-header-icon { font-size: 2rem; line-height: 1; }
.th-header-title { font-size: 1.35rem; font-weight: 700; color: #C9A86C; letter-spacing: 0.02em; margin: 0; }
.th-header-sub { font-size: 0.78rem; color: #8FA3B8; margin: 2px 0 0 0; font-family: 'IBM Plex Mono', monospace; }

.noc-card { background: #1B2A3A; border-radius: 10px; padding: 18px 22px; margin-bottom: 1rem; border: 1px solid #243548; }
.noc-headline { font-size: 1.05rem; font-weight: 600; margin-bottom: 8px; line-height: 1.4; }
.noc-summary { font-size: 0.83rem; color: #8FA3B8; font-family: 'IBM Plex Mono', monospace; line-height: 1.6; }
.noc-meta { font-size: 0.72rem; color: #4A6278; margin-top: 10px; font-family: 'IBM Plex Mono', monospace; }
.noc-kpis { display: flex; gap: 16px; margin-top: 14px; flex-wrap: wrap; }
.noc-kpi { background: #0D1821; border-radius: 7px; padding: 10px 16px; min-width: 120px; border: 1px solid #243548; }
.noc-kpi-value { font-size: 1.4rem; font-weight: 700; color: #C9A86C; font-family: 'IBM Plex Mono', monospace; }
.noc-kpi-label { font-size: 0.7rem; color: #8FA3B8; margin-top: 2px; text-transform: uppercase; letter-spacing: 0.05em; }

.chat-label { font-size: 0.78rem; color: #8FA3B8; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 10px; font-family: 'IBM Plex Mono', monospace; }

.msg-user { display: flex; justify-content: flex-end; margin: 10px 0; }
.msg-user-bubble { background: #1E3A52; border: 1px solid #2A4F6E; border-radius: 14px 14px 4px 14px; padding: 11px 16px; max-width: 75%; font-size: 0.9rem; line-height: 1.5; color: #E8EDF2; }
.msg-bot { display: flex; justify-content: flex-start; margin: 10px 0; gap: 10px; align-items: flex-start; }
.msg-bot-avatar { width: 30px; height: 30px; background: #C9A86C; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 0.85rem; flex-shrink: 0; margin-top: 2px; }
.msg-bot-bubble { background: #1B2A3A; border: 1px solid #243548; border-radius: 4px 14px 14px 14px; padding: 11px 16px; max-width: 78%; font-size: 0.9rem; line-height: 1.6; color: #E8EDF2; }
.msg-bot-bubble table { width: 100%; border-collapse: collapse; font-size: 0.82rem; margin-top: 8px; font-family: 'IBM Plex Mono', monospace; }
.msg-bot-bubble th { background: #0D1821; color: #C9A86C; padding: 6px 10px; text-align: left; font-weight: 500; border-bottom: 1px solid #2A3F55; }
.msg-bot-bubble td { padding: 5px 10px; border-bottom: 1px solid #1E3040; color: #CBD8E4; }
.msg-bot-bubble tr:last-child td { border-bottom: none; }
.suggestions-box { margin-top: 8px; }
.suggestion-pill { display: inline-block; background: #0D1821; border: 1px solid #2A4060; border-radius: 20px; padding: 4px 12px; font-size: 0.78rem; color: #C9A86C; margin: 3px 4px 3px 0; font-family: 'IBM Plex Mono', monospace; cursor: pointer; }

.sql-block { background: #0D1821; border: 1px solid #243548; border-radius: 6px; padding: 10px 14px; font-family: 'IBM Plex Mono', monospace; font-size: 0.75rem; color: #8FA3B8; margin-top: 8px; white-space: pre-wrap; word-break: break-word; }

.stTextInput > div > div > input { background: #1B2A3A !important; border: 1px solid #2A4060 !important; border-radius: 8px !important; color: #E8EDF2 !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 0.88rem !important; }
.stTextInput > div > div > input:focus { border-color: #C9A86C !important; box-shadow: 0 0 0 2px rgba(201,168,108,0.15) !important; }
.stButton > button { background: #C9A86C !important; color: #0D1821 !important; border: none !important; border-radius: 8px !important; font-weight: 600 !important; font-family: 'IBM Plex Sans', sans-serif !important; padding: 0.5rem 1.4rem !important; }
.stButton > button:hover { background: #D4B87C !important; }
.err-box { background: #2A1B1B; border: 1px solid #5C2A2A; border-radius: 8px; padding: 10px 14px; font-size: 0.82rem; color: #E07070; margin-top: 6px; }
</style>
""", unsafe_allow_html=True)


# ── Constants ──────────────────────────────────────────────────────────────────
SNOWFLAKE_HOST  = "YOUR_SNOWFLAKE_ACCOUNT.snowflakecomputing.com"
SNOWFLAKE_URL   = f"https://{SNOWFLAKE_HOST}"
CORTEX_ENDPOINT = "/api/v2/cortex/analyst/message"
SEMANTIC_MODEL  = "@YOUR_SNOWFLAKE_DATABASE.PUBLIC.SEMANTIC_STAGE/tower_health_semantic_model.yaml"
PRIVATE_KEY_PATH = "/home/ubuntu/.streamlit/rsa_key.p8"


# ── Helper: is Arabic ──────────────────────────────────────────────────────────
def is_arabic(text):
    return bool(re.search(r'[\u0600-\u06FF]', text))


# ── JWT token generator ────────────────────────────────────────────────────────
def make_jwt():
    with open(PRIVATE_KEY_PATH, "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)
    pub_raw = private_key.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    fp = "SHA256:" + base64.b64encode(hashlib.sha256(pub_raw).digest()).decode()
    now = datetime.datetime.now(datetime.timezone.utc)
    token = jwt.encode({
        "iss": f"YOUR_SNOWFLAKE_ACCOUNT.YOUR_SNOWFLAKE_USER.{fp}",
        "sub": "YOUR_SNOWFLAKE_ACCOUNT.YOUR_SNOWFLAKE_USER",
        "iat": now,
        "exp": now + datetime.timedelta(minutes=55),
    }, private_key, algorithm="RS256")
    return token


# ── Snowflake connector (password auth for SQL queries) ───────────────────────
@st.cache_resource
def get_connection():
    cfg = st.secrets["snowflake"]
    return snowflake.connector.connect(
        user      = cfg["user"],
        password  = cfg["password"],
        account   = cfg["account"],
        warehouse = cfg["warehouse"],
        database  = cfg["database"],
        schema    = cfg["schema"],
    )


def run_query(sql):
    conn = get_connection()
    cur  = conn.cursor(snowflake.connector.DictCursor)
    cur.execute(sql)
    return cur.fetchall()


# ── Cortex Analyst call (JWT auth) ─────────────────────────────────────────────
def call_cortex_analyst(question):
    try:
        token = make_jwt()
    except Exception as e:
        return {"sql": None, "results": None, "error": f"JWT error: {e}",
                "answer_text": None, "suggestions": None}

    payload = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": question}]}],
        "semantic_model_file": SEMANTIC_MODEL,
    }
    headers = {
        "Content-Type": "application/json",
        "Accept":        "application/json",
        "Authorization": f"Bearer {token}",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
    }
    try:
        resp = requests.post(
            SNOWFLAKE_URL + CORTEX_ENDPOINT,
            json=payload, headers=headers, timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.Timeout:
        return {"sql": None, "results": None, "error": "Request timed out — try again.",
                "answer_text": None, "suggestions": None}
    except requests.exceptions.HTTPError:
        return {"sql": None, "results": None,
                "error": f"HTTP {resp.status_code}: {resp.text[:400]}",
                "answer_text": None, "suggestions": None}
    except Exception as e:
        return {"sql": None, "results": None, "error": str(e),
                "answer_text": None, "suggestions": None}

    blocks      = data.get("message", {}).get("content", [])
    sql         = None
    answer_text = None
    results     = None
    error       = None
    suggestions = None
    texts       = []

    for block in blocks:
        t = block.get("type")
        if t == "sql":
            sql = block.get("statement", "")
        elif t == "text":
            texts.append(block.get("text", ""))
        elif t == "error":
            error = block.get("message", "Unknown error.")
        elif t == "suggestions":
            suggestions = block.get("suggestions", [])

    answer_text = "\n\n".join(texts) if texts else None

    if sql and not error:
        try:
            results = run_query(sql)
        except Exception as e:
            error = f"SQL execution error: {e}"

    return {"sql": sql, "results": results, "error": error,
            "answer_text": answer_text, "suggestions": suggestions}


# ── NOC summary ────────────────────────────────────────────────────────────────
def load_noc_summary():
    try:
        rows = run_query(
            "SELECT * FROM YOUR_SNOWFLAKE_DATABASE.PUBLIC.V_NOC_DAILY_SUMMARY LIMIT 1"
        )
        return rows[0] if rows else None
    except Exception:
        return None


def render_noc_card(noc):
    headline  = noc.get("HEADLINE", "ℹ️ Status unavailable.")
    summary   = noc.get("SUMMARY_TEXT", "")
    gen_at    = noc.get("GENERATED_AT", "")
    critical  = noc.get("CRITICAL_ALARMS", "—")
    total     = noc.get("TOTAL_ALARMS", "—")
    risk_pct  = noc.get("AVG_FAILURE_RISK_PCT", "—")
    high_risk = noc.get("HIGH_RISK_SITES", "—")
    worst     = noc.get("WORST_SITE", "—")
    gen_str   = gen_at.strftime("%Y-%m-%d %H:%M UTC") if hasattr(gen_at, "strftime") else str(gen_at)[:19]

    st.markdown(f"""
    <div class="noc-card">
      <div class="noc-headline">{headline}</div>
      <div class="noc-summary">{summary}</div>
      <div class="noc-kpis">
        <div class="noc-kpi"><div class="noc-kpi-value">{critical}</div><div class="noc-kpi-label">Critical Alarms</div></div>
        <div class="noc-kpi"><div class="noc-kpi-value">{total}</div><div class="noc-kpi-label">Total Alarms</div></div>
        <div class="noc-kpi"><div class="noc-kpi-value">{risk_pct}%</div><div class="noc-kpi-label">Avg Failure Risk</div></div>
        <div class="noc-kpi"><div class="noc-kpi-value">{high_risk}</div><div class="noc-kpi-label">High-Risk Towers</div></div>
        <div class="noc-kpi"><div class="noc-kpi-value" style="font-size:1rem">{worst}</div><div class="noc-kpi-label">Worst Site</div></div>
      </div>
      <div class="noc-meta">🕐 Generated {gen_str} · Source: FACT_ALARMS + FACT_ML_PREDICTIONS</div>
    </div>
    """, unsafe_allow_html=True)


# ── Chat rendering ─────────────────────────────────────────────────────────────
def render_results_table(results):
    if not results:
        return "<em style='color:#8FA3B8'>No rows returned.</em>"
    headers = list(results[0].keys())
    th   = "".join(f"<th>{h}</th>" for h in headers)
    rows = "".join(
        "<tr>" + "".join(f"<td>{r.get(h, '')}</td>" for h in headers) + "</tr>"
        for r in results[:50]
    )
    note = (f"<div style='font-size:0.72rem;color:#4A6278;margin-top:6px'>"
            f"Showing 50 of {len(results)} rows</div>" if len(results) > 50 else "")
    return f"<table><thead><tr>{th}</tr></thead><tbody>{rows}</tbody></table>{note}"


def render_message(role, content):
    if role == "user":
        st.markdown(
            f'<div class="msg-user"><div class="msg-user-bubble">{content["text"]}</div></div>',
            unsafe_allow_html=True)
    else:
        error       = content.get("error")
        answer_text = content.get("answer_text")
        sql         = content.get("sql")
        results     = content.get("results")
        suggestions = content.get("suggestions")

        if error:
            inner = f'<div class="err-box">⚠️ {error}</div>'
        else:
            inner = ""
            if answer_text:
                inner += f"<div>{answer_text}</div>"
            if results is not None:
                inner += render_results_table(results)
            if suggestions:
                pills = "".join(
                    f'<span class="suggestion-pill">💡 {s}</span>' for s in suggestions
                )
                inner += f'<div class="suggestions-box">{pills}</div>'
            if not inner:
                inner = "<em style='color:#8FA3B8'>No response content.</em>"

        sql_block = ""
        if sql:
            sql_esc = sql.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            sql_block = (
                f'<details style="margin-top:8px">'
                f'<summary style="font-size:0.75rem;color:#4A6278;cursor:pointer;'
                f'font-family:\'IBM Plex Mono\',monospace">▶ View generated SQL</summary>'
                f'<div class="sql-block">{sql_esc}</div></details>'
            )

        st.markdown(
            f'<div class="msg-bot">'
            f'<div class="msg-bot-avatar">📡</div>'
            f'<div class="msg-bot-bubble">{inner}{sql_block}</div>'
            f'</div>',
            unsafe_allow_html=True)


# ── Suggested questions ────────────────────────────────────────────────────────
SUGGESTED = [
    ("How many total sites exist across the network?", "كم عدد المواقع الإجمالي في الشبكة؟"),
    ("Show average SINR by site",                      "أظهر متوسط SINR لكل موقع"),
    ("Top 3 cells by call drop rate",                  "أعلى 3 خلايا في معدل انقطاع المكالمات"),
    ("Which towers are high-risk this week?",          "ما هي الأبراج عالية الخطورة هذا الأسبوع؟"),
    ("Average failure probability by region",          "متوسط احتمالية الفشل حسب المنطقة"),
]

# ── Session state ──────────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []
if "pill_clicked" not in st.session_state:
    st.session_state.pill_clicked = None
if "noc_data" not in st.session_state:
    st.session_state.noc_data = load_noc_summary()

# ── RENDER ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="th-header">
  <div class="th-header-icon">📡</div>
  <div>
    <div class="th-header-title">Tower Health — NOC Intelligence</div>
    <div class="th-header-sub">Powered by Snowflake Cortex Analyst · Ask in English or Arabic</div>
  </div>
</div>
""", unsafe_allow_html=True)

with st.expander("📊 Network Status Summary", expanded=True):
    if st.session_state.noc_data:
        render_noc_card(st.session_state.noc_data)
    else:
        st.markdown('<div class="err-box">Could not load V_NOC_DAILY_SUMMARY.</div>',
                    unsafe_allow_html=True)
    if st.button("🔄 Refresh", key="refresh_noc"):
        st.session_state.noc_data = load_noc_summary()
        st.rerun()

st.markdown("---")
st.markdown('<div class="chat-label">💬 Ask the Network</div>', unsafe_allow_html=True)

for msg in st.session_state.messages:
    render_message(msg["role"], msg["content"])

if len(st.session_state.messages) == 0:
    st.caption("Click a suggestion or type below — يتم الكشف عن اللغة تلقائيًا")

cols = st.columns(len(SUGGESTED))
for i, (en, ar) in enumerate(SUGGESTED):
    with cols[i]:
        if st.button(f"💬 {i+1}", key=f"pill_{i}", help=f"{en}\n{ar}"):
            st.session_state.pill_clicked = en

col_input, col_send = st.columns([8, 1])
with col_input:
    user_input = st.text_input(
        label="question", label_visibility="collapsed",
        placeholder="e.g. Show sites by region… أو اكتب سؤالك بالعربية",
        key="chat_input",
    )
with col_send:
    send_clicked = st.button("Send", key="send_btn")

question = None
if send_clicked and user_input.strip():
    question = user_input.strip()
elif st.session_state.pill_clicked:
    question = st.session_state.pill_clicked
    st.session_state.pill_clicked = None

if question:
    arabic = is_arabic(question)
    st.session_state.messages.append({"role": "user", "content": {"text": question}})
    with st.spinner("جارٍ التحليل…" if arabic else "Analyzing…"):
        result = call_cortex_analyst(question)
    st.session_state.messages.append({"role": "assistant", "content": result})
    st.rerun()

st.markdown("""
<div style="margin-top:2rem;padding-top:1rem;border-top:1px solid #1B2A3A;
            font-size:0.72rem;color:#4A6278;font-family:'IBM Plex Mono',monospace;
            display:flex;justify-content:space-between;">
  <span>Tower Health · Graduation Project · ITI Data Engineering</span>
  <span>Semantic model: tower_health_semantic_model.yaml v6</span>
</div>
""", unsafe_allow_html=True)
