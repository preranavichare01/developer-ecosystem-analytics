import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from dotenv import load_dotenv

load_dotenv('config/.env')

st.set_page_config(
    page_title="Developer Ecosystem Analytics",
    page_icon="🚀",
    layout="wide"
)

def get_conn():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )

@st.cache_data(ttl=3600)
def load_metrics():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            FRAMEWORK_NAME,
            ROUND(POPULARITY_SCORE, 3)  AS POPULARITY_SCORE,
            ROUND(SENTIMENT_SCORE, 3)   AS SENTIMENT_SCORE,
            ROUND(HEALTH_INDEX, 3)      AS HEALTH_INDEX,
            STARS_TOTAL,
            WEEKLY_DOWNLOADS,
            HN_STORY_COUNT,
            HN_AVG_SCORE,
            COMMITS_30D,
            ISSUE_RESOLUTION_RATE,
            PR_MERGE_RATE,
            DOWNLOAD_GROWTH_PCT,
            PRIMARY_LANGUAGE
        FROM DEV_ECOSYSTEM_DB.GOLD.FACT_FRAMEWORK_WEEKLY_METRICS
        ORDER BY HEALTH_INDEX DESC
    """)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame(rows, columns=cols)

@st.cache_data(ttl=3600)
def load_summary():
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TOP_FRAMEWORK, FASTEST_GROWING,
               MOST_DISCUSSED_HN, HIGHEST_DOWNLOADS
        FROM DEV_ECOSYSTEM_DB.GOLD.ECOSYSTEM_WEEKLY_SUMMARY
        LIMIT 1
    """)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return pd.DataFrame(rows, columns=cols)

def load_ai_report():
    report_dir = "ai_layer"
    if not os.path.exists(report_dir):
        return None
    files = [f for f in os.listdir(report_dir) if f.endswith('.md')]
    if not files:
        return None
    latest = sorted(files)[-1]
    with open(os.path.join(report_dir, latest), 'r', encoding='utf-8') as f:
        return f.read()


# ── Header ─────────────────────────────────────────────
st.title("🚀 Developer Ecosystem Analytics")
st.markdown("*Live intelligence from GitHub, HackerNews, and PyPI*")
st.divider()

# Load data
with st.spinner("Loading data from Snowflake..."):
    df = load_metrics()
    summary = load_summary()

# ── KPI Cards ──────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

if len(summary) > 0:
    row = summary.iloc[0]
    col1.metric("🏆 Top Framework", row['TOP_FRAMEWORK'])
    col2.metric("📈 Fastest Growing", row['FASTEST_GROWING'])
    col3.metric("💬 Most Discussed HN", row['MOST_DISCUSSED_HN'])
    col4.metric("⬇️ Highest Downloads", row['HIGHEST_DOWNLOADS'])

st.divider()

# ── Sidebar Filters ────────────────────────────────────
st.sidebar.header("🔍 Filters")

languages = ["All"] + sorted(df['PRIMARY_LANGUAGE'].dropna().unique().tolist())
selected_language = st.sidebar.selectbox("Primary Language", languages)

frameworks = df['FRAMEWORK_NAME'].tolist()
selected_frameworks = st.sidebar.multiselect(
    "Select Frameworks",
    frameworks,
    default=frameworks[:8]
)

if selected_language != "All":
    df = df[df['PRIMARY_LANGUAGE'] == selected_language]

if selected_frameworks:
    df_filtered = df[df['FRAMEWORK_NAME'].isin(selected_frameworks)]
else:
    df_filtered = df

# ── Chart 1 — Health Index Bar Chart ──────────────────
st.subheader("📊 Framework Health Index")
fig1 = px.bar(
    df_filtered.sort_values('HEALTH_INDEX', ascending=True),
    x='HEALTH_INDEX',
    y='FRAMEWORK_NAME',
    orientation='h',
    color='HEALTH_INDEX',
    color_continuous_scale='RdYlGn',
    title='Overall Ecosystem Health Score (0-1)',
    labels={'HEALTH_INDEX': 'Health Index', 'FRAMEWORK_NAME': 'Framework'}
)
fig1.update_layout(height=500, showlegend=False)
st.plotly_chart(fig1, use_container_width=True)

# ── Chart 2 — Scatter: Popularity vs Sentiment ─────────
st.subheader("🎯 Popularity vs Sentiment")
fig2 = px.scatter(
    df_filtered,
    x='POPULARITY_SCORE',
    y='SENTIMENT_SCORE',
    size='WEEKLY_DOWNLOADS',
    color='HEALTH_INDEX',
    hover_name='FRAMEWORK_NAME',
    color_continuous_scale='RdYlGn',
    title='Popularity vs Developer Sentiment (bubble size = weekly downloads)',
    labels={
        'POPULARITY_SCORE': 'Popularity Score',
        'SENTIMENT_SCORE': 'Sentiment Score'
    }
)
fig2.update_layout(height=500)
st.plotly_chart(fig2, use_container_width=True)

# ── Chart 3 — PyPI Downloads ───────────────────────────
st.subheader("⬇️ Weekly PyPI Downloads")
fig3 = px.bar(
    df_filtered.sort_values('WEEKLY_DOWNLOADS', ascending=False),
    x='FRAMEWORK_NAME',
    y='WEEKLY_DOWNLOADS',
    color='DOWNLOAD_GROWTH_PCT',
    color_continuous_scale='RdYlGn',
    title='Weekly PyPI Downloads (color = growth %)',
    labels={
        'WEEKLY_DOWNLOADS': 'Weekly Downloads',
        'FRAMEWORK_NAME': 'Framework'
    }
)
fig3.update_layout(height=400)
st.plotly_chart(fig3, use_container_width=True)

# ── Chart 4 — GitHub Activity ──────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("⭐ GitHub Stars")
    fig4 = px.bar(
        df_filtered.sort_values('STARS_TOTAL', ascending=False),
        x='FRAMEWORK_NAME',
        y='STARS_TOTAL',
        color='STARS_TOTAL',
        color_continuous_scale='Blues',
        title='Total GitHub Stars'
    )
    fig4.update_layout(height=350, showlegend=False)
    st.plotly_chart(fig4, use_container_width=True)

with col_right:
    st.subheader("🔀 PR Merge Rate")
    fig5 = px.bar(
        df_filtered.sort_values('PR_MERGE_RATE', ascending=False),
        x='FRAMEWORK_NAME',
        y='PR_MERGE_RATE',
        color='PR_MERGE_RATE',
        color_continuous_scale='Greens',
        title='PR Merge Rate (higher = more active maintenance)'
    )
    fig5.update_layout(height=350, showlegend=False)
    st.plotly_chart(fig5, use_container_width=True)

# ── Data Table ─────────────────────────────────────────
st.subheader("📋 Full Framework Metrics")
st.dataframe(
    df_filtered[[
        'FRAMEWORK_NAME', 'HEALTH_INDEX', 'POPULARITY_SCORE',
        'SENTIMENT_SCORE', 'STARS_TOTAL', 'WEEKLY_DOWNLOADS',
        'HN_STORY_COUNT', 'COMMITS_30D', 'PR_MERGE_RATE'
    ]].sort_values('HEALTH_INDEX', ascending=False),
    use_container_width=True,
    hide_index=True
)

# ── AI Report Panel ────────────────────────────────────
st.divider()
st.subheader("🤖 AI-Generated Trend Report")
st.caption("Generated by Llama 3.3-70B reading live Gold layer data")

report = load_ai_report()
if report:
    st.markdown(report)
else:
    st.info("Run `py -3.11 ai_layer/ecosystem_agent.py` to generate the AI report")

# ── Footer ─────────────────────────────────────────────
st.divider()
st.caption("Data refreshed daily from GitHub API, HackerNews, and PyPI Stats | Built by Prerana Vichare")