import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import logging
import pandas as pd
import snowflake.connector
from openai import OpenAI
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv('config/.env')
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger(__name__)


def get_conn():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )


def read_sf(query):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    cols = [d[0].upper() for d in cursor.description]
    rows = cursor.fetchall()
    conn.close()
    return pd.DataFrame(rows, columns=cols)


def fetch_gold_data():
    metrics = read_sf("""
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
            DOWNLOAD_GROWTH_PCT
        FROM DEV_ECOSYSTEM_DB.GOLD.FACT_FRAMEWORK_WEEKLY_METRICS
        ORDER BY HEALTH_INDEX DESC
    """)

    summary = read_sf("""
        SELECT
            TOP_FRAMEWORK,
            FASTEST_GROWING,
            MOST_DISCUSSED_HN,
            HIGHEST_DOWNLOADS
        FROM DEV_ECOSYSTEM_DB.GOLD.ECOSYSTEM_WEEKLY_SUMMARY
        LIMIT 1
    """)

    return metrics, summary


def build_prompt(metrics: pd.DataFrame, summary: pd.DataFrame) -> str:
    metrics_text = metrics.to_string(index=False)

    top = summary.iloc[0] if len(summary) > 0 else {}

    prompt = f"""
You are a senior technology analyst at a leading developer tools company.
You have access to real-time data from GitHub, HackerNews, and PyPI 
tracking 15 major Python and JavaScript frameworks as of {datetime.now().strftime('%B %Y')}.

Here is the current ecosystem data:

{metrics_text}

Key highlights from this week:
- Top framework by overall health: {top.get('TOP_FRAMEWORK', 'N/A')}
- Fastest growing by downloads: {top.get('FASTEST_GROWING', 'N/A')}
- Most discussed on HackerNews: {top.get('MOST_DISCUSSED_HN', 'N/A')}
- Highest weekly downloads: {top.get('HIGHEST_DOWNLOADS', 'N/A')}

Score guide:
- HEALTH_INDEX: overall ecosystem health (0-1, higher is better)
- POPULARITY_SCORE: adoption + activity signal (0-1)
- SENTIMENT_SCORE: developer satisfaction signal (0-1)
- DOWNLOAD_GROWTH_PCT: week over week PyPI download change

Generate a professional quarterly technology trend report with these sections:

1. EXECUTIVE SUMMARY (3-4 sentences, what does this data tell us?)

2. TOP GROWING FRAMEWORKS (top 3 by health + growth, explain why each is rising)

3. AT-RISK TECHNOLOGIES (frameworks showing decline signals, what teams should watch)

4. HIDDEN GEMS (frameworks with low buzz but high actual usage — underrated picks)

5. WHAT YOUR TEAM SHOULD LEARN NEXT QUARTER (specific recommendation with reasoning)

6. ECOSYSTEM HEALTH SUMMARY (1 paragraph overview of the overall developer ecosystem)

Write in plain English. Be specific. Use the actual numbers from the data.
Avoid generic statements. This report will be read by a CTO making technology decisions.
"""
    return prompt

def generate_trend_report() -> str:
    log.info("Fetching Gold layer data...")
    metrics, summary = fetch_gold_data()
    log.info(f"Loaded {len(metrics)} frameworks from Gold layer")

    prompt = build_prompt(metrics, summary)

    from groq import Groq
    client = Groq(api_key=os.getenv('GROQ_API_KEY'))

    log.info("Calling Groq Llama3 API...")
    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {
                "role": "system",
                "content": "You are a senior technology analyst. Write clear, data-driven reports."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        max_tokens=2000,
        temperature=0.3
    )

    report = response.choices[0].message.content
    log.info("Report generated successfully")
    return report

def save_report(report: str):
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    filename = f"ai_layer/trend_report_{timestamp}.md"
    os.makedirs("ai_layer", exist_ok=True)
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"# Technology Trend Report\n")
        f.write(f"**Generated:** {datetime.now().strftime('%B %d, %Y')}\n\n")
        f.write(report)
    log.info(f"Report saved to {filename}")
    return filename


def run_ecosystem_agent():
    log.info("=" * 55)
    log.info("GPT-4o Ecosystem Agent Started")
    log.info("=" * 55)

    if not os.getenv('GROQ_API_KEY'):
        log.error("GROQ_API_KEY not set in config/.env")
        return

    report = generate_trend_report()

    print("\n" + "=" * 55)
    print("TECHNOLOGY TREND REPORT")
    print("=" * 55)
    print(report)
    print("=" * 55)

    filename = save_report(report)
    log.info(f"Complete. Report at: {filename}")


if __name__ == '__main__':
    run_ecosystem_agent()