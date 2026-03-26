"""Create Redash saved queries for DST/DSTI validation.

Usage:
    python create_redash_queries.py <REDASH_API_KEY> [DATA_SOURCE_ID]

Get your API key from: dev-redash.gdp.data.grubhub.com > Profile > API Key
Data source ID defaults to 2 (Presto prod). Check /api/data_sources to see available sources.
"""

import json
import sys

import requests

REDASH_URL = "https://dev-redash.gdp.data.grubhub.com"

QUERIES = [
    {
        "name": "OED-6225: Regression Check - Old Data Types Pre vs Post",
        "query": """SELECT data_type, period,
    ROUND(AVG(daily_rows)) as avg_daily_rows,
    ROUND(AVG(daily_sessions)) as avg_daily_sessions
FROM (
    SELECT event_date, data_type,
        CASE WHEN event_date < DATE '2026-03-18' THEN 'pre' ELSE 'post' END as period,
        COUNT(*) as daily_rows,
        COUNT(DISTINCT session_id) as daily_sessions
    FROM integrated_events.diner_session_topics
    WHERE event_date BETWEEN DATE '2026-03-10' AND DATE '2026-03-22'
      AND data_type IN ('restaurant', 'cuisine_ribbon', 'order_availability', 'stacked_order_availability', 'menu_item')
    GROUP BY 1, 2
)
GROUP BY 1, 2
ORDER BY 1, 2""",
    },
    {
        "name": "OED-6225: New Data Types Daily Trend (DST)",
        "query": """SELECT event_date, data_type,
    COUNT(*) as rows,
    COUNT(DISTINCT session_id) as sessions
FROM integrated_events.diner_session_topics
WHERE event_date BETWEEN DATE '2026-03-15' AND DATE '2026-03-23'
  AND data_type IN ('category', 'navigation')
GROUP BY 1, 2
ORDER BY 2, 1""",
    },
    {
        "name": "OED-6225: New Data Types DSTI (CTR + Completeness)",
        "query": """SELECT event_date, data_type,
    COUNT(*) as impressions,
    COUNT(DISTINCT session_id) as sessions,
    SUM(CASE WHEN impression_clicked = true THEN 1 ELSE 0 END) as clicks,
    ROUND(100.0 * SUM(CASE WHEN impression_clicked = true THEN 1 ELSE 0 END) / COUNT(*), 2) as ctr_pct,
    ROUND(100.0 * COUNT(topic_id) / COUNT(*), 1) as topic_id_pct,
    ROUND(100.0 * COUNT(restaurant_id) / COUNT(*), 1) as restaurant_id_pct,
    ROUND(100.0 * COUNT(restaurant_name) / COUNT(*), 1) as restaurant_name_pct,
    ROUND(100.0 * COUNT(impression_id) / COUNT(*), 1) as impression_id_pct
FROM integrated_events.diner_session_topics_impressions
WHERE event_date BETWEEN DATE '2026-03-18' AND DATE '2026-03-23'
  AND data_type IN ('category', 'navigation')
GROUP BY 1, 2
ORDER BY 2, 1""",
    },
    {
        "name": "OED-6225: DST vs DSTI Session Match",
        "query": """SELECT dst.data_type,
    dst.dst_rows, dst.dst_sessions,
    dsti.dsti_rows, dsti.dsti_sessions, dsti.dsti_clicks,
    ROUND(100.0 * dsti.dsti_clicks / NULLIF(dsti.dsti_rows, 0), 2) as ctr_pct,
    ROUND(100.0 * dsti.dsti_sessions / NULLIF(dst.dst_sessions, 0), 1) as session_match_pct
FROM (
    SELECT event_date, data_type, COUNT(*) as dst_rows, COUNT(DISTINCT session_id) as dst_sessions
    FROM integrated_events.diner_session_topics
    WHERE event_date = DATE '2026-03-21'
    GROUP BY 1, 2
) dst
LEFT JOIN (
    SELECT event_date, data_type, COUNT(*) as dsti_rows, COUNT(DISTINCT session_id) as dsti_sessions,
        SUM(CASE WHEN impression_clicked = true THEN 1 ELSE 0 END) as dsti_clicks
    FROM integrated_events.diner_session_topics_impressions
    WHERE event_date = DATE '2026-03-21'
    GROUP BY 1, 2
) dsti ON dst.event_date = dsti.event_date AND dst.data_type = dsti.data_type
ORDER BY dst.dst_rows DESC""",
    },
    {
        "name": "OED-6225: Non-Null % Key DSTI Columns",
        "query": """SELECT
    COUNT(*) as total_rows,
    ROUND(100.0 * COUNT(browser_id) / COUNT(*), 1) as browser_id_pct,
    ROUND(100.0 * COUNT(session_id) / COUNT(*), 1) as session_id_pct,
    ROUND(100.0 * COUNT(impression_id) / COUNT(*), 1) as impression_id_pct,
    ROUND(100.0 * COUNT(data_type) / COUNT(*), 1) as data_type_pct,
    ROUND(100.0 * COUNT(topic_id) / COUNT(*), 1) as topic_id_pct,
    ROUND(100.0 * COUNT(restaurant_id) / COUNT(*), 1) as restaurant_id_pct,
    ROUND(100.0 * COUNT(restaurant_name) / COUNT(*), 1) as restaurant_name_pct,
    ROUND(100.0 * COUNT(source_service) / COUNT(*), 1) as source_service_pct
FROM integrated_events.diner_session_topics_impressions
WHERE event_date = DATE '2026-03-21'""",
    },
    {
        "name": "OED-6225: Source (diner_all_events) vs Target (DST)",
        "query": """SELECT 'source' as layer, module_name, COUNT(*) as rows, COUNT(DISTINCT session_id) as sessions
FROM integrated_events.diner_all_events
WHERE event_date = DATE '2026-03-21'
  AND event_name = 'moduleVisible'
  AND module_name IN ('all restaurants', 'cuisine ribbon', 'gotos topic', 'campus dining', 'new verticals ribbon - nvlp')
GROUP BY 1, 2
UNION ALL
SELECT 'target_dst' as layer, data_type as module_name, COUNT(*) as rows, COUNT(DISTINCT session_id) as sessions
FROM integrated_events.diner_session_topics
WHERE event_date = DATE '2026-03-21'
GROUP BY 1, 2
ORDER BY layer, module_name""",
    },
    {
        "name": "OED-6225: New Types by App/Platform (DSTI)",
        "query": """SELECT data_type, app,
    COUNT(*) as rows,
    COUNT(DISTINCT session_id) as sessions,
    SUM(CASE WHEN impression_clicked = true THEN 1 ELSE 0 END) as clicks,
    ROUND(100.0 * SUM(CASE WHEN impression_clicked = true THEN 1 ELSE 0 END) / COUNT(*), 2) as ctr_pct
FROM integrated_events.diner_session_topics_impressions
WHERE event_date = DATE '2026-03-21'
  AND data_type IN ('category', 'navigation')
GROUP BY 1, 2
ORDER BY 1, 3 DESC""",
    },
]


def create_queries(api_key, data_source_id=2):
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Key {api_key}",
        "Content-Type": "application/json",
    })

    created = []
    for q in QUERIES:
        payload = {
            "name": q["name"],
            "query": q["query"],
            "data_source_id": data_source_id,
        }
        resp = session.post(f"{REDASH_URL}/api/queries", json=payload)
        if resp.status_code == 200:
            data = resp.json()
            query_id = data.get("id")
            url = f"{REDASH_URL}/queries/{query_id}"
            print(f"  CREATED: {q['name']}")
            print(f"           {url}")
            created.append(url)
        else:
            print(f"  FAILED:  {q['name']} -> {resp.status_code}: {resp.text[:200]}")

    print(f"\n{len(created)}/{len(QUERIES)} queries created.")
    if created:
        print("\nRedash links:")
        for url in created:
            print(f"  {url}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    api_key = sys.argv[1]
    ds_id = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    create_queries(api_key, ds_id)