"""
services/bigquery.py — last week's media figures from BigQuery.

Credentials come from GOOGLE_APPLICATION_CREDENTIALS (the service-account
JSON mounted into the container by docker-compose) — the client library
picks that env var up automatically, so there's nothing to pass in here.
"""

from google.cloud import bigquery

from config import PROJECT_ID, GBQ_TABLE


def get_gbq_metrics(gbq_name: str) -> dict:
    """Last week's spend / revenue / ROAS for one show, with a source breakdown.

    Uses a parameterised query so the show name can't break the SQL.
    """
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT MAD_Media_Source AS source,
               SUM(MAD_All_Spend)   AS spend,
               SUM(MAD_All_Revenue) AS revenue
        FROM `{GBQ_TABLE}`
        WHERE MAD_Show_Name = @show
          AND MAD_Media_Source IN ('Meta', 'Google Ads', 'Programmatic Spend',
                                    'Programmatic', 'TikTok', 'Pinterest')
          AND Date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 1 WEEK)
          AND Date <= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 1 DAY)
        GROUP BY MAD_Media_Source
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("show", "STRING", gbq_name)
    ])
    rows = list(client.query(query, job_config=job_config).result())

    breakdown = [{"source": r.source, "spend": r.spend or 0.0, "revenue": r.revenue or 0.0}
                 for r in rows]
    spend = sum(b["spend"] for b in breakdown)
    revenue = sum(b["revenue"] for b in breakdown)
    roas = (revenue / spend) if spend > 0 else 0.0
    return {"spend": spend, "revenue": revenue, "roas": roas, "breakdown": breakdown}
