from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(
    title="Austrian Labor Market Resilience API",
    description="Regional employment data, recovery scores, and Prophet forecasts across Austria's nine Bundesländer.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


@app.get("/")
def root():
    return {
        "project": "Austrian Labor Market Resilience Analysis",
        "version": "1.0.0",
        "endpoints": ["/regions", "/regions/{region}/forecast", "/regions/{region}/recovery"],
        "docs": "/docs"
    }


@app.get("/regions")
def list_regions():
    """List all nine Austrian Bundesländer with their latest recovery scores."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT 
            region,
            recovery_score,
            shock_magnitude,
            old_age_dependency_ratio,
            manufacturing_share,
            healthcare_share
        FROM recovery_scores
        ORDER BY recovery_score DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"regions": [dict(r) for r in rows]}


@app.get("/regions/{region}/forecast")
def get_forecast(region: str):
    """Return Prophet employment forecast (2026-2028) for a given region."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT year, region, yhat, yhat_lower, yhat_upper, is_forecast
        FROM model_output
        WHERE LOWER(region) = LOWER(%s)
        ORDER BY year
    """, (region,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if not rows:
        raise HTTPException(status_code=404, detail=f"Region '{region}' not found.")
    return {
        "region": region,
        "historical": [dict(r) for r in rows if not r["is_forecast"]],
        "forecast": [dict(r) for r in rows if r["is_forecast"]]
    }


@app.get("/regions/{region}/recovery")
def get_recovery(region: str):
    """Return recovery score and full demographic breakdown for a given region."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT *
        FROM recovery_scores
        WHERE LOWER(region) = LOWER(%s)
    """, (region,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail=f"Region '{region}' not found.")
    return dict(row)