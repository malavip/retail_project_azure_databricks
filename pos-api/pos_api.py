"""
Mock POS REST API for retail data engineering project.

Serves synthetic transactions from a CSV file, behaving like a real
Point-of-Sale system's transaction endpoint. Designed to be consumed
by Azure Data Factory's REST source with tumbling window triggers.

Endpoints:
    GET /health
        Health check.

    GET /api/v1/transactions?from=<iso>&to=<iso>&page=<n>&page_size=<n>
        Returns transactions whose transaction_time falls in [from, to).
        Supports pagination via page/page_size.
        Requires Bearer token in Authorization header.

Authentication:
    Send header: Authorization: Bearer demo-api-key-12345

Local run:
    pip install fastapi uvicorn pandas
    uvicorn pos_api:app --host 0.0.0.0 --port 8000

Test:
    curl -H "Authorization: Bearer demo-api-key-12345" \\
      "http://localhost:8000/api/v1/transactions?from=2026-04-12T00:00:00&to=2026-04-13T00:00:00&page=1&page_size=10"
"""

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime, timezone
from typing import Optional
import pandas as pd
import os

app = FastAPI(
    title="Mock POS API",
    description="Synthetic Point-of-Sale transactions for data engineering demos",
    version="1.0.0",
)

# ============ CONFIGURATION ============
API_KEY = os.environ.get("API_KEY", "demo-api-key-12345")
DATA_FILE = os.environ.get("DATA_FILE", "transactions.csv")

# ============ LOAD DATA AT STARTUP ============
print(f"Loading transactions from {DATA_FILE}...")
TRANSACTIONS_DF = pd.read_csv(DATA_FILE)
TRANSACTIONS_DF["transaction_time"] = pd.to_datetime(
    TRANSACTIONS_DF["transaction_time"]
)
TRANSACTIONS_DF = TRANSACTIONS_DF.sort_values("transaction_time").reset_index(drop=True)
print(f"  Loaded {len(TRANSACTIONS_DF)} transactions")
print(f"  Date range: {TRANSACTIONS_DF['transaction_time'].min()} to {TRANSACTIONS_DF['transaction_time'].max()}")


# ============ HELPERS ============
def parse_iso(s: str) -> datetime:
    """Parse ISO 8601 strings; allow trailing Z (UTC) and naive timestamps."""
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return dt
    return dt.replace(tzinfo=None)  # match the naive timestamps in the CSV


security = HTTPBearer()

def require_auth(credentials: HTTPAuthorizationCredentials = Depends(security)) -> None:
    """Validate Bearer token via standard FastAPI security scheme."""
    if credentials.credentials != API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )


# ============ ENDPOINTS ============
@app.get("/health")
def health():
    """Health check endpoint — no auth required."""
    return {
        "status": "healthy",
        "service": "mock_pos_api",
        "rows_loaded": len(TRANSACTIONS_DF),
        "data_range": {
            "from": str(TRANSACTIONS_DF["transaction_time"].min()),
            "to": str(TRANSACTIONS_DF["transaction_time"].max()),
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/v1/transactions")
def list_transactions(
    from_time: Optional[str] = Query(None, alias="from"),
    to_time: Optional[str] = Query(None, alias="to"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    _auth: None = Depends(require_auth),
):
    """
    Returns transactions in the time window [from, to).
    
    If from/to are omitted, returns ALL transactions (paginated).
    Useful for backfill scenarios.
    """
    df = TRANSACTIONS_DF

    # Apply time window filter
    if from_time:
        df = df[df["transaction_time"] >= parse_iso(from_time)]
    if to_time:
        df = df[df["transaction_time"] < parse_iso(to_time)]

    total = len(df)

    # Paginate
    start = (page - 1) * page_size
    end = start + page_size
    page_data = df.iloc[start:end].copy()

    # Convert timestamps to ISO strings for JSON
    page_data["transaction_time"] = page_data["transaction_time"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    records = page_data.to_dict(orient="records")

    # Pagination metadata
    next_page = page + 1 if end < total else None

    return {
        "data": records,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total": total,
            "next_page": next_page,
            "has_more": next_page is not None,
        },
        "window": {
            "from": from_time,
            "to": to_time,
        },
    }


@app.get("/")
def root():
    return {
        "service": "Mock POS API",
        "docs": "/docs",
        "health": "/health",
        "endpoint": "/api/v1/transactions",
    }