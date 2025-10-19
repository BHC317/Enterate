from fastapi import Header, HTTPException
from typing import Optional

API_KEY = "demo-key-logistics"

async def require_api_key(x_api_key: Optional[str] = Header(None, alias="X-API-Key")):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")