import httpx
from typing import List, Dict

async def fetch_documents_from_http(path: str) -> List[Dict]:
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(path)
            response.raise_for_status()
            return response.json()  # assuming the response is a list of document metadata
    except httpx.HTTPError as e:
        raise RuntimeError(f"Failed to fetch documents from {path}: {str(e)}")
