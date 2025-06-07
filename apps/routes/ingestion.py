from fastapi import APIRouter, status
from typing import Optional

router = APIRouter(prefix="/ingestion", tags=["Ingestion"])

@router.post("/trigger", status_code=status.HTTP_202_ACCEPTED)
async def trigger_ingestion(manual: Optional[bool] = False):
    """
    Trigger the document ingestion DAG in Airflow.
    If 'manual' is True, the DAG is triggered on demand.
    """
    # TODO: Replace this with actual Airflow trigger logic
    return {"message": "Ingestion triggered", "manual": manual}
