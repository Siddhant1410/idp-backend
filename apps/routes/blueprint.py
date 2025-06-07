from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from apps.models.blueprint import Blueprint as BlueprintModel
from apps.schemas.blueprint import BlueprintTriggerRequest, BlueprintResponse
from apps.services.airflow_trigger import trigger_dag_run
from core.db import get_db

router = APIRouter(prefix="/blueprint", tags=["Blueprint"])

@router.post("/", response_model=BlueprintResponse)
async def trigger_blueprint(
    request: BlueprintTriggerRequest,
    db: AsyncSession = Depends(get_db)
):
    # Fetch blueprint from DB
    result = await db.execute(select(BlueprintModel).where(BlueprintModel.id == request.blueprint_id))
    blueprint = result.scalars().first()

    if not blueprint:
        raise HTTPException(status_code=404, detail="Blueprint not found")

    # Parse data to send to Airflow
    blueprint_conf = {
        "blueprint_id": blueprint.id,
        "process_name": blueprint.process_name,
        "flow": blueprint.flow,
        "metadata": blueprint.metadata,
    }

    # Trigger Airflow DAG
    dag_response = await trigger_dag_run(blueprint_conf)

    return BlueprintResponse(
        status="received",
        message=f"DAG triggered for blueprint ID {blueprint.id}",
        received_flow=blueprint.flow,
    )
