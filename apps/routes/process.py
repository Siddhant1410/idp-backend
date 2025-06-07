from fastapi import APIRouter, Depends, HTTPException
from apps.models.process_instance import ProcessInstance
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from apps.models.process import Process
from apps.models.blueprints import Blueprint
from apps.services.ingestion_service import fetch_documents_from_http
from core.db import get_db
import json
from datetime import datetime

router = APIRouter(prefix="/process", tags=["Process"])

@router.get("/{process_id}/blueprint")
async def get_process_blueprint(process_id: int, db: AsyncSession = Depends(get_db)):
    # Step 1: Get process
    result = await db.execute(select(Process).where(Process.id == process_id))
    process = result.scalars().first()
    if not process:
        raise HTTPException(status_code=404, detail="Process not found")

    # Step 2: Get blueprint by bluePrintId
    result = await db.execute(select(Blueprint).where(Blueprint.id == process.bluePrintId))
    blueprint = result.scalars().first()
    if not blueprint:
        raise HTTPException(status_code=404, detail="Blueprint not found")

    # Step 3: Parse blueprint JSON string
    parsed_blueprint = json.loads(blueprint.bluePrint)

    # --- Update ProcessInstance ---
    instance_result = await db.execute(
        select(ProcessInstance).where(ProcessInstance.processesId == process_id)
    )
    instance = instance_result.scalars().first()

    if instance:
        instance.currentStage = "Ingestion"
        instance.updatedAt = datetime.utcnow()
        await db.commit()
        await db.refresh(instance)
    else:
        raise HTTPException(status_code=404, detail="No ProcessInstance found for this process")

    # Find the ingestion node
    ingestion_node = next((node for node in parsed_blueprint if node.get("nodeName") == "ingestion"), None)
    if not ingestion_node:
        raise HTTPException(status_code=400, detail="Ingestion node not found in blueprint")

    path = ingestion_node["component"].get("path")
    if not path:
        raise HTTPException(status_code=400, detail="Ingestion path is missing")

    # Fetch documents from the path
    try:
        documents = await fetch_documents_from_http(path)
    except RuntimeError as e:
        raise HTTPException(status_code=502, detail=str(e))


    return {
        "process_id": process.id,
        "blueprint_id": blueprint.id,
        "current_stage": instance.currentStage,
        "parsed_blueprint": parsed_blueprint,
        "ingested documents": documents
    }
