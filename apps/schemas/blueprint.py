from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class BlueprintTriggerRequest(BaseModel):
    blueprint_id: int

class BlueprintResponse(BaseModel):
    status: str
    message: str
    received_flow: List[str]
