from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from core.db import Base
from sqlalchemy.sql import func

class ProcessInstance(Base):
    __tablename__ = "ProcessInstances"

    id = Column(Integer, primary_key=True)
    processInstanceName = Column(String(512), nullable=False)
    processInstanceDescription = Column(String(512))
    currentStage = Column(String(512))
    isInstanceRunning = Column(Boolean, default=True)

    createdAt = Column(DateTime, default=func.now())
    updatedAt = Column(DateTime, default=func.now(), onupdate=func.now())
    deletedAt = Column(DateTime, nullable=True)
    
    isDeleted = Column(Boolean, default=False)
    isActive = Column(Boolean, default=True)
    remark = Column(String(512))
    
    processesId = Column(Integer, ForeignKey("Processes.id"))
