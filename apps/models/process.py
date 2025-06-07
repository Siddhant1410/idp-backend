from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from core.db import Base

class Process(Base):
    __tablename__ = "Processes"

    id = Column(Integer, primary_key=True)
    name = Column(String(512), nullable=False)
    description = Column(String(512))
    bluePrintId = Column(Integer, ForeignKey("BluePrint.id"))

    blueprint = relationship(
        "Blueprint",
        foreign_keys=[bluePrintId],
        uselist=False
    )
