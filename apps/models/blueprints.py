from sqlalchemy.orm import relationship
from sqlalchemy import Column, Integer, Text, ForeignKey
from core.db import Base

class Blueprint(Base):
    __tablename__ = "BluePrint"

    id = Column(Integer, primary_key=True)
    bluePrint = Column(Text, nullable=False)