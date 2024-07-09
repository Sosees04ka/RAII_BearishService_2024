from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, Integer, BOOLEAN, Float


class Base(DeclarativeBase): pass


class Flat(Base):
    __tablename__ = "Flat"
    flatId = Column(Integer, primary_key=True, index=True)
    ratio = Column(Float, nullable=False)
    stability = Column(BOOLEAN)
