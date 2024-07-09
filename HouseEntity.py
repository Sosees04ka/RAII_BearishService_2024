from sqlalchemy import Float
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, Integer


class Base(DeclarativeBase): pass


class House(Base):
    __tablename__ = "House"

    house_tkn = Column(Integer, primary_key=True, index=True)
    flat_tkn = Column(Integer)
    unix_payment_period = Column(Integer)
    income = Column(Float)
    debt = Column(Float)
    raised = Column(Float)
    volume_cold = Column(Float)
    volume_hot = Column(Float)
    volume_electr = Column(Float)
