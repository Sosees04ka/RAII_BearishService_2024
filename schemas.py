from typing import Optional

from pydantic import BaseModel, ConfigDict


class STaskAdd(BaseModel):
    name: str
    description: Optional[str] = None


class STask(STaskAdd):
    id: int

    model_config = ConfigDict(from_attributes=True)


class STaskId(BaseModel):
    ok: bool = True
    task_id: int


class HouseResponse(BaseModel):
    house_tkn: int
    count_flat: int
    count_persons: int
    debt_percent: Optional[float]
    water_percent: Optional[float]
    electrical_percent: Optional[float]
    rate_cold_water: Optional[float]
    rate_hot_water: Optional[float]
    rate_electrical: Optional[float]
