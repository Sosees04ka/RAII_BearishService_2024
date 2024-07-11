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


class SearchResponse(BaseModel):
    houses: list[HouseResponse]
    count: int


class ValuePeriod(BaseModel):
    value: int
    period: str


class Flat(BaseModel):
    house_tkn: int
    flat_tkn: int
    flat_persons: list[ValuePeriod]
    cold_water: list[ValuePeriod]
    hot_water: list[ValuePeriod]
    electrical: list[ValuePeriod]
    current_debt: int
    dynamic_index: int
    stability: bool
    water_percent: int
    electrical_percent: int
    debt_cluster: int
