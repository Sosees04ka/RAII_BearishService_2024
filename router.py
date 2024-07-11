from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from sqlalchemy import select, distinct

from HouseEntity import House
from gigachat import get_chat_completion, giga_token
from matrix import Matrix
from database import new_session
from repository import HouseRepository
from schemas import STaskAdd, STask, STaskId, HouseResponse, SearchResponse

from typing import Optional

router = APIRouter(
    prefix="/data",
    tags=["data"],
)


@router.get("/houses/{house_tkn}", response_model=HouseResponse)
async def get_house(house_tkn: int) -> HouseResponse:
    house = await HouseRepository.find_by_tkn(house_tkn)

    if not house:
        raise HTTPException(status_code=404, detail="House not found")

    count_flat = await HouseRepository.get_count_flat(house_tkn)
    debt_percent = await HouseRepository.get_dept_percent(house_tkn)
    water_percent = await HouseRepository.get_water_percent(house_tkn)
    electrical_percent = await HouseRepository.get_energy_percent(house_tkn)

    house_response = HouseResponse(
        house_tkn=house.house_tkn,
        count_flat=count_flat,
        debt_percent=debt_percent,
        water_percent=water_percent,
        electrical_percent=electrical_percent
    )

    return house_response


@router.get("/houses", response_model=SearchResponse)
async def get_houses(q: Optional[str] = None,
                     page: int = 1,
                     page_size: int = 10) -> SearchResponse:
    # Рассчитываем смещение для пагинации
    offset = (page - 1) * page_size

    # Получаем все дома с учетом пагинации
    houses, count = await HouseRepository.find_all(q=q, offset=offset, limit=page_size)

    house_responses = []
    for house in houses:
        house_tkn = house.house_tkn
        tariffs = await Matrix.get_tariffs(house_tkn)
        count_flat = await HouseRepository.get_count_flat(house_tkn)
        debt_percent = await HouseRepository.get_dept_percent(house_tkn)
        count_persons = await HouseRepository.count_people_in_house(house_tkn)
        water_percent = await HouseRepository.get_water_percent(house_tkn)
        electrical_percent = await HouseRepository.get_energy_percent(house_tkn)

        house_response = HouseResponse(
            house_tkn=house_tkn,
            count_flat=count_flat,
            count_persons=count_persons,
            debt_percent=debt_percent,
            water_percent=water_percent,
            electrical_percent=electrical_percent,
            rate_cold_water=tariffs.get('cold_water_tariff'),
            rate_hot_water=tariffs.get('hot_water_tariff'),
            rate_electrical=tariffs.get('electricity_tariff')
        )
        house_responses.append(house_response)

    return SearchResponse(houses=house_responses, count=count)


@router.get("/housesIds", response_model=list[int])
async def get_housesIds() -> list[int]:
    # Получаем все уникальные идентификаторы домов
    async with new_session() as session:
        query = select(distinct(House.house_tkn))
        result = await session.execute(query)
        house_ids = [row[0] for row in result]

    return house_ids

@router.get("/chat")
async def add_question(question: str):
    # Получение ответа от нейронной сети
    neural_response = get_chat_completion(giga_token, question)

    return {"answer": neural_response}