import asyncio
import time
from functools import lru_cache, cache

import numpy as np
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from FlatRatioEntity import Flat, Base
from processing import csv_to_unixtime_df
from repository import TaskRepository

engine = create_async_engine("sqlite+aiosqlite:///data.db")
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def new_session() -> AsyncSession:
    return async_session()


async def create_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def delete_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


def linregress(x, y):
    x = np.array(x, dtype=np.float64)
    y = np.array(y, dtype=np.float64)
    w = np.ones(x.size, dtype=np.float64)
    stability = True

    for mark in x:
        if mark > 0.0:
            stability = False
            break

    wxy = np.sum(w * y * x)
    wx = np.sum(w * x)
    wy = np.sum(w * y)
    wx2 = np.sum(w * x * x)
    sw = np.sum(w)

    den = wx2 * sw - wx * wx

    if den == 0:
        den = np.finfo(np.float64).eps

    k = (sw * wxy - wx * wy) / den

    return k, stability


async def dataAllocation():
    filename = "raai_school_2024.csv"  # Ensure the CSV file is in the correct format
    data = csv_to_unixtime_df(filename)
    data = data[['flat_tkn', 'debt']]

    flatItems = []
    for name, group in data.groupby(['flat_tkn']):
        key = name[0]
        x = group['debt'].tolist()
        y = np.linspace(1, len(x), len(x))
        k, stability = linregress(x, y)

        flatItems.append(Flat(flatId=key, ratio=k, stability=stability))
        print(f"Record #{key}")

    await TaskRepository.addFlats(flatItems)

    # flats_id = await TaskRepository.getFlatIDsGrouped()
    #
    # # print(np.linspace(1, len(x), len(x)))
    # # запрашиваю данные о кв и долгах
    # # беру данные долга запихиваю в x
    # # формирую y при помощи linspace
    # # получаю k
    # # добавляю ид кв + k в массив
    # # массив по окончанию выгружается в бд
    # # хайп
    #
    # flatItems = []
    #
    # count = 0
    # for flat_id in flats_id:
    #     start = time.time()
    #     x = await TaskRepository.getFlatDebtsById(flat_id)
    #     y = np.linspace(1, len(x), len(x))
    #     k, stability = linregress(x, y)
    #
    #     end = time.time()
    #     print(end - start)
    #     flatItems.append(Flat(flatId=flat_id, ratio=k, stability=stability))
    #     print(f"Record #{flat_id}")
    #
    #     if count % 1000 == 0 and count != 0:
    #         await TaskRepository.addFlats(flatItems)


# Принесли Чебурашка и Гена домой ящик пива.
# Чебурашка случайно разбил одну бутылку.
# Гена это увидел, схватил весь ящик, пизданул об стену и заорал:
# "Вот и попили, блять, пивка!".

async def main():
    await delete_tables()
    await create_tables()
    await dataAllocation()


# Запуск асинхронной функции
asyncio.run(main())
