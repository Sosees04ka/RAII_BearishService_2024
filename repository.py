from calendar import calendar
from datetime import datetime, timedelta

import numpy
import sqlalchemy
from sqlalchemy import select, func

from HouseEntity import House
from database import new_session, TaskOrm
from schemas import STaskAdd, STask


class HouseRepository:
    @classmethod
    async def add_one(cls, data: STaskAdd) -> int:
        async with new_session() as session:
            task_dict = data.model_dump()

            task = TaskOrm(**task_dict)
            session.add(task)
            await session.flush()
            await session.commit()
            return task.id

    @classmethod
    async def find_all(cls) -> list[STask]:
        async with new_session() as session:
            query = select(TaskOrm)
            result = await session.execute(query)
            task_models = result.scalars().all()
            task_schemas = [STask.model_validate(task_model) for task_model in task_models]
            return task_schemas

    @classmethod
    async def get_flat_ids_grouped(cls) -> list:
        async with new_session() as session:
            query = select(House.flat_tkn).group_by(House.flat_tkn)
            result = await session.execute(query)
            return result.scalars().all()

    @classmethod
    async def get_flat_debts_by_id(cls, flat_tkn_value) -> list:
        async with new_session() as session:
            query = select(House.debt).where(House.flat_tkn == flat_tkn_value)
            result = await session.execute(query)
            return result.scalars().all()

    @classmethod
    async def add_flats(cls, flats):
        async with new_session() as session:
            try:
                session.add_all(flats)
                await session.flush()
                await session.commit()
            except Exception as e:
                await session.rollback()
                print(f"Error occurred: {e}")
            finally:
                await session.close()

    @classmethod
    async def get_count_flat(cls, house):
        async with new_session() as session:
            stmt = select(func.count(func.distinct(House.flat_tkn))).where(House.house_tkn == house)
            result = await session.execute(stmt)
            count = result.scalar_one()
        return count

    @classmethod
    async def get_dept_percent(cls, house):
        async with new_session() as session:
            stmt_latest = select(House.unix_payment_period).where(
                House.house_tkn == house
            ).order_by(House.unix_payment_period.desc()).limit(1)

            result_latest = await session.execute(stmt_latest)
            latest_unix_time = result_latest.scalar()

            if latest_unix_time is None:
                return None

            stmt_previous = select(House.unix_payment_period).where(
                House.house_tkn == house,
                House.unix_payment_period < latest_unix_time
            ).order_by(House.unix_payment_period.desc()).limit(1)

            result_previous = await session.execute(stmt_previous)
            previous_unix_time = result_previous.scalar()

            if previous_unix_time is None:
                return None

            stmt_current_debt = select(func.sum(House.debt)).where(
                House.house_tkn == house,
                House.unix_payment_period == latest_unix_time
            )

            result_current_debt = await session.execute(stmt_current_debt)
            current_debt = result_current_debt.scalar() or 0

            stmt_previous_debt = select(func.sum(House.debt)).where(
                House.house_tkn == house,
                House.unix_payment_period == previous_unix_time
            )

            result_previous_debt = await session.execute(stmt_previous_debt)
            previous_debt = result_previous_debt.scalar() or 0

            if previous_debt == 0:
                return None

            percent_change = ((current_debt - previous_debt) / abs(previous_debt)) * -100

            return percent_change

    @classmethod
    async def get_energy_percent(cls, house):
        async with new_session() as session:
            stmt_latest = select(House.unix_payment_period).where(
                House.house_tkn == house
            ).order_by(House.unix_payment_period.desc()).limit(1)

            result_latest = await session.execute(stmt_latest)
            latest_unix_time = result_latest.scalar()

            if latest_unix_time is None:
                return None

            stmt_previous = select(House.unix_payment_period).where(
                House.house_tkn == house,
                House.unix_payment_period < latest_unix_time
            ).order_by(House.unix_payment_period.desc()).limit(1)

            result_previous = await session.execute(stmt_previous)
            previous_unix_time = result_previous.scalar()

            if previous_unix_time is None:
                return None

            stmt_current_volume_electr = select(func.sum(House.volume_electr)).where(
                House.house_tkn == house,
                House.unix_payment_period == latest_unix_time
            )

            result_current_volume_electr = await session.execute(stmt_current_volume_electr)
            current_volume_electr = result_current_volume_electr.scalar()

            if current_volume_electr is None:
                return None

            stmt_previous_volume_electr = select(func.sum(House.volume_electr)).where(
                House.house_tkn == house,
                House.unix_payment_period == previous_unix_time
            )

            result_previous_volume_electr = await session.execute(stmt_previous_volume_electr)
            previous_volume_electr = result_previous_volume_electr.scalar()

            if previous_volume_electr is None:
                return None

            percent_change = ((current_volume_electr - previous_volume_electr) / abs(previous_volume_electr)) * -100

            return percent_change
