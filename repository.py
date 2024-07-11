import math

from sqlalchemy import select, func
from sqlalchemy.orm import aliased
from sqlalchemy.ext.asyncio import AsyncSession

from HouseEntity import House
from database import new_session, TaskOrm
from schemas import STaskAdd, STask, HouseResponse


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
    async def find_all(cls, q: str = None, offset: int = 0, limit: int = 10) -> [list[House], int]:
        async with new_session() as session:
            query = select(House).group_by(House.house_tkn)
            count = 0

            # Применяем условный поиск, если задан параметр q
            if q:
                query = query.where(House.house_tkn.contains(q))

            # Добавляем смещение и лимит
            query = query.offset(offset).limit(limit)
            if offset == 0:
                sub_query = select(House.house_tkn).group_by(House.house_tkn)
                if q:
                    sub_query = sub_query.where(House.house_tkn.contains(q))

                query_result = await session.execute(sub_query)
                count = len(query_result.scalars().all())

            result = await session.execute(query)
            houses = result.scalars().all()
            return houses, count

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

            percent_change = ((current_volume_electr - previous_volume_electr) / abs(previous_volume_electr)) * 100

            return percent_change

    @classmethod
    async def get_water_percent(cls, house):
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

            stmt_current_volume_water_hot = select(func.sum(House.volume_hot)).where(
                House.house_tkn == house,
                House.unix_payment_period == latest_unix_time
            )

            stmt_current_volume_water_cold = select(func.sum(House.volume_cold)).where(
                House.house_tkn == house,
                House.unix_payment_period == latest_unix_time
            )

            result_current_volume_water_hot = await session.execute(stmt_current_volume_water_hot)
            result_current_volume_water_cold = await session.execute(stmt_current_volume_water_cold)

            current_volume_water_hot = result_current_volume_water_hot.scalar()
            current_volume_water_cold = result_current_volume_water_cold.scalar()

            if current_volume_water_hot is None or current_volume_water_cold is None:
                return None

            current_volume_water = current_volume_water_hot + current_volume_water_cold

            stmt_previous_volume_water_hot = select(func.sum(House.volume_hot)).where(
                House.house_tkn == house,
                House.unix_payment_period == previous_unix_time
            )

            stmt_previous_volume_water_cold = select(func.sum(House.volume_cold)).where(
                House.house_tkn == house,
                House.unix_payment_period == previous_unix_time
            )

            result_previous_volume_water_hot = await session.execute(stmt_previous_volume_water_hot)
            result_previous_volume_water_cold = await session.execute(stmt_previous_volume_water_cold)

            previous_volume_water_hot = result_previous_volume_water_hot.scalar()
            previous_volume_water_cold = result_previous_volume_water_cold.scalar()

            if previous_volume_water_hot is None or previous_volume_water_cold is None:
                return None

            previous_volume_water = previous_volume_water_hot + previous_volume_water_cold

            if previous_volume_water == 0:
                return None

            percent_change = ((current_volume_water - previous_volume_water) / abs(previous_volume_water)) * 100

            return percent_change

    @classmethod
    async def get_water_percent_flat(cls, flat):
        async with new_session() as session:
            stmt_payment_periods = (
                select(House.unix_payment_period)
                .where(House.flat_tkn == flat)
                .order_by(House.unix_payment_period.desc())
                .limit(2)
            )

            result_payment_periods = await session.execute(stmt_payment_periods)
            payment_periods = result_payment_periods.scalars().all()

            if len(payment_periods) < 2:
                return None

            latest_unix_time, previous_unix_time = payment_periods

            stmt_volumes = (
                select(
                    House.unix_payment_period,
                    func.sum(House.volume_hot).label('sum_hot'),
                    func.sum(House.volume_cold).label('sum_cold')
                )
                .where(
                    House.flat_tkn == flat,
                    House.unix_payment_period.in_([latest_unix_time, previous_unix_time])
                )
                .group_by(House.unix_payment_period)
            )

            result_volumes = await session.execute(stmt_volumes)
            volumes = {row.unix_payment_period: (row.sum_hot, row.sum_cold) for row in result_volumes}

            if latest_unix_time not in volumes or previous_unix_time not in volumes:
                return None

            current_volume_water = sum(volumes[latest_unix_time])
            previous_volume_water = sum(volumes[previous_unix_time])

            if current_volume_water is None or previous_volume_water is None:
                return None

            percent_change = ((current_volume_water - previous_volume_water) / abs(previous_volume_water)) * 100

            return percent_change

    @classmethod
    async def get_energy_percent_flat(cls, flat):
        async with new_session() as session:
            stmt_latest = select(House.unix_payment_period).where(
                House.flat_tkn == flat
            ).order_by(House.unix_payment_period.desc()).limit(1)

            result_latest = await session.execute(stmt_latest)
            latest_unix_time = result_latest.scalar()

            if latest_unix_time is None:
                return None

            stmt_previous = select(House.unix_payment_period).where(
                House.flat_tkn == flat,
                House.unix_payment_period < latest_unix_time
            ).order_by(House.unix_payment_period.desc()).limit(1)

            result_previous = await session.execute(stmt_previous)
            previous_unix_time = result_previous.scalar()

            if previous_unix_time is None:
                return None

            stmt_current_volume_electr = select(func.sum(House.volume_electr)).where(
                House.flat_tkn == flat,
                House.unix_payment_period == latest_unix_time
            )

            result_current_volume_electr = await session.execute(stmt_current_volume_electr)
            current_volume_electr = result_current_volume_electr.scalar()

            if current_volume_electr is None:
                return None

            stmt_previous_volume_electr = select(func.sum(House.volume_electr)).where(
                House.flat_tkn == flat,
                House.unix_payment_period == previous_unix_time
            )

            result_previous_volume_electr = await session.execute(stmt_previous_volume_electr)
            previous_volume_electr = result_previous_volume_electr.scalar()

            if previous_volume_electr is None:
                return None

            percent_change = ((current_volume_electr - previous_volume_electr) / abs(previous_volume_electr)) * 100

            return percent_change

    @classmethod
    async def get_dept_percent_flat(cls, flat):
        async with new_session() as session:
            stmt_latest = select(House.unix_payment_period).where(
                House.flat_tkn == flat
            ).order_by(House.unix_payment_period.desc()).limit(1)

            result_latest = await session.execute(stmt_latest)
            latest_unix_time = result_latest.scalar()

            if latest_unix_time is None:
                return None

            stmt_previous = select(House.unix_payment_period).where(
                House.flat_tkn == flat,
                House.unix_payment_period < latest_unix_time
            ).order_by(House.unix_payment_period.desc()).limit(1)

            result_previous = await session.execute(stmt_previous)
            previous_unix_time = result_previous.scalar()

            if previous_unix_time is None:
                return None

            stmt_current_debt = select(func.sum(House.debt)).where(
                House.flat_tkn == flat,
                House.unix_payment_period == latest_unix_time
            )

            result_current_debt = await session.execute(stmt_current_debt)
            current_debt = result_current_debt.scalar() or 0

            stmt_previous_debt = select(func.sum(House.debt)).where(
                House.flat_tkn == flat,
                House.unix_payment_period == previous_unix_time
            )

            result_previous_debt = await session.execute(stmt_previous_debt)
            previous_debt = result_previous_debt.scalar() or 0

            if previous_debt == current_debt:
                return 0

            percent_change = ((current_debt - previous_debt) / abs(previous_debt)) * -100

            return percent_change

    @classmethod
    async def get_house_data(cls, house: int):
        async with new_session() as session:
            stmt = select(
                House.unix_payment_period,
                House.income,
                House.volume_cold,
                House.volume_hot,
                House.volume_electr
            ).where(House.house_tkn == house)

            result = await session.execute(stmt)
            return result.fetchall()

    @classmethod
    async def find_by_tkn(cls, house_tkn: int):
        async with new_session() as session:
            stmt = select(House).where(House.house_tkn == house_tkn)
            result = await session.execute(stmt)
            house = result.scalar()
        return house

    @classmethod
    async def count_people_in_house(cls, house_tkn: int):
        async with new_session() as session:
            # Вычисляем среднее количество пользователей в каждой квартире
            stmt = (
                select(House.flat_tkn, func.avg(House.count_people))
                .where(House.house_tkn == house_tkn)
                .group_by(House.flat_tkn)
            )

            avg_people_per_flat = await session.execute(stmt)

            total_people = 0
            for flat_tkn, avg_count in avg_people_per_flat:
                rounded_avg_count = math.ceil(avg_count)  # Округляем до целого числа в большую сторону
                total_people += rounded_avg_count

            return total_people
