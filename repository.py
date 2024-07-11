import math

import numpy as np
from matplotlib import pyplot as plt
from sqlalchemy import select, func
from sqlalchemy.orm import aliased
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from HouseEntity import House
from FlatRatioEntity import Flat as FlatEntity
from database import new_session, TaskOrm
from schemas import Flat, ValuePeriod
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
    async def get_info_flat(cls, flat_id: int) -> Flat:
        async with new_session() as session:
            query = select(House.house_tkn).where(House.flat_tkn == flat_id)
            house_id = (await session.execute(query)).scalar()
            house_id = int(house_id) if house_id else None

            query = select(House.unix_payment_period, House.count_people,
                           House.volume_hot, House.volume_cold, House.volume_electr,
                           House.debt) \
                .where(House.flat_tkn == flat_id)
            results = await session.execute(query)
            data = results.all()

            flat_persons = [ValuePeriod(period=row.unix_payment_period, value=row.count_people) for row in data]
            flat_hot = [ValuePeriod(period=row.unix_payment_period, value=row.volume_hot) for row in data]
            flat_cold = [ValuePeriod(period=row.unix_payment_period, value=row.volume_cold) for row in data]
            flat_electr = [ValuePeriod(period=row.unix_payment_period, value=row.volume_electr) for row in data]

            last_debt = data[-1].debt

            query_flat = select(FlatEntity).where(FlatEntity.flatId == flat_id)
            res = await session.execute(query_flat)
            data_flat, *_ = res.all()

            dynamic_index = data_flat[0].debtAverage
            stability = data_flat[0].stability
            class_debt = data_flat[0].class_debt
            water_percent = await cls.get_water_percent_flat(flat_id)
            electrical_percent = await cls.get_energy_percent_flat(flat_id)

            result = Flat(
                house_tkn=house_id,
                flat_tkn=flat_id,
                flat_persons=flat_persons,
                cold_water=flat_cold,
                hot_water=flat_hot,
                electrical=flat_electr,
                current_debt=last_debt,
                dynamic_index=dynamic_index,
                stability=stability,
                water_percent=water_percent,
                electrical_percent=electrical_percent,
                debt_cluster=class_debt
            )

            return result

    @classmethod
    async def find_all(cls, q: str = None, offset: int = 0, limit: int = 10) -> [list[House], int]:
        async with new_session() as session:
            query = select(House).group_by(House.house_tkn)
            sub_query = select(House.house_tkn).group_by(House.house_tkn)

            # Применяем условный поиск, если задан параметр q
            if q:
                query = query.where(House.house_tkn.contains(q))
                sub_query = sub_query.where(House.house_tkn.contains(q))

            # Добавляем смещение и лимит
            query = query.offset(offset).limit(limit)
            # query_count = select(sub_query, func.count())

            result = await session.execute(query)
            count = await session.execute(sub_query)
            houses = result.scalars().all()
            return houses, len(count.scalars().all())

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

            if previous_debt == current_debt:
                return 0

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

            if current_volume_electr == previous_volume_electr:
                return 0

            if previous_volume_electr == 0:
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

            if current_volume_water_hot is None and current_volume_water_cold is None:
                return None

            current_volume_water = (current_volume_water_hot or 0) + (current_volume_water_cold or 0)

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

            if previous_volume_water_hot is None and previous_volume_water_cold is None:
                return None

            previous_volume_water = (previous_volume_water_hot or 0) + (previous_volume_water_cold or 0)

            if previous_volume_water == current_volume_water:
                return 0

            if previous_volume_water == 0:
                return None

            percent_change = ((current_volume_water - previous_volume_water) / abs(previous_volume_water)) * 100

            return percent_change

    @classmethod
    async def get_water_percent_flat(cls, flat):
        async with new_session() as session:
            # Получаем последние два периода оплаты
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

            # Получаем объемы холодной и горячей воды за последние два периода оплаты
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

            # Проверяем наличие данных за оба периода
            if latest_unix_time not in volumes or previous_unix_time not in volumes:
                return None

            # Получаем текущие и предыдущие объемы воды
            latest_sum_hot, latest_sum_cold = volumes[latest_unix_time]
            previous_sum_hot, previous_sum_cold = volumes[previous_unix_time]

            # Если оба значения (горячая и холодная вода) для любого из периодов отсутствуют, возвращаем None
            if (latest_sum_hot is None and latest_sum_cold is None) or (
                    previous_sum_hot is None and previous_sum_cold is None):
                return None

            # Заменяем None на 0 и суммируем
            current_volume_water = (latest_sum_hot or 0) + (latest_sum_cold or 0)
            previous_volume_water = (previous_sum_hot or 0) + (previous_sum_cold or 0)

            # Если после всех проверок один из объемов равен нулю, возвращаем None
            if current_volume_water is None or previous_volume_water is None:
                return None

            if current_volume_water == previous_volume_water:
                return 0

            if previous_volume_water==0:
                return None

            # Вычисляем процентное изменение
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

            if current_volume_electr == previous_volume_electr:
                return 0

            if previous_volume_electr==0:
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

            if previous_debt == 0:
                return None

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

    def plot_linear_regression_with_dates(x_dates, y, coefficient, file_path='linear_regression_plot.png'):
        # Преобразование данных в нужный формат
        y = np.array(y)
        x_dates = np.array([datetime.strptime(str(date), "%Y-%m-%d") for date in x_dates])

        # Линия тренда
        y_trend = coefficient * y

        # Построение графика
        plt.figure(figsize=(12, 8))  # Размер графика 12x8 дюймов
        plt.scatter(x_dates, y, color='blue', label='Данные')
        plt.plot(x_dates, y, color='blue', linestyle='-', linewidth=1, alpha=0.5,
                 label='Соединенные точки')  # Соединяем точки
        plt.plot(x_dates, y_trend, color='red', linestyle='--', linewidth=2,
                 label=f'Линия тренда (коэффициент {coefficient})')
        plt.xlabel('Дата')
        plt.ylabel('Значение')
        plt.title('График линейной регрессии с заданным коэффициентом')
        plt.legend()

        # Добавление значений точек
        for i, (date, value) in enumerate(zip(x_dates, y)):
            plt.text(date, value, f'{value}', ha='center', va='bottom', fontsize=10)

        # Настройка формата дат на оси X
        plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m-%d'))
        plt.gca().xaxis.set_major_locator(plt.matplotlib.dates.AutoDateLocator())
        plt.gcf().autofmt_xdate(rotation=45)

        # Масштабирование графика
        plt.tight_layout()

        # Сохранение графика в файл
        plt.savefig(file_path)
        plt.close()
        print(f"Graph saved to {file_path}")