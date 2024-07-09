from sqlalchemy import select

from HouseEntity import House
from database import new_session, TaskOrm
from schemas import STaskAdd, STask


class TaskRepository:
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
    async def getFlatIDsGrouped(cls) -> list:
        async with new_session() as session:
            query = select(House.flat_tkn).group_by(House.flat_tkn)
            result = await session.execute(query)
            return result.scalars().all()

    @classmethod
    async def getFlatDebtsById(cls, flat_tkn_value) -> list:
        async with new_session() as session:
            query = select(House.debt).where(House.flat_tkn == flat_tkn_value)
            result = await session.execute(query)
            return result.scalars().all()

    @classmethod
    async def addFlats(cls, flats):
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