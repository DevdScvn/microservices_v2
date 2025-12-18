import logging

from fastapi import HTTPException
from sqlalchemy import select, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func
from starlette import status

from gateway.models import Script, ScriptStatus
from gateway.shemas import ScriptCreate

logger = logging.getLogger(__name__)

class ScriptCrud:
    model = Script

    @classmethod
    async def create_script(cls, session: AsyncSession, script_in: ScriptCreate, job_name: str):
        query = insert(cls.model).values(
                    name=script_in.name,
                    status="pending",
                    k8s_job_name=job_name,
                    params=script_in.params
                ).returning(cls.model)
        try:
            result = await session.execute(query)
            await session.commit()
            return result.scalars().first()
        except IntegrityError:
            await session.rollback()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Object already exists"
            ) from None

    @classmethod
    async def get_script(cls, db: AsyncSession, script_id: int) -> Script:
        stmt = select(Script).where(Script.id == script_id)
        result = await db.execute(stmt)
        if script := result.scalar_one_or_none():
            return script
        else:
            raise HTTPException(status_code=404, detail="Object not found")


    # @classmethod
    # async def update_run_status(cls,
    #             db: AsyncSession,
    #                 run_id: int, status: str, exit_code: int | None = None):
    #     r = await db.get(Script, run_id)
    #     if not r:
    #         return None
    #     r.status = status
    #     if exit_code is not None:
    #         r.exit_code = exit_code
    #     if status in ("succeeded", "failed"):
    #         r.completed_at = func.now()
    #     db.add(r)
    #     await db.commit()
    #     await db.refresh(r)
    #     return r

    @classmethod
    async def update_run_status(
            cls,
            db: AsyncSession,
            run_id: int,
            status: str,
            exit_code: int | None = None
    ):
        r = await db.get(Script, run_id)
        if not r:
            logger.error(f"Script {run_id} not found for status update")
            return None

        try:
            status_enum = ScriptStatus(status)
        except ValueError as e:
            logger.error(f"Invalid status value: {status}. Error: {e}")
            return None

        r.status = status_enum
        if exit_code is not None:
            r.exit_code = exit_code

        if status_enum in (ScriptStatus.succeeded, ScriptStatus.failed):
            r.completed_at = func.now()

        db.add(r)
        await db.commit()
        await db.refresh(r)

        logger.info(f"Updated script {run_id} status to {status_enum.value}")
        return r

    @classmethod
    async def save_script_logs(
            cls,
            db: AsyncSession,
            run_id: int,
            logs: str
    ):
        """Сохраняет логи скрипта в БД"""
        from models import Script

        r = await db.get(Script, run_id)
        if not r:
            logger.error(f"Script {run_id} not found for logs save")
            return None

        r.logs = logs
        db.add(r)
        await db.commit()
        await db.refresh(r)

        logger.info(f"Saved logs for script {run_id}, length: {len(logs)}")
        return r