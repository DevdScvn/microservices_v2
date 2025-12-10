from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import func

from gateway.models import Script
from gateway.shemas import ScriptCreate


async def create_run(db: AsyncSession, run_in: ScriptCreate, job_name: str) -> Script:
    r = Script(name=run_in.name, status="pending", k8s_job_name=job_name, params=run_in.params)
    db.add(r)
    await db.commit()
    await db.refresh(r)
    return r


async def get_script(db: AsyncSession, run_id: int) -> Script:
    stmt = select(Script).where(Script.id == run_id)
    result = await db.execute(stmt)
    script = result.scalar_one_or_none()
    # r = await db.get(Script, run_id)
    return script


async def update_run_status(db: AsyncSession, run_id: int, status: str, exit_code: int | None = None):
    r = await db.get(Script, run_id)
    if not r:
        return None
    r.status = status
    if exit_code is not None:
        r.exit_code = exit_code
    if status in ("succeeded", "failed"):
        r.finished_at = func.now()
    db.add(r)
    await db.commit()
    await db.refresh(r)
    return r
