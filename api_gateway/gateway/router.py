import logging
from typing import Annotated, Optional, List

from fastapi import APIRouter
from sqlalchemy import select

from database import db_helper
from gateway.crud import ScriptCrud
from gateway.k8s_client import create_job_async, get_job_status
from gateway.models import Script
from gateway.shemas import ScriptOut, ScriptCreate

import uuid
from fastapi import Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from kubernetes import config

router = APIRouter(prefix="/gateway", tags=["gate"])


logger = logging.getLogger(__name__)

try:
    config.load_incluster_config()
except:
    config.load_kube_config()


@router.post("/runs", response_model=ScriptOut)
async def create_bash_script_k8s(payload: ScriptCreate,
                              db: Annotated[AsyncSession, Depends(db_helper.session_getter)]):
    # generate unique job name
    job_name = f"run-{uuid.uuid4().hex[:8]}"
    # create k8s job
    try:
        await create_job_async(job_name, payload.script)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create k8s Job: {e}")

    # create DB record
    run = await ScriptCrud.create_script(db, payload, job_name)
    return run


@router.get("/runs", response_model=List[ScriptOut])
async def get_pagination_scripts(
        db: Annotated[AsyncSession, Depends(db_helper.session_getter)],
        offset: int = 0,
        limit: int = 10,
        status: Optional[str] = None,
):
    """Получить список всех скриптов"""
    query = select(Script)

    if status:
        query = query.where(Script.status == status)

    query = query.offset(offset).limit(limit)

    result = await db.execute(query)
    scripts = result.scalars().all()

    return scripts


@router.get("/runs/{run_id}")
async def get_script_for_id(
        db: Annotated[AsyncSession, Depends(db_helper.session_getter)],
        run_id: int):
    r = await ScriptCrud.get_script(db, run_id)
    if not r:
        raise HTTPException(status_code=404, detail="Run not found")
    # enrich with k8s job status
    if r.k8s_job_name:
        kstatus = await get_job_status(r.k8s_job_name)
        r.status = kstatus
    return r
