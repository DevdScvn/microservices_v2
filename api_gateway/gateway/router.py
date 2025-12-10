from typing import Annotated

from fastapi import APIRouter

from database import db_helper
from gateway.crud import create_run, get_script
from gateway.k8s_client import create_job_async, get_job_status
from gateway.shemas import ScriptOut, ScriptCreate

import uuid
from fastapi import FastAPI, WebSocket, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from kubernetes import client, watch, config
import asyncio


router = APIRouter(prefix="/gateway", tags=["gate"])


app = FastAPI(title="K8s Runner MVP")

try:
    config.load_incluster_config()
except:
    config.load_kube_config()


@router.post("/runs", response_model=ScriptOut)
async def create_run_endpoint(payload: ScriptCreate,
                              db: Annotated[AsyncSession, Depends(db_helper.session_getter)]):
    # generate unique job name
    job_name = f"run-{uuid.uuid4().hex[:8]}"
    # create k8s job
    try:
        await create_job_async(job_name, payload.script)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create k8s Job: {e}")

    # create DB record
    run = await create_run(db, payload, job_name)
    return run


@router.get("/runs/{run_id}")
async def get_run(
        db: Annotated[AsyncSession, Depends(db_helper.session_getter)],
        run_id: int):
    r = await get_script(db, run_id)
    if not r:
        raise HTTPException(status_code=404, detail="Run not found")
    # enrich with k8s job status
    if r.k8s_job_name:
        kstatus = await get_job_status(r.k8s_job_name)
        r.status = kstatus
    return r


@router.websocket("/runs/{run_id}/logs/ws")
async def websocket_logs(websocket: WebSocket):
    await websocket.accept()
    await websocket.send_text("Hello WebSocket")

# @router.websocket("/runs/{run_id}/logs/ws")
# async def websocket_logs(
#         websocket: WebSocket,
#         # run_id: int,
#         # db: Annotated[AsyncSession, Depends(db_helper.session_getter)]
# ):
#     await websocket.accept()

    # --- Проверки ---
    # r = await get_run(db, run_id)
    # if not r:
    #     await websocket.send_text("Run not found")
    #     await websocket.close()
    #     return
    #
    # if not r.k8s_job_name:
    #     await websocket.send_text("No associated k8s job")
    #     await websocket.close()
    #     return
    #
    # job_name = r.k8s_job_name
    #
    # # --- K8s client ---
    # await config.load_kube_config()  # или load_incluster_config() — если в кластере
    # v1 = client.CoreV1Api()
    #
    # try:
    #     # 1. Ждём, пока появится Pod
    #     pods = await v1.list_namespaced_pod(
    #         namespace="default",
    #         label_selector=f"job-name={job_name}"
    #     )
    #
    #     if not pods.items:
    #         await websocket.send_text("Waiting for pod to be created…")
    #         # Ждем появления
    #         while True:
    #             pods = await v1.list_namespaced_pod(
    #                 namespace="default",
    #                 label_selector=f"job-name={job_name}"
    #             )
    #             if pods.items:
    #                 break
    #             await asyncio.sleep(1)
    #
    #     pod_name = pods.items[0].metadata.name
    #     await websocket.send_text(f"Streaming logs from pod: {pod_name}")
    #
    #     # 2. Стрим логов в реальном времени
    #     w = watch.Watch()
    #     async for line in w.stream(
    #             v1.read_namespaced_pod_log,
    #             name=pod_name,
    #             namespace="default",
    #             follow=True,
    #             _preload_content=False
    #     ):
    #         # line будет бинарным — декодируем
    #         await websocket.send_text(line.decode("utf-8"))
    #
    # except Exception as e:
    #     await websocket.send_text(f"Error: {e}")
    #
    # finally:
    #     await websocket.close()
