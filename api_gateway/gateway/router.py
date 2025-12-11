import logging
from typing import Annotated

from fastapi import APIRouter
from starlette.websockets import WebSocketDisconnect

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

logger = logging.getLogger(__name__)

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


# @router.websocket("/runs/{run_id}/logs/ws")
# async def websocket_logs(websocket: WebSocket):
#     await websocket.accept()
#     await websocket.send_text("Hello WebSocket")


# @router.websocket("/runs/{run_id}/logs/ws")
# async def websocket_logs_fixed(
#     websocket: WebSocket,
#     run_id: int,
#     db: Annotated[AsyncSession, Depends(db_helper.session_getter)]
# ):
#     """
#     Исправленный WebSocket endpoint
#     Путь: /runs/{run_id}/logs/ws
#     """
#     # Принимаем соединение СРАЗУ
#     await websocket.accept()
#     logger.info(f"✅ WebSocket accepted for run_id: {run_id}")
#
#     try:
#         # --- Проверки ---
#         r = await get_run(db, run_id)
#         if not r:
#             await websocket.send_text("Run not found")
#             await websocket.close()
#             return
#
#         if not r.k8s_job_name:
#             await websocket.send_text("No associated k8s job")
#             await websocket.close()
#             return
#
#         job_name = r.k8s_job_name
#
#         # Отправляем информацию о подключении
#         await websocket.send_text(f"Connected to logs for job: {job_name}")
#         await websocket.send_text("Looking for pod...")
#
#         # --- K8s client ---
#         try:
#             config.load_incluster_config()
#         except:
#             config.load_kube_config()
#
#         v1 = client.CoreV1Api()
#
#         # 1. Ждём, пока появится Pod
#         pods = v1.list_namespaced_pod(
#             namespace="default",
#             label_selector=f"job-name={job_name}"
#         )
#
#         if not pods.items:
#             await websocket.send_text("Waiting for pod to be created...")
#             for attempt in range(30):  # Ждем 30 секунд
#                 pods = v1.list_namespaced_pod(
#                     namespace="default",
#                     label_selector=f"job-name={job_name}"
#                 )
#                 if pods.items:
#                     break
#                 await asyncio.sleep(1)
#                 await websocket.send_text(f"Still waiting... ({attempt + 1}/30)")
#
#         if not pods.items:
#             await websocket.send_text("Timeout: Pod not found")
#             await websocket.close()
#             return
#
#         pod_name = pods.items[0].metadata.name
#         await websocket.send_text(f"Found pod: {pod_name}")
#         await websocket.send_text("Starting log stream...")
#
#         # 2. Стрим логов в реальном времени
#         # Используем asyncio для запуска синхронного кода K8s
#         loop = asyncio.get_event_loop()
#
#         def stream_logs_sync():
#             """Синхронная функция для стриминга логов"""
#             w = watch.Watch()
#             try:
#                 # Stream logs
#                 for line in w.stream(
#                     v1.read_namespaced_pod_log,
#                     name=pod_name,
#                     namespace="default",
#                     follow=True,
#                     _preload_content=False
#                 ):
#                     if line:
#                         yield line
#             except Exception as e:
#                 yield f"Error streaming logs: {str(e)}".encode()
#             finally:
#                 w.stop()
#
#         # Запускаем в отдельном потоке
#         import concurrent.futures
#         with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
#             sync_gen = stream_logs_sync()
#
#             while True:
#                 try:
#                     # Получаем следующий chunk через executor
#                     chunk = await loop.run_in_executor(
#                         executor,
#                         lambda: next(sync_gen)
#                     )
#
#                     if isinstance(chunk, bytes):
#                         await websocket.send_text(chunk.decode('utf-8', errors='replace'))
#                     else:
#                         await websocket.send_text(str(chunk))
#
#                 except StopIteration:
#                     break
#                 except Exception as e:
#                     logger.error(f"Error getting log chunk: {e}")
#                     await websocket.send_text(f"Error: {str(e)}")
#                     break
#
#         await websocket.send_text("Log streaming completed")
#
#     except WebSocketDisconnect:
#         logger.info(f"WebSocket disconnected for run {run_id}")
#     except Exception as e:
#         logger.error(f"Error in WebSocket: {e}")
#         try:
#             await websocket.send_text(f"Error: {str(e)}")
#         except:
#             pass
#     finally:
#         try:
#             await websocket.close()
#         except:
#             pass
#
#
# async def stream_pod_logs_async(pod_name: str, namespace: str = "default"):
#     """
#     Асинхронный генератор для стриминга логов из пода
#     """
#     loop = asyncio.get_event_loop()
#
#     # Создаем K8s клиент
#     try:
#         config.load_incluster_config()
#     except:
#         config.load_kube_config()
#
#     v1 = client.CoreV1Api()
#
#     def sync_stream_logs():
#         """Синхронная функция для стриминга логов"""
#         try:
#             # Используем watch с yield для получения логов
#             w = watch.Watch()
#             for event in w.stream(
#                     v1.read_namespaced_pod_log,
#                     name=pod_name,
#                     namespace=namespace,
#                     follow=True,
#                     _preload_content=False,
#                     pretty='false'
#             ):
#                 yield event
#         except Exception as e:
#             yield f"Error streaming logs: {str(e)}".encode()
#
#     # Запускаем синхронный генератор в отдельном потоке
#     import concurrent.futures
#     with concurrent.futures.ThreadPoolExecutor() as pool:
#         # Получаем синхронный генератор
#         sync_gen = sync_stream_logs()
#
#         while True:
#             try:
#                 # Получаем следующий chunk через run_in_executor
#                 chunk = await loop.run_in_executor(
#                     pool,
#                     lambda: next(sync_gen)
#                 )
#
#                 if isinstance(chunk, bytes):
#                     yield chunk
#                 elif isinstance(chunk, str):
#                     yield chunk.encode()
#                 else:
#                     # Если это объект события watch
#                     if hasattr(chunk, 'decode'):
#                         yield chunk
#                     else:
#                         # Пропускаем не-bytes объекты
#                         continue
#
#             except StopIteration:
#                 break
#             except Exception as e:
#                 yield f"Generator error: {str(e)}".encode()
#                 break

# @router.websocket("/runs/{run_id}/logs/ws")
# async def websocket_logs(
#         websocket: WebSocket,
#         run_id: int,
#         db: Annotated[AsyncSession, Depends(db_helper.session_getter)]
# ):
#     await websocket.accept()
#
#     # --- Проверки ---
#     r = await get_run(db, run_id)
#     if not r:
#         await websocket.send_text("Run not found")
#         await websocket.close()
#         return
#
#     if not r.k8s_job_name:
#         await websocket.send_text("No associated k8s job")
#         await websocket.close()
#         return
#
#     job_name = r.k8s_job_name
#
#     # --- K8s client ---
#     await config.load_kube_config()  # или load_incluster_config() — если в кластере
#     v1 = client.CoreV1Api()
#
#     try:
#         # 1. Ждём, пока появится Pod
#         pods = await v1.list_namespaced_pod(
#             namespace="default",
#             label_selector=f"job-name={job_name}"
#         )
#
#         if not pods.items:
#             await websocket.send_text("Waiting for pod to be created…")
#             # Ждем появления
#             while True:
#                 pods = await v1.list_namespaced_pod(
#                     namespace="default",
#                     label_selector=f"job-name={job_name}"
#                 )
#                 if pods.items:
#                     break
#                 await asyncio.sleep(1)
#
#         pod_name = pods.items[0].metadata.name
#         await websocket.send_text(f"Streaming logs from pod: {pod_name}")
#
#         # 2. Стрим логов в реальном времени
#         w = watch.Watch()
#         async for line in w.stream(
#                 v1.read_namespaced_pod_log,
#                 name=pod_name,
#                 namespace="default",
#                 follow=True,
#                 _preload_content=False
#         ):
#             # line будет бинарным — декодируем
#             await websocket.send_text(line.decode("utf-8"))
#
#     except Exception as e:
#         await websocket.send_text(f"Error: {e}")
#
#     finally:
#         await websocket.close()
