import logging

from fastapi import FastAPI, APIRouter, WebSocket, Depends
from typing import Annotated
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
from kubernetes import client, watch, config
from starlette.websockets import WebSocketDisconnect

from database import db_helper
from gateway.router import get_run

router = APIRouter()

logger = logging.getLogger(__name__)


@router.websocket("/runs/{run_id}/logs/ws")
async def websocket_logs_complete(
        websocket: WebSocket,
        run_id: int,
        db: Annotated[AsyncSession, Depends(db_helper.session_getter)]
):
    await websocket.accept()

    try:
        # --- Проверки ---
        r = await get_run(db, run_id)
        if not r:
            await websocket.send_text("Run not found")
            await websocket.close()
            return

        if not r.k8s_job_name:
            await websocket.send_text("No associated k8s job")
            await websocket.close()
            return

        job_name = r.k8s_job_name

        await websocket.send_text(f"Connected to logs for job: {job_name}")

        # --- K8s client ---
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        v1 = client.CoreV1Api()
        batch_v1 = client.BatchV1Api()

        # 1. Ищем Pod
        pods = v1.list_namespaced_pod(
            namespace="default",
            label_selector=f"job-name={job_name}"
        )

        if not pods.items:
            await websocket.send_text("Pod not found. Job may not have started yet.")
            await websocket.close()
            return

        pod = pods.items[0]
        pod_name = pod.metadata.name
        pod_status = pod.status.phase

        await websocket.send_text(f"Pod: {pod_name}")
        await websocket.send_text(f"Status: {pod_status}")

        # 2. В зависимости от статуса читаем логи по-разному
        if pod_status in ["Succeeded", "Failed", "Completed"]:
            # Pod завершен - читаем все логи
            await websocket.send_text(f"Pod is {pod_status}. Reading stored logs...")

            try:
                # Получаем все логи
                logs = v1.read_namespaced_pod_log(
                    name=pod_name,
                    namespace="default",
                    tail_lines=100  # Последние 100 строк
                )

                # Отправляем логи порциями (чтобы не перегружать WebSocket)
                lines = logs.split('\n')
                for i, line in enumerate(lines):
                    if line.strip():  # Пропускаем пустые строки
                        await websocket.send_text(f"{line}\n")

                    # Делаем небольшую паузу каждые 10 строк
                    if i % 10 == 0:
                        await asyncio.sleep(0.01)

                await websocket.send_text(f"\n{'=' * 50}")
                await websocket.send_text(f"End of logs (Pod {pod_status})")

                # Также показываем статус Job
                try:
                    job = batch_v1.read_namespaced_job(job_name, namespace="default")
                    if job.status.succeeded:
                        await websocket.send_text(f"✅ Job {job_name} succeeded")
                    elif job.status.failed:
                        await websocket.send_text(f"❌ Job {job_name} failed")
                except:
                    pass

            except Exception as e:
                await websocket.send_text(f"Error reading logs: {str(e)}")

        elif pod_status in ["Running", "Pending"]:
            # Pod еще работает - стримим в реальном времени
            await websocket.send_text(f"Pod is {pod_status}. Streaming logs...")

            # Создаем асинхронный генератор для логов
            async def stream_logs_realtime():
                loop = asyncio.get_event_loop()

                def sync_stream():
                    w = watch.Watch()
                    try:
                        for event in w.stream(
                                v1.read_namespaced_pod_log,
                                name=pod_name,
                                namespace="default",
                                follow=True,
                                _preload_content=False,
                                tail_lines=10
                        ):
                            if event:
                                yield event
                    except Exception as e:
                        yield f"Error: {str(e)}".encode()
                    finally:
                        w.stop()

                # Запускаем в отдельном потоке
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    sync_gen = sync_stream()

                    while True:
                        try:
                            chunk = await loop.run_in_executor(
                                executor,
                                lambda: next(sync_gen)
                            )
                            yield chunk
                        except StopIteration:
                            break
                        except Exception as e:
                            yield f"Stream error: {str(e)}".encode()
                            break

            # Стримим логи
            async for chunk in stream_logs_realtime():
                if isinstance(chunk, bytes):
                    try:
                        await websocket.send_text(chunk.decode('utf-8', errors='replace'))
                    except:
                        pass
                else:
                    await websocket.send_text(str(chunk))

                # Проверяем, не завершился ли Pod
                try:
                    current_pod = v1.read_namespaced_pod(pod_name, "default")
                    if current_pod.status.phase in ["Succeeded", "Failed"]:
                        await websocket.send_text(f"\nPod {current_pod.status.phase}")
                        break
                except:
                    pass

        else:
            # Неизвестный статус
            await websocket.send_text(f"Pod status '{pod_status}' not supported for log streaming")

        await websocket.send_text("\nLog streaming finished")

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for run {run_id}")
    except Exception as e:
        logger.error(f"Error in WebSocket: {e}")
        try:
            await websocket.send_text(f"Error: {str(e)}")
        except:
            pass
    finally:
        try:
            await websocket.close()
        except:
            pass
