import logging
import asyncio
from fastapi import APIRouter, WebSocket, Depends, BackgroundTasks
from typing import Annotated
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func
from kubernetes import client, watch, config
from starlette.websockets import WebSocketDisconnect

from database import db_helper
from gateway.router import get_script_for_id
from gateway.crud import ScriptCrud
from gateway.k8s_client import get_pod_for_job, get_pod_logs, get_job_status, monitor_job_completion, save_job_logs_to_db

router = APIRouter()
logger = logging.getLogger(__name__)


async def monitor_and_update_job(job_name: str, script_id: int, db: AsyncSession):
    """Фоновая задача для мониторинга Job и обновления статуса"""
    try:
        # Мониторим завершение Job
        status, exit_code = await monitor_job_completion(job_name, timeout=600)

        logger.info(f"Job {job_name} completed with status: {status}, exit_code: {exit_code}")

        # Обновляем статус в БД
        await ScriptCrud.update_run_status(
            db=db,
            run_id=script_id,
            status=status,
            exit_code=exit_code
        )

        # Сохраняем логи в БД
        await save_job_logs_to_db(db, script_id, job_name)

    except Exception as e:
        logger.error(f"Error monitoring job {job_name}: {e}")
        # В случае ошибки обновляем статус на failed
        await ScriptCrud.update_run_status(
            db=db,
            run_id=script_id,
            status="failed",
            exit_code=-1
        )


@router.websocket("/runs/{run_id}/logs/ws")
async def websocket_logs_fixed(
        websocket: WebSocket,
        run_id: int,
        db: Annotated[AsyncSession, Depends(db_helper.session_getter)]
):
    await websocket.accept()

    try:
        # Получаем информацию о скрипте
        script = await get_script_for_id(db, run_id)
        if not script:
            await websocket.send_text("❌ Script not found\n")
            await websocket.close()
            return

        if not script.k8s_job_name:
            await websocket.send_text("❌ No Kubernetes job associated\n")
            await websocket.close()
            return

        job_name = script.k8s_job_name

        await websocket.send_text(f"📡 Connected to logs for job: {job_name}\n")

        # Загружаем K8s конфиг
        try:
            config.load_incluster_config()
        except:
            config.load_kube_config()

        # Получаем информацию о Pod
        pod_info = await get_pod_for_job(job_name)

        if not pod_info:
            await websocket.send_text("❌ Pod not found. Job may not have started yet.\n")

            # Пробуем получить статус Job
            job_status = await get_job_status(job_name)
            await websocket.send_text(f"Job status: {job_status}\n")

            await websocket.close()
            return

        pod_name = pod_info["name"]
        pod_status = pod_info["status"]

        await websocket.send_text(f"✅ Pod found: {pod_name}\n")
        await websocket.send_text(f"📊 Pod status: {pod_status}\n")

        # Обновляем статус в БД если нужно
        if script.status != pod_status.lower():
            await ScriptCrud.update_run_status(
                db=db,
                run_id=run_id,
                status=pod_status.lower(),
                exit_code=None
            )

        # Проверяем, есть ли уже логи в БД
        if script.logs:
            await websocket.send_text("📄 Found saved logs in database:\n")
            await websocket.send_text("=" * 60 + "\n")
            await websocket.send_text(script.logs)
            await websocket.send_text("=" * 60 + "\n")
            await websocket.send_text("✅ End of saved logs\n")

        # В зависимости от статуса Pod
        if pod_status in ["Succeeded", "Failed", "Completed"]:
            # Pod завершен
            await websocket.send_text(f"📄 Pod is {pod_status}. Reading logs...\n")

            # Если логи еще не сохранены в БД, пытаемся получить из K8s
            if not script.logs:
                try:
                    logs = await get_pod_logs(pod_name, tail_lines=200, follow=False)

                    if logs and not logs.startswith("Logs not available"):
                        await websocket.send_text("✅ Logs retrieved from Kubernetes:\n")
                        await websocket.send_text("=" * 60 + "\n")
                        await websocket.send_text(logs)
                        await websocket.send_text("=" * 60 + "\n")

                        # Сохраняем логи в БД
                        from sqlalchemy import update
                        from gateway.models import Script
                        stmt = (
                            update(Script)
                            .where(Script.id == run_id)
                            .values(logs=logs)
                        )
                        await db.execute(stmt)
                        await db.commit()
                    else:
                        await websocket.send_text(f"⚠️ {logs}\n")

                except Exception as e:
                    await websocket.send_text(f"❌ Error reading logs: {str(e)[:200]}\n")

            # Показываем информацию о контейнере
            if pod_info.get("container_status"):
                cs = pod_info["container_status"]
                if cs.state.terminated:
                    await websocket.send_text(f"📦 Container terminated:\n")
                    await websocket.send_text(f"   Exit code: {cs.state.terminated.exit_code}\n")
                    await websocket.send_text(f"   Reason: {cs.state.terminated.reason}\n")
                    if cs.state.terminated.message:
                        await websocket.send_text(f"   Message: {cs.state.terminated.message}\n")

        elif pod_status in ["Running", "Pending"]:
            # Pod еще работает - стримим в реальном времени
            await websocket.send_text(f"🎥 Pod is {pod_status}. Starting real-time log stream...\n")
            await websocket.send_text("(Press Ctrl+C or close window to stop)\n")
            await websocket.send_text("=" * 60 + "\n")

            try:
                # Получаем поток логов
                log_stream = await get_pod_logs(pod_name, tail_lines=10, follow=True)

                # Стримим логи
                for log_chunk in log_stream:
                    await websocket.send_text(log_chunk)

                    # Проверяем, не завершился ли Pod
                    current_pod_info = await get_pod_for_job(job_name)
                    if current_pod_info and current_pod_info["status"] in ["Succeeded", "Failed"]:
                        await websocket.send_text(f"\n🏁 Pod {current_pod_info['status']}\n")
                        break

            except Exception as e:
                await websocket.send_text(f"\n❌ Stream error: {str(e)[:200]}\n")

        else:
            await websocket.send_text(f"❓ Unknown pod status: {pod_status}\n")

        # Заключительное сообщение
        await websocket.send_text("\n" + "=" * 60 + "\n")
        await websocket.send_text("🏁 Log streaming finished\n")
        await websocket.send_text(f"📊 Final status: {pod_status}\n")

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for run {run_id}")
    except Exception as e:
        logger.error(f"Error in WebSocket: {e}")
        try:
            await websocket.send_text(f"❌ Error: {str(e)[:200]}\n")
        except:
            pass
    finally:
        try:
            await websocket.close()
        except:
            pass


# Альтернативный упрощенный endpoint для тестирования
@router.websocket("/ws/test-stream/{run_id}")
async def test_websocket_stream(websocket: WebSocket, run_id: int):
    """Упрощенный тестовый WebSocket"""
    await websocket.accept()

    try:
        await websocket.send_text(f"✅ Test WebSocket connected for run {run_id}\n")
        await websocket.send_text("This is a test stream without K8s dependencies\n")

        for i in range(10):
            await asyncio.sleep(1)
            await websocket.send_text(f"[{i + 1}/10] Test message at {asyncio.get_event_loop().time()}\n")

        await websocket.send_text("✅ Test completed!\n")

    except Exception as e:
        logger.error(f"Test WebSocket error: {e}")
    finally:
        await websocket.close()
