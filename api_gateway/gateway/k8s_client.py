import os
import asyncio
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
import concurrent.futures
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

NAMESPACE = os.getenv("K8S_NAMESPACE", "default")

# Инициализация клиента (делаем один раз)
try:
    config.load_incluster_config()
except Exception:
    config.load_kube_config()

batch_v1 = client.BatchV1Api()
core_v1 = client.CoreV1Api()


def build_job_object(job_name: str, script: str, image: str = "alpine:3.18") -> client.V1Job:
    command = [
        "/bin/sh",
        "-c",
        f"cat > /workspace/script.sh <<'SCRIPT'\n{script}\nSCRIPT\nsh /workspace/script.sh"
    ]

    container = client.V1Container(
        name="runner",
        image=image,
        command=command,
        volume_mounts=[client.V1VolumeMount(mount_path="/workspace", name="workspace")],
        resources=client.V1ResourceRequirements(limits={"cpu": "0.5", "memory": "256Mi"})
    )
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"job-name": job_name}),
        spec=client.V1PodSpec(
            containers=[container],
            restart_policy="Never",
            volumes=[client.V1Volume(name="workspace", empty_dir=client.V1EmptyDirVolumeSource())],
            security_context=client.V1PodSecurityContext(
                run_as_non_root=True,
                run_as_user=1000
            )
        )
    )
    job_spec = client.V1JobSpec(
        template=template,
        backoff_limit=0,
        # Убираем или увеличиваем TTL для отладки
        # ttl_seconds_after_finished=300  # Удалить через 5 минут
    )
    job = client.V1Job(
        metadata=client.V1ObjectMeta(name=job_name),
        spec=job_spec
    )
    return job


async def create_job_async(job_name: str, script: str, image: str = "alpine:3.18"):
    """Асинхронное создание Job с ожиданием Pod"""
    job = build_job_object(job_name, script, image)
    loop = asyncio.get_event_loop()

    try:
        # Создаем Job
        result = await loop.run_in_executor(
            None,
            lambda: batch_v1.create_namespaced_job(
                namespace=NAMESPACE,
                body=job
            )
        )

        logger.info(f"✅ Job {job_name} created")

        # Ждем появления Pod (до 30 секунд)
        pod_name = None
        for attempt in range(30):
            await asyncio.sleep(1)
            pods = await loop.run_in_executor(
                None,
                lambda: core_v1.list_namespaced_pod(
                    namespace=NAMESPACE,
                    label_selector=f"job-name={job_name}"
                )
            )
            if pods.items:
                pod_name = pods.items[0].metadata.name
                pod_status = pods.items[0].status.phase
                logger.info(f"✅ Pod {pod_name} created, status: {pod_status}")
                break

            if attempt % 5 == 0:
                logger.info(f"⏳ Waiting for pod... ({attempt + 1}/30)")

        if not pod_name:
            logger.warning(f"⚠️ Pod not found for job {job_name} after 30 seconds")

        return result, pod_name

    except Exception as e:
        logger.error(f"❌ Error creating job {job_name}: {e}")
        raise


async def monitor_job_completion(job_name: str, timeout: int = 300):
    """Мониторит завершение Job и возвращает статус"""
    loop = asyncio.get_event_loop()

    def sync_monitor():
        w = watch.Watch()
        try:
            for event in w.stream(
                    batch_v1.list_namespaced_job,
                    namespace=NAMESPACE,
                    field_selector=f"metadata.name={job_name}",
                    timeout_seconds=timeout
            ):
                job = event['object']

                if job.status.succeeded:
                    w.stop()
                    return "succeeded", 0
                elif job.status.failed:
                    w.stop()
                    # Пытаемся получить exit code из Pod
                    return "failed", 1

        except Exception as e:
            logger.error(f"Monitor error for {job_name}: {e}")
            return "error", -1
        finally:
            w.stop()

        return "timeout", -1

    return await loop.run_in_executor(None, sync_monitor)


async def get_pod_for_job(job_name: str):
    """Получает информацию о Pod для Job"""
    loop = asyncio.get_event_loop()

    try:
        pods = await loop.run_in_executor(
            None,
            lambda: core_v1.list_namespaced_pod(
                namespace=NAMESPACE,
                label_selector=f"job-name={job_name}"
            )
        )

        if pods.items:
            pod = pods.items[0]
            return {
                "name": pod.metadata.name,
                "status": pod.status.phase,
                "creation_time": pod.metadata.creation_timestamp,
                "container_status": pod.status.container_statuses[0] if pod.status.container_statuses else None
            }
        return None

    except Exception as e:
        logger.error(f"Error getting pod for job {job_name}: {e}")
        return None


async def get_pod_logs(pod_name: str, tail_lines: int = 100, follow: bool = False):
    """Получает логи Pod"""
    loop = asyncio.get_event_loop()

    try:
        if follow:
            # Для стриминга в реальном времени
            def sync_stream():
                w = watch.Watch()
                try:
                    for event in w.stream(
                            core_v1.read_namespaced_pod_log,
                            name=pod_name,
                            namespace=NAMESPACE,
                            follow=True,
                            _preload_content=False,
                            tail_lines=tail_lines
                    ):
                        if event:
                            yield event.decode('utf-8', errors='replace')
                except Exception as e:
                    yield f"Error streaming logs: {str(e)}"
                finally:
                    w.stop()

            return sync_stream()

        else:
            # Для получения логов без follow
            logs = await loop.run_in_executor(
                None,
                lambda: core_v1.read_namespaced_pod_log(
                    name=pod_name,
                    namespace=NAMESPACE,
                    tail_lines=tail_lines
                )
            )
            return logs

    except client.rest.ApiException as e:
        if e.status == 404:
            return f"Logs not available (Pod may have been deleted)"
        raise
    except Exception as e:
        logger.error(f"Error getting logs for pod {pod_name}: {e}")
        raise


async def get_job_status(job_name: str):
    """Получает статус Job"""
    loop = asyncio.get_event_loop()

    try:
        job = await loop.run_in_executor(
            None,
            lambda: batch_v1.read_namespaced_job(job_name, namespace=NAMESPACE)
        )

        if job.status.succeeded:
            return "succeeded"
        if job.status.failed:
            return "failed"
        if job.status.active:
            return "running"
        return "pending"
    except ApiException as e:
        logger.error(f"API Exception for job {job_name}: {e}")
        return "unknown"
    except Exception as e:
        logger.error(f"Error getting job status {job_name}: {e}")
        return "unknown"


async def save_job_logs_to_db(db_session, script_id: int, job_name: str):
    """Сохраняет логи Job в БД"""
    from sqlalchemy import update
    from models import Script

    try:
        # Получаем Pod
        pod_info = await get_pod_for_job(job_name)
        if not pod_info:
            logger.warning(f"No pod found for job {job_name}")
            return

        # Пытаемся получить логи
        logs = await get_pod_logs(pod_info["name"], tail_lines=500, follow=False)

        if logs and not isinstance(logs, str):  # Если это не сообщение об ошибке
            # Обновляем запись в БД
            stmt = (
                update(Script)
                .where(Script.id == script_id)
                .values(logs=logs)
            )
            await db_session.execute(stmt)
            await db_session.commit()
            logger.info(f"✅ Saved logs for script {script_id}, length: {len(logs)}")
        else:
            logger.warning(f"Could not get logs for script {script_id}: {logs}")

    except Exception as e:
        logger.error(f"Error saving logs for script {script_id}: {e}")
        # Не пробрасываем исключение, чтобы не ломать основной поток
