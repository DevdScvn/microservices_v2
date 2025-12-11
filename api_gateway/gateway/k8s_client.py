import os
import uuid
import asyncio
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
import concurrent.futures
import logging

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
        spec=client.V1PodSpec(
            containers=[container],
            restart_policy="Never",
            volumes=[client.V1Volume(name="workspace", empty_dir=client.V1EmptyDirVolumeSource())],
            security_context=client.V1PodSecurityContext()
        )
    )
    job_spec = client.V1JobSpec(template=template, backoff_limit=0)
    job = client.V1Job(metadata=client.V1ObjectMeta(name=job_name), spec=job_spec)
    return job


async def create_job_async(job_name: str, script: str):
    """Асинхронное создание Job"""
    job = build_job_object(job_name, script)
    loop = asyncio.get_event_loop()

    try:
        return await loop.run_in_executor(
            None,
            lambda: batch_v1.create_namespaced_job(
                namespace=NAMESPACE,
                body=job
            )
        )
    except Exception as e:
        logger.error(f"Error creating job {job_name}: {e}")
        raise


async def wait_for_pod_for_job(job_name: str, timeout: int = 60) -> str | None:
    """Асинхронное ожидание появления Pod для Job"""
    loop = asyncio.get_event_loop()

    def sync_wait():
        w = watch.Watch()
        try:
            for event in w.stream(
                    core_v1.list_namespaced_pod,
                    namespace=NAMESPACE,
                    label_selector=f"job-name={job_name}",
                    timeout_seconds=timeout
            ):
                pod = event['object']
                if pod.metadata and pod.metadata.name:
                    w.stop()
                    return pod.metadata.name
        except Exception as e:
            logger.error(f"Error waiting for pod: {e}")
            return None
        return None

    return await loop.run_in_executor(None, sync_wait)


async def stream_pod_logs(job_name: str):
    """
    Async generator which yields log chunks (bytes) for the pod created by the given job_name.
    Исправленная версия с правильной асинхронностью.
    """
    # Ждём появления pod
    pod_name = await wait_for_pod_for_job(job_name, timeout=60)
    if not pod_name:
        yield b"Error: No pod found for job"
        return

    logger.info(f"Streaming logs from pod: {pod_name}")

    loop = asyncio.get_event_loop()

    def sync_log_generator():
        """Синхронный генератор логов"""
        w = watch.Watch()
        try:
            # Получаем логи через watch
            for line in w.stream(
                    core_v1.read_namespaced_pod_log,
                    name=pod_name,
                    namespace=NAMESPACE,
                    follow=True,
                    _preload_content=False,
                    pretty='false'
            ):
                if isinstance(line, bytes):
                    yield line
                else:
                    # Если пришёл не bytes, пробуем преобразовать
                    try:
                        yield str(line).encode('utf-8')
                    except:
                        continue
        except Exception as e:
            yield f"Error in log stream: {str(e)}".encode()
        finally:
            w.stop()

    # Запускаем синхронный генератор в потоке
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        # Получаем синхронный генератор
        sync_gen = sync_log_generator()

        while True:
            try:
                # Получаем следующий элемент через executor
                chunk = await loop.run_in_executor(
                    executor,
                    lambda: next(sync_gen)
                )
                yield chunk
            except StopIteration:
                break
            except Exception as e:
                yield f"Error getting next chunk: {str(e)}".encode()
                break


async def get_job_status(job_name: str):
    """Асинхронное получение статуса Job"""
    loop = asyncio.get_event_loop()

    try:
        job = await loop.run_in_executor(
            None,
            lambda: batch_v1.read_namespaced_job(job_name, namespace=NAMESPACE)
        )

        if job.status.succeeded and job.status.succeeded > 0:
            return "succeeded"
        if job.status.failed and job.status.failed > 0:
            return "failed"
        if job.status.active and job.status.active > 0:
            return "running"
        return "pending"
    except ApiException as e:
        logger.error(f"API Exception for job {job_name}: {e}")
        return "unknown"
    except Exception as e:
        logger.error(f"Error getting job status {job_name}: {e}")
        return "unknown"



# import os
# import uuid
# import asyncio
# from kubernetes import client, config, watch
# from kubernetes.client.rest import ApiException
#
# NAMESPACE = os.getenv("K8S_NAMESPACE", "default")
#
# # try in-cluster, else kubeconfig
# try:
#     config.load_incluster_config()
# except Exception:
#     config.load_kube_config()
#
# batch_v1 = client.BatchV1Api()
# core_v1 = client.CoreV1Api()
#
#
# # helper to create a simple Job which writes the script into a file and runs it
# def build_job_object(job_name: str, script: str, image: str = "alpine:3.18") -> client.V1Job:
#     command = [
#         "/bin/sh",
#         "-c",
#         f"cat > /workspace/script.sh <<'SCRIPT'\n{script}\nSCRIPT\nsh /workspace/script.sh"
#     ]
#
#     container = client.V1Container(
#         name="runner",
#         image=image,
#         command=command,
#         volume_mounts=[client.V1VolumeMount(mount_path="/workspace", name="workspace")],
#         resources=client.V1ResourceRequirements(limits={"cpu": "0.5", "memory": "256Mi"})
#     )
#     template = client.V1PodTemplateSpec(
#         spec=client.V1PodSpec(
#             containers=[container],
#             restart_policy="Never",
#             volumes=[client.V1Volume(name="workspace", empty_dir=client.V1EmptyDirVolumeSource())],
#             security_context=client.V1PodSecurityContext()
#         )
#     )
#     job_spec = client.V1JobSpec(template=template, backoff_limit=0)
#     job = client.V1Job(metadata=client.V1ObjectMeta(name=job_name), spec=job_spec)
#     return job
#
#
# async def create_job_async(job_name: str, script: str):
#     job = build_job_object(job_name, script)
#     # kubernetes client is blocking — run in thread
#     loop = asyncio.get_running_loop()
#     return await loop.run_in_executor(None, lambda: batch_v1.create_namespaced_job(namespace=NAMESPACE, body=job))
#
#
# async def wait_for_pod_for_job(job_name: str, timeout: int = 60) -> str | None:
#     loop = asyncio.get_running_loop()
#     found = await loop.run_in_executor(None, lambda: _wait_for_pod_blocking(job_name, timeout))
#     return found
#
#
# def _wait_for_pod_blocking(job_name: str, timeout: int = 60) -> str | None:
#     w = watch.Watch()
#     try:
#         for event in w.stream(core_v1.list_namespaced_pod, namespace=NAMESPACE, label_selector=f"job-name={job_name}",
#                               timeout_seconds=timeout):
#             pod = event['object']
#         # accept pod once it appears (Pending/Running)
#             if pod.metadata and pod.metadata.name:
#                 return pod.metadata.name
#     except Exception:
#         return None
#     return None
#
#
# async def stream_pod_logs(job_name: str):
#     """
#     Async generator which yields log chunks (bytes) for the pod created by the given job_name.
#     """
#     pod_name = await wait_for_pod_for_job(job_name, timeout=60)
#     if not pod_name:
#         yield "".encode()
#         return
#
#     # stream the pod logs using blocking API in thread
#     loop = asyncio.get_running_loop()
#
#     def blocking_stream():
#         try:
#             resp = core_v1.read_namespaced_pod_log(name=pod_name, namespace=NAMESPACE, follow=True,
#                                                    _preload_content=False)
#             # resp is urllib3.HTTPResponse-like: use stream()
#             for chunk in resp.stream(1024):
#                 if not chunk:
#                     continue
#                 yield chunk
#         except ApiException as e:
#             yield f"ERROR: {e}".encode()
#         except Exception as e:
#             yield f"EXCEPTION: {e}".encode()
#
#     # run the generator in a thread, forwarding yielded chunks
#     gen = blocking_stream()
#     # can't iterate blocking generator directly in event loop — pull via run_in_executor per-chunk
#     try:
#         for chunk in gen:
#             yield chunk
#     except Exception:
#         return
#
#
# async def get_job_status(job_name: str):
#     loop = asyncio.get_running_loop()
#     try:
#         job = await loop.run_in_executor(None, lambda: batch_v1.read_namespaced_job(job_name, namespace=NAMESPACE))
#         if job.status.succeeded and job.status.succeeded > 0:
#             return "succeeded"
#         if job.status.failed and job.status.failed > 0:
#             return "failed"
#         return "running"
#     except ApiException:
#         return "unknown"
