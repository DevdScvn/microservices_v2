from pydantic import BaseModel
from typing import Optional
from enum import Enum


class ScriptStatus(str, Enum):
    pending = "pending"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class ScriptCreate(BaseModel):
    name: Optional[str]
    script: str
    status: str
    params: Optional[dict] = None


class ScriptOut(BaseModel):
    id: int
    name: Optional[str] = None
    status: str
    k8s_job_name: Optional[str] = None

