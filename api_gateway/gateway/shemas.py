from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Dict
from enum import Enum


class ScriptStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# class ScriptCreate(BaseModel):
#     name: str = Field(..., min_length=1, max_length=255)
#     script_content: str = Field(..., min_length=1)
#     namespace: Optional[str] = "default"
#     created_by: Optional[str] = None
#
#
# class ScriptResponse(BaseModel):
#     id: int
#     name: str
#     status: ScriptStatus
#     pod_name: Optional[str]
#     exit_code: Optional[int]

class ScriptCreate(BaseModel):
    name: Optional[str]
    script: str
    status: str
    params: dict | None = None


class ScriptOut(BaseModel):
    id: int
    name: str | None
    status: str
    k8s_job_name: str | None


# class LogStreamRequest(BaseModel):
#     script_id: int
#     follow: bool = True

