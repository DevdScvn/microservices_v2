import enum


from sqlalchemy import String, Enum, Text
from sqlalchemy.dialects.postgresql import JSONB

from sqlalchemy.orm import Mapped, mapped_column

from database import TimeStampModel


class ScriptStatus(enum.Enum):
    pending = "pending"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class Script(TimeStampModel):
    __tablename__ = "scripts"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    status: Mapped[ScriptStatus] = mapped_column(
        Enum(ScriptStatus, name="scriptstatus", create_type=False)
    )
    k8s_job_name: Mapped[str] = mapped_column(String(256), nullable=True)
    params: Mapped[dict] = mapped_column(JSONB)
    exit_code: Mapped[int] = mapped_column(nullable=True)
    logs: Mapped[dict] = mapped_column(JSONB, nullable=True)
    pod_name: Mapped[str] = mapped_column(nullable=True)




