import enum


from sqlalchemy import String, Enum
from sqlalchemy.dialects.postgresql import JSONB

from sqlalchemy.orm import Mapped, mapped_column

from database import Base


class ScriptStatus(enum.Enum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"


class Script(Base):
    __tablename__ = "scripts"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    status: Mapped[ScriptStatus] = mapped_column(
        Enum(ScriptStatus, name="scriptstatus", create_type=False)
    )
    k8s_job_name: Mapped[str] = mapped_column(String(256), nullable=True)
    params: Mapped[dict] = mapped_column(JSONB)
    exit_code: Mapped[int] = mapped_column(nullable=True)



