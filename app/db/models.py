from datetime import datetime
import uuid

from sqlalchemy import UUID, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from uuid_extensions import uuid7


class Base(DeclarativeBase):
    pass


class ImageValidationResult(Base):
    __tablename__ = "image_validation_result"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid7
    )
    is_blank: Mapped[bool] = mapped_column(default=False)
    is_folded: Mapped[bool] = mapped_column(default=False)
    tilt_angle: Mapped[float] = mapped_column(default=0.0)
    gid: Mapped[uuid.UUID] = mapped_column(default=None)
    message_received_time: Mapped[datetime] = mapped_column(nullable=False)
    file_received_time: Mapped[datetime] = mapped_column(nullable=False)
    created_time: Mapped[datetime] = mapped_column(nullable=False)

    def __repr__(self) -> str:
        return (
            f"ImageValidationResult(id={self.id!r}, is_blank={self.is_blank!r}, "
            f"is_folded={self.is_folded!r}, tilt_angle={self.tilt_angle!r}, gid={self.gid!r})"
        )
