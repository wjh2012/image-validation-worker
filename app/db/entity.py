from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ImageValidation(Base):
    __tablename__ = "image_validation"

    id: Mapped[int] = mapped_column(primary_key=True)
    is_blank: Mapped[bool] = mapped_column(default=False)
    is_folded: Mapped[bool] = mapped_column(default=False)
    tilt_angle: Mapped[float] = mapped_column(default=0)

    def __repr__(self) -> str:
        return f"User(id={self.id!r}, is_blank={self.is_blank!r}, is_folded={self.is_folded!r}, tilt_angle={self.tilt_angle!r})"
