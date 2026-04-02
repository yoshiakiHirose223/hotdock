from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.models.base import Base


class ExamUser(Base):
    __tablename__ = "exam_users"

    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    attempts: Mapped[list["StudyAttempt"]] = relationship(back_populates="user")


class ExamQuestion(Base):
    __tablename__ = "exam_questions"

    id: Mapped[int] = mapped_column(primary_key=True)
    slug: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(255))
    prompt: Mapped[str] = mapped_column(Text)
    explanation: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    choices: Mapped[list["ExamChoice"]] = relationship(back_populates="question")
    attempts: Mapped[list["StudyAttempt"]] = relationship(back_populates="question")


class ExamChoice(Base):
    __tablename__ = "exam_choices"

    id: Mapped[int] = mapped_column(primary_key=True)
    question_id: Mapped[int] = mapped_column(ForeignKey("exam_questions.id"))
    label: Mapped[str] = mapped_column(String(10))
    text: Mapped[str] = mapped_column(Text)
    is_correct: Mapped[bool] = mapped_column(Boolean, default=False)

    question: Mapped["ExamQuestion"] = relationship(back_populates="choices")
    attempts: Mapped[list["StudyAttempt"]] = relationship(back_populates="selected_choice")


class StudyAttempt(Base):
    __tablename__ = "study_attempts"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("exam_users.id"), nullable=True)
    question_id: Mapped[int] = mapped_column(ForeignKey("exam_questions.id"))
    selected_choice_id: Mapped[int | None] = mapped_column(ForeignKey("exam_choices.id"), nullable=True)
    is_correct: Mapped[bool] = mapped_column(Boolean, default=False)
    submitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    user: Mapped[ExamUser | None] = relationship(back_populates="attempts")
    question: Mapped[ExamQuestion] = relationship(back_populates="attempts")
    selected_choice: Mapped[ExamChoice | None] = relationship(back_populates="attempts")
