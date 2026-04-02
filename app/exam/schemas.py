from pydantic import BaseModel


class ExamChoiceSchema(BaseModel):
    label: str
    text: str


class ExamQuestionSummary(BaseModel):
    id: int
    title: str
    prompt_preview: str


class ExamQuestionDetail(ExamQuestionSummary):
    explanation: str
    choices: list[ExamChoiceSchema]
    correct_label: str


class ExamAnswerResult(BaseModel):
    selected_label: str
    is_correct: bool
    explanation: str
