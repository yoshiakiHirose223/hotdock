from app.exam.schemas import ExamAnswerResult, ExamQuestionDetail


def evaluate_answer(question: ExamQuestionDetail, selected_label: str) -> ExamAnswerResult:
    normalized_label = selected_label.strip().upper()
    return ExamAnswerResult(
        selected_label=normalized_label,
        is_correct=normalized_label == question.correct_label,
        explanation=question.explanation,
    )
