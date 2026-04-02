from app.exam.logic.question_logic import evaluate_answer
from app.exam.schemas import (
    ExamAnswerResult,
    ExamChoiceSchema,
    ExamQuestionDetail,
    ExamQuestionSummary,
)


class ExamService:
    def __init__(self) -> None:
        self._questions = {
            1: ExamQuestionDetail(
                id=1,
                title="HTTP メソッドの基礎",
                prompt_preview="GET と POST の用途の違いを確認する基礎問題。",
                explanation="GET は取得、POST は新規作成や副作用を伴う送信で使うのが基本です。",
                correct_label="B",
                choices=[
                    ExamChoiceSchema(label="A", text="GET は常にデータ更新に使う。"),
                    ExamChoiceSchema(label="B", text="POST は新規登録や更新要求で使われることがある。"),
                    ExamChoiceSchema(label="C", text="DELETE はデータ取得専用である。"),
                ],
            ),
            2: ExamQuestionDetail(
                id=2,
                title="インデックスの基本",
                prompt_preview="SQL の検索性能に関する初歩問題。",
                explanation="検索頻度の高い列に適切なインデックスを張ると探索コストを下げられます。",
                correct_label="A",
                choices=[
                    ExamChoiceSchema(label="A", text="検索頻度の高い列にはインデックスが有効な場合がある。"),
                    ExamChoiceSchema(label="B", text="インデックスを増やすと更新系クエリは必ず高速化する。"),
                    ExamChoiceSchema(label="C", text="小規模テーブルでは主キー以外の検索はできない。"),
                ],
            ),
        }

    def list_questions(self) -> list[ExamQuestionSummary]:
        return [
            ExamQuestionSummary(
                id=question.id,
                title=question.title,
                prompt_preview=question.prompt_preview,
            )
            for question in self._questions.values()
        ]

    def get_question(self, question_id: int) -> ExamQuestionDetail | None:
        return self._questions.get(question_id)

    def answer_question(self, question_id: int, selected_label: str) -> ExamAnswerResult | None:
        question = self.get_question(question_id)
        if question is None:
            return None
        return evaluate_answer(question, selected_label)
