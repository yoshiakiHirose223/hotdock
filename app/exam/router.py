from fastapi import APIRouter, Form, HTTPException, Request

from app.core.dependencies import build_template_context
from app.exam.service import ExamService

router = APIRouter()
service = ExamService()


@router.get("")
async def exam_index(request: Request):
    context = build_template_context(
        request,
        page_title="Exam",
        questions=service.list_questions(),
    )
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="exam/index.html",
        context=context,
    )


@router.get("/questions/{question_id}")
async def exam_question(request: Request, question_id: int):
    question = service.get_question(question_id)
    if question is None:
        raise HTTPException(status_code=404, detail="Question not found")

    context = build_template_context(
        request,
        page_title=question.title,
        question=question,
        result=None,
    )
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="exam/question.html",
        context=context,
    )


@router.post("/questions/{question_id}")
async def exam_submit_answer(
    request: Request,
    question_id: int,
    selected_label: str = Form(...),
):
    question = service.get_question(question_id)
    if question is None:
        raise HTTPException(status_code=404, detail="Question not found")

    result = service.answer_question(question_id, selected_label)
    context = build_template_context(
        request,
        page_title=question.title,
        question=question,
        result=result,
    )
    return request.app.state.templates.TemplateResponse(
        request=request,
        name="exam/question.html",
        context=context,
    )
