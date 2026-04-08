from typing import Any

from fastapi import APIRouter, File, Form, Request, Response, UploadFile

from app.core.dependencies import build_template_context
from app.tools.service import ToolsService

router = APIRouter()
service = ToolsService()


async def read_tool_input(csv_text: str, upload_file: UploadFile | None) -> str:
    if csv_text.strip():
        return csv_text.strip()
    if upload_file and upload_file.filename:
        content = await upload_file.read()
        return content.decode("utf-8").strip()
    raise ValueError("CSV テキストまたはファイルを入力してください。")


def build_tool_response(
    request: Request,
    template_name: str,
    page_title: str,
    **extra: Any,
):
    context = build_template_context(request, page_title=page_title, **extra)
    return request.app.state.templates.TemplateResponse(
        request=request,
        name=template_name,
        context=context,
    )


@router.get("")
async def tools_index(request: Request):
    return build_tool_response(
        request,
        "tools/index.html",
        "Tools",
        tools=service.list_tools(),
    )


@router.get("/csv-to-json")
async def csv_to_json_form(request: Request):
    return build_tool_response(
        request,
        "tools/csv_to_json.html",
        "CSV to JSON",
    )


@router.get("/csv-column-swap")
async def csv_column_swap_form(request: Request):
    return build_tool_response(
        request,
        "tools/csv_column_swap.html",
        "CSV Column Swap",
        csv_text="",
        first_column="",
        second_column="",
        result=None,
        error=None,
    )


@router.post("/csv-column-swap")
async def csv_column_swap_execute(
    request: Request,
    csv_text: str = Form(default=""),
    first_column: str = Form(default=""),
    second_column: str = Form(default=""),
    action: str = Form(default="preview"),
    upload_file: UploadFile | None = File(default=None),
):
    try:
        input_text = await read_tool_input(csv_text, upload_file)
        result = service.swap_columns(input_text, first_column.strip(), second_column.strip())
        if action == "download":
            headers = {"Content-Disposition": 'attachment; filename="column-swapped.csv"'}
            return Response(content=result, media_type="text/csv", headers=headers)
        return build_tool_response(
            request,
            "tools/csv_column_swap.html",
            "CSV Column Swap",
            csv_text=input_text,
            first_column=first_column,
            second_column=second_column,
            result=result,
            error=None,
        )
    except ValueError as exc:
        return build_tool_response(
            request,
            "tools/csv_column_swap.html",
            "CSV Column Swap",
            csv_text=csv_text,
            first_column=first_column,
            second_column=second_column,
            result=None,
            error=str(exc),
        )
