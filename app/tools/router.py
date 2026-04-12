from typing import Any

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Query, Request, Response, UploadFile, status
from sqlalchemy.orm import Session

from app.core.dependencies import build_template_context
from app.core.database import get_db
from app.tools.conflict_watch_schemas import (
    BranchActionRequest,
    BranchFileIgnoreCreateRequest,
    BranchFileIgnoreMemoUpdateRequest,
    BranchFileIgnoreTargetRequest,
    BranchMemoUpdateRequest,
    ConflictMemoUpdateRequest,
    ConflictStatusUpdateRequest,
    ConflictWatchApiResponse,
    ConflictWatchWebhookAccepted,
    IgnoreRuleCreateRequest,
    RepositoryCreateRequest,
    SettingsUpdateRequest,
    SimulatedWebhookRequest,
    WebhookEventProcessingTraceResponse,
    WebhookEventRawPayloadResponse,
)
from app.tools.conflict_watch_service import ConflictWatchService
from app.tools.service import ToolsService

router = APIRouter()
service = ToolsService()
conflict_watch_service = ConflictWatchService()


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


def build_conflict_watch_response(
    db: Session,
    message: str,
    tone: str = "success",
) -> ConflictWatchApiResponse:
    db.expire_all()
    return ConflictWatchApiResponse(
        message=message,
        tone=tone,
        state=conflict_watch_service.get_state(db),
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


@router.get("/conflict-watch")
async def conflict_watch_form(request: Request):
    response = build_tool_response(
        request,
        "tools/conflict_watch.html",
        "Conflict Watch",
        page_mode="repositories",
        selected_repository_id="",
    )
    response.headers["Cache-Control"] = "no-store"
    return response


@router.get("/conflict-watch/{repository_id:int}")
async def conflict_watch_repository_detail(
    repository_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    repositories = conflict_watch_service.list_repositories(db)
    if not any(repository["id"] == repository_id for repository in repositories):
        raise HTTPException(status_code=404, detail="Repository not found")
    response = build_tool_response(
        request,
        "tools/conflict_watch.html",
        "Conflict Watch Repository",
        page_mode="repository-detail",
        selected_repository_id=str(repository_id),
    )
    response.headers["Cache-Control"] = "no-store"
    return response


@router.get("/conflict-watch/api/state", response_model=dict[str, Any])
async def conflict_watch_state(response: Response, db: Session = Depends(get_db)):
    response.headers["Cache-Control"] = "no-store"
    return conflict_watch_service.get_state(db)


@router.get("/conflict-watch/api/repositories", response_model=list[dict[str, Any]])
async def conflict_watch_repositories(db: Session = Depends(get_db)):
    return conflict_watch_service.list_repositories(db)


@router.post("/conflict-watch/api/repositories", response_model=ConflictWatchApiResponse)
async def conflict_watch_add_repository(
    payload: RepositoryCreateRequest,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.add_repository(
        db,
        provider_type=payload.providerType,
        repository_name=payload.repositoryName.strip(),
        external_repo_id=payload.externalRepoId.strip(),
    )
    return build_conflict_watch_response(db, result.message, result.tone)


@router.post("/conflict-watch/api/repositories/{repository_id}/toggle-active", response_model=ConflictWatchApiResponse)
async def conflict_watch_toggle_repository_active(
    repository_id: int,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.toggle_repository_active(db, repository_id)
    return build_conflict_watch_response(db, result.message, result.tone)


@router.get("/conflict-watch/api/branches", response_model=list[dict[str, Any]])
async def conflict_watch_branches(
    repository_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
):
    return conflict_watch_service.list_branches(db, repository_id)


@router.get("/conflict-watch/api/branches/{branch_id}", response_model=dict[str, Any])
async def conflict_watch_branch_detail(
    branch_id: int,
    db: Session = Depends(get_db),
):
    return conflict_watch_service.get_branch_detail(db, branch_id)


@router.patch("/conflict-watch/api/settings", response_model=ConflictWatchApiResponse)
async def conflict_watch_update_settings(
    payload: SettingsUpdateRequest,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.update_settings(
        db,
        {
            "staleDays": payload.staleDays,
            "longUnresolvedDays": payload.longUnresolvedDays,
            "rawPayloadRetentionDays": payload.rawPayloadRetentionDays,
            "processingTraceEnabled": payload.processingTraceEnabled,
            "forcePushNoteEnabled": payload.forcePushNoteEnabled,
            "suppressNoticeNotifications": payload.suppressNoticeNotifications,
            "notificationDestination": payload.notificationDestination,
            "slackWebhookUrl": payload.slackWebhookUrl,
            "githubWebhookEndpoint": payload.githubWebhookEndpoint,
            "backlogWebhookEndpoint": payload.backlogWebhookEndpoint,
            "githubWebhookSecret": payload.githubWebhookSecret,
            "backlogWebhookSecret": payload.backlogWebhookSecret,
        },
    )
    return build_conflict_watch_response(db, result.message, result.tone)


@router.post("/conflict-watch/api/ignore-rules", response_model=ConflictWatchApiResponse)
async def conflict_watch_add_ignore_rule(
    payload: IgnoreRuleCreateRequest,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.add_ignore_rule(db, payload.repositoryId, payload.pattern)
    return build_conflict_watch_response(db, result.message, result.tone)


@router.post("/conflict-watch/api/ignore-rules/{rule_id}/toggle", response_model=ConflictWatchApiResponse)
async def conflict_watch_toggle_ignore_rule(
    rule_id: int,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.toggle_ignore_rule(db, rule_id)
    return build_conflict_watch_response(db, result.message, result.tone)


@router.patch("/conflict-watch/api/branches/{branch_id}/memo", response_model=ConflictWatchApiResponse)
async def conflict_watch_update_branch_memo(
    branch_id: int,
    payload: BranchMemoUpdateRequest,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.update_branch_memo(db, branch_id, payload.memo)
    return build_conflict_watch_response(db, result.message, result.tone)


@router.post("/conflict-watch/api/branch-file-ignores", response_model=ConflictWatchApiResponse)
async def conflict_watch_add_branch_file_ignore(
    payload: BranchFileIgnoreCreateRequest,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.add_branch_file_ignore(
        db,
        payload.branchId,
        payload.normalizedFilePath,
        payload.memo,
    )
    return build_conflict_watch_response(db, result.message, result.tone)


@router.post("/conflict-watch/api/branch-file-ignores/{ignore_id}/toggle", response_model=ConflictWatchApiResponse)
async def conflict_watch_toggle_branch_file_ignore(
    ignore_id: int,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.toggle_branch_file_ignore(db, ignore_id)
    return build_conflict_watch_response(db, result.message, result.tone)


@router.post("/conflict-watch/api/branch-file-ignores/remove", response_model=ConflictWatchApiResponse)
async def conflict_watch_remove_branch_file_ignore(
    payload: BranchFileIgnoreTargetRequest,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.remove_branch_file_ignore(
        db,
        payload.branchId,
        payload.normalizedFilePath,
    )
    return build_conflict_watch_response(db, result.message, result.tone)


@router.patch("/conflict-watch/api/branch-file-ignores/memo", response_model=ConflictWatchApiResponse)
async def conflict_watch_update_branch_file_ignore_memo(
    payload: BranchFileIgnoreMemoUpdateRequest,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.update_branch_file_ignore_memo(
        db,
        payload.branchId,
        payload.normalizedFilePath,
        payload.memo,
    )
    return build_conflict_watch_response(db, result.message, result.tone)


@router.post("/conflict-watch/api/branches/{branch_id}/actions", response_model=ConflictWatchApiResponse)
async def conflict_watch_branch_action(
    branch_id: int,
    payload: BranchActionRequest,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.apply_branch_action(db, branch_id, payload.action)
    return build_conflict_watch_response(db, result.message, result.tone)


@router.patch("/conflict-watch/api/conflicts/{conflict_id}/memo", response_model=ConflictWatchApiResponse)
async def conflict_watch_update_conflict_memo(
    conflict_id: int,
    payload: ConflictMemoUpdateRequest,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.update_conflict_memo(db, conflict_id, payload.memo)
    return build_conflict_watch_response(db, result.message, result.tone)


@router.get("/conflict-watch/api/conflicts", response_model=list[dict[str, Any]])
async def conflict_watch_conflicts(
    repository_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
):
    return conflict_watch_service.list_conflicts(db, repository_id=repository_id)


@router.get("/conflict-watch/api/conflicts/resolved", response_model=list[dict[str, Any]])
async def conflict_watch_resolved_conflicts(
    repository_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
):
    return conflict_watch_service.list_conflicts(db, repository_id=repository_id, resolved_only=True)


@router.get("/conflict-watch/api/conflicts/{conflict_id}", response_model=dict[str, Any])
async def conflict_watch_conflict_detail(
    conflict_id: int,
    db: Session = Depends(get_db),
):
    return conflict_watch_service.get_conflict_detail(db, conflict_id)


@router.patch("/conflict-watch/api/conflicts/{conflict_id}/status", response_model=ConflictWatchApiResponse)
async def conflict_watch_update_conflict_status(
    conflict_id: int,
    payload: ConflictStatusUpdateRequest,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.update_conflict_status(db, conflict_id, payload.status)
    return build_conflict_watch_response(db, result.message, result.tone)


@router.delete("/conflict-watch/api/conflicts/{conflict_id}", response_model=ConflictWatchApiResponse)
async def conflict_watch_delete_conflict(
    conflict_id: int,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.delete_conflict(db, conflict_id)
    return build_conflict_watch_response(db, result.message, result.tone)


@router.post("/conflict-watch/api/conflicts/{conflict_id}/delete", response_model=ConflictWatchApiResponse)
async def conflict_watch_delete_conflict_post(
    conflict_id: int,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.delete_conflict(db, conflict_id)
    return build_conflict_watch_response(db, result.message, result.tone)


@router.post("/conflict-watch/api/simulate-webhook", response_model=ConflictWatchApiResponse)
async def conflict_watch_simulate_webhook(
    payload: SimulatedWebhookRequest,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.apply_simulated_webhook(db, payload.repositoryId, payload.model_dump())
    return build_conflict_watch_response(db, result.message, result.tone)


@router.post("/conflict-watch/api/webhook-events/{event_id}/reprocess", response_model=ConflictWatchApiResponse)
async def conflict_watch_reprocess_webhook(
    event_id: int,
    db: Session = Depends(get_db),
):
    result = conflict_watch_service.reprocess_webhook_event(db, event_id)
    return build_conflict_watch_response(db, result.message, result.tone)


@router.get("/conflict-watch/api/webhook-events/{event_id}/raw-payload", response_model=WebhookEventRawPayloadResponse)
async def conflict_watch_webhook_raw_payload(
    event_id: int,
    response: Response,
    db: Session = Depends(get_db),
):
    response.headers["Cache-Control"] = "no-store"
    return conflict_watch_service.get_webhook_event_raw_payload(db, event_id)


@router.get("/conflict-watch/api/webhook-events/{event_id}/processing-trace", response_model=WebhookEventProcessingTraceResponse)
async def conflict_watch_webhook_processing_trace(
    event_id: int,
    response: Response,
    db: Session = Depends(get_db),
):
    response.headers["Cache-Control"] = "no-store"
    return conflict_watch_service.get_webhook_event_processing_trace(db, event_id)


@router.post(
    "/conflict-watch/webhooks/github",
    response_model=ConflictWatchWebhookAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def conflict_watch_github_webhook(
    request: Request,
    x_github_delivery: str = Header(...),
    x_github_event: str = Header(...),
    x_hub_signature_256: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    if x_github_event != "push":
        return ConflictWatchWebhookAccepted(message="Ignored non-push GitHub event", delivery_id=x_github_delivery)
    payload_bytes = await request.body()
    result = conflict_watch_service.handle_github_webhook(
        db,
        payload_bytes,
        delivery_id=x_github_delivery,
        signature_header=x_hub_signature_256,
        event_type=x_github_event,
    )
    return ConflictWatchWebhookAccepted(message=result.message, delivery_id=x_github_delivery)


@router.post(
    "/conflict-watch/webhooks/backlog",
    response_model=ConflictWatchWebhookAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def conflict_watch_backlog_webhook(
    request: Request,
    x_backlog_delivery: str | None = Header(default=None),
    x_backlog_secret: str | None = Header(default=None),
    secret: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    payload_bytes = await request.body()
    delivery_id = x_backlog_delivery or f"backlog-{hash(payload_bytes)}"
    result = conflict_watch_service.handle_backlog_webhook(
        db,
        payload_bytes,
        delivery_id=delivery_id,
        provided_secret=x_backlog_secret or secret,
    )
    return ConflictWatchWebhookAccepted(message=result.message, delivery_id=delivery_id)


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
