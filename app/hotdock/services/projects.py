from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
from urllib.parse import quote

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.hotdock.data.projects import (
    BRANCH_STATUS_STYLES,
    CHANGE_TYPE_STYLES,
    NOW,
    PROJECT_BRANCHES,
    PROJECT_SETTINGS,
    PROJECT_STATUS_STYLES,
    PROJECTS,
)
from app.models.project_bookmark import ProjectBookmark

DEFAULT_WORKSPACE_ID = "default-workspace"
SIDEBAR_BOOKMARK_LIMIT = 5
ACTIVE_BRANCH_WINDOW = timedelta(days=7)
RECENT_BRANCH_WINDOW = timedelta(hours=24)

PROJECTS_BY_ID = {project["id"]: project for project in PROJECTS}


def _format_datetime(value: datetime | None) -> str:
    if value is None:
        return "未観測"
    return value.strftime("%Y-%m-%d %H:%M")


def _is_active_branch(last_push_at: datetime | None) -> bool:
    return last_push_at is not None and last_push_at >= NOW - ACTIVE_BRANCH_WINDOW


def _normalize_path(path: str) -> str:
    return quote(path, safe="/._-")


def _project_urls(project_id: int) -> dict[str, str]:
    return {
        "overview": f"/app/projects/{project_id}",
        "branches": f"/app/projects/{project_id}/branches",
        "conflicts": f"/app/projects/{project_id}/conflicts",
        "settings": f"/app/projects/{project_id}/settings",
    }


def list_projects_view() -> list[dict[str, object]]:
    projects: list[dict[str, object]] = []
    for raw in PROJECTS:
        urls = _project_urls(raw["id"])
        projects.append(
            {
                **raw,
                "owner": raw["owner_name"],
                "repo": str(raw["repository_url"]).removeprefix("https://"),
                "detail_url": urls["overview"],
                "branches_url": urls["branches"],
                "status_style": PROJECT_STATUS_STYLES.get(raw["status"], "is-planned"),
            }
        )
    return projects


def get_project_or_404(project_id: int) -> dict[str, object]:
    project = PROJECTS_BY_ID.get(project_id)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")

    urls = _project_urls(project_id)
    return {
        **project,
        "status_style": PROJECT_STATUS_STYLES.get(project["status"], "is-planned"),
        "urls": urls,
        "repository_label": str(project["repository_url"]).removeprefix("https://"),
        "updated_at_display": _format_datetime(project["updated_at"]),
    }


def _raw_observed_branches(project_id: int) -> list[dict[str, object]]:
    return [branch for branch in PROJECT_BRANCHES.get(project_id, []) if branch.get("last_push_at") is not None]


def _build_file_view(file_item: dict[str, object]) -> dict[str, object]:
    return {
        **file_item,
        "change_type_style": CHANGE_TYPE_STYLES.get(str(file_item["change_type"]), "is-modified"),
        "observed_at_display": _format_datetime(file_item.get("observed_at")),
    }


def _build_branch_view(branch: dict[str, object], row_index: int) -> dict[str, object]:
    files = [_build_file_view(file_item) for file_item in branch.get("files", [])]
    conflict_files_count = sum(1 for file_item in files if file_item["is_conflict"])
    last_push_at = branch.get("last_push_at")
    if conflict_files_count > 0:
        status = "競合あり"
    elif not _is_active_branch(last_push_at):
        status = "古い"
    else:
        status = "正常"

    row_key = f"{_normalize_path(str(branch['name']))}-{row_index}"
    return {
        "name": branch["name"],
        "last_push_at": last_push_at,
        "last_push_at_display": _format_datetime(last_push_at),
        "touched_files_count": len(files),
        "conflict_files_count": conflict_files_count,
        "status": status,
        "status_style": BRANCH_STATUS_STYLES.get(status, "is-planned"),
        "files": files,
        "is_active": _is_active_branch(last_push_at),
        "row_id": f"branch-{row_key}",
        "details_id": f"branch-details-{row_key}",
    }


def _parse_bool(raw_value: str | None, default: bool = False) -> bool:
    if raw_value is None:
        return default
    return raw_value.lower() in {"1", "true", "on", "yes"}


def get_branch_filters(query_params: Mapping[str, str]) -> dict[str, object]:
    submitted = query_params.get("submitted") == "1"
    return {
        "search": query_params.get("search", "").strip(),
        "sort": query_params.get("sort", "updated_desc"),
        "conflict_only": _parse_bool(query_params.get("conflict_only"), False),
        "active_only": _parse_bool(query_params.get("active_only"), False),
        "push_only": _parse_bool(query_params.get("push_only"), True if not submitted else False),
    }


def get_branch_summaries(project_id: int, filters: dict[str, object]) -> dict[str, object]:
    observed_branches = [_build_branch_view(branch, index) for index, branch in enumerate(_raw_observed_branches(project_id), start=1)]

    filtered = observed_branches
    search = str(filters["search"]).lower()
    if search:
        filtered = [branch for branch in filtered if search in str(branch["name"]).lower()]
    if filters["conflict_only"]:
        filtered = [branch for branch in filtered if int(branch["conflict_files_count"]) > 0]
    if filters["active_only"]:
        filtered = [branch for branch in filtered if bool(branch["is_active"])]
    if filters["push_only"]:
        filtered = [branch for branch in filtered if branch["last_push_at"] is not None]

    sort_key = filters["sort"]
    if sort_key == "name":
        filtered = sorted(filtered, key=lambda branch: str(branch["name"]).lower())
    elif sort_key == "conflicts_desc":
        filtered = sorted(
            filtered,
            key=lambda branch: (-int(branch["conflict_files_count"]), str(branch["name"]).lower()),
        )
    else:
        filtered = sorted(
            filtered,
            key=lambda branch: (branch["last_push_at"] is None, branch["last_push_at"]),
            reverse=True,
        )

    return {
        "branch_summaries": filtered,
        "total_branches": len(observed_branches),
        "recent_updated_count": sum(
            1 for branch in observed_branches if branch["last_push_at"] and branch["last_push_at"] >= NOW - RECENT_BRANCH_WINDOW
        ),
        "conflict_branch_count": sum(1 for branch in observed_branches if int(branch["conflict_files_count"]) > 0),
    }


def get_project_tabs(project: Mapping[str, object], active_tab: str) -> list[dict[str, object]]:
    urls = project["urls"]
    return [
        {"label": "Branches", "href": urls["branches"], "key": "branches", "is_active": active_tab == "branches"},
        {"label": "Conflicts", "href": urls["conflicts"], "key": "conflicts", "is_active": active_tab == "conflicts"},
        {"label": "Settings", "href": urls["settings"], "key": "settings", "is_active": active_tab == "settings"},
    ]


def get_project_overview(project_id: int) -> dict[str, object]:
    branch_data = get_branch_summaries(project_id, {"search": "", "sort": "updated_desc", "conflict_only": False, "active_only": False, "push_only": True})
    recent_branches = branch_data["branch_summaries"][:3]
    return {
        "summary_cards": [
            {"label": "Observed branches", "value": str(branch_data["total_branches"]), "meta": "push が観測された branch"},
            {"label": "Updated in 24h", "value": str(branch_data["recent_updated_count"]), "meta": "直近 24 時間で更新"},
            {"label": "With conflicts", "value": str(branch_data["conflict_branch_count"]), "meta": "競合ありの branch"},
        ],
        "recent_branches": recent_branches,
        "notes": [
            "Branches は Project / Repository に従属する情報として表示します。",
            "ブックマークはこの Project の Branches へのショートカットです。",
            "直リンクや再読込でもタブ状態が崩れないよう、URL 単位で分離しています。",
        ],
    }


def get_project_conflicts(project_id: int) -> list[dict[str, object]]:
    branch_data = get_branch_summaries(project_id, {"search": "", "sort": "conflicts_desc", "conflict_only": True, "active_only": False, "push_only": True})
    return list(branch_data["branch_summaries"])


def get_project_settings(project_id: int) -> list[dict[str, str]]:
    return PROJECT_SETTINGS.get(project_id, [])


def get_current_path(request_path: str, query_string: str = "") -> str:
    return f"{request_path}?{query_string}" if query_string else request_path


def _safe_next_path(next_path: str | None, fallback: str) -> str:
    if next_path and next_path.startswith("/app/") and not next_path.startswith("//"):
        return next_path
    return fallback


def list_sidebar_bookmarks(db: Session, *, active_project_id: int | None = None, active_tab: str | None = None) -> dict[str, object]:
    rows = db.scalars(
        select(ProjectBookmark)
        .where(ProjectBookmark.workspace_id == DEFAULT_WORKSPACE_ID)
        .order_by(ProjectBookmark.created_at.desc())
    ).all()

    bookmarks: list[dict[str, object]] = []
    for row in rows:
        project = PROJECTS_BY_ID.get(row.project_id)
        if project is None:
            continue
        bookmarks.append(
            {
                "project_id": row.project_id,
                "name": project["name"],
                "href": f"/app/projects/{row.project_id}/branches",
                "is_active": active_tab == "branches" and active_project_id == row.project_id,
            }
        )

    return {
        "items": bookmarks[:SIDEBAR_BOOKMARK_LIMIT],
        "remaining_count": max(0, len(bookmarks) - SIDEBAR_BOOKMARK_LIMIT),
    }


def is_project_bookmarked(db: Session, project_id: int) -> bool:
    bookmark = db.scalar(
        select(ProjectBookmark).where(
            ProjectBookmark.workspace_id == DEFAULT_WORKSPACE_ID,
            ProjectBookmark.project_id == project_id,
        )
    )
    return bookmark is not None


def add_project_bookmark(db: Session, project_id: int) -> None:
    if is_project_bookmarked(db, project_id):
        return
    db.add(ProjectBookmark(workspace_id=DEFAULT_WORKSPACE_ID, project_id=project_id))
    db.commit()


def remove_project_bookmark(db: Session, project_id: int) -> None:
    bookmark = db.scalar(
        select(ProjectBookmark).where(
            ProjectBookmark.workspace_id == DEFAULT_WORKSPACE_ID,
            ProjectBookmark.project_id == project_id,
        )
    )
    if bookmark is None:
        return
    db.delete(bookmark)
    db.commit()


def get_bookmark_redirect(next_path: str | None, fallback: str) -> str:
    return _safe_next_path(next_path, fallback)
