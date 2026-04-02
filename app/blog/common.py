import json

from fastapi import Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.blog.schemas import BlogEditorState
from app.blog.service import (
    BlogService,
    BlogStagedImage,
    BlogUploadedDocument,
    BlogUploadedImage,
    BlogValidationError,
)
from app.core.config import get_settings
from app.core.dependencies import build_template_context

service = BlogService(get_settings().blog_posts_dir, get_settings().blog_images_dir)
BLOG_ADMIN_ROUTE_PREFIX = "/admin-Q7vJ2kD9sLpX4mTa"
BLOG_ADMIN_BASE_PATH = f"/blog{BLOG_ADMIN_ROUTE_PREFIX}"
BLOG_ADMIN_LOGIN_PATH = f"{BLOG_ADMIN_BASE_PATH}/login"
BLOG_ADMIN_LOGOUT_PATH = f"{BLOG_ADMIN_BASE_PATH}/logout"


async def read_uploaded_document(
    upload_file: UploadFile | None,
    *,
    should_import: bool = True,
) -> BlogUploadedDocument | None:
    if not should_import or upload_file is None or not upload_file.filename:
        return None
    if not upload_file.filename.lower().endswith(".md"):
        raise BlogValidationError(["アップロードできるのは .md ファイルのみです。"])
    source_bytes = await upload_file.read()
    try:
        source_text = source_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise BlogValidationError(["Markdown ファイルは UTF-8 で保存してください。"]) from exc
    return BlogUploadedDocument(source_text=source_text, filename=upload_file.filename)


async def read_uploaded_images(upload_files: list[UploadFile] | None) -> list[BlogUploadedImage]:
    uploaded_images: list[BlogUploadedImage] = []
    for upload_file in upload_files or []:
        if upload_file is None or not upload_file.filename:
            continue
        uploaded_images.append(
            BlogUploadedImage(
                source_bytes=await upload_file.read(),
                filename=upload_file.filename,
                content_type=upload_file.content_type or "application/octet-stream",
            )
        )
    return uploaded_images


def parse_deleted_image_ids(raw_value: str) -> list[int]:
    if not raw_value.strip():
        return []
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise BlogValidationError(["画像削除情報を解釈できませんでした。"]) from exc

    if not isinstance(parsed, list):
        raise BlogValidationError(["画像削除情報の形式が不正です。"])

    deleted_image_ids: list[int] = []
    for value in parsed:
        if isinstance(value, bool):
            raise BlogValidationError(["画像削除情報の形式が不正です。"])
        try:
            image_id = int(value)
        except (TypeError, ValueError) as exc:
            raise BlogValidationError(["画像削除情報の形式が不正です。"]) from exc
        if image_id > 0 and image_id not in deleted_image_ids:
            deleted_image_ids.append(image_id)
    return deleted_image_ids


def parse_staged_images(raw_value: str) -> list[BlogStagedImage]:
    if not raw_value.strip():
        return []
    try:
        parsed = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        raise BlogValidationError(["画像登録情報を解釈できませんでした。"]) from exc

    if not isinstance(parsed, list):
        raise BlogValidationError(["画像登録情報の形式が不正です。"])

    staged_images: list[BlogStagedImage] = []
    seen_tokens: set[str] = set()
    for item in parsed:
        if not isinstance(item, dict):
            raise BlogValidationError(["画像登録情報の形式が不正です。"])
        token = str(item.get("token", "")).strip()
        original_filename = str(item.get("original_filename", "")).strip()
        preview_url = str(item.get("preview_url", "")).strip()
        if not token or not original_filename or not preview_url:
            raise BlogValidationError(["画像登録情報の形式が不正です。"])
        if token in seen_tokens:
            continue
        seen_tokens.add(token)
        staged_images.append(
            BlogStagedImage(
                token=token,
                original_filename=original_filename,
                preview_url=preview_url,
            )
        )
    return staged_images


async def read_staged_uploaded_images(
    upload_files: list[UploadFile] | None,
    staged_images: list[BlogStagedImage],
) -> list[BlogUploadedImage]:
    uploaded_images = await read_uploaded_images(upload_files)
    if len(uploaded_images) != len(staged_images):
        raise BlogValidationError(["選択中の画像情報を解釈できませんでした。画像を選び直してください。"])

    staged_by_index = list(staged_images)
    for index, uploaded_image in enumerate(uploaded_images):
        uploaded_image.token = staged_by_index[index].token
    return uploaded_images


def render_blog_template(request: Request, template_name: str, **extra: object):
    context = build_template_context(
        request,
        blog_admin_base_path=BLOG_ADMIN_BASE_PATH,
        blog_admin_new_path=f"{BLOG_ADMIN_BASE_PATH}/new",
        blog_admin_login_path=BLOG_ADMIN_LOGIN_PATH,
        blog_admin_logout_path=BLOG_ADMIN_LOGOUT_PATH,
        **extra,
    )
    return request.app.state.templates.TemplateResponse(
        request=request,
        name=template_name,
        context=context,
    )


def render_blog_json(*, content: dict[str, object], status_code: int = 200) -> JSONResponse:
    return JSONResponse(status_code=status_code, content=jsonable_encoder(content))


def empty_editor_state(db: Session, is_published: bool = False) -> BlogEditorState:
    return service.build_editor_state(
        db=db,
        title="",
        summary_text="",
        slug="",
        markdown_source="",
        published_at_text="",
        selected_tag_slugs=[],
        is_published=is_published,
    )


def build_editor_from_form(
    db: Session,
    *,
    title: str,
    summary: str,
    slug: str,
    markdown_source: str,
    published_at: str,
    selected_tag_slugs: list[str],
    is_published: bool,
    uploaded_document: BlogUploadedDocument | None,
    post_id: int | None = None,
    draft_key: str = "",
    current_source_filename: str = "",
    new_tag_name: str = "",
    staged_images: list[BlogStagedImage] | None = None,
    deleted_image_ids: list[int] | None = None,
) -> BlogEditorState:
    return service.build_editor_state(
        db=db,
        title=title,
        summary_text=summary,
        slug=slug,
        markdown_source=markdown_source,
        published_at_text=published_at,
        selected_tag_slugs=selected_tag_slugs,
        is_published=is_published,
        uploaded_document=uploaded_document,
        post_id=post_id,
        draft_key=draft_key,
        current_source_filename=current_source_filename,
        new_tag_name=new_tag_name,
        staged_images=staged_images,
        deleted_image_ids=deleted_image_ids,
    )


def serialize_editor_state(editor: BlogEditorState) -> dict[str, object]:
    return jsonable_encoder(editor)


def serialize_staged_images(staged_images: list[BlogStagedImage]) -> list[dict[str, str]]:
    return [
        {
            "token": image.token,
            "original_filename": image.original_filename,
            "preview_url": image.preview_url,
        }
        for image in staged_images
    ]
