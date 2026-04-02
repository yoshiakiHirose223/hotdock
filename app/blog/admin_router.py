from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session

from app.blog.auth import require_blog_admin
from app.blog.common import (
    BLOG_ADMIN_BASE_PATH,
    BLOG_ADMIN_ROUTE_PREFIX,
    build_editor_from_form,
    empty_editor_state,
    parse_deleted_image_ids,
    parse_staged_images,
    read_staged_uploaded_images,
    read_uploaded_document,
    read_uploaded_images,
    render_blog_json,
    render_blog_template,
    serialize_editor_state,
    serialize_staged_images,
    service,
)
from app.blog.markdown_loader import parse_bool
from app.blog.service import BlogStagedImage, BlogUploadedDocument, BlogUploadedImage, BlogValidationError
from app.core.database import get_db

router = APIRouter(
    prefix=BLOG_ADMIN_ROUTE_PREFIX,
    dependencies=[Depends(require_blog_admin)],
)


@router.get("")
async def blog_admin_index(request: Request, db: Session = Depends(get_db)):
    return render_blog_template(
        request,
        "blog/admin/index.html",
        page_title="Blog Admin",
        posts=service.list_admin_posts(db),
    )


@router.get("/new")
async def blog_admin_new(request: Request, db: Session = Depends(get_db)):
    return render_blog_template(
        request,
        "blog/admin/form.html",
        page_title="Blog Admin",
        editor=empty_editor_state(db),
        errors=[],
        submit_mode="create",
        deleted_image_ids_state=[],
        staged_image_manifest_state=[],
    )


@router.post("/api/preview")
async def blog_admin_preview(
    db: Session = Depends(get_db),
    title: str = Form(default=""),
    summary: str = Form(default=""),
    slug: str = Form(default=""),
    markdown_source: str = Form(default=""),
    published_at: str = Form(default=""),
    selected_tag_slugs: list[str] = Form(default=[]),
    new_tag_name: str = Form(default=""),
    draft_key: str = Form(default=""),
    current_source_filename: str = Form(default=""),
    post_id: str = Form(default=""),
    is_published: str = Form(default="false"),
    import_uploaded_file: str = Form(default="true"),
    deleted_image_ids: str = Form(default="[]"),
    staged_image_manifest: str = Form(default="[]"),
    upload_file: UploadFile | None = File(default=None),
):
    try:
        resolved_post_id = int(post_id) if post_id.strip() else None
    except ValueError:
        return render_blog_json(content={"errors": ["編集中の記事情報を解釈できませんでした。"]}, status_code=400)

    try:
        parsed_deleted_image_ids = parse_deleted_image_ids(deleted_image_ids)
        staged_images = parse_staged_images(staged_image_manifest)
        uploaded_document = await read_uploaded_document(
            upload_file,
            should_import=parse_bool(import_uploaded_file, default=True),
        )
        editor = build_editor_from_form(
            db,
            title=title,
            summary=summary,
            slug=slug,
            markdown_source=markdown_source,
            published_at=published_at,
            selected_tag_slugs=selected_tag_slugs,
            is_published=parse_bool(is_published, default=False),
            uploaded_document=uploaded_document,
            post_id=resolved_post_id,
            draft_key=draft_key,
            current_source_filename=current_source_filename,
            new_tag_name=new_tag_name,
            staged_images=staged_images,
            deleted_image_ids=parsed_deleted_image_ids,
        )
        return render_blog_json(content={"editor": serialize_editor_state(editor), "errors": []})
    except BlogValidationError as exc:
        return render_blog_json(content={"errors": exc.errors}, status_code=400)
    except ValueError:
        return render_blog_json(content={"errors": ["公開日の日付形式が不正です。"]}, status_code=400)


@router.post("/api/tags")
async def blog_admin_create_tag_api(
    db: Session = Depends(get_db),
    tag_name: str = Form(default=""),
    selected_tag_slugs: list[str] = Form(default=[]),
):
    try:
        created_tag = service.create_tag(db, tag_name)
        normalized_selected = service.normalize_tag_slugs(db, [*selected_tag_slugs, created_tag.slug])
        return render_blog_json(
            content={
                "available_tags": service.list_tag_options(db, normalized_selected),
                "selected_tag_slugs": normalized_selected,
                "errors": [],
            }
        )
    except BlogValidationError as exc:
        return render_blog_json(content={"errors": exc.errors}, status_code=400)


@router.post("/api/tags/{tag_slug}/delete")
async def blog_admin_delete_tag_api(
    tag_slug: str,
    db: Session = Depends(get_db),
    selected_tag_slugs: list[str] = Form(default=[]),
):
    try:
        service.delete_tag(db, tag_slug)
        normalized_selected = service.normalize_tag_slugs(
            db,
            [value for value in selected_tag_slugs if value != tag_slug],
        )
        return render_blog_json(
            content={
                "available_tags": service.list_tag_options(db, normalized_selected),
                "selected_tag_slugs": normalized_selected,
                "errors": [],
            }
        )
    except BlogValidationError as exc:
        return render_blog_json(content={"errors": exc.errors}, status_code=404)


@router.post("/api/images")
async def blog_admin_upload_images_api(
    db: Session = Depends(get_db),
    post_id: str = Form(default=""),
    draft_key: str = Form(default=""),
    image_files: list[UploadFile] | None = File(default=None),
):
    try:
        resolved_post_id = int(post_id) if post_id.strip() else None
    except ValueError:
        return render_blog_json(content={"errors": ["編集中の記事情報を解釈できませんでした。"]}, status_code=400)

    try:
        uploaded_images = await read_uploaded_images(image_files)
        available_images = service.upload_images(
            db,
            uploaded_images,
            post_id=resolved_post_id,
            draft_key=draft_key,
        )
        return render_blog_json(content={"available_images": available_images, "errors": []})
    except BlogValidationError as exc:
        return render_blog_json(content={"errors": exc.errors}, status_code=400)


@router.post("/api/images/{image_id}/delete")
async def blog_admin_delete_image_api(
    image_id: int,
    db: Session = Depends(get_db),
    post_id: str = Form(default=""),
    draft_key: str = Form(default=""),
):
    try:
        resolved_post_id = int(post_id) if post_id.strip() else None
    except ValueError:
        return render_blog_json(content={"errors": ["編集中の記事情報を解釈できませんでした。"]}, status_code=400)

    try:
        available_images = service.delete_image(
            db,
            image_id,
            post_id=resolved_post_id,
            draft_key=draft_key,
        )
        return render_blog_json(content={"available_images": available_images, "errors": []})
    except BlogValidationError as exc:
        return render_blog_json(content={"errors": exc.errors}, status_code=404)


@router.post("/new")
async def blog_admin_create(
    request: Request,
    db: Session = Depends(get_db),
    action: str = Form(default="preview"),
    title: str = Form(default=""),
    summary: str = Form(default=""),
    slug: str = Form(default=""),
    markdown_source: str = Form(default=""),
    published_at: str = Form(default=""),
    selected_tag_slugs: list[str] = Form(default=[]),
    new_tag_name: str = Form(default=""),
    is_published: str = Form(default="false"),
    deleted_image_ids: str = Form(default="[]"),
    staged_image_manifest: str = Form(default="[]"),
    draft_key: str = Form(default=""),
    import_uploaded_file: str = Form(default="true"),
    upload_file: UploadFile | None = File(default=None),
    image_files: list[UploadFile] | None = File(default=None),
):
    editor = empty_editor_state(db)
    uploaded_document: BlogUploadedDocument | None = None
    parsed_deleted_image_ids: list[int] = []
    staged_images: list[BlogStagedImage] = []
    staged_uploaded_images: list[BlogUploadedImage] = []
    try:
        parsed_deleted_image_ids = parse_deleted_image_ids(deleted_image_ids)
        staged_images = parse_staged_images(staged_image_manifest)
        uploaded_document = await read_uploaded_document(
            upload_file,
            should_import=parse_bool(import_uploaded_file, default=True),
        )
        staged_uploaded_images = await read_staged_uploaded_images(image_files, staged_images)
        if action == "add_tag":
            created_tag = service.create_tag(db, new_tag_name)
            selected_tag_slugs = [*selected_tag_slugs, created_tag.slug]
            new_tag_name = ""
        elif action.startswith("delete_tag:"):
            tag_slug = action.split(":", maxsplit=1)[1]
            service.delete_tag(db, tag_slug)
            selected_tag_slugs = [value for value in selected_tag_slugs if value != tag_slug]

        editor_staged_images = service.build_staged_images_from_uploads(staged_uploaded_images) if staged_uploaded_images else staged_images
        editor = build_editor_from_form(
            db,
            title=title,
            summary=summary,
            slug=slug,
            markdown_source=markdown_source,
            published_at=published_at,
            selected_tag_slugs=selected_tag_slugs,
            is_published=parse_bool(is_published, default=False),
            uploaded_document=uploaded_document,
            draft_key=draft_key,
            new_tag_name=new_tag_name,
            staged_images=editor_staged_images,
            deleted_image_ids=parsed_deleted_image_ids,
        )

        if action == "create":
            service.create_post(
                db,
                editor,
                uploaded_images=staged_uploaded_images,
            )
            return RedirectResponse(url=BLOG_ADMIN_BASE_PATH, status_code=303)
        if action == "save_draft":
            service.create_post(
                db,
                editor,
                uploaded_images=staged_uploaded_images,
                publish_state=False,
            )
            return RedirectResponse(url=BLOG_ADMIN_BASE_PATH, status_code=303)

        return render_blog_template(
            request,
            "blog/admin/form.html",
            page_title="Blog Admin",
            editor=editor,
            errors=[],
            submit_mode="create",
            deleted_image_ids_state=parsed_deleted_image_ids,
            staged_image_manifest_state=serialize_staged_images(editor_staged_images),
        )
    except BlogValidationError as exc:
        if editor.title == "" and (title or summary or markdown_source or new_tag_name or selected_tag_slugs):
            try:
                editor_staged_images = (
                    service.build_staged_images_from_uploads(staged_uploaded_images) if staged_uploaded_images else staged_images
                )
                editor = build_editor_from_form(
                    db,
                    title=title,
                    summary=summary,
                    slug=slug,
                    markdown_source=markdown_source,
                    published_at=published_at,
                    selected_tag_slugs=selected_tag_slugs,
                    is_published=parse_bool(is_published, default=False),
                    uploaded_document=uploaded_document,
                    draft_key=draft_key,
                    new_tag_name=new_tag_name,
                    staged_images=editor_staged_images,
                    deleted_image_ids=parsed_deleted_image_ids,
                )
            except ValueError:
                pass
        return render_blog_template(
            request,
            "blog/admin/form.html",
            page_title="Blog Admin",
            editor=editor,
            errors=exc.errors,
            submit_mode="create",
            deleted_image_ids_state=parsed_deleted_image_ids,
            staged_image_manifest_state=serialize_staged_images(
                service.build_staged_images_from_uploads(staged_uploaded_images) if staged_uploaded_images else staged_images
            ),
        )
    except ValueError:
        return render_blog_template(
            request,
            "blog/admin/form.html",
            page_title="Blog Admin",
            editor=editor,
            errors=["公開日の日付形式が不正です。"],
            submit_mode="create",
            deleted_image_ids_state=parsed_deleted_image_ids,
            staged_image_manifest_state=serialize_staged_images(
                service.build_staged_images_from_uploads(staged_uploaded_images) if staged_uploaded_images else staged_images
            ),
        )


@router.get("/{post_id}/edit")
async def blog_admin_edit(request: Request, post_id: int, db: Session = Depends(get_db)):
    editor = service.get_admin_editor(db, post_id)
    if editor is None:
        raise HTTPException(status_code=404, detail="Post not found")
    return render_blog_template(
        request,
        "blog/admin/form.html",
        page_title="Blog Admin",
        editor=editor,
        errors=[],
        submit_mode="edit",
        deleted_image_ids_state=[],
        staged_image_manifest_state=[],
    )


@router.post("/{post_id}/edit")
async def blog_admin_update(
    request: Request,
    post_id: int,
    db: Session = Depends(get_db),
    action: str = Form(default="preview"),
    title: str = Form(default=""),
    summary: str = Form(default=""),
    slug: str = Form(default=""),
    markdown_source: str = Form(default=""),
    published_at: str = Form(default=""),
    selected_tag_slugs: list[str] = Form(default=[]),
    new_tag_name: str = Form(default=""),
    is_published: str = Form(default="false"),
    deleted_image_ids: str = Form(default="[]"),
    staged_image_manifest: str = Form(default="[]"),
    draft_key: str = Form(default=""),
    current_source_filename: str = Form(default=""),
    import_uploaded_file: str = Form(default="true"),
    upload_file: UploadFile | None = File(default=None),
    image_files: list[UploadFile] | None = File(default=None),
):
    existing_editor = service.get_admin_editor(db, post_id)
    if existing_editor is None:
        raise HTTPException(status_code=404, detail="Post not found")

    editor = existing_editor
    uploaded_document: BlogUploadedDocument | None = None
    parsed_deleted_image_ids: list[int] = []
    staged_images: list[BlogStagedImage] = []
    staged_uploaded_images: list[BlogUploadedImage] = []
    try:
        parsed_deleted_image_ids = parse_deleted_image_ids(deleted_image_ids)
        staged_images = parse_staged_images(staged_image_manifest)
        uploaded_document = await read_uploaded_document(
            upload_file,
            should_import=parse_bool(import_uploaded_file, default=True),
        )
        staged_uploaded_images = await read_staged_uploaded_images(image_files, staged_images)
        if action == "add_tag":
            created_tag = service.create_tag(db, new_tag_name)
            selected_tag_slugs = [*selected_tag_slugs, created_tag.slug]
            new_tag_name = ""
        elif action.startswith("delete_tag:"):
            tag_slug = action.split(":", maxsplit=1)[1]
            service.delete_tag(db, tag_slug)
            selected_tag_slugs = [value for value in selected_tag_slugs if value != tag_slug]

        editor_staged_images = service.build_staged_images_from_uploads(staged_uploaded_images) if staged_uploaded_images else staged_images
        editor = build_editor_from_form(
            db,
            title=title,
            summary=summary,
            slug=slug,
            markdown_source=markdown_source,
            published_at=published_at,
            selected_tag_slugs=selected_tag_slugs,
            is_published=parse_bool(is_published, default=existing_editor.is_published),
            uploaded_document=uploaded_document,
            post_id=post_id,
            draft_key=draft_key,
            current_source_filename=current_source_filename or existing_editor.source_filename,
            new_tag_name=new_tag_name,
            staged_images=editor_staged_images,
            deleted_image_ids=parsed_deleted_image_ids,
        )

        if action == "update":
            service.update_post(
                db,
                post_id,
                editor,
                uploaded_images=staged_uploaded_images,
                deleted_image_ids=parsed_deleted_image_ids,
            )
            return RedirectResponse(url=f"{BLOG_ADMIN_BASE_PATH}/{post_id}/edit", status_code=303)

        return render_blog_template(
            request,
            "blog/admin/form.html",
            page_title="Blog Admin",
            editor=editor,
            errors=[],
            submit_mode="edit",
            deleted_image_ids_state=parsed_deleted_image_ids,
            staged_image_manifest_state=serialize_staged_images(editor_staged_images),
        )
    except BlogValidationError as exc:
        if editor is existing_editor and (title or summary or markdown_source or new_tag_name or selected_tag_slugs):
            try:
                editor_staged_images = (
                    service.build_staged_images_from_uploads(staged_uploaded_images) if staged_uploaded_images else staged_images
                )
                editor = build_editor_from_form(
                    db,
                    title=title,
                    summary=summary,
                    slug=slug,
                    markdown_source=markdown_source,
                    published_at=published_at,
                    selected_tag_slugs=selected_tag_slugs,
                    is_published=parse_bool(is_published, default=existing_editor.is_published),
                    uploaded_document=uploaded_document,
                    post_id=post_id,
                    draft_key=draft_key,
                    current_source_filename=current_source_filename or existing_editor.source_filename,
                    new_tag_name=new_tag_name,
                    staged_images=editor_staged_images,
                    deleted_image_ids=parsed_deleted_image_ids,
                )
            except ValueError:
                pass
        return render_blog_template(
            request,
            "blog/admin/form.html",
            page_title="Blog Admin",
            editor=editor,
            errors=exc.errors,
            submit_mode="edit",
            deleted_image_ids_state=parsed_deleted_image_ids,
            staged_image_manifest_state=serialize_staged_images(
                service.build_staged_images_from_uploads(staged_uploaded_images) if staged_uploaded_images else staged_images
            ),
        )
    except ValueError:
        return render_blog_template(
            request,
            "blog/admin/form.html",
            page_title="Blog Admin",
            editor=editor,
            errors=["公開日の日付形式が不正です。"],
            submit_mode="edit",
            deleted_image_ids_state=parsed_deleted_image_ids,
            staged_image_manifest_state=serialize_staged_images(
                service.build_staged_images_from_uploads(staged_uploaded_images) if staged_uploaded_images else staged_images
            ),
        )


@router.post("/{post_id}/publish")
async def blog_admin_publish(post_id: int, db: Session = Depends(get_db)):
    try:
        service.set_publish_state(db, post_id, True)
    except BlogValidationError as exc:
        raise HTTPException(status_code=404, detail=exc.errors[0]) from exc
    return RedirectResponse(url=BLOG_ADMIN_BASE_PATH, status_code=303)


@router.post("/{post_id}/unpublish")
async def blog_admin_unpublish(post_id: int, db: Session = Depends(get_db)):
    try:
        service.set_publish_state(db, post_id, False)
    except BlogValidationError as exc:
        raise HTTPException(status_code=404, detail=exc.errors[0]) from exc
    return RedirectResponse(url=BLOG_ADMIN_BASE_PATH, status_code=303)


@router.post("/{post_id}/visibility")
async def blog_admin_set_visibility(
    post_id: int,
    db: Session = Depends(get_db),
    is_published: str = Form(default="false"),
):
    next_state = parse_bool(is_published, default=False)
    try:
        service.set_publish_state(db, post_id, next_state)
    except BlogValidationError as exc:
        return render_blog_json(content={"errors": exc.errors}, status_code=404)
    return render_blog_json(content={"is_published": next_state, "errors": []})


@router.post("/{post_id}/delete")
async def blog_admin_delete(post_id: int, db: Session = Depends(get_db)):
    try:
        service.delete_post(db, post_id)
    except BlogValidationError as exc:
        raise HTTPException(status_code=404, detail=exc.errors[0]) from exc
    return RedirectResponse(url=BLOG_ADMIN_BASE_PATH, status_code=303)
