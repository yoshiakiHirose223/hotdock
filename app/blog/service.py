from __future__ import annotations

from base64 import b64encode
from dataclasses import dataclass
from datetime import date
from mimetypes import guess_type
from pathlib import Path
from secrets import token_hex
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.blog.markdown_loader import (
    build_front_matter,
    inject_image_placeholders,
    normalize_slug,
    parse_bool,
    parse_date,
    parse_front_matter,
    parse_tags,
    render_markdown,
)
from app.blog.models import BlogImage, BlogPost, BlogTag
from app.blog.schemas import (
    BlogAdminPostSummary,
    BlogEditorState,
    BlogImageAsset,
    BlogPostDetail,
    BlogPostSummary,
    BlogTagOption,
)


class BlogValidationError(Exception):
    def __init__(self, errors: list[str]):
        super().__init__("\n".join(errors))
        self.errors = errors


@dataclass(slots=True)
class BlogUploadedDocument:
    source_text: str
    filename: str = ""


@dataclass(slots=True)
class BlogUploadedImage:
    source_bytes: bytes
    filename: str
    content_type: str = "application/octet-stream"
    token: str = ""


@dataclass(slots=True)
class BlogStagedImage:
    token: str
    original_filename: str
    preview_url: str


class BlogService:
    allowed_image_extensions = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}

    def __init__(self, posts_dir: Path, images_dir: Path | None = None):
        self.posts_dir = posts_dir
        self.images_dir = images_dir or posts_dir.parent / "images"
        self.posts_dir.mkdir(parents=True, exist_ok=True)
        self.images_dir.mkdir(parents=True, exist_ok=True)

    def list_tag_options(
        self,
        db: Session,
        selected_tag_slugs: list[str] | None = None,
    ) -> list[BlogTagOption]:
        normalized_selected_slugs = self._normalize_existing_tag_slugs(db, selected_tag_slugs or [])
        return self._build_tag_options(db, normalized_selected_slugs)

    def normalize_tag_slugs(self, db: Session, tag_slugs: list[str] | None = None) -> list[str]:
        return self._normalize_existing_tag_slugs(db, tag_slugs or [])

    def list_posts(self, db: Session) -> list[BlogPostSummary]:
        self.sync_storage(db)
        statement = (
            select(BlogPost)
            .options(selectinload(BlogPost.tags))
            .where(BlogPost.is_published.is_(True))
            .order_by(BlogPost.published_at.desc(), BlogPost.updated_at.desc())
        )
        posts = db.scalars(statement).unique().all()
        return [self._to_summary(post) for post in posts]

    def get_post(self, db: Session, slug: str) -> BlogPostDetail | None:
        self.sync_storage(db)
        statement = (
            select(BlogPost)
            .options(selectinload(BlogPost.tags), selectinload(BlogPost.images))
            .where(BlogPost.slug == slug, BlogPost.is_published.is_(True))
        )
        post = db.scalars(statement).unique().first()
        if post is None:
            return None
        return self._to_detail(db, post)

    def list_admin_posts(self, db: Session) -> list[BlogAdminPostSummary]:
        self.sync_storage(db)
        statement = (
            select(BlogPost)
            .options(selectinload(BlogPost.tags))
            .order_by(BlogPost.updated_at.desc(), BlogPost.published_at.desc())
        )
        posts = db.scalars(statement).unique().all()
        return [self._to_admin_summary(post) for post in posts]

    def get_admin_editor(self, db: Session, post_id: int) -> BlogEditorState | None:
        self.sync_storage(db)
        post = self._get_post_by_id(db, post_id)
        if post is None:
            return None

        _, markdown_source = self._read_source_document(post.source_filename)
        selected_tag_slugs = [tag.slug for tag in post.tags]
        return BlogEditorState(
            post_id=post.id,
            draft_key="",
            title=post.title,
            summary=post.summary,
            slug=post.slug,
            markdown_source=markdown_source,
            published_at=post.published_at,
            selected_tag_slugs=selected_tag_slugs,
            available_tags=self._build_tag_options(db, selected_tag_slugs),
            available_images=self.resolve_editor_images(db, post_id=post.id),
            new_tag_name="",
            is_published=post.is_published,
            preview_html=self._render_article_html(db, markdown_source, post_id=post.id),
            source_filename=post.source_filename,
        )

    def build_editor_state(
        self,
        db: Session,
        title: str,
        slug: str,
        markdown_source: str,
        published_at_text: str,
        selected_tag_slugs: list[str] | None,
        is_published: bool,
        uploaded_document: BlogUploadedDocument | None = None,
        post_id: int | None = None,
        draft_key: str = "",
        current_source_filename: str = "",
        new_tag_name: str = "",
        staged_images: list[BlogStagedImage] | None = None,
        deleted_image_ids: list[int] | None = None,
        summary_text: str = "",
    ) -> BlogEditorState:
        effective_draft_key = draft_key.strip() or ("" if post_id is not None else self.generate_draft_key())
        derived_title = title.strip()
        derived_summary = summary_text.strip()
        derived_slug = slug.strip()
        derived_markdown = markdown_source
        derived_published_at = published_at_text.strip()
        source_filename = current_source_filename
        selected_slug_set = {value for value in (selected_tag_slugs or []) if value}

        if uploaded_document and uploaded_document.source_text:
            metadata, body = parse_front_matter(uploaded_document.source_text)
            if body.strip():
                derived_markdown = body
            uploaded_title = metadata.get("title", "").strip()
            uploaded_summary = metadata.get("summary", "").strip()
            uploaded_slug = metadata.get("slug", "").strip()
            uploaded_published_at = metadata.get("published_at", "").strip()
            if uploaded_title:
                derived_title = uploaded_title
            if uploaded_summary:
                derived_summary = uploaded_summary
            if uploaded_slug:
                derived_slug = uploaded_slug
            if uploaded_published_at:
                derived_published_at = uploaded_published_at
            uploaded_tag_slugs = self._resolve_tag_slugs(db, parse_tags(metadata.get("tags")))
            if uploaded_tag_slugs:
                selected_slug_set.update(uploaded_tag_slugs)
            source_filename = uploaded_document.filename or source_filename

        effective_slug = normalize_slug(
            derived_slug or derived_title or Path(source_filename or "post.md").stem,
            fallback="post",
        )
        effective_published_at = parse_date(derived_published_at or None)
        normalized_selected_slugs = self._normalize_existing_tag_slugs(db, list(selected_slug_set))
        available_images = self.resolve_editor_images(
            db,
            post_id=post_id,
            draft_key=effective_draft_key,
            staged_images=staged_images,
            deleted_image_ids=deleted_image_ids,
        )
        preview_html = (
            self._render_article_html(
                db,
                derived_markdown,
                post_id=post_id,
                draft_key=effective_draft_key,
                available_images=available_images,
            )
            if derived_markdown.strip()
            else ""
        )

        return BlogEditorState(
            post_id=post_id,
            draft_key=effective_draft_key,
            title=derived_title,
            summary=derived_summary,
            slug=effective_slug,
            markdown_source=derived_markdown,
            published_at=effective_published_at,
            selected_tag_slugs=normalized_selected_slugs,
            available_tags=self._build_tag_options(db, normalized_selected_slugs),
            available_images=available_images,
            new_tag_name=new_tag_name.strip(),
            is_published=is_published,
            preview_html=preview_html,
            source_filename=source_filename,
        )

    def create_post(
        self,
        db: Session,
        editor: BlogEditorState,
        *,
        uploaded_images: list[BlogUploadedImage] | None = None,
        publish_state: bool | None = None,
    ) -> BlogPost:
        self.sync_storage(db)
        errors = self._validate_editor(db, editor)
        if errors:
            raise BlogValidationError(errors)

        source_filename = f"{editor.slug}.md"
        resolved_publish_state = editor.is_published if publish_state is None else publish_state
        post = BlogPost(
            slug=editor.slug,
            title=editor.title.strip(),
            summary=editor.summary.strip(),
            source_filename=source_filename,
            is_published=resolved_publish_state,
            published_at=editor.published_at,
        )
        db.add(post)
        db.flush()
        self._assign_tags_by_slug(db, post, editor.selected_tag_slugs)
        self.persist_uploaded_images(db, uploaded_images or [], post_id=post.id)
        self._write_post_document(
            source_filename=source_filename,
            title=post.title,
            summary=post.summary,
            slug=post.slug,
            published_at=post.published_at,
            tags=[tag.name for tag in post.tags],
            is_published=resolved_publish_state,
            markdown_source=editor.markdown_source,
        )
        db.commit()
        db.refresh(post)
        return post

    def update_post(
        self,
        db: Session,
        post_id: int,
        editor: BlogEditorState,
        *,
        uploaded_images: list[BlogUploadedImage] | None = None,
        deleted_image_ids: list[int] | None = None,
        publish_state: bool | None = None,
    ) -> BlogPost:
        self.sync_storage(db)
        post = self._get_post_by_id(db, post_id)
        if post is None:
            raise BlogValidationError(["対象の記事が見つかりません。"])

        errors = self._validate_editor(db, editor, exclude_post_id=post_id)
        if errors:
            raise BlogValidationError(errors)

        previous_filename = post.source_filename
        post.slug = editor.slug
        post.title = editor.title.strip()
        post.summary = editor.summary.strip()
        post.published_at = editor.published_at
        post.is_published = editor.is_published if publish_state is None else publish_state
        post.source_filename = f"{editor.slug}.md"
        self._assign_tags_by_slug(db, post, editor.selected_tag_slugs)
        self.delete_post_images(db, post, deleted_image_ids or [])
        self.persist_uploaded_images(db, uploaded_images or [], post_id=post.id)
        self._write_post_document(
            source_filename=post.source_filename,
            title=post.title,
            summary=post.summary,
            slug=post.slug,
            published_at=post.published_at,
            tags=[tag.name for tag in post.tags],
            is_published=post.is_published,
            markdown_source=editor.markdown_source,
        )
        self._cleanup_orphan_tags(db)
        if previous_filename != post.source_filename:
            old_path = self.posts_dir / previous_filename
            if old_path.exists():
                old_path.unlink()
        db.commit()
        db.refresh(post)
        return post

    def set_publish_state(self, db: Session, post_id: int, is_published: bool) -> None:
        self.sync_storage(db)
        post = self._get_post_by_id(db, post_id)
        if post is None:
            raise BlogValidationError(["対象の記事が見つかりません。"])

        _, markdown_source = self._read_source_document(post.source_filename)
        post.is_published = is_published
        self._write_post_document(
            source_filename=post.source_filename,
            title=post.title,
            summary=post.summary,
            slug=post.slug,
            published_at=post.published_at,
            tags=[tag.name for tag in post.tags],
            is_published=post.is_published,
            markdown_source=markdown_source,
        )
        db.commit()

    def delete_post(self, db: Session, post_id: int) -> None:
        self.sync_storage(db)
        post = self._get_post_by_id(db, post_id)
        if post is None:
            raise BlogValidationError(["対象の記事が見つかりません。"])

        file_path = self.posts_dir / post.source_filename
        image_paths = self._collect_image_paths(post.images)
        db.delete(post)
        db.flush()
        self._cleanup_orphan_tags(db)
        db.commit()
        if file_path.exists():
            file_path.unlink()
        self._delete_files(image_paths)

    def generate_draft_key(self) -> str:
        return f"draft-{uuid4().hex}"

    def list_image_assets(
        self,
        db: Session,
        *,
        post_id: int | None = None,
        draft_key: str = "",
    ) -> list[BlogImageAsset]:
        statement = select(BlogImage).order_by(BlogImage.created_at.asc(), BlogImage.id.asc())
        if post_id is not None:
            statement = statement.where(BlogImage.post_id == post_id)
        elif draft_key.strip():
            statement = statement.where(BlogImage.draft_key == draft_key.strip())
        else:
            return []

        images = db.scalars(statement).all()
        return [self._to_image_asset(image) for image in images]

    def resolve_editor_images(
        self,
        db: Session,
        *,
        post_id: int | None = None,
        draft_key: str = "",
        staged_images: list[BlogStagedImage] | None = None,
        deleted_image_ids: list[int] | None = None,
    ) -> list[BlogImageAsset]:
        deleted_ids = {image_id for image_id in (deleted_image_ids or []) if image_id > 0}
        persisted_images = [
            image
            for image in self.list_image_assets(db, post_id=post_id, draft_key=draft_key)
            if image.id not in deleted_ids
        ]
        return [*persisted_images, *self.build_staged_image_assets(staged_images or [])]

    def build_staged_image_assets(self, staged_images: list[BlogStagedImage]) -> list[BlogImageAsset]:
        return [
            BlogImageAsset(
                token=image.token,
                placeholder_tag=f"[[image:{image.token}]]",
                url=image.preview_url,
                original_filename=image.original_filename,
                is_staged=True,
            )
            for image in staged_images
        ]

    def build_staged_images_from_uploads(
        self,
        uploaded_images: list[BlogUploadedImage],
    ) -> list[BlogStagedImage]:
        staged_images: list[BlogStagedImage] = []
        for uploaded_image in uploaded_images:
            token = uploaded_image.token.strip() or self._build_ephemeral_image_token(uploaded_image.filename)
            staged_images.append(
                BlogStagedImage(
                    token=token,
                    original_filename=uploaded_image.filename,
                    preview_url=self._build_data_url(uploaded_image),
                )
            )
        return staged_images

    def persist_uploaded_images(
        self,
        db: Session,
        uploaded_images: list[BlogUploadedImage],
        *,
        post_id: int,
    ) -> None:
        if not uploaded_images:
            return
        if self._get_post_by_id(db, post_id) is None:
            raise BlogValidationError(["対象の記事が見つかりません。"])

        written_paths: list[Path] = []
        try:
            for uploaded_image in uploaded_images:
                self._validate_image_upload(uploaded_image)
                token, stored_filename = self._resolve_image_storage(db, uploaded_image)
                file_path = self.images_dir / stored_filename
                file_path.write_bytes(uploaded_image.source_bytes)
                written_paths.append(file_path)
                db.add(
                    BlogImage(
                        post_id=post_id,
                        draft_key=None,
                        token=token,
                        original_filename=uploaded_image.filename,
                        stored_filename=stored_filename,
                        content_type=uploaded_image.content_type or self._guess_image_content_type(stored_filename),
                    )
                )
        except Exception:
            self._delete_files(written_paths)
            raise

    def delete_post_images(self, db: Session, post: BlogPost, image_ids: list[int]) -> None:
        if not image_ids:
            return

        image_id_set = {image_id for image_id in image_ids if image_id > 0}
        if not image_id_set:
            return

        matched_images = [image for image in post.images if image.id in image_id_set]
        if len(matched_images) != len(image_id_set):
            raise BlogValidationError(["削除対象の画像が現在の記事に紐づいていません。"])

        file_paths = self._collect_image_paths(matched_images)
        for image in matched_images:
            db.delete(image)
        db.flush()
        self._delete_files(file_paths)

    def upload_images(
        self,
        db: Session,
        uploaded_images: list[BlogUploadedImage],
        *,
        post_id: int | None = None,
        draft_key: str = "",
    ) -> list[BlogImageAsset]:
        if not uploaded_images:
            raise BlogValidationError(["画像ファイルを選択してください。"])
        if post_id is None and not draft_key.strip():
            raise BlogValidationError(["画像を紐づける記事情報を解釈できませんでした。"])
        if post_id is not None and self._get_post_by_id(db, post_id) is None:
            raise BlogValidationError(["対象の記事が見つかりません。"])

        written_paths: list[Path] = []
        try:
            for uploaded_image in uploaded_images:
                self._validate_image_upload(uploaded_image)
                token, stored_filename = self._build_image_token(db, uploaded_image.filename)
                file_path = self.images_dir / stored_filename
                file_path.write_bytes(uploaded_image.source_bytes)
                written_paths.append(file_path)
                image = BlogImage(
                    post_id=post_id,
                    draft_key=None if post_id is not None else draft_key.strip(),
                    token=token,
                    original_filename=uploaded_image.filename,
                    stored_filename=stored_filename,
                    content_type=uploaded_image.content_type or self._guess_image_content_type(stored_filename),
                )
                db.add(image)
            db.commit()
        except Exception:
            db.rollback()
            self._delete_files(written_paths)
            raise

        return self.list_image_assets(db, post_id=post_id, draft_key=draft_key)

    def delete_image(
        self,
        db: Session,
        image_id: int,
        *,
        post_id: int | None = None,
        draft_key: str = "",
    ) -> list[BlogImageAsset]:
        image = db.scalars(select(BlogImage).where(BlogImage.id == image_id)).first()
        if image is None:
            raise BlogValidationError(["削除対象の画像が見つかりません。"])
        if post_id is not None and image.post_id != post_id:
            raise BlogValidationError(["削除対象の画像が現在の記事に紐づいていません。"])
        if post_id is None and draft_key.strip() and image.draft_key != draft_key.strip():
            raise BlogValidationError(["削除対象の画像が現在の編集中データに紐づいていません。"])

        file_path = self.images_dir / image.stored_filename
        db.delete(image)
        db.commit()
        self._delete_files([file_path])
        return self.list_image_assets(db, post_id=post_id, draft_key=draft_key)

    def get_image_file(self, db: Session, token: str) -> BlogImage | None:
        return db.scalars(select(BlogImage).where(BlogImage.token == token)).first()

    def attach_draft_images_to_post(self, db: Session, post: BlogPost, draft_key: str) -> None:
        cleaned_draft_key = draft_key.strip()
        if not cleaned_draft_key:
            return
        images = db.scalars(select(BlogImage).where(BlogImage.draft_key == cleaned_draft_key)).all()
        for image in images:
            image.post_id = post.id
            image.draft_key = None

    def create_tag(self, db: Session, tag_name: str) -> BlogTag:
        cleaned_name = tag_name.strip()
        if not cleaned_name:
            raise BlogValidationError(["追加するタグ名を入力してください。"])

        tag_slug = normalize_slug(cleaned_name, fallback="tag")
        existing = db.scalars(select(BlogTag).where(BlogTag.slug == tag_slug)).first()
        if existing is not None:
            existing.name = cleaned_name
            db.commit()
            db.refresh(existing)
            return existing

        tag = BlogTag(name=cleaned_name, slug=tag_slug)
        db.add(tag)
        db.commit()
        db.refresh(tag)
        return tag

    def delete_tag(self, db: Session, tag_slug: str) -> None:
        tag = db.scalars(
            select(BlogTag)
            .options(selectinload(BlogTag.posts))
            .where(BlogTag.slug == tag_slug)
        ).first()
        if tag is None:
            raise BlogValidationError(["削除対象のタグが見つかりません。"])

        for post in list(tag.posts):
            post.tags = [candidate for candidate in post.tags if candidate.slug != tag_slug]
            _, markdown_source = self._read_source_document(post.source_filename)
            self._write_post_document(
                source_filename=post.source_filename,
                title=post.title,
                summary=post.summary,
                slug=post.slug,
                published_at=post.published_at,
                tags=[candidate.name for candidate in post.tags],
                is_published=post.is_published,
                markdown_source=markdown_source,
            )
        db.delete(tag)
        db.commit()

    def sync_storage(self, db: Session) -> None:
        statement = select(BlogPost).options(selectinload(BlogPost.tags))
        existing_posts = db.scalars(statement).unique().all()
        posts_by_filename = {post.source_filename: post for post in existing_posts}
        posts_by_slug = {post.slug: post for post in existing_posts}
        seen_filenames: set[str] = set()
        changed = False

        for path in sorted(self.posts_dir.glob("*.md")):
            metadata, _ = self._read_source_document(path.name)
            slug = normalize_slug(metadata.get("slug", path.stem), fallback=path.stem)
            title = metadata.get("title", path.stem.replace("-", " ").title())
            summary = metadata.get("summary", "").strip()
            published_at = parse_date(metadata.get("published_at"))
            is_published = parse_bool(metadata.get("is_published"), default=True)
            tag_slugs = self._ensure_tag_slugs(db, parse_tags(metadata.get("tags")))

            post = posts_by_filename.get(path.name) or posts_by_slug.get(slug)
            if post is None:
                post = BlogPost(
                    slug=slug,
                    title=title,
                    summary=summary,
                    source_filename=path.name,
                    is_published=is_published,
                    published_at=published_at,
                )
                db.add(post)
                db.flush()
                changed = True
            else:
                if post.slug != slug:
                    post.slug = slug
                    changed = True
                if post.title != title:
                    post.title = title
                    changed = True
                if post.summary != summary:
                    post.summary = summary
                    changed = True
                if post.published_at != published_at:
                    post.published_at = published_at
                    changed = True
                if post.is_published != is_published:
                    post.is_published = is_published
                    changed = True
                if post.source_filename != path.name:
                    post.source_filename = path.name
                    changed = True

            if self._assign_tags_by_slug(db, post, tag_slugs):
                changed = True

            posts_by_filename[path.name] = post
            posts_by_slug[slug] = post
            seen_filenames.add(path.name)

        for post in existing_posts:
            if post.source_filename not in seen_filenames:
                db.delete(post)
                changed = True

        if changed:
            db.flush()
            self._cleanup_orphan_tags(db)
            db.commit()

    def _get_post_by_id(self, db: Session, post_id: int) -> BlogPost | None:
        statement = (
            select(BlogPost)
            .options(selectinload(BlogPost.tags), selectinload(BlogPost.images))
            .where(BlogPost.id == post_id)
        )
        return db.scalars(statement).unique().first()

    def _to_summary(self, post: BlogPost) -> BlogPostSummary:
        return BlogPostSummary(
            slug=post.slug,
            title=post.title,
            summary=post.summary,
            published_at=post.published_at,
            tags=[tag.name for tag in post.tags],
        )

    def _to_detail(self, db: Session, post: BlogPost) -> BlogPostDetail:
        _, markdown_source = self._read_source_document(post.source_filename)
        return BlogPostDetail(
            slug=post.slug,
            title=post.title,
            summary=post.summary,
            published_at=post.published_at,
            tags=[tag.name for tag in post.tags],
            content=markdown_source,
            html=self._render_article_html(db, markdown_source, post_id=post.id),
        )

    def _to_admin_summary(self, post: BlogPost) -> BlogAdminPostSummary:
        return BlogAdminPostSummary(
            id=post.id,
            slug=post.slug,
            title=post.title,
            summary=post.summary,
            published_at=post.published_at,
            updated_at=post.updated_at,
            is_published=post.is_published,
            tags=[tag.name for tag in post.tags],
            source_filename=post.source_filename,
        )

    def _validate_editor(
        self,
        db: Session,
        editor: BlogEditorState,
        exclude_post_id: int | None = None,
    ) -> list[str]:
        errors: list[str] = []
        if not editor.title.strip():
            errors.append("タイトルを入力してください。")
        if not editor.slug.strip():
            errors.append("slug を入力してください。")
        if len(editor.summary.strip()) > 50:
            errors.append("概要は50文字以内で入力してください。")
        if not editor.markdown_source.strip():
            errors.append("Markdown 本文を入力するか .md ファイルをアップロードしてください。")

        existing_tag_slugs = set(self._normalize_existing_tag_slugs(db, editor.selected_tag_slugs))
        if len(existing_tag_slugs) != len(editor.selected_tag_slugs):
            errors.append("存在しないタグが選択されています。")

        if exclude_post_id is None:
            duplicate_statement = select(BlogPost).where(BlogPost.slug == editor.slug)
        else:
            duplicate_statement = select(BlogPost).where(
                BlogPost.slug == editor.slug,
                BlogPost.id != exclude_post_id,
            )
        duplicate = db.scalars(duplicate_statement).first()
        if duplicate is not None:
            errors.append("同じ slug の記事が既に存在します。")
        return errors

    def _build_tag_options(self, db: Session, selected_tag_slugs: list[str]) -> list[BlogTagOption]:
        selected_set = set(selected_tag_slugs)
        tags = db.scalars(select(BlogTag).order_by(BlogTag.name.asc())).all()
        return [
            BlogTagOption(
                id=tag.id,
                name=tag.name,
                slug=tag.slug,
                is_selected=tag.slug in selected_set,
            )
            for tag in tags
        ]

    def _normalize_existing_tag_slugs(self, db: Session, selected_tag_slugs: list[str]) -> list[str]:
        if not selected_tag_slugs:
            return []
        requested = [slug for slug in selected_tag_slugs if slug]
        tags = db.scalars(select(BlogTag).where(BlogTag.slug.in_(requested))).all()
        available = {tag.slug for tag in tags}
        ordered_unique: list[str] = []
        seen: set[str] = set()
        for slug in requested:
            if slug in available and slug not in seen:
                ordered_unique.append(slug)
                seen.add(slug)
        return ordered_unique

    def _resolve_tag_slugs(self, db: Session, tag_names: list[str]) -> list[str]:
        if not tag_names:
            return []
        normalized_candidates = [normalize_slug(name, fallback="tag") for name in tag_names]
        tags = db.scalars(select(BlogTag).where(BlogTag.slug.in_(normalized_candidates))).all()
        existing = {tag.slug for tag in tags}
        return [slug for slug in normalized_candidates if slug in existing]

    def _ensure_tag_slugs(self, db: Session, tag_names: list[str]) -> list[str]:
        if not tag_names:
            return []
        requested_names: list[str] = []
        requested_slugs: list[str] = []
        seen: set[str] = set()
        for name in tag_names:
            cleaned_name = name.strip()
            if not cleaned_name:
                continue
            slug = normalize_slug(cleaned_name, fallback="tag")
            if slug in seen:
                continue
            seen.add(slug)
            requested_names.append(cleaned_name)
            requested_slugs.append(slug)

        existing_tags = db.scalars(select(BlogTag).where(BlogTag.slug.in_(requested_slugs))).all()
        existing_by_slug = {tag.slug: tag for tag in existing_tags}
        ordered_slugs: list[str] = []

        for name, slug in zip(requested_names, requested_slugs, strict=False):
            tag = existing_by_slug.get(slug)
            if tag is None:
                tag = BlogTag(name=name, slug=slug)
                db.add(tag)
                db.flush()
                existing_by_slug[slug] = tag
            else:
                tag.name = name
            ordered_slugs.append(slug)
        return ordered_slugs

    def _assign_tags_by_slug(self, db: Session, post: BlogPost, tag_slugs: list[str]) -> bool:
        normalized_slugs = self._normalize_existing_tag_slugs(db, tag_slugs)
        if normalized_slugs:
            tags = db.scalars(select(BlogTag).where(BlogTag.slug.in_(normalized_slugs))).all()
            tags_by_slug = {tag.slug: tag for tag in tags}
            assigned_tags = [tags_by_slug[slug] for slug in normalized_slugs if slug in tags_by_slug]
        else:
            assigned_tags = []

        current_slugs = [tag.slug for tag in post.tags]
        next_slugs = [tag.slug for tag in assigned_tags]
        post.tags = assigned_tags
        return current_slugs != next_slugs

    def _cleanup_orphan_tags(self, db: Session) -> None:
        orphan_tags = db.scalars(select(BlogTag).where(~BlogTag.posts.any())).all()
        for tag in orphan_tags:
            db.delete(tag)

    def _render_article_html(
        self,
        db: Session,
        markdown_source: str,
        *,
        post_id: int | None = None,
        draft_key: str = "",
        available_images: list[BlogImageAsset] | None = None,
    ) -> str:
        images = available_images if available_images is not None else self.list_image_assets(
            db,
            post_id=post_id,
            draft_key=draft_key,
        )
        image_lookup = {
            image.token: {"url": image.url, "alt": Path(image.original_filename).stem.replace("-", " ")}
            for image in images
        }
        rendered_source = inject_image_placeholders(markdown_source, image_lookup)
        return render_markdown(rendered_source)

    def _to_image_asset(self, image: BlogImage) -> BlogImageAsset:
        return BlogImageAsset(
            id=image.id,
            token=image.token,
            placeholder_tag=f"[[image:{image.token}]]",
            url=f"/blog/media/{image.token}",
            original_filename=image.original_filename,
            created_at=image.created_at,
            is_staged=False,
        )

    def _validate_image_upload(self, uploaded_image: BlogUploadedImage) -> None:
        extension = Path(uploaded_image.filename).suffix.lower()
        if extension not in self.allowed_image_extensions:
            raise BlogValidationError(["アップロードできる画像は png, jpg, jpeg, gif, webp, svg のみです。"])
        if uploaded_image.content_type and not uploaded_image.content_type.startswith("image/"):
            raise BlogValidationError(["画像ファイルのみアップロードできます。"])
        if not uploaded_image.source_bytes:
            raise BlogValidationError(["空の画像ファイルは登録できません。"])

    def _build_image_token(self, db: Session, filename: str) -> tuple[str, str]:
        extension = Path(filename).suffix.lower()
        stem = normalize_slug(Path(filename).stem, fallback="image")
        while True:
            suffix = token_hex(4)
            token = f"{stem}-{suffix}"
            stored_filename = f"{token}{extension}"
            duplicate = db.scalars(select(BlogImage).where(BlogImage.token == token)).first()
            if duplicate is None and not (self.images_dir / stored_filename).exists():
                return token, stored_filename

    def _build_ephemeral_image_token(self, filename: str) -> str:
        stem = normalize_slug(Path(filename).stem, fallback="image")
        return f"{stem}-{token_hex(4)}"

    def _resolve_image_storage(self, db: Session, uploaded_image: BlogUploadedImage) -> tuple[str, str]:
        if not uploaded_image.token.strip():
            return self._build_image_token(db, uploaded_image.filename)

        extension = Path(uploaded_image.filename).suffix.lower()
        token = uploaded_image.token.strip()
        stored_filename = f"{token}{extension}"
        duplicate = db.scalars(select(BlogImage).where(BlogImage.token == token)).first()
        if duplicate is not None or (self.images_dir / stored_filename).exists():
            raise BlogValidationError(["画像タグが重複しました。画像を選び直してください。"])
        return token, stored_filename

    def _build_data_url(self, uploaded_image: BlogUploadedImage) -> str:
        content_type = uploaded_image.content_type or self._guess_image_content_type(uploaded_image.filename)
        encoded = b64encode(uploaded_image.source_bytes).decode("ascii")
        return f"data:{content_type};base64,{encoded}"

    def _guess_image_content_type(self, filename: str) -> str:
        guessed, _ = guess_type(filename)
        return guessed or "application/octet-stream"

    def _collect_image_paths(self, images: list[BlogImage]) -> list[Path]:
        return [self.images_dir / image.stored_filename for image in images]

    def _delete_files(self, file_paths: list[Path]) -> None:
        for file_path in file_paths:
            if file_path.exists():
                file_path.unlink()

    def _write_post_document(
        self,
        source_filename: str,
        title: str,
        summary: str,
        slug: str,
        published_at: date,
        tags: list[str],
        is_published: bool,
        markdown_source: str,
    ) -> None:
        metadata = {
            "title": title.strip(),
            "summary": summary.strip(),
            "slug": slug.strip(),
            "published_at": published_at.isoformat(),
            "tags": ", ".join(tags),
            "is_published": "true" if is_published else "false",
        }
        document = f"{build_front_matter(metadata)}\n{markdown_source.strip()}\n"
        (self.posts_dir / source_filename).write_text(document, encoding="utf-8")

    def _read_source_document(self, source_filename: str) -> tuple[dict[str, str], str]:
        raw_text = (self.posts_dir / source_filename).read_text(encoding="utf-8")
        return parse_front_matter(raw_text)
