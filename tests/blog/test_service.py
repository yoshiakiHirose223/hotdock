from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.blog.models import BlogTag
from app.blog.service import BlogService, BlogUploadedDocument, BlogUploadedImage, BlogValidationError
from app.models.base import Base


def build_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)()


def test_create_post_publishes_article_with_selected_db_tags(tmp_path):
    service = BlogService(tmp_path)
    with build_session() as db:
        fastapi_tag = service.create_tag(db, "FastAPI")
        docker_tag = service.create_tag(db, "Docker")

        editor = service.build_editor_state(
            db=db,
            title="Admin Managed Article",
            slug="",
            markdown_source="# Heading\n\n本文です。",
            published_at_text="2025-04-01",
            selected_tag_slugs=[fastapi_tag.slug, docker_tag.slug],
            is_published=True,
        )

        post = service.create_post(db, editor)
        public_post = service.get_post(db, post.slug)

        assert post.is_published is True
        assert public_post is not None
        assert public_post.title == "Admin Managed Article"
        assert public_post.tags == ["FastAPI", "Docker"]
        assert (tmp_path / "admin-managed-article.md").exists()


def test_update_post_rewrites_selected_tags_and_removes_deleted_tag(tmp_path):
    service = BlogService(tmp_path)
    with build_session() as db:
        fastapi_tag = service.create_tag(db, "FastAPI")
        docker_tag = service.create_tag(db, "Docker")
        python_tag = service.create_tag(db, "Python")

        original = service.build_editor_state(
            db=db,
            title="Tag Lifecycle",
            slug="",
            markdown_source="本文",
            published_at_text="2025-04-01",
            selected_tag_slugs=[fastapi_tag.slug, docker_tag.slug],
            is_published=False,
        )
        post = service.create_post(db, original)

        service.delete_tag(db, docker_tag.slug)
        edited = service.build_editor_state(
            db=db,
            title="Tag Lifecycle",
            slug=post.slug,
            markdown_source="本文を更新",
            published_at_text="2025-04-02",
            selected_tag_slugs=[python_tag.slug],
            is_published=True,
            post_id=post.id,
            current_source_filename=post.source_filename,
        )
        service.update_post(db, post.id, edited, publish_state=False)

        stored_tags = db.scalars(select(BlogTag).order_by(BlogTag.slug)).all()
        public_post = service.get_post(db, post.slug)
        document = (tmp_path / f"{post.slug}.md").read_text(encoding="utf-8")

        assert public_post is None
        assert [tag.name for tag in stored_tags] == ["FastAPI", "Python"]
        assert "is_published: false" in document
        assert "tags: Python" in document


def test_uploaded_document_selects_existing_tags_and_overrides_fields(tmp_path):
    service = BlogService(tmp_path)
    with build_session() as db:
        service.create_tag(db, "FastAPI")
        service.create_tag(db, "Python")

        editor = service.build_editor_state(
            db=db,
            title="Old Title",
            slug="old-title",
            markdown_source="旧い本文",
            published_at_text="2025-04-01",
            selected_tag_slugs=[],
            is_published=False,
            uploaded_document=BlogUploadedDocument(
                filename="new-article.md",
                source_text="""---
title: New Title
summary: 記事の概要です
slug: new-title
published_at: 2025-05-01
tags: FastAPI, Python
---
# 新しい本文
""",
            ),
        )

        assert editor.title == "New Title"
        assert editor.summary == "記事の概要です"
        assert editor.slug == "new-title"
        assert editor.selected_tag_slugs == ["fastapi", "python"]
        assert editor.markdown_source == "# 新しい本文"


def test_summary_is_saved_to_db_and_markdown_front_matter(tmp_path):
    service = BlogService(tmp_path)
    with build_session() as db:
        editor = service.build_editor_state(
            db=db,
            title="Summary Article",
            summary_text="一覧向けの短い説明です",
            slug="",
            markdown_source="本文です。",
            published_at_text="2025-04-01",
            selected_tag_slugs=[],
            is_published=True,
        )

        post = service.create_post(db, editor)
        admin_post = service.list_admin_posts(db)[0]
        document = (tmp_path / f"{post.slug}.md").read_text(encoding="utf-8")

        assert post.summary == "一覧向けの短い説明です"
        assert admin_post.summary == "一覧向けの短い説明です"
        assert "summary: 一覧向けの短い説明です" in document


def test_summary_must_be_50_characters_or_less(tmp_path):
    service = BlogService(tmp_path)
    with build_session() as db:
        editor = service.build_editor_state(
            db=db,
            title="Summary Validation",
            summary_text="あ" * 51,
            slug="summary-validation",
            markdown_source="本文です。",
            published_at_text="2025-04-01",
            selected_tag_slugs=[],
            is_published=False,
        )

        try:
            service.create_post(db, editor)
        except BlogValidationError as exc:
            assert exc.errors == ["概要は50文字以内で入力してください。"]
        else:
            raise AssertionError("概要の最大文字数バリデーションが動作していません。")


def test_uploaded_images_generate_placeholders_and_render_in_preview(tmp_path):
    service = BlogService(tmp_path)
    with build_session() as db:
        uploaded_images = [
            BlogUploadedImage(
                filename="architecture.png",
                content_type="image/png",
                source_bytes=b"\x89PNG\r\n\x1a\npreview",
                token="architecture-image-token",
            ),
            BlogUploadedImage(
                filename="flow.webp",
                content_type="image/webp",
                source_bytes=b"RIFFxxxxWEBPVP8 ",
                token="flow-image-token",
            ),
        ]

        staged_images = service.build_staged_images_from_uploads(uploaded_images)
        placeholder = f"[[image:{uploaded_images[0].token}]]"
        editor = service.build_editor_state(
            db=db,
            title="Image Article",
            slug="image-article",
            markdown_source=f"# Heading\n\n{placeholder}\n",
            published_at_text="2025-04-01",
            selected_tag_slugs=[],
            is_published=False,
            staged_images=staged_images,
        )

        post = service.create_post(db, editor, uploaded_images=uploaded_images)
        public_post = service.get_post(db, post.slug)

        assert len(editor.available_images) == 2
        assert "data:image/png;base64" in editor.preview_html
        assert "<img" in editor.preview_html
        assert public_post is None

        service.set_publish_state(db, post.id, True)
        public_post = service.get_post(db, post.slug)

        assert public_post is not None
        assert "<img" in public_post.html


def test_unicode_image_placeholder_renders_in_public_article(tmp_path):
    service = BlogService(tmp_path)
    with build_session() as db:
        uploaded_images = [
            BlogUploadedImage(
                filename="スクリーンショット 2026-03-31 191144.png",
                content_type="image/png",
                source_bytes=b"\x89PNG\r\n\x1a\npreview",
                token="screenshot-jp-token",
            )
        ]
        staged_images = service.build_staged_images_from_uploads(uploaded_images)

        editor = service.build_editor_state(
            db=db,
            title="Unicode Image Article",
            slug="unicode-image-article",
            markdown_source=f"# Heading\n\n[[image:{uploaded_images[0].token}]]\n",
            published_at_text="2025-04-01",
            selected_tag_slugs=[],
            is_published=True,
            staged_images=staged_images,
        )
        post = service.create_post(db, editor, uploaded_images=uploaded_images)
        public_post = service.get_post(db, post.slug)

        assert public_post is not None
        assert uploaded_images[0].token in public_post.html
        assert "<img" in public_post.html


def test_update_post_applies_staged_image_deletion_only_on_save(tmp_path):
    service = BlogService(tmp_path)
    with build_session() as db:
        original_image = BlogUploadedImage(
            filename="existing-image.png",
            content_type="image/png",
            source_bytes=b"\x89PNG\r\n\x1a\npreview",
            token="existing-image-token",
        )
        editor = service.build_editor_state(
            db=db,
            title="Editable Article",
            slug="editable-article",
            markdown_source="本文",
            published_at_text="2025-04-01",
            selected_tag_slugs=[],
            is_published=True,
            staged_images=service.build_staged_images_from_uploads([original_image]),
        )
        post = service.create_post(db, editor, uploaded_images=[original_image])
        initial_images = service.list_image_assets(db, post_id=post.id)

        preview_editor = service.build_editor_state(
            db=db,
            title=post.title,
            slug=post.slug,
            markdown_source="本文",
            published_at_text="2025-04-01",
            selected_tag_slugs=[],
            is_published=True,
            post_id=post.id,
            deleted_image_ids=[initial_images[0].id],
            staged_images=[],
        )

        assert preview_editor.available_images == []
        assert len(service.list_image_assets(db, post_id=post.id)) == 1

        service.update_post(
            db,
            post.id,
            preview_editor,
            deleted_image_ids=[initial_images[0].id],
        )

        assert service.list_image_assets(db, post_id=post.id) == []
