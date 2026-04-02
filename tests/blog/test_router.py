from app.blog.router import BLOG_ADMIN_BASE_PATH, service
from app.core.database import SessionLocal


def login_blog_admin(client):
    response = client.post(
        f"{BLOG_ADMIN_BASE_PATH}/login",
        data={
            "username": "root",
            "password": "root",
            "next_path": BLOG_ADMIN_BASE_PATH,
        },
        follow_redirects=False,
    )

    assert response.status_code == 303


def test_blog_index_displays_posts(client):
    response = client.get("/blog")

    assert response.status_code == 200
    assert "モノリス構成で始める FastAPI サービス基盤" in response.text


def test_blog_detail_displays_markdown_content(client):
    response = client.get("/blog/welcome-to-platform")

    assert response.status_code == 200
    assert "最初は単一アプリで進める" in response.text


def test_blog_admin_index_redirects_to_login_when_unauthenticated(client):
    response = client.get(BLOG_ADMIN_BASE_PATH, follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"].startswith(f"{BLOG_ADMIN_BASE_PATH}/login")


def test_blog_admin_index_displays_management_screen_after_login(client):
    login_blog_admin(client)

    response = client.get(BLOG_ADMIN_BASE_PATH)

    assert response.status_code == 200
    assert "記事管理" in response.text
    assert 'href="/blog/admin"' not in response.text


def test_blog_admin_preview_api_applies_uploaded_markdown_without_redirect(client):
    login_blog_admin(client)

    response = client.post(
        f"{BLOG_ADMIN_BASE_PATH}/api/preview",
        data={
            "title": "",
            "summary": "",
            "slug": "",
            "markdown_source": "",
            "published_at": "",
            "is_published": "false",
            "import_uploaded_file": "true",
        },
        files={
            "upload_file": (
                "preview-post.md",
                b"---\ntitle: Preview Title\nslug: preview-title\npublished_at: 2025-04-10\n---\n# Preview Body\n",
                "text/markdown",
            )
        },
    )

    payload = response.json()

    assert response.status_code == 200
    assert payload["editor"]["title"] == "Preview Title"
    assert payload["editor"]["slug"] == "preview-title"
    assert "Preview Body" in payload["editor"]["preview_html"]


def test_blog_admin_tag_create_api_returns_available_tags(client):
    login_blog_admin(client)

    response = client.post(
        f"{BLOG_ADMIN_BASE_PATH}/api/tags",
        data={"tag_name": "FastAPI", "selected_tag_slugs": []},
    )

    payload = response.json()

    assert response.status_code == 200
    assert payload["selected_tag_slugs"] == ["fastapi"]
    assert any(tag["slug"] == "fastapi" and tag["is_selected"] for tag in payload["available_tags"])


def test_blog_admin_visibility_api_updates_publish_state(client):
    login_blog_admin(client)

    with SessionLocal() as db:
        post_id = service.list_admin_posts(db)[0].id

    response = client.post(
        f"{BLOG_ADMIN_BASE_PATH}/{post_id}/visibility",
        data={"is_published": "false"},
    )

    payload = response.json()

    assert response.status_code == 200
    assert payload["is_published"] is False
