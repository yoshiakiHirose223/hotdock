import pytest


@pytest.mark.parametrize(
    "path",
    [
        "/",
        "/features",
        "/integrations",
        "/integrations/github-app",
        "/how-it-works",
        "/pricing",
        "/security",
        "/faq",
        "/docs",
        "/contact",
        "/compare",
    ],
)
def test_public_routes_return_ok(client, path):
    response = client.get(path)

    assert response.status_code == 200
    assert "Hotdock" in response.text


@pytest.mark.parametrize(
    "path",
    [
        "/login",
        "/signup",
        "/install/github",
    ],
)
def test_auth_routes_return_ok(client, path):
    response = client.get(path)

    assert response.status_code == 200
    assert "Hotdock" in response.text


@pytest.mark.parametrize(
    "path",
    [
        "/app",
        "/app/dashboard",
        "/app/projects",
        "/app/projects/1/branches",
        "/app/projects/1/conflicts",
        "/app/projects/1/settings",
        "/app/conflicts",
        "/app/integrations",
        "/app/notifications",
        "/app/settings",
        "/app/billing",
    ],
)
def test_app_routes_return_ok(client, path):
    response = client.get(path)

    assert response.status_code == 200
    assert "Hotdock" in response.text


def test_home_page_explains_two_entry_paths_and_shared_dashboard(client):
    response = client.get("/")

    assert response.status_code == 200
    assert "GitHub App は導入予定です。" in response.text
    assert "SaaS は登録後に git 連携と通知設定を行って開始します。" in response.text
    assert "/app ダッシュボード" in response.text


def test_github_app_page_clearly_states_unavailable_status(client):
    response = client.get("/integrations/github-app")

    assert response.status_code == 200
    assert "GitHub App は未提供です" in response.text
    assert "インストール URL はまだありません" in response.text


def test_removed_legacy_routes_return_not_found(client):
    for path in ("/blog", "/tools", "/exam"):
        response = client.get(path)
        assert response.status_code == 404


def test_projects_page_links_to_project_detail(client):
    response = client.get("/app/projects")

    assert response.status_code == 200
    assert 'href="/app/projects/1/branches"' in response.text
    assert "web-portal" in response.text


def test_project_base_url_redirects_to_branches(client):
    response = client.get("/app/projects/1", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"] == "/app/projects/1/branches"


def test_project_branches_shows_tabs_and_project_metadata(client):
    response = client.get("/app/projects/1/branches")

    assert response.status_code == 200
    assert "Branches" in response.text
    assert "Conflicts" in response.text
    assert "Settings" in response.text
    assert "Overview" not in response.text
    assert "acme/web-portal" in response.text
    assert "Product Team" in response.text


def test_project_branches_page_shows_branch_rows_and_files(client):
    response = client.get("/app/projects/1/branches")

    assert response.status_code == 200
    assert "feature/login-fix" in response.text
    assert "static/css/login.css" in response.text
    assert "Conflict" in response.text
    assert "Total Branches" in response.text


def test_project_branches_filter_can_limit_to_conflict_rows(client):
    response = client.get("/app/projects/1/branches?submitted=1&conflict_only=1&push_only=1")

    assert response.status_code == 200
    assert "feature/login-fix" in response.text
    assert "feature/home-hero-copy" not in response.text


def test_unknown_project_returns_not_found(client):
    response = client.get("/app/projects/999")

    assert response.status_code == 404


def test_bookmark_add_and_remove_updates_sidebar_shortcut(client):
    add_response = client.post(
        "/app/projects/1/bookmark",
        data={"next": "/app/projects/1/branches"},
        follow_redirects=True,
    )

    assert add_response.status_code == 200
    assert "ブックマーク済み" in add_response.text

    sidebar_response = client.get("/app/dashboard")
    assert 'href="/app/projects/1/branches"' in sidebar_response.text
    assert "web-portal" in sidebar_response.text

    remove_response = client.post(
        "/app/projects/1/unbookmark",
        data={"next": "/app/projects/1/branches"},
        follow_redirects=True,
    )

    assert remove_response.status_code == 200
    dashboard_after_remove = client.get("/app/dashboard")
    assert 'href="/app/projects/1/branches"' not in dashboard_after_remove.text
