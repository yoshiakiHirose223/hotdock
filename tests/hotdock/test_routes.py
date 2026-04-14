import pytest

from app.core.database import SessionLocal
from app.models.github_installation import GithubInstallation
from app.models.workspace import Workspace
from app.models.workspace_invitation import WorkspaceInvitation


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


def test_dashboard_requires_login(client):
    response = client.get("/dashboard", follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"].startswith("/login")


def test_register_creates_workspace_and_redirects_to_workspace_dashboard(client):
    response = client.get("/register")
    csrf_token = client.cookies.get("session")
    assert response.status_code == 200
    anon_csrf = response.text.split('name="csrf_token" value="')[1].split('"', 1)[0]

    submit = client.post(
        "/register",
        data={
            "display_name": "Owner User",
            "email": "owner@example.com",
            "password": "super-secret-password",
            "workspace_name": "Example Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=False,
    )

    assert submit.status_code == 303
    assert submit.headers["location"] == "/dashboard"

    follow = client.get("/dashboard", follow_redirects=True)
    assert follow.status_code == 200
    assert "Example Team" in follow.text
    assert "Workspace Dashboard" in follow.text


def test_github_claim_flow_claims_installation_to_workspace(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Owner User",
            "email": "owner2@example.com",
            "password": "super-secret-password",
            "workspace_name": "Claim Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="claim-team").one()
    db.close()

    setup_response = client.get("/integrations/github/setup?installation_id=1001", follow_redirects=False)
    assert setup_response.status_code == 303
    claim_url = setup_response.headers["location"]
    claim_token = claim_url.rsplit("/", 1)[-1]
    csrf_token = client.cookies.get("hotdock_csrf")

    select_workspace = client.post(
        f"/integrations/github/claim/{claim_token}/workspace",
        data={"workspace_id": workspace.id, "csrf_token": csrf_token},
        follow_redirects=False,
    )
    assert select_workspace.status_code == 303
    assert select_workspace.headers["location"].startswith("/integrations/github/authorize/start")

    complete = client.get(select_workspace.headers["location"], follow_redirects=True)
    assert complete.status_code == 200
    assert "Claim Team" in complete.text

    db = SessionLocal()
    installation = db.query(GithubInstallation).filter_by(installation_id=1001).one()
    assert installation.claimed_workspace_id == workspace.id
    db.close()


def test_workspace_invitation_accept_flow(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Owner User",
            "email": "owner3@example.com",
            "password": "super-secret-password",
            "workspace_name": "Invite Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )
    owner_csrf = client.cookies.get("hotdock_csrf")
    invite = client.post(
        "/workspaces/invite-team/members/invite",
        data={"email": "member@example.com", "role": "member", "csrf_token": owner_csrf},
        follow_redirects=True,
    )
    assert invite.status_code == 200
    assert "招待リンクを発行しました" in invite.text

    db = SessionLocal()
    invitation = db.query(WorkspaceInvitation).filter_by(email="member@example.com").one()
    db.close()
    invitation_token = invite.text.split("/invitations/")[1].split("<", 1)[0]
    assert invitation_token

    client.post("/logout", data={"csrf_token": owner_csrf}, follow_redirects=False)

    register_member = client.get("/register")
    member_anon_csrf = register_member.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Member User",
            "email": "member@example.com",
            "password": "super-secret-password",
            "workspace_name": "",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": member_anon_csrf,
        },
        follow_redirects=True,
    )
    member_csrf = client.cookies.get("hotdock_csrf")
    accept = client.post(f"/invitations/{invitation_token}/accept", data={"csrf_token": member_csrf}, follow_redirects=True)
    assert accept.status_code == 200
    assert "Invite Team" in accept.text
