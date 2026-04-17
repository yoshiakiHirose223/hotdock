import hashlib
import hmac
import json

import pytest
from fastapi.testclient import TestClient

from app.hotdock.services.github import sync_claimed_installation_repositories
from app.main import app
from app.core.database import SessionLocal
from app.models.branch import Branch
from app.models.branch_event import BranchEvent
from app.models.branch_file import BranchFile
from app.models.file_collision import FileCollision
from app.models.audit_log import AuditLog
from app.models.github_installation import GithubInstallation
from app.models.github_installation_repository import GithubInstallationRepository
from app.models.repository import Repository
from app.models.workspace import Workspace
from app.models.workspace_invitation import WorkspaceInvitation
from app.models.workspace_member import WorkspaceMember


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
def test_legacy_app_routes_require_login(client, path):
    response = client.get(path, follow_redirects=False)

    assert response.status_code == 303
    assert response.headers["location"].startswith("/login")


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


def test_legacy_app_routes_redirect_to_workspace_after_login(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Legacy User",
            "email": "legacy@example.com",
            "password": "super-secret-password",
            "workspace_name": "Legacy Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    dashboard_redirect = client.get("/app/dashboard", follow_redirects=False)
    assert dashboard_redirect.status_code == 303
    assert dashboard_redirect.headers["location"] == "/dashboard"

    settings_redirect = client.get("/app/settings", follow_redirects=False)
    assert settings_redirect.status_code == 303
    assert settings_redirect.headers["location"] == "/workspaces/legacy-team/settings"

    billing_redirect = client.get("/app/billing", follow_redirects=False)
    assert billing_redirect.status_code == 303
    assert billing_redirect.headers["location"] == "/workspaces/legacy-team/billing"


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

    install_start = client.get("/integrations/github/install/start?workspace=claim-team", follow_redirects=False)
    assert install_start.status_code == 303
    redirect_location = install_start.headers["location"]
    state = redirect_location.split("state=")[1]

    complete = client.get(
        f"/integrations/github/callback?code=mock-code&state={state}&installation_id=1001&setup_action=install",
        follow_redirects=True,
    )
    assert complete.status_code == 200
    assert "Claim Team" in complete.text

    db = SessionLocal()
    installation = db.query(GithubInstallation).filter_by(installation_id=1001).one()
    assert installation.claimed_workspace_id == workspace.id
    db.close()


def test_install_time_callback_without_state_creates_claim_and_redirects_to_login(client):
    response = client.get(
        "/integrations/github/callback?code=mock-code&installation_id=1001&setup_action=install",
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"].startswith("/login?next=%2Fintegrations%2Fgithub%2Fclaim%2F")


def test_install_time_callback_without_installation_id_redirects_to_install(client):
    response = client.get(
        "/integrations/github/callback?code=mock-code&setup_action=install",
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/install/github"


def test_install_time_callback_can_resume_without_session_using_db_intent(client):
    install_start = client.get("/integrations/github/install/start", follow_redirects=False)
    assert install_start.status_code == 303
    state = install_start.headers["location"].split("state=")[1]

    with TestClient(app) as fresh_client:
        callback = fresh_client.get(
            f"/integrations/github/callback?code=mock-code&state={state}&installation_id=1001&setup_action=install",
            follow_redirects=False,
        )

    assert callback.status_code == 303
    assert callback.headers["location"].startswith("/login?next=%2Fintegrations%2Fgithub%2Fclaim%2F")


def test_push_webhook_uses_compare_and_creates_collisions(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Webhook User",
            "email": "webhook@example.com",
            "password": "super-secret-password",
            "workspace_name": "Webhook Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="webhook-team").one()
    installation = GithubInstallation(
        installation_id=1001,
        github_account_id=9001,
        github_account_login="mock-org",
        github_account_type="Organization",
        target_type="Organization",
        installation_status="active",
        claimed_workspace_id=workspace.id,
    )
    db.add(installation)
    db.flush()
    repository = Repository(
        workspace_id=workspace.id,
        github_installation_id=installation.id,
        github_repository_id=501,
        full_name="mock-org/repository-1001",
        display_name="repository-1001",
        default_branch="main",
        provider="github",
        visibility="private",
        is_available=True,
        is_active=True,
        selection_status="active",
        detail_sync_status="completed",
        sync_status="active",
    )
    db.add(repository)
    db.commit()
    db.close()

    def post_push(delivery_id: str, ref: str, before_sha: str, after_sha: str):
        payload = {
            "installation": {"id": 1001},
            "repository": {
                "id": 501,
                "full_name": "mock-org/repository-1001",
                "name": "repository-1001",
                "default_branch": "main",
                "private": True,
                "pushed_at": 1776239000,
            },
            "ref": ref,
            "before": before_sha,
            "after": after_sha,
            "created": False,
            "deleted": False,
            "forced": False,
            "head_commit": {"id": after_sha},
        }
        body = json.dumps(payload).encode("utf-8")
        signature = "sha256=" + hmac.new(
            b"test-webhook-secret",
            body,
            hashlib.sha256,
        ).hexdigest()
        return client.post(
            "/webhooks/github",
            data=body,
            headers={
                "content-type": "application/json",
                "x-hub-signature-256": signature,
                "x-github-delivery": delivery_id,
                "x-github-event": "push",
            },
        )

    first = post_push("delivery-1", "refs/heads/feature-a", "a" * 40, "b" * 40)
    assert first.status_code == 200
    second = post_push("delivery-2", "refs/heads/feature-b", "c" * 40, "d" * 40)
    assert second.status_code == 200

    db = SessionLocal()
    assert db.query(BranchEvent).count() == 2
    assert db.query(Branch).filter_by(name="feature-a").one().touched_files_count > 0
    assert db.query(BranchFile).filter(BranchFile.repository_id == repository.id).count() > 0
    collision = db.query(FileCollision).filter_by(repository_id=repository.id, collision_status="open").one()
    assert collision.active_branch_count == 2
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


def test_owner_can_unlink_installation(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Owner User",
            "email": "unlink-owner@example.com",
            "password": "super-secret-password",
            "workspace_name": "Unlink Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )
    owner_csrf = client.cookies.get("hotdock_csrf")

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="unlink-team").one()
    installation = GithubInstallation(
        installation_id=2001,
        github_account_id=9101,
        github_account_login="unlink-org",
        github_account_type="Organization",
        target_type="Organization",
        installation_status="active",
        claimed_workspace_id=workspace.id,
    )
    db.add(installation)
    db.flush()
    repository = Repository(
        workspace_id=workspace.id,
        github_installation_id=installation.id,
        github_repository_id=701,
        full_name="unlink-org/repository-2001",
        display_name="repository-2001",
        default_branch="main",
        provider="github",
        visibility="private",
        is_available=True,
        is_active=True,
        selection_status="active",
        detail_sync_status="completed",
        sync_status="active",
    )
    db.add(repository)
    db.commit()
    db.close()

    response = client.post(
        "/integrations/github/installations/2001/unlink",
        data={"workspace_slug": "unlink-team", "csrf_token": owner_csrf},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "installation の紐付けを解除しました" in response.text

    db = SessionLocal()
    installation = db.query(GithubInstallation).filter_by(installation_id=2001).one()
    repository = db.query(Repository).filter_by(github_repository_id=701).one()
    assert installation.claimed_workspace_id is None
    assert installation.installation_status == "unlinked"
    assert repository.sync_status == "unlinked"
    assert repository.is_active is False
    assert repository.is_available is False
    assert repository.selection_status == "inaccessible"
    db.close()


def test_last_owner_cannot_be_demoted_or_revoked(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Owner User",
            "email": "owner-guard@example.com",
            "password": "super-secret-password",
            "workspace_name": "Owner Guard Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )
    owner_csrf = client.cookies.get("hotdock_csrf")

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="owner-guard-team").one()
    db.close()

    members_page = client.get(f"/workspaces/{workspace.slug}/members", follow_redirects=True)
    assert members_page.status_code == 200

    db = SessionLocal()
    from app.models.workspace_member import WorkspaceMember

    owner_membership = db.query(WorkspaceMember).filter_by(workspace_id=workspace.id).one()
    db.close()

    demote = client.post(
        f"/workspaces/{workspace.slug}/members/{owner_membership.id}/role",
        data={"role": "admin", "csrf_token": owner_csrf},
        follow_redirects=True,
    )
    assert demote.status_code == 200
    assert "Last owner cannot be removed or demoted" in demote.text

    revoke = client.post(
        f"/workspaces/{workspace.slug}/members/{owner_membership.id}/revoke",
        data={"csrf_token": owner_csrf},
        follow_redirects=True,
    )
    assert revoke.status_code == 200
    assert "Last owner cannot be removed or demoted" in revoke.text


def test_last_owner_cannot_leave_workspace(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Solo Owner",
            "email": "solo-owner@example.com",
            "password": "super-secret-password",
            "workspace_name": "Solo Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )
    owner_csrf = client.cookies.get("hotdock_csrf")

    response = client.post(
        "/workspaces/solo-team/leave",
        data={"csrf_token": owner_csrf},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "Last owner cannot be removed or demoted" in response.text

    db = SessionLocal()
    membership = db.query(WorkspaceMember).join(Workspace, Workspace.id == WorkspaceMember.workspace_id).filter(Workspace.slug == "solo-team").one()
    assert membership.status == "active"
    db.close()


def test_owner_transfer_allows_previous_owner_to_leave(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Primary Owner",
            "email": "primary-owner@example.com",
            "password": "super-secret-password",
            "workspace_name": "Transfer Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )
    owner_csrf = client.cookies.get("hotdock_csrf")

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="transfer-team").one()
    owner = db.query(WorkspaceMember).filter_by(workspace_id=workspace.id).one()
    second_user_workspace = WorkspaceMember(
        workspace_id=workspace.id,
        user_id="member-user-2",
        role="admin",
        status="active",
    )
    db.add(second_user_workspace)
    db.commit()
    db.refresh(second_user_workspace)
    second_member_id = second_user_workspace.id
    db.close()

    promote = client.post(
        f"/workspaces/{workspace.slug}/members/{second_member_id}/role",
        data={"role": "owner", "csrf_token": owner_csrf},
        follow_redirects=True,
    )
    assert promote.status_code == 200

    leave = client.post(
        f"/workspaces/{workspace.slug}/leave",
        data={"csrf_token": owner_csrf},
        follow_redirects=False,
    )
    assert leave.status_code == 303
    assert leave.headers["location"] == "/dashboard"

    db = SessionLocal()
    remaining_members = db.query(WorkspaceMember).filter_by(workspace_id=workspace.id).all()
    assert len(remaining_members) == 1
    assert remaining_members[0].id == second_member_id
    assert remaining_members[0].role == "owner"
    leave_audit = db.query(AuditLog).filter_by(action="workspace_member_leave", workspace_id=workspace.id).one()
    assert leave_audit.actor_id == owner.user_id
    db.close()


def test_workspace_delete_removes_workspace_owned_data_and_unlinks_installations(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Delete Owner",
            "email": "delete-owner@example.com",
            "password": "super-secret-password",
            "workspace_name": "Delete Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )
    owner_csrf = client.cookies.get("hotdock_csrf")

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="delete-team").one()
    installation = GithubInstallation(
        installation_id=3001,
        github_account_id=99001,
        github_account_login="delete-org",
        github_account_type="Organization",
        target_type="Organization",
        installation_status="active",
        claimed_workspace_id=workspace.id,
    )
    db.add(installation)
    db.flush()
    repository = Repository(
        workspace_id=workspace.id,
        github_installation_id=installation.id,
        github_repository_id=801,
        full_name="delete-org/repository-3001",
        display_name="repository-3001",
        default_branch="main",
        provider="github",
        visibility="private",
        is_active=True,
        sync_status="active",
    )
    db.add(repository)
    db.commit()
    db.close()

    response = client.post(
        "/workspaces/delete-team/delete",
        data={"csrf_token": owner_csrf, "confirm_slug": "delete-team"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/workspaces/new"

    db = SessionLocal()
    assert db.query(Workspace).filter_by(slug="delete-team").count() == 0
    assert db.query(WorkspaceMember).filter_by(workspace_id=workspace.id).count() == 0
    assert db.query(Repository).filter_by(workspace_id=workspace.id).count() == 0
    installation = db.query(GithubInstallation).filter_by(installation_id=3001).one()
    assert installation.claimed_workspace_id is None
    assert installation.installation_status == "unlinked"
    deletion_audit = db.query(AuditLog).filter_by(action="workspace_deleted", target_id=workspace.id).one()
    assert deletion_audit.actor_type == "user"
    db.close()

    gone = client.get("/workspaces/delete-team/dashboard", follow_redirects=False)
    assert gone.status_code == 404


def test_unlinked_installation_push_webhook_is_ignored(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Webhook Ignore User",
            "email": "webhook-ignore@example.com",
            "password": "super-secret-password",
            "workspace_name": "Webhook Ignore Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="webhook-ignore-team").one()
    installation = GithubInstallation(
        installation_id=4001,
        github_account_id=99901,
        github_account_login="ignore-org",
        github_account_type="Organization",
        target_type="Organization",
        installation_status="unlinked",
        claimed_workspace_id=None,
    )
    db.add(installation)
    db.commit()
    db.close()

    payload = {
        "installation": {"id": 4001},
        "repository": {"id": 9001, "full_name": "ignore-org/repo", "default_branch": "main"},
        "ref": "refs/heads/feature/test",
        "before": "0000000000000000000000000000000000000001",
        "after": "0000000000000000000000000000000000000002",
        "created": False,
        "deleted": False,
        "forced": False,
        "head_commit": {"id": "0000000000000000000000000000000000000002"},
    }
    body = json.dumps(payload).encode("utf-8")
    signature = "sha256=" + hmac.new(b"test-webhook-secret", body, hashlib.sha256).hexdigest()

    response = client.post(
        "/webhooks/github",
        data=body,
        headers={
            "X-GitHub-Delivery": "delivery-unlinked-1",
            "X-GitHub-Event": "push",
            "X-Hub-Signature-256": signature,
            "Content-Type": "application/json",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ignored"
    assert response.json()["reason"] == "installation_unlinked"


def test_repository_catalog_sync_creates_unselected_candidates_without_branch_fetch(client, monkeypatch):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Catalog Owner",
            "email": "catalog-owner@example.com",
            "password": "super-secret-password",
            "workspace_name": "Catalog Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )
    owner_csrf = client.cookies.get("hotdock_csrf")

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="catalog-team").one()
    installation = GithubInstallation(
        installation_id=5001,
        github_account_id=99101,
        github_account_login="catalog-org",
        github_account_type="Organization",
        target_type="Organization",
        installation_status="active",
        claimed_workspace_id=workspace.id,
    )
    db.add(installation)
    db.commit()
    db.close()

    async def fake_create_installation_token(self, installation_id):
        assert installation_id == 5001
        return {"token": "installation-token"}

    async def fake_fetch_installation_repositories(self, installation_token):
        assert installation_token == "installation-token"
        return [
            {
                "id": 91001,
                "name": "repo-a",
                "full_name": "catalog-org/repo-a",
                "private": True,
                "default_branch": "main",
            },
            {
                "id": 91002,
                "name": "repo-b",
                "full_name": "catalog-org/repo-b",
                "private": False,
                "default_branch": "develop",
            },
        ]

    async def fail_fetch_repository_branches(self, installation_token, full_name):
        raise AssertionError(f"branch fetch should not run during catalog sync: {full_name}")

    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.create_installation_token", fake_create_installation_token)
    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.fetch_installation_repositories", fake_fetch_installation_repositories)
    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.fetch_repository_branches", fail_fetch_repository_branches)

    response = client.post(
        "/workspaces/catalog-team/repositories/sync",
        data={"csrf_token": owner_csrf},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "2 件の repository 候補を反映しました。" in response.text

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="catalog-team").one()
    repositories = db.query(Repository).filter_by(workspace_id=workspace.id).order_by(Repository.github_repository_id.asc()).all()
    installation = db.query(GithubInstallation).filter_by(installation_id=5001).one()
    assert len(repositories) == 2
    assert [repository.selection_status for repository in repositories] == ["unselected", "unselected"]
    assert all(repository.is_active is False for repository in repositories)
    assert all(repository.is_available is True for repository in repositories)
    assert all(repository.detail_sync_status == "not_started" for repository in repositories)
    assert db.query(Branch).filter(Branch.workspace_id == workspace.id).count() == 0
    raw_catalog = db.query(GithubInstallationRepository).filter_by(installation_ref_id=installation.id).count()
    assert raw_catalog == 2
    db.close()


def test_repository_activation_switches_active_target(client, monkeypatch):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Switch Owner",
            "email": "switch-owner@example.com",
            "password": "super-secret-password",
            "workspace_name": "Switch Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )
    owner_csrf = client.cookies.get("hotdock_csrf")

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="switch-team").one()
    installation = GithubInstallation(
        installation_id=6001,
        github_account_id=99201,
        github_account_login="switch-org",
        github_account_type="Organization",
        target_type="Organization",
        installation_status="active",
        claimed_workspace_id=workspace.id,
    )
    db.add(installation)
    db.flush()
    repo_a = Repository(
        workspace_id=workspace.id,
        github_installation_id=installation.id,
        github_repository_id=92001,
        full_name="switch-org/repo-a",
        display_name="repo-a",
        default_branch="main",
        provider="github",
        visibility="private",
        is_available=True,
        is_active=False,
        selection_status="unselected",
        detail_sync_status="not_started",
        sync_status="catalog_synced",
    )
    repo_b = Repository(
        workspace_id=workspace.id,
        github_installation_id=installation.id,
        github_repository_id=92002,
        full_name="switch-org/repo-b",
        display_name="repo-b",
        default_branch="main",
        provider="github",
        visibility="private",
        is_available=True,
        is_active=False,
        selection_status="unselected",
        detail_sync_status="not_started",
        sync_status="catalog_synced",
    )
    db.add_all([repo_a, repo_b])
    db.commit()
    db.refresh(repo_a)
    db.refresh(repo_b)
    repo_a_id = repo_a.id
    repo_b_id = repo_b.id
    db.close()

    async def fake_create_installation_token(self, installation_id):
        assert installation_id == 6001
        return {"token": "installation-token"}

    async def fake_fetch_repository_branches(self, installation_token, full_name):
        return [{"name": "main", "commit": {"sha": f"sha-{full_name}"}}]

    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.create_installation_token", fake_create_installation_token)
    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.fetch_repository_branches", fake_fetch_repository_branches)

    first = client.post(
        f"/workspaces/switch-team/repositories/{repo_a_id}/activate",
        data={"csrf_token": owner_csrf},
        follow_redirects=True,
    )
    assert first.status_code == 200
    assert "監視対象を切り替え、詳細同期を実行しました。" in first.text

    second = client.post(
        f"/workspaces/switch-team/repositories/{repo_b_id}/activate",
        data={"csrf_token": owner_csrf},
        follow_redirects=True,
    )
    assert second.status_code == 200

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="switch-team").one()
    repo_a = db.query(Repository).filter_by(id=repo_a_id).one()
    repo_b = db.query(Repository).filter_by(id=repo_b_id).one()
    assert repo_a.selection_status == "inactive"
    assert repo_a.is_active is False
    assert repo_b.selection_status == "active"
    assert repo_b.is_active is True
    assert repo_b.detail_sync_status == "completed"
    assert db.query(Repository).filter_by(workspace_id=workspace.id, selection_status="active").count() == 1
    db.close()


def test_push_webhook_for_unselected_repository_is_skipped(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Push Owner",
            "email": "push-owner@example.com",
            "password": "super-secret-password",
            "workspace_name": "Push Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="push-team").one()
    installation = GithubInstallation(
        installation_id=7001,
        github_account_id=99301,
        github_account_login="push-org",
        github_account_type="Organization",
        target_type="Organization",
        installation_status="active",
        claimed_workspace_id=workspace.id,
    )
    db.add(installation)
    db.flush()
    repository = Repository(
        workspace_id=workspace.id,
        github_installation_id=installation.id,
        github_repository_id=93001,
        full_name="push-org/repo-a",
        display_name="repo-a",
        default_branch="main",
        provider="github",
        visibility="private",
        is_available=True,
        is_active=False,
        selection_status="unselected",
        detail_sync_status="not_started",
        sync_status="catalog_synced",
    )
    db.add(repository)
    db.commit()
    db.close()

    payload = {
        "installation": {"id": 7001},
        "repository": {"id": 93001, "full_name": "push-org/repo-a", "default_branch": "main"},
        "ref": "refs/heads/feature/test",
        "before": "0000000000000000000000000000000000000001",
        "after": "0000000000000000000000000000000000000002",
        "created": False,
        "deleted": False,
        "forced": False,
        "head_commit": {"id": "0000000000000000000000000000000000000002"},
    }
    body = json.dumps(payload).encode("utf-8")
    signature = "sha256=" + hmac.new(b"test-webhook-secret", body, hashlib.sha256).hexdigest()

    response = client.post(
        "/webhooks/github",
        data=body,
        headers={
            "X-GitHub-Delivery": "delivery-unselected-1",
            "X-GitHub-Event": "push",
            "X-Hub-Signature-256": signature,
            "Content-Type": "application/json",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ignored"
    assert response.json()["reason"] == "repository_not_active"

    db = SessionLocal()
    assert db.query(BranchEvent).count() == 0
    assert db.query(Branch).count() == 0
    db.close()


def test_catalog_sync_marks_removed_active_repository_inaccessible_and_keeps_history(client):
    db = SessionLocal()
    workspace = Workspace(name="History Team", slug="history-team")
    db.add(workspace)
    db.flush()
    installation = GithubInstallation(
        installation_id=8001,
        github_account_id=99401,
        github_account_login="history-org",
        github_account_type="Organization",
        target_type="Organization",
        installation_status="active",
        claimed_workspace_id=workspace.id,
    )
    db.add(installation)
    db.flush()
    repository = Repository(
        workspace_id=workspace.id,
        github_installation_id=installation.id,
        github_repository_id=94001,
        full_name="history-org/repo-a",
        display_name="repo-a",
        default_branch="main",
        provider="github",
        visibility="private",
        is_available=True,
        is_active=True,
        selection_status="active",
        detail_sync_status="completed",
        sync_status="active",
    )
    db.add(repository)
    db.flush()
    branch = Branch(
        workspace_id=workspace.id,
        repository_id=repository.id,
        name="feature/a",
        current_head_sha="abc123",
        last_after_sha="abc123",
        branch_status="normal",
        is_active=True,
        is_deleted=False,
    )
    db.add(branch)
    db.flush()
    branch_file = BranchFile(
        workspace_id=workspace.id,
        repository_id=repository.id,
        branch_id=branch.id,
        path="src/app.py",
        normalized_path="src/app.py",
        change_type="modified",
        last_change_type="modified",
        last_seen_commit_sha="abc123",
        is_active=True,
    )
    db.add(branch_file)
    raw_repo = GithubInstallationRepository(
        installation_ref_id=installation.id,
        workspace_id=workspace.id,
        github_repository_id=94001,
        full_name="history-org/repo-a",
        name="repo-a",
        private=True,
        default_branch="main",
        status="removed",
    )
    db.add(raw_repo)
    db.commit()

    sync_claimed_installation_repositories(db, installation)

    repository = db.query(Repository).filter_by(id=repository.id).one()
    assert repository.selection_status == "inaccessible"
    assert repository.is_active is False
    assert repository.is_available is False
    assert repository.inaccessible_reason == "removed_from_installation"
    assert db.query(Branch).filter_by(repository_id=repository.id).count() == 1
    assert db.query(BranchFile).filter_by(repository_id=repository.id).count() == 1
    db.close()


def test_repository_catalog_sync_creates_unselected_candidates_without_branch_fetch(client, monkeypatch):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Catalog Owner",
            "email": "catalog-owner@example.com",
            "password": "super-secret-password",
            "workspace_name": "Catalog Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )
    owner_csrf = client.cookies.get("hotdock_csrf")

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="catalog-team").one()
    installation = GithubInstallation(
        installation_id=5001,
        github_account_id=99101,
        github_account_login="catalog-org",
        github_account_type="Organization",
        target_type="Organization",
        installation_status="active",
        claimed_workspace_id=workspace.id,
    )
    db.add(installation)
    db.commit()
    db.close()

    async def fake_create_installation_token(self, installation_id):
        assert installation_id == 5001
        return {"token": "installation-token"}

    async def fake_fetch_installation_repositories(self, installation_token):
        assert installation_token == "installation-token"
        return [
            {
                "id": 91001,
                "name": "repo-a",
                "full_name": "catalog-org/repo-a",
                "private": True,
                "default_branch": "main",
            },
            {
                "id": 91002,
                "name": "repo-b",
                "full_name": "catalog-org/repo-b",
                "private": False,
                "default_branch": "develop",
            },
        ]

    async def fail_fetch_repository_branches(self, installation_token, full_name):
        raise AssertionError(f"branch fetch should not run during catalog sync: {full_name}")

    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.create_installation_token", fake_create_installation_token)
    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.fetch_installation_repositories", fake_fetch_installation_repositories)
    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.fetch_repository_branches", fail_fetch_repository_branches)

    response = client.post(
        "/workspaces/catalog-team/repositories/sync",
        data={"csrf_token": owner_csrf},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "2 件の repository 候補を反映しました。" in response.text

    db = SessionLocal()
    repositories = db.query(Repository).filter_by(workspace_id=workspace.id).order_by(Repository.github_repository_id.asc()).all()
    assert len(repositories) == 2
    assert [repository.selection_status for repository in repositories] == ["unselected", "unselected"]
    assert all(repository.is_active is False for repository in repositories)
    assert all(repository.is_available is True for repository in repositories)
    assert all(repository.detail_sync_status == "not_started" for repository in repositories)
    assert db.query(Branch).filter(Branch.workspace_id == workspace.id).count() == 0
    raw_catalog = db.query(GithubInstallationRepository).filter_by(installation_ref_id=installation.id).count()
    assert raw_catalog == 2
    db.close()


def test_repository_activation_switches_active_target(client, monkeypatch):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Switch Owner",
            "email": "switch-owner@example.com",
            "password": "super-secret-password",
            "workspace_name": "Switch Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )
    owner_csrf = client.cookies.get("hotdock_csrf")

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="switch-team").one()
    installation = GithubInstallation(
        installation_id=6001,
        github_account_id=99201,
        github_account_login="switch-org",
        github_account_type="Organization",
        target_type="Organization",
        installation_status="active",
        claimed_workspace_id=workspace.id,
    )
    db.add(installation)
    db.flush()
    repo_a = Repository(
        workspace_id=workspace.id,
        github_installation_id=installation.id,
        github_repository_id=92001,
        full_name="switch-org/repo-a",
        display_name="repo-a",
        default_branch="main",
        provider="github",
        visibility="private",
        is_available=True,
        is_active=False,
        selection_status="unselected",
        detail_sync_status="not_started",
        sync_status="catalog_synced",
    )
    repo_b = Repository(
        workspace_id=workspace.id,
        github_installation_id=installation.id,
        github_repository_id=92002,
        full_name="switch-org/repo-b",
        display_name="repo-b",
        default_branch="main",
        provider="github",
        visibility="private",
        is_available=True,
        is_active=False,
        selection_status="unselected",
        detail_sync_status="not_started",
        sync_status="catalog_synced",
    )
    db.add_all([repo_a, repo_b])
    db.commit()
    db.refresh(repo_a)
    db.refresh(repo_b)
    repo_a_id = repo_a.id
    repo_b_id = repo_b.id
    db.close()

    async def fake_create_installation_token(self, installation_id):
        assert installation_id == 6001
        return {"token": "installation-token"}

    async def fake_fetch_repository_branches(self, installation_token, full_name):
        return [{"name": "main", "commit": {"sha": f"sha-{full_name}"}}]

    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.create_installation_token", fake_create_installation_token)
    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.fetch_repository_branches", fake_fetch_repository_branches)

    first = client.post(
        f"/workspaces/switch-team/repositories/{repo_a_id}/activate",
        data={"csrf_token": owner_csrf},
        follow_redirects=True,
    )
    assert first.status_code == 200
    assert "監視対象を切り替え、詳細同期を実行しました。" in first.text

    second = client.post(
        f"/workspaces/switch-team/repositories/{repo_b_id}/activate",
        data={"csrf_token": owner_csrf},
        follow_redirects=True,
    )
    assert second.status_code == 200

    db = SessionLocal()
    repo_a = db.query(Repository).filter_by(id=repo_a_id).one()
    repo_b = db.query(Repository).filter_by(id=repo_b_id).one()
    assert repo_a.selection_status == "inactive"
    assert repo_a.is_active is False
    assert repo_b.selection_status == "active"
    assert repo_b.is_active is True
    assert repo_b.detail_sync_status == "completed"
    assert db.query(Repository).filter_by(workspace_id=workspace.id, selection_status="active").count() == 1
    db.close()


def test_push_webhook_for_unselected_repository_is_skipped(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Push Owner",
            "email": "push-owner@example.com",
            "password": "super-secret-password",
            "workspace_name": "Push Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="push-team").one()
    installation = GithubInstallation(
        installation_id=7001,
        github_account_id=99301,
        github_account_login="push-org",
        github_account_type="Organization",
        target_type="Organization",
        installation_status="active",
        claimed_workspace_id=workspace.id,
    )
    db.add(installation)
    db.flush()
    repository = Repository(
        workspace_id=workspace.id,
        github_installation_id=installation.id,
        github_repository_id=93001,
        full_name="push-org/repo-a",
        display_name="repo-a",
        default_branch="main",
        provider="github",
        visibility="private",
        is_available=True,
        is_active=False,
        selection_status="unselected",
        detail_sync_status="not_started",
        sync_status="catalog_synced",
    )
    db.add(repository)
    db.commit()
    db.close()

    payload = {
        "installation": {"id": 7001},
        "repository": {"id": 93001, "full_name": "push-org/repo-a", "default_branch": "main"},
        "ref": "refs/heads/feature/test",
        "before": "0000000000000000000000000000000000000001",
        "after": "0000000000000000000000000000000000000002",
        "created": False,
        "deleted": False,
        "forced": False,
        "head_commit": {"id": "0000000000000000000000000000000000000002"},
    }
    body = json.dumps(payload).encode("utf-8")
    signature = "sha256=" + hmac.new(b"test-webhook-secret", body, hashlib.sha256).hexdigest()

    response = client.post(
        "/webhooks/github",
        data=body,
        headers={
            "X-GitHub-Delivery": "delivery-unselected-1",
            "X-GitHub-Event": "push",
            "X-Hub-Signature-256": signature,
            "Content-Type": "application/json",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ignored"
    assert response.json()["reason"] == "repository_not_active"

    db = SessionLocal()
    assert db.query(BranchEvent).count() == 0
    assert db.query(Branch).count() == 0
    db.close()
