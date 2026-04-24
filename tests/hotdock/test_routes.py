import hashlib
import hmac
import json
from datetime import datetime, timedelta

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
    assert "ダッシュボード" in follow.text


def test_workspace_dashboard_prioritizes_actions_and_hides_internal_copy(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Dashboard User",
            "email": "dashboard@example.com",
            "password": "super-secret-password",
            "workspace_name": "Dashboard Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="dashboard-team").one()
    installation = GithubInstallation(
        installation_id=9101,
        github_account_id=88101,
        github_account_login="dashboard-org",
        github_account_type="Organization",
        target_type="Organization",
        installation_status="active",
        claimed_workspace_id=workspace.id,
    )
    db.add(installation)
    db.flush()
    active_repository = Repository(
        workspace_id=workspace.id,
        github_installation_id=installation.id,
        github_repository_id=99101,
        full_name="dashboard-org/repo-a",
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
    candidate_repository = Repository(
        workspace_id=workspace.id,
        github_installation_id=installation.id,
        github_repository_id=99102,
        full_name="dashboard-org/repo-b",
        display_name="repo-b",
        default_branch="main",
        provider="github",
        visibility="private",
        is_available=True,
        is_active=False,
        selection_status="unselected",
        detail_sync_status="not_started",
        sync_status="pending",
    )
    db.add(active_repository)
    db.add(candidate_repository)
    db.flush()
    primary_branch = Branch(
        workspace_id=workspace.id,
        repository_id=active_repository.id,
        name="feature/conflict-ui",
        branch_status="tracked",
        is_active=True,
        is_deleted=False,
        observed_via="webhook",
    )
    manual_branch = Branch(
        workspace_id=workspace.id,
        repository_id=active_repository.id,
        name="fix/theme-token",
        branch_status="tracked",
        is_active=True,
        is_deleted=False,
        observed_via="manual",
        touch_seed_source="manual_diff",
    )
    db.add(primary_branch)
    db.add(manual_branch)
    db.flush()
    now = datetime.utcnow()
    db.add_all(
        [
            BranchFile(
                workspace_id=workspace.id,
                repository_id=active_repository.id,
                branch_id=primary_branch.id,
                path="app/models/user.rb",
                normalized_path="app/models/user.rb",
                change_type="modified",
                last_change_type="modified",
                source_kind="compare",
                last_seen_at=now - timedelta(days=1),
                observed_at=now - timedelta(days=1),
                is_active=True,
                is_conflict=True,
            ),
            BranchFile(
                workspace_id=workspace.id,
                repository_id=active_repository.id,
                branch_id=manual_branch.id,
                path="app/models/user.rb",
                normalized_path="app/models/user.rb",
                change_type="modified",
                last_change_type="modified",
                source_kind="manual_input",
                last_seen_at=now - timedelta(days=2),
                observed_at=now - timedelta(days=2),
                is_active=True,
                is_conflict=True,
            ),
            BranchFile(
                workspace_id=workspace.id,
                repository_id=active_repository.id,
                branch_id=manual_branch.id,
                path="app/ui/Button.tsx",
                normalized_path="app/ui/Button.tsx",
                change_type="modified",
                last_change_type="modified",
                source_kind="manual_input",
                last_seen_at=now - timedelta(days=10),
                observed_at=now - timedelta(days=10),
                is_active=True,
            ),
        ]
    )
    db.add(
        FileCollision(
            repository_id=active_repository.id,
            normalized_path="app/models/user.rb",
            active_branch_count=2,
            collision_status="open",
        )
    )
    db.commit()
    db.close()

    response = client.get("/workspaces/dashboard-team/dashboard")

    assert response.status_code == 200
    assert "競合" in response.text
    assert "ディレクトリ更新状況" in response.text
    assert "Webhook または手動登録で観測されたファイルの変更状況を表示します" in response.text
    assert "競合中のパス" in response.text
    assert "7日以内更新" in response.text
    assert "28日以内更新" in response.text
    assert "長期間更新なし" in response.text
    assert "手動追跡" in response.text
    assert "観測済みツリー" in response.text
    assert "app/models/user.rb" in response.text
    assert "app/ui/Button.tsx" in response.text
    assert "fix/theme-token" in response.text
    assert "概要" in response.text
    assert "監視" in response.text
    assert "ワークスペース" in response.text
    assert "現在の状態" not in response.text
    assert 'aria-label="Breadcrumb"' not in response.text
    assert "競合と監視状況の概要" not in response.text
    assert "集約された観測済みファイルを表示しています" not in response.text
    assert "ブランチ一覧へ移動" not in response.text
    assert "コピー" not in response.text
    assert "リポジトリを見る" not in response.text
    assert "ブランチを見る" not in response.text
    assert "GitHub App 接続" not in response.text
    assert "利用開始ガイド" not in response.text
    assert "最近のイベント" not in response.text
    assert "次に取る行動" not in response.text
    assert "Current Account" not in response.text
    assert "Sync status" not in response.text
    assert "ログイン中のアカウント" not in response.text
    assert "owner 権限の workspace" not in response.text
    assert "installation_repositories webhook と手動同期で候補一覧を更新します。" not in response.text
    assert "監視対象に選んだ repository へ push webhook が入った branch だけを作成・更新します。" not in response.text
    assert "SSR" not in response.text
    assert "FastAPI" not in response.text
    assert "ID: 9101" not in response.text
    assert "unselected / not_started" not in response.text


def test_workspace_dashboard_zero_state_surfaces_next_action(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Zero State User",
            "email": "zero-state@example.com",
            "password": "super-secret-password",
            "workspace_name": "Zero State Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    response = client.get("/workspaces/zero-state-team/dashboard")

    assert response.status_code == 200
    assert "GitHub App が未接続です" in response.text
    assert "GitHub App を連携" in response.text
    assert "Backlogから連携" in response.text
    assert "Git導入" in response.text
    assert "ディレクトリ更新状況" in response.text
    assert "まだ観測済みファイルはありません" in response.text
    assert "Push/Webhook または手動登録で検知されたファイルが表示されます" in response.text
    assert "現在の状態" in response.text
    assert 'aria-label="Breadcrumb"' not in response.text
    assert "競合と監視状況の概要" not in response.text
    assert "次に取る行動" not in response.text
    assert "GitHub App 接続" not in response.text
    assert "利用開始ガイド" not in response.text
    assert "監視対象を選ぶ" not in response.text
    assert "同期状態" not in response.text
    assert "リポジトリを見る" not in response.text
    assert "ブランチを見る" not in response.text


def test_workspace_repositories_unconnected_focuses_on_installation_cta(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Repository User",
            "email": "repository-user@example.com",
            "password": "super-secret-password",
            "workspace_name": "Repository Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    response = client.get("/workspaces/repository-team/repositories")

    assert response.status_code == 200
    assert "GitHub App が未接続です" in response.text
    assert "GitHub App を連携" in response.text
    assert "現在の状態" not in response.text
    assert "接続状態" not in response.text
    assert "候補数" not in response.text
    assert "監視対象" not in response.text
    assert "開始までの流れ" not in response.text
    assert "連携する" not in response.text
    assert "Git未連携です" in response.text
    assert "詳細を見る" not in response.text
    assert "No claimed installations" not in response.text
    assert "連携された repository はページ表示時に候補一覧として同期します。" not in response.text
    assert "repository を選択したあと、その repository への push webhook を受けた branch だけが" not in response.text
    assert "Visibility" not in response.text
    assert "Webhook Sync" not in response.text


def test_workspace_repositories_connected_without_candidates_shows_compact_empty_state(client, monkeypatch):
    async def fake_manual_sync_workspace_installation_repositories(*args, **kwargs):
        return {"repositories_synced": 0, "skipped_installations": 0}

    monkeypatch.setattr(
        "app.hotdock.router_workspace.manual_sync_workspace_installation_repositories",
        fake_manual_sync_workspace_installation_repositories,
    )

    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Connected Empty User",
            "email": "connected-empty@example.com",
            "password": "super-secret-password",
            "workspace_name": "Connected Empty Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="connected-empty-team").one()
    db.add(
        GithubInstallation(
            installation_id=9201,
            github_account_id=88201,
            github_account_login="connected-empty-org",
            github_account_type="Organization",
            target_type="Organization",
            installation_status="active",
            claimed_workspace_id=workspace.id,
        )
    )
    db.commit()
    db.close()

    response = client.get("/workspaces/connected-empty-team/repositories")

    assert response.status_code == 200
    assert "候補 repository はまだ同期されていません" in response.text
    assert "repository を再同期" in response.text
    assert "GitHub App が未接続です" not in response.text
    assert "No claimed installations" not in response.text
    assert "接続状態" not in response.text
    assert "開始までの流れ" not in response.text
    assert "ブランチ補助導線" not in response.text


def test_workspace_repositories_ready_hides_status_helpers_and_last_sync_column(client, monkeypatch):
    async def fake_manual_sync_workspace_installation_repositories(*args, **kwargs):
        return {"repositories_synced": 0, "skipped_installations": 0}

    monkeypatch.setattr(
        "app.hotdock.router_workspace.manual_sync_workspace_installation_repositories",
        fake_manual_sync_workspace_installation_repositories,
    )

    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Repository Ready User",
            "email": "repository-ready@example.com",
            "password": "super-secret-password",
            "workspace_name": "Repository Ready Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="repository-ready-team").one()
    installation = GithubInstallation(
        installation_id=9301,
        github_account_id=88301,
        github_account_login="repository-ready-org",
        github_account_type="Organization",
        target_type="Organization",
        installation_status="active",
        claimed_workspace_id=workspace.id,
    )
    db.add(installation)
    db.flush()
    db.add_all(
        [
            Repository(
                workspace_id=workspace.id,
                github_installation_id=installation.id,
                github_repository_id=99301,
                full_name="repository-ready-org/repo-a",
                display_name="repo-a",
                default_branch="main",
                provider="github",
                visibility="private",
                is_available=True,
                is_active=True,
                selection_status="active",
                detail_sync_status="completed",
                sync_status="active",
            ),
            Repository(
                workspace_id=workspace.id,
                github_installation_id=installation.id,
                github_repository_id=99302,
                full_name="repository-ready-org/repo-b",
                display_name="repo-b",
                default_branch="main",
                provider="github",
                visibility="private",
                is_available=True,
                is_active=False,
                selection_status="inactive",
                detail_sync_status="completed",
                sync_status="inactive",
            ),
        ]
    )
    db.commit()
    db.close()

    response = client.get("/workspaces/repository-ready-team/repositories")

    assert response.status_code == 200
    assert "Last Sync" not in response.text
    assert "現在の監視対象です" not in response.text
    assert "以前の監視対象です" not in response.text
    assert "現在の状態" not in response.text
    assert "開始までの流れ" not in response.text
    assert "ブランチ補助導線" not in response.text


def test_workspace_branches_hides_manual_register_when_github_not_connected(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Branch Viewer",
            "email": "branch-viewer@example.com",
            "password": "super-secret-password",
            "workspace_name": "Branch Viewer Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    response = client.get("/workspaces/branch-viewer-team/branches")

    assert response.status_code == 200
    assert "ブランチ一覧" in response.text
    assert "Branches" not in response.text
    assert "検索" in response.text
    assert "並び順" in response.text
    assert "Git手動登録" not in response.text
    assert "branch 一覧と touched files の主導線は" not in response.text
    assert 'aria-label="Breadcrumb"' not in response.text


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


def test_initial_branch_push_seeds_files_from_commits_payload_without_compare(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Seed User",
            "email": "seed@example.com",
            "password": "super-secret-password",
            "workspace_name": "Seed Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="seed-team").one()
    installation = GithubInstallation(
        installation_id=1002,
        github_account_id=9002,
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
        github_repository_id=502,
        full_name="mock-org/repository-1002",
        display_name="repository-1002",
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

    payload = {
        "installation": {"id": 1002},
        "repository": {
            "id": 502,
            "full_name": "mock-org/repository-1002",
            "name": "repository-1002",
            "default_branch": "main",
            "private": True,
            "pushed_at": 1776239000,
        },
        "ref": "refs/heads/feature/hokkaido-test",
        "before": "0" * 40,
        "after": "1" * 40,
        "created": True,
        "deleted": False,
        "forced": False,
        "head_commit": {"id": "1" * 40},
        "commits": [
            {
                "id": "1" * 40,
                "added": ["test_manual/hokkaido.txt", "test_manual/tohoku.txt", "test_manual/aomori.txt"],
                "modified": [],
                "removed": [],
            }
        ],
    }
    body = json.dumps(payload).encode("utf-8")
    signature = "sha256=" + hmac.new(
        b"test-webhook-secret",
        body,
        hashlib.sha256,
    ).hexdigest()

    response = client.post(
        "/webhooks/github",
        data=body,
        headers={
            "content-type": "application/json",
            "x-hub-signature-256": signature,
            "x-github-delivery": "delivery-seed-1",
            "x-github-event": "push",
        },
    )

    assert response.status_code == 200

    db = SessionLocal()
    branch = db.query(Branch).filter_by(name="feature/hokkaido-test").one()
    event = db.query(BranchEvent).filter_by(webhook_delivery_id="delivery-seed-1").one()
    files = db.query(BranchFile).filter_by(branch_id=branch.id).all()
    file_paths = {item.path for item in files}
    assert event.compare_requested is False
    assert event.reason == "initial_branch_push_seeded_from_payload_commits"
    assert file_paths == {"test_manual/hokkaido.txt", "test_manual/tohoku.txt", "test_manual/aomori.txt"}
    assert branch.touch_seed_status == "seeded_from_payload"
    assert branch.has_authoritative_compare_history is False
    assert branch.touched_files_count == 3
    db.close()


def test_initial_branch_push_with_missing_commit_files_marks_branch_api_error(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Seed Error User",
            "email": "seed-error@example.com",
            "password": "super-secret-password",
            "workspace_name": "Seed Error Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="seed-error-team").one()
    installation = GithubInstallation(
        installation_id=1003,
        github_account_id=9003,
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
        github_repository_id=503,
        full_name="mock-org/repository-1003",
        display_name="repository-1003",
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

    payload = {
        "installation": {"id": 1003},
        "repository": {
            "id": 503,
            "full_name": "mock-org/repository-1003",
            "name": "repository-1003",
            "default_branch": "main",
            "private": True,
            "pushed_at": 1776239000,
        },
        "ref": "refs/heads/feature/error-seed",
        "before": "0" * 40,
        "after": "2" * 40,
        "created": True,
        "deleted": False,
        "forced": False,
        "head_commit": {"id": "2" * 40},
        "commits": [],
    }
    body = json.dumps(payload).encode("utf-8")
    signature = "sha256=" + hmac.new(
        b"test-webhook-secret",
        body,
        hashlib.sha256,
    ).hexdigest()

    response = client.post(
        "/webhooks/github",
        data=body,
        headers={
            "content-type": "application/json",
            "x-hub-signature-256": signature,
            "x-github-delivery": "delivery-seed-2",
            "x-github-event": "push",
        },
    )

    assert response.status_code == 200

    db = SessionLocal()
    branch = db.query(Branch).filter_by(name="feature/error-seed").one()
    event = db.query(BranchEvent).filter_by(webhook_delivery_id="delivery-seed-2").one()
    assert event.compare_requested is False
    assert event.reason == "initial_branch_push_seeded_from_payload_commits_partial"
    assert branch.touch_seed_status == "api_error"
    assert branch.branch_status == "api_error"
    assert branch.touch_seed_error_message is not None
    db.close()


def test_followup_push_after_initial_seed_uses_compare_as_authoritative_source(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Seed Followup User",
            "email": "seed-followup@example.com",
            "password": "super-secret-password",
            "workspace_name": "Seed Followup Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="seed-followup-team").one()
    installation = GithubInstallation(
        installation_id=1004,
        github_account_id=9004,
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
        github_repository_id=504,
        full_name="mock-org/repository-1004",
        display_name="repository-1004",
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

    def sign_and_post(delivery_id: str, payload: dict[str, object]):
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

    first_payload = {
        "installation": {"id": 1004},
        "repository": {
            "id": 504,
            "full_name": "mock-org/repository-1004",
            "name": "repository-1004",
            "default_branch": "main",
            "private": True,
            "pushed_at": 1776239000,
        },
        "ref": "refs/heads/feature/followup",
        "before": "0" * 40,
        "after": "3" * 40,
        "created": True,
        "deleted": False,
        "forced": False,
        "head_commit": {"id": "3" * 40},
        "commits": [{"id": "3" * 40, "added": ["test_manual/seeded.txt"], "modified": [], "removed": []}],
    }
    second_payload = {
        "installation": {"id": 1004},
        "repository": {
            "id": 504,
            "full_name": "mock-org/repository-1004",
            "name": "repository-1004",
            "default_branch": "main",
            "private": True,
            "pushed_at": 1776239010,
        },
        "ref": "refs/heads/feature/followup",
        "before": "3" * 40,
        "after": "4" * 40,
        "created": False,
        "deleted": False,
        "forced": False,
        "head_commit": {"id": "4" * 40},
    }

    first = sign_and_post("delivery-seed-3", first_payload)
    second = sign_and_post("delivery-seed-4", second_payload)

    assert first.status_code == 200
    assert second.status_code == 200

    db = SessionLocal()
    branch = db.query(Branch).filter_by(name="feature/followup").one()
    second_event = db.query(BranchEvent).filter_by(webhook_delivery_id="delivery-seed-4").one()
    assert second_event.compare_requested is True
    assert second_event.reason == "compare_completed"
    assert branch.has_authoritative_compare_history is True
    assert branch.touch_seed_status is None
    db.close()


def test_initial_branch_push_deduplicates_paths_and_uses_last_change_type(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name=\"csrf_token\" value=\"')[1].split('\"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Seed Dedup User",
            "email": "seed-dedup@example.com",
            "password": "super-secret-password",
            "workspace_name": "Seed Dedup Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="seed-dedup-team").one()
    installation = GithubInstallation(
        installation_id=1005,
        github_account_id=9005,
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
        github_repository_id=505,
        full_name="mock-org/repository-1005",
        display_name="repository-1005",
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

    payload = {
        "installation": {"id": 1005},
        "repository": {
            "id": 505,
            "full_name": "mock-org/repository-1005",
            "name": "repository-1005",
            "default_branch": "main",
            "private": True,
            "pushed_at": 1776239000,
        },
        "ref": "refs/heads/feature/dedup",
        "before": "0" * 40,
        "after": "5" * 40,
        "created": True,
        "deleted": False,
        "forced": False,
        "head_commit": {"id": "5" * 40},
        "commits": [
            {
                "id": "5" * 40,
                "added": ["test_manual/california.txt", "test_manual/texas.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "6" * 40,
                "added": [],
                "modified": ["test_manual/california.txt", "test_manual/florida.txt"],
                "removed": ["test_manual/texas.txt", "test_manual/new-york.txt"],
            },
        ],
    }
    body = json.dumps(payload).encode("utf-8")
    signature = "sha256=" + hmac.new(
        b"test-webhook-secret",
        body,
        hashlib.sha256,
    ).hexdigest()

    response = client.post(
        "/webhooks/github",
        data=body,
        headers={
            "content-type": "application/json",
            "x-hub-signature-256": signature,
            "x-github-delivery": "delivery-seed-5",
            "x-github-event": "push",
        },
    )

    assert response.status_code == 200

    db = SessionLocal()
    branch = db.query(Branch).filter_by(name="feature/dedup").one()
    files = {item.path: item for item in db.query(BranchFile).filter_by(branch_id=branch.id).all()}
    assert branch.touched_files_count == 4
    assert files["test_manual/california.txt"].first_seen_change_type == "added"
    assert files["test_manual/california.txt"].last_change_type == "modified"
    assert files["test_manual/california.txt"].source_kind == "initial_payload_seed"
    assert files["test_manual/texas.txt"].first_seen_change_type == "added"
    assert files["test_manual/texas.txt"].last_change_type == "removed"
    assert files["test_manual/texas.txt"].is_active is True
    assert "test_manual/florida.txt" in files
    assert "test_manual/new-york.txt" in files
    db.close()


def test_initial_branch_push_can_create_collision_from_payload_seed(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Seed Collision User",
            "email": "seed-collision@example.com",
            "password": "super-secret-password",
            "workspace_name": "Seed Collision Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="seed-collision-team").one()
    installation = GithubInstallation(
        installation_id=1006,
        github_account_id=9006,
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
        github_repository_id=506,
        full_name="mock-org/repository-1006",
        display_name="repository-1006",
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

    def post_seed_push(delivery_id: str, branch_name: str):
        payload = {
            "installation": {"id": 1006},
            "repository": {
                "id": 506,
                "full_name": "mock-org/repository-1006",
                "name": "repository-1006",
                "default_branch": "main",
                "private": True,
                "pushed_at": 1776239000,
            },
            "ref": f"refs/heads/{branch_name}",
            "before": "0" * 40,
            "after": ("a" if branch_name.endswith("a") else "b") * 40,
            "created": True,
            "deleted": False,
            "forced": False,
            "head_commit": {"id": ("a" if branch_name.endswith("a") else "b") * 40},
            "commits": [
                {
                    "id": ("a" if branch_name.endswith("a") else "b") * 40,
                    "added": ["test_manual/shared.txt"],
                    "modified": [],
                    "removed": [],
                }
            ],
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

    first = post_seed_push("delivery-seed-collision-1", "feature/seed-a")
    second = post_seed_push("delivery-seed-collision-2", "feature/seed-b")

    assert first.status_code == 200
    assert second.status_code == 200

    db = SessionLocal()
    branch_a = db.query(Branch).filter_by(name="feature/seed-a").one()
    branch_b = db.query(Branch).filter_by(name="feature/seed-b").one()
    collision = db.query(FileCollision).filter_by(repository_id=repository.id, normalized_path="test_manual/shared.txt").one()
    assert collision.collision_status == "open"
    assert collision.active_branch_count == 2
    assert branch_a.branch_status == "has_conflict"
    assert branch_b.branch_status == "has_conflict"
    assert branch_a.conflict_files_count == 1
    assert branch_b.conflict_files_count == 1
    assert db.query(BranchFile).filter_by(branch_id=branch_a.id, path="test_manual/shared.txt").one().is_conflict is True
    assert db.query(BranchFile).filter_by(branch_id=branch_b.id, path="test_manual/shared.txt").one().is_conflict is True
    collision_audit = db.query(AuditLog).filter_by(action="file_collision_detected").all()
    assert len(collision_audit) == 1
    assert collision_audit[0].workspace_id == workspace.id
    assert collision_audit[0].event_metadata["path"] == "test_manual/shared.txt"
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

    first = client.post(
        f"/workspaces/switch-team/repositories/{repo_a_id}/activate",
        data={"csrf_token": owner_csrf},
        follow_redirects=True,
    )
    assert first.status_code == 200
    assert "監視対象を切り替えました。以後はこの repository への push webhook を受けた branch だけを表示します。" in first.text

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
    assert repo_b.detail_sync_status == "not_started"
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


def test_manual_branch_registration_creates_branch_files_and_collision(client, monkeypatch):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Manual Owner",
            "email": "manual-owner@example.com",
            "password": "super-secret-password",
            "workspace_name": "Manual Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )
    owner_csrf = client.cookies.get("hotdock_csrf")

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="manual-team").one()
    installation = GithubInstallation(
        installation_id=8101,
        github_account_id=99501,
        github_account_login="manual-org",
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
        github_repository_id=95001,
        full_name="manual-org/repo-a",
        display_name="repo-a",
        default_branch="main",
        provider="github",
        visibility="private",
        is_available=True,
        is_active=True,
        selection_status="active",
        detail_sync_status="not_started",
        sync_status="active",
    )
    db.add(repository)
    db.flush()
    existing_branch = Branch(
        workspace_id=workspace.id,
        repository_id=repository.id,
        name="feature/existing",
        branch_status="tracked",
        is_active=True,
        is_deleted=False,
        observed_via="manual",
        touch_seed_source="manual_diff",
        has_webhook_history=False,
    )
    db.add(existing_branch)
    db.flush()
    existing_file = BranchFile(
        workspace_id=workspace.id,
        repository_id=repository.id,
        branch_id=existing_branch.id,
        path="app/models/user.rb",
        normalized_path="app/models/user.rb",
        change_type="modified",
        last_change_type="modified",
        is_active=True,
    )
    db.add(existing_file)
    db.commit()
    repository_id = repository.id
    db.close()

    async def fake_create_installation_token(self, installation_id):
        assert installation_id == 8101
        return {"token": "installation-token"}

    async def fake_fetch_repository_branch(self, installation_token, repository_full_name, branch_name):
        assert installation_token == "installation-token"
        assert repository_full_name == "manual-org/repo-a"
        assert branch_name == "feature/login-form"
        return {"name": branch_name, "commit": {"sha": "manual-head-sha"}}

    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.create_installation_token", fake_create_installation_token)
    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.fetch_repository_branch", fake_fetch_repository_branch)

    response = client.post(
        f"/workspaces/manual-team/repositories/{repository_id}/branches/manual-register",
        data={
            "csrf_token": owner_csrf,
            "manual_branch_input": "BRANCH:feature/login-form\nM\tapp/models/user.rb\nA\tapp/views/login/new.html.erb\nR100\tapp/models/user_old.rb\tapp/models/user_profile.rb\nD\tapp/tmp/old_login.txt",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "ブランチを手動登録しました。" in response.text

    db = SessionLocal()
    branch = db.query(Branch).filter_by(repository_id=repository_id, name="feature/login-form").one()
    assert branch.observed_via == "manual"
    assert branch.touch_seed_source == "manual_diff"
    assert branch.has_webhook_history is False
    files = db.query(BranchFile).filter_by(branch_id=branch.id).all()
    assert len(files) == 4
    removed_file = db.query(BranchFile).filter_by(branch_id=branch.id, path="app/tmp/old_login.txt").one()
    assert removed_file.is_active is True
    renamed_file = db.query(BranchFile).filter_by(branch_id=branch.id, path="app/models/user_profile.rb").one()
    assert renamed_file.previous_path == "app/models/user_old.rb"
    collision = db.query(FileCollision).filter_by(repository_id=repository_id, normalized_path="app/models/user.rb", collision_status="open").one()
    assert collision.active_branch_count == 2
    db.close()


def test_manual_branch_registration_replaces_snapshot_and_counts_active_paths(client, monkeypatch):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Manual Snapshot",
            "email": "manual-snapshot@example.com",
            "password": "super-secret-password",
            "workspace_name": "Manual Snapshot Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )
    owner_csrf = client.cookies.get("hotdock_csrf")

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="manual-snapshot-team").one()
    installation = GithubInstallation(
        installation_id=8151,
        github_account_id=99551,
        github_account_login="manual-snapshot-org",
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
        github_repository_id=95501,
        full_name="manual-snapshot-org/repo-a",
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
        name="feature/login-form",
        branch_status="tracked",
        is_active=True,
        is_deleted=False,
        observed_via="manual",
        touch_seed_source="manual_diff",
        has_webhook_history=False,
    )
    db.add(branch)
    db.flush()
    db.add_all(
        [
            BranchFile(
                workspace_id=workspace.id,
                repository_id=repository.id,
                branch_id=branch.id,
                path="app/models/user_old.rb",
                normalized_path="app/models/user_old.rb",
                change_type="modified",
                last_change_type="modified",
                is_active=True,
            ),
            BranchFile(
                workspace_id=workspace.id,
                repository_id=repository.id,
                branch_id=branch.id,
                path="app/obsolete.txt",
                normalized_path="app/obsolete.txt",
                change_type="modified",
                last_change_type="modified",
                is_active=True,
            ),
        ]
    )
    db.commit()
    repository_id = repository.id
    db.close()

    async def fake_create_installation_token(self, installation_id):
        assert installation_id == 8151
        return {"token": "installation-token"}

    async def fake_fetch_repository_branch(self, installation_token, repository_full_name, branch_name):
        assert installation_token == "installation-token"
        assert repository_full_name == "manual-snapshot-org/repo-a"
        assert branch_name == "feature/login-form"
        return {"name": branch_name, "commit": {"sha": "snapshot-head-sha"}}

    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.create_installation_token", fake_create_installation_token)
    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.fetch_repository_branch", fake_fetch_repository_branch)

    response = client.post(
        f"/workspaces/manual-snapshot-team/repositories/{repository_id}/branches/manual-register",
        data={
            "csrf_token": owner_csrf,
            "manual_branch_input": "BRANCH:feature/login-form\nM\tapp/models/user.rb\nM\tapp/models/user.rb\nR100\tapp/models/user_old.rb\tapp/models/user_profile.rb\nD\tapp/tmp/old_login.txt",
        },
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "touched files を 3 件反映しました。" in response.text

    db = SessionLocal()
    branch = db.query(Branch).filter_by(repository_id=repository_id, name="feature/login-form").one()
    assert branch.touched_files_count == 3
    renamed_old = db.query(BranchFile).filter_by(branch_id=branch.id, path="app/models/user_old.rb").one()
    assert renamed_old.is_active is False
    dropped_file = db.query(BranchFile).filter_by(branch_id=branch.id, path="app/obsolete.txt").one()
    assert dropped_file.is_active is False
    renamed_new = db.query(BranchFile).filter_by(branch_id=branch.id, path="app/models/user_profile.rb").one()
    assert renamed_new.is_active is True
    removed_file = db.query(BranchFile).filter_by(branch_id=branch.id, path="app/tmp/old_login.txt").one()
    assert removed_file.is_active is True
    db.close()


def test_manual_branch_registration_rejects_invalid_first_line(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Manual Invalid",
            "email": "manual-invalid@example.com",
            "password": "super-secret-password",
            "workspace_name": "Manual Invalid Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )
    owner_csrf = client.cookies.get("hotdock_csrf")

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="manual-invalid-team").one()
    installation = GithubInstallation(
        installation_id=8201,
        github_account_id=99601,
        github_account_login="manual-invalid-org",
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
        github_repository_id=96001,
        full_name="manual-invalid-org/repo-a",
        display_name="repo-a",
        default_branch="main",
        provider="github",
        visibility="private",
        is_available=True,
        is_active=True,
        selection_status="active",
        detail_sync_status="not_started",
        sync_status="active",
    )
    db.add(repository)
    db.commit()
    repository_id = repository.id
    db.close()

    response = client.post(
        f"/workspaces/manual-invalid-team/repositories/{repository_id}/branches/manual-register",
        data={"csrf_token": owner_csrf, "manual_branch_input": "branch:feature/login-form\nM\tapp/models/user.rb"},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "1行目は BRANCH:&lt;branch_name&gt; 形式で入力してください" in response.text or "1行目は BRANCH:<branch_name> 形式で入力してください" in response.text


def test_manual_branch_registration_rejects_non_active_repository(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Manual Inactive",
            "email": "manual-inactive@example.com",
            "password": "super-secret-password",
            "workspace_name": "Manual Inactive Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )
    owner_csrf = client.cookies.get("hotdock_csrf")

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="manual-inactive-team").one()
    installation = GithubInstallation(
        installation_id=8301,
        github_account_id=99701,
        github_account_login="manual-inactive-org",
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
        github_repository_id=97001,
        full_name="manual-inactive-org/repo-a",
        display_name="repo-a",
        default_branch="main",
        provider="github",
        visibility="private",
        is_available=True,
        is_active=False,
        selection_status="inactive",
        detail_sync_status="not_started",
        sync_status="inactive",
    )
    db.add(repository)
    db.commit()
    repository_id = repository.id
    db.close()

    response = client.post(
        f"/workspaces/manual-inactive-team/repositories/{repository_id}/branches/manual-register",
        data={"csrf_token": owner_csrf, "manual_branch_input": "BRANCH:feature/login-form\nM\tapp/models/user.rb"},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "現在この repository は監視対象ではありません" in response.text


def test_workspace_branches_shows_manual_registration_guidance_for_active_repository(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Manual UI",
            "email": "manual-ui@example.com",
            "password": "super-secret-password",
            "workspace_name": "Manual UI Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="manual-ui-team").one()
    installation = GithubInstallation(
        installation_id=8351,
        github_account_id=99751,
        github_account_login="manual-ui-org",
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
        github_repository_id=97501,
        full_name="manual-ui-org/repo-a",
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
    db.commit()
    db.close()

    response = client.get("/workspaces/manual-ui-team/branches")

    assert response.status_code == 200
    assert "既存ブランチを手動登録" in response.text
    assert "Git手動登録を開く" in response.text
    assert response.text.index("ブランチ一覧") < response.text.index("Git手動登録を開く")
    assert 'BRANCH=&quot;feature/login-form&quot;' in response.text or 'BRANCH="feature/login-form"' in response.text
    assert "git diff --name-status origin/master" in response.text
    assert "受け入れ可能な出力例" in response.text
    assert "BRANCH:feature/login-form" in response.text


def test_workspace_branches_table_uses_japanese_labels_and_hides_old_seed_copy(client):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Branch Table User",
            "email": "branch-table@example.com",
            "password": "super-secret-password",
            "workspace_name": "Branch Table Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="branch-table-team").one()
    installation = GithubInstallation(
        installation_id=8451,
        github_account_id=99761,
        github_account_login="branch-table-org",
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
        github_repository_id=97601,
        full_name="branch-table-org/repo-a",
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
    now = datetime.utcnow()
    branch = Branch(
        workspace_id=workspace.id,
        repository_id=repository.id,
        name="feature/table-refresh",
        branch_status="normal",
        current_head_sha="1" * 40,
        touched_files_count=1,
        conflict_files_count=0,
        touch_seed_status="seeded_from_payload",
        has_authoritative_compare_history=False,
        last_push_at=now,
        is_active=True,
        is_deleted=False,
        observed_via="webhook",
    )
    db.add(branch)
    db.flush()
    db.add(
        BranchFile(
            workspace_id=workspace.id,
            repository_id=repository.id,
            branch_id=branch.id,
            path="app/models/user.rb",
            normalized_path="app/models/user.rb",
            change_type="added",
            first_seen_change_type="added",
            last_change_type="added",
            source_kind="initial_payload_seed",
            observed_at=now,
            last_seen_at=now,
            is_active=True,
        )
    )
    db.commit()
    db.close()

    response = client.get("/workspaces/branch-table-team/branches")

    assert response.status_code == 200
    assert "ブランチ一覧" in response.text
    assert "最終更新" in response.text
    assert "検索" in response.text
    assert "並び順" in response.text
    assert "更新日順（新しい順）" in response.text
    assert "Head" not in response.text
    assert "比較待ち" in response.text
    assert "初回seed済み" not in response.text
    assert "初回 push の commits payload から touched files を取り込みました。次回以降の compare が正本です。" not in response.text
    assert "first added" not in response.text
    assert "last added" not in response.text
    assert "branch-table-org/repo-a" not in response.text


def test_manual_branch_registration_rescues_webhook_seeded_branch(client, monkeypatch):
    register_page = client.get("/register")
    anon_csrf = register_page.text.split('name="csrf_token" value="')[1].split('"', 1)[0]
    client.post(
        "/register",
        data={
            "display_name": "Manual Reject",
            "email": "manual-reject@example.com",
            "password": "super-secret-password",
            "workspace_name": "Manual Reject Team",
            "workspace_scale": "1-5 人",
            "next": "/dashboard",
            "csrf_token": anon_csrf,
        },
        follow_redirects=True,
    )
    owner_csrf = client.cookies.get("hotdock_csrf")

    db = SessionLocal()
    workspace = db.query(Workspace).filter_by(slug="manual-reject-team").one()
    installation = GithubInstallation(
        installation_id=8401,
        github_account_id=99801,
        github_account_login="manual-reject-org",
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
        github_repository_id=98001,
        full_name="manual-reject-org/repo-a",
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
        name="feature/login-form",
        branch_status="api_error",
        is_active=True,
        is_deleted=False,
        observed_via="webhook",
        touch_seed_source="payload_commits",
        touch_seed_status="api_error",
        touch_seed_error_message="初回 push payload から touched files を取り出せませんでした。",
        has_authoritative_compare_history=False,
        has_webhook_history=True,
    )
    db.add(branch)
    db.commit()
    repository_id = repository.id
    db.close()

    async def fake_create_installation_token(self, installation_id):
        return {"token": "installation-token"}

    async def fake_fetch_repository_branch(self, installation_token, repository_full_name, branch_name):
        return {"name": branch_name, "commit": {"sha": "manual-head-sha"}}

    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.create_installation_token", fake_create_installation_token)
    monkeypatch.setattr("app.hotdock.services.github.GithubAppClient.fetch_repository_branch", fake_fetch_repository_branch)

    response = client.post(
        f"/workspaces/manual-reject-team/repositories/{repository_id}/branches/manual-register",
        data={"csrf_token": owner_csrf, "manual_branch_input": "BRANCH:feature/login-form\nM\tapp/models/user.rb"},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "手動登録により touched files を確定しました。" in response.text

    db = SessionLocal()
    branch = db.query(Branch).filter_by(repository_id=repository_id, name="feature/login-form").one()
    assert branch.observed_via == "manual"
    assert branch.touch_seed_source == "manual_diff"
    assert branch.touch_seed_status is None
    assert branch.touch_seed_error_message is None
    assert branch.has_webhook_history is True
    assert branch.has_authoritative_compare_history is False
    file_record = db.query(BranchFile).filter_by(branch_id=branch.id, path="app/models/user.rb").one()
    assert file_record.is_active is True
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

    first = client.post(
        f"/workspaces/switch-team/repositories/{repo_a_id}/activate",
        data={"csrf_token": owner_csrf},
        follow_redirects=True,
    )
    assert first.status_code == 200
    assert "監視対象を切り替えました。以後はこの repository への push webhook を受けた branch だけを表示します。" in first.text

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
    assert repo_b.detail_sync_status == "not_started"
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
