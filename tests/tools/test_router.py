import hashlib
import hmac
import json


def test_csv_to_json_page(client):
    response = client.get("/tools/csv-to-json")

    assert response.status_code == 200
    assert "CSV to JSON" in response.text
    assert "JSON変換スタート" in response.text
    assert "/static/js/tools/csv-to-json/app.js" in response.text


def test_conflict_watch_page(client):
    response = client.get("/tools/conflict-watch")

    assert response.status_code == 200
    assert "Conflict Watch" in response.text
    assert "/static/js/tools/conflict-watch/app.js?v=conflict-watch-20250410-09" in response.text
    assert response.headers["cache-control"] == "no-store"
    assert 'data-page-mode="repositories"' in response.text


def test_tools_index_lists_conflict_watch(client):
    response = client.get("/tools")

    assert response.status_code == 200
    assert "Conflict Watch" in response.text
    assert "/tools/conflict-watch" in response.text


def test_conflict_watch_state_api(client):
    response = client.get("/tools/conflict-watch/api/state")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    payload = response.json()
    assert "repositories" in payload
    assert "settings" in payload
    assert "now" in payload


def test_conflict_watch_repositories_api(client):
    response = client.get("/tools/conflict-watch/api/repositories")

    assert response.status_code == 200
    assert response.json() == []


def test_conflict_watch_add_repository(client):
    response = client.post(
        "/tools/conflict-watch/api/repositories",
        json={
            "providerType": "github",
            "repositoryName": "hotdock",
            "externalRepoId": "yoshiakiHirose223/hotdock",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["message"].startswith("repository を追加しました")
    assert any(repository["externalRepoId"] == "yoshiakiHirose223/hotdock" for repository in payload["state"]["repositories"])


def test_conflict_watch_repository_detail_page(client):
    repository_response = client.post(
        "/tools/conflict-watch/api/repositories",
        json={
            "providerType": "github",
            "repositoryName": "hotdock",
            "externalRepoId": "detail/test",
        },
    )
    repository_id = next(
        repository["id"]
        for repository in repository_response.json()["state"]["repositories"]
        if repository["externalRepoId"] == "detail/test"
    )

    response = client.get(f"/tools/conflict-watch/{repository_id}")

    assert response.status_code == 200
    assert 'data-page-mode="repository-detail"' in response.text
    assert f'data-selected-repository-id="{repository_id}"' in response.text


def test_conflict_watch_update_settings_persists_slack_webhook(client):
    response = client.patch(
        "/tools/conflict-watch/api/settings",
        json={
            "staleDays": 21,
            "longUnresolvedDays": 9,
            "rawPayloadRetentionDays": 15,
            "forcePushNoteEnabled": True,
            "suppressNoticeNotifications": False,
            "notificationDestination": "#alerts",
            "slackWebhookUrl": "https://hooks.slack.com/services/test/example",
            "githubWebhookEndpoint": "/tools/conflict-watch/webhooks/github",
            "backlogWebhookEndpoint": "/tools/conflict-watch/webhooks/backlog",
            "githubWebhookSecret": "ghs_demo_hotdock",
            "backlogWebhookSecret": "backlog_demo_secret",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["state"]["settings"]["slackWebhookUrl"] == "https://hooks.slack.com/services/test/example"


def test_conflict_watch_simulated_webhook_creates_branch(client):
    repository_response = client.post(
        "/tools/conflict-watch/api/repositories",
        json={
            "providerType": "github",
            "repositoryName": "hotdock",
            "externalRepoId": "yoshiakiHirose223/hotdock",
        },
    )
    repository_id = repository_response.json()["state"]["repositories"][0]["id"]

    response = client.post(
        "/tools/conflict-watch/api/simulate-webhook",
        json={
            "repositoryId": repository_id,
            "provider": "github",
            "deliveryId": "test-delivery-1",
            "branchName": "feature/conflict-watch",
            "pusher": "tester",
            "signatureStatus": "valid",
            "deletedState": "false",
            "simulateFailure": False,
            "isForced": False,
            "added": "",
            "modified": "app/conflicts/service.py",
            "removed": "",
            "renamed": "",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert any(branch["branchName"] == "feature/conflict-watch" for branch in payload["state"]["branches"])

    branches_response = client.get("/tools/conflict-watch/api/branches", params={"repository_id": repository_id})
    assert branches_response.status_code == 200
    branch_id = branches_response.json()[0]["id"]

    detail_response = client.get(f"/tools/conflict-watch/api/branches/{branch_id}")
    assert detail_response.status_code == 200
    assert detail_response.json()["branch"]["branchName"] == "feature/conflict-watch"


def test_conflict_watch_simulated_webhook_duplicate_delivery_is_idempotent(client):
    repository_response = client.post(
        "/tools/conflict-watch/api/repositories",
        json={
            "providerType": "github",
            "repositoryName": "hotdock",
            "externalRepoId": "yoshiakiHirose223/hotdock",
        },
    )
    repository_id = repository_response.json()["state"]["repositories"][0]["id"]
    request_payload = {
        "repositoryId": repository_id,
        "provider": "github",
        "deliveryId": "duplicate-delivery-1",
        "branchName": "feature/conflict-watch",
        "pusher": "tester",
        "signatureStatus": "valid",
        "deletedState": "false",
        "simulateFailure": False,
        "isForced": False,
        "added": "",
        "modified": "app/conflicts/service.py",
        "removed": "",
        "renamed": "",
    }

    first = client.post("/tools/conflict-watch/api/simulate-webhook", json=request_payload)
    second = client.post("/tools/conflict-watch/api/simulate-webhook", json=request_payload)

    assert first.status_code == 200
    assert second.status_code == 200
    assert "冪等性により再処理をスキップ" in second.json()["message"]
    assert len(second.json()["state"]["webhookEvents"]) == 1


def test_conflict_watch_conflict_detection_creates_notification_log_entry(client):
    repository_response = client.post(
        "/tools/conflict-watch/api/repositories",
        json={
            "providerType": "github",
            "repositoryName": "hotdock",
            "externalRepoId": "notify/test",
        },
    )
    repository_id = repository_response.json()["state"]["repositories"][0]["id"]

    first = client.post(
        "/tools/conflict-watch/api/simulate-webhook",
        json={
            "repositoryId": repository_id,
            "provider": "github",
            "deliveryId": "notify-delivery-1",
            "branchName": "feature/notify-a",
            "pusher": "tester",
            "signatureStatus": "valid",
            "deletedState": "false",
            "simulateFailure": False,
            "isForced": False,
            "added": "",
            "modified": "app/conflicts/service.py",
            "removed": "",
            "renamed": "",
        },
    )
    assert first.status_code == 200

    second = client.post(
        "/tools/conflict-watch/api/simulate-webhook",
        json={
            "repositoryId": repository_id,
            "provider": "github",
            "deliveryId": "notify-delivery-2",
            "branchName": "feature/notify-b",
            "pusher": "tester",
            "signatureStatus": "valid",
            "deletedState": "false",
            "simulateFailure": False,
            "isForced": False,
            "added": "",
            "modified": "app/conflicts/service.py",
            "removed": "",
            "renamed": "",
        },
    )

    assert second.status_code == 200
    notifications = second.json()["state"]["notifications"]
    assert any(notification["notificationType"] == "conflict_created" for notification in notifications)
    assert any(notification["destinationType"] == "slack" for notification in notifications)
    assert any(notification["status"] == "sent" for notification in notifications)


def test_conflict_watch_delete_resolved_conflict(client):
    repository_response = client.post(
        "/tools/conflict-watch/api/repositories",
        json={
            "providerType": "github",
            "repositoryName": "hotdock",
            "externalRepoId": "resolved/delete-test",
        },
    )
    repository_id = repository_response.json()["state"]["repositories"][0]["id"]

    for delivery_id, branch_name in (
        ("resolved-delivery-1", "feature/cleanup-a"),
        ("resolved-delivery-2", "feature/cleanup-b"),
    ):
        response = client.post(
            "/tools/conflict-watch/api/simulate-webhook",
            json={
                "repositoryId": repository_id,
                "provider": "github",
                "deliveryId": delivery_id,
                "branchName": branch_name,
                "pusher": "tester",
                "signatureStatus": "valid",
                "deletedState": "false",
                "simulateFailure": False,
                "isForced": False,
                "added": "",
                "modified": "app/conflicts/service.py",
                "removed": "",
                "renamed": "",
            },
        )
        assert response.status_code == 200

    state_before_resolve = client.get("/tools/conflict-watch/api/state").json()
    branch_to_delete = next(branch for branch in state_before_resolve["branches"] if branch["branchName"] == "feature/cleanup-b")

    branch_delete = client.post(
        f"/tools/conflict-watch/api/branches/{branch_to_delete['id']}/actions",
        json={"action": "delete"},
    )
    assert branch_delete.status_code == 200

    resolved_conflict = next(conflict for conflict in branch_delete.json()["state"]["conflicts"] if conflict["status"] == "resolved")

    delete_response = client.post(f"/tools/conflict-watch/api/conflicts/{resolved_conflict['id']}/delete")

    assert delete_response.status_code == 200
    assert all(conflict["id"] != resolved_conflict["id"] for conflict in delete_response.json()["state"]["conflicts"])

    state_after_delete = client.get("/tools/conflict-watch/api/state")

    assert state_after_delete.status_code == 200
    assert all(conflict["id"] != resolved_conflict["id"] for conflict in state_after_delete.json()["conflicts"])


def test_conflict_watch_branch_file_ignore_resolves_conflict_with_memo(client):
    repository_response = client.post(
        "/tools/conflict-watch/api/repositories",
        json={
            "providerType": "github",
            "repositoryName": "hotdock",
            "externalRepoId": "branch-file-ignore/test",
        },
    )
    repository_id = repository_response.json()["state"]["repositories"][0]["id"]

    for delivery_id, branch_name in (
        ("branch-file-ignore-1", "feature/ignore-a"),
        ("branch-file-ignore-2", "feature/ignore-b"),
    ):
        response = client.post(
            "/tools/conflict-watch/api/simulate-webhook",
            json={
                "repositoryId": repository_id,
                "provider": "github",
                "deliveryId": delivery_id,
                "branchName": branch_name,
                "pusher": "tester",
                "signatureStatus": "valid",
                "deletedState": "false",
                "simulateFailure": False,
                "isForced": False,
                "added": "",
                "modified": "app/conflicts/service.py",
                "removed": "",
                "renamed": "",
            },
        )
        assert response.status_code == 200

    state_before_ignore = client.get("/tools/conflict-watch/api/state").json()
    target_branch = next(branch for branch in state_before_ignore["branches"] if branch["branchName"] == "feature/ignore-a")
    active_conflict = next(conflict for conflict in state_before_ignore["conflicts"] if conflict["status"] == "warning")

    ignore_response = client.post(
        "/tools/conflict-watch/api/branch-file-ignores",
        json={
            "branchId": target_branch["id"],
            "normalizedFilePath": "app/conflicts/service.py",
            "memo": "legacy branch なので個別に除外",
        },
    )

    assert ignore_response.status_code == 200
    ignore_state = ignore_response.json()["state"]
    assert any(
        item["branchId"] == target_branch["id"]
        and item["normalizedFilePath"] == "app/conflicts/service.py"
        and item["memo"] == "legacy branch なので個別に除外"
        for item in ignore_state["branchFileIgnores"]
    )
    updated_conflict = next(conflict for conflict in ignore_state["conflicts"] if conflict["id"] == active_conflict["id"])
    assert updated_conflict["status"] == "resolved"
    assert updated_conflict["activeBranchIds"] == []


def test_conflict_watch_github_webhook_signature_validation(client):
    payload = {
        "ref": "refs/heads/feature/conflict-watch",
        "before": "before-sha",
        "after": "after-sha",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "yoshiakiHirose223/hotdock",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "added": [],
                "modified": ["app/conflicts/service.py"],
                "removed": [],
            }
        ],
    }
    payload_bytes = json.dumps(payload).encode("utf-8")
    secret = "ghs_demo_hotdock"
    signature = "sha256=" + hmac.new(secret.encode("utf-8"), payload_bytes, hashlib.sha256).hexdigest()

    response = client.post(
        "/tools/conflict-watch/webhooks/github",
        content=payload_bytes,
        headers={
            "X-GitHub-Delivery": "github-delivery-1",
            "X-GitHub-Event": "push",
            "X-Hub-Signature-256": signature,
            "Content-Type": "application/json",
        },
    )

    assert response.status_code == 202
    assert response.json()["accepted"] is True

    state_response = client.get("/tools/conflict-watch/api/state")
    branches = state_response.json()["branches"]
    assert any(branch["branchName"] == "feature/conflict-watch" for branch in branches)

    conflicts_response = client.get("/tools/conflict-watch/api/conflicts")
    assert conflicts_response.status_code == 200

    duplicate = client.post(
        "/tools/conflict-watch/webhooks/github",
        content=payload_bytes,
        headers={
            "X-GitHub-Delivery": "github-delivery-1",
            "X-GitHub-Event": "push",
            "X-Hub-Signature-256": signature,
            "Content-Type": "application/json",
        },
    )
    assert duplicate.status_code == 202
    assert "冪等性により再処理をスキップ" in duplicate.json()["message"]


def test_conflict_watch_backlog_webhook_secret_validation(client):
    payload = {
        "project": {"projectKey": "LEGACY"},
        "repository": {"name": "reporting"},
        "ref": "refs/heads/feature/backlog-sync",
        "before": "before-rev",
        "rev": "after-rev",
        "forced": False,
        "deleted": False,
        "user": {"name": "tester"},
        "commits": [
            {
                "added": [],
                "modified": ["app/reports/legacy.php"],
                "removed": [],
            }
        ],
    }

    invalid = client.post(
        "/tools/conflict-watch/webhooks/backlog",
        content=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    assert invalid.status_code == 401

    valid = client.post(
        "/tools/conflict-watch/webhooks/backlog?secret=backlog_demo_secret",
        content=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    assert valid.status_code == 202
    assert valid.json()["accepted"] is True

    repos = client.get("/tools/conflict-watch/api/repositories").json()
    assert any(repo["externalRepoId"] == "LEGACY/reporting" for repo in repos)


def test_csv_column_swap_preview(client):
    response = client.post(
        "/tools/csv-column-swap",
        data={
            "csv_text": "name,score,team\nAlice,90,A\nBob,82,B",
            "first_column": "score",
            "second_column": "team",
            "action": "preview",
        },
    )

    assert response.status_code == 200
    assert "name,team,score" in response.text
