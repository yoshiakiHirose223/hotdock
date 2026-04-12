import hashlib
import hmac
import json


def post_github_webhook(client, delivery_id, payload, *, secret="ghs_demo_hotdock"):
    payload_bytes = json.dumps(payload).encode("utf-8")
    signature = "sha256=" + hmac.new(secret.encode("utf-8"), payload_bytes, hashlib.sha256).hexdigest()
    return client.post(
        "/tools/conflict-watch/webhooks/github",
        content=payload_bytes,
        headers={
            "X-GitHub-Delivery": delivery_id,
            "X-GitHub-Event": "push",
            "X-Hub-Signature-256": signature,
            "Content-Type": "application/json",
        },
    )


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
    assert "/static/js/tools/conflict-watch/app.js?v=conflict-watch-20250410-22" in response.text
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
            "processingTraceEnabled": False,
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
    assert payload["state"]["settings"]["processingTraceEnabled"] is False


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


def test_conflict_watch_simulated_webhook_raw_payload_is_viewable(client):
    repository_response = client.post(
        "/tools/conflict-watch/api/repositories",
        json={
            "providerType": "github",
            "repositoryName": "hotdock",
            "externalRepoId": "raw-payload/simulated",
        },
    )
    repository_id = repository_response.json()["state"]["repositories"][0]["id"]

    response = client.post(
        "/tools/conflict-watch/api/simulate-webhook",
        json={
            "repositoryId": repository_id,
            "provider": "github",
            "deliveryId": "raw-payload-simulated-1",
            "branchName": "feature/raw-payload",
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
    event = next(
        item
        for item in response.json()["state"]["webhookEvents"]
        if item["deliveryId"] == "raw-payload-simulated-1"
    )

    payload_response = client.get(f"/tools/conflict-watch/api/webhook-events/{event['id']}/raw-payload")

    assert payload_response.status_code == 200
    payload = payload_response.json()
    assert payload["isAvailable"] is True
    assert payload["rawPayloadRef"].endswith(".json")
    raw_content = json.loads(payload["content"])
    assert raw_content["branchName"] == "feature/raw-payload"
    assert raw_content["modified"] == "app/conflicts/service.py"


def test_conflict_watch_simulated_webhook_processing_trace_is_viewable(client):
    repository_response = client.post(
        "/tools/conflict-watch/api/repositories",
        json={
            "providerType": "github",
            "repositoryName": "hotdock",
            "externalRepoId": "processing-trace/simulated",
        },
    )
    repository_id = repository_response.json()["state"]["repositories"][0]["id"]

    response = client.post(
        "/tools/conflict-watch/api/simulate-webhook",
        json={
            "repositoryId": repository_id,
            "provider": "github",
            "deliveryId": "processing-trace-simulated-1",
            "branchName": "feature/processing-trace",
            "pusher": "tester",
            "signatureStatus": "valid",
            "deletedState": "false",
            "simulateFailure": False,
            "isForced": False,
            "added": "app/conflicts/service.py",
            "modified": "",
            "removed": "",
            "renamed": "",
        },
    )

    assert response.status_code == 200
    event = next(
        item
        for item in response.json()["state"]["webhookEvents"]
        if item["deliveryId"] == "processing-trace-simulated-1"
    )

    trace_response = client.get(f"/tools/conflict-watch/api/webhook-events/{event['id']}/processing-trace")

    assert trace_response.status_code == 200
    trace_payload = trace_response.json()
    assert trace_payload["isAvailable"] is True
    trace_json = json.loads(trace_payload["content"])
    assert trace_json["totalElapsedMs"] >= 0
    assert trace_json["totalElapsedSeconds"] >= 0
    assert all("elapsedMs" in entry and entry["elapsedMs"] >= 0 for entry in trace_json["entries"])
    assert all("elapsedSeconds" in entry and entry["elapsedSeconds"] >= 0 for entry in trace_json["entries"])
    labels = [entry["label"] for entry in trace_json["entries"]]
    assert "webhook_event_created" in labels
    assert "normal_push_completed" in labels


def test_conflict_watch_processing_trace_can_be_disabled_from_settings(client):
    settings_response = client.patch(
        "/tools/conflict-watch/api/settings",
        json={
            "staleDays": 21,
            "longUnresolvedDays": 9,
            "rawPayloadRetentionDays": 15,
            "processingTraceEnabled": False,
            "forcePushNoteEnabled": True,
            "suppressNoticeNotifications": False,
            "notificationDestination": "#alerts",
            "slackWebhookUrl": "",
            "githubWebhookEndpoint": "/tools/conflict-watch/webhooks/github",
            "backlogWebhookEndpoint": "/tools/conflict-watch/webhooks/backlog",
            "githubWebhookSecret": "ghs_demo_hotdock",
            "backlogWebhookSecret": "backlog_demo_secret",
        },
    )
    assert settings_response.status_code == 200

    repository_response = client.post(
        "/tools/conflict-watch/api/repositories",
        json={
            "providerType": "github",
            "repositoryName": "hotdock",
            "externalRepoId": "processing-trace/disabled",
        },
    )
    repository_id = repository_response.json()["state"]["repositories"][0]["id"]

    response = client.post(
        "/tools/conflict-watch/api/simulate-webhook",
        json={
            "repositoryId": repository_id,
            "provider": "github",
            "deliveryId": "processing-trace-disabled-1",
            "branchName": "feature/processing-trace-disabled",
            "pusher": "tester",
            "signatureStatus": "valid",
            "deletedState": "false",
            "simulateFailure": False,
            "isForced": False,
            "added": "app/conflicts/service.py",
            "modified": "",
            "removed": "",
            "renamed": "",
        },
    )

    assert response.status_code == 200
    event = next(
        item
        for item in response.json()["state"]["webhookEvents"]
        if item["deliveryId"] == "processing-trace-disabled-1"
    )

    trace_response = client.get(f"/tools/conflict-watch/api/webhook-events/{event['id']}/processing-trace")

    assert trace_response.status_code == 200
    assert trace_response.json()["isAvailable"] is False


def test_conflict_watch_simulated_webhook_payload_hash_is_fixed_length(client):
    repository_response = client.post(
        "/tools/conflict-watch/api/repositories",
        json={
            "providerType": "github",
            "repositoryName": "hotdock",
            "externalRepoId": "payload-hash/fixed-length",
        },
    )
    repository_id = repository_response.json()["state"]["repositories"][0]["id"]
    modified_paths = "\n".join(
        f"very/long/path/segment/{index:03d}/" + ("nested-" * 8) + f"file-{index:03d}.py"
        for index in range(40)
    )

    response = client.post(
        "/tools/conflict-watch/api/simulate-webhook",
        json={
            "repositoryId": repository_id,
            "provider": "github",
            "deliveryId": "payload-hash-fixed-length-1",
            "branchName": "feature/payload-hash-fixed-length",
            "pusher": "tester",
            "signatureStatus": "valid",
            "deletedState": "false",
            "simulateFailure": False,
            "isForced": False,
            "added": "",
            "modified": modified_paths,
            "removed": "",
            "renamed": "",
        },
    )

    assert response.status_code == 200
    event = next(
        item
        for item in response.json()["state"]["webhookEvents"]
        if item["deliveryId"] == "payload-hash-fixed-length-1"
    )
    assert event["payloadHash"].startswith("sha256:")
    assert len(event["payloadHash"]) == 71


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
    assert all(
        branch["id"] != branch_to_delete["id"]
        for branch in branch_delete.json()["state"]["branches"]
    )

    resolved_conflict = next(conflict for conflict in branch_delete.json()["state"]["conflicts"] if conflict["status"] == "resolved")
    assert {item["branchName"] for item in resolved_conflict["lastRelatedBranches"]} == {
        "feature/cleanup-a",
        "feature/cleanup-b",
    }
    assert resolved_conflict["resolvedReason"] == "branch_deleted"
    assert resolved_conflict["resolvedContext"]["branchName"] == "feature/cleanup-b"
    assert "ブランチ削除で解消" in resolved_conflict["resolvedContext"]["summary"]
    assert any(entry["label"] == "resolved" for entry in resolved_conflict["history"])

    delete_response = client.post(f"/tools/conflict-watch/api/conflicts/{resolved_conflict['id']}/delete")

    assert delete_response.status_code == 200
    assert all(conflict["id"] != resolved_conflict["id"] for conflict in delete_response.json()["state"]["conflicts"])

    state_after_delete = client.get("/tools/conflict-watch/api/state")

    assert state_after_delete.status_code == 200
    assert all(conflict["id"] != resolved_conflict["id"] for conflict in state_after_delete.json()["conflicts"])
    assert all(branch["id"] != branch_to_delete["id"] for branch in state_after_delete.json()["branches"])


def test_conflict_watch_add_ignore_rule_resolves_conflict(client):
    repository_response = client.post(
        "/tools/conflict-watch/api/repositories",
        json={
            "providerType": "github",
            "repositoryName": "hotdock",
            "externalRepoId": "ignore-rule/test",
        },
    )
    repository_id = repository_response.json()["state"]["repositories"][0]["id"]

    for delivery_id, branch_name in (
        ("ignore-rule-1", "feature/ignore-rule-a"),
        ("ignore-rule-2", "feature/ignore-rule-b"),
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
                "modified": "generated/conflicts/report.json",
                "removed": "",
                "renamed": "",
            },
        )
        assert response.status_code == 200

    ignore_response = client.post(
        "/tools/conflict-watch/api/ignore-rules",
        json={
            "repositoryId": repository_id,
            "pattern": "generated/**",
        },
    )

    assert ignore_response.status_code == 200
    ignore_state = ignore_response.json()["state"]
    assert any(
        rule["repositoryId"] == repository_id
        and rule["pattern"] == "generated/**"
        and rule["isActive"] is True
        for rule in ignore_state["ignoreRules"]
    )
    resolved_conflict = next(
        conflict
        for conflict in ignore_state["conflicts"]
        if conflict["normalizedFilePath"] == "generated/conflicts/report.json"
    )
    assert resolved_conflict["status"] == "resolved"
    assert resolved_conflict["activeBranchIds"] == []
    assert resolved_conflict["resolvedReason"] == "ignore_rule_added"
    assert resolved_conflict["resolvedContext"]["pattern"] == "generated/**"
    assert "repository ignore rule 追加で解消" in resolved_conflict["resolvedContext"]["summary"]


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
    assert updated_conflict["resolvedReason"] == "branch_file_ignored"
    assert updated_conflict["resolvedContext"]["branchName"] == "feature/ignore-a"
    assert updated_conflict["resolvedContext"]["normalizedFilePath"] == "app/conflicts/service.py"


def test_conflict_watch_webhook_resolution_context_includes_branch_and_delivery(client):
    repository_response = client.post(
        "/tools/conflict-watch/api/repositories",
        json={
            "providerType": "github",
            "repositoryName": "hotdock",
            "externalRepoId": "resolved/webhook-context",
        },
    )
    repository_id = repository_response.json()["state"]["repositories"][0]["id"]

    for delivery_id, branch_name in (
        ("resolved-webhook-1", "feature/webhook-a"),
        ("resolved-webhook-2", "feature/webhook-b"),
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

    delete_response = client.post(
        "/tools/conflict-watch/api/simulate-webhook",
        json={
            "repositoryId": repository_id,
            "provider": "github",
            "deliveryId": "resolved-webhook-delete",
            "branchName": "feature/webhook-b",
            "pusher": "tester",
            "signatureStatus": "valid",
            "deletedState": "true",
            "simulateFailure": False,
            "isForced": False,
            "added": "",
            "modified": "",
            "removed": "",
            "renamed": "",
        },
    )

    assert delete_response.status_code == 200
    resolved_conflict = next(conflict for conflict in delete_response.json()["state"]["conflicts"] if conflict["status"] == "resolved")
    assert resolved_conflict["resolvedReason"] == "webhook_branch_deleted"
    assert resolved_conflict["resolvedContext"]["branchName"] == "feature/webhook-b"
    assert resolved_conflict["resolvedContext"]["deliveryId"] == "resolved-webhook-delete"


def test_conflict_watch_remove_branch_file_ignore_restores_conflict_and_updates_memo(client):
    repository_response = client.post(
        "/tools/conflict-watch/api/repositories",
        json={
            "providerType": "github",
            "repositoryName": "hotdock",
            "externalRepoId": "branch-file-ignore/toggle-test",
        },
    )
    repository_id = repository_response.json()["state"]["repositories"][0]["id"]

    for delivery_id, branch_name in (
        ("branch-file-toggle-1", "feature/toggle-a"),
        ("branch-file-toggle-2", "feature/toggle-b"),
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
    target_branch = next(branch for branch in state_before_ignore["branches"] if branch["branchName"] == "feature/toggle-a")

    ignore_response = client.post(
        "/tools/conflict-watch/api/branch-file-ignores",
        json={
            "branchId": target_branch["id"],
            "normalizedFilePath": "app/conflicts/service.py",
            "memo": "一時的に対象外",
        },
    )

    assert ignore_response.status_code == 200
    active_ignore = next(
        item
        for item in ignore_response.json()["state"]["branchFileIgnores"]
        if item["branchId"] == target_branch["id"]
        and item["normalizedFilePath"] == "app/conflicts/service.py"
        and item["isActive"] is True
    )

    memo_response = client.patch(
        "/tools/conflict-watch/api/branch-file-ignores/memo",
        json={
            "branchId": target_branch["id"],
            "normalizedFilePath": "app/conflicts/service.py",
            "memo": "解除前にメモ更新",
        },
    )

    assert memo_response.status_code == 200
    memo_updated_ignore = next(item for item in memo_response.json()["state"]["branchFileIgnores"] if item["id"] == active_ignore["id"])
    assert memo_updated_ignore["memo"] == "解除前にメモ更新"

    restore_response = client.post(
        "/tools/conflict-watch/api/branch-file-ignores/remove",
        json={
            "branchId": target_branch["id"],
            "normalizedFilePath": "app/conflicts/service.py",
        },
    )

    assert restore_response.status_code == 200
    restored_state = restore_response.json()["state"]
    restored_ignore = next(item for item in restored_state["branchFileIgnores"] if item["id"] == active_ignore["id"])
    assert restored_ignore["isActive"] is False
    restored_conflict = next(
        conflict
        for conflict in restored_state["conflicts"]
        if conflict["normalizedFilePath"] == "app/conflicts/service.py"
    )
    assert restored_conflict["status"] == "warning"
    assert sorted(restored_conflict["activeBranchIds"]) == sorted(
        branch["id"]
        for branch in restored_state["branches"]
        if branch["branchName"] in {"feature/toggle-a", "feature/toggle-b"}
    )


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


def test_conflict_watch_github_webhook_raw_payload_preserves_original_json(client):
    payload = {
        "ref": "refs/heads/feature/raw-payload-github",
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
    signature = "sha256=" + hmac.new("ghs_demo_hotdock".encode("utf-8"), payload_bytes, hashlib.sha256).hexdigest()

    response = client.post(
        "/tools/conflict-watch/webhooks/github",
        content=payload_bytes,
        headers={
            "X-GitHub-Delivery": "github-delivery-raw-payload",
            "X-GitHub-Event": "push",
            "X-Hub-Signature-256": signature,
            "Content-Type": "application/json",
        },
    )

    assert response.status_code == 202

    state = client.get("/tools/conflict-watch/api/state").json()
    event = next(
        item
        for item in state["webhookEvents"]
        if item["deliveryId"] == "github-delivery-raw-payload"
    )

    payload_response = client.get(f"/tools/conflict-watch/api/webhook-events/{event['id']}/raw-payload")

    assert payload_response.status_code == 200
    payload_log = payload_response.json()
    assert payload_log["isAvailable"] is True
    assert json.loads(payload_log["content"]) == payload


def test_conflict_watch_github_webhook_processing_trace_is_viewable(client):
    payload = {
        "ref": "refs/heads/feature/github-processing-trace",
        "before": "0000000000000000000000000000000000000000",
        "after": "trace-commit-1",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "github/processing-trace",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "trace-commit-1",
                "added": ["app/conflicts/service.py"],
                "modified": [],
                "removed": [],
            }
        ],
    }

    response = post_github_webhook(client, "github-delivery-processing-trace", payload)

    assert response.status_code == 202

    state = client.get("/tools/conflict-watch/api/state").json()
    event = next(
        item
        for item in state["webhookEvents"]
        if item["deliveryId"] == "github-delivery-processing-trace"
    )

    trace_response = client.get(f"/tools/conflict-watch/api/webhook-events/{event['id']}/processing-trace")

    assert trace_response.status_code == 200
    trace_payload = trace_response.json()
    assert trace_payload["isAvailable"] is True
    trace_json = json.loads(trace_payload["content"])
    assert trace_json["totalElapsedMs"] >= 0
    assert trace_json["totalElapsedSeconds"] >= 0
    assert all("elapsedMs" in entry and entry["elapsedMs"] >= 0 for entry in trace_json["entries"])
    assert all("elapsedSeconds" in entry and entry["elapsedSeconds"] >= 0 for entry in trace_json["entries"])
    labels = [entry["label"] for entry in trace_json["entries"]]
    assert "payload_parsed" in labels
    assert "observed_commits_extracted" in labels
    assert "normal_push_completed" in labels
    assert "reconcile_repository_completed" in labels


def test_conflict_watch_github_webhook_persists_branch_commit_history(client):
    payload = {
        "ref": "refs/heads/feature/history",
        "before": "0000000000000000000000000000000000000000",
        "after": "commit-c",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "history/test",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "commit-a",
                "added": ["lab_normal/a.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "commit-b",
                "added": ["lab_normal/b.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "commit-c",
                "added": [],
                "modified": ["lab_normal/a.txt"],
                "removed": [],
            },
        ],
    }

    response = post_github_webhook(client, "github-commit-history-1", payload)

    assert response.status_code == 202
    state = client.get("/tools/conflict-watch/api/state").json()
    branch = next(branch for branch in state["branches"] if branch["branchName"] == "feature/history")
    branch_files = [item["normalizedFilePath"] for item in state["branchFiles"] if item["branchId"] == branch["id"]]
    branch_commits = [item for item in state["branchCommits"] if item["branchId"] == branch["id"]]
    branch_commit_files = [item for item in state["branchCommitFiles"] if item["branchId"] == branch["id"]]

    assert sorted(branch_files) == ["lab_normal/a.txt", "lab_normal/b.txt"]
    assert [item["commitSha"] for item in sorted(branch_commits, key=lambda item: item["sequenceNo"])] == [
        "commit-a",
        "commit-b",
        "commit-c",
    ]
    assert all(item["isActive"] is True for item in branch_commits)
    assert len(branch_commit_files) == 3


def test_conflict_watch_github_force_push_records_observed_head_files(client):
    first_payload = {
        "ref": "refs/heads/feature/reset-known-after",
        "before": "0000000000000000000000000000000000000000",
        "after": "reset-c",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/known-after",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "reset-a",
                "added": ["lab_reset/a.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "reset-b",
                "added": ["lab_reset/b.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "reset-c",
                "added": ["lab_reset/c.txt"],
                "modified": [],
                "removed": [],
            },
        ],
    }
    second_payload = {
        "ref": "refs/heads/feature/reset-known-after",
        "before": "reset-c",
        "after": "reset-b",
        "deleted": False,
        "forced": True,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/known-after",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [],
        "head_commit": {
            "id": "reset-b",
            "added": [],
            "modified": [],
            "removed": [],
        },
    }

    assert post_github_webhook(client, "github-force-known-1", first_payload).status_code == 202
    assert post_github_webhook(client, "github-force-known-2", second_payload).status_code == 202

    state = client.get("/tools/conflict-watch/api/state").json()
    branch = next(branch for branch in state["branches"] if branch["branchName"] == "feature/reset-known-after")
    branch_files = sorted(
        item["normalizedFilePath"] for item in state["branchFiles"] if item["branchId"] == branch["id"]
    )
    branch_commits = sorted(
        [item for item in state["branchCommits"] if item["branchId"] == branch["id"]],
        key=lambda item: item["sequenceNo"],
    )

    assert branch["possiblyInconsistent"] is False
    assert branch_files == ["lab_reset/a.txt", "lab_reset/b.txt", "lab_reset/c.txt"]
    assert [item["isActive"] for item in branch_commits] == [True, True, True]


def test_conflict_watch_github_force_push_does_not_drop_observed_files_on_reset(client):
    first_payload = {
        "ref": "refs/heads/feature/reset-known-after-first-commit",
        "before": "0000000000000000000000000000000000000000",
        "after": "reset-c",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/known-after-first",
            "pushed_at": 1735690000,
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "reset-a",
                "added": ["lab_reset/a.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "reset-b",
                "added": ["lab_reset/b.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "reset-c",
                "added": ["lab_reset/c.txt"],
                "modified": [],
                "removed": [],
            },
        ],
    }
    second_payload = {
        "ref": "refs/heads/feature/reset-known-after-first-commit",
        "before": "reset-c",
        "after": "reset-a",
        "deleted": False,
        "forced": True,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/known-after-first",
            "pushed_at": 1735690002,
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [],
        "head_commit": {
            "id": "reset-a",
            "added": ["lab_reset/a.txt"],
            "modified": [],
            "removed": [],
        },
    }

    assert post_github_webhook(client, "github-force-known-first-1", first_payload).status_code == 202
    assert post_github_webhook(client, "github-force-known-first-2", second_payload).status_code == 202

    state = client.get("/tools/conflict-watch/api/state").json()
    branch = next(
        branch for branch in state["branches"] if branch["branchName"] == "feature/reset-known-after-first-commit"
    )
    branch_files = sorted(
        item["normalizedFilePath"] for item in state["branchFiles"] if item["branchId"] == branch["id"]
    )
    branch_commits = sorted(
        [item for item in state["branchCommits"] if item["branchId"] == branch["id"]],
        key=lambda item: item["sequenceNo"],
    )
    branch_commit_files = sorted(
        [item for item in state["branchCommitFiles"] if item["branchId"] == branch["id"]],
        key=lambda item: (item["commitSha"], item["normalizedFilePath"]),
    )

    assert branch["possiblyInconsistent"] is False
    assert branch_files == ["lab_reset/a.txt", "lab_reset/b.txt", "lab_reset/c.txt"]
    assert [item["isActive"] for item in branch_commits] == [True, True, True]
    assert [(item["normalizedFilePath"], item["isActive"]) for item in branch_commit_files] == [
        ("lab_reset/a.txt", True),
        ("lab_reset/b.txt", True),
        ("lab_reset/c.txt", True),
    ]


def test_conflict_watch_github_out_of_order_webhooks_are_still_recorded(client):
    force_payload = {
        "ref": "refs/heads/lab/retest2-reset-force",
        "before": "reset-c",
        "after": "reset-a",
        "deleted": False,
        "forced": True,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/out-of-order-reset",
            "pushed_at": 1735690002,
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [],
        "head_commit": {
            "id": "reset-a",
            "timestamp": "2025-01-01T00:00:02+00:00",
            "added": ["retest2_reset/a.txt"],
            "modified": [],
            "removed": [],
        },
    }
    initial_payload = {
        "ref": "refs/heads/lab/retest2-reset-force",
        "before": "0000000000000000000000000000000000000000",
        "after": "reset-c",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/out-of-order-reset",
            "pushed_at": 1735690000,
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "reset-a",
                "added": ["retest2_reset/a.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "reset-b",
                "added": ["retest2_reset/b.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "reset-c",
                "added": ["retest2_reset/c.txt"],
                "modified": [],
                "removed": [],
            },
        ],
        "head_commit": {
            "id": "reset-c",
            "timestamp": "2025-01-01T00:00:00+00:00",
            "added": ["retest2_reset/c.txt"],
            "modified": [],
            "removed": [],
        },
    }

    assert post_github_webhook(client, "github-force-order-reset-2", force_payload).status_code == 202
    assert post_github_webhook(client, "github-force-order-reset-1", initial_payload).status_code == 202

    state = client.get("/tools/conflict-watch/api/state").json()
    branch = next(branch for branch in state["branches"] if branch["branchName"] == "lab/retest2-reset-force")
    branch_files = sorted(
        item["normalizedFilePath"] for item in state["branchFiles"] if item["branchId"] == branch["id"]
    )
    branch_commits = {
        item["commitSha"]: item
        for item in state["branchCommits"]
        if item["branchId"] == branch["id"]
    }

    assert branch_files == ["retest2_reset/a.txt", "retest2_reset/b.txt", "retest2_reset/c.txt"]
    assert branch["possiblyInconsistent"] is False
    assert branch_commits["reset-a"]["isActive"] is True
    assert branch_commits["reset-b"]["isActive"] is True
    assert branch_commits["reset-c"]["isActive"] is True


def test_conflict_watch_github_unknown_after_force_push_records_observed_files(client):
    first_payload = {
        "ref": "refs/heads/feature/unknown-after",
        "before": "0000000000000000000000000000000000000000",
        "after": "unknown-b",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/unknown-after",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "unknown-a",
                "added": ["lab_rebase/a.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "unknown-b",
                "added": ["lab_rebase/b.txt"],
                "modified": [],
                "removed": [],
            },
        ],
    }
    second_payload = {
        "ref": "refs/heads/feature/unknown-after",
        "before": "unknown-b",
        "after": "unknown-rewritten",
        "deleted": False,
        "forced": True,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/unknown-after",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "unknown-rewritten",
                "added": ["lab_rebase/c.txt"],
                "modified": [],
                "removed": [],
            },
        ],
    }

    assert post_github_webhook(client, "github-force-unknown-1", first_payload).status_code == 202
    assert post_github_webhook(client, "github-force-unknown-2", second_payload).status_code == 202

    state = client.get("/tools/conflict-watch/api/state").json()
    branch = next(branch for branch in state["branches"] if branch["branchName"] == "feature/unknown-after")
    branch_files = sorted(
        item["normalizedFilePath"] for item in state["branchFiles"] if item["branchId"] == branch["id"]
    )
    branch_commits = {
        item["commitSha"]: item
        for item in state["branchCommits"]
        if item["branchId"] == branch["id"]
    }

    assert branch["possiblyInconsistent"] is False
    assert branch_files == ["lab_rebase/a.txt", "lab_rebase/b.txt", "lab_rebase/c.txt"]
    assert branch_commits["unknown-a"]["isActive"] is True
    assert branch_commits["unknown-b"]["isActive"] is True
    assert branch_commits["unknown-rewritten"]["isActive"] is True


def test_conflict_watch_force_push_updates_branch_cache_without_commit_replay(client):
    first_payload = {
        "ref": "refs/heads/feature/cache-update-force-push",
        "before": "0000000000000000000000000000000000000000",
        "after": "cache-2",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/cache-update",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "cache-1",
                "added": ["cache_force/a.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "cache-2",
                "added": ["cache_force/b.txt"],
                "modified": [],
                "removed": [],
            },
        ],
    }
    second_payload = {
        "ref": "refs/heads/feature/cache-update-force-push",
        "before": "cache-2",
        "after": "cache-3",
        "deleted": False,
        "forced": True,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/cache-update",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "cache-3",
                "added": ["cache_force/c.txt"],
                "modified": [],
                "removed": [],
            },
        ],
    }

    assert post_github_webhook(client, "github-cache-update-1", first_payload).status_code == 202
    assert post_github_webhook(client, "github-cache-update-2", second_payload).status_code == 202

    state = client.get("/tools/conflict-watch/api/state").json()
    branch = next(
        branch for branch in state["branches"] if branch["branchName"] == "feature/cache-update-force-push"
    )
    branch_files = sorted(
        item["normalizedFilePath"] for item in state["branchFiles"] if item["branchId"] == branch["id"]
    )
    event = next(item for item in state["webhookEvents"] if item["deliveryId"] == "github-cache-update-2")
    trace_response = client.get(f"/tools/conflict-watch/api/webhook-events/{event['id']}/processing-trace")

    assert trace_response.status_code == 200
    trace_json = json.loads(trace_response.json()["content"])
    cache_entry = next(entry for entry in trace_json["entries"] if entry["label"] == "branch_cache_updated")
    force_entry = next(entry for entry in trace_json["entries"] if entry["label"] == "force_push_observed_only")

    assert branch_files == ["cache_force/a.txt", "cache_force/b.txt", "cache_force/c.txt"]
    assert cache_entry["detail"]["commitReplayCount"] == 0
    assert cache_entry["detail"]["touchedFileCount"] == 1
    assert force_entry["detail"]["touchedFileCount"] == 1


def test_conflict_watch_github_amend_force_push_keeps_old_and_new_observed_commits(client):
    first_payload = {
        "ref": "refs/heads/feature/amend-force-push",
        "before": "0000000000000000000000000000000000000000",
        "after": "amend-old",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/amend",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "amend-old",
                "message": "feat: add amend target",
                "added": ["lab_amend/a.txt"],
                "modified": [],
                "removed": [],
            },
        ],
    }
    second_payload = {
        "ref": "refs/heads/feature/amend-force-push",
        "before": "amend-old",
        "after": "amend-new",
        "deleted": False,
        "forced": True,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/amend",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "amend-new",
                "message": "feat: add amend target (amended)",
                "added": ["lab_amend/a.txt"],
                "modified": [],
                "removed": [],
            },
        ],
    }

    assert post_github_webhook(client, "github-amend-1", first_payload).status_code == 202
    assert post_github_webhook(client, "github-amend-2", second_payload).status_code == 202

    state = client.get("/tools/conflict-watch/api/state").json()
    branch = next(branch for branch in state["branches"] if branch["branchName"] == "feature/amend-force-push")
    branch_commits = {
        item["commitSha"]: item
        for item in state["branchCommits"]
        if item["branchId"] == branch["id"]
    }
    branch_files = sorted(
        item["normalizedFilePath"] for item in state["branchFiles"] if item["branchId"] == branch["id"]
    )

    assert branch["possiblyInconsistent"] is False
    assert branch_commits["amend-old"]["isActive"] is True
    assert branch_commits["amend-new"]["isActive"] is True
    assert branch_files == ["lab_amend/a.txt"]


def test_conflict_watch_github_force_push_keeps_shared_path_as_observed_touch(client):
    first_payload = {
        "ref": "refs/heads/feature/rebase-drop",
        "before": "0000000000000000000000000000000000000000",
        "after": "rebase-old-3",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/rebase-drop",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "rebase-old-1",
                "message": "feat: add base path",
                "added": ["lab_rebase/base.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "rebase-old-2",
                "message": "feat: update shared path 1",
                "added": [],
                "modified": ["lab_rebase/shared.txt"],
                "removed": [],
            },
            {
                "id": "rebase-old-3",
                "message": "feat: update shared path 2",
                "added": [],
                "modified": ["lab_rebase/shared.txt"],
                "removed": [],
            },
        ],
    }
    second_payload = {
        "ref": "refs/heads/feature/rebase-drop",
        "before": "rebase-old-3",
        "after": "rebase-new-1",
        "deleted": False,
        "forced": True,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/rebase-drop",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "rebase-new-1",
                "message": "feat: rewritten shared path",
                "added": [],
                "modified": ["lab_rebase/shared.txt"],
                "removed": [],
            },
        ],
    }

    assert post_github_webhook(client, "github-rebase-drop-1", first_payload).status_code == 202
    assert post_github_webhook(client, "github-rebase-drop-2", second_payload).status_code == 202

    state = client.get("/tools/conflict-watch/api/state").json()
    branch = next(branch for branch in state["branches"] if branch["branchName"] == "feature/rebase-drop")
    branch_commits = {
        item["commitSha"]: item
        for item in state["branchCommits"]
        if item["branchId"] == branch["id"]
    }
    branch_files = sorted(
        item["normalizedFilePath"] for item in state["branchFiles"] if item["branchId"] == branch["id"]
    )

    assert branch_commits["rebase-old-1"]["isActive"] is True
    assert branch_commits["rebase-old-2"]["isActive"] is True
    assert branch_commits["rebase-old-3"]["isActive"] is True
    assert branch_commits["rebase-new-1"]["isActive"] is True
    assert branch_files == ["lab_rebase/base.txt", "lab_rebase/shared.txt"]


def test_conflict_watch_github_force_push_keeps_disjoint_middle_touch_history(client):
    first_payload = {
        "ref": "refs/heads/lab/retest3-rebase-force",
        "before": "0000000000000000000000000000000000000000",
        "after": "rebase-c3",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/rebase-drop-disjoint",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "rebase-c1",
                "message": "retest3: rebase c1",
                "added": ["retest3_rebase/a.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "rebase-c2",
                "message": "retest3: rebase c2",
                "added": ["retest3_rebase/b.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "rebase-c3",
                "message": "retest3: rebase c3",
                "added": ["retest3_rebase/c.txt"],
                "modified": [],
                "removed": [],
            },
        ],
    }
    second_payload = {
        "ref": "refs/heads/lab/retest3-rebase-force",
        "before": "rebase-c3",
        "after": "rebase-c3-rewritten",
        "deleted": False,
        "forced": True,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/rebase-drop-disjoint",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "rebase-c3-rewritten",
                "message": "retest3: rebase c3",
                "added": ["retest3_rebase/c.txt"],
                "modified": [],
                "removed": [],
            },
        ],
    }

    assert post_github_webhook(client, "github-rebase-drop-disjoint-1", first_payload).status_code == 202
    assert post_github_webhook(client, "github-rebase-drop-disjoint-2", second_payload).status_code == 202

    state = client.get("/tools/conflict-watch/api/state").json()
    branch = next(branch for branch in state["branches"] if branch["branchName"] == "lab/retest3-rebase-force")
    branch_files = sorted(
        item["normalizedFilePath"] for item in state["branchFiles"] if item["branchId"] == branch["id"]
    )
    branch_commits = {
        item["commitSha"]: item
        for item in state["branchCommits"]
        if item["branchId"] == branch["id"]
    }

    assert branch["possiblyInconsistent"] is False
    assert branch_files == ["retest3_rebase/a.txt", "retest3_rebase/b.txt", "retest3_rebase/c.txt"]
    assert branch_commits["rebase-c1"]["isActive"] is True
    assert branch_commits["rebase-c2"]["isActive"] is True
    assert branch_commits["rebase-c3"]["isActive"] is True
    assert branch_commits["rebase-c3-rewritten"]["isActive"] is True


def test_conflict_watch_github_force_push_keeps_multi_commit_touch_history(client):
    first_payload = {
        "ref": "refs/heads/feature/rebase-drop-multi",
        "before": "0000000000000000000000000000000000000000",
        "after": "rebase-old-4",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/rebase-drop-multi",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "rebase-old-1",
                "message": "multi: c1",
                "added": ["lab_rebase_multi/a.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "rebase-old-2",
                "message": "multi: c2",
                "added": ["lab_rebase_multi/b.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "rebase-old-3",
                "message": "multi: c3",
                "added": ["lab_rebase_multi/c.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "rebase-old-4",
                "message": "multi: c4",
                "added": ["lab_rebase_multi/d.txt"],
                "modified": [],
                "removed": [],
            },
        ],
    }
    second_payload = {
        "ref": "refs/heads/feature/rebase-drop-multi",
        "before": "rebase-old-4",
        "after": "rebase-new-4",
        "deleted": False,
        "forced": True,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/rebase-drop-multi",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "rebase-new-3",
                "message": "multi: c3",
                "added": ["lab_rebase_multi/c.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "rebase-new-4",
                "message": "multi: c4",
                "added": ["lab_rebase_multi/d.txt"],
                "modified": [],
                "removed": [],
            },
        ],
    }

    assert post_github_webhook(client, "github-rebase-drop-multi-1", first_payload).status_code == 202
    assert post_github_webhook(client, "github-rebase-drop-multi-2", second_payload).status_code == 202

    state = client.get("/tools/conflict-watch/api/state").json()
    branch = next(branch for branch in state["branches"] if branch["branchName"] == "feature/rebase-drop-multi")
    branch_files = sorted(
        item["normalizedFilePath"] for item in state["branchFiles"] if item["branchId"] == branch["id"]
    )
    branch_commits = {
        item["commitSha"]: item
        for item in state["branchCommits"]
        if item["branchId"] == branch["id"]
    }

    assert branch["possiblyInconsistent"] is False
    assert branch_files == [
        "lab_rebase_multi/a.txt",
        "lab_rebase_multi/b.txt",
        "lab_rebase_multi/c.txt",
        "lab_rebase_multi/d.txt",
    ]
    assert branch_commits["rebase-old-1"]["isActive"] is True
    assert branch_commits["rebase-old-2"]["isActive"] is True
    assert branch_commits["rebase-old-3"]["isActive"] is True
    assert branch_commits["rebase-old-4"]["isActive"] is True
    assert branch_commits["rebase-new-3"]["isActive"] is True
    assert branch_commits["rebase-new-4"]["isActive"] is True


def test_conflict_watch_github_squash_force_push_adds_new_observed_commit(client):
    first_payload = {
        "ref": "refs/heads/feature/squash-force-push",
        "before": "0000000000000000000000000000000000000000",
        "after": "squash-old-3",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/squash",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "squash-old-1",
                "message": "feat: add a",
                "added": ["lab_squash/a.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "squash-old-2",
                "message": "feat: add b",
                "added": ["lab_squash/b.txt"],
                "modified": [],
                "removed": [],
            },
            {
                "id": "squash-old-3",
                "message": "feat: add c",
                "added": ["lab_squash/c.txt"],
                "modified": [],
                "removed": [],
            },
        ],
    }
    second_payload = {
        "ref": "refs/heads/feature/squash-force-push",
        "before": "squash-old-3",
        "after": "squash-new-1",
        "deleted": False,
        "forced": True,
        "repository": {
            "name": "hotdock",
            "full_name": "force-push/squash",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "squash-new-1",
                "message": "feat: squash a b c",
                "added": ["lab_squash/a.txt", "lab_squash/b.txt", "lab_squash/c.txt"],
                "modified": [],
                "removed": [],
            },
        ],
    }

    assert post_github_webhook(client, "github-squash-1", first_payload).status_code == 202
    assert post_github_webhook(client, "github-squash-2", second_payload).status_code == 202

    state = client.get("/tools/conflict-watch/api/state").json()
    branch = next(branch for branch in state["branches"] if branch["branchName"] == "feature/squash-force-push")
    branch_commits = {
        item["commitSha"]: item
        for item in state["branchCommits"]
        if item["branchId"] == branch["id"]
    }
    branch_files = sorted(
        item["normalizedFilePath"] for item in state["branchFiles"] if item["branchId"] == branch["id"]
    )

    assert branch_commits["squash-old-1"]["isActive"] is True
    assert branch_commits["squash-old-2"]["isActive"] is True
    assert branch_commits["squash-old-3"]["isActive"] is True
    assert branch_commits["squash-new-1"]["isActive"] is True
    assert branch_files == ["lab_squash/a.txt", "lab_squash/b.txt", "lab_squash/c.txt"]


def test_conflict_watch_github_rename_infers_previous_path_and_updates_touched_files(client):
    payload = {
        "ref": "refs/heads/feature/rename-inference",
        "before": "0000000000000000000000000000000000000000",
        "after": "rename-1",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "rename/inference",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "rename-1",
                "message": "rename old to new",
                "added": ["lab_rename/new_name.txt"],
                "modified": [],
                "removed": ["lab_rename/old_name.txt"],
            },
        ],
    }

    assert post_github_webhook(client, "github-rename-1", payload).status_code == 202

    state = client.get("/tools/conflict-watch/api/state").json()
    branch = next(branch for branch in state["branches"] if branch["branchName"] == "feature/rename-inference")
    event = next(item for item in state["webhookEvents"] if item["deliveryId"] == "github-rename-1")
    branch_files = [item for item in state["branchFiles"] if item["branchId"] == branch["id"]]
    branch_commit_file = next(
        item
        for item in state["branchCommitFiles"]
        if item["branchId"] == branch["id"] and item["commitSha"] == "rename-1"
    )

    branch_files_by_path = {item["normalizedFilePath"]: item for item in branch_files}

    assert event["filesRenamed"] == [{"oldPath": "lab_rename/old_name.txt", "newPath": "lab_rename/new_name.txt"}]
    assert sorted(branch_files_by_path) == ["lab_rename/new_name.txt", "lab_rename/old_name.txt"]
    assert branch_files_by_path["lab_rename/new_name.txt"]["previousPath"] == "lab_rename/old_name.txt"
    assert branch_files_by_path["lab_rename/new_name.txt"]["changeType"] == "renamed"
    assert branch_files_by_path["lab_rename/old_name.txt"]["previousPath"] == "lab_rename/new_name.txt"
    assert branch_files_by_path["lab_rename/old_name.txt"]["changeType"] == "removed"
    assert branch_commit_file["changeType"] == "renamed"
    assert branch_commit_file["previousPath"] == "lab_rename/old_name.txt"


def test_conflict_watch_github_revert_push_keeps_observed_touched_path(client):
    first_payload = {
        "ref": "refs/heads/feature/revert-heuristic",
        "before": "0000000000000000000000000000000000000000",
        "after": "abcdef1",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "revert/heuristic",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "abcdef1",
                "message": "feat: modify revert target",
                "added": [],
                "modified": ["lab_revert/a.txt"],
                "removed": [],
            },
        ],
    }
    second_payload = {
        "ref": "refs/heads/feature/revert-heuristic",
        "before": "abcdef1",
        "after": "abcdef2",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "revert/heuristic",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "abcdef2",
                "message": "Revert \"feat: modify revert target\"\n\nThis reverts commit abcdef1.",
                "added": [],
                "modified": ["lab_revert/a.txt"],
                "removed": [],
            },
        ],
    }

    assert post_github_webhook(client, "github-revert-1", first_payload).status_code == 202
    assert post_github_webhook(client, "github-revert-2", second_payload).status_code == 202

    state = client.get("/tools/conflict-watch/api/state").json()
    branch = next(branch for branch in state["branches"] if branch["branchName"] == "feature/revert-heuristic")
    branch_files = sorted(
        item["normalizedFilePath"] for item in state["branchFiles"] if item["branchId"] == branch["id"]
    )

    assert branch_files == ["lab_revert/a.txt"]


def test_conflict_watch_possibly_inconsistent_branch_is_not_treated_as_normal_conflict_input(client):
    first_payload = {
        "ref": "refs/heads/feature/inconsistent-a",
        "before": "0000000000000000000000000000000000000000",
        "after": "inconsistent-a-1",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "conflict/inconsistent",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "inconsistent-a-1",
                "message": "feat: branch a",
                "added": [],
                "modified": ["lab_conflict/shared.txt"],
                "removed": [],
            },
        ],
    }
    second_payload = {
        "ref": "refs/heads/feature/inconsistent-b",
        "before": "0000000000000000000000000000000000000000",
        "after": "inconsistent-b-1",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "conflict/inconsistent",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "inconsistent-b-1",
                "message": "feat: branch b",
                "added": [],
                "modified": ["lab_conflict/shared.txt"],
                "removed": [],
            },
        ],
    }
    third_payload = {
        "ref": "refs/heads/feature/inconsistent-b",
        "before": "unknown-previous-head",
        "after": "unknown-rewrite-head",
        "deleted": False,
        "forced": True,
        "repository": {
            "name": "hotdock",
            "full_name": "conflict/inconsistent",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "unknown-rewrite-head",
                "message": "feat: rewritten branch b",
                "added": [],
                "modified": ["lab_conflict/other.txt"],
                "removed": [],
            },
        ],
    }

    assert post_github_webhook(client, "github-inconsistent-1", first_payload).status_code == 202
    second_response = post_github_webhook(client, "github-inconsistent-2", second_payload)
    assert second_response.status_code == 202
    second_state = client.get("/tools/conflict-watch/api/state").json()
    active_conflict = next(
        conflict for conflict in second_state["conflicts"] if conflict["normalizedFilePath"] == "lab_conflict/shared.txt"
    )
    assert active_conflict["status"] == "warning"

    third_response = post_github_webhook(client, "github-inconsistent-3", third_payload)
    assert third_response.status_code == 202
    updated_state = client.get("/tools/conflict-watch/api/state").json()
    branch_b = next(branch for branch in updated_state["branches"] if branch["branchName"] == "feature/inconsistent-b")
    updated_conflict = next(
        conflict for conflict in updated_state["conflicts"] if conflict["normalizedFilePath"] == "lab_conflict/shared.txt"
    )

    assert branch_b["possiblyInconsistent"] is False
    assert updated_conflict["status"] == "warning"
    assert updated_conflict["confidence"] in {"high", "medium", "low"}


def test_conflict_watch_github_branch_delete_marks_branch_deleted_and_clears_current_files(client):
    from sqlalchemy import select

    from app.core.database import SessionLocal
    from app.tools.conflict_watch_models import ConflictWatchBranch, ConflictWatchBranchFile

    push_payload = {
        "ref": "refs/heads/lab/webhook-delete",
        "before": "0000000000000000000000000000000000000000",
        "after": "delete-a",
        "deleted": False,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "delete/test",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [
            {
                "id": "delete-a",
                "added": ["lab_delete/a.txt"],
                "modified": [],
                "removed": [],
            },
        ],
    }
    delete_payload = {
        "ref": "refs/heads/lab/webhook-delete",
        "before": "delete-a",
        "after": "0000000000000000000000000000000000000000",
        "deleted": True,
        "forced": False,
        "repository": {
            "name": "hotdock",
            "full_name": "delete/test",
        },
        "pusher": {
            "name": "tester",
        },
        "commits": [],
    }

    assert post_github_webhook(client, "github-branch-delete-1", push_payload).status_code == 202
    assert post_github_webhook(client, "github-branch-delete-2", delete_payload).status_code == 202

    state = client.get("/tools/conflict-watch/api/state").json()
    assert all(branch["branchName"] != "lab/webhook-delete" for branch in state["branches"])

    with SessionLocal() as db:
        branch = db.scalar(
            select(ConflictWatchBranch).where(ConflictWatchBranch.branch_name == "lab/webhook-delete")
        )
        assert branch is not None
        assert branch.is_deleted is True
        assert db.scalars(
            select(ConflictWatchBranchFile).where(ConflictWatchBranchFile.branch_id == branch.id)
        ).all() == []


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
