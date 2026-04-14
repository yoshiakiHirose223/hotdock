from datetime import datetime, timedelta

NOW = datetime(2026, 4, 14, 12, 0)

PROJECT_STATUS_STYLES = {
    "監視中": "is-available",
    "設定中": "is-planned",
    "停止中": "is-soon",
}

BRANCH_STATUS_STYLES = {
    "正常": "is-available",
    "競合あり": "is-conflict",
    "古い": "is-stale",
    "未観測": "is-planned",
}

CHANGE_TYPE_STYLES = {
    "modified": "is-modified",
    "added": "is-added",
    "removed": "is-removed",
}

PROJECTS = [
    {
        "id": 1,
        "slug": "web-portal",
        "name": "web-portal",
        "repository_url": "https://github.com/acme/web-portal",
        "repository_full_name": "acme/web-portal",
        "owner_name": "Product Team",
        "provider": "github",
        "status": "監視中",
        "created_at": NOW - timedelta(days=40),
        "updated_at": NOW - timedelta(minutes=5),
    },
    {
        "id": 2,
        "slug": "billing-api",
        "name": "billing-api",
        "repository_url": "https://github.com/acme/billing-api",
        "repository_full_name": "acme/billing-api",
        "owner_name": "Platform Team",
        "provider": "github",
        "status": "監視中",
        "created_at": NOW - timedelta(days=60),
        "updated_at": NOW - timedelta(minutes=18),
    },
    {
        "id": 3,
        "slug": "ops-console",
        "name": "ops-console",
        "repository_url": "https://github.com/acme/ops-console",
        "repository_full_name": "acme/ops-console",
        "owner_name": "Ops Team",
        "provider": "github",
        "status": "設定中",
        "created_at": NOW - timedelta(days=14),
        "updated_at": NOW - timedelta(hours=2),
    },
    {
        "id": 4,
        "slug": "docs-hub",
        "name": "docs-hub",
        "repository_url": "https://github.com/acme/docs-hub",
        "repository_full_name": "acme/docs-hub",
        "owner_name": "Enablement Team",
        "provider": "github",
        "status": "停止中",
        "created_at": NOW - timedelta(days=90),
        "updated_at": NOW - timedelta(days=4),
    },
]

PROJECT_SETTINGS = {
    1: [
        {"label": "Provider", "value": "GitHub"},
        {"label": "Repository full name", "value": "acme/web-portal"},
        {"label": "Default notification channel", "value": "#merge-watch"},
    ],
    2: [
        {"label": "Provider", "value": "GitHub"},
        {"label": "Repository full name", "value": "acme/billing-api"},
        {"label": "Default notification channel", "value": "billing-alerts@hotdock.jp"},
    ],
    3: [
        {"label": "Provider", "value": "GitHub"},
        {"label": "Repository full name", "value": "acme/ops-console"},
        {"label": "Default notification channel", "value": "Chatwork / 開発共有ルーム"},
    ],
    4: [
        {"label": "Provider", "value": "GitHub"},
        {"label": "Repository full name", "value": "acme/docs-hub"},
        {"label": "Default notification channel", "value": "未設定"},
    ],
}

PROJECT_BRANCHES = {
    1: [
        {
            "name": "feature/login-fix",
            "last_push_at": NOW - timedelta(hours=1, minutes=48),
            "files": [
                {"path": "app/auth.py", "change_type": "modified", "is_conflict": False, "observed_at": NOW - timedelta(hours=1, minutes=48)},
                {"path": "templates/login.html", "change_type": "modified", "is_conflict": False, "observed_at": NOW - timedelta(hours=1, minutes=45)},
                {"path": "static/css/login.css", "change_type": "modified", "is_conflict": True, "observed_at": NOW - timedelta(hours=1, minutes=43)},
                {"path": "app/services/session.py", "change_type": "modified", "is_conflict": True, "observed_at": NOW - timedelta(hours=1, minutes=40)},
            ],
        },
        {
            "name": "feature/home-hero-copy",
            "last_push_at": NOW - timedelta(hours=7, minutes=10),
            "files": [
                {"path": "templates/home.html", "change_type": "modified", "is_conflict": False, "observed_at": NOW - timedelta(hours=7)},
                {"path": "app/hotdock/data/content.py", "change_type": "modified", "is_conflict": False, "observed_at": NOW - timedelta(hours=6, minutes=55)},
            ],
        },
        {
            "name": "release/2026-04-14",
            "last_push_at": NOW - timedelta(days=2, hours=5),
            "files": [
                {"path": "app/main.py", "change_type": "modified", "is_conflict": False, "observed_at": NOW - timedelta(days=2, hours=4)},
                {"path": "nginx/conf/https-ready.conf.template", "change_type": "modified", "is_conflict": False, "observed_at": NOW - timedelta(days=2, hours=3)},
                {"path": "README.md", "change_type": "modified", "is_conflict": False, "observed_at": NOW - timedelta(days=2, hours=2)},
            ],
        },
        {
            "name": "chore/archive-assets",
            "last_push_at": None,
            "files": [],
        },
    ],
    2: [
        {
            "name": "feature/invoice-retry",
            "last_push_at": NOW - timedelta(minutes=26),
            "files": [
                {"path": "app/invoices/retry.py", "change_type": "modified", "is_conflict": False, "observed_at": NOW - timedelta(minutes=26)},
                {"path": "app/invoices/tasks.py", "change_type": "modified", "is_conflict": True, "observed_at": NOW - timedelta(minutes=22)},
                {"path": "tests/test_retry.py", "change_type": "added", "is_conflict": False, "observed_at": NOW - timedelta(minutes=20)},
            ],
        },
        {
            "name": "fix/tax-rounding",
            "last_push_at": NOW - timedelta(hours=20),
            "files": [
                {"path": "app/tax/service.py", "change_type": "modified", "is_conflict": False, "observed_at": NOW - timedelta(hours=20)},
                {"path": "tests/test_tax.py", "change_type": "modified", "is_conflict": False, "observed_at": NOW - timedelta(hours=19, minutes=50)},
            ],
        },
        {
            "name": "refactor/payment-events",
            "last_push_at": NOW - timedelta(days=9),
            "files": [
                {"path": "app/payments/events.py", "change_type": "modified", "is_conflict": False, "observed_at": NOW - timedelta(days=9)},
                {"path": "app/payments/schema.py", "change_type": "removed", "is_conflict": False, "observed_at": NOW - timedelta(days=9, minutes=10)},
            ],
        },
    ],
    3: [
        {
            "name": "feature/operator-note",
            "last_push_at": NOW - timedelta(hours=3, minutes=5),
            "files": [
                {"path": "app/ops/notes.py", "change_type": "modified", "is_conflict": False, "observed_at": NOW - timedelta(hours=3, minutes=5)},
                {"path": "templates/ops/notes.html", "change_type": "added", "is_conflict": False, "observed_at": NOW - timedelta(hours=2, minutes=50)},
            ],
        },
        {
            "name": "feature/oncall-handbook-link",
            "last_push_at": NOW - timedelta(days=1, hours=1),
            "files": [
                {"path": "templates/settings.html", "change_type": "modified", "is_conflict": True, "observed_at": NOW - timedelta(days=1, minutes=30)},
                {"path": "app/settings/router.py", "change_type": "modified", "is_conflict": True, "observed_at": NOW - timedelta(days=1, minutes=15)},
            ],
        },
    ],
    4: [],
}
