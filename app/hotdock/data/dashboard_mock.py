APP_OVERVIEW = {
    "welcome": "GitHub App と SaaS のどちらから始めても、最終的な管理はこの共通ダッシュボードに集約される想定です。",
    "checklist": [
        "監視対象プロジェクトの登録",
        "git 連携状態の確認",
        "通知先の初期設定",
        "競合候補の確認フロー整理",
    ],
}

SUMMARY_CARDS = [
    {"label": "監視中プロジェクト", "value": "12", "meta": "うち 3 件が重点監視"},
    {"label": "競合候補", "value": "18", "meta": "要確認 6 / 進行中 9 / 解消待ち 3"},
    {"label": "通知先", "value": "9", "meta": "メール 3 / Slack 4 / Chatwork 2"},
    {"label": "連携状態", "value": "安定", "meta": "git 連携 12/12, GitHub App は導入予定"},
]

RECENT_CONFLICTS = [
    {"project": "web-portal", "status": "要確認", "branches": 3, "files": 8, "updated_at": "5分前"},
    {"project": "billing-api", "status": "進行中", "branches": 2, "files": 4, "updated_at": "18分前"},
    {"project": "infra-tools", "status": "解消待ち", "branches": 2, "files": 2, "updated_at": "42分前"},
    {"project": "ops-console", "status": "要確認", "branches": 4, "files": 11, "updated_at": "1時間前"},
]

PROJECTS = [
    {"name": "web-portal", "status": "監視中", "repo": "github.com/acme/web-portal", "owner": "Product Team"},
    {"name": "billing-api", "status": "監視中", "repo": "github.com/acme/billing-api", "owner": "Platform Team"},
    {"name": "ops-console", "status": "設定中", "repo": "github.com/acme/ops-console", "owner": "Ops Team"},
]

INTEGRATION_STATUS = [
    {"name": "git 連携", "status": "接続済み", "detail": "12 プロジェクトで利用中"},
    {"name": "GitHub App", "status": "導入予定", "detail": "公開前のため案内ページを表示"},
    {"name": "Slack", "status": "接続済み", "detail": "#dev-alerts, #release-watch"},
    {"name": "Chatwork", "status": "接続済み", "detail": "2 ルームで通知中"},
    {"name": "メール", "status": "接続済み", "detail": "3 宛先へ配信"},
]

NOTIFICATION_CHANNELS = [
    {"name": "メール", "target": "devops@hotdock.jp", "rule": "要確認と解消待ちを送信", "status": "有効"},
    {"name": "Slack", "target": "#merge-watch", "rule": "新規検知と悪化を送信", "status": "有効"},
    {"name": "Chatwork", "target": "開発共有ルーム", "rule": "毎朝のサマリーを送信", "status": "有効"},
    {"name": "将来追加用", "target": "Webhook / 他通知先", "rule": "拡張用の空きスロット", "status": "準備枠"},
]

BILLING_OVERVIEW = {
    "plan": "SaaS Team",
    "usage": "監視中 12 / 契約上限 25 リポジトリ",
    "renewal": "次回請求日: 2026-05-01",
    "placeholder": "請求書、支払い方法、契約更新フローを将来接続しやすい骨組みです。",
}

SETTINGS_SECTIONS = [
    {
        "title": "組織設定",
        "fields": [
            {"label": "組織名", "value": "Hotdock Sample Team"},
            {"label": "運用方針メモ", "value": "競合候補は毎日 10:00 に要確認だけ共有"},
        ],
    },
    {
        "title": "ユーザー設定",
        "fields": [
            {"label": "表示名", "value": "Project Owner"},
            {"label": "通知言語", "value": "日本語"},
        ],
    },
]
