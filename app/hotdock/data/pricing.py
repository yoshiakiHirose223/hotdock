PRICING_PLANS = [
    {
        "name": "GitHub App Lite",
        "price": "公開時に案内予定",
        "description": "GitHub App 導線向けの入口プラン。提供開始後に詳細を公開します。",
        "audience": "GitHub App から始めたいチーム向け",
        "badge": "導入予定",
        "cta": {"label": "導入予定を確認", "href": "/integrations/github-app"},
    },
    {
        "name": "SaaS Starter",
        "price": "¥12,000 / 月",
        "description": "まずは数プロジェクトで衝突候補の可視化を始めたいチーム向けです。",
        "audience": "小規模チーム",
        "badge": "標準",
        "cta": {"label": "新規登録", "href": "/signup"},
    },
    {
        "name": "SaaS Team",
        "price": "¥39,000 / 月",
        "description": "複数プロジェクトと複数通知先を運用したいチーム向けです。",
        "audience": "複数チーム運用",
        "badge": "おすすめ",
        "cta": {"label": "料金相談", "href": "/contact"},
    },
    {
        "name": "SaaS Business",
        "price": "個別見積",
        "description": "権限管理や運用支援を含めて導入したい組織向けです。",
        "audience": "部門横断・複数組織",
        "badge": "相談ベース",
        "cta": {"label": "導入相談", "href": "/contact"},
    },
]

PRICING_COMPARISON = [
    {"label": "監視リポジトリ数", "values": ["公開時に案内", "5", "25", "要件に応じて調整"]},
    {"label": "プロジェクト数", "values": ["公開時に案内", "3", "10", "無制限相談"]},
    {"label": "通知先数", "values": ["公開時に案内", "5", "20", "無制限相談"]},
    {"label": "ユーザー数", "values": ["公開時に案内", "5", "25", "SSO 含めて相談"]},
    {"label": "履歴保持期間", "values": ["公開時に案内", "30日", "180日", "365日+"]},
    {"label": "メール通知", "values": ["予定", "含む", "含む", "含む"]},
    {"label": "Slack 通知", "values": ["予定", "含む", "含む", "含む"]},
    {"label": "Chatwork 通知", "values": ["予定", "含む", "含む", "含む"]},
    {"label": "権限管理", "values": ["予定", "基本", "チーム別", "詳細設定"]},
    {"label": "サポート", "values": ["公開時に案内", "メール", "優先メール", "個別対応"]},
]

PRICING_NOTES = [
    "表示価格は初期案です。正式公開時には提供範囲に合わせて調整される可能性があります。",
    "GitHub App Lite は未提供のため、現時点では案内ページのみです。",
    "SaaS プランでは git 連携と通知設定を完了した後に共通ダッシュボードを利用開始します。",
]
