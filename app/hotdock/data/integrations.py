INTEGRATION_STATUS_STYLES = {
    "利用可能": "is-available",
    "導入予定": "is-planned",
    "準備中": "is-soon",
}

INTEGRATIONS = [
    {
        "name": "git 連携",
        "status": "利用可能",
        "category": "Core",
        "description": "Hotdock 側から git diff などの git 操作が可能な接続状態を前提にします。",
        "notes": "Git API 利用そのものではなく、git 操作ができることが本質です。",
    },
    {
        "name": "GitHub",
        "status": "利用可能",
        "category": "Repository host",
        "description": "SaaS 導線から利用する際の主要なホストとして想定しています。",
        "notes": "private repository を含む運用を想定した説明を用意しています。",
    },
    {
        "name": "GitHub App",
        "status": "導入予定",
        "category": "Start path",
        "description": "GitHub App を入口とする導線を準備中です。将来的には共通 /app へ接続します。",
        "notes": "未提供のため、実在するインストール URL は案内しません。",
    },
    {
        "name": "メール通知",
        "status": "利用可能",
        "category": "Notification",
        "description": "チーム共有しやすい基本の通知手段として扱います。",
        "notes": "通知先の追加や停止をダッシュボードから整理できる構成を想定します。",
    },
    {
        "name": "Slack",
        "status": "利用可能",
        "category": "Notification",
        "description": "状態変化をチャンネル単位で受け取りやすい通知先です。",
        "notes": "通知先ごとにルールを増やしやすい設計にしています。",
    },
    {
        "name": "Chatwork",
        "status": "利用可能",
        "category": "Notification",
        "description": "国内チーム向けの通知先として初期想定に含めます。",
        "notes": "通知先一覧から有効化できる前提で UI を組んでいます。",
    },
    {
        "name": "Backlog",
        "status": "準備中",
        "category": "Future",
        "description": "将来的な開発フロー連携候補です。",
        "notes": "今回は案内のみで、管理画面の拡張余地を残します。",
    },
    {
        "name": "GitLab",
        "status": "準備中",
        "category": "Future",
        "description": "別ホストへの拡張先として整理しています。",
        "notes": "データ構造上は追加しやすい形で管理します。",
    },
    {
        "name": "Bitbucket",
        "status": "準備中",
        "category": "Future",
        "description": "別ホストへの拡張先として整理しています。",
        "notes": "比較表や FAQ と矛盾しない表現で掲載します。",
    },
]

GITHUB_APP_PAGE = {
    "hero_title": "GitHub App は導入予定です",
    "hero_copy": "Hotdock では GitHub App を始め方のひとつとして準備しています。ただし、現時点ではまだ未提供であり、導入用 URL も公開していません。",
    "principles": [
        {
            "title": "入口のひとつとして設計",
            "description": "GitHub App から始めた場合でも、最終的な管理体験は共通 /app へ集約します。",
        },
        {
            "title": "権限は最小化の思想で整理",
            "description": "将来の提供時には、必要な範囲だけを説明可能な形で扱う前提で設計します。",
        },
        {
            "title": "通知と可視化は共通体験",
            "description": "SaaS 利用者と同じダッシュボードで、競合候補と通知先の状況を確認できる構成を目指します。",
        },
    ],
    "expected_flow": [
        "GitHub App の公開後、インストール対象リポジトリを選択する",
        "Hotdock 側で監視対象と通知先を確認する",
        "共通ダッシュボード /app に移動して、競合候補の確認を始める",
    ],
    "permissions": [
        "必要な権限を導入前に明示する",
        "監視対象と通知設定を分けて説明する",
        "共通ダッシュボードで利用状況を見返せるようにする",
    ],
}
