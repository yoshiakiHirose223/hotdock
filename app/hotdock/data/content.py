from app.hotdock.data.features import IMPLEMENTATION_STEPS

HOME_PAGE = {
    "hero_eyebrow": "Git の衝突を、早く、わかりやすく。",
    "hero_title": "ブランチの競合を、マージ前に見つける。",
    "hero_description": "Hotdock は Git の競合・バッティング・衝突候補を見える化し、チームが問題へ早めに気づけるようにするサービスです。",
    "hero_actions": [
        {"label": "GitHub App を確認する", "href": "/integrations/github-app", "variant": "primary"},
        {"label": "SaaS 版で始める", "href": "/signup", "variant": "secondary"},
        {"label": "仕組みを見る", "href": "/how-it-works", "variant": "ghost"},
    ],
    "entry_summary": [
        "GitHub App は導入予定です。",
        "SaaS は登録後に git 連携と通知設定を行って開始します。",
        "どちらから入っても、最終的なダッシュボード体験は共通です。",
    ],
    "steps": IMPLEMENTATION_STEPS,
}

HOW_IT_WORKS_CONTENT = {
    "topics": [
        {
            "title": "更新検知",
            "description": "監視対象でブランチ更新を追い、比較対象の差分を再評価します。",
        },
        {
            "title": "ブランチ単位の変更把握",
            "description": "変更箇所をブランチ単位で整理し、どこが重なりそうかを見つけます。",
        },
        {
            "title": "競合候補抽出",
            "description": "衝突に発展しやすい箇所を候補として扱い、優先度をつけて一覧化します。",
        },
        {
            "title": "状態変化通知",
            "description": "候補の新規発生、悪化、解消に応じて通知ルールを適用します。",
        },
        {
            "title": "解消判定",
            "description": "差分の変化を見直し、候補が消えたか、監視継続が必要かを再判定します。",
        },
    ],
    "git_link_definition": "Hotdock における git 連携とは、Hotdock 側から git 操作が可能な接続状態のことです。Git API は実現手段のひとつであり、本質そのものではありません。",
}

SECURITY_CONTENT = [
    {
        "title": "権限最小化",
        "description": "必要な権限だけを説明可能な単位で扱う前提で設計します。GitHub App 提供時も同じ思想を維持します。",
    },
    {
        "title": "認証・認可",
        "description": "ログイン、組織単位の閲覧範囲、通知設定の変更権限を分離しやすい骨組みにしています。",
    },
    {
        "title": "データ取り扱い",
        "description": "監視対象や通知先情報は、運用に必要な情報を整理して扱う前提です。将来のデータポリシー追加にも対応しやすい構成です。",
    },
    {
        "title": "git 連携時の基本方針",
        "description": "git 連携は git 操作が可能な状態を指します。方式よりも、何ができる状態なのかを明確に説明します。",
    },
    {
        "title": "通知先情報の扱い",
        "description": "メールアドレス、Slack、Chatwork などの通知先は、運用目的に沿って分離・停止・追加しやすい形で管理します。",
    },
    {
        "title": "インフラ運用",
        "description": "Nginx、Gunicorn、FastAPI、PostgreSQL を前提に、サーバレンダリング中心の構成で保守しやすさを優先します。",
    },
]

GLOBAL_CTA = {
    "title": "入口が違っても、運用画面はひとつにまとめる。",
    "description": "GitHub App は導入予定、SaaS は今後の登録導線として整理しつつ、最終的な管理体験を共通の /app に集約する前提で設計しています。",
    "primary": {"label": "SaaS 版で始める", "href": "/signup"},
    "secondary": {"label": "導入相談をする", "href": "/contact"},
}
