HOME_PROBLEMS = [
    {
        "title": "レビュー時点で競合に気づく",
        "description": "レビューは通っていても、別ブランチの変更が積み上がっていて最後のマージで衝突することがあります。",
    },
    {
        "title": "どこが危ないか共有しづらい",
        "description": "衝突候補の把握が個人依存だと、開発者以外のメンバーには進行リスクが見えにくくなります。",
    },
    {
        "title": "通知先と運用ルールが散らばる",
        "description": "メールや Slack ごとに手動で確認する運用では、状態変化の共有が継続しません。",
    },
]

HOME_SOLUTIONS = [
    {
        "title": "競合候補を先に見える化",
        "description": "ブランチ差分の変化を追い、衝突しそうな箇所を一覧化します。",
    },
    {
        "title": "状態変化を通知で共有",
        "description": "新規検知、悪化、解消をチーム向けの通知先へまとめて送れます。",
    },
    {
        "title": "最終的な管理画面は共通",
        "description": "GitHub App から始めても、SaaS 登録から始めても、管理体験は同じ /app に集約されます。",
    },
]

START_PATHS = [
    {
        "name": "GitHub App",
        "status": "導入予定",
        "description": "GitHub App を入口にした導入フローを準備中です。現在は案内ページと問い合わせ導線のみ提供します。",
        "points": [
            "始め方のひとつとして設計",
            "将来的にはインストール後に共通ダッシュボードへ接続",
            "現時点では未提供であることを明記",
        ],
        "primary_cta": {"label": "導入予定を確認", "href": "/integrations/github-app"},
        "secondary_cta": {"label": "公開予定を見る", "href": "/install/github"},
    },
    {
        "name": "SaaS",
        "status": "登録して開始",
        "description": "アカウント作成後、git 連携と通知先設定を行い、共通ダッシュボードの利用を開始します。",
        "points": [
            "GitHub App は必須ではない",
            "利用開始には git 操作が可能な接続状態が必要",
            "通知先はメール、Slack、Chatwork を想定",
        ],
        "primary_cta": {"label": "SaaS 版で始める", "href": "/signup"},
        "secondary_cta": {"label": "比較を見る", "href": "/compare"},
    },
]

FEATURES = [
    {
        "title": "競合候補の可視化",
        "what": "変更が重なりそうなブランチやファイルを一覧で把握できます。",
        "benefit": "マージ直前ではなく、作業中の段階で相談や調整を始めやすくなります。",
    },
    {
        "title": "状態変化通知",
        "what": "新しく見つかった候補、悪化した候補、落ち着いた候補を通知できます。",
        "benefit": "確認が必要なタイミングだけを共有し、ノイズの少ない運用を組み立てやすくなります。",
    },
    {
        "title": "ブランチ横断把握",
        "what": "複数のブランチやプロジェクトをまたいだ状況を同じ画面で見られます。",
        "benefit": "担当者ごとの把握に閉じず、チーム全体の衝突リスクを共有できます。",
    },
    {
        "title": "履歴整理",
        "what": "候補の発生から解消までの履歴を時系列で追跡できます。",
        "benefit": "同じ種類の衝突が繰り返される場所や運用上の癖を見直しやすくなります。",
    },
    {
        "title": "通知手段管理",
        "what": "メール、Slack、Chatwork などの通知先を運用単位で切り替えられます。",
        "benefit": "将来の通知先追加や、チームごとの通知ルール分離に対応しやすくなります。",
    },
    {
        "title": "チーム運用",
        "what": "プロジェクト単位でメンバー、通知、確認フローを整理できます。",
        "benefit": "個人依存の確認を減らし、引き継ぎや権限整理をしやすくします。",
    },
    {
        "title": "共通ダッシュボード",
        "what": "GitHub App でも SaaS でも、着地点は同じダッシュボードです。",
        "benefit": "入口の違いを残したまま、運用画面や説明資料を共通化できます。",
    },
    {
        "title": "拡張可能な連携設計",
        "what": "GitHub 以外のホストや通知手段を追加しやすい構成で設計しています。",
        "benefit": "将来の GitLab、Bitbucket、Backlog 対応へ無理なく広げられます。",
    },
]

HOME_FEATURE_HIGHLIGHTS = FEATURES[:6]

IMPLEMENTATION_STEPS = [
    {
        "step": "01",
        "title": "始め方を選ぶ",
        "description": "GitHub App 導入予定を確認するか、SaaS 版へ登録するかを選びます。",
    },
    {
        "step": "02",
        "title": "git 連携を設定する",
        "description": "Hotdock から git 操作ができる接続状態を用意し、監視対象を指定します。",
    },
    {
        "step": "03",
        "title": "通知先とダッシュボードを整える",
        "description": "メール、Slack、Chatwork などを設定し、共通 /app で状況を確認します。",
    },
]
