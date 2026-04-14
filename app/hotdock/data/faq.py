FAQ_CATEGORIES = [
    {
        "title": "導入方法",
        "items": [
            {
                "question": "GitHub App と SaaS の違いは何ですか。",
                "answer": "違いは始め方です。GitHub App は導入予定の入口、SaaS は登録して始める入口です。どちらから入っても、最終的な管理画面は共通の /app を想定しています。",
            },
            {
                "question": "GitHub App はもう使えますか。",
                "answer": "いいえ。現在は未提供です。導入用 URL もまだ公開していません。案内ページでは、将来どう始められるかだけを説明しています。",
            },
            {
                "question": "GitHub App から始めた後にダッシュボードは使えますか。",
                "answer": "はい。将来的には GitHub App から始めた場合でも、最終的には共通の /app ダッシュボードに接続される想定です。",
            },
            {
                "question": "SaaS 利用に GitHub App は必須ですか。",
                "answer": "必須ではありません。SaaS 導線では、アカウント登録後に必要な git 連携と通知設定を行って利用を始めます。",
            },
        ],
    },
    {
        "title": "git 連携",
        "items": [
            {
                "question": "git 連携とは何ですか。",
                "answer": "Hotdock から git diff などの git 操作が可能な状態を指します。単に API のトークン連携だけを意味するものではありません。",
            },
            {
                "question": "Git API 連携と何が違いますか。",
                "answer": "Git API は実現手段のひとつにすぎません。Hotdock で重要なのは、競合候補の把握に必要な git 操作が可能であることです。",
            },
            {
                "question": "private repository でも使えますか。",
                "answer": "使える想定で設計しています。接続方法や権限の扱いは、導入方式ごとに明示できる形で整理していきます。",
            },
        ],
    },
    {
        "title": "通知と拡張",
        "items": [
            {
                "question": "通知はどこに届きますか。",
                "answer": "初期想定ではメール、Slack、Chatwork を対象にしています。将来的には他の通知先も追加できる前提です。",
            },
            {
                "question": "Slack / Chatwork / メールに対応していますか。",
                "answer": "はい。初期構成ではこの3種類を前提に UI とデータ構造を整理しています。",
            },
            {
                "question": "Backlog や GitLab にも対応予定ですか。",
                "answer": "はい。今回は準備中として案内し、将来の拡張先として UI とデータに含めています。",
            },
        ],
    },
]
