# HotDock

FastAPI を使った SSR ベースのモノリス構成です。`blog`, `tools`, `exam` を 1 つのアプリで動かしつつ、内部は機能単位で分離しています。

## セットアップ

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python run.py
```

## Docker 起動

`docker-compose.yml` は `nginx` / `web` / `db` の 3 サービス構成です。`nginx` が 80 番で受け、`web` の Gunicorn + UvicornWorker にプロキシします。`/static/` は Nginx が直接配信します。

```bash
cp .env.example .env
docker compose up --build
```

アプリ側は `DATABASE_URL` が未設定でも `POSTGRES_*` から接続先を組み立てます。compose では `db` サービス名をそのままホスト名として使います。`web` の 8000 番は外部公開せず、Nginx 経由でのみアクセスします。

## ディレクトリ方針

- `app/site`: トップページとサイト共通導線
- `app/core`: 設定、DB、共通依存
- `app/blog`: Markdown 記事表示
- `app/tools`: CSV 変換ツール
- `app/exam`: 問題表示と解答処理の土台
- `storage`: 記事やアップロードなどの永続データ置き場
- `nginx/conf`: Nginx 設定

## 主な URL

- `/`: トップページ
- `/blog`: 記事一覧
- `/blog/{slug}`: 記事詳細
- `/tools`: ツール一覧
- `/tools/csv-to-json`: CSV to JSON
- `/tools/csv-column-swap`: CSV カラム入れ替え
- `/exam`: 問題集トップ
- `/exam/questions/{id}`: 問題詳細

## 今の実装範囲

- Blog 一覧と詳細
- CSV to JSON
- CSV Column Swap
- Exam 問題一覧と解答確認
- SQLAlchemy ベースの exam モデル土台
