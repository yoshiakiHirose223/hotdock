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

`docker-compose.yml` は `nginx` / `web` / `db` / `certbot` の 4 サービス構成です。`nginx` が 80/443 で受け、`web` の Gunicorn + UvicornWorker にプロキシします。`/static/` は Nginx が直接配信し、`certbot` とは ACME challenge 用の `certbot_www` と証明書用の `letsencrypt` volume を共有します。

```bash
cp .env.example .env
docker-compose up --build
```

アプリ側は `DATABASE_URL` が未設定でも `POSTGRES_*` から接続先を組み立てます。compose では `db` サービス名をそのままホスト名として使います。`web` の 8000 番は外部公開せず、Nginx 経由でのみアクセスします。

### HTTPS 用の準備

本番ドメインは `hotdock.jp` 前提です。`.env` の `DOMAIN=hotdock.jp`、`CERTBOT_EMAIL`、必要なら `NGINX_CERT_WATCH_INTERVAL` を設定してから証明書発行を行ってください。

証明書がまだない間も Nginx は 80 番で起動し、`/.well-known/acme-challenge/` をそのまま返します。`/etc/letsencrypt/live/${DOMAIN}` に `fullchain.pem` と `privkey.pem` が揃った状態で Nginx を再起動すると、443 を有効にして HTTPS 配信へ切り替わります。

ACME challenge 配信確認:

```bash
docker-compose exec certbot sh -lc 'mkdir -p /var/www/certbot/.well-known/acme-challenge && printf test > /var/www/certbot/.well-known/acme-challenge/healthcheck'
curl http://hotdock.jp/.well-known/acme-challenge/healthcheck
```

初回の証明書発行:

```bash
./certbot/issue-initial-certificate.sh
```

このスクリプトは `docker-compose up -d`、`certbot certonly`、`nginx` の設定再生成と reload までをまとめて実行します。

以後の更新は `certbot` コンテナが `renew` を定期実行し、`nginx` コンテナ側の証明書監視スクリプトが変更を検知して自動で `nginx -s reload` します。

## ディレクトリ方針

- `app/site`: トップページとサイト共通導線
- `app/core`: 設定、DB、共通依存
- `app/blog`: Markdown 記事表示
- `app/tools`: CSV 変換ツール
- `app/exam`: 問題表示と解答処理の土台
- `storage`: 記事やアップロードなどの永続データ置き場
- `certbot`: 証明書発行と更新用スクリプト
- `nginx/conf`: Nginx 設定
- `nginx/entrypoint`: Nginx の起動時設定切り替え

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
