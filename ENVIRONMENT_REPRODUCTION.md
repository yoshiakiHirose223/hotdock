# HotDock 環境再現指示書

## 目的

この文書は、`/Users/hiroseyoshiaki/Desktop/project` の現在の実行環境を、そのまま再現するための手順をまとめたものです。

前提:

- アプリケーションコードはこの repository の現行状態を使う
- DB データの復元は不要
- DB は空でよい
- ただし `web` コンテナを起動するとアプリ起動前に `init_db()` が走るため、空 DB のまま維持したい場合は `web` を起動しない

## 現在の環境構成

現在の本番向け構成は `docker compose` の 4 サービス構成です。

1. `nginx`
   - image: `nginx:1.27-alpine`
   - host port: `80`, `443`
   - 役割: reverse proxy / static 配信 / HTTPS 終端

2. `web`
   - build: repository 直下の [Dockerfile](/Users/hiroseyoshiaki/Desktop/project/Dockerfile)
   - 内部 port: `8000`
   - 役割: FastAPI + Gunicorn + UvicornWorker

3. `db`
   - image: `postgres:15`
   - 役割: PostgreSQL

4. `certbot`
   - image: `certbot/certbot:latest`
   - 役割: 証明書の初回発行補助と定期更新

## 使用ファイル

- compose: [docker-compose.yml](/Users/hiroseyoshiaki/Desktop/project/docker-compose.yml)
- app image build: [Dockerfile](/Users/hiroseyoshiaki/Desktop/project/Dockerfile)
- env テンプレート: [.env.example](/Users/hiroseyoshiaki/Desktop/project/.env.example)
- 実行時 env: [.env](/Users/hiroseyoshiaki/Desktop/project/.env)
- nginx HTTP テンプレート: [nginx/conf/http-only.conf.template](/Users/hiroseyoshiaki/Desktop/project/nginx/conf/http-only.conf.template)
- nginx HTTPS テンプレート: [nginx/conf/https-ready.conf.template](/Users/hiroseyoshiaki/Desktop/project/nginx/conf/https-ready.conf.template)
- certbot renew loop: [certbot/renew-loop.sh](/Users/hiroseyoshiaki/Desktop/project/certbot/renew-loop.sh)
- 初回証明書発行: [certbot/issue-initial-certificate.sh](/Users/hiroseyoshiaki/Desktop/project/certbot/issue-initial-certificate.sh)
- app 設定: [app/core/config.py](/Users/hiroseyoshiaki/Desktop/project/app/core/config.py)
- app entrypoint: [app/main.py](/Users/hiroseyoshiaki/Desktop/project/app/main.py)

## 前提ソフトウェア

ホスト側に以下が必要です。

- Docker
- Docker Compose V2 (`docker compose`)
- `80/tcp`, `443/tcp` を listen できる権限

HTTPS まで同じ構成で再現する場合は追加で以下が必要です。

- `DOMAIN` に設定するドメインがホストへ向いていること
- Let's Encrypt 発行用のメールアドレス

## 現在の env 設定

基本は [.env.example](/Users/hiroseyoshiaki/Desktop/project/.env.example) と同じです。現行 `.env` との差分は `CERTBOT_EMAIL` だけです。

現在の設定値は以下です。

```dotenv
APP_NAME=HotDock
APP_ENV=development
DEBUG=true
SECRET_KEY=change-me
SITE_URL=https://hotdock.jp
GUNICORN_WORKERS=2
DOMAIN=hotdock.jp
CERTBOT_EMAIL=yoshiaki.business02231998@gmail.com
NGINX_CERT_WATCH_INTERVAL=15
PROXY_TRUSTED_HOSTS=*
BLOG_ADMIN_USERNAME=yoshiaki0223
BLOG_ADMIN_PASSWORD_HASH=9047061c525874f0e3158f1648cba7ed:439b1780e7ca54bdfc6a3deb487ea4690c955dd9b880e93b70d94381c13c7e62

POSTGRES_USER=appuser
POSTGRES_PASSWORD=password
POSTGRES_DB=appdb
POSTGRES_HOST=db
POSTGRES_PORT=5432

DATABASE_CONNECT_RETRIES=30
DATABASE_CONNECT_RETRY_INTERVAL=1.0
```

補足:

- `DATABASE_URL` は未設定でもよいです。未設定時は `POSTGRES_*` から自動組み立てされます。
- `web` サービスでは `INIT_DB_ON_STARTUP=false` を渡していますが、[Dockerfile](/Users/hiroseyoshiaki/Desktop/project/Dockerfile) の `CMD` で `init_db()` を呼ぶため、`web` 起動時には結局 DB 初期化が走ります。

## volume / mount 構成

### named volumes

- `postgres_data`
- `certbot_www`
- `letsencrypt`

### bind mounts

- `./storage:/app/storage`
- `./app/static:/app/static:ro`
- `./nginx/conf/http-only.conf.template:/etc/nginx/conf-templates/http-only.conf.template:ro`
- `./nginx/conf/https-ready.conf.template:/etc/nginx/conf-templates/https-ready.conf.template:ro`
- `./nginx/entrypoint/40-configure-https.sh:/docker-entrypoint.d/40-configure-https.sh:ro`
- `./nginx/entrypoint/50-watch-certificates.sh:/docker-entrypoint.d/50-watch-certificates.sh:ro`
- `./certbot/renew-loop.sh:/usr/local/bin/renew-loop.sh:ro`

## 再現手順

### 1. ソースを配置

この repository 一式を同じ構成で配置します。

```bash
git clone <repository-url> hotdock
cd hotdock
```

### 2. env を配置

完全に同じ構成にしたい場合は、元環境の [.env](/Users/hiroseyoshiaki/Desktop/project/.env) をそのままコピーしてください。

最小構成なら以下でも可です。

```bash
cp .env.example .env
```

その上で、少なくとも以下を現行値に合わせます。

- `SITE_URL=https://hotdock.jp`
- `DOMAIN=hotdock.jp`
- `CERTBOT_EMAIL=yoshiaki.business02231998@gmail.com`
- `POSTGRES_HOST=db`

### 3. 空 DB から始める

DB の中身は不要なので、PostgreSQL volume は新規で構いません。

既存 volume を使わず空 DB で始めたい場合:

- 新しい Docker 環境で起動する
- もしくは `postgres_data` volume を新しく作る

### 4. コンテナを起動

```bash
docker compose up -d --build
```

このコマンドで以下が起動します。

- `nginx`
- `web`
- `db`
- `certbot`

### 5. 起動確認

```bash
docker compose ps
docker compose logs web --tail=100
docker compose logs nginx --tail=100
```

正常時の期待:

- `web` は `0.0.0.0:8000` で Gunicorn 起動
- `nginx` は `80/443` を listen
- `db` は PostgreSQL 15 起動
- `certbot` は renew loop 待機

### 6. HTTP 動作確認

```bash
curl -I http://127.0.0.1/
```

または、ドメインが向いているなら:

```bash
curl -I http://hotdock.jp/
```

### 7. HTTPS を有効化する場合

証明書未発行の初期状態では、Nginx は HTTP only 構成で起動します。

初回発行:

```bash
./certbot/issue-initial-certificate.sh
```

このスクリプトは以下を実行します。

- `docker-compose up -d web db nginx certbot`
- `certbot certonly --webroot`
- `nginx` 設定再生成
- `nginx -s reload`

証明書が揃うと HTTPS 構成へ切り替わります。

## DB を「空のまま」にしたい場合の注意

この repository の現行構成では、`web` 起動時に [Dockerfile](/Users/hiroseyoshiaki/Desktop/project/Dockerfile) の `CMD` 内で次が実行されます。

```sh
python -c "from app.core.database import init_db; init_db()"
```

そのため:

- `db` だけ起動している間は空 DB にできます
- `web` を起動するとテーブルは自動作成されます

つまり、「構成だけ再現して DB は空でよい」という意味であれば問題ありませんが、「web まで起動した上でテーブルなし」は現行構成のままでは両立しません。

## 現行構成の技術的な固定値

### web

- base image: `python:3.12`
- Python packages: [requirements.txt](/Users/hiroseyoshiaki/Desktop/project/requirements.txt)
  - `fastapi`
  - `uvicorn[standard]`
  - `gunicorn`
  - `sqlalchemy`
  - `psycopg2-binary`
  - `pydantic-settings`
  - `jinja2`
  - `python-multipart`
  - `markdown`
  - `itsdangerous`
  - `pytest`
  - `httpx`
- Gunicorn worker 数: `GUNICORN_WORKERS=2`

### app

- framework: FastAPI
- static mount: `/static`
- router:
  - `/`
  - `/blog`
  - `/tools`
  - `/exam`

### db

- engine: PostgreSQL 15
- database: `appdb`
- user: `appuser`

### nginx

- `/static/` は nginx が直接配信
- app 本体は `web:8000` に reverse proxy
- `X-Forwarded-Proto` を app へ転送
- ACME challenge は `/var/www/certbot`

## 期待される最終状態

再現後、以下になっていれば現行構成と同じです。

- `docker compose` の 4 サービス構成
- `nginx` が 80/443 公開
- `web` が Gunicorn + UvicornWorker で動作
- `db` が PostgreSQL 15
- `certbot` が renew loop を持つ
- `storage` が bind mount される
- `.env` が現行値に一致
- `SITE_URL=https://hotdock.jp`
- `DOMAIN=hotdock.jp`

## 補足

ローカル簡易起動用に [run.py](/Users/hiroseyoshiaki/Desktop/project/run.py) もありますが、これは `uvicorn` の reload 起動であり、現在の compose 本番構成とは別物です。現行環境をそのまま再現したい場合は、必ず `docker compose` 構成を使ってください。
