---
title: LAMPしか知らない自分がFastAPIとuvicornを理解するまで
summary: FastAPIとuvicornの仕組みをLAMPと比較
slug: whats_fastapi_uvicorn
published_at: 2026-04-02
tags: FastAPI, PHP, uvicorn, LAMP, Python
is_published: true
---
## はじめに

これまでPHP（LAMP）で開発していると、

- `/blog` にアクセス → `/blog/index.php` が実行される

という感覚が当たり前です。

しかしFastAPIではこの動きが大きく変わります。

最初は違和感がありますが、構造を分解するとシンプルです。

---

# 全体構造の比較

まずは全体の対応関係です。

## LAMP

```text
ブラウザ → Apache → PHP → index.php → 処理 → HTML
```

## FastAPI

```text
ブラウザ → Uvicorn → FastAPI → main.py → router → service → HTML
```

👉 やっていることは同じ  
👉 「分割の仕方」と「役割」が違うだけ

---

# uvicornとは何か

## 結論

👉 **Apacheの代わり（Webサーバ）**

---

## 役割

| 項目 | 内容 |
|------|------|
| 名前 | Uvicorn |
| 役割 | Webサーバ |
| 仕事 | HTTPリクエストを受ける |

---

## 流れ比較

### LAMP

```text
ブラウザ → Apache → PHP
```

### FastAPI

```text
ブラウザ → Uvicorn → FastAPI
```

---

# FastAPIとは何か

## 結論

👉 **PHP（アプリ本体）に相当**

---

## 例

### PHP

```php
if ($_GET['page'] == 'blog') {
    echo "ブログ一覧";
}
```

### FastAPI

```python
@app.get("/blog")
def blog():
    return {"message": "ブログ一覧"}
```

---

## 役割

| 項目 | 内容 |
|------|------|
| FastAPI | Webアプリ |
| 仕事 | URLごとの処理を書く |

---

# 「リクエストを渡す」とは何か

## 流れ

```text
① ブラウザが /blog にアクセス
② Uvicornが受ける
③ FastAPIに渡す
④ FastAPIが処理する
```

---

## イメージ

```text
[ブラウザ]
   ↓
[Uvicorn]（受付）
   ↓
[FastAPI]（振り分け）
   ↓
[アプリコード]
```

👉 「渡す」とは  
👉 **処理をバトンタッチするだけ**

---

# main.pyの役割

## 結論

👉 **ルーターを登録するだけ**

---

## コード

```python
from fastapi import FastAPI
from routers.blog import router

app = FastAPI()

app.include_router(router)
```

---

## 意味

```text
このアプリに「ブログのルール」を追加する
```

---

## 重要ポイント

❌ main.pyがURLを判断する  
👉 **違う**

✅ FastAPIが自動で判断する

---

# Routerとは何か

## 結論

👉 **URLと処理の対応表**

---

## 例

```python
router = APIRouter(prefix="/blog")

@router.get("")
def blog_index():
    return "記事一覧"
```

---

## 意味

```text
/blog にアクセス → blog_index() 実行
```

---

## LAMP的な書き方

```php
if ($_SERVER['REQUEST_URI'] == '/blog') {
    blog_index();
}
```

👉 Routerがこれを自動化している

---

# 実際の処理の流れ

```text
GET /blog
```

↓

```text
① Uvicornが受ける
② FastAPIに渡す
③ FastAPIがルーター一覧を見る
④ prefix="/blog" に一致するルーターを選ぶ
⑤ @router.get("") に一致
⑥ blog_index() 実行
⑦ serviceでデータ取得
⑧ HTML生成
⑨ レスポンス返却
```

---

# 起動ファイルはどこか？

## 結論

👉 uvicornで指定する

```bash
uvicorn app.main:app
```

---

## 分解

```text
app.main → app/main.py
:app     → FastAPIインスタンス
```

---

## LAMPとの違い

| LAMP | FastAPI |
|------|--------|
| index.phpが自動 | 自分で指定 |

---

# ファイルごとの役割

| ファイル | 役割 |
|--------|------|
| main.py | 起動・ルーター登録 |
| router.py | URLと処理 |
| service.py | ロジック |
| HTML | 表示 |

---

# 重要な違い（本質）

## LAMP

```text
URL → index.php → if文で分岐
```

---

## FastAPI

```text
URL → 自動で関数にマッピング
```

👉 if文が不要

---

# ルーターの競合について

## 問題のケース

```python
# blog
prefix="/blog"
@router.get("test") → /blog/test

# blog_test
prefix="/blog/test"
@router.get("") → /blog/test
```

👉 両方同じURL

---

## 結論

👉 **先に登録された方が使われる**

---

## 例

```python
app.include_router(blog_router, prefix="/blog")
app.include_router(blog_test_router, prefix="/blog/test")
```

👉 `/blog/test` → blog_router

---

逆に

```python
app.include_router(blog_test_router, prefix="/blog/test")
app.include_router(blog_router, prefix="/blog")
```

👉 `/blog/test` → blog_test_router

---

## 理由

内部ではこうなる

```text
1. /blog/test → blog_router
2. /blog/test → blog_test_router
```

👉 上から順にチェック

---

# Laravelとの違い

| 項目 | Laravel | FastAPI |
|------|--------|--------|
| 重複ルート | エラー or 上書き | 両方登録 |
| 判定 | 明示的 | 順番 |

---

# 設計ルール（重要）

## NG

```text
同じURLを複数ルーターで定義
```

---

## OK

### ① 1つにまとめる

```python
router = APIRouter(prefix="/blog")

@router.get("/test")
```

---

### ② prefixを分ける

```text
/blog/test
/tools/test
```

---

# まとめ

FastAPI構成で「どこに何を書くか」は以下の通り整理できます。

---

## 各要素の役割

| 要素 | 役割 | やること |
|------|------|----------|
| uvicorn | Webサーバ | FastAPIアプリを起動する |
| FastAPI | ルーティング本体 | URLと処理をつなぐ |
| main.py | 司令塔 | ルーターを登録する |
| router.py | 入口 | URLごとの処理を書く |
| service.py | ロジック | データ処理を書く |

---

## 最小構成イメージ

### 起動

```bash
uvicorn app.main:app
```

---

### main.py

```python
app.include_router(blog_router, prefix="/blog")
```

---

### router.py

```python
@router.get("")
def index():
    return "記事一覧"
```

---

## 処理の流れ

```text
① ブラウザが /blog にアクセス
② uvicornが受け取る
③ FastAPIに渡す
④ main.pyの設定から該当routerを選択
⑤ router.pyの関数が実行される
⑥ 必要に応じてserviceを呼ぶ
⑦ 結果を返す
```

---

## 一言で

```text
URLが来たら対応する関数を実行する仕組みを分割して書いているだけ
```
