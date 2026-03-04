# syntheticdata-ideason

企業向け合成データ活用デモ基盤

HR部門（データオーナー）が機密性の高い従業員データから合成データを生成・公開し、社内の提案者がそのデータに対する分析アルゴリズムを提出・評価できるプラットフォームです。

## 特徴

- **合成データ生成**: 実データからノイズ付加・シャッフルにより統計的特性を保持した合成データを自動生成
- **品質レポート**: 合成データと実データの統計的類似度を定量評価
- **提出物の自動実行**: ZIP形式で提出されたPythonスクリプトをサンドボックス内で実行
- **Web UI + CLI**: 同一のビジネスロジックをWeb（HTMX対応）とCLIの両方から利用可能
- **ロールベースアクセス**: HR（データ管理）、提案者（分析提出）、管理者

## 技術スタック

- **Python** 3.11+
- **FastAPI** + Jinja2 + HTMX（Web UI）
- **Typer**（CLI）
- **SQLAlchemy** + SQLite（データベース）
- **Pandas / NumPy**（データ処理・合成データ生成）
- **uv**（パッケージ管理）

## セットアップ

```bash
# 依存関係のインストール
uv sync

# デモユーザーとサンプルデータの作成
app-cli users seed
app-cli demo seed-data

# Webサーバーの起動
uvicorn app.main:app --reload
```

http://localhost:8000 にアクセスし、以下のユーザーでログインできます:

| ユーザーID | ロール |
|---|---|
| `hr_demo` | HR（データオーナー） |
| `user_demo_01` | 提案者 |
| `user_demo_02` | 提案者 |
| `admin_demo` | 管理者 |

## 使い方

### Web UI

ログイン後、ロールに応じたダッシュボードに遷移します。

- **HR**: データセット作成 → 合成データ生成 → 公開 → 提出物レビュー → 実行・結果公開
- **提案者**: 公開データセット閲覧 → 分析スクリプト提出 → 結果確認

### CLI

```bash
# データセット作成
app-cli dataset create --owner hr_demo --name "従業員分析" \
  --employee-master ./examples/sample_data/employee_master.csv \
  --project-allocation ./examples/sample_data/project_allocation.csv \
  --working-hours ./examples/sample_data/working_hours.csv

# 合成データ生成・公開
app-cli synthetic generate --dataset-id DS0001
app-cli synthetic publish --dataset-id DS0001 --public true

# 提出物の作成・承認・実行
app-cli submission create --user user_demo_01 --dataset-id DS0001 \
  --title "分析スクリプト" --description "説明" \
  --zip ./examples/submissions/submission_ok.zip
app-cli submission approve --submission-id SUB0001 --approver hr_demo
app-cli execution run --submission-id SUB0001 --mode synthetic --executor hr_demo
```

すべてのコマンドは `--json` フラグで機械可読な出力に対応しています。

## 提出物の形式

提出物はZIPファイルで、以下を含みます:

```
submission.zip
├── manifest.json    # {"entry_point": "main.py", "requirements": []}
└── main.py          # エントリポイント
```

実行時に以下の引数が渡されます:

```bash
python main.py \
  --employee-master <path> \
  --project-allocation <path> \
  --working-hours <path> \
  --output <output.json>
```

サンプルは `examples/submissions/` を参照してください。

## 開発

```bash
# テスト実行
uv run pytest

# リンター
uv run ruff check .
uv run ruff check --fix .
```

## ディレクトリ構成

```
app/
├── cli/            # Typer CLIコマンド
├── db/             # SQLAlchemy モデル・セッション管理
├── schemas/        # Pydantic DTO
├── services/       # ビジネスロジック層（Web・CLI共通）
├── execution/      # 提出物の実行・バリデーション
├── synthetic/      # 合成データ生成・品質評価
├── storage/        # ファイルストレージ管理
├── web/            # FastAPIルーター・テンプレート・静的ファイル
├── config.py       # 設定値
├── dependencies.py # FastAPI DI
└── main.py         # アプリケーションエントリポイント
```

## ライセンス

Private
