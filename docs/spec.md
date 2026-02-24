
# spec.md（改訂版）

## 企業向け 合成データ活用デモ基盤（FastAPI + HTMX / Web + CLI両対応）

## 1. 目的（改訂）

本システムは、社内の機微データを保持するデータオーナー部門（例：人事部）が、実データを直接開示せずに、合成データを介して全社から分析活用アイディアと分析ロジックを募集・評価するためのデモ基盤である。

今回の改訂では、以下を追加目的とする。

* **同一の業務機能を Web UI と CLI の双方から実行できること**
* **Web/CLI の差分を UI 層だけに閉じ込め、サービス層を共通化すること**
* **将来的な自動化（CI, バッチ, API連携）につながるCLI運用を先に確立すること**

---

## 2. 技術選定（確定）

### 2.1 採用技術

* **Backend / Web**

  * Python 3.11+（推奨）
  * FastAPI
  * Jinja2 Templates（サーバーサイドレンダリング）
  * HTMX（画面部分更新）
* **CLI**

  * Python CLI（`typer` 推奨、または `argparse`）
* **DB**

  * SQLite（ローカルMVP）
  * SQLAlchemy（ORM）
* **Validation**

  * Pydantic
* **Testing**

  * pytest
* **Packaging / Run**

  * uv または venv + pip
* **Optional**

  * pandas（CSV処理）
  * numpy（合成データ生成）
  * python-multipart（ファイルアップロード）

### 2.2 技術選定理由

* **FastAPI**

  * API/HTML/内部処理の責務を整理しやすい
  * DIとテスト容易性が高い ([FastAPI][1])
* **HTMX**

  * SPAを組まずに、サーバーレンダリングで動的UIを実現しやすい
  * MVPデモの速度と保守性を両立しやすい ([htmx][2])
* **CLI併設**

  * デモ用操作だけでなく、将来のバッチ/自動評価運用に直結する
  * Web依存を避けた回帰テスト導線を作れる

---

## 3. 重要設計方針（今回の中核）

## 3.1 Hexagonal / Layered に近い分離（実装簡素版）

**UI（Web/CLI）から直接DB・ファイル処理を書かない。**
必ずサービス層を経由する。

### レイヤ構成

1. **Presentation**

   * Web routes (FastAPI + HTML/HTMX)
   * CLI commands
2. **Application / Service**

   * 業務フロー（登録、合成、公開、提出、承認、実行）
3. **Domain (軽量)**

   * エンティティ、状態遷移ルール
4. **Infrastructure**

   * SQLite/SQLAlchemy
   * File storage
   * Execution runner
   * CSV I/O

## 3.2 WebとCLIは同じユースケースを呼ぶ

例：

* Web画面「合成データ生成」ボタン → `SyntheticDatasetService.generate(...)`
* CLI `synthetic generate ...` → `SyntheticDatasetService.generate(...)`

これにより、**UIの違いで挙動がずれる問題**を抑える。

---

## 4. 実行モード（Web / CLI）

## 4.1 Webモード（デモ用）

* FastAPI サーバーを起動
* ブラウザから操作
* HTMXで部分更新（一覧更新、進捗表示、結果表示）

### 想定URL例

* `/login`
* `/hr/datasets`
* `/hr/datasets/{id}`
* `/hr/datasets/{id}/synthetic`
* `/proposer/datasets`
* `/proposer/submissions/new`
* `/hr/submissions/{id}`
* `/hr/executions/{id}`

## 4.2 CLIモード（運用/検証用）

* ローカル端末からコマンド実行
* Webなしで同等フローを実行可能

### CLIカテゴリ（例）

* `users`（初期ユーザー作成）
* `dataset`（登録・一覧）
* `synthetic`（生成・公開）
* `submission`（提出・一覧）
* `execution`（承認・実行・結果確認）
* `demo`（サンプル一括投入）

---

## 5. CLI仕様（追加）

以下はClaude Codeに実装させるための**具体的コマンド仕様（MVP）**。

## 5.1 CLIコマンド体系（例）

```bash
app-cli users seed
app-cli demo seed-data

app-cli dataset create \
  --owner hr_demo \
  --name "HR Demo Dataset Jan" \
  --employee-master ./data/employee_master.csv \
  --project-allocation ./data/project_allocation.csv \
  --working-hours ./data/working_hours.csv

app-cli dataset list
app-cli dataset show --dataset-id DS0001

app-cli synthetic generate --dataset-id DS0001 --seed 42
app-cli synthetic publish --dataset-id DS0001 --public true

app-cli submission create \
  --user user_demo_01 \
  --dataset-id DS0001 \
  --title "残業偏在の検出" \
  --description "高残業・高稼働の継続を検出" \
  --zip ./examples/submission_ok.zip

app-cli submission list --dataset-id DS0001

app-cli submission approve --submission-id SUB0001 --approver hr_demo
app-cli execution run --submission-id SUB0001 --mode real --executor hr_demo

app-cli execution show --execution-id EX0001
app-cli execution publish-result --execution-id EX0001 --scope submitter
```

## 5.2 CLIの出力方針

* 標準出力：人が読みやすい形式
* `--json` オプション：機械処理向けJSON
* エラー時：

  * 非0終了コード
  * 明確なメッセージ（入力不足、権限違反、ファイル不正 等）

---

## 6. Web UI仕様（FastAPI + HTMX向けに再定義）

## 6.1 UI実装原則

* 基本はサーバーサイドHTML（Jinja2）
* 部分更新だけ HTMX
* JSは最小限（HTMX + 少量の補助のみ）
* 画面状態はサーバー側を正とする

## 6.2 HTMX利用箇所（MVP）

### A. 人事部：データセット一覧更新

* データ登録後に一覧部分だけ更新
* `hx-post` + `hx-target="#dataset-list"` を利用

### B. 人事部：合成生成結果表示

* 実行ボタン押下後に品質サマリ領域だけ差し替え
* `hx-post` → fragment HTML返却

### C. 全社メンバ：提出フォーム送信結果

* バリデーション結果をフォーム下に表示
* 成功時は提出一覧断片を更新

### D. 人事部：承認・実行ボタン

* 行単位で状態バッジ更新（承認済、実行済、失敗）

## 6.3 HTMXレスポンス設計の注意

* 通常HTTPアクセス時：フルHTML
* HTMXリクエスト時：HTML断片
* `HX-Request` ヘッダを見て分岐（必要に応じて `Vary: HX-Request` を考慮）([htmx][2])

---

## 7. API / ルーティング設計（FastAPI）

## 7.1 ルーティングの分割（推奨）

* `web_auth_router`
* `web_hr_router`
* `web_proposer_router`
* `api_internal_router`（必要最低限）
* `api_cli_compat_router`（将来拡張用、MVPでは不要でも可）

> 今回は「HTMX + サーバーHTML」が主でよい。JSON APIは内部/将来用に限定。

## 7.2 Dependency Injection（FastAPI）

依存性注入で以下を供給：

* DBセッション
* 現在ユーザー
* ストレージ設定
* サービスクラスファクトリ

FastAPIのDI機構を用い、テスト時は `dependency_overrides` で差し替え可能な構造にする。([FastAPI][1])

---

## 8. 共通サービス層（Web/CLI共通）

以下を **Claude Codeで先に実装** させる（UIより先に）。

## 8.1 サービス一覧

* `AuthService`
* `DatasetService`
* `SyntheticGenerationService`
* `PublicationService`
* `SubmissionService`
* `ExecutionService`
* `ResultService`
* `AuditLogService`

## 8.2 サービスメソッド例

### DatasetService

* `create_dataset(owner_user_id, name, files...)`
* `list_datasets_for_owner(owner_user_id)`
* `list_published_datasets()`
* `get_dataset(dataset_id, actor)`

### SyntheticGenerationService

* `generate(dataset_id, actor, options)`
* `get_quality_report(dataset_id, actor)`

### SubmissionService

* `create_submission(actor, dataset_id, title, description, zip_file)`
* `validate_submission_package(path)`
* `list_submissions(dataset_id, actor)`
* `approve_submission(submission_id, approver)`
* `reject_submission(submission_id, approver, reason)`

### ExecutionService

* `run_submission(submission_id, executor, mode)`  # mode: synthetic/real
* `get_execution_result(execution_id, actor)`
* `publish_execution_result(execution_id, actor, scope)`

---

## 9. 実行インターフェース（提出プログラムIF）※CLI/Web共通

前回仕様を維持しつつ、**Runner層で統一**する。

## 9.1 実行契約

* システムは提出物を展開し、規定CLIで `main.py` を実行
* 入力：CSV paths
* 出力：JSON file path

```bash
python main.py \
  --employee-master <path> \
  --project-allocation <path> \
  --working-hours <path> \
  --output <path_to_output_json>
```

## 9.2 Runner層

`ExecutionRunner` を作成し、Web/CLIの双方はこれを呼ぶだけにする。

### 役割

* 展開ディレクトリ作成
* manifest確認
* subprocess実行
* timeout管理
* stdout/stderr保存
* output.json検証
* execution record保存

---

## 10. ドメインモデル / 状態遷移（明確化）

## 10.1 主なエンティティ

* `User`
* `Dataset`
* `DatasetFile`
* `SyntheticArtifact`
* `Submission`
* `Execution`
* `ExecutionResult`
* `AuditLog`

## 10.2 Submission状態

* `draft`（任意）
* `submitted`
* `validation_failed`
* `under_review`
* `approved`
* `rejected`
* `executed_synthetic`（任意）
* `executed_real`
* `execution_failed`

## 10.3 Execution状態

* `queued`（MVPでは即時実行でも可）
* `running`
* `succeeded`
* `failed`
* `timeout`

> 状態をDBで持つことで、Web/CLIどちらから触っても整合性を保てる。

---

## 11. ディレクトリ構成（FastAPI + HTMX + CLI対応版）

```text
project-root/
  app/
    main.py                 # FastAPI app entry
    cli.py                  # Typer/argparse entry
    config.py
    dependencies.py
    db/
      base.py
      session.py
      models.py
      repositories/
    schemas/
      dto.py
      forms.py
    services/
      auth_service.py
      dataset_service.py
      synthetic_service.py
      submission_service.py
      execution_service.py
      result_service.py
      audit_service.py
    execution/
      runner.py
      package_validator.py
      output_validator.py
    synthetic/
      generator.py
      quality_report.py
    web/
      routers/
        auth.py
        hr.py
        proposer.py
      templates/
        base.html
        login.html
        hr/
        proposer/
        fragments/
      static/
        css/
    cli/
      commands/
        users.py
        dataset.py
        synthetic.py
        submission.py
        execution.py
        demo.py
    storage/
      file_store.py
    utils/
      ids.py
      time.py
      csv_utils.py
  data_store/
    real/
    synthetic/
    submissions/
    results/
    logs/
  db/
    app.db
  examples/
    sample_data/
    submissions/
      submission_ok.zip
      submission_bad.zip
  tests/
    unit/
    integration/
    e2e/
```

---

## 12. Web/CLI共通の認証・権限仕様（MVP）

## 12.1 Web

* セッションベース認証（簡易）
* ログイン後にロール別画面へ遷移
* ルータ依存性でロールチェック

## 12.2 CLI

* `--user` 指定で操作主体を明示（MVP）
* コマンド実行時にロール・権限チェックをサービス層で実施
* 将来はトークン認証に変更可能

### 例

```bash
app-cli synthetic generate --dataset-id DS0001 --user hr_demo
# proposerユーザーが実行した場合は権限エラー
```

---

## 13. データ入出力インターフェース（Web/CLI共通の標準化）

## 13.1 CSV列定義の管理

* `schemas/` で期待列を定義
* Webアップロード時とCLI登録時で同じバリデーションを使う

## 13.2 バリデーションエラー例

* 必須列不足
* 列名の表記ゆれ
* 型変換不可（数値列に文字列）
* キー重複
* 月フォーマット不正

## 13.3 エラーメッセージ方針

* ユーザーに修正可能な形で返す
* 例：「working_hours.csv の `overtime_hours` 列がありません」

---

## 14. テスト方針（Web/CLI両対応を踏まえた改訂）

## 14.1 テスト戦略

### A. サービス層単体テスト（最優先）

* Web/CLI共通ロジックの品質確保
* ここが通ればUI差分で壊れにくい

### B. CLI統合テスト

* コマンド投入 → DB/ファイル生成確認
* `subprocess` を使ったE2E風テストも可

### C. Webルートテスト

* FastAPI TestClientでHTML/HTMXレスポンス確認
* 権限チェック
* フォーム投稿

### D. デモシナリオE2E（簡易）

* `demo seed-data`
* Webで提出
* CLIで承認・実行（または逆）
* 結果表示まで通す

## 14.2 HTMXテスト観点（追加）

* `HX-Request` 有無で返すテンプレートが切り替わる
* fragment返却時に必要要素IDが存在する
* 部分更新後の状態がDBと一致する

---

## 15. 非機能要件（改訂）

## 15.1 操作性

* Webでのデモが直感的であること
* CLIでの再現がスクリプト化しやすいこと

## 15.2 保守性

* UIロジックをサービス層に持ち込まない
* Web/CLI双方から同一サービスを呼ぶ
* DTO/フォーム/DBモデルを混在させない

## 15.3 拡張性

* 将来のAPI公開、非同期実行、ジョブキューに移行しやすい構造
* Runner/Storage/SyntheticGeneratorを差し替え可能にする

---

## 16. 受け入れ基準（改訂版）

MVP完了条件に、以下を追加する。

### 16.1 Web受け入れ

* ログイン後、ロール別画面に遷移できる
* 人事部がWebからデータ登録→合成→公開できる
* 全社メンバがWebから合成データ閲覧→提出できる
* 人事部がWebから承認→実行→結果確認できる

### 16.2 CLI受け入れ

* CLIでデータ登録→合成生成→公開ができる
* CLIで提出登録→承認→実行→結果確認ができる
* Webで作成したデータ/提出物をCLIから扱える（逆も同様）

### 16.3 共通性の受け入れ（重要）

* Web/CLIで同じサービス層を呼んでいる（重複実装なし）
* 権限判定がサービス層で一貫している
* 実行結果JSONの検証ロジックが共通化されている

---

## 17. Claude Code向け実装順序（推奨）

実装を円滑にするため、以下順で進める。

### Phase 1: コア実装（UIなし）

1. DBモデル
2. ストレージ層
3. サービス層
4. 提出物Runner
5. 合成データ生成（簡易）
6. 単体テスト

### Phase 2: CLI

7. `app-cli` 実装
8. CLI統合テスト
9. サンプルデータ/サンプル提出物同梱

### Phase 3: Web（FastAPI + HTMX）

10. 認証/セッション
11. 人事部画面
12. 全社メンバ画面
13. HTMX部分更新
14. Webテスト

### Phase 4: デモ整備

15. `demo seed-data`
16. デモ操作台本
17. README（起動方法、制約事項、セキュリティ注意）

---

## 18. 実装上の注意（ブレ防止）

* **FastAPIはAPI専用にせず、HTMLも返す**（今回のMVPではそれが主役）
* **HTMXを使うが、過度にJSを書かない**
* **CLIとWebで別々に業務ロジックを書かない**
* **提出プログラムIFは固定（CLI + JSON）**
* **任意コード実行の制約はREADMEに明記**
* **MVPで非同期ジョブ化しない（同期で可）**
  ※ 将来 `ExecutionService` の中をジョブキュー化できるようにしておく

---

# 追加で作ると実装が速くなる資料（次の一手）

必要であれば、次に以下を作成します。Claude Codeの実装効率がかなり上がります。

1. **`plugin_spec.md`**（提出プログラムIFの完全仕様：manifest/output JSON schema 含む）
2. **`cli_spec.md`**（各コマンドの引数・戻り値・終了コード）
3. **`screen_spec.md`**（FastAPI + HTMX向け画面遷移・fragment一覧）
4. **`tasks.md`**（task-001〜の実装分解）

ここまで定義すると、Claude Codeに「Phase 1から順に実装」と指示しやすくなります。

[1]: https://fastapi.tiangolo.com/tutorial/dependencies/?utm_source=chatgpt.com "Dependencies"
[2]: https://htmx.org/docs/?utm_source=chatgpt.com "</> htmx ~ Documentation"

