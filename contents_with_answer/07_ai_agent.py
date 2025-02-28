# Databricks notebook source
# MAGIC %md
# MAGIC # 起票されたケースの解決策を示すAgentを作成する(標準時間:60分)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 本ノートブックの目的：Databricks における AI Agent 
# MAGIC Q1. RAG Agent の構築<br>
# MAGIC Q2. Driver Notebookの実行とReview Apps による評価<br>
# MAGIC Q3. Databricks Apps との連携

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. RAG Agent の構築(標準時間：10分)

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks の AI Playground を使用して、`openhack-gpt-4o` エンドポイントと `manual_retriever` ツールを登録し、ringo computer のマニュアルに関する質問に答えるエージェントを作成する手順は以下の通りです:
# MAGIC
# MAGIC 1. Databricks ワークスペースにログインし、左側のナビゲーションから [Machine Learning] > [Playground] を選択します。
# MAGIC
# MAGIC 2. AI Playground の画面で、左上のドロップダウンメニューから `openhack-gpt-4o` エンドポイントを選択します。
# MAGIC ![](/Workspace/Shared/contents_with_answer/data/img/endpoint_selection.png)
# MAGIC
# MAGIC 3. [ツール] セクションを探し、`manual_retriever` ツールを選択します。このツールが表示されない場合は、カスタムツールとして追加する必要があります。
# MAGIC ![](/Workspace/Shared/contents_with_answer/data/img/tool_selection.png)
# MAGIC
# MAGIC 4. システムプロンプトを設定します。例えば:
# MAGIC    "あなたは ringo computer のマニュアルに関する質問に答えるエキスパートです。manual_retriever ツールを使用して、適切な情報を取得し、ユーザーの質問に答えてください。"
# MAGIC    ![](/Workspace/Shared/contents_with_answer/data/img/export_notebook.png)
# MAGIC
# MAGIC 5. チャットインターフェースを使用して、エージェントとの対話をテストします。以下のようなサンプル質問を試してみましょう:
# MAGIC    - "ringo computer の電源の入れ方を教えてください。"
# MAGIC    - "ringo computer のメモリを増設する方法は？"
# MAGIC    - "ringo computer のOSをアップデートする手順を説明してください。"
# MAGIC
# MAGIC 6. エージェントの応答を確認し、必要に応じてシステムプロンプトや設定を調整します。
# MAGIC
# MAGIC 7. エージェントの動作に満足したら、画面右上の [エクスポート] ボタンをクリックします。
# MAGIC
# MAGIC 8. エクスポートされたノートブックには以下のファイルが含まれます:
# MAGIC    - `agent` ノートブック: LangChain を使用してエージェントを定義する Python コード
# MAGIC    - `driver` ノートブック: エージェントのログ記録、トレース、登録、デプロイ用のコード
# MAGIC    - `config.yml`: エージェントの設定情報
# MAGIC
# MAGIC 9. エクスポートされたノートブックを開き、コードを確認します。必要に応じて調整を行います。
# MAGIC
# MAGIC 10. `driver` ノートブックを実行して、エージェントをモデルサービングエンドポイントにデプロイします。
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. Driver Notebookの実行とReview Apps による評価(標準時間：10分)
# MAGIC AI Playgroundで作成されたdriver notebookは、エージェントのログ記録、評価、登録、およびデプロイを行うための重要なコンポーネントです。
# MAGIC
# MAGIC ### `driver` ノートブックの流れ
# MAGIC - Mosaic AIエージェントフレームワークを使用してPythonエージェントの処理を行います
# MAGIC - エージェントをMLflowにログ記録します
# MAGIC - エージェントをUnity Catalogに登録します
# MAGIC - エージェントをモデルサービングエンドポイントにデプロイします
# MAGIC - エージェントを評価するReview Appをデプロイして、ステークホルダーに配布できます
# MAGIC
# MAGIC ### メリット
# MAGIC - 自動生成されるため、開発時間を短縮できます
# MAGIC - エージェントのライフサイクル管理を一元化し、効率的に行えます
# MAGIC - MLflowとの統合により、実験管理や再現性の確保が容易になります
# MAGIC - Unity Catalogへの登録により、エージェントの共有と再利用が促進されます
# MAGIC - モデルサービングエンドポイントへのデプロイにより、本番環境での利用が簡単になります
# MAGIC
# MAGIC ### 注意
# MAGIC エクスポートされたコードは AI Playground セッションとは異なる動作をする可能性があります。<br>
# MAGIC 1. `driver` ノートブックのセル3でインストールするlanggraphのバージョンを以下のように固定してください
# MAGIC ```langgraph==0.2.74```
# MAGIC
# MAGIC 2. セル11においてlanggraphバージョンを固定してください。
# MAGIC ```
# MAGIC pip_requirements=[
# MAGIC             "mlflow",
# MAGIC             "langchain",
# MAGIC             "langgraph==0.2.74",
# MAGIC             "databricks-langchain",
# MAGIC             "unitycatalog-langchain[databricks]",
# MAGIC             "pydantic",
# MAGIC         ]
# MAGIC ```
# MAGIC
# MAGIC 3. セル11において、mlflowのモデルが権限を持つ必要があるリソースを指定する必要があります。`mlflow.models.resources`のモジュールからエンドポイントとベクトルサーチインデックスを指定します。
# MAGIC ```
# MAGIC from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint, DatabricksVectorSearchIndex
# MAGIC ```
# MAGIC 4. セル11において、DatabricksVectorSearchIndex のリソースを追加します。カタログとスキーマを埋めましょう。
# MAGIC ```
# MAGIC resources = [
# MAGIC     DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME), 
# MAGIC     DatabricksServingEndpoint(endpoint_name="openhack-text-embedding-ada-002"),
# MAGIC     DatabricksVectorSearchIndex(index_name="{catalog}.{schema}.product_documentation_vs"),
# MAGIC     DatabricksFunction(function_name="{catalog}.common.manual_retriever"),
# MAGIC     ]
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Databricks Apps との連携
# MAGIC Databricks AppsのGUIを使用してchatbot agentをデプロイする手順は以下の通りです:
# MAGIC
# MAGIC 1. Databricksワークスペースにログインします。
# MAGIC
# MAGIC 2. 左側のナビゲーションバーで「コンピュート」をクリックし、「アプリ」タブに移動します。
# MAGIC
# MAGIC 3. 「新規アプリの作成」ボタンをクリックします。
# MAGIC
# MAGIC 4. アプリの名前を入力します。例えば「chatbot-agent」などです。
# MAGIC
# MAGIC 5. アプリのテンプレートとして「Chatbot」または類似のオプションを選択します。
# MAGIC
# MAGIC 6. アプリの設定画面で、以下の情報を入力します:
# MAGIC    - アプリの説明
# MAGIC    - 
# MAGIC    - 環境変数（必要に応じて）
# MAGIC
# MAGIC 7. エージェントのエンドポイントを指定します。
# MAGIC
# MAGIC 8. MLflow ChatModelを使用してagentを実装していることを確認します。これにより、Databricks AI Agent機能との互換性が確保されます。
# MAGIC
# MAGIC 9. 設定が完了したら、「デプロイ」または「作成」ボタンをクリックします。
# MAGIC
# MAGIC 10. デプロイが完了すると、アプリの詳細ページが表示されます。ここでステータスや公開URLを確認できます。
# MAGIC
# MAGIC 11. アプリのテストを行い、必要に応じて設定やコードを調整します。
# MAGIC
# MAGIC 12. 公開の準備ができたら、アクセス権限を設定し、公開URLを通じてユーザーがアプリにアクセスできるようにします。
