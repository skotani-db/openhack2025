# Databricks notebook source
# MAGIC %md
# MAGIC # 起票されたケースの解決策を示すAgentを作成する(標準時間:60分)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 本ノートブックの目的：Databricks における AI Agent 
# MAGIC Q1. RAG Agent の構築<br>
# MAGIC Q2. Driver Notebookの実行とReview Apps による評価<br>
# MAGIC Q3. Review Apps による評価<br>
# MAGIC Q4. Databricks Apps との連携

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. RAG Agent の構築(標準時間：10分)

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks の AI Playground を使用して、`openhack-gpt-4o` エンドポイントと `manual_retriever` ツールを登録し、ringo computer のマニュアルに関する質問に答えるエージェントを作成する手順は以下の通りです:
# MAGIC
# MAGIC 1. Databricks ワークスペースにログインし、左側のナビゲーションから [機械学習] > [Playground] を選択します。
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
# MAGIC    - "電源の入れ方を教えてください。"
# MAGIC    - "メモリを増設する方法は？"
# MAGIC    - "OSをアップデートする手順を説明してください。"
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
# MAGIC 9. エクスポートされたノートブックを開き、コードを確認します。Q2でコードの調整を行います。
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. driver ノートブックの実行(標準時間：10分)

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️この手順は各班の**代表者1人**が行ってください。⚠️
# MAGIC
# MAGIC AI Playgroundで作成されたdriver ノートブックは、エージェントのログ記録、評価、登録、およびデプロイを行うための重要なコンポーネントです。
# MAGIC ![](/Workspace/Shared/contents_with_answer/data/img/driver_notebook.png)
# MAGIC
# MAGIC ### `driver` ノートブックの流れ
# MAGIC - Mosaic AIエージェントフレームワークを使用してLangGraphエージェントの定義を行います
# MAGIC - エージェントをテストして、MLflowにログ記録します
# MAGIC - エージェントをMosaic AI Agent Evaluation を使用して、レスポンスの評価を行います。
# MAGIC - エージェントをUnity Catalogのモデルに登録します
# MAGIC - エージェントをモデルサービングエンドポイントとReview Appにデプロイして、ステークホルダーに配布します
# MAGIC
# MAGIC ### メリット
# MAGIC - 自動生成されるため、開発時間を短縮できます
# MAGIC - エージェントのライフサイクル管理を一元化し、効率的に行えます
# MAGIC - MLflowとの統合により、実験管理や再現性の確保が容易になります
# MAGIC - Unity Catalogへの登録により、エージェントの共有と再利用が促進されます
# MAGIC - モデルサービングエンドポイントへのデプロイにより、本番環境での利用が簡単になります
# MAGIC
# MAGIC ### 手順
# MAGIC 基本的には、driver ノートブックを上から実行していきましょう！<br>
# MAGIC 注意として、エクスポートされたコードを AI Playground セッション と同じように動作させるためには以下の手順を実行する必要があります<br>
# MAGIC
# MAGIC 1. セル1において、`catalog`, `schema`, `model_name`を指定します。
# MAGIC ```
# MAGIC catalog = "{team}_catalog"
# MAGIC schema = "07_ai_agent_for_{username}"
# MAGIC model_name = "manual_agent"
# MAGIC ```
# MAGIC
# MAGIC 2. セル3において、インストールするlanggraphのバージョンを以下のように固定してください
# MAGIC ```langgraph==0.2.74```
# MAGIC
# MAGIC 3. セル5の`## Define agent logic`のすぐ下の行に以下のコードを追加します。これによって、後に検索器の性能評価が行えるようになります。
# MAGIC ```
# MAGIC mlflow.models.set_retriever_schema(
# MAGIC     name="manual_retriever",
# MAGIC     primary_key="chunk_id",
# MAGIC     text_column="content",
# MAGIC     doc_uri="doc_uri"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC 4. セル11において、モデルがデプロイ時にインストールするlanggraphのバージョンを固定してください。
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
# MAGIC 5. セル11において、mlflowのモデルが権限を持つ必要があるリソースを指定してください。 <br>
# MAGIC `mlflow.models.resources`のモジュールで、Databricksの各種リソースを指定することができます。 <br>
# MAGIC デフォルトで生成されたノートブックでは`DatabricksVectorSearchIndex`のモジュールのインポートが含まれていないので追加しましょう。
# MAGIC ```
# MAGIC from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint, DatabricksVectorSearchIndex
# MAGIC ```
# MAGIC 6. セル11において、Agentがアクセスする必要のあるリソース一覧を定義します。 <br>
# MAGIC これによってModel Servingでホストされているモデルがローカルと同様にリソースへアクセスできます。 <br>
# MAGIC 具体的には、ベクトルサーチのインデックスおよび検索の関数、埋め込みと生成のエンドポイントが必要です。 <br>
# MAGIC ベクトルサーチインデックスはカタログ名とスキーマ名を入力してから追加しましょう。
# MAGIC ```
# MAGIC resources = [
# MAGIC     DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME), 
# MAGIC     DatabricksServingEndpoint(endpoint_name="openhack-text-embedding-ada-002"),
# MAGIC     DatabricksVectorSearchIndex(index_name="{catalog}.{schema}.product_documentation_vs"),
# MAGIC     DatabricksFunction(function_name="{catalog}.common.manual_retriever"),
# MAGIC     ]
# MAGIC ```
# MAGIC 7. 最後のセルで Model Serving エンドポイントと Review App が作れたら完了です。<br>
# MAGIC Model Serving エンドポイントの初期作成には少し時間がかかるので、休憩を取ったり
# MAGIC [本番環境でのエージェントの評価のドキュメント](https://learn.microsoft.com/ja-jp/azure/databricks/generative-ai/agent-evaluation/evaluating-production-traffic)を見ながら、エージェントの品質を監視、改善していく取り組みについて考えましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Review Apps による評価

# COMMAND ----------

# MAGIC %md
# MAGIC ✅この手順は班の**皆さん**で実行しましょう
# MAGIC
# MAGIC driver ノートブックをランスルーして最後のセルを実行すると、`Review Apps`が起動します。
# MAGIC ![](/Workspace/Shared/contents_with_answer/data/img/review_app_url.png)
# MAGIC
# MAGIC Databricks Review Appsは、AIアプリケーションの品質評価を効率的に行うためのツールです。その概要と意義、および評価手順は以下の通りです:
# MAGIC
# MAGIC ### `Review Apps`の概要
# MAGIC
# MAGIC 1. 人間のレビュー担当者からAIアプリケーションの品質に関するフィードバックを簡単に収集できます
# MAGIC 2. 専門家の利害関係者がLLMと対話し、フィードバックを提供する環境を提供します
# MAGIC 3. すべての質問、回答、フィードバックを推論テーブルに記録し、LLMのパフォーマンスを分析可能にします
# MAGIC 4. アプリケーションが提供する回答の品質と安全性を確保するのに役立ちます
# MAGIC
# MAGIC ### Review Apps UIにアクセスした後の評価手順
# MAGIC
# MAGIC #### 生成評価
# MAGIC
# MAGIC 1. **ボットをテストする**タブでAIとチャットを開始する
# MAGIC 2. AIの回答を評価し、「はい」「いいえ」「わからない」から選択する
# MAGIC 3. 必要に応じて回答を直接編集する
# MAGIC 4. 追加情報や具体的なフィードバックを提供する
# MAGIC
# MAGIC #### 検索評価
# MAGIC
# MAGIC 1. `retrieved_context`からチャンクを確認する
# MAGIC 2. 関連性の高いチャンクに対して「サムズアップ👍」を選択する
# MAGIC 3. 選択されたチャンクの`doc_uri`が質問の`expected_retrieved_context`に含まれる
# MAGIC
# MAGIC #### 評価データの記録
# MAGIC
# MAGIC 1. 👍（親指を立てた）リクエスト:
# MAGIC    - `request`: ユーザーの入力
# MAGIC    - `expected_response`: ユーザーが編集した応答、または編集がない場合はモデルの生成した応答
# MAGIC
# MAGIC 2. 👎（親指を下げた）リクエスト:
# MAGIC    - `request`: ユーザーの入力
# MAGIC    - `expected_response`: ユーザーが編集した応答、または編集がない場合はnull
# MAGIC
# MAGIC 3. フィードバックのないリクエスト:
# MAGIC    - `request`: ユーザーの入力
# MAGIC
# MAGIC これらの手順により、AIアプリケーションの品質向上と、人間のフィードバックを効果的に収集・分析することが可能になります。Databricksは評価セットに少なくとも30の質問を含めることを推奨しています。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4. Databricks Apps との連携

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️この手順は各班の**代表者1人**が行ってください。⚠️
# MAGIC
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
# MAGIC 5. アプリのテンプレートとして「Chatbot」のオプションを選択します。
# MAGIC
# MAGIC 6. エージェントのエンドポイントを指定します。
# MAGIC
# MAGIC 7. 設定が完了したら、「デプロイ」または「作成」ボタンをクリックします。
# MAGIC
# MAGIC 8. デプロイが完了すると、アプリの詳細ページが表示されます。ここでステータスや公開URLを確認できます。
# MAGIC
# MAGIC 9. 「デプロイメント」セクションに表示されているアプリのコードが保存されているURLをクリックします。
# MAGIC
# MAGIC 10. ワークスペースのエクスプローラーで`app.py`を開き、`query_llm`関数を以下のように丸ごと置換します。
# MAGIC
# MAGIC ```
# MAGIC def query_llm(message, history):
# MAGIC     """
# MAGIC     Query the LLM with the given message and chat history.
# MAGIC     """
# MAGIC     from mlflow.deployments import get_deploy_client
# MAGIC     # Initialize the mlflow deployment client
# MAGIC     client = get_deploy_client("databricks")
# MAGIC     
# MAGIC     if not message.strip():
# MAGIC         return "ERROR: The question should not be empty"
# MAGIC
# MAGIC     prompt = "Answer this question like a helpful assistant: "
# MAGIC     messages = prompt + message
# MAGIC
# MAGIC     try:
# MAGIC         logger.info(f"Sending request to model endpoint: {os.getenv('SERVING_ENDPOINT')}")
# MAGIC         input_data = {
# MAGIC             "dataframe_records": [
# MAGIC                 {
# MAGIC                     "messages": [
# MAGIC                         {
# MAGIC                             "role": "user",
# MAGIC                             "content": messages
# MAGIC                         }
# MAGIC                     ]
# MAGIC                 }
# MAGIC             ]
# MAGIC         }
# MAGIC
# MAGIC         response = client.predict(endpoint=os.getenv('SERVING_ENDPOINT'), inputs=input_data)
# MAGIC         return response["predictions"]["messages"][-1]["content"]
# MAGIC     
# MAGIC     except Exception as e:
# MAGIC         logger.error(f"Error querying model: {str(e)}", exc_info=True)
# MAGIC         return f"Error: {str(e)}"
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC 11. 8で開いたアプリの詳細ページへ戻り、実行中と表示されているURLをクリックして、実際にアプリを使用してみましょう！
# MAGIC
# MAGIC ### 変更が必要な理由
# MAGIC デフォルトの処理では、Databricks SDK の Workspace Client から Model Serving Endpoint へクエリをしていました。 <br>
# MAGIC しかし、`databricks.sdk.service.serving.Servingendpoints`のドキュメントは、パスするパラメータが明示的でなく、混乱されるユーザーが多いです。
# MAGIC したがって、`mlflow.deployments`の Client からクエリをすることを推奨します。 <br>
# MAGIC
# MAGIC [Databricks SDK](https://databricks-sdk-py.readthedocs.io/en/stable/workspace/serving/serving_endpoints.html#databricks.sdk.service.serving.ServingEndpointsExt.query) <br>
# MAGIC [mlflw deployments](https://mlflow.org/docs/latest/python_api/mlflow.deployments.html#mlflow.deployments.DatabricksDeploymentClient.predict) <br>
# MAGIC [mlflow deployments チュートリアル](https://mlflow.org/docs/latest/llms/deployments/guides/step2-query-deployments.html)
