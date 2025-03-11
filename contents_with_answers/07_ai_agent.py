# Databricks notebook source
# MAGIC %md
# MAGIC # 起票されたケースの解決策を示すAgentを作成する(標準時間:80分)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 本ノートブックの目的：Databricks における AI Agent 
# MAGIC Q1. RAG Agent の構築<br>
# MAGIC Q2. Driver Notebookの実行とReview Apps による評価<br>
# MAGIC Q3. Review Apps による評価<br>
# MAGIC Q4. Databricks Apps との連携<br>
# MAGIC Challenge 1. 推論テーブルの分析によるエージェントのモニタリング<br>
# MAGIC Challenge 2. 自律的なAgentの作成

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. RAG Agent の構築(標準時間：10分)

# COMMAND ----------

# MAGIC %md
# MAGIC ✅この手順は班の**皆さん**で実行しましょう
# MAGIC
# MAGIC Databricks の AI Playground を使用して、`openhack-gpt-4o` エンドポイントと `manual_retriever` ツールを登録し、ringo computer のマニュアルに関する質問に答えるエージェントを作成する手順は以下の通りです:
# MAGIC
# MAGIC 1. Databricks ワークスペースにログインし、左側のナビゲーションから [機械学習] > [Playground] を選択します。
# MAGIC
# MAGIC 2. AI Playground の画面で、左上のドロップダウンメニューから `openhack-gpt-4o` エンドポイントを選択します。
# MAGIC ![](/Workspace/Shared/openhack2025/contents_with_answers/data/img/endpoint_selection.png)
# MAGIC
# MAGIC 3. [ツール] セクションを探し、`manual_retriever` ツールを選択します。このツールが表示されない場合は、カスタムツールとして追加する必要があります。
# MAGIC ![](/Workspace/Shared/openhack2025/contents_with_answers/data/img/tool_selection.png)
# MAGIC
# MAGIC 4. チャットインターフェースを使用して、エージェントとの対話をテストします。以下のようなサンプル質問を試してみましょう:
# MAGIC    - "電源の入れ方を教えてください。"
# MAGIC    - "メモリを増設する方法は？"
# MAGIC    - "OSをアップデートする手順を説明してください。"
# MAGIC
# MAGIC 5. エージェントの応答を確認し、必要に応じてシステムプロンプトや設定を調整します。
# MAGIC
# MAGIC 6. エージェントの動作に満足したら、画面右上の [エクスポート] ボタンをクリックします。
# MAGIC    ![](/Workspace/Shared/openhack2025/contents_with_answers/data/img/export_notebook.png)
# MAGIC
# MAGIC 7. エクスポートされたノートブックには以下のファイルが含まれます:
# MAGIC    - `agent` ノートブック: LangChain を使用してエージェントを定義する Python コード
# MAGIC    - `driver` ノートブック: エージェントのログ記録、トレース、登録、デプロイ用のコード
# MAGIC    - `config.yml`: エージェントの設定情報
# MAGIC 8. エクスポートされたノートブックを開き、コードを確認します。Q2でコードの調整を行います。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. driver ノートブックの実行(標準時間：30分)

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️この手順は各班の**代表者1人**が行ってください。⚠️
# MAGIC
# MAGIC AI Playgroundで作成されたdriverノートブックは、エージェントのログ記録、評価、登録、およびデプロイを行うための重要なコンポーネントです。
# MAGIC driverノートブックの記述で分からないポイントがあれば、Databricks Assistantへ聞いてみましょう！
# MAGIC ![](/Workspace/Shared/openhack2025/contents_with_answers/data/img/driver_notebook.png)
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
# MAGIC 1. セル2において、インストールするlanggraphのバージョンを以下のように固定してください
# MAGIC ```langgraph==0.2.74```
# MAGIC
# MAGIC 2. セル4の`## Define agent logic`のすぐ下の行に以下のコードを追加します。これによって、後に検索器の性能評価が行えるようになります。
# MAGIC ```
# MAGIC mlflow.models.set_retriever_schema(
# MAGIC     name="manual_retriever",
# MAGIC     primary_key="chunk_id",
# MAGIC     text_column="content",
# MAGIC     doc_uri="doc_uri"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC 3. セル10において、モデルがデプロイ時にインストールするlanggraphのバージョンを固定してください。
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
# MAGIC 4. セル10において、mlflowのモデルが権限を持つ必要があるリソースを指定してください。 <br>
# MAGIC `mlflow.models.resources`のモジュールで、Databricksの各種リソースを指定することができます。 <br>
# MAGIC デフォルトで生成されたノートブックでは`DatabricksVectorSearchIndex`のモジュールのインポートが含まれていないので追加しましょう。
# MAGIC ```
# MAGIC from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint, DatabricksVectorSearchIndex
# MAGIC ```
# MAGIC 5. セル10において、Agentがアクセスする必要のあるリソース一覧を定義します。 <br>
# MAGIC これによってModel Servingでホストされているモデルがローカルと同様にリソースへアクセスできます。 <br>
# MAGIC 具体的には、ベクトルサーチのインデックスおよび検索の関数、埋め込みと生成のエンドポイントが必要です。 <br>
# MAGIC 大括弧内の{catalog}はあらかじめ置換してから、コードに追加しましょう
# MAGIC ```
# MAGIC resources = [
# MAGIC     DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME), 
# MAGIC     DatabricksServingEndpoint(endpoint_name="openhack-text-embedding-ada-002"),
# MAGIC     DatabricksVectorSearchIndex(index_name=f"{catalog}.common.product_documentation_vs"),
# MAGIC     DatabricksFunction(function_name=f"{catalog}.common.manual_retriever"),
# MAGIC     ]
# MAGIC ```
# MAGIC
# MAGIC 6. セル17において、`catalog`, `schema`, `model_name`を指定します。
# MAGIC ```
# MAGIC catalog = "{team}_catalog"
# MAGIC schema = "common"
# MAGIC model_name = "manual_agent"
# MAGIC ```
# MAGIC
# MAGIC 7. 最後のセルで Model Serving エンドポイントと Review App が作れたら完了です。<br>
# MAGIC Model Serving エンドポイントの作成が完了したら、エンドポイントの詳細ページに行って、班全員に「Can Query」の権限を付与してください。<br>
# MAGIC Model Serving エンドポイントの初期作成には少し時間がかかるので、休憩を取りましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Review Apps による評価(標準時間：25分)

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️各班の**代表者1人**がデプロイしたReview Appsを用いて、**班全員**でレビューに参加しましょう。⚠️
# MAGIC
# MAGIC driver ノートブックをランスルーして最後のセルを実行すると、`Review Apps`が起動します。<br>
# MAGIC
# MAGIC ![](/Workspace/Shared/openhack2025/contents_with_answers/data/img/review_app_url.png)
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
# MAGIC 1. **ボットをテストする**タブでAIとチャットを開始します
# MAGIC 2. 以下のようなサンプル質問をAgentに聞いてみましょう (いくつか選んで行いましょう)
# MAGIC    - コンピュータの電源が入らない場合、どのように対処すればよいですか？
# MAGIC    - タッチペンが反応しなくなりました。原因と解決方法を教えてください。
# MAGIC    - Wi-Fiに接続できないときのチェックポイントを教えてください。
# MAGIC    - 画面がフリーズする原因と対策を教えてください。
# MAGIC    - バッテリーの寿命を延ばす方法を教えてください。
# MAGIC    - 初回セットアップの手順を詳しく教えてください。
# MAGIC    - タッチパネルとタッチペンの違いは何ですか？
# MAGIC    - 外部モニターに接続する方法を教えてください。
# MAGIC    - スリープと休止状態の違いは何ですか？
# MAGIC    - OSのアップデートは必ず行う必要がありますか？
# MAGIC    - タッチペンを紛失した場合、どのように対応すればよいですか？
# MAGIC    - 画面やペン先のメンテナンス方法を教えてください。
# MAGIC    - 使用上の注意点を教えてください。
# MAGIC    - バッテリー診断ツールの使い方を教えてください。
# MAGIC    - ソフトウェア的な問題とハードウェア的な問題の違いは何ですか？
# MAGIC    - ファームウェアの更新方法を教えてください。
# MAGIC    - 重要データのバックアップ方法を教えてください。
# MAGIC    - PCを完全シャットダウンする方法を教えてください。
# MAGIC    - ACアダプターの接続確認方法を教えてください。
# MAGIC    - コンセントが通電しているか確認する方法を教えてください。
# MAGIC    - タッチペンの電池交換方法を教えてください。
# MAGIC    - ペン先の摩耗を確認する方法を教えてください。
# MAGIC    - ペン先の交換方法を教えてください。
# MAGIC    - 液晶画面のクリーニング方法を教えてください。
# MAGIC    - 作業スペースの準備方法を教えてください。
# MAGIC    - 静電気対策の方法を教えてください。
# MAGIC    - バッテリー不具合の原因と対策を教えてください。
# MAGIC    - OSやドライバーのアップデート方法を教えてください。
# MAGIC    - 古いファームウェアが原因のバッテリー問題について教えてください。
# MAGIC    - バッテリーの急激な消耗の原因と対策を教えてください。
# MAGIC 3. AIの回答を評価し、「はい」「いいえ」「わかりません」から選択します。できるだけ理由のチェックボックスを選択して評価するようにしましょう。
# MAGIC 3. 必要に応じて「✏️応答を編集」から回答を直接編集します
# MAGIC 4. 必要に応じて追加情報や具体的なフィードバックを提供する
# MAGIC
# MAGIC #### 検索評価
# MAGIC
# MAGIC 1. ソースに表示されている参考情報をクリックします
# MAGIC 2. 検索結果が疑問の解決に役立つか「はい」「いいえ」「わかりません」で評価します。
# MAGIC
# MAGIC #### 評価データの記録
# MAGIC
# MAGIC アセスメント結果はどこに保存されているでしょうか？<br>
# MAGIC 推論テーブルという機能によって、自動でリクエストやレスポンス、またアセスメント結果が記録されています。
# MAGIC ![](/Workspace/Shared/contents_with_answer/data/img/review_tables.png)
# MAGIC
# MAGIC 推論テーブルの詳細は以下の通りです。
# MAGIC
# MAGIC | テーブル | Unity Catalog 上のテーブル名 | 説明 |
# MAGIC |-------|----------------------------------|------------------------|
# MAGIC | Payload | `{catalog_name}.{schema_name}.{model_name}_payload` | 生のJSONリクエストとレスポンス |
# MAGIC | Payload request logs | `{catalog_name}.{schema_name}.{model_name}_payload_request_logs` | フォーマットされたリクエストとレスポンス, MLflow トレース |
# MAGIC | Payload assessment logs | `{catalog_name}.{schema_name}.{model_name}_payload_assessment_logs` | リクエストごとにフォーマットされたReview Appsのアセスメントログ |
# MAGIC
# MAGIC これらのテーブルを分析することによって、エージェントの性能のボトルネックの特定を行い、改善のアイデアを得ます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4. Databricks Apps との連携(標準時間15分)

# COMMAND ----------

# MAGIC %md
# MAGIC ⚠️各班の**代表者1人**がデプロイしたアプリを、**班全員**で使用してみましょう 。⚠️
# MAGIC
# MAGIC Databricks AppsのGUIを使用してchatbot agentをデプロイする手順は以下の通りです:
# MAGIC
# MAGIC 1. Databricksワークスペースにログインします。
# MAGIC
# MAGIC 2. 左側のナビゲーションバーで「クラスター」をクリックし、「アプリ」タブに移動します。
# MAGIC
# MAGIC 3. 「新規アプリの作成」ボタンをクリックします。
# MAGIC
# MAGIC 4. アプリのテンプレートとして「Gradio」「Chatbot」のオプションを選択します。
# MAGIC
# MAGIC 5. アプリの名前を入力します。例えば「chatbot-agent-{x}」などです。xにはチームのアルファベットを入力します。
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
# MAGIC 11. 8で開いたアプリの詳細ページへ戻り、右上の「deploy」から再度デプロイして変更を反映します
# MAGIC 12. また右上の「Permission」から班全員に「Can Use」の権限を渡します。
# MAGIC 13. 実行中と表示されているURLをクリックして、実際にアプリを使用してみましょう！
# MAGIC
# MAGIC ### TIPS
# MAGIC デフォルトの処理では、Databricks SDK の Workspace Client から Model Serving Endpoint へクエリをしていました。 <br>
# MAGIC しかし、`databricks.sdk.service.serving.Servingendpoints`のドキュメントは、パスするパラメータが明示的でなく、混乱されるユーザーが多いです。
# MAGIC したがって、`mlflow.deployments`の Client からクエリをすることを推奨します。 <br>
# MAGIC
# MAGIC [Databricks SDK](https://databricks-sdk-py.readthedocs.io/en/stable/workspace/serving/serving_endpoints.html#databricks.sdk.service.serving.ServingEndpointsExt.query) <br>
# MAGIC [mlflw deployments](https://mlflow.org/docs/latest/python_api/mlflow.deployments.html#mlflow.deployments.DatabricksDeploymentClient.predict) <br>
# MAGIC [mlflow deployments チュートリアル](https://mlflow.org/docs/latest/llms/deployments/guides/step2-query-deployments.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge . 自律的なAgentの作成
# MAGIC
# MAGIC ✅この手順は班の**皆さん**で実行しましょう
# MAGIC
# MAGIC Databricks の AI Playground を使用して、`openhack-gpt-4o` エンドポイントと `manual_retriever`, `get_high_priority_new_cases`ツールを登録し、起票されたケースから特に解決するべきものに対して、ringo computer のマニュアルに参考に回答の素案を作成するエージェントを作成します。手順は以下の通りです:
# MAGIC
# MAGIC 1. Databricks ワークスペースにログインし、左側のナビゲーションから [機械学習] > [Playground] を選択します。
# MAGIC
# MAGIC 2. AI Playground の画面で、左上のドロップダウンメニューから `openhack-gpt-4o` エンドポイントを選択します。
# MAGIC ![](/Workspace/Shared/openhack2025/contents_with_answers/data/img/endpoint_selection.png)
# MAGIC
# MAGIC 3. [ツール] セクションを探し、`manual_retriever`と`get_high_priority_new_cases`ツールを選択します。このツールが表示されない場合は、カスタムツールとして追加する必要があります。
# MAGIC
# MAGIC 4. チャットインターフェースを使用して、エージェントとの対話をテストします。以下のようなサンプル質問を試してみましょう:
# MAGIC    - "あなたは ringo computer のマニュアルに関する質問に答えるエキスパートです。解決するべき優先度の高いケースを`get_high_priority_new_cases`ツールを使用して抽出し、`manual_retriever` ツールを使用して、適切な情報を取得し、ケースの回答の素案を作成してください。"
# MAGIC
# MAGIC 5. エージェントの応答を確認し、必要に応じてシステムプロンプトや設定を調整します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## TIPS. 推論テーブルの分析によるエージェントのモニタリング
# MAGIC 推論テーブルからエージェントの改善ポイントを抽出したり、実際の運用状況をモニタリングできます！<br>
# MAGIC [エージェント評価を行うサンプルコード](https://docs.databricks.com/_extras/notebooks/source/generative-ai/agent-evaluation-online-monitoring-notebook.html)を実行して、エージェント運用、アセスメント結果のモニタリングを行うダッシュボードを作成します。<br>
# MAGIC ### ダッシュボードの構成
# MAGIC - エンドポイント使用状況のモニタリング
# MAGIC - アセスメント結果（生成、検索）
# MAGIC - LLM as a Judge によるレスポンス評価
# MAGIC
# MAGIC ![](https://learn.microsoft.com/ja-jp/azure/databricks/_static/images/generative-ai/online-monitoring-dashboard.gif)
