# Databricks notebook source
# MAGIC %md
# MAGIC # Openhack 202503 コンテンツ検討

# COMMAND ----------

# MAGIC %md
# MAGIC | 開始 | 終了 | 時間 | 内容 | Owner | 形式 |
# MAGIC | ---- | ---- | ---- | ---- | ---- | ---- |
# MAGIC | 10:00 | 10:50 | 50分 | オープニング | MS 小田さん、武田さん | 全体
# MAGIC | 10:50 | 11:50 | 60分 | **データエンジニアリングチャレンジ(Pipeline)** | | 各チーム |  
# MAGIC | 11:50 | 12:50 | 60分 | ランチ休憩 | 
# MAGIC | 12:50 | 13:20 | 30分 | ヒントセッション (Genie / AI/BI Dashboard機能説明 活用例ご紹介) | Databricks 小谷さん NSSOL?| 全体 | 
# MAGIC | 13:20 | 14:20 | 60分 | **生成AIチャレンジ(Genie)** |  | 各チーム | 
# MAGIC | 14:20 | 14:40 | 20分 | 休憩 |  |  | 
# MAGIC | 14:40 | 15:00 | 20分 | ヒントセッション (RAG / Agent AI) | Databricks 小谷さん NSSOL | 全体 | 
# MAGIC | 15:00 | 17:00 | 120分 | **RAG Agent AIチャレンジ(休憩は各チームで)** |  | 各チーム | 
# MAGIC | 17:00 | 17:20 | 20分 | AI Agent発表会 ⇒ 発表会に向けたモチベーションになるが、チーム毎の色がでるとよい。 |  | 数チーム | 
# MAGIC | 17:20 | 17:50 | 30分 | クロージング NSSOL/マイクロソフト支援サービス紹介 |  | 全体 | 
# MAGIC | 17:50 | 17:55 | 5分 | QA・アンケート |  | 全体 | 
# MAGIC | 17:55 | 18:00 | 5分 | 記念撮影 |  | 全体 | 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 01_medallion_architectureとDataengineering 60min
# MAGIC メダリオンアーキテクチャの概要とSQLを用いたデータエンジニアリング  
# MAGIC - 研修環境のセットアップ
# MAGIC - 本日利用するデータの説明(SFAデータ)
# MAGIC - Brone層
# MAGIC   - SQLを使ったデータの取り込み実装例
# MAGIC   - ファイルに対するクエリ
# MAGIC   - フェデレーションクエリ
# MAGIC - Silver層
# MAGIC - Glod層
# MAGIC - オプション(Auto Loader)
# MAGIC - オプション(DLT)
# MAGIC ## 02_data_analysis_by_gen_ai(Genie) 60min
# MAGIC AI/BI Genie Space にて生成 AI によるデータ分析の実践  
# MAGIC 目的：AI/BI Genie Space を用いた自然言語によるデータ分析の方法論を理解する
# MAGIC - 事前準備 (標準時間：10分)
# MAGIC - Q1. Genie スペース を作成 (標準時間：20分)
# MAGIC - Q2. General Instructions 修正による Genie スペース の改善 (標準時間：10分)
# MAGIC - Q3. Example SQL Queries 追加による Genie スペース の改善 (標準時間：10分)
# MAGIC - Q4. Trusted Assets 追加による Genie スペース の改善 (標準時間：10分)
# MAGIC - Challenge1. Genie スペースの最適な利用方法を検討してください。
# MAGIC ## 03_rag_agent_ai 120min
# MAGIC LLM チャットボットを Mosaic AI エージェント評価と Lakehouse アプリケーションで展開する。  
# MAGIC 目的：RAG(Retrieval Augmented Generation)、AOAI text-embedding-large3 を使用して、ユーザが○○に関する質問に答えられるように、独自のチャットボットアシスタント(AI Agent)を構築する方法を学ぶ。  
# MAGIC AIアプリのモデルは AOAI GPT-4o を利用。
# MAGIC
# MAGIC 1. データ準備とベクトルストア(Index)作成
# MAGIC 1. RAGエージェントのエンドポイント作成
# MAGIC 1. Lakehouse APPのデプロイ
# MAGIC
# MAGIC 検討：  
# MAGIC - Agent AIの部分はNotebookに丁寧な解説が必要(Shift Enter連打になりそう。。)
# MAGIC - Vector Searchのエンドポイント作成、インデックスの作成はユーザ毎に行う?(時間がかかる、リソース消費)
# MAGIC - コンテンツ：  
# MAGIC   - AI Agentの仕事
# MAGIC     - 担当営業が自身のフォーキャストを予測
# MAGIC     - 案件をクローズするためのステップ、アクションをAI Agentに相談
# MAGIC     - todoリストを作成する?
# MAGIC   - 追加するデータ
# MAGIC     - 商談データ、QAデータなどの架空の非構造化データを生成
# MAGIC     - ※ 構造化データに問い合わせのテーブルがあるので、問い合わせ対応系でも良いかもしれない。
# MAGIC   
# MAGIC - 優先的に対応するトラブルは?
# MAGIC   - エージェント部分のアイデアなのですが  
# MAGIC リングコンピューターにおける修理メンテナンスの検索エンジンとかどうでしょう  
# MAGIC 例えば、クエリは今フォローすべきケースをどのように解決すべきでしょうか？  
# MAGIC **緊急度や起票日が古いものを構造化データ**から検索してきて、(Function)   
# MAGIC **ケースの内容や型番から修理マニュアルを検索**してきて、 (RAG)
# MAGIC 修理手順を生成する  

# COMMAND ----------

# MAGIC %md
# MAGIC ### 参考:前回スケジュール
# MAGIC ![schedule.png](./schedule.png "schedule.png")

# COMMAND ----------

# MAGIC %md
# MAGIC # End
