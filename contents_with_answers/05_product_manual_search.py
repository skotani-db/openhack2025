# Databricks notebook source
# MAGIC %md
# MAGIC # プロダクトマニュアルをVector Search Indexで検索可能な状態にする(標準時間:40分)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 本ノートブックの目的：Databricks における Vector Search のテキスト検索と Index へのガバナンスを理解する
# MAGIC Q1. プロダクトマニュアルの前処理<br>
# MAGIC Q2. Vector Search Index の作成<br>
# MAGIC Q3. Vector Search のツール化とアクセス制御

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前準備 (標準時間：10分)

# COMMAND ----------

# MAGIC %pip install -U bs4 langchain_core langchain_text_splitters databricks-vectorsearch pydantic transformers unitycatalog-ai[databricks] unitycatalog-langchain[databricks] databricks-langchain

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# 本ノートブックで利用するスキーマを作成
schema_name = f"05_vector_search_index_for_{user_name}"
print(f"schema_name: `{schema_name}`")
spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
    """
)
spark.sql(f"USE {catalog_name}.{schema_name}")

# 本ノートブックで利用するボリュームを作成
volume_name = f"product_manual"
print(f"volume_name: `{volume_name}`")
spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ソースデータファイルのダウンロードとボリュームへのアップロード
# MAGIC 1. 現在のノートブックの左型タブにある`Workspace (Ctrl + Alt + E)`を選択し、現在のディレクトリにある`data`フォルダに移動
# MAGIC 1. `product_manual`フォルダのハンバーガーメニュー（`︙`）を選択して、 `ダウンロード形式：` -> `ソースファイル` をクリックしてデータファイルをダウンロード
# MAGIC 1. ダウンロードした Zip ファイルを解凍
# MAGIC 1. 現在のノートブックの左型タブにある`Catalog (Ctrl + Alt + C)`を選択後、`05_vector_search_index_for_{username}`スキーマ下にある`product_manual` Volume にてハンバーガーメニュー（`︙`）を選択し、`このボリュームにアップロード`を選択
# MAGIC 1. 表示されたウィンドウに解凍した html ファイルをすべて配置して、`アップロード`を選択
# MAGIC 1. 下記のセルを実行し、ファイルが配置されていることを確認

# COMMAND ----------

# ファイルが配置されていることを確認
src_file_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
file_list = dbutils.fs.ls(src_file_dir)
if file_list == []:
    raise Exception("ファイルがディレクトリにありません。ファイルを配置してください。")
display(dbutils.fs.ls(src_file_dir))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. プロダクトマニュアルの前処理(標準時間：10分)
# MAGIC ダウンロード後に解凍した`product_manual`フォルダから、`index.html`を開いてみましょう。 \
# MAGIC マニュアルはHTMLで記述され、FAQやトラブルシューティングといったセクションに分かれています。 
# MAGIC
# MAGIC 起票されたサポートチケットに対してマニュアルに基づいて適切な回答を作成するためには、マニュアルの記載をLLMに入力して適切な回答を考えてもらう必要があります。 \
# MAGIC しかし、膨大なマニュアルの全てをLLMへ入力することはモデルでは通常入力可能なトークンの数が決まっているため不可能であり、コストや性能の観点からも望ましくありません。
# MAGIC
# MAGIC このため、マニュアルを小さな塊に分割し、クエリと関連しそうな記述の塊（一般的にチャンクと呼ばれます）を抽出して、LLMへ与える文脈情報とします。今回はHTMLのタグによってHTMLファイルを分割します。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 実践例(langchainの公式ドキュメントから引用)
# MAGIC HTMLの構造は以下のようにタグとその中のコンテンツを区別して簡潔に説明できます：
# MAGIC
# MAGIC 1. h1タグ: 最も重要な大見出し
# MAGIC    - ページの主題やタイトルを表す
# MAGIC    - 通常、1ページに1つのみ使用
# MAGIC
# MAGIC 2. h2タグ: 中見出し
# MAGIC    - 主要なセクションや章を表す
# MAGIC    - h1の下位階層として複数使用可能
# MAGIC
# MAGIC 3. h3タグ: 小見出し
# MAGIC    - h2の下位階層として使用
# MAGIC    - セクション内の重要なポイントを表す
# MAGIC
# MAGIC 4. pタグ: 段落
# MAGIC    - 本文のテキストを含む
# MAGIC    - 見出しの下に配置され、関連するコンテンツを表示
# MAGIC
# MAGIC 今回はBeautifulSoupと呼ばれるHTMLの解析を行うPythonパッケージを用いて、マニュアルの各ファイルを分割し、テーブル形式で保存します。

# COMMAND ----------

html_string = """
<!DOCTYPE html>
<html lang='ja'>
<head>
  <meta charset='UTF-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1.0'>
  <title>おしゃれなHTMLページの例</title>
</head>
<body>
  <h1>メインタイトル</h1>
  <p>これは基本的な内容を含む導入段落です。</p>
  
  <h2>セクション1：はじめに</h2>
  <p>このセクションではトピックを紹介します。以下はリストです：</p>
  <ul>
    <li>最初の項目</li>
    <li>2番目の項目</li>
    <li><strong>太字のテキスト</strong>と<a href='#'>リンク</a>を含む3番目の項目</li>
  </ul>
  
  <h3>サブセクション1.1：詳細</h3>
  <p>このサブセクションでは追加の詳細を提供します。以下は表です：</p>
  <table border='1'>
    <thead>
      <tr>
        <th>ヘッダー1</th>
        <th>ヘッダー2</th>
        <th>ヘッダー3</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>1行目、1列目</td>
        <td>1行目、2列目</td>
        <td>1行目、3列目</td>
      </tr>
      <tr>
        <td>2行目、1列目</td>
        <td>2行目、2列目</td>
        <td>2行目、3列目</td>
      </tr>
    </tbody>
  </table>

  <h2>結論</h2>
  <p>これは文書の結論です。</p>
</body>
</html>


"""

# COMMAND ----------

# 処理の詳細についてはここでは深く触れません

from langchain.text_splitter import RecursiveCharacterTextSplitter
from bs4 import BeautifulSoup
from transformers import OpenAIGPTTokenizer

max_chunk_size = 500
tokenizer = OpenAIGPTTokenizer.from_pretrained("openai-gpt")
text_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(tokenizer, chunk_size=max_chunk_size, chunk_overlap=50)

def split_html_with_h3_p(html, min_chunk_size=0, max_chunk_size=500):
    """
    HTMLコンテンツをh2, h3, pタグに基づいてチャンクに分割する関数。
    分割後、チャンクがmin_chunk_size以上のトークン数であるものだけを返す。

    Args:
        html (str): 分割対象のHTML文字列
        min_chunk_size (int): 分割後のチャンクが保持すべき最小トークン数
        max_chunk_size (int): 各チャンクの最大トークン数

    Returns:
        list[str]: 分割されたテキストチャンクのリスト
    """

    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    chunks = []
    current_h2 = None
    current_h3 = None
    current_chunk = ""

    for tag in soup.find_all(["h2", "h3", "p"]):
        text = tag.get_text(strip=True)
        if not text:
            continue

        if tag.name == "h2":
            # 新しいh2タグが現れた場合、既存のチャンクを分割してリストに追加。
            if current_chunk:
                chunks.extend(text_splitter.split_text(current_chunk.strip()))
            current_h2 = text
            current_h3 = None  # h3をリセット
            current_chunk = f"{current_h2}\n"
            
        elif tag.name == "p":
            # pは現在のh2またはh3に追加
            if len(tokenizer.encode(current_chunk + text)) <= max_chunk_size:
                current_chunk += text + "\n"
            else:
                chunks.extend(text_splitter.split_text(current_chunk.strip()))
                current_chunk = text + "\n"

    if current_chunk:
        chunks.extend(text_splitter.split_text(current_chunk.strip()))

    return [c for c in chunks if len(tokenizer.encode(c)) > min_chunk_size]

# COMMAND ----------

# MAGIC %md
# MAGIC ### ToDo: HTMLファイルのレコードをチャンクへ分割し、`product_documentation`テーブルへ格納

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Databricks の Vector Search Index は ソーステーブルとインデックス間を Delta Live Tables が変更を同期します
# MAGIC -- そのためソーステーブルで Change Data Feed を有効にする必要があり、インデックス作成前に設定します
# MAGIC -- Hint: https://learn.microsoft.com/ja-jp/azure/databricks/delta/delta-change-data-feed#enable
# MAGIC
# MAGIC -- Answer:
# MAGIC CREATE TABLE IF NOT EXISTS product_documentation (
# MAGIC   id BIGINT GENERATED BY DEFAULT AS IDENTITY,
# MAGIC   filename STRING,
# MAGIC   content STRING
# MAGIC ) TBLPROPERTIES (delta.enableChangeDataFeed = true); 
# MAGIC
# MAGIC -- ToDo:
# MAGIC -- CREATE TABLE IF NOT EXISTS product_documentation (
# MAGIC --   id BIGINT GENERATED BY DEFAULT AS IDENTITY,
# MAGIC --   filename STRING,
# MAGIC --   content STRING
# MAGIC -- ) TBLPROPERTIES --(ToDo); 

# COMMAND ----------

# HTMLファイルを読み込む
# ファイルパスを指定します
import pyspark.sql.functions as F

# Asnwer:
html_file = (
    spark
    .read
    .option("wholetext", True)
    .text(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/*.html")
    .withColumn("filename", F.col("_metadata.file_path"))
    .withColumn("filename", F.regexp_replace(F.col("filename"), r'^.*/', ''))
    )

# ToDo:
# html_file = (
#     spark
#     .read
#     .option("wholetext", True)
#     .text(<ToDo: パス指定>)
#     .withColumn("filename", F.col("_metadata.file_path"))
#     .withColumn("filename", F.regexp_replace(F.col("filename"), r'^.*/', ''))
#     )

display(html_file)

# COMMAND ----------

import pandas as pd

# HTMLをh2, h3, pタグに基づいてチャンクに分割する処理をPandas UDFによってspark dataframeで実行
# Pandas UDFとは？　https://qiita.com/ktmrmshk/items/24aa8467b906c56c332f
@F.pandas_udf("array<string>")
def parse_and_split(docs: pd.Series) -> pd.Series:
    return docs.apply(split_html_with_h3_p)

# COMMAND ----------

# HTMLをh2, h3, pタグに基づいてチャンクに分割する処理をコールバック関数としてexplode()で展開
# ToDo: 書き込み時には"filename"列と"content"列を残す
# Hint: https://qiita.com/taka4sato/items/4ab2cf9e941599f1c0ca#filter-select%E3%81%A7%E6%9D%A1%E4%BB%B6%E4%BB%98%E3%81%8D%E6%A4%9C%E7%B4%A2

# Asnwer:
(
    html_file
    .withColumn("content", F.explode(parse_and_split("value")))
    .select("filename", "content")
    .write.mode('overwrite').saveAsTable("product_documentation")
)

# # ToDO:
# (
#     html_file
#     .withColumn("content", F.explode(parse_and_split("value")))
#     .<ToDo>
#     .write.mode('overwrite').saveAsTable("product_documentation")
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM product_documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. Vector Search Index の作成(標準時間：10分)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ベクトル検索とは
# MAGIC ![ベクトル検索とは](/Workspace/Shared/openhack2025/contents_with_answers/data/img/vector_index.png)
# MAGIC
# MAGIC この画像は、ベクトル検索の概念をわかりやすく表現しています。
# MAGIC
# MAGIC *   **ベクトル表現（赤い点）:** 各赤い点は、特定のデータオブジェクト（例えば、画像、テキストドキュメント）のベクトル表現を表しています。ディープラーニングモデルなどの手法を用いて、データオブジェクトはその意味や内容に基づいて数値ベクトルに変換されます。
# MAGIC *   **クラスタリング:** 同様のデータオブジェクトは、多次元空間内で互いに近い位置に集まり、クラスタを形成します。画像の例では、「Dogs」と「Cat」のアイコンで示されたクラスタが、犬と猫の画像に対応しています。
# MAGIC *   **クエリベクトル（青い点）:** 「find me all cats」というテキストのクエリは、同様にベクトルに変換され、多次元空間内の青い点で表されます。
# MAGIC *   **類似度検索:** ベクトル検索では、クエリベクトルに最も近いデータベクトルを見つけます。画像では、青い点から猫のクラスタ内の赤い点への線が、この類似度を示しています。
# MAGIC
# MAGIC Databricks Vector Searchにおいて、インデックスとエンドポイントは以下のように異なる役割を持っています:
# MAGIC
# MAGIC
# MAGIC ### Index (インデックス)
# MAGIC
# MAGIC - Vector Searchインデックスは、データのベクトル表現の集合です。
# MAGIC - 効率的な近似最近傍検索クエリをサポートします。
# MAGIC - ソースのDeltaテーブルの特定のカラムから作成されます。
# MAGIC - Unity Catalog内でテーブルのようなオブジェクトとして表示されます。
# MAGIC
# MAGIC ### Endpoint (エンドポイント)
# MAGIC
# MAGIC - Vector Searchエンドポイントは、インデックスが存在するエンティティです。
# MAGIC - 検索リクエストを処理するためのエントリーポイントとして機能します。
# MAGIC - 複数のインデックスを含むことができます。
# MAGIC - インデックスのサイズや並列リクエストの数に応じて自動的にスケールします。
# MAGIC - REST APIやSDKを使用してクエリや更新に利用できます。
# MAGIC
# MAGIC
# MAGIC Vector Searchを使用する際は、まずエンドポイントを作成し、その後そのエンドポイント内に1つ以上のインデックスを作成します。エンドポイントはインデックスを管理し、クエリ処理のためのアクセスポイントとして機能します。
# MAGIC
# MAGIC インデックスに格納されているデータのベクトル表現とエンドポイントは以下のように確認することができます。

# COMMAND ----------

import mlflow.deployments
# External Model Serving に登録されている埋め込みのエンドポイントを使って、埋め込みを生成する
# Hint: https://mlflow.org/docs/latest/llms/deployments/guides/step2-query-deployments/#example-3-embeddings

# Databricks のデプロイメントクライアントを取得
deploy_client = mlflow.deployments.get_deploy_client("databricks")

# OpenAI の text-embedding-ada-002 を使用（エンベディング生成）を実行
# エンドポイント "openhack-text-embedding-ada-002" に対して、"What is Apache Spark?" という入力テキストを渡す
response = deploy_client.predict(
    endpoint="openhack-text-embedding-ada-002",
    inputs={"input": ["Apache Spark とはなんでしょう?"]}
)

# 結果を取得
embeddings = [e["embedding"] for e in response.data]
print(embeddings)

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
# 使用可能なエンドポイントの一覧を取得する
# https://api-docs.databricks.com/python/vector-search/databricks.vector_search.html
vsc = VectorSearchClient()
VECTOR_SEARCH_ENDPOINT_NAME = "vs_endpoint"

# Vector Searchエンドポイントの作成には10分～15分ほどかかります。 本日は時間がないので、作成済みのエンドポイントを利用します。
# vsc.create_endpoint_and_wait("openhack-text-embedding-ada-002", VECOTR_SEARCH_ENDPOINT_NAME)

# 作成済みのエンドポイント
# Answer:
vsc.list_endpoints()

# ToDo:
# <作成済みのendpoint一覧を得る>

# COMMAND ----------

# MAGIC %md
# MAGIC ### ToDo: `product_documentation_vs`インデックスを作成する
# MAGIC ⚠️この手順は各班の**代表者1人**が行ってください。⚠️
# MAGIC
# MAGIC Databricks Vector SearchでUnity CatalogのUIを使用して、`product_documentation`テーブルをソースとして、`product_documentation_vs`という名前のVector Indexを作成する手順は以下の通りです。
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/index_creation.gif?raw=true" width="600px" style="float: right">
# MAGIC
# MAGIC
# MAGIC **手順:**
# MAGIC
# MAGIC 1.  **Unity Catalogへのアクセス:**
# MAGIC
# MAGIC     *   Databricksワークスペースにログインします。
# MAGIC     *   サイドバーの「カタログ」をクリックし、Unity Catalogを開きます。
# MAGIC
# MAGIC 2.  **テーブルの選択:**
# MAGIC
# MAGIC     *   必要なカタログとスキーマを選択して、`product_documentation`テーブルを見つけます。
# MAGIC     *   `product_documentation`テーブルをクリックして、詳細を表示します。
# MAGIC
# MAGIC 3.  **Vector Indexの作成:**
# MAGIC
# MAGIC     *   画面右上の「作成」タブで、「ベクトル検索インデックス」タブを選択します。(もしこのタブが表示されていない場合、DatabricksアカウントでVector Searchが有効になっていることを確認してください。)
# MAGIC     *   「Create Vector Index」ボタンをクリックします。
# MAGIC
# MAGIC 4.  **Vector Indexの設定:**
# MAGIC
# MAGIC     *   **名前:**  `product_documentation_vs`と入力します。
# MAGIC     *   **プライマリーキー (オプション):** 主キーとして使用する列を選択します。`id`と入力します。
# MAGIC     *   **エンドポイント:** リアルタイム同期またはバッチ同期を選択します。`vs_endpoint`を選択します。
# MAGIC     *   **同期する列:** インデックスに同期する列を選択します。`filename`と`content`を選択します。
# MAGIC     *   **ソース埋め込み:** 今回は文字列をインプットとして埋め込み関数を使用して埋め込み列を作成するため`コンピュート埋め込み`を選択します。`既存の埋め込み列を使用`を選択すると、ユーザー側で埋め込みを計算した列を同期します。
# MAGIC     *   **埋め込みソース列:** 埋め込み`content`を指定します。
# MAGIC     *   **埋め込みモデル:** 使用する埋め込み関数を選択します。Databricksが提供する関数またはカスタムUDFを使用できます。Azure OpenAI Serviceを登録したサービングエンドポイントの`openhack-text-embedding-ada-002`を選択します。
# MAGIC     *   **同期モード:** 今回のチャレンジでは`トリガー`を選択します。リアルタイムでの同期が必要な場合は`連続`を選択ください。
# MAGIC
# MAGIC 5.  **Vector Indexのデプロイ:**
# MAGIC
# MAGIC     *   設定を確認し、「作成」ボタンをクリックします。
# MAGIC     *   Vector Indexの作成プロセスが開始されます。ステータスはUIで監視できます。
# MAGIC
# MAGIC 6. **権限の付与:**
# MAGIC     * インデックス作成完了後、作成したインデックスを他の方が検索できるようにするため、カタログで`product_documentation_vs`の詳細を開きます
# MAGIC     * `権限`タブから`SELECT`権限を班全員に付与します
# MAGIC
# MAGIC ✅以降のノートブックのセルは班全員で実行することができるようになっています

# COMMAND ----------

# インデックスの作成を確認
VECTOR_SEARCH_INDEX_NAME = "product_documentation_vs"
vs_schema_name = schema_name
vs_index_fullname = f"{catalog_name}.{vs_schema_name}.{VECTOR_SEARCH_INDEX_NAME}"

index = vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)
index.describe()

# COMMAND ----------

# 類似コンテンツの検索
question = "バッテリー交換"

# question に関連する文書を検索する
# https://learn.microsoft.com/ja-jp/azure/databricks/generative-ai/create-query-vector-search#pythonsdk-2
# Answer:
results = index.similarity_search(
  query_text=question,
  columns=["filename", "content"],
  num_results=3)

# ToDo:
# results = index.<ToDo>

docs = results.get('result', {}).get('data_array', [])
docs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Vector Search のツール化とアクセス制御(標準時間：10分)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 実践例
# MAGIC Databricks Unity Catalog ではよく使うクエリを関数としてスキーマに登録することができます。
# MAGIC スキーマに登録された関数は AI Agent の部品(`UCFunctionToolkit`) として簡単に呼び出すことができます。
# MAGIC
# MAGIC 以下の例では、 Ringo Computer のサポート部門に寄せられたケースのうち、緊急に処理すべき新規ケースを抽出する関数を Unity Catalog へ登録しています。
# MAGIC
# MAGIC 登録が完了したら実際にカタログで確認してみましょう！
# MAGIC

# COMMAND ----------

create_func = f"""
CREATE OR REPLACE FUNCTION {catalog_name}.{schema_name}.get_high_priority_new_cases()
RETURNS TABLE (
  CaseNumber STRING,
  SuppliedName STRING,
  SuppliedEmail STRING,
  SuppliedPhone STRING,
  SuppliedCompany STRING,
  Type STRING,
  Status STRING,
  Subject STRING,
  Description STRING
)
COMMENT 'この関数は、優先順位の高い新規ケースをケース・テーブルから検索する。'
RETURN
  SELECT CaseNumber, SuppliedName, SuppliedEmail, SuppliedPhone, SuppliedCompany, Type, Status, Subject, Description 
  FROM {catalog_name}.03_data_analysis_by_gen_ai_for_{user_name}.case
  WHERE Status = "新規" AND Priority = "高" AND Type = "問い合わせ"
  ORDER BY CaseNumber DESC
  LIMIT 1;
"""

spark.sql(create_func)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 登録した関数を呼び出します
# MAGIC SELECT * FROM get_high_priority_new_cases();

# COMMAND ----------

from unitycatalog.ai.core.databricks import DatabricksFunctionClient
import pandas as pd
import io

# ケースを取得する関数のフルパス
case_func_fullname = f"{catalog_name}.{schema_name}.get_high_priority_new_cases"

# DatabricksFunctionClientから関数呼び出し
client = DatabricksFunctionClient()
result = client.execute_function(case_func_fullname)

# 検索結果のPandas DataFrameによるパース
df = pd.read_csv(io.StringIO(result.value))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ToDo: Vector Search を行う関数`manual_retriever`をカタログへ登録する
# MAGIC Hint: [ドキュメント](https://docs.databricks.com/aws/ja/generative-ai/agent-framework/unstructured-retrieval-tools#unity-catalog%E6%A9%9F%E8%83%BD%E3%82%92%E6%8C%81%E3%81%A4%E3%83%99%E3%82%AF%E3%83%88%E3%83%AB%E6%A4%9C%E7%B4%A2%E3%83%AC%E3%83%88%E3%83%AA%E3%83%BC%E3%83%90%E3%83%BC%E3%83%84%E3%83%BC%E3%83%AB)  を参考にカタログへベクトル検索関数を登録しましょう
# MAGIC
# MAGIC ベクトル検索関数のカタログへの登録が完了したら、権限をチーム内のメンバーに割り当ててみましょう

# COMMAND ----------

# Answer:
create_retriever = f"""
CREATE OR REPLACE FUNCTION {catalog_name}.{schema_name}.manual_retriever (
  query STRING
  COMMENT 'The query string for searching our product documentation.'
) RETURNS TABLE
COMMENT 'Executes a search on product documentation to retrieve text documents most relevant to the input query.' RETURN
SELECT
  content as page_content,
  map('doc_uri', filename, 'chunk_id', CAST(id AS STRING)) as metadata
FROM
  vector_search(
    index => '{vs_index_fullname}',
    query => query,
    num_results => 3
  )
"""

# ToDo:
# create_retriever = f"""
# CREATE OR REPLACE FUNCTION {catalog_name}.{schema_name}.manual_retriever (
#   query STRING
#   COMMENT 'The query string for searching our product documentation.'
# ) RETURNS TABLE
# COMMENT 'Executes a search on product documentation to retrieve text documents most relevant to the input query.' RETURN
# SELECT
#   content as page_content,
#   map('doc_uri', filename, 'chunk_id', CAST(id AS STRING)) as metadata
# FROM
#   <ToDo>
# """

spark.sql(create_retriever)

# COMMAND ----------

import mlflow
from databricks_langchain import VectorSearchRetrieverTool

# LangChain/LangGraphに組み込むことを想定して、databricks vector search をツールとしてコールできるようにする
# Hint: https://learn.microsoft.com/ja-jp/azure/databricks/generative-ai/agent-framework/unstructured-retrieval-tools#langchainlanggraph

# Answer:
vs_tool = VectorSearchRetrieverTool(
  index_name=vs_index_fullname,
  tool_name="manual_retriever",
  tool_description="Ringo Computerの製品マニュアルを検索するツールです",
  columns=["id", "filename"]
)

# ToDo:
# vs_tool = <ToDo>

vs_tool.invoke("バッテリー交換")
