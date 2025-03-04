# Databricks notebook source
# MAGIC %md
# MAGIC # AI/BI Genie Space にて生成 AI によるデータ分析の実績 (標準時間：60分)__

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 本ノートブックの目的：AI/BI Genie Space を用いた自然言語によるデータ分析の方法論を理解する
# MAGIC
# MAGIC Q1. Genie スペース を作成<br>
# MAGIC Q2. General Instructions 修正による Genie スペース の改善<br>
# MAGIC Q3. Example SQL Queries 追加による Genie スペース の改善 <br>
# MAGIC Q4. Trusted Assets 追加による Genie スペース の改善

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前準備 (標準時間：10分)

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# 本ノートブックで利用するスキーマを作成
schema_name = f"03_data_analysis_by_gen_ai_for_{user_name}"
print(f"schema_name: `{schema_name}`")
spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
    """
)

# COMMAND ----------

# 本ノートブックで利用するテーブルの作成とデータの挿入（5 分程度で完了）
init_notebooks = [
    "./includes/03_data_analysis_by_gen_ai/01_create_tables",
    "./includes/03_data_analysis_by_gen_ai/02_add_constraint",
    "./includes/03_data_analysis_by_gen_ai/03_write_data",
]
notebook_parameters = {
    "catalog_name": catalog_name,
    "schema_name": schema_name,
}
for init_n in init_notebooks:
    dbutils.notebook.run(
        init_n,
        0,
        notebook_parameters,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. Genie スペース を作成 (標準時間：20分)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. 現在のノートブックの左型タブにある`Workspace (Ctrl + Alt + E)`を選択し、現在のディレクトリ（`contents`)を表示
# MAGIC 1. ケバブメニュー（`︙`）を選択し、`作成` -> `Genieスペース`を選択 *1
# MAGIC 1. Genieスペースの作成画面にて下記セルの出力結果を設定して`Save`を選択
# MAGIC 1. チャットウィンドウにて、`データセットに含まれるテーブルについて説明して`という質問の回答が来ることを確認
# MAGIC
# MAGIC *1 Genie スペースを作成できない場合には、下記の手順を実施して Genie スペースの有効化が必要です
# MAGIC
# MAGIC 1. Databricks Workspace にて右上のユーザーアイコンを選択後、`Previews`を選択
# MAGIC 1. `Genie`の有効化に関するトグルを`On`に設定
# MAGIC 1. ページをリロード後、Databricks Workspace の左メニューにて Genie が追加されたことを確認 
# MAGIC
# MAGIC 参考リンク
# MAGIC
# MAGIC - [AI/BI Genie スペースとは](https://learn.microsoft.com/ja-jp/azure/databricks/genie/)
# MAGIC - [Use trusted assets in AI/BI Genie spaces](https://learn.microsoft.com/ja-jp/azure/databricks/genie/trusted-assets)
# MAGIC - [効果的な Genie スペースをキュレーションする](https://learn.microsoft.com/ja-jp/azure/databricks/genie/best-practices)

# COMMAND ----------

print("-- Title")
print(f"SFA Analysis by {user_name}")
print("")

print("-- Description")
print("""
基本的なふるまい：
- 日本語で回答して

データセットについて:
- Ringo Computer Company という法人向け PC、タブレット、スマートフォンを販売している会社の Sales Force Automation に関するデータセット
""")
print("")

print("-- Default warehouse")
print("Serverless Starter Warehouse")
print("")

print("-- Tables")
table_list_df = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}")

with_cols_conf = {
    "Catalog": lit(catalog_name),
    "Scheam": lit(schema_name), 
    "Table": col("tableName"),
}
table_list_df = table_list_df.withColumns(with_cols_conf)
table_list_df = table_list_df.select(*with_cols_conf.keys())
table_list_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. General Instructions 修正による Genie スペース の改善 (標準時間：10分)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. 左型にある`Instructions`タブを選択
# MAGIC 2. `General Instructions`を下記のように書き換えて`Save`を選択
# MAGIC 3. 左型にある`Chats`タブを選択してチャットウィンドウに戻る
# MAGIC 4. `+ New chat`を選択して`データセットに含まれるテーブルについて説明して`という質問の回答が来ることを確認
# MAGIC 5. チャット履歴から前回との出力結果を比較して回答が改善されることを確認
# MAGIC

# COMMAND ----------

print("-- Description")
print("""
基本的な動作：
- 日本語で回答して

データセットについて:
- Ringo Computer Company という法人向け PC、タブレット、スマートフォンを販売している会社の Sales Force Automation に関するデータセット
- lead -> opportunity -> order という順に営業活動が進みます

テーブル名の概要:

| テーブル        | 日本語テーブル名 | 概要                                                         |
| --------------- | ---------------- | ------------------------------------------------------------ |
| lead            | リード           | 潜在顧客の情報を管理するためのオブジェクト。                 |
| opportunity     | 商談             | 商談や販売機会の情報を管理するためのオブジェクト。           |
| order           | 注文             | 顧客からの注文情報を管理するためのオブジェクト。             |
| case            | ケース           | 顧客からの問い合わせやサポートリクエストの情報を管理するためのオブジェクト。 |
| account         | 取引先           | 取引先情報を管理するためのオブジェクト。顧客やパートナー企業などの情報を保持。 |
| contact         | 取引先責任者     | 取引先に関連する担当者情報を管理するためのオブジェクト。     |
| campaign        | キャンペーン     | マーケティングキャンペーンの情報を管理するためのオブジェクト。 |
| product         | 製品             | 販売する製品やサービスの情報を管理するためのオブジェクト。   |
| pricebook_entry | 価格表エントリ   | 製品の価格情報を管理するためのオブジェクト。                 |
| user            | ユーザー         | ユーザー情報（営業担当者）を管理するためのオブジェクト。     |

""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Example SQL Queries 追加による Genie スペース の改善 (標準時間：10分)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. `Show me the sales amount by order date.`という質問。適切な回答がこないことを確認。
# MAGIC 1. 左型にある`Instructions`タブを選択
# MAGIC 1. `Example SQL Queries`における`+ Add example query`を選択
# MAGIC 1. `What question does this query answer?`とクエリを各領域に下記セルの出力結果を張り付けて、`Save`を選択
# MAGIC 1. `+ New chat`を選択して`Show me the sales amount by order date.`という質問の回答が来ることを確認。もし想定通りに回答がこない場合には、`Show me the sales amount by order date based on Example SQL Queries.`という質問を実施。

# COMMAND ----------

sql = f"""
SELECT
  CAST(ord.ActivatedDate AS DATE) AS order_date -- 注文日
  ,SUM(opp.TotalOpportunityQuantity * pbe.UnitPrice) AS total_ammount -- 受注金額

FROM
  {catalog_name}.{schema_name}.`order` ord

INNER JOIN {catalog_name}.{schema_name}.opportunity opp
ON 
  ord.OpportunityId__c = opp.Id

INNER JOIN {catalog_name}.{schema_name}.product2 prd
ON 
  opp.Product2Id__c = prd.Id

INNER JOIN {catalog_name}.{schema_name}.pricebook_entry pbe
ON 
  prd.Id = pbe.Product2Id

WHERE
  StatusCode = 'Completed'
GROUP BY ALL
""".strip()
print("-- What question does this query answer?")
print("Show me the sales amount by order date.")
print("")
print("-- Query")
print(sql)

df = spark.sql(sql)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4. Trusted Assets 追加による Genie スペース の改善 (標準時間：10分)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. 左型にある`Instructions`タブを選択
# MAGIC 1. `SQL Functions`における`Add SQL Function`を選択
# MAGIC 1. `Catalog`、`Schema`、および、`Function`に下記セルの出力結果を張り付けて、`Save`を選択
# MAGIC 1. `+ New chat`を選択して`What is the pipeline for 東京都 and 大阪府?`という質問の回答が来ることを確認。データが存在しない場合には、データが存在する県（state）に変更。この質問における`pipeline`は、データエンジニアリング（Delta live Tables）のパイプラインではなく、営業活動に関連するパイプラインを指します。
# MAGIC
# MAGIC 参考リンク
# MAGIC
# MAGIC - [Use trusted assets in AI/BI Genie spaces](https://learn.microsoft.com/ja-jp/azure/databricks/genie/trusted-assets)
# MAGIC - [パイプライン管理とは？効果的に運用するための4ステップを解説](https://www.salesforce.com/jp/blog/jp-pipeline-management/)

# COMMAND ----------

function_name = "open_opps_in_states"
sql = f"""
CREATE
OR REPLACE FUNCTION {catalog_name}.{schema_name}.{function_name} (
  states ARRAY < STRING >
  COMMENT 'List of states.  Example: ["東京都", "大阪府", ]' DEFAULT NULL
) RETURNS TABLE
COMMENT 'Addresses questions about the pipeline in the specified states by returning
 a list of all the open opportunities. If no state is specified, returns all open opportunities.
 Example questions: "What is the pipeline for 東京駅 and 大阪府?", "Open opportunities in
 APAC"' RETURN
SELECT
  o.id AS `OppId`,
  a.BillingState AS `State`,
  o.name AS `Opportunity Name`,
  o.forecastcategory AS `Forecast Category`,
  o.stagename,
  o.closedate AS `Close Date`,
  o.amount AS `Opp Amount`
FROM
  {catalog_name}.{schema_name}.opportunity o
  JOIN {catalog_name}.{schema_name}.account a ON o.accountid = a.id
WHERE
  o.forecastcategory = 'Pipeline'
  AND o.stagename NOT LIKE '%closed%'
  AND (
    isnull({function_name}.states)
    OR array_contains({function_name}.states, BillingState)
  );
"""
spark.sql(sql)

print("-- Catalog")
print(catalog_name)
print()

print("-- Schema")
print(schema_name)
print()

print("-- Function")
print(function_name)
print()

print("-- データが存在する states")
states_sql = f"""
SELECT DISTINCT
  a.BillingState AS `State`
FROM
  {catalog_name}.{schema_name}.opportunity o
  JOIN {catalog_name}.{schema_name}.account a ON o.accountid = a.id
WHERE
  o.forecastcategory = 'Pipeline'
  AND o.stagename NOT LIKE '%closed%'
"""
spark.sql(states_sql).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge1. Genie スペースの最適な利用方法を検討してください。
# MAGIC
# MAGIC こちらは Challenge のコンテンツであり、実施は任意です。
# MAGIC
# MAGIC 以下のGeneiスペースに関するドキュメントを参考に、Genieスペースを利用する際の基本方針（例：利用用途に応じて、小規模な利用から初めて反復的な改善を実施すること。）を5つ程度検討してください。新たな質問への回答精度の向上やベストプラクティスの整理など、アプローチは自由です。
# MAGIC
# MAGIC - [AI/BI Genie スペースとは - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/genie/)
# MAGIC - [Use trusted assets in AI/BI Genie spaces - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/genie/trusted-assets)
# MAGIC - [効果的な Genie スペースをキュレーションする - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/genie/best-practices)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Todo
# MAGIC
# MAGIC 1. 
# MAGIC 1. 
# MAGIC 1. 
# MAGIC 1. 
# MAGIC 1. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## End
