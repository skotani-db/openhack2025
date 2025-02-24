# Databricks notebook source
# MAGIC %md
# MAGIC ## 概要
# MAGIC
# MAGIC 本ノートブックは AI/BI コンテスト の Dashboard で使用するデータセットの定義を出力します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前準備

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# 本ノートブックで利用するスキーマ
schema_name = f"03_data_analysis_by_gen_ai_for_{user_name}"
print(f"schema_name: `{schema_name}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## データセット作成コードの生成
# MAGIC AI/BIダッシュボードで使用する以下のデータセットのクエリを生成するセクションです
# MAGIC - ケース
# MAGIC - 問い合わせ
# MAGIC - クレーム
# MAGIC - 未クローズ
# MAGIC - 未クローズかつ優先度高い
# MAGIC - ケースのレビュー
# MAGIC
# MAGIC
# MAGIC なお、ケースのレビューはセル7番に **-- ケースのレビューテーブル作成** 以下のクエリをコピーしてテーブル作成します。
# MAGIC または左ペインからSQLエディタを展開して、クエリを実行します。

# COMMAND ----------

print("-- ケース")
case = f"""select * from {catalog_name}.{schema_name}.case
"""
print(case)


print("-- 問い合わせ")
query = f"""select * from {catalog_name}.{schema_name}.case
where type = "問い合わせ"
"""
print(query)


print("-- クレーム")
claim = f"""SELECT * FROM {catalog_name}.{schema_name}.case
WHERE Type = 'クレーム';
"""
print(claim)


print("-- 未クローズ")
not_closed = f"""SELECT * FROM {catalog_name}.{schema_name}.case
WHERE IsClosed = false;
"""
print(not_closed)


print("-- 未クローズかつ優先度高い")
not_closed_high_priority = f"""SELECT * FROM {catalog_name}.{schema_name}.case
WHERE IsClosed = false and Priority = "高";
"""
print(not_closed_high_priority)


print("-- ケースのレビューテーブル作成")
review_ctas = f"""CREATE TABLE {catalog_name}.{schema_name}.case_classified AS 
SELECT *, ai_classify(
    Description,
    ARRAY(
      "ソフトウェアのバグ",
      "ハードウェアの動作不良",
      "ハードウェアの破損",
      "ネットワークの動作不良",
      "その他"
    )
  ) AS predict
FROM {catalog_name}.{schema_name}.`case`
LIMIT 100;
"""
print(review_ctas)


print("-- ケースのレビュー")
review = f"""SELECT * FROM {catalog_name}.{schema_name}.case_classified"""
print(review)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ケースのレビューテーブル作成
# MAGIC -- ケース
# MAGIC select * from openhack2025.03_data_analysis_by_gen_ai_for_demo.case;
# MAGIC
# MAGIC -- 問い合わせ
# MAGIC select * from openhack2025.03_data_analysis_by_gen_ai_for_demo.case
# MAGIC where type = "問い合わせ";
# MAGIC -- クレーム
# MAGIC SELECT * FROM openhack2025.03_data_analysis_by_gen_ai_for_demo.case
# MAGIC WHERE Type = 'クレーム';
# MAGIC
# MAGIC -- 未クローズ
# MAGIC SELECT * FROM openhack2025.03_data_analysis_by_gen_ai_for_demo.case
# MAGIC WHERE IsClosed = false;
# MAGIC
# MAGIC -- 未クローズかつ優先度高い
# MAGIC SELECT * FROM openhack2025.03_data_analysis_by_gen_ai_for_demo.case
# MAGIC WHERE IsClosed = false and Priority = "高";
# MAGIC
# MAGIC -- ケースのレビューテーブル作成
# MAGIC CREATE TABLE openhack2025.03_data_analysis_by_gen_ai_for_demo.case_classified AS 
# MAGIC SELECT *, ai_classify(
# MAGIC     Description,
# MAGIC     ARRAY(
# MAGIC       "ソフトウェアのバグ",
# MAGIC       "ハードウェアの動作不良",
# MAGIC       "ハードウェアの破損",
# MAGIC       "ネットワークの動作不良",
# MAGIC       "その他"
# MAGIC     )
# MAGIC   ) AS predict
# MAGIC FROM openhack2025.03_data_analysis_by_gen_ai_for_demo.`case`
# MAGIC LIMIT 100;
# MAGIC
# MAGIC -- ケースのレビュー
# MAGIC SELECT * FROM openhack2025.03_data_analysis_by_gen_ai_for_demo.case_classified;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ダッシュボード作成例
# MAGIC https://www.youtube.com/watch?v=zrYAOUcpyDQ

# COMMAND ----------


