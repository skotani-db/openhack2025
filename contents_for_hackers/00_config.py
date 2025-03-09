# Databricks notebook source
# MAGIC %md
# MAGIC ##　Config(設定)

# COMMAND ----------

# MAGIC %md
# MAGIC 各チームでOpenhackを実施するための設定をこのファイルで行います。  
# MAGIC 各セルのコメント文を確認し、設定を確認してください。

# COMMAND ----------

# DBTITLE 1,チーム名、ユーザー名を設定
# チーム名、及び、ユーザー名を設定
team_name = <Todo: 各チームのカタログ名を設定>  ## team_name はカタログ名となる
user_name = <Todo: ユーザーごとの名前を指定>  ## user_name はスキーマ名の一部となる
print(f"user_name: `{user_name}`")

# 利用するカタログ名を設定
catalog_name = team_name
print(f"catalog_name: `{catalog_name}`")

# COMMAND ----------

# ソースファイルを配置する Volume 名を設定(原則変更不要)
src_volume_name = "src_data"
src_schema_name = "default"
src_folder_name = "sample_data_01"

# COMMAND ----------


