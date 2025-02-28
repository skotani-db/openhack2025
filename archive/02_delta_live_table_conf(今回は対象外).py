# Databricks notebook source
# MAGIC %md
# MAGIC ## 概要
# MAGIC
# MAGIC 本ノートブックは Delta Live table の定義を記述しており、**02_data_engineering**ノートブックにて利用します。

# COMMAND ----------

try:
    catalog_name = spark.conf.get("mypipeline.catalog_name")
    schema_name = spark.conf.get("mypipeline.schema_name")
except Exception as e:
    print(f"Error retrieving configuration: {e}")
    # 下記はデバック用の値を変数にセット
    catalog_name = "openhachason_team_01"
    schema_name = "02_data_engineering_for_team_01"

# COMMAND ----------

volume_name = "src_file_volume_01"

# COMMAND ----------

import dlt
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. Bronzeテーブルのパイプラインを作成してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 実践例

# COMMAND ----------

@dlt.table(
    name="product2__bronze",
    comment="This table contains the bronze data for product2",
)
def product2_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("overwriteSchema", "true")
        .load(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/Product2*.csv")
    )

    df = (
        df.select("*", "_metadata")
        .withColumn("_datasource", df["_metadata.file_path"])
        .withColumn("_ingest_timestamp", df["_metadata.file_modification_time"])
        .drop("_metadata")
    )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### ToDo `pricebook_entry__bronze`のパイプラインを作成してください。

# COMMAND ----------

# ToDo


# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. Silver テーブルのパイプラインを作成してください

# COMMAND ----------

# MAGIC %md
# MAGIC ### 実践例

# COMMAND ----------

@dlt.table(
  name="product2__silver",
)
def product2__silver():
    df = spark.sql(
        """
        with slv_records (
        SELECT
            Id,
            MAX(_ingest_timestamp) AS max_ingest_timestamp
            FROM
                LIVE.product2__bronze
            GROUP BY
                id
        )
        SELECT
            brz.`Id`,
            brz.`Name`,
            brz.`ProductCode`,
            brz.`Description`,
            brz.`IsActive`::BOOLEAN,
            brz.`CreatedDate`::TIMESTAMP,
            brz.`CreatedById`,
            brz.`LastModifiedDate`::TIMESTAMP,
            brz.`LastModifiedById`,
            brz.`SystemModstamp`::TIMESTAMP,
            brz.`Family`,
            brz.`ExternalDataSourceId`,
            brz.`ExternalId`,
            brz.`DisplayUrl`,
            brz.`QuantityUnitOfMeasure`,
            brz.`IsDeleted`::BOOLEAN,
            brz.`IsArchived`::BOOLEAN,
            brz.`LastViewedDate`::TIMESTAMP,
            brz.`LastReferencedDate`::TIMESTAMP,
            brz.`StockKeepingUnit`,
            brz._datasource,
            brz._ingest_timestamp::timestamp
            
            FROM
                LIVE.product2__bronze AS brz
            INNER JOIN 
                slv_records AS slv
                ON 
                    brz.id =  slv.id
                    AND brz._ingest_timestamp =  slv.max_ingest_timestamp
        """
    )
    df = df.drop_duplicates(['Id'])
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### ToDo `pricebook_entry__silver`のパイプラインを作成してください。
# MAGIC
# MAGIC `pricebook_entry__silver`テーブルにおける主キーは`Id`列です。

# COMMAND ----------

# ToDo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Gold Tableのパイプラインを作成してください

# COMMAND ----------

# MAGIC %md
# MAGIC ### 実践例

# COMMAND ----------

@dlt.table(
  name="product_count_by_family",
)
def product_count_by_family():
    df = spark.sql(
        """
        SELECT
            Family,
            COUNT(*) AS product_count
        FROM
            LIVE.product2__silver
        GROUP BY
            ALL
        """
    )
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### ToDo `d_product`パイプラインを作成してください。
# MAGIC
# MAGIC `Product2`をベースに、`pricebook_entry`にある`UnitPrice`を追加したデータのテーブルを作成してください。
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   prd.*
# MAGIC     EXCEPT (
# MAGIC       _datasource,
# MAGIC       _ingest_timestamp
# MAGIC     ),
# MAGIC   pbk.UnitPrice
# MAGIC   FROM
# MAGIC     LIVE.product2__silver prd
# MAGIC   INNER JOIN 
# MAGIC     LIVE.pricebook_entry__silver pbk
# MAGIC   on 
# MAGIC     prd.id = pbk.Product2Id
# MAGIC ```

# COMMAND ----------

# ToDo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Challenge1. メダリオンアーキテクチャの導入の是非について、既存のデータ分析システムのアーキテクチャと比較した上で検討してください。
# MAGIC
# MAGIC こちらは Challenge のコンテンツであり、実施は任意です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## End
