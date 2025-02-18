# Databricks notebook source
# MAGIC %md
# MAGIC ## カタログ名とスキーマ名を定義

# COMMAND ----------

dbutils.widgets.text("catalog_name", "")
catalog_name = dbutils.widgets.get("catalog_name")
print(f"catalog_name is {catalog_name}")

dbutils.widgets.text("schema_name", "")
schema_name = dbutils.widgets.get("schema_name")
print(f"schema_name is {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 共通処理

# COMMAND ----------

import inspect

from pyspark.sql.utils import AnalysisException

# COMMAND ----------

def add_pk(
    catalog_name,
    schema_name,
    table_name,
    pk_key="Id",
    pk_already_exists_text="already exists. Please delete the old constraint first",
):
    full_table_name = f"`{catalog_name}`.`{schema_name}`.`{table_name}`"
    add_pk_sql = f"""
    ALTER TABLE {full_table_name} ADD CONSTRAINT `{table_name}_pk`
        PRIMARY KEY({pk_key})
        RELY;
    """
    add_pk_sql = inspect.cleandoc(add_pk_sql)
    print("-- Add PK")
    print(add_pk_sql)
    try:
        spark.sql(add_pk_sql)
        print("Success")
        print("")
    except AnalysisException as e:
        if pk_already_exists_text not in str(e):
            raise e
        else:
            print(e)


def add_fk(
    child_relationships,
    catalog_name,
    schema_name,
    fk_already_exists_text="already has a foreign key constraint",
):
    if len(child_relationships) != 0:
        first_child_relationship = child_relationships[0]
        parent_table = first_child_relationship["parent_table"].lower()
        parent_full_table_name = f"`{catalog_name}`.`{schema_name}`.`{parent_table}`"
        parent_column_name = first_child_relationship["parent_column"]

        for child_r in child_relationships:
            child_table = child_r["childSObject"].lower()
            child_full_table_name = f"`{catalog_name}`.`{schema_name}`.`{child_table}`"
            child_column_name = child_r["field"]

            add_fk_sql = f"""
            ALTER TABLE {child_full_table_name} ADD CONSTRAINT {child_column_name}_of_{child_table}_to_{parent_table}r_fk
                FOREIGN KEY({child_column_name}) REFERENCES {parent_full_table_name};
            """
            add_fk_sql = inspect.cleandoc(add_fk_sql)
            print("-- Add FK")
            print(add_fk_sql)
            try:
                spark.sql(add_fk_sql)
                print("Success")
                print("")
            except AnalysisException as e:
                if fk_already_exists_text not in str(e):
                    raise e
                else:
                    print(e)
                    print("")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Account (取引先)

# COMMAND ----------

table_name = "account"
child_relationships = [
    {
        "childSObject": "Account",
        "field": "ParentId",
        "parent_column": "Id",
        "parent_table": "Account",
    },
    {
        "childSObject": "Case",
        "field": "AccountId",
        "parent_column": "Id",
        "parent_table": "Account",
    },
    {
        "childSObject": "Contact",
        "field": "AccountId",
        "parent_column": "Id",
        "parent_table": "Account",
    },
    {
        "childSObject": "Lead",
        "field": "ConvertedAccountId",
        "parent_column": "Id",
        "parent_table": "Account",
    },
    {
        "childSObject": "Opportunity",
        "field": "AccountId",
        "parent_column": "Id",
        "parent_table": "Account",
    },
    {
        "childSObject": "Order",
        "field": "AccountId",
        "parent_column": "Id",
        "parent_table": "Account",
    },
    {
        "childSObject": "User",
        "field": "AccountId",
        "parent_column": "Id",
        "parent_table": "Account",
    },
]

# COMMAND ----------

add_pk(
    catalog_name,
    schema_name,
    table_name,
)

# COMMAND ----------

add_fk(
    child_relationships,
    catalog_name,
    schema_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contract (契約)

# COMMAND ----------

table_name = "contact"
child_relationships = [
    {
        "childSObject": "Case",
        "field": "ContactId",
        "parent_column": "Id",
        "parent_table": "Contact",
    },
    {
        "childSObject": "Contact",
        "field": "ReportsToId",
        "parent_column": "Id",
        "parent_table": "Contact",
    },
    {
        "childSObject": "Lead",
        "field": "ConvertedContactId",
        "parent_column": "Id",
        "parent_table": "Contact",
    },
    {
        "childSObject": "Opportunity",
        "field": "ContactId",
        "parent_column": "Id",
        "parent_table": "Contact",
    },
    {
        "childSObject": "Order",
        "field": "BillToContactId",
        "parent_column": "Id",
        "parent_table": "Contact",
    },
    {
        "childSObject": "Order",
        "field": "CustomerAuthorizedById",
        "parent_column": "Id",
        "parent_table": "Contact",
    },
    {
        "childSObject": "Order",
        "field": "ShipToContactId",
        "parent_column": "Id",
        "parent_table": "Contact",
    },
    {
        "childSObject": "User",
        "field": "ContactId",
        "parent_column": "Id",
        "parent_table": "Contact",
    },
]

# COMMAND ----------

add_pk(
    catalog_name,
    schema_name,
    table_name,
)

# COMMAND ----------

add_fk(
    child_relationships,
    catalog_name,
    schema_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lead (リード)

# COMMAND ----------

table_name = "lead"
child_relationships = []

# COMMAND ----------

add_pk(
    catalog_name,
    schema_name,
    table_name,
)

# COMMAND ----------

add_fk(
    child_relationships,
    catalog_name,
    schema_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Campaign (キャンペーン)

# COMMAND ----------

table_name = "campaign"
child_relationships = [
    {
        "childSObject": "Campaign",
        "field": "ParentId",
        "parent_column": "Id",
        "parent_table": "Campaign",
    },
    {
        "childSObject": "Opportunity",
        "field": "CampaignId",
        "parent_column": "Id",
        "parent_table": "Campaign",
    },
]

# COMMAND ----------

add_pk(
    catalog_name,
    schema_name,
    table_name,
)

# COMMAND ----------

add_fk(
    child_relationships,
    catalog_name,
    schema_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opportunity (商談)

# COMMAND ----------

table_name = "opportunity"
child_relationships = [
    {
        "childSObject": "Lead",
        "field": "ConvertedOpportunityId",
        "parent_column": "Id",
        "parent_table": "Opportunity",
    }
]

# COMMAND ----------

add_pk(
    catalog_name,
    schema_name,
    table_name,
)

# COMMAND ----------

add_fk(
    child_relationships,
    catalog_name,
    schema_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product2 (製品)

# COMMAND ----------

table_name = "product2"
child_relationships = [
    {
        "childSObject": "pricebook_entry",
        "field": "Product2Id",
        "parent_column": "Id",
        "parent_table": "Product2",
    },
    {
        "childSObject": "opportunity",
        "field": "Product2Id__c",
        "parent_column": "Id",
        "parent_table": "Product2",
    },
    {
        "childSObject": "case",
        "field": "Product2Id__c",
        "parent_column": "Id",
        "parent_table": "Product2",
    },
]

# COMMAND ----------

add_pk(
    catalog_name,
    schema_name,
    table_name,
)

# COMMAND ----------

add_fk(
    child_relationships,
    catalog_name,
    schema_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Price Book Entry (価格表エントリ)

# COMMAND ----------

table_name = "pricebook_entry"
child_relationships = []

# COMMAND ----------

add_pk(
    catalog_name,
    schema_name,
    table_name,
)

# COMMAND ----------

add_fk(
    child_relationships,
    catalog_name,
    schema_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Case (ケース)

# COMMAND ----------

table_name = "case"
child_relationships = [
    {
        "childSObject": "Case",
        "field": "ParentId",
        "parent_column": "Id",
        "parent_table": "case",
    }
]

# COMMAND ----------

add_pk(
    catalog_name,
    schema_name,
    table_name,
)

# COMMAND ----------

add_fk(
    child_relationships,
    catalog_name,
    schema_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## User（ユーザ）

# COMMAND ----------

table_name = "user"
child_relationships = [
    {
        "childSObject": "Account",
        "field": "CreatedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Account",
        "field": "LastModifiedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Account",
        "field": "OwnerId",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Campaign",
        "field": "CreatedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Campaign",
        "field": "LastModifiedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Campaign",
        "field": "OwnerId",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Case",
        "field": "CreatedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Case",
        "field": "LastModifiedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Case",
        "field": "OwnerId",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Contact",
        "field": "CreatedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Contact",
        "field": "LastModifiedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Contact",
        "field": "OwnerId",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Lead",
        "field": "CreatedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Lead",
        "field": "LastModifiedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Lead",
        "field": "OwnerId",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Opportunity",
        "field": "CreatedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Opportunity",
        "field": "LastModifiedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Opportunity",
        "field": "OwnerId",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Order",
        "field": "ActivatedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Order",
        "field": "CompanyAuthorizedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Order",
        "field": "CreatedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Order",
        "field": "LastModifiedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Order",
        "field": "OwnerId",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "pricebook_entry",
        "field": "CreatedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "pricebook_entry",
        "field": "LastModifiedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Product2",
        "field": "CreatedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "Product2",
        "field": "LastModifiedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "User",
        "field": "CreatedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "User",
        "field": "DelegatedApproverId",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "User",
        "field": "LastModifiedById",
        "parent_column": "Id",
        "parent_table": "user",
    },
    {
        "childSObject": "User",
        "field": "ManagerId",
        "parent_column": "Id",
        "parent_table": "user",
    },
]

# COMMAND ----------

add_pk(
    catalog_name,
    schema_name,
    table_name,
)

# COMMAND ----------

add_fk(
    child_relationships,
    catalog_name,
    schema_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order (受注)

# COMMAND ----------

table_name = "order"
child_relationships = [
    {
        "childSObject": "Order",
        "field": "OriginalOrderId",
        "parent_column": "Id",
        "parent_table": "Order",
    }
]

# COMMAND ----------

add_pk(
    catalog_name,
    schema_name,
    table_name,
)

# COMMAND ----------

add_fk(
    child_relationships,
    catalog_name,
    schema_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC # End
