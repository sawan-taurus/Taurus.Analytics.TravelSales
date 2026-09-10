# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d69a91e4-0efe-4e5a-81c6-43b1d5fab183",
# META       "default_lakehouse_name": "TaurusGoldLH",
# META       "default_lakehouse_workspace_id": "9c958852-649b-48e2-ac6a-f03167ff64dc",
# META       "known_lakehouses": [
# META         {
# META           "id": "d69a91e4-0efe-4e5a-81c6-43b1d5fab183"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Source_Type = "Atoz"  # Change to "Sales" or "Oasis" as needed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DecimalType
)
from decimal import Decimal
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

Silver_Lakehouse = "TaurusSilverLH"
Gold_Lakehouse = "TaurusGoldLH"

dimension_schemas = {
    "tblSchemeHeaders": {
        "columns": ["FriendlyName"],
        "rename": {"FriendlyName": "FriendlyName"},
        "types": {
            "SchemeHeaderId": "bigint",
            "FriendlyName": "string"
        },
        "order_by": "FriendlyName",
        "id_column": "SchemeHeaderId"
    }
}

type_mapping = {
    "bigint": LongType(),
    "int": IntegerType(),
    "string": StringType(),
    "decimal(10,2)": DecimalType(10, 2)
}

def get_dimension_table_name(base_table_name, source_type):
    stripped = base_table_name[3:]
    words = re.findall(r'[A-Z][a-z]*', stripped)
    return "Dim" + "".join(word.capitalize() for word in words)

def create_or_update_dimension(source_type: str, silver_lakehouse: str, gold_lakehouse: str):
    for base_table_name, schema in dimension_schemas.items():
        source_table_name = f"{base_table_name}{source_type}"
        id_column = schema["id_column"]
        dim_table_name = get_dimension_table_name(base_table_name, source_type)
        silver_table_name = f"{silver_lakehouse}.silver_" + re.sub(r'(?<!^)(?=[A-Z])', '_', source_table_name[3:]).lower()
        gold_table_name = f"{gold_lakehouse}.{dim_table_name}"

        print(f"🔄 Processing: {source_table_name}")
        print(f"Silver Table: {silver_table_name}")
        print(f"Gold Table: {gold_table_name}")

        df_silver_raw = spark.read.table(silver_table_name).select(*schema["columns"]).dropDuplicates()

        try:
            df_existing = spark.read.table(gold_table_name)
            max_id = df_existing.agg({id_column: "max"}).collect()[0][0] or 0

            if df_existing.filter(col(id_column) == 0).count() == 0:
                print(f"Adding default 'Unknown' row to {gold_table_name}")
                default_row = []
                for field in df_existing.schema:
                    if isinstance(field.dataType, StringType):
                        default_row.append("Unknown")
                    elif isinstance(field.dataType, DecimalType):
                        default_row.append(Decimal("0.00"))
                    else:
                        default_row.append(0)
                df_default = spark.createDataFrame([tuple(default_row)], schema=df_existing.schema)
                df_default.write.mode("append").saveAsTable(gold_table_name)

        except:
            print(f"{gold_table_name} does not exist yet — creating with default row")
            renamed_cols = [schema["rename"].get(c, c) for c in schema["columns"]]

            fields = [StructField(id_column, type_mapping[schema["types"][id_column]], True)]
            for col_name in renamed_cols:
                fields.append(StructField(col_name, type_mapping[schema["types"][col_name]], True))
            table_schema = StructType(fields)

            default_row = [0]
            for col_name in renamed_cols:
                dtype = schema["types"][col_name]
                if dtype == "string":
                    default_row.append("Unknown")
                elif dtype in ["bigint", "int"]:
                    default_row.append(0)
                elif dtype == "decimal(10,2)":
                    default_row.append(Decimal("0.00"))
                else:
                    default_row.append(None)

            df_default = spark.createDataFrame([tuple(default_row)], schema=table_schema)
            df_default.write.mode("overwrite").saveAsTable(gold_table_name)
            df_existing = df_default
            max_id = 0

        df_existing_unrenamed = df_existing
        for old_col, new_col in schema["rename"].items():
            df_existing_unrenamed = df_existing_unrenamed.withColumnRenamed(new_col, old_col)

        df_new_raw = df_silver_raw.join(
            df_existing_unrenamed.drop(id_column),
            on=schema["columns"],
            how="left_anti"
        )

        for col_name in schema["columns"]:
            df_new_raw = df_new_raw.filter(
                (F.col(col_name).isNotNull()) & (F.trim(F.col(col_name)) != "")
            )

        df_new = df_new_raw
        for old_col, new_col in schema["rename"].items():
            df_new = df_new.withColumnRenamed(old_col, new_col)

        if df_new.count() > 0:
            window_spec = Window.orderBy(schema["order_by"])
            df_new_with_id = df_new.withColumn(
                id_column,
                (row_number().over(window_spec) + max_id).cast(type_mapping[schema["types"][id_column]])
            )

            for col_name, dtype in schema["types"].items():
                df_new_with_id = df_new_with_id.withColumn(
                    col_name,
                    F.col(col_name).cast(type_mapping[dtype])
                )

            existing_schema = df_existing.schema
            existing_columns = [f.name for f in existing_schema]

            for field in existing_schema:
                df_new_with_id = df_new_with_id.withColumn(
                    field.name,
                    F.col(field.name).cast(field.dataType)
                )

            df_final = df_new_with_id.select(*existing_columns)
            df_final.write.mode("append").saveAsTable(gold_table_name)
            print(f"✅ {df_final.count()} new records inserted into {gold_table_name}")
        else:
            print("ℹ️ No new valid records to insert.")

# Example usage

create_or_update_dimension(Source_Type, Silver_Lakehouse, Gold_Lakehouse)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
