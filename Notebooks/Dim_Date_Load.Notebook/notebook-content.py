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

# MARKDOWN ********************

# # **Generate Dim_Date table (one off code)**

# CELL ********************

from pyspark.sql import functions as F

start_date = "2020-01-01"
end_date = "2026-12-31"

# Generate date range
date_range_df = spark.range(0, 1).select(
    F.explode(
        F.expr(f"sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)")
    ).alias('Date')
)

# Build the date dimension DataFrame
dim_date_df = date_range_df \
    .withColumn("Year", F.year("Date")) \
    .withColumn("Quarter", F.quarter("Date")) \
    .withColumn("Month", F.month("Date")) \
    .withColumn("DayOfWeek", F.dayofweek("Date")) \
    .withColumn("DayOfMonth", F.dayofmonth("Date")) \
    .withColumn(
        "DateId",
        F.concat(
            F.col("Year").cast("string"),
            F.format_string("%02d", F.col("Month")),
            F.format_string("%02d", F.col("DayOfMonth"))
        ).cast("bigint")
    )

# 🚨 Drop old version of the table to avoid conflicts
# spark.sql("DROP TABLE IF EXISTS DimDate")

# ✅ Save new table with fresh schema
dim_date_df.write.mode("overwrite").saveAsTable("DimDate")

# Confirm it's working
# spark.sql("SELECT * FROM DimDate LIMIT 10").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
