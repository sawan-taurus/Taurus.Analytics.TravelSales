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

# PARAMETERS CELL ********************

# File_Type = 'AtozAgg'
# Process_Date = '2025-05-02'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, sum as _sum

if File_Type in ['Trusted', 'Viva', 'SOI', 'Start']:
    File_Type = 'Sales'

# Define your workspace and file paths
WORKSPACE = "Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = "TaurusSilverLH"
SRC_TABLE = 'Daily' + File_Type
Gold_LAKEHOUSE = "TaurusGoldLH"

DIM_DATE_TABLE = "DimDate"
DIM_TRAVEL_BRAND_TABLE = "DimTravelBrand"

FACT_TABLE = "FactDaily" + File_Type

# Define file paths
src_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{SRC_TABLE}"
dim_travel_brand_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/{DIM_TRAVEL_BRAND_TABLE}"
dim_date_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/{DIM_DATE_TABLE}"
fact_table_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/{FACT_TABLE}"

# Set processing date
PROCESS_DATE = Process_Date  # Change this as needed

# Read required tables
silver_df = spark.read.format("delta").load(src_file_path).filter(col("Date") == PROCESS_DATE)
dim_travel_brand_df = spark.read.format("delta").load(dim_travel_brand_path)
dim_date_df = spark.read.format("delta").load(dim_date_path)
fact_existing_df = spark.read.format("delta").load(fact_table_path)

# Get DateID for the processing date
date_id_row = dim_date_df.filter(col("Date") == PROCESS_DATE).select("DateId").collect()
if not date_id_row:
    raise ValueError(f"Date '{PROCESS_DATE}' not found in DimDate table.")

date_id = date_id_row[0]["DateId"]

silver_df = silver_df.withColumnRenamed("travel_brand", "TravelBrand")

# Prepare new fact data for that day, including all dimensions
fact_df = silver_df \
    .join(dim_travel_brand_df, "TravelBrand", "inner") \
    .join(dim_date_df, silver_df["Date"] == dim_date_df["Date"], "inner") \
    .select(
        col("TravelBrandID"),
        col("DateId"),
        col("Volume"),
        col("GWPIncIPTDay"),
        col("GWPExIPTDay"),
        col("VolumeMonth"),
        col("TotalGrossIncIPTMonth"),
        col("TotalGrossExcIPTMonth")
    )

fact_df_aggregated = fact_df.groupBy(
    "TravelBrandID", "DateId"
).agg(
    _sum("Volume").alias("Volume"),
    _sum("GWPIncIPTDay").alias("GWPIncIPTDay"),
    _sum("GWPExIPTDay").alias("GWPExIPTDay"),
    _sum("VolumeMonth").alias("VolumeMonth"),
    _sum("TotalGrossIncIPTMonth").alias("TotalGrossIncIPTMonth"),
    _sum("TotalGrossExcIPTMonth").alias("TotalGrossExcIPTMonth")
)

# # # Remove existing data for that DateID
fact_df_cleaned = fact_existing_df.filter(col("DateId") != date_id)

# # # Append new data
# final_df = fact_df_cleaned.unionByName(fact_df_aggregated)

final_df = fact_df_cleaned.unionByName(fact_df_aggregated.select(
    col("TravelBrandID"),
    col("DateId"),
    col("Volume"),
    col("GWPIncIPTDay"),
    col("GWPExIPTDay"),
    col("VolumeMonth"),
    col("TotalGrossIncIPTMonth"),
    col("TotalGrossExcIPTMonth")
))

# # # Write back to Fact table (overwrite full table)
final_df.write.format("delta").mode("overwrite").save(fact_table_path)

print(f"FactDaily{File_Type} table updated for {PROCESS_DATE} (DateId = {date_id}) with all dimensions added.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from factdailysalesreport
# MAGIC where dateid = 20250826

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
