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

File_Type = 'Oasis'
Process_Date = '2025-04-01'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, sum as _sum

# Define your workspace and file paths
WORKSPACE = "Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = "TaurusSilverLH"
SRC_TABLE = File_Type
Gold_LAKEHOUSE = "TaurusGoldLH"

DIM_DATE_TABLE = "DimDate"
DIM_PRODUCT_BRAND_TABLE = "DimProductBrand"
DIM_SCHEME_TABLE = "DimScheme"
DIM_CHANNEL_TABLE = "DimChannel"  # Added DimChannel
DIM_PRODUCT_TYPE_TABLE = "DimProductType"  # Added DimProductType

FACT_TABLE = "FactDaily" + File_Type

# Define file paths
src_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{SRC_TABLE}"
dim_product_brand_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/{DIM_PRODUCT_BRAND_TABLE}"
dim_date_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/{DIM_DATE_TABLE}"
dim_scheme_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/{DIM_SCHEME_TABLE}"
dim_channel_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/{DIM_CHANNEL_TABLE}"
dim_product_type_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/{DIM_PRODUCT_TYPE_TABLE}"
fact_table_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/{FACT_TABLE}"

# Set processing date
PROCESS_DATE = Process_Date  # Change this as needed

# Read required tables
silver_df = spark.read.format("delta").load(src_file_path).filter(col("Date") == PROCESS_DATE)
dim_product_brand_df = spark.read.format("delta").load(dim_product_brand_path)
dim_date_df = spark.read.format("delta").load(dim_date_path)
dim_scheme_df = spark.read.format("delta").load(dim_scheme_path)
dim_channel_df = spark.read.format("delta").load(dim_channel_path)  # Load DimChannel
dim_product_type_df = spark.read.format("delta").load(dim_product_type_path)  # Load DimProductType
fact_existing_df = spark.read.format("delta").load(fact_table_path)

# Get DateID for the processing date
date_id_row = dim_date_df.filter(col("Date") == PROCESS_DATE).select("DateId").collect()
if not date_id_row:
    raise ValueError(f"Date '{PROCESS_DATE}' not found in DimDate table.")

date_id = date_id_row[0]["DateId"]

# Prepare new fact data for that day, including all dimensions
fact_df = silver_df \
    .join(dim_product_brand_df, "ProductBrand", "inner") \
    .join(dim_scheme_df, "Scheme", "inner") \
    .join(dim_channel_df, "Channel", "inner") \
    .join(dim_product_type_df, "ProductType", "inner") \
    .join(dim_date_df, silver_df["Date"] == dim_date_df["Date"], "inner") \
    .select(
        col("ProductBrandID"),
        col("SchemeID"),
        col("ChannelID"),
        col("ProductTypeID"),
        col("DateId"),
        col("VolumeDay"),
        col("GWPIncIPTDay"),
        col("GWPExIPTDay"),
        col("NTUDay"),
        col("VolumeMth"),
        col("GWPIncIPTMth"),
        col("GWPExIPTMth"),
        col("NTUMth")
    )

fact_df_aggregated = fact_df.groupBy(
    "ProductBrandID", "SchemeID", "ChannelID", "ProductTypeID", "DateId"
).agg(
    _sum("VolumeDay").alias("VolumeDay"),
    _sum("GWPIncIPTDay").alias("GWPIncIPTDay"),
    _sum("GWPExIPTDay").alias("GWPExIPTDay"),
    _sum("NTUDay").alias("NTUDay"),
    _sum("VolumeMth").alias("VolumeMth"),
    _sum("GWPIncIPTMth").alias("GWPIncIPTMth"),
    _sum("GWPExIPTMth").alias("GWPExIPTMth"),
    _sum("NTUMth").alias("NTUMth")
)

# Remove existing data for that DateID
fact_df_cleaned = fact_existing_df.filter(col("DateId") != date_id)

# Append new data
final_df = fact_df_cleaned.unionByName(fact_df_aggregated)

# Write back to Fact table (overwrite full table)
final_df.write.format("delta").mode("overwrite").save(fact_table_path)

print(f"FactDaily{File_Type} table updated for {PROCESS_DATE} (DateId = {date_id}) with all dimensions added.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
