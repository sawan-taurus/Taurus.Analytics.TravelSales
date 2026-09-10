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

# File_Type = 'DailyRenewal'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window

if File_Type in ['DailyTrusted', 'DailyViva', 'DailySOI', 'DailyStart']:
    File_Type = 'DailySales'

# Define your workspace and file path
WORKSPACE = "Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = "TaurusSilverLH"
SRC_TABLE = File_Type  # Ensure File_Type is defined earlier
DEST_LAKEHOUSE = "TaurusGoldLH"
DEST_TABLE = "DimTravelBrand"

# Define file paths
src_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{SRC_TABLE}"
dest_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{DEST_LAKEHOUSE}.Lakehouse/Tables/{DEST_TABLE}"

# Read the source Delta table
silver_df = spark.read.format("delta").load(src_file_path)

# Extract distinct Travel Brands
distinct_travel_brands = silver_df.select("travel_brand").distinct()

try:
    # Read the existing DimTravelBrand table
    existing_dim_df = spark.read.format("delta").load(dest_file_path)

    # Rename column for consistency
    existing_dim_df = existing_dim_df.withColumnRenamed("TravelBrand", "travel_brand")

    # Identify new Travel Brands not already in the destination table
    new_entries_df = distinct_travel_brands.join(existing_dim_df, "travel_brand", "left_anti")

    # Add sequential TravelBrandID starting from the max ID
    max_id = existing_dim_df.agg({"TravelBrandID": "max"}).collect()[0][0] or 0
    window_spec = Window.orderBy("travel_brand")
    new_entries_df = new_entries_df.withColumn("TravelBrandID", row_number().over(window_spec) + max_id)

    # Rename column to match target schema and select final columns
    new_entries_df = new_entries_df.withColumnRenamed("travel_brand", "TravelBrand")
    new_entries_df = new_entries_df.select("TravelBrandID", "TravelBrand")

    # Combine and write updated dimension table
    existing_dim_df = existing_dim_df.withColumnRenamed("travel_brand", "TravelBrand")
    combined_dim_df = existing_dim_df.union(new_entries_df)
    combined_dim_df.write.format("delta").mode("overwrite").save(dest_file_path)

    print(f"New Travel Brands appended to: {dest_file_path}")

except:
    # If the destination table does not exist, create it
    window_spec = Window.orderBy("travel_brand")
    travel_brand_dimension = distinct_travel_brands.withColumn("TravelBrandID", row_number().over(window_spec))
    travel_brand_dimension = travel_brand_dimension.withColumnRenamed("travel_brand", "TravelBrand")
    travel_brand_dimension = travel_brand_dimension.select("TravelBrandID", "TravelBrand")
    travel_brand_dimension.write.format("delta").mode("overwrite").save(dest_file_path)

    print(f"Travel Brand Dimension created at: {dest_file_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
