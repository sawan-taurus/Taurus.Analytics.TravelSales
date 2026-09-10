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

File_Type = 'Daily'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Define your workspace and file path
WORKSPACE = "Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = "TaurusSilverLH"
SRC_TABLE = File_Type
DEST_LAKEHOUSE = "TaurusGoldLH"  # Destination Lakehouse
DEST_TABLE = "DimProductType"

# Define file paths
src_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{SRC_TABLE}"
dest_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{DEST_LAKEHOUSE}.Lakehouse/Tables/{DEST_TABLE}"

# Read the source Delta table
silver_df = spark.read.format("delta").load(src_file_path)

# Extract distinct Product Type
distinct_product_type = silver_df.select("ProductType").distinct()

try:
    # Read the existing DimProductType table
    existing_dim_df = spark.read.format("delta").load(dest_file_path)
    
    # Identify new ProductType (not already in the destination table)
    new_entries_df = distinct_product_type.join(existing_dim_df, "ProductType", "left_anti")
    
    # Add sequential ProductTypeID starting from the max ID in the existing table
    max_id = existing_dim_df.agg({"ProductTypeID": "max"}).collect()[0][0] or 0
    window_spec = Window.orderBy("ProductType")
    new_entries_df = new_entries_df.withColumn("ProductTypeID", row_number().over(window_spec) + max_id)
    
    # Rearrange columns to make ProductTypeID the first column
    new_entries_df = new_entries_df.select("ProductTypeID", "ProductType")
    
    # Append the new entries to the existing DimProductType table
    combined_dim_df = existing_dim_df.union(new_entries_df)
    combined_dim_df.write.format("delta").mode("overwrite").save(dest_file_path)
    
    print(f"New Product Type appended to: {dest_file_path}")
    
except:
    # If the destination table does not exist, create it with new entries
    window_spec = Window.orderBy("ProductType")
    product_type_dimension = distinct_product_type.withColumn("ProductTypeID", row_number().over(window_spec))
    product_type_dimension = product_type_dimension.select("ProductTypeID", "ProductType")
    product_type_dimension.write.format("delta").mode("overwrite").save(dest_file_path)
    
    print(f"ProductType Dimension created at: {dest_file_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
