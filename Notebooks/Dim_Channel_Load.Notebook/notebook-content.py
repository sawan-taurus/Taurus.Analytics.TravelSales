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

File_Type = 'Atoz'

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
DEST_TABLE = "DimChannel"

# Define file paths
src_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{SRC_TABLE}"
dest_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{DEST_LAKEHOUSE}.Lakehouse/Tables/{DEST_TABLE}"

# Read the source Delta table
silver_df = spark.read.format("delta").load(src_file_path)

# Extract distinct Channel
distinct_channel = silver_df.select("Channel").distinct()

try:
    # Read the existing DimChannel table
    existing_dim_df = spark.read.format("delta").load(dest_file_path)
    
    # Identify new Channel (not already in the destination table)
    new_entries_df = distinct_channel.join(existing_dim_df, "Channel", "left_anti")
    
    # Add sequential ChannelID starting from the max ID in the existing table
    max_id = existing_dim_df.agg({"ChannelID": "max"}).collect()[0][0] or 0
    window_spec = Window.orderBy("Channel")
    new_entries_df = new_entries_df.withColumn("ChannelID", row_number().over(window_spec) + max_id)
    
    # Rearrange columns to make ChannelID the first column
    new_entries_df = new_entries_df.select("ChannelID", "Channel")
    
    # Append the new entries to the existing DimChannel table
    combined_dim_df = existing_dim_df.union(new_entries_df)
    combined_dim_df.write.format("delta").mode("overwrite").save(dest_file_path)
    
    print(f"New Channel appended to: {dest_file_path}")
    
except:
    # If the destination table does not exist, create it with new entries
    window_spec = Window.orderBy("Channel")
    channel_dimension = distinct_channel.withColumn("ChannelID", row_number().over(window_spec))
    channel_dimension = channel_dimension.select("ChannelID", "Channel")
    channel_dimension.write.format("delta").mode("overwrite").save(dest_file_path)
    
    print(f"Channel Dimension created at: {dest_file_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
