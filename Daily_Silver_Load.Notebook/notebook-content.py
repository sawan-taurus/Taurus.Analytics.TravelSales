# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dbfc334d-f83f-43fc-965f-0a38145f4700",
# META       "default_lakehouse_name": "TaurusSilverLH",
# META       "default_lakehouse_workspace_id": "9c958852-649b-48e2-ac6a-f03167ff64dc",
# META       "known_lakehouses": [
# META         {
# META           "id": "dbfc334d-f83f-43fc-965f-0a38145f4700"
# META         },
# META         {
# META           "id": "3b2d8490-dd7d-4046-a7c8-21da791cfcdd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

WORKSPACE = f"Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = f"TaurusBronzeLH"
SRC_TABLE = f"Daily"

TRG_LAKEHOUSE = f"TaurusSilverLH"
TRG_SILVER_TABLE = f"Daily"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_silver_layer(src_file_path: str, dest_file_path: str):
    # Load the Bronze table (read the schema from the source file dynamically)
    bronze_df = spark.read.format("Delta").load(src_file_path)

    # Automatically infer the schema from the loaded DataFrame (no need to define manually)
    silver_df = bronze_df

    # Show the schema of the loaded Bronze table (optional for verification)
    bronze_df.printSchema()

    # Optionally, transform or clean the data here if needed (e.g., renaming columns, converting datatypes)

    # Write the Silver table to the destination
    silver_df.write.format("Delta").mode("overwrite").save(dest_file_path)

# Example Usage
src_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{SRC_TABLE}"
dest_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{TRG_LAKEHOUSE}.Lakehouse/Tables/{TRG_SILVER_TABLE}"

create_silver_layer(src_file_path, dest_file_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# bronze_df = spark.read.format("Delta").load(f"abfss://{SRC_WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{SRC_TABLE}")                      


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
