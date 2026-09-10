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

# PARAMETERS CELL ********************

# File_Type = 'SalesScheme'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StringType, DateType, DoubleType, StructType, IntegerType, StructField

WORKSPACE = f"Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = f"TaurusSilverLH"
SRC_TABLE = File_Type

TRG_LAKEHOUSE = f"TaurusGoldLH"
TRG_SILVER_TABLE = File_Type

column_mappings_forecast_summary_2025 = {
    "Brand": ("Brand", StringType()),
    "Type": ("Type", StringType()),
    "2024est": ("2024est", IntegerType()),
    "2025": ("2025", IntegerType()),
    "2026": ("2026", IntegerType()),
    "2027": ("2027", IntegerType()),
    "Jan-25": ("Jan-25", IntegerType()),
    "Feb-25": ("Feb-25", IntegerType()),
    "Mar-25": ("Mar-25", IntegerType()),
    "Apr-25": ("Apr-25", IntegerType()),
    "May-25": ("May-25", IntegerType()),
    "Jun-25": ("Jun-25", IntegerType()),
    "Jul-25": ("Jul-25", IntegerType()),
    "Aug-25": ("Aug-25", IntegerType()),
    "Sep-25": ("Sep-25", IntegerType()),
    "Oct-25": ("Oct-25", IntegerType()),
    "Nov-25": ("Nov-25", IntegerType()),
    "Dec-25": ("Dec-25", IntegerType()),
}

column_mappings_soi_forecast_summary_2025 = {
    "Brand": ("Brand", StringType()),
    "Jan-25": ("Jan-25", DoubleType()),
    "Feb-25": ("Feb-25", DoubleType()),
    "Mar-25": ("Mar-25", DoubleType()),
    "Apr-25": ("Apr-25", DoubleType()),
    "May-25": ("May-25", DoubleType()),
    "Jun-25": ("Jun-25", DoubleType()),
    "Jul-25": ("Jul-25", DoubleType()),
    "Aug-25": ("Aug-25", DoubleType()),
    "Sep-25": ("Sep-25", DoubleType()),
    "Oct-25": ("Oct-25", DoubleType()),
    "Nov-25": ("Nov-25", DoubleType()),
    "Dec-25": ("Dec-25", DoubleType()),
}

column_mappings_new_soi_forecast_summary_2025 = {
    "Type": ("Brand", StringType()),
    "2025": ("2025", IntegerType()),
    "2026": ("2026", IntegerType()),
    "2027": ("2027", IntegerType()),
    "Jan-25": ("Jan-25", DoubleType()),
    "Feb-25": ("Feb-25", DoubleType()),
    "Mar-25": ("Mar-25", DoubleType()),
    "Apr-25": ("Apr-25", DoubleType()),
    "May-25": ("May-25", DoubleType()),
    "Jun-25": ("Jun-25", DoubleType()),
    "Jul-25": ("Jul-25", DoubleType()),
    "Aug-25": ("Aug-25", DoubleType()),
    "Sep-25": ("Sep-25", DoubleType()),
    "Oct-25": ("Oct-25", DoubleType()),
    "Nov-25": ("Nov-25", DoubleType()),
    "Dec-25": ("Dec-25", DoubleType()),
    "Jan-26": ("Jan-26", DoubleType()),
    "Feb-26": ("Feb-26", DoubleType()),
    "Mar-26": ("Mar-26", DoubleType()),
    "Apr-26": ("Apr-26", DoubleType()),
    "May-26": ("May-26", DoubleType()),
    "Jun-26": ("Jun-26", DoubleType()),
    "Jul-26": ("Jul-26", DoubleType()),
    "Aug-26": ("Aug-26", DoubleType()),
    "Sep-26": ("Sep-26", DoubleType()),
    "Oct-26": ("Oct-26", DoubleType()),
    "Nov-26": ("Nov-26", DoubleType()),
    "Dec-26": ("Dec-26", DoubleType()),

}

column_mappings_atoz_forecast_summary_2025 = {
    "Type": ("Brand", StringType()),
    "2025": ("2025", IntegerType()),
    "2026": ("2026", IntegerType()),
    "2027": ("2027", IntegerType()),
    "Jan-25": ("Jan-25", DoubleType()),
    "Feb-25": ("Feb-25", DoubleType()),
    "Mar-25": ("Mar-25", DoubleType()),
    "Apr-25": ("Apr-25", DoubleType()),
    "May-25": ("May-25", DoubleType()),
    "Jun-25": ("Jun-25", DoubleType()),
    "Jul-25": ("Jul-25", DoubleType()),
    "Aug-25": ("Aug-25", DoubleType()),
    "Sep-25": ("Sep-25", DoubleType()),
    "Oct-25": ("Oct-25", DoubleType()),
    "Nov-25": ("Nov-25", DoubleType()),
    "Dec-25": ("Dec-25", DoubleType()),
    "Jan-26": ("Jan-26", DoubleType()),
    "Feb-26": ("Feb-26", DoubleType()),
    "Mar-26": ("Mar-26", DoubleType()),
    "Apr-26": ("Apr-26", DoubleType()),
    "May-26": ("May-26", DoubleType()),
    "Jun-26": ("Jun-26", DoubleType()),
    "Jul-26": ("Jul-26", DoubleType()),
    "Aug-26": ("Aug-26", DoubleType()),
    "Sep-26": ("Sep-26", DoubleType()),
    "Oct-26": ("Oct-26", DoubleType()),
    "Nov-26": ("Nov-26", DoubleType()),
    "Dec-26": ("Dec-26", DoubleType()),
}

column_mappings_oasis_forecast_summary_2025 = {
    "Type": ("Brand", StringType()),
    "2025": ("2025", IntegerType()),
    "2026": ("2026", IntegerType()),
    "2027": ("2027", IntegerType()),
    "Jan-25": ("Jan-25", DoubleType()),
    "Feb-25": ("Feb-25", DoubleType()),
    "Mar-25": ("Mar-25", DoubleType()),
    "Apr-25": ("Apr-25", DoubleType()),
    "May-25": ("May-25", DoubleType()),
    "Jun-25": ("Jun-25", DoubleType()),
    "Jul-25": ("Jul-25", DoubleType()),
    "Aug-25": ("Aug-25", DoubleType()),
    "Sep-25": ("Sep-25", DoubleType()),
    "Oct-25": ("Oct-25", DoubleType()),
    "Nov-25": ("Nov-25", DoubleType()),
    "Dec-25": ("Dec-25", DoubleType()),
    "Jan-26": ("Jan-26", DoubleType()),
    "Feb-26": ("Feb-26", DoubleType()),
    "Mar-26": ("Mar-26", DoubleType()),
    "Apr-26": ("Apr-26", DoubleType()),
    "May-26": ("May-26", DoubleType()),
    "Jun-26": ("Jun-26", DoubleType()),
    "Jul-26": ("Jul-26", DoubleType()),
    "Aug-26": ("Aug-26", DoubleType()),
    "Sep-26": ("Sep-26", DoubleType()),
    "Oct-26": ("Oct-26", DoubleType()),
    "Nov-26": ("Nov-26", DoubleType()),
    "Dec-26": ("Dec-26", DoubleType()),
}

column_mappings_trusted_forecast_summary_2025 = {
    "Type": ("Brand", StringType()),
    "2025": ("2025", IntegerType()),
    "2026": ("2026", IntegerType()),
    "2027": ("2027", IntegerType()),
    "Jan-25": ("Jan-25", DoubleType()),
    "Feb-25": ("Feb-25", DoubleType()),
    "Mar-25": ("Mar-25", DoubleType()),
    "Apr-25": ("Apr-25", DoubleType()),
    "May-25": ("May-25", DoubleType()),
    "Jun-25": ("Jun-25", DoubleType()),
    "Jul-25": ("Jul-25", DoubleType()),
    "Aug-25": ("Aug-25", DoubleType()),
    "Sep-25": ("Sep-25", DoubleType()),
    "Oct-25": ("Oct-25", DoubleType()),
    "Nov-25": ("Nov-25", DoubleType()),
    "Dec-25": ("Dec-25", DoubleType()),
    "Jan-26": ("Jan-26", DoubleType()),
    "Feb-26": ("Feb-26", DoubleType()),
    "Mar-26": ("Mar-26", DoubleType()),
    "Apr-26": ("Apr-26", DoubleType()),
    "May-26": ("May-26", DoubleType()),
    "Jun-26": ("Jun-26", DoubleType()),
    "Jul-26": ("Jul-26", DoubleType()),
    "Aug-26": ("Aug-26", DoubleType()),
    "Sep-26": ("Sep-26", DoubleType()),
    "Oct-26": ("Oct-26", DoubleType()),
    "Nov-26": ("Nov-26", DoubleType()),
    "Dec-26": ("Dec-26", DoubleType()),
}

column_mappings_viva_forecast_summary_2025 = {
    "Type": ("Brand", StringType()),
    "2025": ("2025", IntegerType()),
    "2026": ("2026", IntegerType()),
    "2027": ("2027", IntegerType()),
    "Jan-25": ("Jan-25", DoubleType()),
    "Feb-25": ("Feb-25", DoubleType()),
    "Mar-25": ("Mar-25", DoubleType()),
    "Apr-25": ("Apr-25", DoubleType()),
    "May-25": ("May-25", DoubleType()),
    "Jun-25": ("Jun-25", DoubleType()),
    "Jul-25": ("Jul-25", DoubleType()),
    "Aug-25": ("Aug-25", DoubleType()),
    "Sep-25": ("Sep-25", DoubleType()),
    "Oct-25": ("Oct-25", DoubleType()),
    "Nov-25": ("Nov-25", DoubleType()),
    "Dec-25": ("Dec-25", DoubleType()),
    "Jan-26": ("Jan-26", DoubleType()),
    "Feb-26": ("Feb-26", DoubleType()),
    "Mar-26": ("Mar-26", DoubleType()),
    "Apr-26": ("Apr-26", DoubleType()),
    "May-26": ("May-26", DoubleType()),
    "Jun-26": ("Jun-26", DoubleType()),
    "Jul-26": ("Jul-26", DoubleType()),
    "Aug-26": ("Aug-26", DoubleType()),
    "Sep-26": ("Sep-26", DoubleType()),
    "Oct-26": ("Oct-26", DoubleType()),
    "Nov-26": ("Nov-26", DoubleType()),
    "Dec-26": ("Dec-26", DoubleType()),
}


column_mappings_oasisscheme = {
    "SchemeName": ("SchemeName", StringType()),
    "Channel": ("Channel", StringType()),
}

column_mappings_atozscheme = {
    "SchemeName": ("SchemeName", StringType()),
    "Channel": ("Channel", StringType()),
}

column_mappings_salesscheme = {
    "SchemeName": ("SchemeName", StringType()),
    "SalesType": ("SalesType", StringType()),
    "Channel": ("Channel", StringType()),
}

# Mapping the source table to the appropriate column mappings
mapping_files = {
    "forecast_summary_2025_source_file": column_mappings_forecast_summary_2025,
    "soi_forecast_summary_2025_source_file": column_mappings_soi_forecast_summary_2025,
    "SOI2025_Budget_Summary": column_mappings_new_soi_forecast_summary_2025,
    "ViVA2025_Budget_Summary": column_mappings_viva_forecast_summary_2025,
    "Trusted2025_Budget_Summary": column_mappings_trusted_forecast_summary_2025,
    "Oasis2025_Budget_Summary": column_mappings_oasis_forecast_summary_2025,
    "AtoZ2025_Budget_Summary": column_mappings_atoz_forecast_summary_2025,
    "OasisScheme": column_mappings_oasisscheme,
    "AtozScheme": column_mappings_atozscheme,
    "SalesScheme": column_mappings_salesscheme,
}

# Function to dynamically pick the column mappings based on the source file
def get_column_mappings(SRC_TABLE):
    # Logic to determine the key for the source file
    if SRC_TABLE == 'ForecastSummary2025':
        return column_mappings_forecast_summary_2025
    if SRC_TABLE == 'SOIForecastSummary2025':
        return column_mappings_soi_forecast_summary_2025
    if SRC_TABLE == 'SOI2025_Budget_Summary':
        return column_mappings_new_soi_forecast_summary_2025
    if SRC_TABLE == 'ViVA2025_Budget_Summary':
        return column_mappings_viva_forecast_summary_2025
    if SRC_TABLE == 'Trusted2025_Budget_Summary':
        return column_mappings_trusted_forecast_summary_2025
    if SRC_TABLE == 'Oasis2025_Budget_Summary':
        return column_mappings_oasis_forecast_summary_2025
    if SRC_TABLE == 'AtoZ2025_Budget_Summary':
        return column_mappings_atoz_forecast_summary_2025
    if SRC_TABLE == 'OasisScheme':
        return column_mappings_oasisscheme
    if SRC_TABLE == 'AtozScheme':
        return column_mappings_atozscheme
    if SRC_TABLE == 'SalesScheme':
        return column_mappings_salesscheme
    else:
        raise ValueError(f"Mapping not defined for source file: {src_file_path}")

def create_silver_layer(src_file_path: str, dest_file_path: str, column_mappings: dict):
    # Read the Bronze Delta table
    bronze_df = spark.read.format("Delta").load(src_file_path)

    # Write the updated Silver Delta table back to the destination path in append mode
    bronze_df.write.format("Delta").mode("overwrite").save(dest_file_path)

    # Print schema for debugging purposes
    bronze_df.printSchema()

# Example Usage
src_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{SRC_TABLE}"
dest_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{TRG_LAKEHOUSE}.Lakehouse/Tables/{TRG_SILVER_TABLE}"

# Dynamically fetch the column mappings based on the source file
column_mappings = get_column_mappings(SRC_TABLE)

create_silver_layer(src_file_path, dest_file_path, column_mappings)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
