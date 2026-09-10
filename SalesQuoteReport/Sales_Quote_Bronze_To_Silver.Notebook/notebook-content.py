# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3b2d8490-dd7d-4046-a7c8-21da791cfcdd",
# META       "default_lakehouse_name": "TaurusBronzeLH",
# META       "default_lakehouse_workspace_id": "9c958852-649b-48e2-ac6a-f03167ff64dc",
# META       "known_lakehouses": [
# META         {
# META           "id": "3b2d8490-dd7d-4046-a7c8-21da791cfcdd"
# META         },
# META         {
# META           "id": "dbfc334d-f83f-43fc-965f-0a38145f4700"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

tblTravelSalesTransactions - oasis
tblTravelSalesTransactions_1 - sales 
tblTravelSalesTransactions_2 - atoz



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set your database name
database_name = "TaurusSilverLH"

# Switch to the database
spark.catalog.setCurrentDatabase(database_name)

# List all tables in the database
tables = spark.catalog.listTables()

# Filter tables that start with 'silver_'
silver_tables = [table.name for table in tables if table.name.startswith("silver_")]

# Drop each table
for table_name in silver_tables:
    spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table_name}")
    print(f"Dropped table: {database_name}.{table_name}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parameters for manual run
# Source_Type = "Sales"
# Source_Table_Name = "tblTravelEnquirySales"
# Load_Type = "Incremental Load" 
# Date_Column = "quote_date"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re

if Date_Column:
    # Convert camelCase/PascalCase to snake_case
    Date_Column = re.sub(r'(?<!^)(?=[A-Z])', '_', Date_Column).lower()
    print(Date_Column)
else:
    print("Date_Column is null or empty" + Date_Column)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run ./Silver_Tables_Schemas


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_date

def process_bronze_to_silver(df_bronze, silver_table: str, date_column: str):
    """
    Deletes existing dates from silver_table and appends df_bronze.

    Args:
        df_bronze: Bronze DataFrame.
        silver_table: Name of the silver table in metastore.
        date_column: Name of the date column in both tables.
    """

    # Step 1: Extract distinct dates from bronze
    quote_dates = (
        df_bronze
        .select(to_date(date_column).alias(date_column))
        .distinct()
        .rdd
        .flatMap(lambda x: x)
        .collect()
    )

    print(f"Bronze dates: {quote_dates}")

    if not quote_dates:
        print("No dates found in bronze to process.")
        return

    # Step 2: Format dates for SQL
    date_list_str = ",".join([f"DATE '{str(date)}'" for date in quote_dates])
    print(f"Formatted date list: {date_list_str}")

    # Step 3: Delete from silver
    sql = f"""
        DELETE FROM {silver_table}
        WHERE to_date({date_column}) IN ({date_list_str})
    """
    spark.sql(sql)

    print(f"✅ Deleted records for dates: {quote_dates} from table: {silver_table}")

    # Step 4: Append bronze to silver
    df_bronze.write.format("delta").mode("append").saveAsTable(silver_table)

    print(f"✅ Appended bronze data to silver table: {silver_table}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
import re

# Start Spark session
spark = SparkSession.builder.appName("BronzeToSilverETL").getOrCreate()

Bronze_Lakehouse = "TaurusBronzeLH"
Silver_Lakehouse = "TaurusSilverLH"

# Dynamically resolve rename map
# Extract base name from Source_Table_Name (e.g., "tblSchemesSales" → "tblSchemes")
base_table_name = re.sub(rf"{Source_Type}$", "", Source_Table_Name)
rename_map_var = f"column_rename_map_{base_table_name}"
rename_map = globals().get(rename_map_var)
print(rename_map_var)
if not rename_map:
    raise ValueError(f"❌ Rename map not found for table: {base_table_name}{Source_Type}")

# Format table names

stripped_name = Source_Table_Name[3:]
formatted_name = re.sub(r'(?<!^)(?=[A-Z])', '_', stripped_name).lower()
source_table = f"TaurusBronzeLH.{Source_Table_Name}"
bronze_table = f"bronze_{formatted_name}"

stripped = Source_Table_Name[3:]  # Remove 'tbl' prefix
formatted = re.sub(r'(?<!^)(?=[A-Z])', '_', stripped).lower()
bronze_table = f"{Bronze_Lakehouse}.bronze_{formatted_name}"
silver_table = f"{Silver_Lakehouse}.silver_{formatted}"

# Load and transform
print(f"\n🔄 Processing: {bronze_table} → {silver_table} | Load Type: {Load_Type}")
df_bronze = spark.read.table(bronze_table)

# Rename columns
for old_col, new_col in rename_map.items():
    if old_col in df_bronze.columns:
        df_bronze = df_bronze.withColumnRenamed(old_col, new_col)

# Add metadata
df_bronze = df_bronze.withColumn("ingestion_timestamp", current_timestamp())

if Load_Type.lower() == "full load":
    if spark.catalog.tableExists(silver_table):
        print("♻️ Full load — overwriting existing silver table")
    else:
        print("🆕 Silver table does not exist — creating it")
    df_bronze.write.format("delta").mode("overwrite").saveAsTable(silver_table)
else:
    print("📥 Incremental load — appending to silver table")
    process_bronze_to_silver(df_bronze,silver_table,Date_Column)
    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
