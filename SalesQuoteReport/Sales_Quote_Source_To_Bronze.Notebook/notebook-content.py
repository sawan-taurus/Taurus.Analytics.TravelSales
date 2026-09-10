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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Source_Type = "Sales"
# Source_Table_Name = "tblTravelEnquiryOptionsAtoz"
# Load_Type = "Incremental Load"
# Data_Load_Type = None
# Watermark_Column = "TravelEnquiryOptionID"
# Last_Process_Id = 0
# Source_Date_Column = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

New_Last_Process_Id = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_sub, current_date

# Start Spark session
spark = SparkSession.builder.appName("Dynamic_ETL").getOrCreate()

# Format table names
stripped_name = Source_Table_Name[3:]
formatted_name = re.sub(r'(?<!^)(?=[A-Z])', '_', stripped_name).lower()
source_table = f"TaurusBronzeLH.{Source_Table_Name}"
bronze_table = f"bronze_{formatted_name}"

print(f"🔗 Source Table: {source_table}")
print(f"🔽 Bronze Table: {bronze_table}")
print(f"🧠 Load Type: {Load_Type} — Data Load Type (Historical/Yesterday): {Data_Load_Type}")

# Read source
df = spark.read.table(source_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, to_date, date_sub, current_date

# Apply filtering based on load type, watermark, and source_date_column
if Load_Type and Load_Type.lower() == "incremental load":
    if Watermark_Column:
        print(f"🧭 Incremental load using watermark column: {Watermark_Column}")
        yesterday = date_sub(current_date(), 1)

        if Source_Date_Column:
            print(f"📅 Applying both watermark and date column ({Source_Date_Column}) filter till yesterday")
            df = (
                df.filter(
                    (col(Watermark_Column) > Last_Process_Id) &
                    (to_date(col(Source_Date_Column)) < current_date())
                )
            )
        else:
            print(f"⚠️ No Source_Date_Column provided — applying only watermark filter")
            df = (
                df.filter(
                    col(Watermark_Column) > Last_Process_Id
                )
            )

        if df.rdd.isEmpty():
            print("⚠️ No new records found after filtering — retaining previous Last_Process_Id")
            New_Last_Process_Id = Last_Process_Id
        else:
            max_last_process_id = df.agg({Watermark_Column: "max"}).collect()[0][0]
            New_Last_Process_Id = max_last_process_id
            print(f"📌 New Max Last_Process_Id: {max_last_process_id}")
    else:
        print("⚠️ Incremental load requested but watermark column is null — applying fallback logic")
        if Data_Load_Type and Data_Load_Type.lower() == "yesterday":
            if Source_Date_Column:
                print(f"📅 Filtering for previous day's transactions using {Source_Date_Column} (no watermark column)")
                yesterday = date_sub(current_date(), 1)
                df = (
                    df.withColumn("SourceDateOnly", to_date(col(Source_Date_Column)))
                      .filter(col("SourceDateOnly") == yesterday)
                      .drop("SourceDateOnly")
                )
            else:
                print("⚠️ No Source_Date_Column provided — cannot filter for yesterday")
        else:
            print("🕰️ Loading all available historical incremental data (no watermark column)")

elif Data_Load_Type and Data_Load_Type.lower() == "historical":
    print("📜 Historical load — filtering data till yesterday")
    if Source_Date_Column:
        df = df.filter(to_date(col(Source_Date_Column)) < current_date())
    else:
        print("⚠️ No Source_Date_Column provided — skipping date filter for historical load")
else:
    print("🔄 Performing full load")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if Data_Load_Type and Data_Load_Type.lower() == "historical":
    print("🔁 Switching Data_Load_Type from 'historical' to 'yesterday' for next run")
    Data_Load_Type = "yesterday"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


if Source_Table_Name in ["tblTravelEnquiryPersonsSales", "tblTravelEnquiryPersonsOasis", "tblTravelEnquiryPersonsAtoz"]:
    df = df.withColumn("PersonDOB", col("PersonDOB").cast("string"))
    print("PersonDOB to String")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Write to bronze layer
df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
print(f"✅ Data successfully loaded into: {bronze_table}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = {
    "LastProcessId": New_Last_Process_Id,
    "DataLoadType": Data_Load_Type
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# mssparkutils.notebook.exit(str(result))
# Convert to JSON string, ensuring None becomes null
json_result = json.dumps(result)

mssparkutils.notebook.exit(json_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
