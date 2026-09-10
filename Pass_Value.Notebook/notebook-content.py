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

import json


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run the SQL query using Spark
df = spark.sql("""
    SELECT MAX(TravelSalesTransactionID) AS max_id
    FROM TaurusBronzeLH.bronze_travel_sales_transactions_sales
""")

# Collect the result into a Python variable
max_id = df.collect()[0]['max_id']

# Print or use the variable
print(f"The max TravelSalesTransactionID is: {max_id}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

LastProcessId = max_id
DataLoadType = 'historical'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

LastProcessId = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if DataLoadType and DataLoadType == 'historical':
    DataLoadType == 'yesterday'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

result = {
    "LastProcessId": LastProcessId,
    "DataLoadType": DataLoadType
}

# Convert to JSON string, ensuring None becomes null
json_result = json.dumps(result)

mssparkutils.notebook.exit(json_result)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import json

# # Example values
# New_Last_Process_Id = None  # or some value
# Data_Load_Type = "Full"     # or None

# # Build the result dictionary
# result = {
#     "LastProcessId": New_Last_Process_Id,
#     "DataLoadType": Data_Load_Type
# }

# # Optional: log or handle the None case
# if New_Last_Process_Id is None:
#     print("New_Last_Process_Id is None, setting it as null in JSON.")

# # Convert to JSON string
# json_result = json.dumps(result)

# # Exit the notebook with the JSON result
# mssparkutils.notebook.exit(json_result)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
