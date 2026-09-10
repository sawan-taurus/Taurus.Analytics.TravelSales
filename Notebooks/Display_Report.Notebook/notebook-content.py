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

# CELL ********************

from pyspark.sql import SparkSession

# Set your report date
report_date = '2025-04-23'

# Correct path to file in Fabric Lakehouse
# Note: "/lakehouse/default/Files/Reports/" is the right way to access Files folder
html_path = f"/Files/Reports/DailySales_{report_date}.html"

# Use Spark to read file line-by-line
html_df = spark.read.text(html_path)

# Now collect the content into one string
html_content = "\n".join(row['value'] for row in html_df.collect())

# Confirm the content
print(html_content[:500])  # preview first 500 characters


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
