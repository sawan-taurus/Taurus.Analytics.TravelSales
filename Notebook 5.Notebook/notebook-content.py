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

# MAGIC %%sql 
# MAGIC delete from FactDailySales 
# MAGIC where DateId = '20250904'


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
