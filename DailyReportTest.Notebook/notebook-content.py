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
# MAGIC 
# MAGIC select * from tblTravelSalesTransactions_1
# MAGIC where CAST(TransactionDate AS DATE) = '2025-09-17'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC SELECT 
# MAGIC         CASE
# MAGIC             WHEN LEFT(PolicyNumber, 3) IN ('SOI','SOM') THEN 'switched_on'
# MAGIC             WHEN LEFT(PolicyNumber, 3) IN ('STA','STM') THEN 'start_travel'
# MAGIC             WHEN LEFT(PolicyNumber, 3) IN ('TC-') THEN 'thomas_cook'
# MAGIC             WHEN LEFT(PolicyNumber, 3) IN ('OI-','OIM') THEN 'oasis_travel'
# MAGIC             WHEN LEFT(PolicyNumber, 3) IN ('TRU','TRM') THEN 'trusted_ins'
# MAGIC             WHEN LEFT(PolicyNumber, 3) IN ('VIV','VIM') THEN 'viva_ins'
# MAGIC             WHEN LEFT(PolicyNumber, 3) IN ('ATO','Ato') THEN 'atoz_travel'
# MAGIC         ELSE 'UNKNOWN'
# MAGIC         END travel_brand,
# MAGIC         CAST(TransactionDate AS DATE) AS Date,
# MAGIC         -- SUM(CASE WHEN TransactionType = 'New Issue' THEN 1 
# MAGIC         --          WHEN TransactionType = 'Cancellation' THEN -1 ELSE 0 END) AS Volume
# MAGIC         COUNT(*) AS Volume
# MAGIC     FROM  tblTravelSalesTransactions
# MAGIC     WHERE 
# MAGIC         CAST(TransactionDate AS DATE) = '2025-09-17'
# MAGIC     GROUP BY
# MAGIC         CASE
# MAGIC             WHEN LEFT(PolicyNumber, 3) IN ('SOI','SOM') THEN 'switched_on'
# MAGIC             WHEN LEFT(PolicyNumber, 3) IN ('STA','STM') THEN 'start_travel'
# MAGIC             WHEN LEFT(PolicyNumber, 3) IN ('TC-') THEN 'thomas_cook'
# MAGIC             WHEN LEFT(PolicyNumber, 3) IN ('OI-','OIM') THEN 'oasis_travel'
# MAGIC             WHEN LEFT(PolicyNumber, 3) IN ('TRU','TRM') THEN 'trusted_ins'
# MAGIC             WHEN LEFT(PolicyNumber, 3) IN ('VIV','VIM') THEN 'viva_ins'
# MAGIC             WHEN LEFT(PolicyNumber, 3) IN ('ATO','Ato') THEN 'atoz_travel'
# MAGIC         ELSE 'UNKNOWN'
# MAGIC         END,
# MAGIC         CAST(TransactionDate AS DATE)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from TaurusBronzeLH.dailyviva
# MAGIC union all 
# MAGIC select * from TaurusBronzeLH.dailytrusted
# MAGIC union all 
# MAGIC select * from TaurusBronzeLH.dailysoi
# MAGIC union all 
# MAGIC select * from TaurusBronzeLH.dailyoasis
# MAGIC union all 
# MAGIC select * from TaurusBronzeLH.dailyatoz


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
