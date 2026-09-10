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
# META           "id": "d69a91e4-0efe-4e5a-81c6-43b1d5fab183"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE SelectedDate (
# MAGIC     selecteddate VARCHAR(255)
# MAGIC );


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC delete from SelectedDate 
# MAGIC where selecteddate not in ( '2025-04-19')
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from SelectedDate 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC INSERT INTO SelectedDate (selecteddate)
# MAGIC VALUES 
# MAGIC ('2025-05-27'),('2025-05-28')  --3hrs--3-6
# MAGIC ,('2025-05-29'),('2025-05-30'),('2025-05-31') --3hrs--6-9pm
# MAGIC 
# MAGIC --at 9pm today 23rd June, change the variable to -1 day. 
# MAGIC --check that it wil run tomorrow morning
# MAGIC --then check that pbi report will send report?


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from TaurusGoldLH.DimDate 
# MAGIC where left(DateId,6) in (202505) 
# MAGIC and  DateId not in (SELECT distinct DateId FROM TaurusGoldLH.FactDailySalesReport
# MAGIC where left(DateId,6) in (202505,202506))


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
