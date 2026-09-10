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
# META           "id": "d69a91e4-0efe-4e5a-81c6-43b1d5fab183"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     count(*)
# MAGIC FROM TaurusSilverLH.silver_travel_enquiry_persons_sales p
# MAGIC LEFT JOIN TaurusSilverLH.silver_travel_enquiry_sales e
# MAGIC     USING (TravelEnquiryID)
# MAGIC WHERE e.AgentName LIKE '%Paying%'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     COUNT(*)                                             AS TravellerRows,
# MAGIC     COUNT(DISTINCT TravelEnquiryID)                      AS DistinctEnquiries,
# MAGIC     SUM(CASE WHEN p.PersonDOB IS NULL THEN 1 ELSE 0 END) AS MissingDOB,
# MAGIC     SUM(CASE WHEN e.StartDate  IS NULL THEN 1 ELSE 0 END) AS MissingStartDate,
# MAGIC     SUM(CASE WHEN FLOOR(MONTHS_BETWEEN(e.StartDate, p.PersonDOB) / 12) < 0
# MAGIC              THEN 1 ELSE 0 END)                          AS NegativeAge
# MAGIC FROM TaurusSilverLH.silver_travel_enquiry_persons_sales p
# MAGIC LEFT JOIN TaurusSilverLH.silver_travel_enquiry_sales e
# MAGIC     USING (TravelEnquiryID)
# MAGIC WHERE e.AgentName LIKE '%Paying%'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC WITH joined AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         FLOOR(MONTHS_BETWEEN(e.StartDate, p.PersonDOB) / 12) AS AgeAtEnquiryStart
# MAGIC     FROM TaurusSilverLH.silver_travel_enquiry_persons_sales p
# MAGIC     LEFT JOIN TaurusSilverLH.silver_travel_enquiry_sales e
# MAGIC         USING (TravelEnquiryID)
# MAGIC )
# MAGIC SELECT
# MAGIC     *,
# MAGIC     MAX(AgeAtEnquiryStart) OVER (PARTITION BY TravelEnquiryID) AS MaxAgeAtEnquiryStart,
# MAGIC     CASE
# MAGIC         WHEN MAX(AgeAtEnquiryStart) OVER (PARTITION BY TravelEnquiryID) BETWEEN 0  AND 20 THEN '0 to 20'
# MAGIC         WHEN MAX(AgeAtEnquiryStart) OVER (PARTITION BY TravelEnquiryID) BETWEEN 21 AND 30 THEN '21 to 30'
# MAGIC         WHEN MAX(AgeAtEnquiryStart) OVER (PARTITION BY TravelEnquiryID) BETWEEN 31 AND 40 THEN '31 to 40'
# MAGIC         WHEN MAX(AgeAtEnquiryStart) OVER (PARTITION BY TravelEnquiryID) BETWEEN 41 AND 50 THEN '41 to 50'
# MAGIC         WHEN MAX(AgeAtEnquiryStart) OVER (PARTITION BY TravelEnquiryID) BETWEEN 51 AND 65 THEN '51 to 65'
# MAGIC         WHEN MAX(AgeAtEnquiryStart) OVER (PARTITION BY TravelEnquiryID) BETWEEN 66 AND 75 THEN '66 to 75'
# MAGIC         WHEN MAX(AgeAtEnquiryStart) OVER (PARTITION BY TravelEnquiryID) >= 76             THEN '76 +'
# MAGIC         ELSE 'Invalid Age'
# MAGIC     END AS AgeGroup
# MAGIC FROM joined

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM TaurusSilverLH.silver_travel_enquiry_persons_sales LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run the query
df = spark.sql("""
WITH age_per_enquiry AS (
    SELECT
        p.TravelEnquiryID,
        MIN(p.PersonDOB)                                                 AS OldestTravellerDOB,
        COUNT(*)                                                         AS TravellersOnEnquiry,
        MAX(FLOOR(MONTHS_BETWEEN(e.StartDate, p.PersonDOB) / 12))        AS MaxAgeAtEnquiryStart
    FROM TaurusSilverLH.silver_travel_enquiry_persons_sales p
    LEFT JOIN TaurusSilverLH.silver_travel_enquiry_sales e
        ON p.TravelEnquiryID = e.TravelEnquiryID
    GROUP BY p.TravelEnquiryID
)
SELECT
    e.*,                                    -- every column from silver_travel_enquiry_sales
    a.OldestTravellerDOB,
    a.TravellersOnEnquiry,
    a.MaxAgeAtEnquiryStart,
    CASE
        WHEN a.MaxAgeAtEnquiryStart BETWEEN 0  AND 20 THEN '0 to 20'
        WHEN a.MaxAgeAtEnquiryStart BETWEEN 21 AND 30 THEN '21 to 30'
        WHEN a.MaxAgeAtEnquiryStart BETWEEN 31 AND 40 THEN '31 to 40'
        WHEN a.MaxAgeAtEnquiryStart BETWEEN 41 AND 50 THEN '41 to 50'
        WHEN a.MaxAgeAtEnquiryStart BETWEEN 51 AND 65 THEN '51 to 65'
        WHEN a.MaxAgeAtEnquiryStart BETWEEN 66 AND 75 THEN '66 to 75'
        WHEN a.MaxAgeAtEnquiryStart >= 76             THEN '76 +'
        ELSE 'Invalid Age'
    END AS AgeGroup
FROM TaurusSilverLH.silver_travel_enquiry_sales e
LEFT JOIN age_per_enquiry a
    ON e.TravelEnquiryID = a.TravelEnquiryID
WHERE e.AgentName LIKE '%Paying%'
""")

print(f"Total rows: {df.count()}")

# Write a single CSV into Files/QuoteData
import os

output_dir = "/lakehouse/default/Files/QuoteData"
os.makedirs(output_dir, exist_ok=True)

df.toPandas().to_csv(f"{output_dir}/travel_enquiry_sales_paying_with_age.csv", index=False)
print("Saved to Files/QuoteData/travel_enquiry_sales_paying_with_age.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from TaurusSilverLH.silver_travel_sales_transactions_sales
# MAGIC where AgentName like '%Paying%'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select *  from TaurusSilverLH.silver_travel_enquiry_sales
# MAGIC where agentname like '%Paying%'


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
