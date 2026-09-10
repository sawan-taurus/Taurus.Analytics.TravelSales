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

file = 'New'
SelectedDate = '2025-04-28'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if file == 'Direct':
    travel_brand_expr = "'Direct'"
    additional_filter = """
    AND (SchemeName LIKE 'Direct%' OR SchemeName LIKE '%Renewal%')
    """
elif file == 'Renewal':
    travel_brand_expr = "'Renewal'"
    additional_filter = """
    AND SchemeName LIKE '%Renewal%'
    """
elif file == 'New':
    travel_brand_expr = "'New'"
    additional_filter = """
    AND (SchemeName LIKE 'Direct%' AND SchemeName NOT LIKE '%Renewal%')
    """
else:
    travel_brand_expr = """
    CASE
        WHEN LEFT(PolicyNumber, 3) IN ('SOI','SOM') THEN 'switched_on'
        WHEN LEFT(PolicyNumber, 3) IN ('STA','STM') THEN 'start_travel'
        WHEN LEFT(PolicyNumber, 3) IN ('TC-') THEN 'thomas_cook'
        WHEN LEFT(PolicyNumber, 3) IN ('OI-','OIM') THEN 'oasis_travel'
        WHEN LEFT(PolicyNumber, 3) IN ('TRU','TRM') THEN 'trusted_ins'
        WHEN LEFT(PolicyNumber, 3) IN ('VIV','VIM') THEN 'viva_ins'
        WHEN LEFT(PolicyNumber, 3) IN ('ATO','Ato') THEN 'atoz_travel'
        ELSE 'UNKNOWN'
    END
    """
    additional_filter = ""

query = f"""
WITH MAGENTA_TRAVEL_BRAND AS 
(
    SELECT 
        {travel_brand_expr} AS travel_brand,
        CAST(TransactionDate AS DATE) AS Date,
        COUNT(*) AS Volume, 
        SUM(TotalGrossIncIPT) AS GWPincIPTDay, 
        SUM(TotalGrossExcIPT) AS GWPexIPTDay
    FROM tblTravelSalesTransactions_1
    WHERE 
         TransactionDate >= DATE('{SelectedDate}')
        AND TransactionDate < DATE_ADD(DATE('{SelectedDate}'), 1)
        AND TransactionType = 'New Issue'
        {additional_filter}
    GROUP BY
        {travel_brand_expr},
        CAST(TransactionDate AS DATE)
),
MONTHLY_TOTALS AS 
(
    SELECT 
        {travel_brand_expr} AS travel_brand,
        COUNT(*) AS VolumeMonth,
        SUM(TotalGrossIncIPT) AS TotalGrossIncIPTMonth,
        SUM(TotalGrossExcIPT) AS TotalGrossExcIPTMonth
    FROM tblTravelSalesTransactions_1
    WHERE 
         TransactionDate >= DATE_TRUNC('MONTH', DATE('{SelectedDate}'))
        AND TransactionDate <= DATE('{SelectedDate}')
        AND TransactionType = 'New Issue'
        {additional_filter}
    GROUP BY
        {travel_brand_expr}
)
SELECT 
    daily.travel_brand,
    daily.Date,
    daily.Volume,
    daily.GWPincIPTDay,
    daily.GWPexIPTDay,
    monthly.VolumeMonth,
    monthly.TotalGrossIncIPTMonth,
    monthly.TotalGrossExcIPTMonth
FROM MAGENTA_TRAVEL_BRAND daily
LEFT JOIN MONTHLY_TOTALS monthly
    ON daily.travel_brand = monthly.travel_brand
where daily.travel_brand <> 'UNKNOWN'
"""
# Now run this query
df = spark.sql(query)
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
