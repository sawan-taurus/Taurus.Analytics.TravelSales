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

SelectedDate = '2025-05-12'
SourceType = 'Sales'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if SourceType == "Sales":
    base_table = "tblTravelSalesTransactions_1"
elif SourceType == "Oasis":
    base_table = "tblTravelSalesTransactions"
elif SourceType == "Atoz":
    base_table = "tblTravelSalesTransactions_2"
elif SourceType == "Direct":
    base_table = "tblTravelSalesTransactions_1"
elif SourceType == "New":
    base_table = "tblTravelSalesTransactions_1"
elif SourceType == "Renewal":
    base_table = "tblTravelSalesTransactions_1"
else:
    raise ValueError(f"Unsupported SourceType: {SourceType}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set the selected date as a parameter (passed from pipeline or UI)| SelectedDate = '2025-04-22'  # You can make this dynamic using widgets or pipeline parameters

# Register the parameter as a Spark SQL variable
spark.conf.set("spark.sql.selected_date", SelectedDate)
spark.conf.set("spark.sql.source_type", SourceType)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table = "TaurusBronzeLH.Daily" + SourceType
selected_date = SelectedDate  # Assuming you’ve set this earlier


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# query = f"""
# WITH MAGENTA_TRAVEL_BRAND AS 
# (
#     SELECT 
#         CASE
#             WHEN LEFT(PolicyNumber, 3) IN ('SOI','SOM') THEN 'switched_on'
#             WHEN LEFT(PolicyNumber, 3) IN ('STA','STM') THEN 'start_travel'
#             WHEN LEFT(PolicyNumber, 3) IN ('TC-') THEN 'thomas_cook'
#             WHEN LEFT(PolicyNumber, 3) IN ('OI-','OIM') THEN 'oasis_travel'
#             WHEN LEFT(PolicyNumber, 3) IN ('TRU','TRM') THEN 'trusted_ins'
#             WHEN LEFT(PolicyNumber, 3) IN ('VIV','VIM') THEN 'viva_ins'
#             WHEN LEFT(PolicyNumber, 3) IN ('ATO','Ato') THEN 'atoz_travel'
#             ELSE 'UNKNOWN'
#         END AS travel_brand,
#         CAST(TransactionDate AS DATE) AS Date,
#         COUNT(*) AS Volume, 
#         SUM(TotalGrossIncIPT) AS GWPincIPTDay, 
#         SUM(TotalGrossExcIPT) AS GWPexIPTDay
#     FROM {base_table}
#     WHERE 
#         TransactionDate >= DATE('{SelectedDate}')
#         AND TransactionDate < DATE_ADD(DATE('{SelectedDate}'), 1)
#         AND TransactionType = 'New Issue'
#     GROUP BY
#         CASE
#             WHEN LEFT(PolicyNumber, 3) IN ('SOI','SOM') THEN 'switched_on'
#             WHEN LEFT(PolicyNumber, 3) IN ('STA','STM') THEN 'start_travel'
#             WHEN LEFT(PolicyNumber, 3) IN ('TC-') THEN 'thomas_cook'
#             WHEN LEFT(PolicyNumber, 3) IN ('OI-','OIM') THEN 'oasis_travel'
#             WHEN LEFT(PolicyNumber, 3) IN ('TRU','TRM') THEN 'trusted_ins'
#             WHEN LEFT(PolicyNumber, 3) IN ('VIV','VIM') THEN 'viva_ins'
#             WHEN LEFT(PolicyNumber, 3) IN ('ATO','Ato') THEN 'atoz_travel'
#             ELSE 'UNKNOWN'
#         END,
#         CAST(TransactionDate AS DATE)
# ),
# MONTHLY_TOTALS AS 
# (
#     SELECT 
#         CASE
#             WHEN LEFT(PolicyNumber, 3) IN ('SOI','SOM') THEN 'switched_on'
#             WHEN LEFT(PolicyNumber, 3) IN ('STA','STM') THEN 'start_travel'
#             WHEN LEFT(PolicyNumber, 3) IN ('TC-') THEN 'thomas_cook'
#             WHEN LEFT(PolicyNumber, 3) IN ('OI-','OIM') THEN 'oasis_travel'
#             WHEN LEFT(PolicyNumber, 3) IN ('TRU','TRM') THEN 'trusted_ins'
#             WHEN LEFT(PolicyNumber, 3) IN ('VIV','VIM') THEN 'viva_ins'
#             WHEN LEFT(PolicyNumber, 3) IN ('ATO','Ato') THEN 'atoz_travel'
#             ELSE 'UNKNOWN'
#         END AS travel_brand,
#         COUNT(*) AS VolumeMonth,
#         SUM(TotalGrossIncIPT) AS TotalGrossIncIPTMonth,
#         SUM(TotalGrossExcIPT) AS TotalGrossExcIPTMonth
#     FROM {base_table}
#     WHERE 
#         TransactionDate >= DATE_TRUNC('MONTH', DATE('{SelectedDate}'))
#         AND TransactionDate <= DATE('{SelectedDate}')
#         AND TransactionType = 'New Issue'
#     GROUP BY
#         CASE
#             WHEN LEFT(PolicyNumber, 3) IN ('SOI','SOM') THEN 'switched_on'
#             WHEN LEFT(PolicyNumber, 3) IN ('STA','STM') THEN 'start_travel'
#             WHEN LEFT(PolicyNumber, 3) IN ('TC-') THEN 'thomas_cook'
#             WHEN LEFT(PolicyNumber, 3) IN ('OI-','OIM') THEN 'oasis_travel'
#             WHEN LEFT(PolicyNumber, 3) IN ('TRU','TRM') THEN 'trusted_ins'
#             WHEN LEFT(PolicyNumber, 3) IN ('VIV','VIM') THEN 'viva_ins'
#             WHEN LEFT(PolicyNumber, 3) IN ('ATO','Ato') THEN 'atoz_travel'
#             ELSE 'UNKNOWN'
#         END
# )
# SELECT 
#     daily.travel_brand,
#     daily.Date,
#     daily.Volume,
#     daily.GWPincIPTDay,
#     daily.GWPexIPTDay,
#     monthly.VolumeMonth,
#     monthly.TotalGrossIncIPTMonth,
#     monthly.TotalGrossExcIPTMonth
# FROM MAGENTA_TRAVEL_BRAND daily
# LEFT JOIN MONTHLY_TOTALS monthly
#     ON daily.travel_brand = monthly.travel_brand
# where daily.travel_brand <> 'UNKNOWN'
# """

# # Step 4: Run the query
# df = spark.sql(query)
# display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if SourceType == 'Direct':
    travel_brand_expr = "'Direct'"
    additional_filter = """
    AND (SchemeName LIKE 'Direct%' OR SchemeName LIKE '%Renewal%')
    """
elif SourceType == 'Renewal':
    travel_brand_expr = "'Renewal'"
    additional_filter = """
    AND SchemeName LIKE '%Renewal%'
    """
elif SourceType == 'New':
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
    FROM  {base_table}
    WHERE 
        CAST(TransactionDate AS DATE) = DATE('{SelectedDate}')
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
    FROM  {base_table}
    WHERE 
        TransactionDate >= DATE_TRUNC('MONTH', DATE('{SelectedDate}'))
        AND TransactionDate < ADD_MONTHS(DATE_TRUNC('MONTH', DATE('{SelectedDate}')), 1)
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

# CELL ********************

SelectedDate = '2025-05-12'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = f"""
SELECT 
        CASE
        WHEN LEFT(PolicyNumber, 3) IN ('SOI','SOM') THEN 'switched_on'
        WHEN LEFT(PolicyNumber, 3) IN ('STA','STM') THEN 'start_travel'
        WHEN LEFT(PolicyNumber, 3) IN ('TC-') THEN 'thomas_cook'
        WHEN LEFT(PolicyNumber, 3) IN ('OI-','OIM') THEN 'oasis_travel'
        WHEN LEFT(PolicyNumber, 3) IN ('TRU','TRM') THEN 'trusted_ins'
        WHEN LEFT(PolicyNumber, 3) IN ('VIV','VIM') THEN 'viva_ins'
        WHEN LEFT(PolicyNumber, 3) IN ('ATO','Ato') THEN 'atoz_travel'
        ELSE 'UNKNOWN'
    END AS travel_brand,
        COUNT(*) AS VolumeMonth,
        SUM(TotalGrossIncIPT) AS TotalGrossIncIPTMonth,
        SUM(TotalGrossExcIPT) AS TotalGrossExcIPTMonth
    FROM  tblTravelSalesTransactions_1
    WHERE 
        TransactionDate >= DATE_TRUNC('MONTH', DATE('{SelectedDate}'))
        AND TransactionDate < ADD_MONTHS(DATE_TRUNC('MONTH', DATE('{SelectedDate}')), 1)
        AND TransactionType = 'New Issue'
    GROUP BY
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
# Now run this query
df = spark.sql(query)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SelectedDate = '2025-05-13'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = f"""
SELECT 
        CASE
        WHEN LEFT(PolicyNumber, 3) IN ('SOI','SOM') THEN 'switched_on'
        WHEN LEFT(PolicyNumber, 3) IN ('STA','STM') THEN 'start_travel'
        WHEN LEFT(PolicyNumber, 3) IN ('TC-') THEN 'thomas_cook'
        WHEN LEFT(PolicyNumber, 3) IN ('OI-','OIM') THEN 'oasis_travel'
        WHEN LEFT(PolicyNumber, 3) IN ('TRU','TRM') THEN 'trusted_ins'
        WHEN LEFT(PolicyNumber, 3) IN ('VIV','VIM') THEN 'viva_ins'
        WHEN LEFT(PolicyNumber, 3) IN ('ATO','Ato') THEN 'atoz_travel'
        ELSE 'UNKNOWN'
    END AS travel_brand,
    TransactionType,
    cast(TransactionDate as Date) as TransactionDate,
        COUNT(*) as VolumeMath,
        SUM(TotalGrossIncIPT) AS TotalGrossIncIPTMonth,
        SUM(TotalGrossExcIPT) AS TotalGrossExcIPTMonth
    FROM  tblTravelSalesTransactions_1
    WHERE 

    TransactionDate >= DATE_TRUNC('MONTH', DATE('{SelectedDate}')) 
    AND TransactionDate <= DATE('{SelectedDate}')

        and LEFT(PolicyNumber, 3) IN ('TRU','TRM') 
    GROUP By 
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
    ,cast(TransactionDate as Date)
    ,TransactionType
    order by cast(TransactionDate as Date)
    
"""
# Now run this query
df = spark.sql(query)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SelectedDate = '2025-05-12'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = f"""
SELECT 
        CASE
        WHEN LEFT(PolicyNumber, 3) IN ('SOI','SOM') THEN 'switched_on'
        WHEN LEFT(PolicyNumber, 3) IN ('STA','STM') THEN 'start_travel'
        WHEN LEFT(PolicyNumber, 3) IN ('TC-') THEN 'thomas_cook'
        WHEN LEFT(PolicyNumber, 3) IN ('OI-','OIM') THEN 'oasis_travel'
        WHEN LEFT(PolicyNumber, 3) IN ('TRU','TRM') THEN 'trusted_ins'
        WHEN LEFT(PolicyNumber, 3) IN ('VIV','VIM') THEN 'viva_ins'
        WHEN LEFT(PolicyNumber, 3) IN ('ATO','Ato') THEN 'atoz_travel'
        ELSE 'UNKNOWN'
    END AS travel_brand,
    TransactionType,
    cast(TransactionDate as Date) as TransactionDate,
    COUNT(*) as VolumeMath,
    SUM(TotalGrossIncIPT) AS TotalGrossIncIPTMonth,
    SUM(TotalGrossExcIPT) AS TotalGrossExcIPTMonth
FROM tblTravelSalesTransactions_1
WHERE TransactionDate BETWEEN DATE_TRUNC('MONTH', DATE('{SelectedDate}')) 
                        AND DATE('{SelectedDate}') + INTERVAL '1 DAY'  
AND LEFT(PolicyNumber, 3) IN ('TRU','TRM') 
GROUP BY 
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
    ,cast(TransactionDate as Date)
    ,TransactionType
ORDER BY cast(TransactionDate as Date)
"""
df = spark.sql(query)
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query = f"""
SELECT 
    CASE
        WHEN LEFT(PolicyNumber, 3) IN ('SOI','SOM') THEN 'switched_on'
        WHEN LEFT(PolicyNumber, 3) IN ('STA','STM') THEN 'start_travel'
        WHEN LEFT(PolicyNumber, 3) IN ('TC-') THEN 'thomas_cook'
        WHEN LEFT(PolicyNumber, 3) IN ('OI-','OIM') THEN 'oasis_travel'
        WHEN LEFT(PolicyNumber, 3) IN ('TRU','TRM') THEN 'trusted_ins'
        WHEN LEFT(PolicyNumber, 3) IN ('VIV','VIM') THEN 'viva_ins'
        WHEN LEFT(PolicyNumber, 3) IN ('ATO','Ato') THEN 'atoz_travel'
        ELSE 'UNKNOWN'
    END AS travel_brand,    
    SUM(CASE WHEN TransactionType = 'New Issue' THEN 1 
             WHEN TransactionType = 'Cancellation' THEN -1 ELSE 0 END) AS VolumeMath,
    
    -- New Issue sum minus Cancellation sum
    SUM(CASE WHEN TransactionType = 'New Issue' THEN TotalGrossIncIPT 
             WHEN TransactionType = 'Cancellation' THEN TotalGrossIncIPT ELSE 0 END) AS TotalGrossIncIPTMonth,

    SUM(CASE WHEN TransactionType = 'New Issue' THEN TotalGrossExcIPT 
             WHEN TransactionType = 'Cancellation' THEN TotalGrossExcIPT ELSE 0 END) AS TotalGrossExcIPTMonth

FROM tblTravelSalesTransactions_1
WHERE TransactionDate BETWEEN DATE_TRUNC('MONTH', DATE('{SelectedDate}')) 
                        AND DATE('{SelectedDate}') + INTERVAL '1 DAY'  
GROUP BY 
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
ORDER BY travel_brand
"""
df = spark.sql(query)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from DailySales

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
