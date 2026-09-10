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

import html

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

report_date = '2025-04-23'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("report_date", report_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from DimTravelBrand

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from FactDailySales


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

report_df = spark.sql("""
WITH SchemeClassification AS (
    SELECT 
        SchemeID,
        CASE 
            WHEN Scheme LIKE 'Start%' THEN 'Start'
            WHEN Scheme LIKE 'Trusted%' THEN 'Trusted'
            WHEN Scheme LIKE 'Viva%' THEN 'Viva'
            ELSE 'SOI'
        END AS SchemeSubset
    FROM DimScheme
),
ForcastSummary AS 
(
    SELECT 
    CASE 
        WHEN Brand = 'ViVA' THEN 'Viva' 
        WHEN Brand = 'Switched On' THEN 'SOI' 
        WHEN Brand = 'Start Travel' THEN 'Start' 
        WHEN Brand = 'Trusted' THEN 'Trusted' 
        WHEN Brand = 'Oasis' THEN 'Oasis' 
        WHEN Brand = 'A to Z' THEN 'Atoz' 
        ELSE 'Unknown'
    END AS BrandNew,
    
    SUM(CASE WHEN Type = 'GPW' THEN `25-Apr` ELSE 0 END) AS GPW,
    SUM(CASE WHEN Type = 'Taurus Gross Retained' THEN `25-Apr` ELSE 0 END) AS `TaurusGrossRetained`,
    SUM(CASE WHEN Type = 'Taurus Net Retained' THEN `25-Apr` ELSE 0 END) AS `TaurusNetRetained`,
    SUM(CASE WHEN Type = 'Taurus Retained' THEN `25-Apr` ELSE 0 END) AS `TaurusRetained`
FROM ForecastSummary2025
WHERE Brand IN ('ViVA','Switched On','Start Travel','Trusted','Oasis','A to Z') 
  AND Type IN ('GPW', 'Taurus Gross Retained', 'Taurus Net Retained','Taurus Retained')
GROUP BY 
    CASE 
        WHEN Brand = 'ViVA' THEN 'Viva' 
        WHEN Brand = 'Switched On' THEN 'SOI' 
        WHEN Brand = 'Start Travel' THEN 'Start' 
        WHEN Brand = 'Trusted' THEN 'Trusted' 
        WHEN Brand = 'Oasis' THEN 'Oasis' 
        WHEN Brand = 'A to Z' THEN 'Atoz' 
        ELSE 'Unknown'
    END
),
SOIDirectRow AS (
    SELECT 
        dd.Date,
        'SOI Direct' AS SchemeSubset,
        SUM(fds.VolumeDay) AS DateVol,
        ROUND(SUM(fds.GWPIncIPTDay), 2) AS DateRetail,
        -- SUM(fds.GWPExIPTDay) AS TotalGWPExIPTDay,
        -- SUM(fds.NTUDay) AS TotalNTUDay,
        SUM(fds.VolumeMth) AS MTDVol,
        ROUND(SUM(fds.GWPIncIPTMth),2) AS MTDRetail,
        -- SUM(fds.GWPExIPTMth) AS TotalGWPExIPTMth,
        -- SUM(fds.NTUMth) AS TotalNTUMth,
        ROUND(SUM(fds.VolumeMth) / CAST(day(dd.Date - 1) AS INT), 0)  AS AvgPolPerDay,
        ROUND(ROUND(SUM(fds.GWPIncIPTMth),2) / SUM(fds.VolumeMth), 2) as AvgP,
        ROUND(SUM(fds.VolumeMth / CAST(day(dd.Date - 1) AS INT)) * day(last_day(dd.Date))) AS EstMonEndVol,
        ROUND(ROUND(SUM(fds.GWPIncIPTMth),2) / SUM(fds.VolumeMth) * ROUND(SUM(fds.VolumeMth / CAST(day(dd.Date - 1) AS INT)) * day(last_day(dd.Date))), 2) AS EstMonEndRetail,
        'EstMonEndGrossMargin','EstMonEndNetMargin',
        'GWP','TaurusGrossRetained','TaurusNetRetained'
            ,'AvGrossMargin','AvNetMargin'
    FROM FactDailySales fds
    JOIN DimScheme ds ON fds.SchemeID = ds.SchemeID
    JOIN DimDate dd ON fds.DateId = dd.DateId
    WHERE 
        ds.Scheme LIKE 'PCW Renewal%' OR 
        ds.Scheme LIKE 'SOI Direct Renewal%' OR 
        ds.Scheme LIKE 'Direct%' OR 
        ds.Scheme LIKE 'Start%'
    GROUP BY dd.Date, dd.DateId
),
NewRow AS (
    SELECT 
        dd.Date,
        'New' AS SchemeSubset,
        SUM(fds.VolumeDay) AS DateVol,
        ROUND(SUM(fds.GWPIncIPTDay), 2) AS DateRetail,
        -- SUM(fds.GWPExIPTDay) AS TotalGWPExIPTDay,
        -- SUM(fds.NTUDay) AS TotalNTUDay,
        SUM(fds.VolumeMth) AS MTDVol,
        ROUND(SUM(fds.GWPIncIPTMth),2) AS MTDRetail,
        -- SUM(fds.GWPExIPTMth) AS TotalGWPExIPTMth,
        -- SUM(fds.NTUMth) AS TotalNTUMth,
        ROUND(SUM(fds.VolumeMth) / CAST(day(dd.Date - 1) AS INT), 0)  AS AvgPolPerDay,
        ROUND(ROUND(SUM(fds.GWPIncIPTMth),2) / SUM(fds.VolumeMth), 2) as AvgP,
        ROUND(SUM(fds.VolumeMth) / CAST(day(dd.Date - 1) AS INT) * day(last_day(dd.Date))) AS EstMonEndVol,
        SUM(fds.GWPIncIPTMth) / SUM(fds.VolumeMth) * SUM(fds.VolumeMth / CAST(day(dd.Date - 1) AS INT)) * day(last_day(dd.Date)) AS EstMonEndRetail ,
        'EstMonEndGrossMargin','EstMonEndNetMargin',
        'GWP' ,'TaurusGrossRetained','TaurusNetRetained'
            ,'AvGrossMargin','AvNetMargin'
    FROM FactDailySales fds
    JOIN DimScheme ds ON fds.SchemeID = ds.SchemeID
    JOIN DimDate dd ON fds.DateId = dd.DateId
    WHERE 
        ds.Scheme LIKE 'Direct AMT%' OR 
        ds.Scheme LIKE 'Direct Single%' OR 
        ds.Scheme LIKE 'Direct Backpacker%'
    GROUP BY dd.Date
),
ReNewRow AS (
    SELECT 
        sd.Date,
        'ReNew' AS SchemeSubset,
        sd.DateVol - n.DateVol AS DateVol,
        ROUND(sd.DateRetail - n.DateRetail, 2) AS DateRetail,
        -- sd.TotalGWPExIPTDay - n.TotalGWPExIPTDay AS TotalGWPExIPTDay,
        -- sd.TotalNTUDay - n.TotalNTUDay AS TotalNTUDay,
        sd.MTDVol - n.MTDVol AS MTDVol,
        ROUND(sd.MTDRetail - n.MTDRetail,2) AS MTDRetail,
        -- sd.TotalGWPExIPTMth - n.TotalGWPExIPTMth AS TotalGWPExIPTMth,
        -- sd.TotalNTUMth - n.TotalNTUMth AS TotalNTUMth,
        ROUND((sd.MTDVol - n.MTDVol) / CAST(day(sd.Date - 1) AS INT),0)  AS AvgPolPerDay,
        ROUND((sd.MTDRetail - n.MTDRetail) / (sd.MTDVol - n.MTDVol), 2) as AvgP,
        ROUND((sd.MTDVol - n.MTDVol) / CAST(day(sd.Date - 1) AS INT)  * day(last_day(sd.Date))) AS EstMonEndVol,
        ROUND((sd.MTDRetail - n.MTDRetail) / (sd.MTDVol - n.MTDVol) * (sd.MTDVol - n.MTDVol) / CAST(day(sd.Date - 1) AS INT)  * day(last_day(sd.Date)), 2) AS EstMonEndRetail,
        'EstMonEndGrossMargin','EstMonEndNetMargin',
        'GWP','TaurusGrossRetained','TaurusNetRetained'
            ,'AvGrossMargin','AvNetMargin'
    FROM SOIDirectRow sd
    JOIN NewRow n ON sd.Date = n.Date
    ),

AggregatedData AS (
    SELECT 
        dd.Date,
        sc.SchemeSubset,
        SUM(fds.VolumeDay) AS DateVol,
        ROUND(SUM(fds.GWPIncIPTDay), 2) AS DateRetail,
        -- SUM(fds.GWPExIPTDay) AS TotalGWPExIPTDay,
        -- SUM(fds.NTUDay) AS TotalNTUDay,
        SUM(fds.VolumeMth) AS MTDVol,
        ROUND(SUM(fds.GWPIncIPTMth),2) AS MTDRetail,
        -- SUM(fds.GWPExIPTMth) AS TotalGWPExIPTMth,
        -- SUM(fds.NTUMth) AS TotalNTUMth,
        ROUND(SUM(fds.VolumeMth) / CAST(day(dd.Date - 1) AS INT), 0)  AS AvgPolPerDay,
        ROUND(SUM(fds.GWPIncIPTMth) / SUM(fds.VolumeMth), 2) AS AvgP,
        ROUND(SUM(fds.VolumeMth) / CAST(day(dd.Date - 1) AS INT) * day(last_day(dd.Date))) AS EstMonEndVol,
        ROUND(SUM(fds.GWPIncIPTMth) / SUM(fds.VolumeMth) * SUM(fds.VolumeMth / CAST(day(dd.Date - 1) AS INT)) * day(last_day(dd.Date)), 2) AS EstMonEndRetail,
        ROUND((SUM(fds.GWPIncIPTMth) / SUM(fds.VolumeMth) * SUM(fds.VolumeMth / CAST(day(dd.Date - 1) AS INT)) * day(last_day(dd.Date))) / 1.2 * ((TaurusGrossRetained / GPW) * 0.9), 2) AS EstMonEndGrossMargin,
        ROUND((SUM(fds.GWPIncIPTMth) / SUM(fds.VolumeMth) * SUM(fds.VolumeMth / CAST(day(dd.Date - 1) AS INT)) * day(last_day(dd.Date))) / 1.2 * ((TaurusNetRetained / GPW) * 0.9), 2) AS EstMonEndNetMargin,
        GPW ,
        TaurusGrossRetained,
        TaurusNetRetained,
        ROUND((TaurusGrossRetained / GPW) * 0.9 * 100, 2) AS AvGrossMargin,
        ROUND((TaurusNetRetained / GPW) * 0.9 * 100, 2)  AS AvNetMargin
    FROM FactDailySales fds
    JOIN SchemeClassification sc ON fds.SchemeID = sc.SchemeID
    JOIN DimDate dd ON fds.DateId = dd.DateId
    JOIN ForcastSummary fcs on fcs.BrandNew = sc.SchemeSubset
    GROUP BY dd.Date, sc.SchemeSubset,GPW,TaurusGrossRetained,TaurusNetRetained
),
TotalOasisRow AS (
    SELECT 
        dd.Date,
        'Oasis' AS SchemeSubset,
        SUM(fdo.VolumeDay) AS DateVol,
        ROUND(SUM(fdo.GWPIncIPTDay) * 0.89, 2) AS DateRetail,
        -- SUM(fdo.GWPExIPTDay) AS TotalGWPExIPTDay,
        -- SUM(fdo.NTUDay) AS TotalNTUDay,
        SUM(fdo.VolumeMth) AS MTDVol,
        ROUND(SUM(fdo.GWPIncIPTMth) * 0.89, 2) AS MTDRetail,
        -- SUM(fdo.GWPExIPTMth) AS TotalGWPExIPTMth,
        -- SUM(fdo.NTUMth) AS TotalNTUMth,
        ROUND(SUM(fdo.VolumeMth) / CAST(day(dd.Date - 1) AS INT), 0)  AS AvgPolPerDay,
        ROUND((SUM(fdo.GWPIncIPTMth) * 0.89) / SUM(fdo.VolumeMth),2) AS AvgP,
        ROUND(SUM(fdo.VolumeMth) / CAST(day(dd.Date - 1) AS INT) * day(last_day(dd.Date))) AS EstMonEndVol,
        ROUND((SUM(fdo.GWPIncIPTMth) * 0.89) / SUM(fdo.VolumeMth) * SUM(fdo.VolumeMth) / CAST(day(dd.Date - 1) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetail ,
        ROUND((SUM(fdo.GWPIncIPTMth) * 0.89) / SUM(fdo.VolumeMth) * SUM(fdo.VolumeMth) / CAST(day(dd.Date - 1) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndGrossMargin,
        ROUND((SUM(fdo.GWPIncIPTMth) * 0.89) / SUM(fdo.VolumeMth) * SUM(fdo.VolumeMth) / CAST(day(dd.Date - 1) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndNetMargin ,
        GPW ,TaurusRetained,TaurusRetained,
        ROUND((TaurusRetained / GPW) * 0.9 * 100, 2) AS AvGrossMargin,
        ROUND((TaurusRetained / GPW) * 0.9 * 100, 2)  AS AvNetMargin

    FROM FactDailyOasis fdo
    JOIN DimDate dd ON fdo.DateId = dd.DateId
    JOIN ForcastSummary fcs ON fcs.BrandNew = 'Oasis'
    GROUP BY dd.Date,GPW,TaurusRetained,TaurusRetained
),

TotalAtozRow AS (
    SELECT 
        dd.Date,
        'Atoz' AS SchemeSubset,
        SUM(fa.VolumeDay) AS DateVol,
        ROUND(SUM(fa.GWPIncIPTDay), 2) AS DateRetail,
        -- SUM(fa.GWPExIPTDay) AS TotalGWPExIPTDay,
        -- SUM(fa.NTUDay) AS TotalNTUDay,
        SUM(fa.VolumeMth) AS MTDVol,
        ROUND(SUM(fa.GWPIncIPTMth),2) AS MTDRetail,
        -- SUM(fa.GWPExIPTMth) AS TotalGWPExIPTMth,
        -- SUM(fa.NTUMth) AS TotalNTUMth,
        ROUND(SUM(fa.VolumeMth) / CAST(day(dd.Date - 1) AS INT), 0)  AS AvgPolPerDay,
        ROUND(SUM(fa.GWPIncIPTMth) / SUM(fa.VolumeMth),2) AS AvgP,
        ROUND(SUM(fa.VolumeMth) / CAST(day(dd.Date - 1) AS INT) * day(last_day(dd.Date))) AS EstMonEndVol,
        ROUND(SUM(fa.GWPIncIPTMth) / SUM(fa.VolumeMth) * SUM(fa.VolumeMth / CAST(day(dd.Date - 1) AS INT)) * day(last_day(dd.Date)), 2) AS EstMonEndRetail  ,
        ROUND((SUM(fa.GWPIncIPTMth) * 0.89) / SUM(fa.VolumeMth) * SUM(fa.VolumeMth) / CAST(day(dd.Date - 1) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndGrossMargin,
        ROUND((SUM(fa.GWPIncIPTMth) * 0.89) / SUM(fa.VolumeMth) * SUM(fa.VolumeMth) / CAST(day(dd.Date - 1) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndNetMargin ,
        GPW,TaurusRetained,TaurusRetained,
        ROUND((TaurusRetained / GPW) * 0.9 * 100, 2) AS AvGrossMargin,
        ROUND((TaurusRetained / GPW) * 0.9 * 100, 2)  AS AvNetMargin
    FROM FactDailyAtoz fa
    JOIN DimDate dd ON fa.DateId = dd.DateId
    JOIN ForcastSummary fcs ON fcs.BrandNew = 'Atoz'
    GROUP BY dd.Date,GPW,TaurusRetained,TaurusRetained
),

TodaysSubSetsTotal AS (
    SELECT 
        Date,
        'TodaysSubSetsTotal' AS SchemeSubset,
        SUM(DateVol) AS DateVol,
        ROUND(SUM(DateRetail), 2) AS DateRetail,
        -- SUM(TotalGWPExIPTDay) AS TotalGWPExIPTDay,
        -- SUM(TotalNTUDay) AS TotalNTUDay,
        SUM(MTDVol) AS MTDVol,
        ROUND(SUM(MTDRetail),2) AS MTDRetail,
        -- SUM(TotalGWPExIPTMth) AS TotalGWPExIPTMth,
        -- SUM(TotalNTUMth) AS TotalNTUMth,
        ROUND(SUM(MTDVol) / CAST(day(Date - 1) AS INT), 0)  AS AvgPolPerDay,
        ROUND(SUM(MTDRetail) / SUM(MTDVol),2) AS AvgP,
        ROUND(SUM(MTDVol) / CAST(day(Date - 1) AS INT) * day(last_day(Date))) AS EstMonEndVol,
        ROUND(SUM(MTDRetail) / SUM(MTDVol) * SUM(MTDVol) / CAST(day(Date - 1) AS INT) * day(last_day(Date)), 2) AS EstMonEndRetail,
        SUM(EstMonEndGrossMargin),
        SUM(EstMonEndNetMargin),
        SUM(GPW)
        ,SUM(TaurusGrossRetained),SUM(TaurusNetRetained)
        ,'AvGrossMargin','AvNetMargin'
    FROM (
        SELECT * FROM AggregatedData WHERE SchemeSubset IN ('Start', 'SOI', 'Trusted', 'Viva')
        UNION ALL
        SELECT * FROM TotalAtozRow
        UNION ALL
        SELECT * FROM TotalOasisRow
    ) AS SubsetData
    GROUP BY Date
),

SOITotalRow AS (
    SELECT 
        Date,
        'SOI Total' AS SchemeSubset,
        SUM(DateVol) AS DateVol,
        ROUND(SUM(DateRetail), 2) AS DateRetail,
        -- SUM(TotalGWPExIPTDay) AS TotalGWPExIPTDay,
        -- SUM(TotalNTUDay) AS TotalNTUDay,
        SUM(MTDVol) AS MTDVol,
        ROUND(SUM(MTDRetail),2) AS MTDRetail,
        -- SUM(TotalGWPExIPTMth) AS TotalGWPExIPTMth,
        -- SUM(TotalNTUMth) AS TotalNTUMth,
        ROUND(SUM(MTDVol) / CAST(day(Date - 1) AS INT), 0)  AS AvgPolPerDay,
        ROUND(SUM(MTDRetail) / SUM(MTDVol),2) AS AvgP,
        ROUND(SUM(MTDVol) / CAST(day(Date - 1) AS INT) * day(last_day(Date))) AS EstMonEndVol,
        ROUND(SUM(MTDRetail) / SUM(MTDVol) * SUM(MTDVol) / CAST(day(Date - 1) AS INT) * day(last_day(Date))) AS EstMonEndRetail,
        'EstMonEndGrossMargin','EstMonEndNetMargin',
        'GWP','TaurusGrossRetained','TaurusNetRetained'
            ,'AvGrossMargin','AvNetMargin'
    FROM AggregatedData
    WHERE SchemeSubset = 'SOI'
    GROUP BY Date
),
AggsRow AS (
    SELECT 
        st.Date,
        'Aggs' AS SchemeSubset,
        st.DateVol - sd.DateVol AS DateVol,
        ROUND(st.DateRetail - sd.DateRetail, 2) AS DateRetail,
        -- st.TotalGWPExIPTDay - sd.TotalGWPExIPTDay AS TotalGWPExIPTDay,
        -- st.TotalNTUDay - sd.TotalNTUDay AS TotalNTUDay,
        st.MTDVol - sd.MTDVol AS MTDVol,
        ROUND(st.MTDRetail - sd.MTDRetail,2) AS MTDRetail,
        -- st.TotalGWPExIPTMth - sd.TotalGWPExIPTMth AS TotalGWPExIPTMth,
        -- st.TotalNTUMth - sd.TotalNTUMth AS TotalNTUMth,
        ROUND((st.MTDVol - sd.MTDVol) / CAST(day(st.Date - 1) AS INT),0)  AS AvgPolPerDay,
        ROUND((st.MTDRetail - sd.MTDRetail) / (st.MTDVol - sd.MTDVol),2) AS AvgP,
        ROUND((st.MTDVol - sd.MTDVol) / CAST(day(st.Date - 1) AS INT) * day(last_day(st.Date))) AS EstMonEndVol,
        (st.MTDRetail - sd.MTDRetail) / (st.MTDVol - sd.MTDVol) * (st.MTDVol - sd.MTDVol) / CAST(day(st.Date - 1) AS INT) * day(last_day(st.Date)) AS EstMonEndRetail,
        'EstMonEndGrossMargin','EstMonEndNetMargin',
        'GWP','TaurusGrossRetained','TaurusNetRetained'
            ,'AvGrossMargin','AvNetMargin'
    FROM SOITotalRow st
    JOIN SOIDirectRow sd ON st.Date = sd.Date
)
SELECT * FROM AggregatedData where Date = '${report_date}'
UNION ALL SELECT * FROM TotalOasisRow where Date = '${report_date}'
UNION ALL SELECT * FROM TotalAtozRow where Date = '${report_date}'
UNION ALL SELECT * FROM TodaysSubSetsTotal where Date = '${report_date}'
UNION ALL SELECT * FROM SOITotalRow where Date = '${report_date}'
UNION ALL SELECT * FROM SOIDirectRow  where Date = '${report_date}'
UNION ALL SELECT * FROM NewRow where Date = '${report_date}'
UNION ALL SELECT * FROM ReNewRow where Date = '${report_date}'
UNION ALL SELECT * FROM AggsRow where Date = '${report_date}'

ORDER BY 
    Date,
    CASE 
        WHEN SchemeSubset = 'Start' THEN 1
        WHEN SchemeSubset = 'SOI' THEN 2
        WHEN SchemeSubset = 'Trusted' THEN 3
        WHEN SchemeSubset = 'Viva' THEN 4
        WHEN SchemeSubset = 'Atoz' THEN 5
        WHEN SchemeSubset = 'Oasis' THEN 6
        WHEN SchemeSubset = 'TodaysSubSetsTotal' THEN 7
        WHEN SchemeSubset = 'SOI Total' THEN 8
        WHEN SchemeSubset = 'Aggs' THEN 9
        WHEN SchemeSubset = 'SOI Direct' THEN 10
        WHEN SchemeSubset = 'New' THEN 11
        WHEN SchemeSubset = 'ReNew' THEN 12
        ELSE 13
    END;
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(report_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# report_df.write.mode("overwrite").option("header", True).csv(f"Files/Reports/DailySales_{report_date}.csv")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Convert Spark DataFrame to Pandas
# pandas_df = report_df.toPandas()

# # Now convert to HTML
# html_table = pandas_df.to_html(index=False, border=1, justify='center')

# # Optional: Add a title
# html_content = f"<h3>Daily Sales Report – {report_date}</h3>{html_table}"

# # Return the HTML content to Data Factory
# from notebookutils import mssparkutils
# mssparkutils.notebook.exit(html_content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pandas = report_df.toPandas()

# Step 4: Format numeric values (optional)
df_pandas["MTDRetail"] = df_pandas["MTDRetail"].map("{:,.2f}".format)
df_pandas["EstMonEndRetail"] = df_pandas["EstMonEndRetail"].map("{:,.2f}".format)

# Step 5: Convert to HTML table and clean newlines
html_table = df_pandas.to_html(index=False, border=1).replace("\n", "")

# Optional: Add a little styling
html = f"""
<h2>Daily Sales Report – {report_date}</h2>
{html_table}
"""

# Step 6: Return the HTML to Data Factory
from notebookutils import mssparkutils
mssparkutils.notebook.exit(html.strip())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
