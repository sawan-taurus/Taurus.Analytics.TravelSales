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

report_date = "2025-06-08"

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

# MARKDOWN ********************

# ### Query For Report Results:

# CELL ********************

report_df = spark.sql("""
WITH SchemeClassification AS (
SELECT 
        TravelBrandID,
        CASE 
            WHEN TravelBrand = 'start_travel' THEN 'Start'
            WHEN TravelBrand = 'trusted_ins' THEN 'Trusted'
            WHEN TravelBrand = 'viva_ins' THEN 'Viva'
            WHEN TravelBrand = 'switched_on' THEN 'SOI'
            ElSE 'UNKNOWN'
        END AS SchemeSubset
    FROM DimTravelBrand
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
    
    SUM(CASE WHEN Type = 'GPW' THEN `25-May` ELSE 0 END) AS GPW,
    SUM(CASE WHEN Type = 'Taurus Gross Retained' THEN `25-May` ELSE 0 END) AS `TaurusGrossRetained`,
    SUM(CASE WHEN Type = 'Taurus Net Retained' THEN `25-May` ELSE 0 END) AS `TaurusNetRetained`,
    SUM(CASE WHEN Type = 'Taurus Retained' THEN `25-May` ELSE 0 END) AS `TaurusRetained`
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
SOIAggsForcastSummary AS 
(
    SELECT 
    'Aggs' AS BrandNew,
    SUM(CASE WHEN Brand = 'Aggs' THEN 
        CASE 
            WHEN `25-May` IN ('N/A', 'Not Available', ' ') THEN 0
            ELSE TRY_CAST(REPLACE(REPLACE(TRIM(`25-May`), ',', ''), ' ', '') AS DOUBLE)
        END
    ELSE 0 END) AS GPW,
    SUM(CASE WHEN Brand = 'AggsMargin' THEN 
        CASE 
            WHEN `25-May` IN ('N/A', 'Not Available', ' ') THEN 0
            ELSE TRY_CAST(REPLACE(REPLACE(TRIM(`25-May`), ',', ''), ' ', '') AS DOUBLE)
        END
    ELSE 0 END) AS TaurusGrossRetained,
    SUM(CASE WHEN Brand = 'AggsNetMargin' THEN 
        CASE 
            WHEN `25-May` IN ('N/A', 'Not Available', ' ') THEN 0
            ELSE TRY_CAST(REPLACE(REPLACE(TRIM(`25-May`), ',', ''), ' ', '') AS DOUBLE)
        END
    ELSE 0 END) AS TaurusNetRetained
FROM SOIForecastSummary2025
WHERE Brand IN ('Aggs', 'AggsMargin', 'AggsNetMargin')
),
SOIDirectForcastSummary AS 
(
    SELECT 
    'Direct' AS BrandNew,
    SUM(CASE WHEN Brand = 'Direct' THEN 
        CASE 
            WHEN `25-May` IN ('N/A', 'Not Available', ' ') THEN 0
            ELSE TRY_CAST(REPLACE(REPLACE(TRIM(`25-May`), ',', ''), ' ', '') AS DOUBLE)
        END
    ELSE 0 END) AS GPW,
    SUM(CASE WHEN Brand = 'DirectMargin' THEN 
        CASE 
            WHEN `25-May` IN ('N/A', 'Not Available', ' ') THEN 0
            ELSE TRY_CAST(REPLACE(REPLACE(TRIM(`25-May`), ',', ''), ' ', '') AS DOUBLE)
        END
    ELSE 0 END) AS TaurusGrossRetained,
    SUM(CASE WHEN Brand = 'DirectNetMargin' THEN 
        CASE 
            WHEN `25-May` IN ('N/A', 'Not Available', ' ') THEN 0
            ELSE TRY_CAST(REPLACE(REPLACE(TRIM(`25-May`), ',', ''), ' ', '') AS DOUBLE)
        END
    ELSE 0 END) AS TaurusNetRetained
FROM SOIForecastSummary2025
WHERE Brand IN ('Direct', 'DirectMargin', 'DirectNetMargin')
),
TotalOasisRow AS (
    SELECT 
        dd.Date,
        'Oasis' AS SchemeSubset,
        SUM(fdo.Volume) AS DateVol,
        ROUND(SUM(fdo.GWPincIPTDay) * 0.89, 2) AS DateRetail,
        SUM(fdo.VolumeMonth) AS MTDVol,
        ROUND(SUM(fdo.TotalGrossIncIPTMonth) * 0.89, 2) AS MTDRetail,
        ROUND(SUM(fdo.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDay,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth),2) AS AvgP,
        ROUND(SUM(fdo.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVol,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / CAST(day(dd.Date - 1) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetail ,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / CAST(day(dd.Date - 1) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndGrossMargin,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / CAST(day(dd.Date - 1) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndNetMargin ,
        CONCAT(CAST(ROUND(((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / DAY(dd.Date - 1) * DAY(LAST_DAY(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) / TaurusRetained) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        CONCAT(CAST(ROUND(((((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth)) * (SUM(fdo.VolumeMonth) / DAY(dd.Date - 1)) * DAY(LAST_DAY(dd.Date))) / 1.2 * ((TaurusRetained / GPW) * 0.9) / TaurusRetained) * 100, 0) AS INT), '%') AS PerToFCastMargin,
        ROUND(GPW * 1.2,1) as PerToFCastNo,
        GPW ,TaurusRetained,TaurusRetained,
        CONCAT(ROUND((TaurusRetained / GPW) * 0.9 * 100, 2),'%') AS AvGrossMargin,
        CONCAT(ROUND((TaurusRetained / GPW) * 0.9 * 100, 2),'%')  AS AvNetMargin

    FROM FactDailyOasis fdo
    JOIN DimDate dd ON fdo.DateId = dd.DateId
    JOIN ForcastSummary fcs ON fcs.BrandNew = 'Oasis'
    GROUP BY dd.Date,GPW,TaurusRetained,TaurusRetained
),
TotalAtozRow AS (
    SELECT 
        dd.Date,
        'Atoz' AS SchemeSubset,
        SUM(fdo.Volume) AS DateVol,
        ROUND(SUM(fdo.GWPincIPTDay) * 0.89, 2) AS DateRetail,
        SUM(fdo.VolumeMonth) AS MTDVol,
        ROUND(SUM(fdo.TotalGrossIncIPTMonth) * 0.89, 2) AS MTDRetail,
        ROUND(SUM(fdo.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDay,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth),2) AS AvgP,
        ROUND(SUM(fdo.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVol,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / CAST(day(dd.Date - 1) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetail ,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / CAST(day(dd.Date - 1) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndGrossMargin,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / CAST(day(dd.Date - 1) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndNetMargin,
        CONCAT(CAST(ROUND(((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / DAY(dd.Date - 1) * DAY(LAST_DAY(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) / TaurusRetained) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        CONCAT(CAST(ROUND(((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / DAY(dd.Date - 1) * DAY(LAST_DAY(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) / TaurusRetained) * 100, 2) AS DECIMAL(10,2)), '%') AS PerToFCastMargin,
        ROUND(GPW * 1.2,1) as PerToFCastNo,
        GPW ,TaurusRetained,TaurusRetained,
        CONCAT(ROUND((TaurusRetained / GPW) * 0.9 * 100, 2),'%') AS AvGrossMargin,
        CONCAT(ROUND((TaurusRetained / GPW) * 0.9 * 100, 2),'%')  AS AvNetMargin
        
    FROM FactDailyAtoz fdo
    JOIN DimDate dd ON fdo.DateId = dd.DateId
    JOIN ForcastSummary fcs ON fcs.BrandNew = 'Atoz'
    GROUP BY dd.Date,GPW,TaurusRetained,TaurusRetained
),
AggregatedData AS (
    SELECT 
        dd.Date,
        sc.SchemeSubset,
        SUM(fds.Volume) AS DateVol,
        ROUND(SUM(fds.GWPincIPTDay), 2) AS DateRetail,
        SUM(fds.VolumeMonth) AS MTDVol,
        ROUND(SUM(fds.TotalGrossIncIPTMonth),2) AS MTDRetail,
        ROUND(SUM(fds.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDay,
        ROUND(SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth), 2) AS AvgP,
        ROUND(SUM(fds.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVol,
        ROUND(SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * day(last_day(dd.Date)), 2) AS EstMonEndRetail,
        ROUND((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * day(last_day(dd.Date))) / 1.2 * ((TaurusGrossRetained / GPW) * 0.9), 2) AS EstMonEndGrossMargin,
        ROUND((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * day(last_day(dd.Date))) / 1.2 * ((TaurusNetRetained / GPW) * 0.9), 2) AS EstMonEndNetMargin,
        CONCAT(CAST(ROUND((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * DAY(LAST_DAY(dd.Date)) / (GPW * 1.2)) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        CONCAT(CAST(ROUND(((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * DAY(LAST_DAY(dd.Date))) / 1.2 * ((TaurusGrossRetained / GPW) * 0.9) / TaurusGrossRetained) * 100, 0) AS INT), '%') AS PerToFCastMargin,
        ROUND(GPW * 1.2,1) as PerToFCastNo,
        GPW,
        TaurusGrossRetained,
        TaurusNetRetained,
        CONCAT(ROUND((TaurusGrossRetained / GPW) * 0.9 * 100, 2),'%') AS AvGrossMargin,
        CONCAT(ROUND((TaurusNetRetained / GPW) * 0.9 * 100, 2),'%')  AS AvNetMargin
    FROM FactDailySales fds
    JOIN SchemeClassification sc ON fds.TravelBrandID = sc.TravelBrandID
    JOIN DimDate dd ON fds.DateId = dd.DateId
    JOIN ForcastSummary fcs on fcs.BrandNew = sc.SchemeSubset
    GROUP BY dd.Date, sc.SchemeSubset,GPW,TaurusGrossRetained,TaurusNetRetained
),
TodaysSubSetsTotal AS (
    SELECT 
        Date,
        'Total' AS SchemeSubset,
        SUM(DateVol) AS DateVol,
        ROUND(SUM(DateRetail), 2) AS DateRetail,
        SUM(MTDVol) AS MTDVol,
        ROUND(SUM(MTDRetail),2) AS MTDRetail,
        ROUND(SUM(MTDVol) / CAST(day(Date) AS INT), 0)  AS AvgPolPerDay,
        ROUND(SUM(MTDRetail) / SUM(MTDVol),2) AS AvgP,
        ROUND(SUM(MTDVol) / CAST(day(Date) AS INT) * day(last_day(Date))) AS EstMonEndVol,
        ROUND(SUM(MTDRetail) / SUM(MTDVol) * SUM(MTDVol) / CAST(day(Date - 1) AS INT) * day(last_day(Date)), 2) AS EstMonEndRetail,
        ROUND(SUM(EstMonEndGrossMargin),2) as EstMonEndGrossMargin,
        ROUND(SUM(EstMonEndNetMargin),2) as EstMonEndNetMargin,
        CONCAT(CAST(ROUND((SUM(MTDRetail) / SUM(MTDVol) * SUM(MTDVol) / DAY(Date - 1) * DAY(LAST_DAY(Date)) / SUM(GPW) * 1.2) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        CONCAT(CAST(ROUND((SUM(EstMonEndGrossMargin) / SUM(TaurusGrossRetained)) * 100, 0) AS INT), '%') AS PerToFCastMargin,
        ROUND(SUM(GPW) * 1.2,1) as PerToFCastNo,
        SUM(GPW) as GPW,
        SUM(TaurusGrossRetained) as TaurusGrossRetained ,SUM(TaurusNetRetained) as TaurusNetRetained,
        NULL AS AvGrossMargin,
        NULL AS AvNetMargin
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
        SUM(MTDVol) AS MTDVol,
        ROUND(SUM(MTDRetail),2) AS MTDRetail,
        ROUND(SUM(MTDVol) / CAST(day(Date) AS INT), 0)  AS AvgPolPerDay,
        ROUND(SUM(MTDRetail) / SUM(MTDVol),2) AS AvgP,
        ROUND(SUM(MTDVol) / CAST(day(Date) AS INT) * day(last_day(Date))) AS EstMonEndVol,
        ROUND(SUM(MTDRetail) / SUM(MTDVol) * SUM(MTDVol) / CAST(day(Date - 1) AS INT) * day(last_day(Date))) AS EstMonEndRetail,
        0 AS EstMonEndGrossMargin, 
        0 AS EstMonEndNetMargin,
        PerToFCastRetail,
        PerToFCastMargin,
        ROUND(GPW * 1.2,1) as PerToFCastNo,
        GPW,
        0 AS TaurusGrossRetained,
        NULL AS TaurusNetRetained,
        0 as AvGrossMargin,
        NULL AS AvNetMargin    
    FROM AggregatedData
    WHERE SchemeSubset = 'SOI'
    GROUP BY Date,GPW,PerToFCastRetail,PerToFCastMargin
),
DirectRow AS (
    SELECT 
        dd.Date,
        'Direct' AS SchemeSubset,
        SUM(fds.Volume) AS DateVol,
        ROUND(SUM(fds.GWPincIPTDay), 2) AS DateRetail,
        SUM(fds.VolumeMonth) AS MTDVol,
        ROUND(SUM(fds.TotalGrossIncIPTMonth),2) AS MTDRetail,
        ROUND(SUM(fds.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDay,
        ROUND(ROUND(SUM(fds.TotalGrossIncIPTMonth),2) / SUM(fds.VolumeMonth), 2) as AvgP,
        ROUND(SUM(fds.VolumeMonth / CAST(day(dd.Date) AS INT)) * day(last_day(dd.Date))) AS EstMonEndVol,
        ROUND(SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * ROUND(SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * day(last_day(dd.Date))), 2) AS EstMonEndRetail,
        ROUND(((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * ROUND(SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * day(last_day(dd.Date))) / 1.2) * (TaurusGrossRetained / GPW) * 0.9 * 100), 2) AS EstMonEndGrossMargin,
        ROUND(((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * ROUND(SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * day(last_day(dd.Date))) / 1.2) * (TaurusNetRetained / GPW) * 0.9 * 100), 2) AS EstMonEndNetMargin,    
        CONCAT(CAST(ROUND((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * ROUND(SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * DAY(LAST_DAY(dd.Date))) / (sdfcs.GPW * 1.2)) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        CONCAT(CAST(ROUND(((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * ROUND(SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * DAY(LAST_DAY(dd.Date))) / 1.2) * (TaurusGrossRetained / GPW) * 0.9 * 100) / (sdfcs.GPW * 1.2), 0) AS INT), '%') AS PerToFCastMargin,
        ROUND(sdfcs.GPW * 1.2, 2) as PerToFCastNo,
        sdfcs.GPW AS GPW,
        sdfcs.TaurusGrossRetained AS TaurusGrossRetained,
        sdfcs.TaurusNetRetained AS TaurusNetRetained,
        CONCAT(ROUND((TaurusGrossRetained / GPW) * 0.9 * 100, 2),'%') AS AvGrossMargin,
        CONCAT(ROUND((TaurusNetRetained / GPW) * 0.9 * 100, 2),'%')  AS AvNetMargin
    FROM FactDailyDirect fds
    JOIN DimDate dd ON fds.DateId = dd.DateId
    JOIN SOIDirectForcastSummary sdfcs on sdfcs.BrandNew = 'Direct' 
    GROUP BY dd.Date, dd.DateId,sdfcs.GPW,sdfcs.TaurusGrossRetained,sdfcs.TaurusNetRetained
),
NewRow AS (
    SELECT 
        dd.Date,
        '  New' AS SchemeSubset,
        SUM(fds.Volume) AS DateVol,
        ROUND(SUM(fds.GWPincIPTDay), 2) AS DateRetail,
        SUM(fds.VolumeMonth) AS MTDVol,
        ROUND(SUM(fds.TotalGrossIncIPTMonth),2) AS MTDRetail,
        ROUND(SUM(fds.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDay,
        ROUND(ROUND(SUM(fds.TotalGrossIncIPTMonth),2) / SUM(fds.VolumeMonth), 2) as AvgP,
        ROUND(SUM(fds.VolumeMonth / CAST(day(dd.Date) AS INT)) * day(last_day(dd.Date))) AS EstMonEndVol,
        ROUND(ROUND(SUM(fds.TotalGrossIncIPTMonth),2) / SUM(fds.VolumeMonth) * ROUND(SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * day(last_day(dd.Date))), 2) AS EstMonEndRetail,
        NULL as PerToFCastRetail,
        NULL as PerToFCastMargin,
        NULL AS EstMonEndGrossMargin,
        NULL AS EstMonEndNetMargin,
        NULL as PerToFCastNo,
        NULL AS GPW,
        NULL AS TaurusGrossRetained,
        NULL AS TaurusNetRetained,
        NULL AS AvGrossMargin,
        NULL AS AvNetMargin
    FROM FactDailyNew fds
    JOIN DimDate dd ON fds.DateId = dd.DateId
    GROUP BY dd.Date, dd.DateId
),
ReNewRow AS (
    SELECT 
        dd.Date,
        '  Renewal' AS SchemeSubset,
        SUM(fds.Volume) AS DateVol,
        ROUND(SUM(fds.GWPincIPTDay), 2) AS DateRetail,
        SUM(fds.VolumeMonth) AS MTDVol,
        ROUND(SUM(fds.TotalGrossIncIPTMonth),2) AS MTDRetail,
        ROUND(SUM(fds.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDay,
        ROUND(ROUND(SUM(fds.TotalGrossIncIPTMonth),2) / SUM(fds.VolumeMonth), 2) as AvgP,
        ROUND(SUM(fds.VolumeMonth / CAST(day(dd.Date) AS INT)) * day(last_day(dd.Date))) AS EstMonEndVol,
        ROUND(ROUND(SUM(fds.TotalGrossIncIPTMonth),2) / SUM(fds.VolumeMonth) * ROUND(SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * day(last_day(dd.Date))), 2) AS EstMonEndRetail,
        NULL AS EstMonEndGrossMargin,
        NULL AS EstMonEndNetMargin,
        NULL as PerToFCastRetail,
        NULL as PerToFCastMargin,
        NULL as PerToFCastNo,
        NULL AS GPW,
        NULL AS TaurusGrossRetained,
        NULL AS TaurusNetRetained,
        NULL AS AvGrossMargin,
        NULL AS AvNetMargin
    FROM FactDailyRenewal fds
    JOIN DimDate dd ON fds.DateId = dd.DateId
    GROUP BY dd.Date, dd.DateId
),
AggsRow AS (
    SELECT 
        st.Date,
        'Aggs' AS SchemeSubset,
        st.DateVol - sd.DateVol AS DateVol,
        ROUND(st.DateRetail - sd.DateRetail, 2) AS DateRetail,
        st.MTDVol - sd.MTDVol AS MTDVol,
        ROUND(st.MTDRetail - sd.MTDRetail,2) AS MTDRetail,
        ROUND((st.MTDVol - sd.MTDVol) / CAST(day(st.Date) AS INT),0)  AS AvgPolPerDay,
        ROUND((st.MTDRetail - sd.MTDRetail) / (st.MTDVol - sd.MTDVol),2) AS AvgP,
        ROUND((st.MTDVol - sd.MTDVol) / CAST(day(st.Date) AS INT) * day(last_day(st.Date))) AS EstMonEndVol,
        ROUND((st.MTDRetail - sd.MTDRetail) / (st.MTDVol - sd.MTDVol) * (st.MTDVol - sd.MTDVol) / CAST(day(st.Date - 1) AS INT) * day(last_day(st.Date)),2) AS EstMonEndRetail,
        ROUND((((st.MTDRetail - sd.MTDRetail) / (st.MTDVol - sd.MTDVol) * (st.MTDVol - sd.MTDVol) / CAST(DAY(st.Date - 1) AS INT) * DAY(LAST_DAY(st.Date)) / 1.2) * ((sfcs.TaurusGrossRetained / sfcs.GPW) * 0.9)), 2) AS EstMonEndGrossMargin,
        ROUND((((st.MTDRetail - sd.MTDRetail) / (st.MTDVol - sd.MTDVol) * (st.MTDVol - sd.MTDVol) / CAST(DAY(st.Date - 1) AS INT) * DAY(LAST_DAY(st.Date)) / 1.2) * ((sfcs.TaurusNetRetained / sfcs.GPW) * 0.9)), 0) AS EstMonEndNetMargin,
        CONCAT(CAST(ROUND((((st.MTDRetail - sd.MTDRetail) / (st.MTDVol - sd.MTDVol) * (st.MTDVol - sd.MTDVol) / CAST(DAY(st.Date - 1) AS INT) * DAY(LAST_DAY(st.Date))) / (sfcs.GPW * 1.2)) * 100, 0) AS INT), '%') AS  PerToFCastRetail,
        CONCAT(CAST(ROUND(((st.MTDRetail - sd.MTDRetail) / (st.MTDVol - sd.MTDVol) * (st.MTDVol - sd.MTDVol) / CAST(day(st.Date - 1) AS INT) * day(last_day(st.Date)) / 1.2) * ((sfcs.TaurusGrossRetained / sfcs.GPW) * 0.9 * 100) / sfcs.TaurusGrossRetained,0) AS INT), '%')  AS PerToFCastMargin,
        ROUND(sfcs.GPW * 1.2 ,1) as PerToFCastNo,
        sfcs.GPW AS GPW,
        sfcs.TaurusGrossRetained AS TaurusGrossRetained,
        sfcs.TaurusNetRetained AS TaurusNetRetained,
        ROUND((st.MTDVol - sd.MTDVol) / CAST(day(st.Date - 1) AS INT),0) AS AvgPolPerDa,
        CONCAT(ROUND((sfcs.TaurusGrossRetained / sfcs.GPW) * 0.9 * 100, 2),'%') AS AvGrossMargin,
        CONCAT(ROUND((sfcs.TaurusNetRetained / sfcs.GPW) * 0.9 * 100, 2),'%')  AS AvNetMargin
    FROM SOITotalRow st
    JOIN SOIAggsForcastSummary sfcs on sfcs.BrandNew = 'Aggs'
    JOIN DirectRow sd ON st.Date = sd.Date
)
SELECT Date,SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,EstMonEndGrossMargin,EstMonEndNetMargin,PerToFCastRetail,PerToFCastMargin,PerToFCastNo,GPW,TaurusGrossRetained,TaurusNetRetained,AvGrossMargin,AvNetMargin FROM AggregatedData WHERE Date = '${report_date}' 
UNION ALL 
SELECT Date,SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,EstMonEndGrossMargin,EstMonEndNetMargin,PerToFCastRetail,PerToFCastMargin,PerToFCastNo,GPW,TaurusRetained,TaurusRetained,AvGrossMargin,AvNetMargin FROM TotalOasisRow WHERE Date = '${report_date}'
UNION ALL 
SELECT Date,SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,EstMonEndGrossMargin,EstMonEndNetMargin,PerToFCastRetail,PerToFCastMargin,PerToFCastNo,GPW,TaurusRetained,TaurusRetained,AvGrossMargin,AvNetMargin FROM TotalAtozRow WHERE Date = '${report_date}'
UNION ALL 
SELECT Date,SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,EstMonEndGrossMargin,EstMonEndNetMargin,PerToFCastRetail,PerToFCastMargin,PerToFCastNo,GPW,TaurusGrossRetained,TaurusNetRetained,AvGrossMargin,AvNetMargin FROM TodaysSubSetsTotal WHERE Date = '${report_date}'
UNION ALL 
SELECT Date,SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,EstMonEndGrossMargin,EstMonEndNetMargin,PerToFCastRetail,PerToFCastMargin,PerToFCastNo,GPW,TaurusGrossRetained,TaurusNetRetained,AvGrossMargin,AvNetMargin FROM AggsRow WHERE Date = '${report_date}'
UNION ALL 
SELECT Date,SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,EstMonEndGrossMargin,EstMonEndNetMargin,PerToFCastRetail,PerToFCastMargin,PerToFCastNo,GPW,TaurusGrossRetained,TaurusNetRetained,AvGrossMargin,AvNetMargin FROM DirectRow WHERE Date = '${report_date}'
UNION ALL 
SELECT Date,SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,EstMonEndGrossMargin,EstMonEndNetMargin,PerToFCastRetail,PerToFCastMargin,PerToFCastNo,GPW,TaurusGrossRetained,TaurusNetRetained,AvGrossMargin,AvNetMargin FROM NewRow WHERE Date = '${report_date}'
UNION ALL 
SELECT Date,SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,EstMonEndGrossMargin,EstMonEndNetMargin,PerToFCastRetail,PerToFCastMargin,PerToFCastNo,GPW,TaurusGrossRetained,TaurusNetRetained,AvGrossMargin,AvNetMargin FROM ReNewRow WHERE Date = '${report_date}'
UNION ALL 
SELECT 
    st.Date,st.SchemeSubset,st.DateVol,st.DateRetail,st.MTDVol,st.MTDRetail,st.AvgPolPerDay,st.AvgP,st.EstMonEndVol,st.EstMonEndRetail,ROUND(st.EstMonEndGrossMargin + COALESCE(aggs.EstMonEndGrossMargin, 0) + COALESCE(direct.EstMonEndGrossMargin, 0), 2) AS EstMonEndGrossMargin,
    ROUND(st.EstMonEndNetMargin + COALESCE(aggs.EstMonEndNetMargin, 0) + COALESCE(direct.EstMonEndNetMargin, 0), 2) as EstMonEndNetMargin, 
    st.PerToFCastRetail,st.PerToFCastMargin,
    st.PerToFCastNo,st.GPW,
    ROUND(st.TaurusGrossRetained + COALESCE(aggs.TaurusGrossRetained, 0) + COALESCE(direct.TaurusGrossRetained, 0), 2) as TaurusGrossRetained,st.TaurusNetRetained,
    CONCAT(CAST(ROUND(st.EstMonEndRetail / ((st.EstMonEndGrossMargin + COALESCE(aggs.EstMonEndGrossMargin, 0) + COALESCE(direct.EstMonEndGrossMargin, 0)) * 1.2), 0) AS INT), '%') as AvGrossMargin,
    st.AvNetMargin 
FROM SOITotalRow st
LEFT JOIN AggsRow aggs ON st.Date = aggs.Date
LEFT JOIN DirectRow direct ON st.Date = direct.Date
WHERE st.Date = '${report_date}'
""")
display(report_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
