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

report_date = "2025-05-01"

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
            WHEN TravelBrand = 'OasisAgg' THEN 'Oasis AGG'
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
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetail ,
        --ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) 
            --/ CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndGrossMargin,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / CAST(day(dd.Date) AS INT) * 
            day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndNetMargin,
        CONCAT(CAST(ROUND(((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / DAY(dd.Date) * 
            DAY(LAST_DAY(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) / TaurusRetained) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        CONCAT(CAST(ROUND(((((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth)) * (SUM(fdo.VolumeMonth) / DAY(dd.Date)) * 
            DAY(LAST_DAY(dd.Date))) / 1.2 * ((TaurusRetained / GPW) * 0.9) / TaurusRetained) * 100, 0) AS INT), '%') AS PerToFCastMargin,
        ROUND(GPW * 1.2,1) as PerToFCastNo,
        GPW ,TaurusRetained,TaurusRetained,
        CONCAT(ROUND((TaurusRetained / GPW) * 0.9 * 100, 2),'%') AS AvGrossMargin,
        CONCAT(ROUND((TaurusRetained / GPW) * 0.9 * 100, 2),'%')  AS AvNetMargin,

        --AGG
        SUM(fda.Volume) as DateVolAgg,
        ROUND(SUM(fda.GWPincIPTDay) * 0.89, 2) AS DateRetailAgg,
        SUM(fda.VolumeMonth) AS MTDVolAgg,
        ROUND(SUM(fda.TotalGrossIncIPTMonth) * 0.89, 2) AS MTDRetailAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth) * 0.89) / SUM(fda.VolumeMonth),2) AS AvgPAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth) * 0.89) / SUM(fda.VolumeMonth) * SUM(fda.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetailAgg ,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndNetMarginAgg,

        --Direct
        SUM(fdd.Volume) as DateVolDirect,
        ROUND(SUM(fdd.GWPincIPTDay) * 0.89, 2) AS DateRetailDirect,
        SUM(fdd.VolumeMonth) AS MTDVolDirect,
        ROUND(SUM(fdd.TotalGrossIncIPTMonth) * 0.89, 2) AS MTDRetailDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / SUM(fdd.VolumeMonth),2) AS AvgPDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / SUM(fdd.VolumeMonth) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetailDirect ,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / SUM(fdd.VolumeMonth) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndNetMarginDirect

    FROM FactDailyOasis fdo
    LEFT JOIN TaurusGoldLH.factdailyoasisagg fda ON fdo.DateId = fda.DateId
    LEFT JOIN TaurusGoldLH.FactDailyOasisDirect fdd ON fdo.DateId = fdd.DateId
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
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / CAST(day(dd.Date - 1) AS INT) * 
            day(last_day(dd.Date)),2) AS EstMonEndRetail ,
        --# ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / CAST(day(dd.Date - 1) AS INT) * 
        --#     day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndGrossMargin,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / CAST(day(dd.Date - 1) AS INT) * 
            day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndNetMargin,
        CONCAT(CAST(ROUND(((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) / TaurusRetained) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        CONCAT(CAST(ROUND(((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) / TaurusRetained) * 100, 2) AS DECIMAL(10,2)), '%') AS PerToFCastMargin,
        ROUND(GPW * 1.2,1) as PerToFCastNo,
        GPW ,TaurusRetained,TaurusRetained,
        CONCAT(ROUND((TaurusRetained / GPW) * 0.9 * 100, 2),'%') AS AvGrossMargin,
        CONCAT(ROUND((TaurusRetained / GPW) * 0.9 * 100, 2),'%')  AS AvNetMargin,

        --AGG
        SUM(fda.Volume) as DateVolAgg,
        ROUND(SUM(fda.GWPincIPTDay) * 0.89, 2) AS DateRetailAgg,
        SUM(fda.VolumeMonth) AS MTDVolAgg,
        ROUND(SUM(fda.TotalGrossIncIPTMonth) * 0.89, 2) AS MTDRetailAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth) * 0.89) / SUM(fda.VolumeMonth),2) AS AvgPAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth) * 0.89) / SUM(fda.VolumeMonth) * SUM(fda.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetailAgg ,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / SUM(fdo.VolumeMonth) * SUM(fdo.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndNetMarginAgg,

        --Direct
        SUM(fdd.Volume) as DateVolDirect,
        ROUND(SUM(fdd.GWPincIPTDay) * 0.89, 2) AS DateRetailDirect,
        SUM(fdd.VolumeMonth) AS MTDVolDirect,
        ROUND(SUM(fdd.TotalGrossIncIPTMonth) * 0.89, 2) AS MTDRetailDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / SUM(fdd.VolumeMonth),2) AS AvgPDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / SUM(fdd.VolumeMonth) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetailDirect ,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / SUM(fdd.VolumeMonth) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / GPW) * 0.9) , 2) AS EstMonEndNetMarginDirect
            
    FROM FactDailyAtoz fdo
    LEFT JOIN TaurusGoldLH.factdailyatozagg fda ON fdo.DateId = fda.DateId
    LEFT JOIN TaurusGoldLH.FactDailyatozDirect fdd ON fdo.DateId = fdd.DateId
    JOIN DimDate dd ON fdo.DateId = dd.DateId
    JOIN ForcastSummary fcs ON fcs.BrandNew = 'Atoz'
    GROUP BY dd.Date,GPW,TaurusRetained,TaurusRetained
),
TotalVivaRow AS (
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
        ROUND(SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) 
            * day(last_day(dd.Date)), 2) AS EstMonEndRetail,
        --ROUND((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * 
        --    day(last_day(dd.Date))) / 1.2 * ((TaurusGrossRetained / GPW) * 0.9), 2) 
        --AS EstMonEndGrossMargin,
        ROUND((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * 
            day(last_day(dd.Date))) / 1.2 * ((TaurusNetRetained / GPW) * 0.9), 2) AS EstMonEndNetMargin,
        CONCAT(CAST(ROUND((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date)) / (GPW * 1.2)) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        CONCAT(CAST(ROUND(((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date))) / 1.2 * ((TaurusGrossRetained / GPW) * 0.9) / TaurusGrossRetained) * 100, 0) AS INT), '%') 
                AS PerToFCastMargin,
        ROUND(GPW * 1.2,1) as PerToFCastNo,
        GPW,
        TaurusGrossRetained,
        TaurusNetRetained,
        CONCAT(ROUND((TaurusGrossRetained / GPW) * 0.9 * 100, 2),'%') AS AvGrossMargin,
        CONCAT(ROUND((TaurusNetRetained / GPW) * 0.9 * 100, 2),'%')  AS AvNetMargin,

    
        --AGG
        SUM(fda.Volume) as DateVolAgg,
        ROUND(SUM(fda.GWPincIPTDay) * 0.89, 2) AS DateRetailAgg,
        SUM(fda.VolumeMonth) AS MTDVolAgg,
        ROUND(SUM(fda.TotalGrossIncIPTMonth) * 0.89, 2) AS MTDRetailAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth) * 0.89) / SUM(fda.VolumeMonth),2) AS AvgPAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth) * 0.89) / SUM(fda.VolumeMonth) * SUM(fda.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetailAgg ,
        ROUND((SUM(fds.TotalGrossIncIPTMonth) * 0.89) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusNetRetained / GPW) * 0.9) , 2) AS EstMonEndNetMarginAgg,

        --Direct
        SUM(fdd.Volume) as DateVolDirect,
        ROUND(SUM(fdd.GWPincIPTDay) * 0.89, 2) AS DateRetailDirect,
        SUM(fdd.VolumeMonth) AS MTDVolDirect,
        ROUND(SUM(fdd.TotalGrossIncIPTMonth) * 0.89, 2) AS MTDRetailDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / SUM(fdd.VolumeMonth),2) AS AvgPDirect,
         ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / SUM(fdd.VolumeMonth) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetailDirect ,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / SUM(fdd.VolumeMonth) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusNetRetained / GPW) * 0.9) , 2) AS EstMonEndNetMarginDirect
        FROM FactDailySales fds
    JOIN SchemeClassification sc ON fds.TravelBrandID = sc.TravelBrandID
    LEFT JOIN TaurusGoldLH.factdailyvivaagg fda ON fds.DateId = fda.DateId
    LEFT JOIN TaurusGoldLH.FactDailyvivaDirect fdd ON fds.DateId = fdd.DateId
    JOIN DimDate dd ON fds.DateId = dd.DateId
    JOIN ForcastSummary fcs on fcs.BrandNew = sc.SchemeSubset
    where sc.SchemeSubset = 'Viva'
    GROUP BY dd.Date, sc.SchemeSubset,GPW,TaurusGrossRetained,TaurusNetRetained
),
TotalSOIRow AS (
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
        ROUND(SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) 
            * day(last_day(dd.Date)), 2) AS EstMonEndRetail,
        --ROUND((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * 
        --    day(last_day(dd.Date))) / 1.2 * ((TaurusGrossRetained / GPW) * 0.9), 2) 
        --AS EstMonEndGrossMargin,
        ROUND((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * 
            day(last_day(dd.Date))) / 1.2 * ((TaurusNetRetained / GPW) * 0.9), 2) AS EstMonEndNetMargin,
        CONCAT(CAST(ROUND((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date)) / (GPW * 1.2)) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        CONCAT(CAST(ROUND(((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date))) / 1.2 * ((TaurusGrossRetained / GPW) * 0.9) / TaurusGrossRetained) * 100, 0) AS INT), '%') 
                AS PerToFCastMargin,
        ROUND(GPW * 1.2,1) as PerToFCastNo,
        GPW,
        TaurusGrossRetained,
        TaurusNetRetained,
        CONCAT(ROUND((TaurusGrossRetained / GPW) * 0.9 * 100, 2),'%') AS AvGrossMargin,
        CONCAT(ROUND((TaurusNetRetained / GPW) * 0.9 * 100, 2),'%')  AS AvNetMargin,

    
        --AGG
        SUM(fda.Volume) as DateVolAgg,
        ROUND(SUM(fda.GWPincIPTDay) * 0.89, 2) AS DateRetailAgg,
        SUM(fda.VolumeMonth) AS MTDVolAgg,
        ROUND(SUM(fda.TotalGrossIncIPTMonth) * 0.89, 2) AS MTDRetailAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth) * 0.89) / SUM(fda.VolumeMonth),2) AS AvgPAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth) * 0.89) / SUM(fda.VolumeMonth) * SUM(fda.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetailAgg ,
        ROUND((SUM(fds.TotalGrossIncIPTMonth) * 0.89) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusNetRetained / GPW) * 0.9) , 2) AS EstMonEndNetMarginAgg,

        --Direct
        SUM(fdd.Volume) as DateVolDirect,
        ROUND(SUM(fdd.GWPincIPTDay) * 0.89, 2) AS DateRetailDirect,
        SUM(fdd.VolumeMonth) AS MTDVolDirect,
        ROUND(SUM(fdd.TotalGrossIncIPTMonth) * 0.89, 2) AS MTDRetailDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / SUM(fdd.VolumeMonth),2) AS AvgPDirect,
         ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / SUM(fdd.VolumeMonth) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetailDirect ,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / SUM(fdd.VolumeMonth) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusNetRetained / GPW) * 0.9) , 2) AS EstMonEndNetMarginDirect
        FROM FactDailySales fds
    JOIN SchemeClassification sc ON fds.TravelBrandID = sc.TravelBrandID
    LEFT JOIN TaurusGoldLH.factdailysoiagg fda ON fds.DateId = fda.DateId
    LEFT JOIN TaurusGoldLH.FactDailysoiDirect fdd ON fds.DateId = fdd.DateId
    JOIN DimDate dd ON fds.DateId = dd.DateId
    JOIN ForcastSummary fcs on fcs.BrandNew = sc.SchemeSubset
    where sc.SchemeSubset = 'SOI'
    GROUP BY dd.Date, sc.SchemeSubset,GPW,TaurusGrossRetained,TaurusNetRetained   
),
TotalTrustedRow AS (
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
        ROUND(SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) 
            * day(last_day(dd.Date)), 2) AS EstMonEndRetail,
        --ROUND((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * 
        --    day(last_day(dd.Date))) / 1.2 * ((TaurusGrossRetained / GPW) * 0.9), 2) 
        --AS EstMonEndGrossMargin,
        ROUND((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * 
            day(last_day(dd.Date))) / 1.2 * ((TaurusNetRetained / GPW) * 0.9), 2) AS EstMonEndNetMargin,

        CONCAT(CAST(ROUND((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date)) / (GPW * 1.2)) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        CONCAT(CAST(ROUND(((SUM(fds.TotalGrossIncIPTMonth) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date))) / 1.2 * ((TaurusGrossRetained / GPW) * 0.9) / TaurusGrossRetained) * 100, 0) AS INT), '%') 
                AS PerToFCastMargin,
        ROUND(GPW * 1.2,1) as PerToFCastNo,

        GPW,
        TaurusGrossRetained,
        TaurusNetRetained,
        CONCAT(ROUND((TaurusGrossRetained / GPW) * 0.9 * 100, 2),'%') AS AvGrossMargin,
        CONCAT(ROUND((TaurusNetRetained / GPW) * 0.9 * 100, 2),'%')  AS AvNetMargin,

    
        --AGG
        SUM(fda.Volume) as DateVolAgg,
        ROUND(SUM(fda.GWPincIPTDay) * 0.89, 2) AS DateRetailAgg,
        SUM(fda.VolumeMonth) AS MTDVolAgg,
        ROUND(SUM(fda.TotalGrossIncIPTMonth) * 0.89, 2) AS MTDRetailAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth) * 0.89) / SUM(fda.VolumeMonth),2) AS AvgPAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth) * 0.89) / SUM(fda.VolumeMonth) * SUM(fda.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetailAgg ,
        ROUND((SUM(fds.TotalGrossIncIPTMonth) * 0.89) / SUM(fds.VolumeMonth) * SUM(fds.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusNetRetained / GPW) * 0.9) , 2) AS EstMonEndNetMarginAgg,

        --Direct
        SUM(fdd.Volume) as DateVolDirect,
        ROUND(SUM(fdd.GWPincIPTDay) * 0.89, 2) AS DateRetailDirect,
        SUM(fdd.VolumeMonth) AS MTDVolDirect,
        ROUND(SUM(fdd.TotalGrossIncIPTMonth) * 0.89, 2) AS MTDRetailDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / SUM(fdd.VolumeMonth),2) AS AvgPDirect,
         ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / SUM(fdd.VolumeMonth) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetailDirect ,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / SUM(fdd.VolumeMonth) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) / 1.2 * ((TaurusNetRetained / GPW) * 0.9) , 2) AS EstMonEndNetMarginDirect
    FROM FactDailySales fds
    JOIN SchemeClassification sc ON fds.TravelBrandID = sc.TravelBrandID
    LEFT JOIN TaurusGoldLH.factdailytrustedagg fda ON fds.DateId = fda.DateId
    LEFT JOIN TaurusGoldLH.FactDailytrustedDirect fdd ON fds.DateId = fdd.DateId
    JOIN DimDate dd ON fds.DateId = dd.DateId
    JOIN ForcastSummary fcs on fcs.BrandNew = sc.SchemeSubset
    where sc.SchemeSubset = 'Trusted'
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
        --ROUND(SUM(EstMonEndGrossMargin),2) as EstMonEndGrossMargin,
        ROUND(SUM(EstMonEndNetMargin),2) as EstMonEndNetMargin,
        CONCAT(CAST(ROUND((SUM(MTDRetail) / SUM(MTDVol) * SUM(MTDVol) / DAY(Date - 1) * DAY(LAST_DAY(Date)) / SUM(GPW) * 1.2) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        NULL AS PerToFCastMargin,
        ROUND(SUM(GPW) * 1.2,1) as PerToFCastNo,

        SUM(GPW) as GPW,
        SUM(TaurusGrossRetained) as TaurusGrossRetained,
        SUM(TaurusNetRetained) as TaurusNetRetained,
        NULL AS AvGrossMargin,
        NULL AS AvNetMargin,

        --AGG
        SUM(DateVolAgg) as DateVolAgg,
        ROUND(SUM(DateRetailAgg),2) AS DateRetailAgg,
        SUM(MTDVolAgg) AS MTDVolAgg,
        ROUND(SUM(MTDRetailAgg), 2) AS MTDRetailAgg,
        ROUND(SUM(MTDVolAgg) / CAST(day(Date) AS INT), 0)  AS AvgPolPerDayAgg,
        ROUND((SUM(MTDRetailAgg)) / SUM(MTDVolAgg),2) AS AvgPAgg,
        ROUND(SUM(MTDVolAgg) / CAST(day(Date) AS INT) * day(last_day(Date))) AS EstMonEndVolAgg,
        ROUND(SUM(EstMonEndRetailAgg),2) AS EstMonEndRetailAgg ,
        ROUND(SUM(EstMonEndNetMarginAgg), 2) AS EstMonEndNetMarginAgg,

        --Direct
        SUM(DateVolDirect) AS DateVolDirect,
        ROUND(SUM(DateRetailDirect), 2) AS DateRetailDirect,
        SUM(MTDVolDirect) AS MTDVolDirect,
        ROUND(SUM(MTDRetailDirect), 2) AS MTDRetailDirect,
        ROUND(SUM(MTDVolDirect) / CAST(day(Date) AS INT), 0)  AS AvgPolPerDayDirect,
        ROUND((SUM(MTDRetailDirect) * 0.89) / SUM(MTDVolDirect),2) AS AvgPDirect,
         ROUND(SUM(MTDVolDirect) / CAST(day(Date) AS INT) * day(last_day(Date))) AS EstMonEndVolDirect,
        ROUND(SUM(EstMonEndRetailDirect),2) AS EstMonEndRetailDirect ,
        ROUND(SUM(EstMonEndNetMarginDirect), 2) AS EstMonEndNetMarginDirect
        
    FROM (
        SELECT * FROM TotalTrustedRow UNION ALL 
        SELECT * FROM TotalVivaRow UNION ALL 
        SELECT * FROM TotalSOIRow UNION ALL 
        SELECT * FROM TotalAtozRow UNION ALL
        SELECT * FROM TotalOasisRow
    ) AS SubsetData
    GROUP BY Date
)

---------------------------------TotalSubSet--------------------------------------------------

SELECT Date, 'TotalSubSet', 'TotalSubSet' AS SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,EstMonEndNetMargin,
NULL as BudgetVol,NULL as BudgetGWP,NULL as BudgetNetMargin,NULL as PercentToVol,NULL as PercentToGWP,NULL as PercentToNetMargin
FROM TodaysSubSetsTotal WHERE Date = '${report_date}' 

---------------------------------Trusted--------------------------------------------------

UNION ALL
SELECT Date, 'Trusted', 'Trusted' AS SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,EstMonEndNetMargin,
NULL as BudgetVol,NULL as BudgetGWP,NULL as BudgetNetMargin,NULL as PercentToVol,NULL as PercentToGWP,NULL as PercentToNetMargin
FROM TotalTrustedRow WHERE Date = '${report_date}' 

UNION ALL
SELECT Date, 'Trusted', 'Trusted AGG' AS SchemeSubset, 
    DateVolAgg AS DateVol, DateRetailAgg AS DateRetail, MTDVolAgg AS MTDVol, MTDRetailAgg AS MTDRetail, 
    AvgPolPerDayAgg AS AvgPolPerDay, AvgPAgg AS AvgP, EstMonEndVolAgg AS EstMonEndVol, 
    EstMonEndRetailAgg AS EstMonEndRetail, EstMonEndNetMarginAgg AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25` 
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetVol,
    CAST(REPLACE(
        CASE 
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVol / BudgetVol) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetail / BudgetGWP) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,

ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMargin / BudgetNetMargin) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalTrustedRow
JOIN Trusted2025_Budget_Summary BudgetSummary ON BudgetSummary.Prop_0 = 'Agg Volume'
JOIN Trusted2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Prop_0 = 'Agg GWP'
JOIN Trusted2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Prop_0 = 'Agg Net Margin'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'Trusted', 'Trusted Direct' AS SchemeSubset, 
    DateVolDirect AS DateVol, DateRetailDirect AS DateRetail, MTDVolDirect AS MTDVol, MTDRetailDirect AS MTDRetail, 
    AvgPolPerDayDirect AS AvgPolPerDay, AvgPDirect AS AvgP, EstMonEndVolDirect AS EstMonEndVol, 
    EstMonEndRetailDirect AS EstMonEndRetail, EstMonEndNetMarginDirect AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        ELSE '0'  -- Ensure no NULL values
    END, ',', '') AS INT), 0) AS BudgetGWP,

    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVol / BudgetVol) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetail / BudgetGWP) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,

ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMargin / BudgetNetMargin) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalTrustedRow
JOIN Trusted2025_Budget_Summary BudgetSummary ON BudgetSummary.Prop_0 = 'Direct Volume'
JOIN Trusted2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Prop_0 = 'Direct GWP'
JOIN Trusted2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Prop_0 = 'Direct Net Margin'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'Trusted', 'Trusted Total' AS SchemeSubset, 
    COALESCE(DateVolAgg, 0) + COALESCE(DateVolDirect, 0) AS DateVol, 
    COALESCE(DateRetailAgg, 0) + COALESCE(DateRetailDirect, 0) AS DateRetail, 
    COALESCE(MTDVolAgg, 0) + COALESCE(MTDVolDirect, 0) AS MTDVol, 
    ROUND(COALESCE(MTDRetailAgg, 0) + COALESCE(MTDRetailDirect, 0),2) AS MTDRetail, 
    ((COALESCE(AvgPolPerDayAgg, 0) + COALESCE(AvgPolPerDayDirect, 0)) / 2) AS AvgPolPerDay, 
    ((COALESCE(AvgPAgg, 0) + COALESCE(AvgPDirect, 0)) / 2) AS AvgP, 
    COALESCE(EstMonEndVolAgg, 0) + COALESCE(EstMonEndVolDirect, 0) AS EstMonEndVol, 
    COALESCE(EstMonEndRetailAgg, 0) + COALESCE(EstMonEndRetailDirect, 0) AS EstMonEndRetail, 
    ROUND(COALESCE(EstMonEndNetMarginAgg, 0) + COALESCE(EstMonEndNetMarginDirect, 0),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        ELSE NULL  -- Don't return '0' unless necessary
    END, ',', '') AS INT), NULL) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVol / BudgetVol) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
    ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetail / BudgetGWP) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMargin / BudgetNetMargin) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalTrustedRow
LEFT JOIN Trusted2025_Budget_Summary BudgetSummary ON BudgetSummary.Prop_0 = 'TOTAL VOLUME'
LEFT JOIN Trusted2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Prop_0 = 'TOTAL GWP'
LEFT JOIN Trusted2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Prop_0 = 'TOTAL NET MARGIN'
WHERE Date = '${report_date}'
UNION ALL 

---------------------------------Switched On--------------------------------------------------

SELECT Date, 'SOI', 'SOI' AS SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,EstMonEndNetMargin,
NULL as BudgetVol,NULL as BudgetGWP,NULL as BudgetNetMargin,NULL as PercentToVol,NULL as PercentToGWP,NULL as PercentToNetMargin
FROM TotalSOIRow WHERE Date = '${report_date}' 

UNION ALL
SELECT Date, 'SOI', 'SOI AGG' AS SchemeSubset, 
    DateVolAgg AS DateVol, DateRetailAgg AS DateRetail, MTDVolAgg AS MTDVol, MTDRetailAgg AS MTDRetail, 
    AvgPolPerDayAgg AS AvgPolPerDay, AvgPAgg AS AvgP, EstMonEndVolAgg AS EstMonEndVol, 
    EstMonEndRetailAgg AS EstMonEndRetail, EstMonEndNetMarginAgg AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetVol,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVol / BudgetVol) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetail / BudgetGWP) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,

ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMargin / BudgetNetMargin) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalSOIRow
JOIN SOI2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Agg Volume'
JOIN SOI2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Agg GWP'
JOIN SOI2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Agg Net Margin'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'SOI', 'SOI Direct' AS SchemeSubset, 
    DateVolDirect AS DateVol, DateRetailDirect AS DateRetail, MTDVolDirect AS MTDVol, MTDRetailDirect AS MTDRetail, 
    AvgPolPerDayDirect AS AvgPolPerDay, AvgPDirect AS AvgP, EstMonEndVolDirect AS EstMonEndVol, 
    EstMonEndRetailDirect AS EstMonEndRetail, EstMonEndNetMarginDirect AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        ELSE '0'  -- Ensure no NULL values
    END, ',', '') AS INT), 0) AS BudgetGWP,

    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVol / BudgetVol) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetail / BudgetGWP) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,

ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMargin / BudgetNetMargin) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalSOIRow
JOIN SOI2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Direct Volume'
JOIN SOI2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Direct GWP'
JOIN SOI2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Direct Net Margin'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'SOI', 'SOI Total' AS SchemeSubset, 
    COALESCE(DateVolAgg, 0) + COALESCE(DateVolDirect, 0) AS DateVol, 
    COALESCE(DateRetailAgg, 0) + COALESCE(DateRetailDirect, 0) AS DateRetail, 
    COALESCE(MTDVolAgg, 0) + COALESCE(MTDVolDirect, 0) AS MTDVol, 
    ROUND(COALESCE(MTDRetailAgg, 0) + COALESCE(MTDRetailDirect, 0),2) AS MTDRetail, 
    ((COALESCE(AvgPolPerDayAgg, 0) + COALESCE(AvgPolPerDayDirect, 0)) / 2) AS AvgPolPerDay, 
    ((COALESCE(AvgPAgg, 0) + COALESCE(AvgPDirect, 0)) / 2) AS AvgP, 
    COALESCE(EstMonEndVolAgg, 0) + COALESCE(EstMonEndVolDirect, 0) AS EstMonEndVol, 
    COALESCE(EstMonEndRetailAgg, 0) + COALESCE(EstMonEndRetailDirect, 0) AS EstMonEndRetail, 
    ROUND(COALESCE(EstMonEndNetMarginAgg, 0) + COALESCE(EstMonEndNetMarginDirect, 0),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        ELSE NULL  -- Don't return '0' unless necessary
    END, ',', '') AS INT), NULL) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVol / BudgetVol) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
    ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetail / BudgetGWP) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMargin / BudgetNetMargin) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalSOIRow
LEFT JOIN SOI2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'TOTAL VOLUME'
LEFT JOIN SOI2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'TOTAL GWP'
LEFT JOIN SOI2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'TOTAL NET MARGIN'
WHERE Date = '${report_date}'
UNION ALL 

---------------------------------Viva--------------------------------------------------

SELECT Date, 'Viva', 'Viva' AS SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,EstMonEndNetMargin,
NULL as BudgetVol,NULL as BudgetGWP,NULL as BudgetNetMargin,NULL as PercentToVol,NULL as PercentToGWP,NULL as PercentToNetMargin
FROM TotalVivaRow WHERE Date = '${report_date}' 

UNION ALL
SELECT Date, 'Viva', 'Viva AGG' AS SchemeSubset, 
    DateVolAgg AS DateVol, DateRetailAgg AS DateRetail, MTDVolAgg AS MTDVol, MTDRetailAgg AS MTDRetail, 
    AvgPolPerDayAgg AS AvgPolPerDay, AvgPAgg AS AvgP, EstMonEndVolAgg AS EstMonEndVol, 
    EstMonEndRetailAgg AS EstMonEndRetail, EstMonEndNetMarginAgg AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetVol,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVol / BudgetVol) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetail / BudgetGWP) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,

ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMargin / BudgetNetMargin) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalVivaRow
JOIN ViVA2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Agg Volume'
JOIN ViVA2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Agg GWP'
JOIN ViVA2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Agg Net Margin'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'Viva', 'Viva Direct' AS SchemeSubset, 
    DateVolDirect AS DateVol, DateRetailDirect AS DateRetail, MTDVolDirect AS MTDVol, MTDRetailDirect AS MTDRetail, 
    AvgPolPerDayDirect AS AvgPolPerDay, AvgPDirect AS AvgP, EstMonEndVolDirect AS EstMonEndVol, 
    EstMonEndRetailDirect AS EstMonEndRetail, EstMonEndNetMarginDirect AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        ELSE '0'  -- Ensure no NULL values
    END, ',', '') AS INT), 0) AS BudgetGWP,

    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVol / BudgetVol) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetail / BudgetGWP) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,

ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMargin / BudgetNetMargin) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalVivaRow
JOIN ViVA2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Direct Volume'
JOIN ViVA2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Direct GWP'
JOIN ViVA2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Direct Net Margin'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'Viva', 'Viva Total' AS SchemeSubset, 
    COALESCE(DateVolAgg, 0) + COALESCE(DateVolDirect, 0) AS DateVol, 
    COALESCE(DateRetailAgg, 0) + COALESCE(DateRetailDirect, 0) AS DateRetail, 
    COALESCE(MTDVolAgg, 0) + COALESCE(MTDVolDirect, 0) AS MTDVol, 
    ROUND(COALESCE(MTDRetailAgg, 0) + COALESCE(MTDRetailDirect, 0),2) AS MTDRetail, 
    ((COALESCE(AvgPolPerDayAgg, 0) + COALESCE(AvgPolPerDayDirect, 0)) / 2) AS AvgPolPerDay, 
    ((COALESCE(AvgPAgg, 0) + COALESCE(AvgPDirect, 0)) / 2) AS AvgP, 
    COALESCE(EstMonEndVolAgg, 0) + COALESCE(EstMonEndVolDirect, 0) AS EstMonEndVol, 
    COALESCE(EstMonEndRetailAgg, 0) + COALESCE(EstMonEndRetailDirect, 0) AS EstMonEndRetail, 
    ROUND(COALESCE(EstMonEndNetMarginAgg, 0) + COALESCE(EstMonEndNetMarginDirect, 0),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        ELSE NULL  -- Don't return '0' unless necessary
    END, ',', '') AS INT), NULL) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVol / BudgetVol) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
    ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetail / BudgetGWP) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMargin / BudgetNetMargin) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalVivaRow
LEFT JOIN ViVA2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'TOTAL VOLUME'
LEFT JOIN ViVA2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'TOTAL GWP'
LEFT JOIN ViVA2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'TOTAL NET MARGIN'
WHERE Date = '${report_date}'
UNION ALL 

---------------------------------Oasis--------------------------------------------------

SELECT Date, 'Oasis', 'Oasis' AS SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,EstMonEndNetMargin,
NULL as BudgetVol,NULL as BudgetGWP,NULL as BudgetNetMargin,NULL as PercentToVol,NULL as PercentToGWP,NULL as PercentToNetMargin
FROM TotalOasisRow WHERE Date = '${report_date}' 

UNION ALL
SELECT Date, 'Oasis', 'Oasis AGG' AS SchemeSubset, 
    DateVolAgg AS DateVol, DateRetailAgg AS DateRetail, MTDVolAgg AS MTDVol, MTDRetailAgg AS MTDRetail, 
    AvgPolPerDayAgg AS AvgPolPerDay, AvgPAgg AS AvgP, EstMonEndVolAgg AS EstMonEndVol, 
    EstMonEndRetailAgg AS EstMonEndRetail, EstMonEndNetMarginAgg AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetVol,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVol / BudgetVol) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetail / BudgetGWP) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,

ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMargin / BudgetNetMargin) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalOasisRow
JOIN Oasis2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Agg Volume'
JOIN Oasis2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Agg GWP'
JOIN Oasis2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Agg Net Margin'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'Oasis', 'Oasis Direct' AS SchemeSubset, 
    DateVolDirect AS DateVol, DateRetailDirect AS DateRetail, MTDVolDirect AS MTDVol, MTDRetailDirect AS MTDRetail, 
    AvgPolPerDayDirect AS AvgPolPerDay, AvgPDirect AS AvgP, EstMonEndVolDirect AS EstMonEndVol, 
    EstMonEndRetailDirect AS EstMonEndRetail, EstMonEndNetMarginDirect AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        ELSE '0'  -- Ensure no NULL values
    END, ',', '') AS INT), 0) AS BudgetGWP,

    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVol / BudgetVol) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetail / BudgetGWP) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,

ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMargin / BudgetNetMargin) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalOasisRow
JOIN Oasis2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Direct Volume'
JOIN Oasis2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Direct GWP'
JOIN Oasis2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Direct Net Margin'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'Oasis', 'Oasis Total' AS SchemeSubset, 
    COALESCE(DateVolAgg, 0) + COALESCE(DateVolDirect, 0) AS DateVol, 
    COALESCE(DateRetailAgg, 0) + COALESCE(DateRetailDirect, 0) AS DateRetail, 
    COALESCE(MTDVolAgg, 0) + COALESCE(MTDVolDirect, 0) AS MTDVol, 
    ROUND(COALESCE(MTDRetailAgg, 0) + COALESCE(MTDRetailDirect, 0),2) AS MTDRetail, 
    ((COALESCE(AvgPolPerDayAgg, 0) + COALESCE(AvgPolPerDayDirect, 0)) / 2) AS AvgPolPerDay, 
    ((COALESCE(AvgPAgg, 0) + COALESCE(AvgPDirect, 0)) / 2) AS AvgP, 
    COALESCE(EstMonEndVolAgg, 0) + COALESCE(EstMonEndVolDirect, 0) AS EstMonEndVol, 
    COALESCE(EstMonEndRetailAgg, 0) + COALESCE(EstMonEndRetailDirect, 0) AS EstMonEndRetail, 
    ROUND(COALESCE(EstMonEndNetMarginAgg, 0) + COALESCE(EstMonEndNetMarginDirect, 0),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        ELSE NULL  -- Don't return '0' unless necessary
    END, ',', '') AS INT), NULL) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVol / BudgetVol) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
    ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetail / BudgetGWP) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMargin / BudgetNetMargin) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalOasisRow
LEFT JOIN Oasis2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'TOTAL VOLUME'
LEFT JOIN Oasis2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'TOTAL GWP'
LEFT JOIN Oasis2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'TOTAL NET MARGIN'
WHERE Date = '${report_date}'
UNION ALL 
---------------------------------Atoz--------------------------------------------------
SELECT Date, 'Atoz', 'Atoz' AS SchemeSubset, DateVol, DateRetail, MTDVol, MTDRetail, AvgPolPerDay, AvgP, EstMonEndVol, EstMonEndRetail, EstMonEndNetMargin,
NULL as BudgetVol, NULL as BudgetGWP, NULL as BudgetNetMargin, NULL as PercentToVol, NULL as PercentToGWP, NULL as PercentToNetMargin
FROM TotalAtozRow WHERE Date = '${report_date}' 

UNION ALL

SELECT Date, 'Atoz', 'Atoz AGG' AS SchemeSubset, 
    DateVolAgg AS DateVol, DateRetailAgg AS DateRetail, MTDVolAgg AS MTDVol, MTDRetailAgg AS MTDRetail, 
    AvgPolPerDayAgg AS AvgPolPerDay, AvgPAgg AS AvgP, EstMonEndVolAgg AS EstMonEndVol, 
    EstMonEndRetailAgg AS EstMonEndRetail, EstMonEndNetMarginAgg AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
             WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
             WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
             ELSE NULL
        END, ',', '') AS INT) AS BudgetVol,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
             WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
             WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
             ELSE NULL
        END, ',', '') AS INT) AS BudgetGWP,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
        ELSE NULL
    END, ',', '') AS INT), 0) AS BudgetNetMargin,

    ROUND(CASE WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 THEN (EstMonEndVol / BudgetVol) * 100 ELSE NULL END, 2) AS PercentToVol,
    ROUND(CASE WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 THEN (EstMonEndRetail / BudgetGWP) * 100 ELSE NULL END, 2) AS PercentToGWP,
    ROUND(CASE WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 THEN (EstMonEndNetMargin / BudgetNetMargin) * 100 ELSE NULL END, 2) AS PercentToNetMargin
FROM TotalAtozRow
JOIN AtoZ2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Agg Volume'
JOIN AtoZ2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Agg GWP'
JOIN AtoZ2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Agg Net Margin'
WHERE Date = '${report_date}'

UNION ALL

SELECT Date, 'Atoz', 'Atoz Direct' AS SchemeSubset, 
    DateVolDirect AS DateVol, DateRetailDirect AS DateRetail, MTDVolDirect AS MTDVol, MTDRetailDirect AS MTDRetail, 
    AvgPolPerDayDirect AS AvgPolPerDay, AvgPDirect AS AvgP, EstMonEndVolDirect AS EstMonEndVol, 
    EstMonEndRetailDirect AS EstMonEndRetail, EstMonEndNetMarginDirect AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
             WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
             WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
             ELSE NULL
        END, ',', '') AS INT) AS BudgetVol,
    COALESCE(CAST(REPLACE(
        CASE WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
             WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
             WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
             ELSE '0'
        END, ',', '') AS INT), 0) AS BudgetGWP,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
        ELSE NULL
    END, ',', '') AS INT), 0) AS BudgetNetMargin,

    ROUND(CASE WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 THEN (EstMonEndVol / BudgetVol) * 100 ELSE NULL END, 2) AS PercentToVol,
    ROUND(CASE WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 THEN (EstMonEndRetail / BudgetGWP) * 100 ELSE NULL END, 2) AS PercentToGWP,
    ROUND(CASE WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 THEN (EstMonEndNetMargin / BudgetNetMargin) * 100 ELSE NULL END, 2) AS PercentToNetMargin
FROM TotalAtozRow
JOIN AtoZ2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Direct Volume'
JOIN AtoZ2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Direct GWP'
JOIN AtoZ2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Direct Net Margin'
WHERE Date = '${report_date}'

UNION ALL

SELECT Date, 'Atoz', 'Atoz Total' AS SchemeSubset, 
COALESCE(DateVolAgg, 0) + COALESCE(DateVolDirect, 0) AS DateVol, 
    COALESCE(DateRetailAgg, 0) + COALESCE(DateRetailDirect, 0) AS DateRetail, 
    COALESCE(MTDVolAgg, 0) + COALESCE(MTDVolDirect, 0) AS MTDVol, 
    ROUND(COALESCE(MTDRetailAgg, 0) + COALESCE(MTDRetailDirect, 0),2) AS MTDRetail, 
    ((COALESCE(AvgPolPerDayAgg, 0) + COALESCE(AvgPolPerDayDirect, 0)) / 2) AS AvgPolPerDay, 
    ((COALESCE(AvgPAgg, 0) + COALESCE(AvgPDirect, 0)) / 2) AS AvgP, 
    COALESCE(EstMonEndVolAgg, 0) + COALESCE(EstMonEndVolDirect, 0) AS EstMonEndVol, 
    COALESCE(EstMonEndRetailAgg, 0) + COALESCE(EstMonEndRetailDirect, 0) AS EstMonEndRetail, 
    ROUND(COALESCE(EstMonEndNetMarginAgg, 0) + COALESCE(EstMonEndNetMarginDirect, 0),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        ELSE NULL  -- Don't return '0' unless necessary
    END, ',', '') AS INT), NULL) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            ELSE NULL
        END, ',', '') AS INT) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVol / BudgetVol) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
    ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetail / BudgetGWP) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMargin / BudgetNetMargin) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin

FROM TotalAtozRow
LEFT JOIN AtoZ2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'TOTAL VOLUME'
LEFT JOIN AtoZ2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'TOTAL GWP'
LEFT JOIN AtoZ2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'TOTAL NET MARGIN'
WHERE Date = '${report_date}'

""")
display(report_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Insert new data into DimSchemeSubSet Dimension

# CELL ********************

from pyspark.sql.functions import row_number, max
from pyspark.sql.window import Window

# Define your workspace and file paths
WORKSPACE = "Taurus_TravelInsurance_Dev"
DEST_LAKEHOUSE = "TaurusGoldLH"
DEST_TABLE = "DimSchemeSubSet"

# Define file paths
dest_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{DEST_LAKEHOUSE}.Lakehouse/Tables/{DEST_TABLE}"

# Step 1: Register report_data as a temporary table
report_df.createOrReplaceTempView("report_df_temp")

# Step 2: Extract distinct SchemeSubset from report_data
distinct_schemesubset = spark.sql("""
    SELECT DISTINCT SchemeSubset
    FROM report_df_temp
    WHERE SchemeSubset IS NOT NULL
""")

# Debugging Step: Check data counts
print(f"Total distinct SchemeSubset values: {distinct_schemesubset.count()}")

try:
    # Step 3: Read existing DimSchemeSubSet table
    existing_dim_df = spark.read.format("delta").load(dest_file_path).select("SchemeSubset", "SchemeSubsetId")

    # Debugging Step: Verify existing table
    print(f"Total existing SchemeSubset values: {existing_dim_df.count()}")

    # Step 4: Identify new SchemeSubset values not already in DimSchemeSubSet
    new_entries_df = distinct_schemesubset.join(existing_dim_df, "SchemeSubset", "left_anti")  # Filter out existing records

    # Debugging Step: Check new entries
    print(f"New entries found: {new_entries_df.count()}")

    if new_entries_df.count() > 0:
        # Get the highest existing SchemeSubsetId
        max_id = existing_dim_df.agg(max("SchemeSubsetId")).collect()[0][0] or 0

        # Assign sequential SchemeSubsetId
        window_spec = Window.orderBy("SchemeSubset")
        new_entries_df = new_entries_df.withColumn("SchemeSubsetId", row_number().over(window_spec) + max_id)

        # Ensure correct schema before writing
        new_entries_df = new_entries_df.select("SchemeSubsetId", "SchemeSubset")

        # Append new records to DimSchemeSubSet
        new_entries_df.write \
            .format("delta") \
            .mode("append") \
            .save(dest_file_path)

        print(f"Inserted {new_entries_df.count()} new SchemeSubset records into {DEST_TABLE}.")
    else:
        print(f"No new SchemeSubset records found. Table is up to date.")

except Exception as e:
    print(f"Error encountered: {str(e)}")
    
    # Step 5: If DimSchemeSubSet does not exist, create it from new entries
    window_spec = Window.orderBy("SchemeSubset")
    schemesubset_dimension = distinct_schemesubset.withColumn("SchemeSubsetId", row_number().over(window_spec))

    # Save the new table
    schemesubset_dimension.write.format("delta").mode("overwrite").save(dest_file_path)
    
    print(f"DimSchemeSubSet table created at: {dest_file_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DimSubSet
from pyspark.sql.functions import row_number, max
from pyspark.sql.window import Window

# Define your workspace and file paths
WORKSPACE = "Taurus_TravelInsurance_Dev"
DEST_LAKEHOUSE = "TaurusGoldLH"
DEST_TABLE = "DimSubSet"

# Define file paths
dest_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{DEST_LAKEHOUSE}.Lakehouse/Tables/{DEST_TABLE}"

# Step 1: Register report_data as a temporary table
report_df.createOrReplaceTempView("report_df_temp")

# Step 2: Extract distinct TotalSubSet from report_data
distinct_totalsubset = spark.sql("""
    SELECT DISTINCT TotalSubSet
    FROM report_df_temp
    WHERE TotalSubSet IS NOT NULL
""")

# Debugging Step: Check data counts
print(f"Total distinct TotalSubSet values: {distinct_totalsubset.count()}")

try:
    # Step 3: Read existing DimSubSet table
    existing_dim_df = spark.read.format("delta").load(dest_file_path).select("TotalSubSet", "TotalSubSetId")

    # Debugging Step: Verify existing table
    print(f"Total existing TotalSubSet values: {existing_dim_df.count()}")

    # Step 4: Identify new TotalSubSet values not already in DimSubSet
    new_entries_df = distinct_totalsubset.join(existing_dim_df, "TotalSubSet", "left_anti")  # Filter out existing records

    # Debugging Step: Check new entries
    print(f"New entries found: {new_entries_df.count()}")

    if new_entries_df.count() > 0:
        # Get the highest existing TotalSubSetId
        max_id = existing_dim_df.agg(max("TotalSubSetId")).collect()[0][0] or 0

        # Assign sequential TotalSubSetId
        window_spec = Window.orderBy("TotalSubSet")
        new_entries_df = new_entries_df.withColumn("TotalSubSetId", row_number().over(window_spec) + max_id)

        # Ensure correct schema before writing
        new_entries_df = new_entries_df.select("TotalSubSetId", "TotalSubSet")

        # Append new records to DimSubSet
        new_entries_df.write \
            .format("delta") \
            .mode("append") \
            .save(dest_file_path)

        print(f"Inserted {new_entries_df.count()} new TotalSubSet records into {DEST_TABLE}.")
    else:
        print(f"No new TotalSubSet records found. Table is up to date.")

except Exception as e:
    print(f"Error encountered: {str(e)}")
    
    # Step 5: If DimSubSet does not exist, create it from new entries
    window_spec = Window.orderBy("TotalSubSet")
    totalsubset_dimension = distinct_totalsubset.withColumn("TotalSubSetId", row_number().over(window_spec))

    # Save the new table
    totalsubset_dimension.write.format("delta").mode("overwrite").save(dest_file_path)
    
    print(f"DimSubSet table created at: {dest_file_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define workspace and file paths
WORKSPACE = "Taurus_TravelInsurance_Dev"
DEST_LAKEHOUSE = "TaurusGoldLH"
DEST_TABLE = "FactDailyReport"

dest_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{DEST_LAKEHOUSE}.Lakehouse/Tables/{DEST_TABLE}"

# Load DimDate, DimSubSet and DimSchemeSubSet
dim_date_df = spark.read.format("delta").load(f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{DEST_LAKEHOUSE}.Lakehouse/Tables/DimDate")
dim_schemesubset_df = spark.read.format("delta").load(f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{DEST_LAKEHOUSE}.Lakehouse/Tables/DimSchemeSubSet")
dim_subset_df = spark.read.format("delta").load(f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{DEST_LAKEHOUSE}.Lakehouse/Tables/DimSubSet")


# Register as temp tables
dim_date_df.createOrReplaceTempView("dim_date_temp")
dim_schemesubset_df.createOrReplaceTempView("dim_schemesubset_temp")
dim_subset_df.createOrReplaceTempView("dim_subset_temp")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fact Daily Report

# CELL ********************

# fact_daily_report_df = spark.sql("""
#     SELECT 
#         d.DateId,
#         s.SchemeSubsetId,
#         r.DateVol,
#         r.DateRetail,
#         r.MTDVol,
#         r.MTDRetail,
#         r.AvgPolPerDay,
#         r.AvgP,
#         r.EstMonEndVol,
#         r.EstMonEndRetail,
#         r.EstMonEndNetMargin,
#         r.BudgetVol,
#         r.BudgetGWP,
#         r.BudgetNetMargin,
#         r.PercentToVol,
#         r.PercentToGWP,
#         r.PercentToNetMargin
#     FROM report_df_temp r
#     JOIN dim_date_temp d ON r.Date = d.Date
#     JOIN dim_schemesubset_temp s ON r.SchemeSubset = s.SchemeSubset
# """)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_daily_sales_report_df = spark.sql("""
    SELECT 
        d.DateId,
        s.SchemeSubsetId,
        sub.TotalSubSetId,
        r.DateVol,
        r.DateRetail,
        r.MTDVol,
        r.MTDRetail,
        r.AvgPolPerDay,
        r.AvgP,
        r.EstMonEndVol,
        r.EstMonEndRetail,
        r.EstMonEndNetMargin,
        r.BudgetVol,
        r.BudgetGWP,
        r.BudgetNetMargin,
        r.PercentToVol,
        r.PercentToGWP,
        r.PercentToNetMargin
    FROM report_df_temp r
    JOIN dim_date_temp d ON r.Date = d.Date
    JOIN dim_schemesubset_temp s ON r.SchemeSubset = s.SchemeSubset
    JOIN dim_subset_temp sub ON r.TotalSubSet = sub.TotalSubSet
    WHERE s.SchemeSubset IN ('Trusted', 'SOI', 'Viva', 'Oasis', 'Atoz', 'TotalSubSet')
""")
display(fact_daily_sales_report_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_daily_forecast_report_df = spark.sql("""
    SELECT 
        d.DateId,
        s.SchemeSubsetId,
        sub.TotalSubSetId,
        r.DateVol,
        r.DateRetail,
        r.MTDVol,
        r.MTDRetail,
        r.AvgPolPerDay,
        r.AvgP,
        r.EstMonEndVol,
        r.EstMonEndRetail,
        r.EstMonEndNetMargin,
        r.BudgetVol,
        r.BudgetGWP,
        r.BudgetNetMargin,
        r.PercentToVol,
        r.PercentToGWP,
        r.PercentToNetMargin
    FROM report_df_temp r
    JOIN dim_date_temp d ON r.Date = d.Date
    JOIN dim_schemesubset_temp s ON r.SchemeSubset = s.SchemeSubset
    JOIN dim_subset_temp sub ON r.TotalSubSet = sub.TotalSubSet
    WHERE s.SchemeSubset IN (
        'Trusted AGG', 'Trusted Direct', 'Trusted Total', 
        'Atoz AGG', 'Atoz Direct', 'Atoz Total', 
        'Oasis AGG', 'Oasis Direct', 'Oasis Total', 
        'SOI AGG', 'SOI Direct', 'SOI Total', 
        'Viva AGG', 'Viva Direct', 'Viva Total'
    )
""")
display(fact_daily_forecast_report_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Delete Process Date data from FactDailyReport

# CELL ********************

clean_date = report_date.replace("-", "")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
    DELETE FROM TaurusGoldLH.FactDailySalesReport
    WHERE DateId  = '{clean_date}'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
    DELETE FROM TaurusGoldLH.FactDailyForcastReport
    WHERE DateId  = '{clean_date}'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # Load the schema of the Delta table
# from pyspark.sql.types import DecimalType, LongType, StringType
# import pyspark.sql.functions as F
# existing_df = spark.read.format("delta").load(dest_file_path)
# existing_schema = existing_df.schema

# # Manually cast columns in the DataFrame to match the existing schema
# for field in existing_schema.fields:
#     if isinstance(field.dataType, DecimalType):
#         fact_daily_report_df = fact_daily_report_df.withColumn(
#             field.name, 
#             F.col(field.name).cast(DecimalType(field.dataType.precision, field.dataType.scale))
#         )
#     elif isinstance(field.dataType, LongType):
#         fact_daily_report_df = fact_daily_report_df.withColumn(
#             field.name, 
#             F.col(field.name).cast(LongType())
#         )
#     elif isinstance(field.dataType, StringType):
#         fact_daily_report_df = fact_daily_report_df.withColumn(
#             field.name, 
#             F.col(field.name).cast(StringType())
#         )

# # Now, try writing to the Delta table
# fact_daily_report_df.write \
#     .format("delta") \
#     .mode("append") \
#     .save(dest_file_path)

# print(f"Inserted {fact_daily_report_df.count()} new records into FactDailyReport.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the schema of the Delta table
from pyspark.sql.types import DecimalType, LongType, StringType
import pyspark.sql.functions as F

# Define destination path for FactDailySalesReport
sales_dest_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{DEST_LAKEHOUSE}.Lakehouse/Tables/FactDailySalesReport"

# Load existing schema
existing_df = spark.read.format("delta").load(sales_dest_path)
existing_schema = existing_df.schema

# Manually cast columns in the DataFrame to match the existing schema
for field in existing_schema.fields:
    if isinstance(field.dataType, DecimalType):
        fact_daily_sales_report_df = fact_daily_sales_report_df.withColumn(
            field.name, 
            F.col(field.name).cast(DecimalType(field.dataType.precision, field.dataType.scale))
        )
    elif isinstance(field.dataType, LongType):
        fact_daily_sales_report_df = fact_daily_sales_report_df.withColumn(
            field.name, 
            F.col(field.name).cast(LongType())
        )
    elif isinstance(field.dataType, StringType):
        fact_daily_sales_report_df = fact_daily_sales_report_df.withColumn(
            field.name, 
            F.col(field.name).cast(StringType())
        )

# Write to Delta table
fact_daily_sales_report_df.write \
    .format("delta") \
    .mode("append") \
    .save(sales_dest_path)

print(f"Inserted {fact_daily_sales_report_df.count()} new records into FactDailySalesReport.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Load the schema of the Delta table
from pyspark.sql.types import DecimalType, LongType, StringType
import pyspark.sql.functions as F

# Define destination path for FactDailyForecastReport
forecast_dest_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{DEST_LAKEHOUSE}.Lakehouse/Tables/FactDailyForecastReport"

# Load existing schema
existing_df = spark.read.format("delta").load(forecast_dest_path)
existing_schema = existing_df.schema

# Manually cast columns in the DataFrame to match the existing schema
for field in existing_schema.fields:
    if isinstance(field.dataType, DecimalType):
        fact_daily_forecast_report_df = fact_daily_forecast_report_df.withColumn(
            field.name, 
            F.col(field.name).cast(DecimalType(field.dataType.precision, field.dataType.scale))
        )
    elif isinstance(field.dataType, LongType):
        fact_daily_forecast_report_df = fact_daily_forecast_report_df.withColumn(
            field.name, 
            F.col(field.name).cast(LongType())
        )
    elif isinstance(field.dataType, StringType):
        fact_daily_forecast_report_df = fact_daily_forecast_report_df.withColumn(
            field.name, 
            F.col(field.name).cast(StringType())
        )

# Write to Delta table
fact_daily_forecast_report_df.write \
    .format("delta") \
    .mode("append") \
    .save(forecast_dest_path)

print(f"Inserted {fact_daily_forecast_report_df.count()} new records into FactDailyForecastReport.")


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

# # Step 1: Convert Spark DataFrame to Pandas DataFrame
# df_pandas = report_df.toPandas()

# # Step 2: Format numeric columns
# df_pandas["MTDRetail"] = df_pandas["MTDRetail"].map("{:,.2f}".format)
# df_pandas["EstMonEndRetail"] = df_pandas["EstMonEndRetail"].map("{:,.2f}".format)

# # Step 3: Convert to HTML table and clean newlines
# html_table = df_pandas.to_html(index=False, border=1).replace("\n", "").replace("\r", "")

# # Step 4: Create full HTML
# html = f"""<h2>Daily Sales Report - {report_date}</h2>{html_table}"""

# # Step 5: Encode the HTML in Base64
# import base64
# encoded_html = base64.b64encode(html.strip().encode('utf-8')).decode('utf-8')

# # Step 6: Exit notebook with the encoded HTML
# from notebookutils import mssparkutils
# mssparkutils.notebook.exit(encoded_html)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from factdailysalesreport
# MAGIC where dateid = 20250501


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from factdailyforecastreport
# MAGIC where dateid = 20250501

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from DimSchemeSubSet

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC WITH SchemeClassification AS (
# MAGIC SELECT 
# MAGIC         TravelBrandID,
# MAGIC         CASE 
# MAGIC             WHEN TravelBrand = 'start_travel' THEN 'Start'
# MAGIC             WHEN TravelBrand = 'trusted_ins' THEN 'Trusted'
# MAGIC             WHEN TravelBrand = 'viva_ins' THEN 'Viva'
# MAGIC             WHEN TravelBrand = 'switched_on' THEN 'SOI'
# MAGIC             WHEN TravelBrand = 'OasisAgg' THEN 'Oasis AGG'
# MAGIC             ElSE 'UNKNOWN'
# MAGIC         END AS SchemeSubset
# MAGIC     FROM DimTravelBrand
# MAGIC ),
# MAGIC ForcastSummary AS 
# MAGIC (
# MAGIC     SELECT 
# MAGIC     CASE 
# MAGIC         WHEN Brand = 'ViVA' THEN 'Viva' 
# MAGIC         WHEN Brand = 'Switched On' THEN 'SOI' 
# MAGIC         WHEN Brand = 'Start Travel' THEN 'Start' 
# MAGIC         WHEN Brand = 'Trusted' THEN 'Trusted' 
# MAGIC         WHEN Brand = 'Oasis' THEN 'Oasis' 
# MAGIC         WHEN Brand = 'A to Z' THEN 'Atoz' 
# MAGIC         ELSE 'Unknown'
# MAGIC     END AS BrandNew,
# MAGIC     
# MAGIC     SUM(CASE WHEN Type = 'GPW' THEN `25-May` ELSE 0 END) AS GPW,
# MAGIC     SUM(CASE WHEN Type = 'Taurus Gross Retained' THEN `25-May` ELSE 0 END) AS `TaurusGrossRetained`,
# MAGIC     SUM(CASE WHEN Type = 'Taurus Net Retained' THEN `25-May` ELSE 0 END) AS `TaurusNetRetained`,
# MAGIC     SUM(CASE WHEN Type = 'Taurus Retained' THEN `25-May` ELSE 0 END) AS `TaurusRetained`
# MAGIC FROM ForecastSummary2025
# MAGIC WHERE Brand IN ('ViVA','Switched On','Start Travel','Trusted','Oasis','A to Z') 
# MAGIC   AND Type IN ('GPW', 'Taurus Gross Retained', 'Taurus Net Retained','Taurus Retained')
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN Brand = 'ViVA' THEN 'Viva' 
# MAGIC         WHEN Brand = 'Switched On' THEN 'SOI' 
# MAGIC         WHEN Brand = 'Start Travel' THEN 'Start' 
# MAGIC         WHEN Brand = 'Trusted' THEN 'Trusted' 
# MAGIC         WHEN Brand = 'Oasis' THEN 'Oasis' 
# MAGIC         WHEN Brand = 'A to Z' THEN 'Atoz' 
# MAGIC         ELSE 'Unknown'
# MAGIC     END
# MAGIC ),
# MAGIC TO
# MAGIC select dd.Date,sc.SchemeSubset,SUM(fds.Volume) AS DateVol
# MAGIC         
# MAGIC FROM FactDailySales fds
# MAGIC     JOIN SchemeClassification sc ON fds.TravelBrandID = sc.TravelBrandID
# MAGIC     LEFT JOIN TaurusGoldLH.factdailysoiagg fda ON fds.DateId = fda.DateId
# MAGIC     LEFT JOIN TaurusGoldLH.FactDailysoiDirect fdd ON fds.DateId = fdd.DateId
# MAGIC     JOIN DimDate dd ON fds.DateId = dd.DateId
# MAGIC     JOIN ForcastSummary fcs on fcs.BrandNew = sc.SchemeSubset
# MAGIC     where sc.SchemeSubset = 'SOI' AND dd.DATEID = 20250501
# MAGIC     GROUP BY dd.Date, sc.SchemeSubset
# MAGIC 
# MAGIC SELECT Date, 'SOI', 'SOI' AS SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,EstMonEndNetMargin,
# MAGIC NULL as BudgetVol,NULL as BudgetGWP,NULL as BudgetNetMargin,NULL as PercentToVol,NULL as PercentToGWP,NULL as PercentToNetMargin
# MAGIC FROM TotalOasisRow WHERE Date = '${report_date}' 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select dateid from factdailysalesreport


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
