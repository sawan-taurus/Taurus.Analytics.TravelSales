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

# report_date = "2026-07-01"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("report_date", report_date)

month_col = spark.sql( "SELECT DATE_FORMAT('${report_date}', 'MMM-yy')" ).collect()[0][0]

print(month_col)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# <!-- pletion mean 
# spark.conf.set("report_date", report_date) -->

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
    
    SUM(CASE WHEN Type = 'GPW' THEN Feb26 ELSE 0 END) AS GPW,
    SUM(CASE WHEN Type = 'Taurus Gross Retained' THEN Feb26 ELSE 0 END) AS `TaurusGrossRetained`,
    SUM(CASE WHEN Type = 'Taurus Net Retained' THEN Feb26 ELSE 0 END) AS `TaurusNetRetained`,
    SUM(CASE WHEN Type = 'Taurus Retained' THEN Feb26 ELSE 0 END) AS `TaurusRetained`
FROM TravelForecastSummary
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
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / NULLIF(SUM(fdo.VolumeMonth), 0),2) AS AvgP,
        ROUND(SUM(fdo.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVol,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / NULLIF(SUM(fdo.VolumeMonth), 0),2)  * 
        ROUND(SUM(fdo.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndRetail,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / NULLIF(SUM(fdo.VolumeMonth), 0) * SUM(fdo.VolumeMonth) / CAST(day(dd.Date) AS INT) * 
            day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / NULLIF(GPW, 0)) * 0.9) , 2) AS EstMonEndNetMargin,
        CONCAT(CAST(ROUND(((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / NULLIF(SUM(fdo.VolumeMonth), 0) * SUM(fdo.VolumeMonth) / DAY(dd.Date) * 
            DAY(LAST_DAY(dd.Date)) / 1.2 * ((TaurusRetained / NULLIF(GPW, 0)) * 0.9) / NULLIF(TaurusRetained, 0)) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        CONCAT(CAST(ROUND(((((SUM(fdo.TotalGrossIncIPTMonth) * 0.89) / NULLIF(SUM(fdo.VolumeMonth), 0)) * (SUM(fdo.VolumeMonth) / DAY(dd.Date)) * 
            DAY(LAST_DAY(dd.Date))) / 1.2 * ((TaurusRetained / NULLIF(GPW, 0)) * 0.9) / NULLIF(TaurusRetained, 0)) * 100, 0) AS INT), '%') AS PerToFCastMargin,
        ROUND(GPW * 1.2,1) as PerToFCastNo,
        GPW ,TaurusRetained,TaurusRetained,
        CONCAT(ROUND((TaurusRetained / NULLIF(GPW, 0)) * 0.9 * 100, 2),'%') AS AvGrossMargin,
        CONCAT(ROUND((TaurusRetained / NULLIF(GPW, 0)) * 0.9 * 100, 2),'%')  AS AvNetMargin,

        --AGG
        SUM(fda.Volume) as DateVolAgg,
        ROUND(SUM(fda.GWPincIPTDay) * 0.89, 2) AS DateRetailAgg,
        SUM(fda.VolumeMonth) AS MTDVolAgg,
        ROUND(SUM(fda.TotalGrossIncIPTMonth) * 0.89, 2) AS MTDRetailAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth) * 0.89) / NULLIF(SUM(fda.VolumeMonth), 0),2) AS AvgPAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth) * 0.89) / NULLIF(SUM(fda.VolumeMonth), 0) * SUM(fda.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetailAgg ,
        ROUND((SUM(fda.TotalGrossIncIPTMonth) * 0.89) / NULLIF(SUM(fda.VolumeMonth), 0) * SUM(fda.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndNetMarginAgg,

        --Direct
        SUM(fdd.Volume) as DateVolDirect,
        ROUND(SUM(fdd.GWPincIPTDay) * 0.89, 2) AS DateRetailDirect,
        SUM(fdd.VolumeMonth) AS MTDVolDirect,
        ROUND(SUM(fdd.TotalGrossIncIPTMonth) * 0.89, 2) AS MTDRetailDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / NULLIF(SUM(fdd.VolumeMonth), 0),2) AS AvgPDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / NULLIF(SUM(fdd.VolumeMonth), 0) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndRetailDirect ,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth) * 0.89) / NULLIF(SUM(fdd.VolumeMonth), 0) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndNetMarginDirect

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
        ROUND(SUM(fdo.GWPincIPTDay), 2) AS DateRetail,
        SUM(fdo.VolumeMonth) AS MTDVol,
        ROUND(SUM(fdo.TotalGrossIncIPTMonth), 2) AS MTDRetail,
        ROUND(SUM(fdo.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDay,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth)) / NULLIF(SUM(fdo.VolumeMonth), 0),2) AS AvgP,
        ROUND(SUM(fdo.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVol,
        ROUND(SUM(fdo.TotalGrossIncIPTMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) ,2) AS EstMonEndRetail,
        ROUND((SUM(fdo.TotalGrossIncIPTMonth)) / NULLIF(SUM(fdo.VolumeMonth), 0) * SUM(fdo.VolumeMonth) / CAST(day(dd.Date - 1) AS INT) * 
            day(last_day(dd.Date)) / 1.2 * ((TaurusRetained / NULLIF(GPW, 0)) * 0.9) , 2) AS EstMonEndNetMargin,
        CONCAT(CAST(ROUND(((SUM(fdo.TotalGrossIncIPTMonth)) / NULLIF(SUM(fdo.VolumeMonth), 0) * SUM(fdo.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date)) / 1.2 * ((TaurusRetained / NULLIF(GPW, 0)) * 0.9) / NULLIF(TaurusRetained, 0)) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        CONCAT(CAST(ROUND(((SUM(fdo.TotalGrossIncIPTMonth)) / NULLIF(SUM(fdo.VolumeMonth), 0) * SUM(fdo.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date)) / 1.2 * ((TaurusRetained / NULLIF(GPW, 0))) / NULLIF(TaurusRetained, 0)) * 100, 2) AS DECIMAL(10,2)), '%') AS PerToFCastMargin,
        ROUND(GPW * 1.2,1) as PerToFCastNo,
        GPW ,TaurusRetained,TaurusRetained,
        CONCAT(ROUND((TaurusRetained / NULLIF(GPW, 0)) * 0.9 * 100, 2),'%') AS AvGrossMargin,
        CONCAT(ROUND((TaurusRetained / NULLIF(GPW, 0)) * 0.9 * 100, 2),'%')  AS AvNetMargin,

        --AGG
        SUM(fda.Volume) as DateVolAgg,
        ROUND(SUM(fda.GWPincIPTDay), 2) AS DateRetailAgg,
        SUM(fda.VolumeMonth) AS MTDVolAgg,
        ROUND(SUM(fda.TotalGrossIncIPTMonth), 2) AS MTDRetailAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth)) / NULLIF(SUM(fda.VolumeMonth), 0),2) AS AvgPAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolAgg,
        ROUND(SUM(fda.TotalGrossIncIPTMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) ,2) AS EstMonEndRetailAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth)) / NULLIF(SUM(fda.VolumeMonth), 0) * SUM(fda.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndNetMarginAgg,

        --Direct
        SUM(fdd.Volume) as DateVolDirect,
        ROUND(SUM(fdd.GWPincIPTDay), 2) AS DateRetailDirect,
        SUM(fdd.VolumeMonth) AS MTDVolDirect,
        ROUND(SUM(fdd.TotalGrossIncIPTMonth), 2) AS MTDRetailDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth)) / NULLIF(SUM(fdd.VolumeMonth), 0),2) AS AvgPDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolDirect,
        ROUND(SUM(fdd.TotalGrossIncIPTMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) ,2) AS EstMonEndRetailDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth)) / NULLIF(SUM(fdd.VolumeMonth), 0) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndNetMarginDirect
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
        ROUND(SUM(fds.TotalGrossIncIPTMonth) / NULLIF(SUM(fds.VolumeMonth), 0), 2) AS AvgP,
        ROUND(SUM(fds.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVol,
        ROUND(SUM(fds.TotalGrossIncIPTMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) ,2) AS EstMonEndRetail,
        ROUND((SUM(fds.TotalGrossIncIPTMonth) / NULLIF(SUM(fds.VolumeMonth), 0) * SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * 
            day(last_day(dd.Date))) / 1.2 * ((TaurusNetRetained / NULLIF(GPW, 0)) * 0.9), 2) AS EstMonEndNetMargin,
        CONCAT(CAST(ROUND((SUM(fds.TotalGrossIncIPTMonth) / NULLIF(SUM(fds.VolumeMonth), 0) * SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date)) / NULLIF((GPW * 1.2), 0)) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        CONCAT(CAST(ROUND(((SUM(fds.TotalGrossIncIPTMonth) / NULLIF(SUM(fds.VolumeMonth), 0) * SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date))) / 1.2 * ((TaurusGrossRetained / NULLIF(GPW, 0)) * 0.9) / NULLIF(TaurusGrossRetained, 0)) * 100, 0) AS INT), '%') 
                AS PerToFCastMargin,
        ROUND(GPW * 1.2,1) as PerToFCastNo,
        GPW,
        TaurusGrossRetained,
        TaurusNetRetained,
        CONCAT(ROUND((TaurusGrossRetained / NULLIF(GPW, 0)) * 0.9 * 100, 2),'%') AS AvGrossMargin,
        CONCAT(ROUND((TaurusNetRetained / NULLIF(GPW, 0)) * 0.9 * 100, 2),'%')  AS AvNetMargin,

    
        --AGG
        SUM(fda.Volume) as DateVolAgg,
        ROUND(SUM(fda.GWPincIPTDay), 2) AS DateRetailAgg,
        SUM(fda.VolumeMonth) AS MTDVolAgg,
        ROUND(SUM(fda.TotalGrossIncIPTMonth), 2) AS MTDRetailAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth)) / NULLIF(SUM(fda.VolumeMonth), 0),2) AS AvgPAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolAgg,
        ROUND(SUM(fda.TotalGrossIncIPTMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) ,2) AS EstMonEndRetailAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth)) / NULLIF(SUM(fda.VolumeMonth), 0) * SUM(fda.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndNetMarginAgg,

        --Direct
        SUM(fdd.Volume) as DateVolDirect,
        ROUND(SUM(fdd.GWPincIPTDay), 2) AS DateRetailDirect,
        SUM(fdd.VolumeMonth) AS MTDVolDirect,
        ROUND(SUM(fdd.TotalGrossIncIPTMonth), 2) AS MTDRetailDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth)) / NULLIF(SUM(fdd.VolumeMonth), 0),2) AS AvgPDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolDirect,
        ROUND(SUM(fdd.TotalGrossIncIPTMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) ,2) AS EstMonEndRetailDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth)) / NULLIF(SUM(fdd.VolumeMonth), 0) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndNetMarginDirect
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
        ROUND(SUM(fds.TotalGrossIncIPTMonth) / NULLIF(SUM(fds.VolumeMonth), 0), 2) AS AvgP,
        ROUND(SUM(fds.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVol,
        ROUND(SUM(fds.TotalGrossIncIPTMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) ,2) AS EstMonEndRetail,

        ROUND((SUM(fds.TotalGrossIncIPTMonth) / NULLIF(SUM(fds.VolumeMonth), 0) * SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * 
            day(last_day(dd.Date))) / 1.2 * ((TaurusNetRetained / NULLIF(GPW, 0)) * 0.9), 2) AS EstMonEndNetMargin,
        CONCAT(CAST(ROUND((SUM(fds.TotalGrossIncIPTMonth) / NULLIF(SUM(fds.VolumeMonth), 0) * SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date)) / NULLIF((GPW * 1.2), 0)) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        CONCAT(CAST(ROUND(((SUM(fds.TotalGrossIncIPTMonth) / NULLIF(SUM(fds.VolumeMonth), 0) * SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date))) / 1.2 * ((TaurusGrossRetained / NULLIF(GPW, 0)) * 0.9) / NULLIF(TaurusGrossRetained, 0)) * 100, 0) AS INT), '%') 
                AS PerToFCastMargin,
        ROUND(GPW * 1.2,1) as PerToFCastNo,
        GPW,
        TaurusGrossRetained,
        TaurusNetRetained,
        CONCAT(ROUND((TaurusGrossRetained / NULLIF(GPW, 0)) * 0.9 * 100, 2),'%') AS AvGrossMargin,
        CONCAT(ROUND((TaurusNetRetained / NULLIF(GPW, 0)) * 0.9 * 100, 2),'%')  AS AvNetMargin,

    
        --AGG
        SUM(fda.Volume) as DateVolAgg,
        ROUND(SUM(fda.GWPincIPTDay), 2) AS DateRetailAgg,
        SUM(fda.VolumeMonth) AS MTDVolAgg,
        ROUND(SUM(fda.TotalGrossIncIPTMonth), 2) AS MTDRetailAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth)) / NULLIF(SUM(fda.VolumeMonth), 0),2) AS AvgPAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolAgg,
        ROUND(SUM(fda.TotalGrossIncIPTMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) ,2) AS EstMonEndRetailAgg,

        ROUND((SUM(fda.TotalGrossIncIPTMonth)) / NULLIF(SUM(fda.VolumeMonth), 0) * SUM(fda.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndNetMarginAgg,

        --Direct
        SUM(fdd.Volume) as DateVolDirect,
        ROUND(SUM(fdd.GWPincIPTDay), 2) AS DateRetailDirect,
        SUM(fdd.VolumeMonth) AS MTDVolDirect,
        ROUND(SUM(fdd.TotalGrossIncIPTMonth), 2) AS MTDRetailDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth)) / NULLIF(SUM(fdd.VolumeMonth), 0),2) AS AvgPDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolDirect,
        ROUND(SUM(fdd.TotalGrossIncIPTMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) ,2) AS EstMonEndRetailDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth)) / NULLIF(SUM(fdd.VolumeMonth), 0) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndNetMarginDirect
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
        ROUND(SUM(fds.TotalGrossIncIPTMonth) / NULLIF(SUM(fds.VolumeMonth), 0), 2) AS AvgP,
        ROUND(SUM(fds.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVol,
        ROUND(SUM(fds.TotalGrossIncIPTMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) ,2) AS EstMonEndRetail,

        ROUND((SUM(fds.TotalGrossIncIPTMonth) / NULLIF(SUM(fds.VolumeMonth), 0) * SUM(fds.VolumeMonth / CAST(day(dd.Date - 1) AS INT)) * 
            day(last_day(dd.Date))) / 1.2 * ((TaurusNetRetained / NULLIF(GPW, 0)) * 0.9), 2) AS EstMonEndNetMargin,
        CONCAT(CAST(ROUND((SUM(fds.TotalGrossIncIPTMonth) / NULLIF(SUM(fds.VolumeMonth), 0) * SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date)) / NULLIF((GPW * 1.2), 0)) * 100, 0) AS INT), '%') AS PerToFCastRetail,
        CONCAT(CAST(ROUND(((SUM(fds.TotalGrossIncIPTMonth) / NULLIF(SUM(fds.VolumeMonth), 0) * SUM(fds.VolumeMonth) / DAY(dd.Date - 1) * 
            DAY(LAST_DAY(dd.Date))) / 1.2 * ((TaurusGrossRetained / NULLIF(GPW, 0)) * 0.9) / NULLIF(TaurusGrossRetained, 0)) * 100, 0) AS INT), '%') 
                AS PerToFCastMargin,
        ROUND(GPW * 1.2,1) as PerToFCastNo,
        GPW,
        TaurusGrossRetained,
        TaurusNetRetained,
        CONCAT(ROUND((TaurusGrossRetained / NULLIF(GPW, 0)) * 0.9 * 100, 2),'%') AS AvGrossMargin,
        CONCAT(ROUND((TaurusNetRetained / NULLIF(GPW, 0)) * 0.9 * 100, 2),'%')  AS AvNetMargin,

    
        --AGG
        SUM(fda.Volume) as DateVolAgg,
        ROUND(SUM(fda.GWPincIPTDay), 2) AS DateRetailAgg,
        SUM(fda.VolumeMonth) AS MTDVolAgg,
        ROUND(SUM(fda.TotalGrossIncIPTMonth), 2) AS MTDRetailAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayAgg,
        ROUND((SUM(fda.TotalGrossIncIPTMonth)) / NULLIF(SUM(fda.VolumeMonth), 0),2) AS AvgPAgg,
        ROUND(SUM(fda.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolAgg,
        ROUND(SUM(fda.TotalGrossIncIPTMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) ,2) AS EstMonEndRetailAgg,

        ROUND((SUM(fda.TotalGrossIncIPTMonth)) / NULLIF(SUM(fda.VolumeMonth), 0) * SUM(fda.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndNetMarginAgg,

        --Direct
        SUM(fdd.Volume) as DateVolDirect,
        ROUND(SUM(fdd.GWPincIPTDay), 2) AS DateRetailDirect,
        SUM(fdd.VolumeMonth) AS MTDVolDirect,
        ROUND(SUM(fdd.TotalGrossIncIPTMonth), 2) AS MTDRetailDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT), 0)  AS AvgPolPerDayDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth)) / NULLIF(SUM(fdd.VolumeMonth), 0),2) AS AvgPDirect,
        ROUND(SUM(fdd.VolumeMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date))) AS EstMonEndVolDirect,
        ROUND(SUM(fdd.TotalGrossIncIPTMonth) / CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)) ,2) AS EstMonEndRetailDirect,
        ROUND((SUM(fdd.TotalGrossIncIPTMonth)) / NULLIF(SUM(fdd.VolumeMonth), 0) * SUM(fdd.VolumeMonth) / 
            CAST(day(dd.Date) AS INT) * day(last_day(dd.Date)),2) AS EstMonEndNetMarginDirect
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
        ROUND(SUM(MTDRetail) / NULLIF(SUM(MTDVol), 0),2) AS AvgP,
        ROUND(SUM(MTDVol) / CAST(day(Date) AS INT) * day(last_day(Date))) AS EstMonEndVol,
        ROUND(SUM(MTDRetail) / CAST(day(Date) AS INT) * day(last_day(Date)) ,2) AS EstMonEndRetail,
        ROUND(SUM(EstMonEndNetMargin),2) as EstMonEndNetMargin,
        CONCAT(CAST(ROUND((SUM(MTDRetail) / NULLIF(SUM(MTDVol), 0) * SUM(MTDVol) / DAY(Date - 1) * DAY(LAST_DAY(Date)) / NULLIF(SUM(GPW), 0) * 1.2) * 100, 0) AS INT), '%') AS PerToFCastRetail,
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
        ROUND((SUM(MTDRetailAgg)) / NULLIF(SUM(MTDVolAgg), 0),2) AS AvgPAgg,
        ROUND(SUM(MTDVolAgg) / CAST(day(Date) AS INT) * day(last_day(Date))) AS EstMonEndVolAgg,
        ROUND(SUM(EstMonEndRetailAgg),2) AS EstMonEndRetailAgg ,
        ROUND(SUM(EstMonEndNetMarginAgg), 2) AS EstMonEndNetMarginAgg,

        --Direct
        SUM(DateVolDirect) AS DateVolDirect,
        ROUND(SUM(DateRetailDirect), 2) AS DateRetailDirect,
        SUM(MTDVolDirect) AS MTDVolDirect,
        ROUND(SUM(MTDRetailDirect), 2) AS MTDRetailDirect,
        ROUND(SUM(MTDVolDirect) / CAST(day(Date) AS INT), 0)  AS AvgPolPerDayDirect,
        ROUND((SUM(MTDRetailDirect) * 0.89) / NULLIF(SUM(MTDVolDirect), 0),2) AS AvgPDirect,
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
),
EverythingInOne AS (
---------------------------------Trusted--------------------------------------------------
SELECT Date, 'Trusted' as TotalSubSet, 'Trusted' AS SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,
ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`

                    ELSE '0'
                END,'%', ''),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)  AS EstMonEndNetMargin,

    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummaryTotal.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummaryTotal.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummaryTotal.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummaryTotal.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummaryTotal.`Sep-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummaryTotal.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummaryTotal.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummaryTotal.`Dec-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummaryTotal.`Jan-26`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummaryTotal.`Feb-26`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummaryTotal.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummaryTotal.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummaryTotal.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummaryTotal.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummaryTotal.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummaryTotal.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummaryTotal.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummaryTotal.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummaryTotal.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummaryTotal.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummaryTotal.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummaryTotal.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummaryTotal.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummaryTotal.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummaryTotal.`Sep-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummaryTotal.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummaryTotal.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummaryTotal.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummaryTotal.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummaryTotal.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummaryTotal.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummaryTotal.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummaryTotal.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummaryTotal.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummaryTotal.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummaryTotal.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummaryTotal.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummaryTotal.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummaryTotal.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummaryTotal.`Dec-26`
        ELSE NULL  -- Don't return '0' unless necessary
    END, ',', '') AS DOUBLE), NULL) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummaryTotal.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummaryTotal.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummaryTotal.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummaryTotal.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummaryTotal.`Sep-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummaryTotal.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummaryTotal.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummaryTotal.`Dec-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummaryTotal.`Jan-26`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummaryTotal.`Feb-26`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummaryTotal.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummaryTotal.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummaryTotal.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummaryTotal.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummaryTotal.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummaryTotal.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummaryTotal.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummaryTotal.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummaryTotal.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummaryTotal.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolAgg + EstMonEndVolDirect)  / NULLIF(BudgetVol, 0) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
    ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN ((EstMonEndRetailAgg + EstMonEndRetailDirect) / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN 
        (ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2))/ NULLIF(BudgetNetMargin, 0) *100
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalTrustedRow 
LEFT JOIN Trusted2025_Budget_Summary BudgetSummaryTotal ON BudgetSummaryTotal.Type = 'TOTAL VOLUME'
LEFT JOIN Trusted2025_Budget_Summary GWPBudgetSummaryTotal ON GWPBudgetSummaryTotal.Type = 'TOTAL GWP'
LEFT JOIN Trusted2025_Budget_Summary NetMarginBudgetSummaryTotal ON NetMarginBudgetSummaryTotal.Type = 'TOTAL NET MARGIN'
LEFT JOIN Trusted2025_Budget_Summary BudgetSummaryAgg ON BudgetSummaryAgg.Type = 'Agg Volume'
LEFT JOIN Trusted2025_Budget_Summary GWPBudgetSummaryAgg ON GWPBudgetSummaryAgg.Type = 'Agg GWP'
LEFT JOIN Trusted2025_Budget_Summary NetMarginBudgetSummaryAgg ON NetMarginBudgetSummaryAgg.Type = 'Agg Net Margin'
LEFT JOIN Trusted2025_Budget_Summary BudgetSummaryDirect ON BudgetSummaryDirect.Type = 'Direct Volume'
LEFT JOIN Trusted2025_Budget_Summary GWPBudgetSummaryDirect ON GWPBudgetSummaryDirect.Type = 'Direct GWP'
LEFT JOIN Trusted2025_Budget_Summary NetMarginBudgetSummaryDirect ON NetMarginBudgetSummaryDirect.Type = 'Direct Net Margin'
LEFT JOIN Trusted2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
LEFT JOIN Trusted2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}' 

UNION ALL
SELECT Date, 'Trusted', 'Trusted AGG' AS SchemeSubset, 
    DateVolAgg AS DateVol, DateRetailAgg AS DateRetail, MTDVolAgg AS MTDVol, MTDRetailAgg AS MTDRetail, 
    AvgPolPerDayAgg AS AvgPolPerDay, AvgPAgg AS AvgP, EstMonEndVolAgg AS EstMonEndVol, 
    EstMonEndRetailAgg AS EstMonEndRetail, ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25` 
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    CAST(REPLACE(
        CASE 
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolAgg / NULLIF(BudgetVol, 0)) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetailAgg / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100) / NULLIF(BudgetNetMargin, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalTrustedRow
JOIN Trusted2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Agg Volume'
JOIN Trusted2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Agg GWP'
JOIN Trusted2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Agg Net Margin'
JOIN Trusted2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
JOIN Trusted2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'Trusted', 'Trusted Direct' AS SchemeSubset, 
    DateVolDirect AS DateVol, DateRetailDirect AS DateRetail, MTDVolDirect AS MTDVol, MTDRetailDirect AS MTDRetail, 
    AvgPolPerDayDirect AS AvgPolPerDay, AvgPDirect AS AvgP, EstMonEndVolDirect AS EstMonEndVol, 
    EstMonEndRetailDirect AS EstMonEndRetail, ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummary.`Dec-26`
        ELSE '0'  -- Ensure no NULL values
    END, ',', '') AS DOUBLE), 0.0) AS BudgetGWP,

    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolDirect / NULLIF(BudgetVol, 0)) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetailDirect / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100) / NULLIF(BudgetNetMargin, 0) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalTrustedRow
JOIN Trusted2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Direct Volume'
JOIN Trusted2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Direct GWP'
JOIN Trusted2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Direct Net Margin'
JOIN Trusted2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
JOIN Trusted2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'Trusted', 'Trusted Total' AS SchemeSubset, 
    COALESCE(DateVolAgg, 0) + COALESCE(DateVolDirect, 0) AS DateVol, 
    COALESCE(DateRetailAgg, 0) + COALESCE(DateRetailDirect, 0) AS DateRetail, 
    COALESCE(MTDVolAgg, 0) + COALESCE(MTDVolDirect, 0) AS MTDVol, 
    ROUND(COALESCE(MTDRetailAgg, 0) + COALESCE(MTDRetailDirect, 0),2) AS MTDRetail, 
    AvgPolPerDayAgg + AvgPolPerDayDirect AS AvgPolPerDay, 
    (COALESCE(MTDRetailAgg, 0) + COALESCE(MTDRetailDirect, 0)) / NULLIF((COALESCE(MTDVolAgg, 0) + COALESCE(MTDVolDirect, 0)), 0) AS AvgP, 
    COALESCE(EstMonEndVolAgg, 0) + COALESCE(EstMonEndVolDirect, 0) AS EstMonEndVol, 
    COALESCE(EstMonEndRetailAgg, 0) + COALESCE(EstMonEndRetailDirect, 0) AS EstMonEndRetail, 
    ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummary.`Sep-25`
                   WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummary.`Oct-25`
                WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummary.`Dec-26`
        ELSE NULL  -- Don't return '0' unless necessary
    END, ',', '') AS DOUBLE), NULL) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummary.`Dec-26`

            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolAgg + EstMonEndVolDirect)  / NULLIF(BudgetVol, 0) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
    ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN ((EstMonEndRetailAgg + EstMonEndRetailDirect) / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN 
        (ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2))/ NULLIF(BudgetNetMargin, 0) *100


        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalTrustedRow
LEFT JOIN Trusted2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'TOTAL VOLUME'
LEFT JOIN Trusted2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'TOTAL GWP'
LEFT JOIN Trusted2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'TOTAL NET MARGIN'
JOIN Trusted2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
JOIN Trusted2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}'
UNION ALL 

---------------------------------Switched On--------------------------------------------------

SELECT Date, 'SOI', 'SOI' AS SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,
ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)  AS EstMonEndNetMargin,

    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummaryTotal.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummaryTotal.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummaryTotal.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummaryTotal.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummaryTotal.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummaryTotal.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummaryTotal.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummaryTotal.`Dec-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummaryTotal.`Jan-26`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummaryTotal.`Feb-26`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummaryTotal.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummaryTotal.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummaryTotal.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummaryTotal.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummaryTotal.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummaryTotal.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummaryTotal.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummaryTotal.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummaryTotal.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummaryTotal.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummaryTotal.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummaryTotal.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummaryTotal.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummaryTotal.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummaryTotal.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummaryTotal.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummaryTotal.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummaryTotal.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummaryTotal.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummaryTotal.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummaryTotal.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummaryTotal.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummaryTotal.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummaryTotal.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummaryTotal.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummaryTotal.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummaryTotal.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummaryTotal.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummaryTotal.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummaryTotal.`Dec-26`
        ELSE NULL  -- Don't return '0' unless necessary
    END, ',', '') AS DOUBLE), NULL) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummaryTotal.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummaryTotal.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummaryTotal.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummaryTotal.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummaryTotal.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummaryTotal.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummaryTotal.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummaryTotal.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummaryTotal.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummaryTotal.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummaryTotal.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummaryTotal.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummaryTotal.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummaryTotal.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummaryTotal.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummaryTotal.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummaryTotal.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummaryTotal.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummaryTotal.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummaryTotal.`Dec-26`

            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolAgg + EstMonEndVolDirect)  / NULLIF(BudgetVol, 0) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
    ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN ((EstMonEndRetailAgg + EstMonEndRetailDirect) / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN 
        (ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2))/ NULLIF(BudgetNetMargin, 0) *100
        ELSE NULL 
    END, 2) AS PercentToNetMargin

FROM TotalSOIRow 
LEFT JOIN SOI2025_Budget_Summary BudgetSummaryTotal ON BudgetSummaryTotal.Type = 'TOTAL VOLUME'
LEFT JOIN SOI2025_Budget_Summary GWPBudgetSummaryTotal ON GWPBudgetSummaryTotal.Type = 'TOTAL GWP'
LEFT JOIN SOI2025_Budget_Summary NetMarginBudgetSummaryTotal ON NetMarginBudgetSummaryTotal.Type = 'TOTAL NET MARGIN'
LEFT JOIN SOI2025_Budget_Summary BudgetSummaryAgg ON BudgetSummaryAgg.Type = 'Agg Volume'
LEFT JOIN SOI2025_Budget_Summary GWPBudgetSummaryAgg ON GWPBudgetSummaryAgg.Type = 'Agg GWP'
LEFT JOIN SOI2025_Budget_Summary NetMarginBudgetSummaryAgg ON NetMarginBudgetSummaryAgg.Type = 'Agg Net Margin'
LEFT JOIN SOI2025_Budget_Summary BudgetSummaryDirect ON BudgetSummaryDirect.Type = 'Direct Volume'
LEFT JOIN SOI2025_Budget_Summary GWPBudgetSummaryDirect ON GWPBudgetSummaryDirect.Type = 'Direct GWP'
LEFT JOIN SOI2025_Budget_Summary NetMarginBudgetSummaryDirect ON NetMarginBudgetSummaryDirect.Type = 'Direct Net Margin'
LEFT JOIN SOI2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
LEFT JOIN SOI2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}' 

UNION ALL
SELECT Date, 'SOI', 'SOI AGG' AS SchemeSubset, 
    DateVolAgg AS DateVol, DateRetailAgg AS DateRetail, MTDVolAgg AS MTDVol, MTDRetailAgg AS MTDRetail, 
    AvgPolPerDayAgg AS AvgPolPerDay, AvgPAgg AS AvgP, EstMonEndVolAgg AS EstMonEndVol, 
    EstMonEndRetailAgg AS EstMonEndRetail, ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummary.`Sep-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummary.`Oct-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummary.`Nov-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummary.`Dec-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummary.`Jan-26`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummary.`Feb-26`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolAgg / NULLIF(BudgetVol, 0)) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        CASE 
            WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
            THEN (EstMonEndRetailAgg * 100.0) / NULLIF(BudgetGWP, 0)
            ELSE NULL 
        END AS PercentToGWP,

ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100) / NULLIF(BudgetNetMargin, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalSOIRow
JOIN SOI2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Agg Volume'
JOIN SOI2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Agg GWP'
JOIN SOI2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Agg Net Margin'
JOIN SOI2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
JOIN SOI2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'SOI', 'SOI Direct' AS SchemeSubset, 
    DateVolDirect AS DateVol, DateRetailDirect AS DateRetail, MTDVolDirect AS MTDVol, MTDRetailDirect AS MTDRetail, 
    AvgPolPerDayDirect AS AvgPolPerDay, AvgPDirect AS AvgP, EstMonEndVolDirect AS EstMonEndVol, 
    EstMonEndRetailDirect AS EstMonEndRetail, ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummary.`Dec-26`
        ELSE '0'  -- Ensure no NULL values
    END, ',', '') AS DOUBLE), 0.0) AS BudgetGWP,

    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolDirect / NULLIF(BudgetVol, 0)) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetailDirect / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,

ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100) / NULLIF(BudgetNetMargin, 0) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalSOIRow
JOIN SOI2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Direct Volume'
JOIN SOI2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Direct GWP'
JOIN SOI2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Direct Net Margin'
JOIN SOI2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
JOIN SOI2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'SOI', 'SOI Total' AS SchemeSubset, 
    COALESCE(DateVolAgg, 0) + COALESCE(DateVolDirect, 0) AS DateVol, 
    COALESCE(DateRetailAgg, 0) + COALESCE(DateRetailDirect, 0) AS DateRetail, 
    COALESCE(MTDVolAgg, 0) + COALESCE(MTDVolDirect, 0) AS MTDVol, 
    ROUND(COALESCE(MTDRetailAgg, 0) + COALESCE(MTDRetailDirect, 0),2) AS MTDRetail, 
    AvgPolPerDayAgg + AvgPolPerDayDirect AS AvgPolPerDay, 
    (COALESCE(MTDRetailAgg, 0) + COALESCE(MTDRetailDirect, 0)) / NULLIF((COALESCE(MTDVolAgg, 0) + COALESCE(MTDVolDirect, 0)), 0)   AS AvgP, 
    COALESCE(EstMonEndVolAgg, 0) + COALESCE(EstMonEndVolDirect, 0) AS EstMonEndVol, 
    COALESCE(EstMonEndRetailAgg, 0) + COALESCE(EstMonEndRetailDirect, 0) AS EstMonEndRetail, 
    ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)  AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummary.`Dec-26`
        ELSE NULL  -- Don't return '0' unless necessary
    END, ',', '') AS DOUBLE), NULL) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolAgg + EstMonEndVolDirect)  / NULLIF(BudgetVol, 0) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
    ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN ((EstMonEndRetailAgg + EstMonEndRetailDirect) / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN 
        (ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2))/ NULLIF(BudgetNetMargin, 0) *100


        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalSOIRow
LEFT JOIN SOI2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'TOTAL VOLUME'
LEFT JOIN SOI2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'TOTAL GWP'
LEFT JOIN SOI2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'TOTAL NET MARGIN'
JOIN SOI2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
JOIN SOI2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}'
UNION ALL 

---------------------------------Viva--------------------------------------------------

SELECT Date, 'Viva', 'Viva' AS SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,
ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)  AS EstMonEndNetMargin,

    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummaryTotal.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummaryTotal.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummaryTotal.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummaryTotal.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummaryTotal.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummaryTotal.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummaryTotal.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummaryTotal.`Dec-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummaryTotal.`Jan-26`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummaryTotal.`Feb-26`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummaryTotal.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummaryTotal.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummaryTotal.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummaryTotal.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummaryTotal.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummaryTotal.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummaryTotal.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummaryTotal.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummaryTotal.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummaryTotal.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummaryTotal.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummaryTotal.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummaryTotal.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummaryTotal.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummaryTotal.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummaryTotal.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummaryTotal.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummaryTotal.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummaryTotal.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummaryTotal.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummaryTotal.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummaryTotal.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummaryTotal.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummaryTotal.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummaryTotal.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummaryTotal.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummaryTotal.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummaryTotal.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummaryTotal.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummaryTotal.`Dec-26`
        ELSE NULL  -- Don't return '0' unless necessary
    END, ',', '') AS DOUBLE), NULL) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummaryTotal.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummaryTotal.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummaryTotal.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummaryTotal.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummaryTotal.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummaryTotal.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummaryTotal.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummaryTotal.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummaryTotal.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummaryTotal.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummaryTotal.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummaryTotal.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummaryTotal.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummaryTotal.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummaryTotal.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummaryTotal.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummaryTotal.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummaryTotal.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummaryTotal.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummaryTotal.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolAgg + EstMonEndVolDirect)  / NULLIF(BudgetVol, 0) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
    ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN ((EstMonEndRetailAgg + EstMonEndRetailDirect) / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN 
        (ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2))/ NULLIF(BudgetNetMargin, 0) *100
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalVivaRow 
LEFT JOIN ViVA2025_Budget_Summary BudgetSummaryTotal ON BudgetSummaryTotal.Type = 'TOTAL VOLUME'
LEFT JOIN ViVA2025_Budget_Summary GWPBudgetSummaryTotal ON GWPBudgetSummaryTotal.Type = 'TOTAL GWP'
LEFT JOIN ViVA2025_Budget_Summary NetMarginBudgetSummaryTotal ON NetMarginBudgetSummaryTotal.Type = 'TOTAL NET MARGIN'
LEFT JOIN ViVA2025_Budget_Summary BudgetSummaryAgg ON BudgetSummaryAgg.Type = 'Agg Volume'
LEFT JOIN ViVA2025_Budget_Summary GWPBudgetSummaryAgg ON GWPBudgetSummaryAgg.Type = 'Agg GWP'
LEFT JOIN ViVA2025_Budget_Summary NetMarginBudgetSummaryAgg ON NetMarginBudgetSummaryAgg.Type = 'Agg Net Margin'
LEFT JOIN ViVA2025_Budget_Summary BudgetSummaryDirect ON BudgetSummaryDirect.Type = 'Direct Volume'
LEFT JOIN ViVA2025_Budget_Summary GWPBudgetSummaryDirect ON GWPBudgetSummaryDirect.Type = 'Direct GWP'
LEFT JOIN ViVA2025_Budget_Summary NetMarginBudgetSummaryDirect ON NetMarginBudgetSummaryDirect.Type = 'Direct Net Margin'
LEFT JOIN ViVA2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
LEFT JOIN ViVA2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}' 

UNION ALL
SELECT Date, 'Viva', 'Viva AGG' AS SchemeSubset, 
    DateVolAgg AS DateVol, DateRetailAgg AS DateRetail, MTDVolAgg AS MTDVol, MTDRetailAgg AS MTDRetail, 
    AvgPolPerDayAgg AS AvgPolPerDay, AvgPAgg AS AvgP, EstMonEndVolAgg AS EstMonEndVol, 
    EstMonEndRetailAgg AS EstMonEndRetail, ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummary.`Dec-26`

            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolAgg / NULLIF(BudgetVol, 0)) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetailAgg / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,

ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100) / NULLIF(BudgetNetMargin, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalVivaRow
JOIN ViVA2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Agg Volume'
JOIN ViVA2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Agg GWP'
JOIN ViVA2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Agg Net Margin'
JOIN ViVA2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
JOIN ViVA2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'Viva', 'Viva Direct' AS SchemeSubset, 
    DateVolDirect AS DateVol, DateRetailDirect AS DateRetail, MTDVolDirect AS MTDVol, MTDRetailDirect AS MTDRetail, 
    AvgPolPerDayDirect AS AvgPolPerDay, AvgPDirect AS AvgP, EstMonEndVolDirect AS EstMonEndVol, 
    EstMonEndRetailDirect AS EstMonEndRetail, ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummary.`Dec-26`

        ELSE '0'  -- Ensure no NULL values
    END, ',', '') AS DOUBLE), 0.0) AS BudgetGWP,

    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolDirect / NULLIF(BudgetVol, 0)) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetailDirect / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,

ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100) / NULLIF(BudgetNetMargin, 0) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalVivaRow
JOIN ViVA2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Direct Volume'
JOIN ViVA2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Direct GWP'
JOIN ViVA2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Direct Net Margin'
JOIN ViVA2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
JOIN ViVA2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'Viva', 'Viva Total' AS SchemeSubset, 
    COALESCE(DateVolAgg, 0) + COALESCE(DateVolDirect, 0) AS DateVol, 
    COALESCE(DateRetailAgg, 0) + COALESCE(DateRetailDirect, 0) AS DateRetail, 
    COALESCE(MTDVolAgg, 0) + COALESCE(MTDVolDirect, 0) AS MTDVol, 
    ROUND(COALESCE(MTDRetailAgg, 0) + COALESCE(MTDRetailDirect, 0),2) AS MTDRetail, 
    AvgPolPerDayAgg + AvgPolPerDayDirect AS AvgPolPerDay, 
    (COALESCE(MTDRetailAgg, 0) + COALESCE(MTDRetailDirect, 0)) / NULLIF((COALESCE(MTDVolAgg, 0) + COALESCE(MTDVolDirect, 0)), 0) AS AvgP, 
    COALESCE(EstMonEndVolAgg, 0) + COALESCE(EstMonEndVolDirect, 0) AS EstMonEndVol, 
    COALESCE(EstMonEndRetailAgg, 0) + COALESCE(EstMonEndRetailDirect, 0) AS EstMonEndRetail, 
    ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)  AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummary.`Dec-26`
        ELSE NULL  -- Don't return '0' unless necessary
    END, ',', '') AS DOUBLE), NULL) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolAgg + EstMonEndVolDirect)  / NULLIF(BudgetVol, 0) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
    ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN ((EstMonEndRetailAgg + EstMonEndRetailDirect) / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN 
        (ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2))/ NULLIF(BudgetNetMargin, 0) *100


        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalVivaRow
LEFT JOIN ViVA2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'TOTAL VOLUME'
LEFT JOIN ViVA2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'TOTAL GWP'
LEFT JOIN ViVA2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'TOTAL NET MARGIN'
JOIN ViVA2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
JOIN ViVA2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}'
UNION ALL 

---------------------------------Oasis--------------------------------------------------

SELECT Date, 'Oasis', 'Oasis' AS SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail,AvgPolPerDay,AvgP,EstMonEndVol,EstMonEndRetail,
ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)  AS EstMonEndNetMargin,

    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummaryTotal.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummaryTotal.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummaryTotal.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummaryTotal.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummaryTotal.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummaryTotal.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummaryTotal.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummaryTotal.`Dec-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummaryTotal.`Jan-26`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummaryTotal.`Feb-26`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummaryTotal.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummaryTotal.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummaryTotal.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummaryTotal.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummaryTotal.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummaryTotal.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummaryTotal.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummaryTotal.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummaryTotal.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummaryTotal.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummaryTotal.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummaryTotal.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummaryTotal.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummaryTotal.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummaryTotal.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummaryTotal.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummaryTotal.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummaryTotal.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummaryTotal.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummaryTotal.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummaryTotal.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummaryTotal.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummaryTotal.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummaryTotal.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummaryTotal.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummaryTotal.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummaryTotal.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummaryTotal.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummaryTotal.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummaryTotal.`Dec-26`
        ELSE NULL  -- Don't return '0' unless necessary
    END, ',', '') AS DOUBLE), NULL) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummaryTotal.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummaryTotal.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummaryTotal.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummaryTotal.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummaryTotal.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummaryTotal.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummaryTotal.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummaryTotal.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummaryTotal.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummaryTotal.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummaryTotal.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummaryTotal.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummaryTotal.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummaryTotal.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummaryTotal.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummaryTotal.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummaryTotal.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummaryTotal.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummaryTotal.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummaryTotal.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolAgg + EstMonEndVolDirect)  / NULLIF(BudgetVol, 0) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
    ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN ((EstMonEndRetailAgg + EstMonEndRetailDirect) / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN 
        (ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2))/ NULLIF(BudgetNetMargin, 0) *100
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalOasisRow 
LEFT JOIN Oasis2025_Budget_Summary BudgetSummaryTotal ON BudgetSummaryTotal.Type = 'TOTAL VOLUME'
LEFT JOIN Oasis2025_Budget_Summary GWPBudgetSummaryTotal ON GWPBudgetSummaryTotal.Type = 'TOTAL GWP'
LEFT JOIN Oasis2025_Budget_Summary NetMarginBudgetSummaryTotal ON NetMarginBudgetSummaryTotal.Type = 'TOTAL NET MARGIN'
LEFT JOIN Oasis2025_Budget_Summary BudgetSummaryAgg ON BudgetSummaryAgg.Type = 'Agg Volume'
LEFT JOIN Oasis2025_Budget_Summary GWPBudgetSummaryAgg ON GWPBudgetSummaryAgg.Type = 'Agg GWP'
LEFT JOIN Oasis2025_Budget_Summary NetMarginBudgetSummaryAgg ON NetMarginBudgetSummaryAgg.Type = 'Agg Net Margin'
LEFT JOIN Oasis2025_Budget_Summary BudgetSummaryDirect ON BudgetSummaryDirect.Type = 'Direct Volume'
LEFT JOIN Oasis2025_Budget_Summary GWPBudgetSummaryDirect ON GWPBudgetSummaryDirect.Type = 'Direct GWP'
LEFT JOIN Oasis2025_Budget_Summary NetMarginBudgetSummaryDirect ON NetMarginBudgetSummaryDirect.Type = 'Direct Net Margin'
LEFT JOIN Oasis2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
LEFT JOIN Oasis2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}' 

UNION ALL
SELECT Date, 'Oasis', 'Oasis AGG' AS SchemeSubset, 
    DateVolAgg AS DateVol, DateRetailAgg AS DateRetail, MTDVolAgg AS MTDVol, MTDRetailAgg AS MTDRetail, 
    AvgPolPerDayAgg AS AvgPolPerDay, AvgPAgg AS AvgP, EstMonEndVolAgg AS EstMonEndVol, 
    EstMonEndRetailAgg AS EstMonEndRetail, ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummary.`Dec-26`

            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummary.`Dec-26`

            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummary.`Dec-26`

            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolAgg / NULLIF(BudgetVol, 0)) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetailAgg / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,

ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100) / NULLIF(BudgetNetMargin, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalOasisRow
JOIN Oasis2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Agg Volume'
JOIN Oasis2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Agg GWP'
JOIN Oasis2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Agg Net Margin'
JOIN Oasis2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
JOIN Oasis2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'Oasis', 'Oasis Direct' AS SchemeSubset, 
    DateVolDirect AS DateVol, DateRetailDirect AS DateRetail, MTDVolDirect AS MTDVol, MTDRetailDirect AS MTDRetail, 
    AvgPolPerDayDirect AS AvgPolPerDay, AvgPDirect AS AvgP, EstMonEndVolDirect AS EstMonEndVol, 
    EstMonEndRetailDirect AS EstMonEndRetail, ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummary.`Dec-26`

            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummary.`Dec-26`

        ELSE '0'  -- Ensure no NULL values
    END, ',', '') AS DOUBLE), 0.0) AS BudgetGWP,

    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolDirect / NULLIF(BudgetVol, 0)) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
        ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN (EstMonEndRetailDirect / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,

ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100) / NULLIF(BudgetNetMargin, 0) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalOasisRow
JOIN Oasis2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Direct Volume'
JOIN Oasis2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Direct GWP'
JOIN Oasis2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Direct Net Margin'
JOIN Oasis2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
JOIN Oasis2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}'

UNION ALL
SELECT Date, 'Oasis', 'Oasis Total' AS SchemeSubset, 
    COALESCE(DateVolAgg, 0) + COALESCE(DateVolDirect, 0) AS DateVol, 
    COALESCE(DateRetailAgg, 0) + COALESCE(DateRetailDirect, 0) AS DateRetail, 
    COALESCE(MTDVolAgg, 0) + COALESCE(MTDVolDirect, 0) AS MTDVol, 
    ROUND(COALESCE(MTDRetailAgg, 0) + COALESCE(MTDRetailDirect, 0),2) AS MTDRetail, 
    AvgPolPerDayAgg + AvgPolPerDayDirect AS AvgPolPerDay, 
    (COALESCE(MTDRetailAgg, 0) + COALESCE(MTDRetailDirect, 0)) / NULLIF((COALESCE(MTDVolAgg, 0) + COALESCE(MTDVolDirect, 0)), 0)   AS AvgP, 
    COALESCE(EstMonEndVolAgg, 0) + COALESCE(EstMonEndVolDirect, 0) AS EstMonEndVol, 
    COALESCE(EstMonEndRetailAgg, 0) + COALESCE(EstMonEndRetailDirect, 0) AS EstMonEndRetail, 
    ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)  AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummary.`Dec-26`

        ELSE NULL  -- Don't return '0' unless necessary
    END, ',', '') AS DOUBLE), NULL) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummary.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolAgg + EstMonEndVolDirect)  / NULLIF(BudgetVol, 0) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
    ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN ((EstMonEndRetailAgg + EstMonEndRetailDirect) / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN 
        (ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2))/ NULLIF(BudgetNetMargin, 0) *100


        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalOasisRow
LEFT JOIN Oasis2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'TOTAL VOLUME'
LEFT JOIN Oasis2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'TOTAL GWP'
LEFT JOIN Oasis2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'TOTAL NET MARGIN'
JOIN Oasis2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
JOIN Oasis2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}'
UNION ALL 
---------------------------------Atoz--------------------------------------------------
SELECT Date, 'Atoz', 'Atoz' AS SchemeSubset, DateVol, DateRetail, MTDVol, MTDRetail, AvgPolPerDay, AvgP, EstMonEndVol, EstMonEndRetail, 
ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)  AS EstMonEndNetMargin,

    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummaryTotal.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummaryTotal.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummaryTotal.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummaryTotal.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummaryTotal.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummaryTotal.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummaryTotal.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummaryTotal.`Dec-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummaryTotal.`Jan-26`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummaryTotal.`Feb-26`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummaryTotal.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummaryTotal.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummaryTotal.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummaryTotal.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummaryTotal.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummaryTotal.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummaryTotal.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummaryTotal.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummaryTotal.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummaryTotal.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummaryTotal.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummaryTotal.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummaryTotal.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummaryTotal.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummaryTotal.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummaryTotal.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummaryTotal.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummaryTotal.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummaryTotal.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummaryTotal.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummaryTotal.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummaryTotal.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummaryTotal.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummaryTotal.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummaryTotal.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummaryTotal.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummaryTotal.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummaryTotal.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummaryTotal.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummaryTotal.`Dec-26`

        ELSE NULL  -- Don't return '0' unless necessary
    END, ',', '') AS DOUBLE), NULL) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummaryTotal.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummaryTotal.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummaryTotal.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummaryTotal.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummaryTotal.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummaryTotal.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummaryTotal.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummaryTotal.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummaryTotal.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummaryTotal.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummaryTotal.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummaryTotal.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummaryTotal.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummaryTotal.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummaryTotal.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummaryTotal.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummaryTotal.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummaryTotal.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummaryTotal.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummaryTotal.`Dec-26`
            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolAgg + EstMonEndVolDirect)  / NULLIF(BudgetVol, 0) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
    ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN ((EstMonEndRetailAgg + EstMonEndRetailDirect) / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN 
        (ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2))/ NULLIF(BudgetNetMargin, 0) *100
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalAtozRow 
LEFT JOIN AtoZ2025_Budget_Summary BudgetSummaryTotal ON BudgetSummaryTotal.Type = 'TOTAL VOLUME'
LEFT JOIN AtoZ2025_Budget_Summary GWPBudgetSummaryTotal ON GWPBudgetSummaryTotal.Type = 'TOTAL GWP'
LEFT JOIN AtoZ2025_Budget_Summary NetMarginBudgetSummaryTotal ON NetMarginBudgetSummaryTotal.Type = 'TOTAL NET MARGIN'
LEFT JOIN AtoZ2025_Budget_Summary BudgetSummaryAgg ON BudgetSummaryAgg.Type = 'Agg Volume'
LEFT JOIN AtoZ2025_Budget_Summary GWPBudgetSummaryAgg ON GWPBudgetSummaryAgg.Type = 'Agg GWP'
LEFT JOIN AtoZ2025_Budget_Summary NetMarginBudgetSummaryAgg ON NetMarginBudgetSummaryAgg.Type = 'Agg Net Margin'
LEFT JOIN AtoZ2025_Budget_Summary BudgetSummaryDirect ON BudgetSummaryDirect.Type = 'Direct Volume'
LEFT JOIN AtoZ2025_Budget_Summary GWPBudgetSummaryDirect ON GWPBudgetSummaryDirect.Type = 'Direct GWP'
LEFT JOIN AtoZ2025_Budget_Summary NetMarginBudgetSummaryDirect ON NetMarginBudgetSummaryDirect.Type = 'Direct Net Margin'
LEFT JOIN AtoZ2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
LEFT JOIN AtoZ2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}' 

UNION ALL

SELECT Date, 'Atoz', 'Atoz AGG' AS SchemeSubset, 
    DateVolAgg AS DateVol, DateRetailAgg AS DateRetail, MTDVolAgg AS MTDVol, MTDRetailAgg AS MTDRetail, 
    AvgPolPerDayAgg AS AvgPolPerDay, AvgPAgg AS AvgP, EstMonEndVolAgg AS EstMonEndVol, 
    EstMonEndRetailAgg AS EstMonEndRetail, ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummary.`Dec-26`

            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummary.`Dec-26`

            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetGWP,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummary.`Dec-26`
        ELSE NULL
    END, ',', '') AS DOUBLE), 0.0) AS BudgetNetMargin,

    ROUND(CASE WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 THEN (EstMonEndVolAgg / NULLIF(BudgetVol, 0)) * 100 ELSE NULL END, 2) AS PercentToVol,
    ROUND(CASE WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 THEN (EstMonEndRetailAgg / NULLIF(BudgetGWP, 0)) * 100 ELSE NULL END, 2) AS PercentToGWP,
    ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN (EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100) / NULLIF(BudgetNetMargin, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin
FROM TotalAtozRow
JOIN AtoZ2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Agg Volume'
JOIN AtoZ2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Agg GWP'
JOIN AtoZ2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Agg Net Margin'
JOIN AtoZ2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
JOIN AtoZ2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}'

UNION ALL

SELECT Date, 'Atoz', 'Atoz Direct' AS SchemeSubset, 
    DateVolDirect AS DateVol, DateRetailDirect AS DateRetail, MTDVolDirect AS MTDVol, MTDRetailDirect AS MTDRetail, 
    AvgPolPerDayDirect AS AvgPolPerDay, AvgPDirect AS AvgP, EstMonEndVolDirect AS EstMonEndVol, 
    EstMonEndRetailDirect AS EstMonEndRetail, ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummary.`Dec-26`

            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    COALESCE(CAST(REPLACE(
        CASE WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummary.`Dec-26`

            ELSE '0'
        END, ',', '') AS DOUBLE), 0.0) AS BudgetGWP,
    COALESCE(CAST(REPLACE(
    CASE 
    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummary.`Dec-26`
        ELSE NULL
    END, ',', '') AS DOUBLE), 0.0) AS BudgetNetMargin,

    ROUND(CASE WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 THEN (EstMonEndVolDirect / NULLIF(BudgetVol, 0)) * 100 ELSE NULL END, 2) AS PercentToVol,
    ROUND(CASE WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 THEN (EstMonEndRetailDirect / NULLIF(BudgetGWP, 0)) * 100 ELSE NULL END, 2) AS PercentToGWP,

ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100) / NULLIF(BudgetNetMargin, 0) * 100 
        ELSE NULL 
    END, 2) AS PercentToNetMargin

FROM TotalAtozRow
JOIN AtoZ2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'Direct Volume'
JOIN AtoZ2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'Direct GWP'
JOIN AtoZ2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'Direct Net Margin'
JOIN AtoZ2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
JOIN AtoZ2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}'

UNION ALL

SELECT Date, 'Atoz', 'Atoz Total' AS SchemeSubset, 
COALESCE(DateVolAgg, 0) + COALESCE(DateVolDirect, 0) AS DateVol, 
    COALESCE(DateRetailAgg, 0) + COALESCE(DateRetailDirect, 0) AS DateRetail, 
    COALESCE(MTDVolAgg, 0) + COALESCE(MTDVolDirect, 0) AS MTDVol, 
    ROUND(COALESCE(MTDRetailAgg, 0) + COALESCE(MTDRetailDirect, 0),2) AS MTDRetail, 
    AvgPolPerDayAgg + AvgPolPerDayDirect AS AvgPolPerDay, 
    (COALESCE(MTDRetailAgg, 0) + COALESCE(MTDRetailDirect, 0)) / NULLIF((COALESCE(MTDVolAgg, 0) + COALESCE(MTDVolDirect, 0)), 0)   AS AvgP, 
    COALESCE(EstMonEndVolAgg, 0) + COALESCE(EstMonEndVolDirect, 0) AS EstMonEndVol, 
    COALESCE(EstMonEndRetailAgg, 0) + COALESCE(EstMonEndRetailDirect, 0) AS EstMonEndRetail, 
    ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2) AS EstMonEndNetMargin,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN BudgetSummary.`May-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN BudgetSummary.`Jun-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN BudgetSummary.`Jul-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN BudgetSummary.`Aug-25`
            WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN BudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN BudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN BudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN BudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN BudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN BudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN BudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN BudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN BudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN BudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN BudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN BudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN BudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN BudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN BudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN BudgetSummary.`Dec-26`

            ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetVol,
    COALESCE(CAST(REPLACE(
    CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN GWPBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN GWPBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN GWPBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN GWPBudgetSummary.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN GWPBudgetSummary.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN GWPBudgetSummary.`Oct-25`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN GWPBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN GWPBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN GWPBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN GWPBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN GWPBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN GWPBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN GWPBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN GWPBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN GWPBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN GWPBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN GWPBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN GWPBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN GWPBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN GWPBudgetSummary.`Dec-26`

        ELSE NULL  -- Don't return '0' unless necessary
    END, ',', '') AS DOUBLE), NULL) AS BudgetGWP,
    CAST(REPLACE(
        CASE 
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN NetMarginBudgetSummary.`May-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN NetMarginBudgetSummary.`Jun-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN NetMarginBudgetSummary.`Jul-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN NetMarginBudgetSummary.`Aug-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN NetMarginBudgetSummary.`Sep-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN NetMarginBudgetSummary.`Oct-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN NetMarginBudgetSummary.`Nov-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN NetMarginBudgetSummary.`Dec-25`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN NetMarginBudgetSummary.`Jan-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN NetMarginBudgetSummary.`Feb-26`
        WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN NetMarginBudgetSummary.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN NetMarginBudgetSummary.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN NetMarginBudgetSummary.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN NetMarginBudgetSummary.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN NetMarginBudgetSummary.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN NetMarginBudgetSummary.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN NetMarginBudgetSummary.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN NetMarginBudgetSummary.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN NetMarginBudgetSummary.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN NetMarginBudgetSummary.`Dec-26`
    ELSE NULL
        END, ',', '') AS DOUBLE) AS BudgetNetMargin,
    ROUND(
        CASE 
            WHEN BudgetVol IS NOT NULL AND BudgetVol > 0 
            THEN (EstMonEndVolAgg + EstMonEndVolDirect)  / NULLIF(BudgetVol, 0) * 100 
            ELSE NULL 
        END, 2) AS PercentToVol,
    ROUND(
    CASE 
        WHEN BudgetGWP IS NOT NULL AND BudgetGWP > 0 
        THEN ((EstMonEndRetailAgg + EstMonEndRetailDirect) / NULLIF(BudgetGWP, 0)) * 100 
        ELSE NULL 
    END, 2) AS PercentToGWP,
ROUND(
    CASE 
        WHEN BudgetNetMargin IS NOT NULL AND BudgetNetMargin > 0 
        THEN 
        (ROUND(EstMonEndNetMarginAgg *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN AggNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN AggNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN AggNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN AggNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN AggNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN AggNetPct.`Oct-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN AggNetPct.`Nov-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN AggNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN AggNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN AggNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN AggNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN AggNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN AggNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN AggNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN AggNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN AggNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN AggNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN AggNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN AggNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN AggNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2)
    +
    ROUND(EstMonEndNetMarginDirect *(
    CAST(
        REPLACE(
            REPLACE(
                CASE 
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-25' THEN DirectNetPct.`May-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-25' THEN DirectNetPct.`Jun-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-25' THEN DirectNetPct.`Jul-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-25' THEN DirectNetPct.`Aug-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-25' THEN DirectNetPct.`Sep-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-25' THEN DirectNetPct.`Oct-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-25' THEN DirectNetPct.`Nov-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-25' THEN DirectNetPct.`Dec-25`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jan-26' THEN DirectNetPct.`Jan-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Feb-26' THEN DirectNetPct.`Feb-26`
                    WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Mar-26' THEN DirectNetPct.`Mar-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Apr-26' THEN DirectNetPct.`Apr-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'May-26' THEN DirectNetPct.`May-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jun-26' THEN DirectNetPct.`Jun-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Jul-26' THEN DirectNetPct.`Jul-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Aug-26' THEN DirectNetPct.`Aug-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Sep-26' THEN DirectNetPct.`Sep-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Oct-26' THEN DirectNetPct.`Oct-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Nov-26' THEN DirectNetPct.`Nov-26`
WHEN DATE_FORMAT('${report_date}', 'MMM-yy') = 'Dec-26' THEN DirectNetPct.`Dec-26`
                    ELSE '0'
                END,
                '%', ''
            ),
            ',', ''
        ) AS FLOAT
    ) / 100),2))/ NULLIF(BudgetNetMargin, 0) *100


        ELSE NULL 
    END, 2) AS PercentToNetMargin

FROM TotalAtozRow
LEFT JOIN AtoZ2025_Budget_Summary BudgetSummary ON BudgetSummary.Type = 'TOTAL VOLUME'
LEFT JOIN AtoZ2025_Budget_Summary GWPBudgetSummary ON GWPBudgetSummary.Type = 'TOTAL GWP'
LEFT JOIN AtoZ2025_Budget_Summary NetMarginBudgetSummary ON NetMarginBudgetSummary.Type = 'TOTAL NET MARGIN'
JOIN AtoZ2025_Budget_Summary AggNetPct ON REPLACE(AggNetPct.Type, ' ', '') = 'AggNetPercentage'
JOIN AtoZ2025_Budget_Summary DirectNetPct ON REPLACE(DirectNetPct.Type, ' ', '') = 'DirectNetPercentage'
WHERE Date = '${report_date}'
),
NetMarginValue AS (
    SELECT EstMonEndNetMargin
    FROM EverythingInOne
    WHERE SchemeSubset LIKE '%Total' AND Date = '${report_date}'
)
SELECT t.Date,  'TotalSubSet' AS TotalSubSet,  'TotalSubSet' AS SchemeSubset, t.DateVol, t.DateRetail, t.MTDVol, t.MTDRetail, t.AvgPolPerDay,
ROUND(t.DateRetail / t.DateVol, 2) AS AvgP, t.EstMonEndVol, t.EstMonEndRetail, 
(SELECT SUM(EstMonEndNetMargin) FROM NetMarginValue) / 1.2 AS EstMonEndNetMargin,
(SELECT SUM(BudgetVol) FROM EverythingInOne WHERE SchemeSubset IN ('Trusted Total','SOI Total','Viva Total','Oasis Total','Atoz Total')) AS BudgetVol,
(SELECT SUM(BudgetGWP) FROM EverythingInOne WHERE SchemeSubset IN ('Trusted Total','SOI Total','Viva Total','Oasis Total','Atoz Total')) AS BudgetGWP, 
(SELECT SUM(BudgetNetMargin) FROM EverythingInOne WHERE SchemeSubset IN ('Trusted Total','SOI Total','Viva Total','Oasis Total','Atoz Total')) AS BudgetNetMargin, 
ROUND((t.EstMonEndVol * 100 / (SELECT SUM(BudgetVol) FROM EverythingInOne WHERE SchemeSubset IN ('Trusted Total','SOI Total','Viva Total','Oasis Total','Atoz Total'))), 2) AS PercentToVol,
ROUND((t.EstMonEndRetail * 100 / 1.2 / (SELECT SUM(BudgetGWP) FROM EverythingInOne WHERE SchemeSubset IN ('Trusted Total','SOI Total','Viva Total','Oasis Total','Atoz Total'))), 2) AS PercentToGWP,
ROUND(((SELECT SUM(EstMonEndNetMargin) / 1.2  FROM NetMarginValue) * 100 / (SELECT SUM(BudgetNetMargin) FROM EverythingInOne WHERE SchemeSubset IN ('Trusted Total','SOI Total','Viva Total','Oasis Total','Atoz Total'))), 2) AS PercentToNetMargin

FROM TodaysSubSetsTotal t
WHERE t.Date = '${report_date}'
UNION ALL

SELECT Date, TotalSubSet, SchemeSubset,DateVol,DateRetail,MTDVol,MTDRetail, AvgPolPerDay, AvgP, EstMonEndVol, EstMonEndRetail, EstMonEndNetMargin / 1.2 ,
BudgetVol, BudgetGWP, BudgetNetMargin, PercentToVol, PercentToGWP, PercentToNetMargin
FROM EverythingInOne
UNION ALL

SELECT t.Date,  'TotalBrandSubSet' AS TotalSubSet,  'TotalBrandSubSet' AS SchemeSubset, t.DateVol, t.DateRetail, t.MTDVol, t.MTDRetail, 
t.AvgPolPerDay,ROUND(t.DateRetail / t.DateVol, 2) AS AvgP, t.EstMonEndVol, t.EstMonEndRetail, 
(SELECT SUM(EstMonEndNetMargin)  / 1.2  FROM NetMarginValue) AS EstMonEndNetMargin, 
(SELECT SUM(BudgetVol) FROM EverythingInOne WHERE SchemeSubset IN ('Trusted Total','SOI Total','Viva Total','Oasis Total','Atoz Total')) AS BudgetVol,
(SELECT SUM(BudgetGWP) FROM EverythingInOne WHERE SchemeSubset IN ('Trusted Total','SOI Total','Viva Total','Oasis Total','Atoz Total')) AS BudgetGWP, 
(SELECT SUM(BudgetNetMargin) FROM EverythingInOne WHERE SchemeSubset IN ('Trusted Total','SOI Total','Viva Total','Oasis Total','Atoz Total')) AS BudgetNetMargin,
ROUND((t.EstMonEndVol * 100 / (SELECT SUM(BudgetVol) FROM EverythingInOne WHERE SchemeSubset IN ('Trusted Total','SOI Total','Viva Total','Oasis Total','Atoz Total'))), 2) AS PercentToVol,
ROUND((t.EstMonEndRetail * 100 / 1.2 / (SELECT SUM(BudgetGWP) FROM EverythingInOne WHERE SchemeSubset IN ('Trusted Total','SOI Total','Viva Total','Oasis Total','Atoz Total'))), 2) AS PercentToGWP,
ROUND(((SELECT SUM(EstMonEndNetMargin)  / 1.2  FROM NetMarginValue) * 100 / (SELECT SUM(BudgetNetMargin) FROM EverythingInOne WHERE SchemeSubset IN ('Trusted Total','SOI Total','Viva Total','Oasis Total','Atoz Total'))), 2) AS PercentToNetMargin
FROM TodaysSubSetsTotal t
WHERE t.Date = '${report_date}'

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
    WHERE s.SchemeSubset IN ('Trusted', 'SOI', 'Viva', 'Atoz', 'TotalSubSet')
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
        'Viva AGG', 'Viva Direct', 'Viva Total',
        'TotalBrandSubSet'
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
    DELETE FROM TaurusGoldLH.FactDailyForecastReport
    WHERE DateId  = '{clean_date}'
""")

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
