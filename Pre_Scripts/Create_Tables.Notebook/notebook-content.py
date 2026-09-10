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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.caseSensitive", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **DimDate - Create Table**

# CELL ********************

create_table_query = """
CREATE TABLE DimDate (
    DateId BIGINT,                         -- Unique identifier for each date
    Date DATE NOT NULL,                  -- The actual date
    Year INT NOT NULL,                   -- Year part of the date
    Quarter INT NOT NULL,                -- Quarter of the year (1 to 4)
    Month INT NOT NULL,                  -- Month part of the date (1 to 12)
    DayOfWeek INT NOT NULL,            -- Day of the week (1 = Sunday, 7 = Saturday)
    DayOfMonth INT NOT NULL            -- Day of the month (1 to 31)
)
"""
spark.sql(create_table_query)
spark.sql("SHOW TABLES").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Start a Spark session (if it's not already created in your notebook)
from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("DimDateTable").getOrCreate()

# SQL command to create the 'dim_date' table
create_table_query = """
CREATE TABLE dim_date (
    date_id INT,                         -- Unique identifier for each date
    date DATE NOT NULL,                  -- The actual date
    year INT NOT NULL,                   -- Year part of the date
    quarter INT NOT NULL,                -- Quarter of the year (1 to 4)
    month INT NOT NULL,                  -- Month part of the date (1 to 12)
    day_of_week INT NOT NULL,            -- Day of the week (1 = Sunday, 7 = Saturday)
    day_of_month INT NOT NULL            -- Day of the month (1 to 31)
)
"""

# Execute the SQL command to create the table
spark.sql(create_table_query)

# Verify that the table has been created
spark.sql("SHOW TABLES").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **FactDailySales - Create Table**

# MARKDOWN ********************


# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailySales (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailySales';
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailyOasis (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyOasis';
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailyAtoz (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyAtoz';
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailyDirect (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyDirect';
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailyNew (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyNew';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailyRenewal (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyRenewal';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailyAtozAgg (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyAtozAgg';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailyAtozDirect (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyAtozDirect';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailyOasisAgg (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyOasisAgg';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailyOasisDirect (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyOasisDirect';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailyTrustedAgg (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyTrustedAgg';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailyTrustedDirect (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyTrustedDirect';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailySOIAgg (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailySOIAgg';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailySOIDirect (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailySOIDirect';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailyVivaAgg (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyVivaAgg';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailyVivaDirect (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyVivaDirect';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailyStartAgg (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyStartAgg';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS FactDailyStartDirect (
# MAGIC     TravelBrandID INT,
# MAGIC     DateId BIGINT,
# MAGIC     Volume DOUBLE,
# MAGIC     GWPincIPTDay DOUBLE,
# MAGIC     GWPExIPTDay DOUBLE,
# MAGIC     VolumeMonth DOUBLE,
# MAGIC     TotalGrossIncIPTMonth DOUBLE,
# MAGIC     TotalGrossExcIPTMonth DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyStartDirect';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC drop table FactDailyReport

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC describe table FactDailyReport

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC CREATE TABLE FactDailyReport (
# MAGIC     DateId BIGINT,
# MAGIC     SchemeSubsetId BIGINT,
# MAGIC     DateVol DECIMAL(18,2),
# MAGIC     DateRetail DECIMAL(18,2),
# MAGIC     MTDVol DECIMAL(18,2),
# MAGIC     MTDRetail DECIMAL(18,2),
# MAGIC     AvgPolPerDay DECIMAL(18,2),
# MAGIC     AvgP DECIMAL(18,2),
# MAGIC     EstMonEndVol DECIMAL(18,2),
# MAGIC     EstMonEndRetail DECIMAL(18,2),
# MAGIC     EstMonEndNetMargin DECIMAL(18,2),
# MAGIC     BudgetVol INT,
# MAGIC     BudgetGWP INT,
# MAGIC     BudgetNetMargin INT,
# MAGIC     PercentToVol DECIMAL(18,2),
# MAGIC     PercentToGWP DECIMAL(18,2),
# MAGIC     PercentToNetMargin DECIMAL(18,2)
# MAGIC ) USING DELTA LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyReport';
# MAGIC 
# MAGIC 
# MAGIC -- %%sql 
# MAGIC -- CREATE TABLE FactDailyReport (
# MAGIC --     DateId BIGINT,
# MAGIC --     SchemeSubsetId BIGINT,
# MAGIC --     DateVol DECIMAL(18,2),
# MAGIC --     DateRetail DECIMAL(18,2),
# MAGIC --     MTDVol DECIMAL(18,2),
# MAGIC --     MTDRetail DECIMAL(18,2),
# MAGIC --     AvgPolPerDay DECIMAL(18,2),
# MAGIC --     AvgP DECIMAL(18,2),
# MAGIC --     EstMonEndVol DECIMAL(18,2),
# MAGIC --     EstMonEndRetail DECIMAL(18,2),
# MAGIC --     EstMonEndGrossMargin DECIMAL(18,2),
# MAGIC --     EstMonEndNetMargin DECIMAL(18,2),
# MAGIC --     PerToFCastRetail STRING,
# MAGIC --     PerToFCastMargin STRING,
# MAGIC --     PerToFCastNo DECIMAL(18,2),
# MAGIC --     GPW DECIMAL(18,2),
# MAGIC --     TaurusGrossRetained DECIMAL(18,2),
# MAGIC --     TaurusNetRetained DECIMAL(18,2),
# MAGIC --     AvGrossMargin STRING,
# MAGIC --     AvNetMargin STRING
# MAGIC -- ) USING DELTA LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyReport';
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE FactDailySalesReport (
# MAGIC     DateId BIGINT,
# MAGIC     SchemeSubsetId BIGINT,
# MAGIC     TotalSubSetId BIGINT,
# MAGIC     DateVol DECIMAL(18,2),
# MAGIC     DateRetail DECIMAL(18,2),
# MAGIC     MTDVol DECIMAL(18,2),
# MAGIC     MTDRetail DECIMAL(18,2),
# MAGIC     AvgPolPerDay DECIMAL(18,2),
# MAGIC     AvgP DECIMAL(18,2),
# MAGIC     EstMonEndVol DECIMAL(18,2),
# MAGIC     EstMonEndRetail DECIMAL(18,2),
# MAGIC     EstMonEndNetMargin DECIMAL(18,2),
# MAGIC     BudgetVol INT,
# MAGIC     BudgetGWP INT,
# MAGIC     BudgetNetMargin INT,
# MAGIC     PercentToVol DECIMAL(18,2),
# MAGIC     PercentToGWP DECIMAL(18,2),
# MAGIC     PercentToNetMargin DECIMAL(18,2)
# MAGIC ) USING DELTA LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailySalesReport';
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE FactDailyForecastReport (
# MAGIC     DateId BIGINT,
# MAGIC     SchemeSubsetId BIGINT,
# MAGIC     TotalSubSetId BIGINT,
# MAGIC     DateVol DECIMAL(18,2),
# MAGIC     DateRetail DECIMAL(18,2),
# MAGIC     MTDVol DECIMAL(18,2),
# MAGIC     MTDRetail DECIMAL(18,2),
# MAGIC     AvgPolPerDay DECIMAL(18,2),
# MAGIC     AvgP DECIMAL(18,2),
# MAGIC     EstMonEndVol DECIMAL(18,2),
# MAGIC     EstMonEndRetail DECIMAL(18,2),
# MAGIC     EstMonEndNetMargin DECIMAL(18,2),
# MAGIC     BudgetVol INT,
# MAGIC     BudgetGWP INT,
# MAGIC     BudgetNetMargin INT,
# MAGIC     PercentToVol DECIMAL(18,2),
# MAGIC     PercentToGWP DECIMAL(18,2),
# MAGIC     PercentToNetMargin DECIMAL(18,2)
# MAGIC ) USING DELTA LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactDailyForecastReport';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE FactySalesReport (
# MAGIC     DateId BIGINT,
# MAGIC     BrandId BIGINT,                -- previously SchemeSubset
# MAGIC     ChannelId BIGINT,              -- previously SubSet / TotalSubSet
# MAGIC     Day_Volume DECIMAL(18,2),    -- DateVol
# MAGIC     Day_Retail DECIMAL(18,2),    -- DateRetail
# MAGIC     MTD_Volume DECIMAL(18,2),    -- MTDVol
# MAGIC     MTD_Retail DECIMAL(18,2),    -- MTDRetail
# MAGIC     Average_Volume_Per_Day DECIMAL(18,2), -- AvgPolPerDay
# MAGIC     Average_Retail DECIMAL(18,2),         -- AvgP
# MAGIC     Month_End_Est_Volume DECIMAL(18,2),   -- EstMonEndVol
# MAGIC     Month_End_Est_Retail DECIMAL(18,2),   -- EstMonEndRetail
# MAGIC     Month_End_Est_Net_Margin DECIMAL(18,2), -- EstMonEndNetMargin
# MAGIC     Budget_Voume INT,             -- BudgetVol (note typo kept as-is per original)
# MAGIC     Budget_GWP INT,               -- BudgetGWP
# MAGIC     Budget_Net Margin INT,        -- BudgetNetMargin (space kept from original name)
# MAGIC     Volume_Percentage_To_Budget DECIMAL(18,2),     -- PercentToVol
# MAGIC     GWP_Percentage_To _udget DECIMAL(18,2),        -- PercentToGWP (typo kept)
# MAGIC     Net_Margin_Percentage_To_Budget DECIMAL(18,2)  -- PercentToNetMargin
# MAGIC ) USING DELTA LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactySalesReport';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE FactForecastReport (
# MAGIC     DateId BIGINT,
# MAGIC     BrandId BIGINT,                -- previously SchemeSubset
# MAGIC     ChannelId BIGINT, 
# MAGIC     Day_Volume DECIMAL(18,2),
# MAGIC     Day_Retail DECIMAL(18,2),
# MAGIC     MTD_Volume DECIMAL(18,2),
# MAGIC     MTD_Retail DECIMAL(18,2),
# MAGIC     Average_Volume_Per_Day DECIMAL(18,2),
# MAGIC     Average_Retail DECIMAL(18,2),
# MAGIC     Month_End_Est_Volume DECIMAL(18,2),
# MAGIC     Month_End_Est_Retail DECIMAL(18,2),
# MAGIC     Month_End_Est_Net_Margin DECIMAL(18,2),
# MAGIC     Budget_Voume INT,
# MAGIC     Budget_GWP INT,
# MAGIC     Budget_Net Margin INT,
# MAGIC     Volume_Percentage_To_Budget DECIMAL(18,2),
# MAGIC     GWP_Percentage_To _udget DECIMAL(18,2),
# MAGIC     Net_Margin_Percentage_To_Budget DECIMAL(18,2)
# MAGIC ) USING DELTA LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactForecastReport';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
