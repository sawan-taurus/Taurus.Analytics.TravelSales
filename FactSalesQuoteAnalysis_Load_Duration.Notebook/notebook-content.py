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

# =====================================================
#  FactEnquiryQuoteSales - Gold Layer Build Script
#  Date: 2025-10-16
#  Purpose: Aggregate enquiries, quotes, and sales by Date and Duration
# =====================================================

from pyspark.sql.functions import col, to_date, countDistinct, when
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# =====================================================
# CONFIGURATION
# =====================================================
WORKSPACE = "Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = "TaurusSilverLH"
GOLD_LAKEHOUSE = "TaurusGoldLH"
FACT_TABLE = "FactEnquiryQuoteSales"

# =====================================================
# SOURCE TABLES
# =====================================================
src_sales_table = "silver_travel_sales_transactions_sales"
src_enquiry_table = "silver_travel_enquiry_sales"
src_enquiry_results_table = "silver_travel_enquiry_results_sales"
dim_date_table = "DimDate"
dim_duration_table = "DimDuration"

# =====================================================
# PATHS
# =====================================================
sales_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_sales_table}"
enquiry_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_enquiry_table}"
enquiry_results_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_enquiry_results_table}"
dim_date_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/{dim_date_table}"
dim_duration_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/{dim_duration_table}"

# =====================================================
# LOAD DATA
# =====================================================
enquiry_df = spark.read.format("delta").load(enquiry_path)
enquiry_results_df = spark.read.format("delta").load(enquiry_results_path)
sales_df = spark.read.format("delta").load(sales_path)
dim_date_df = spark.read.format("delta").load(dim_date_path)
dim_duration_df = spark.read.format("delta").load(dim_duration_path)

# =====================================================
# STEP 1: FILTER BY DATE RANGE (>= 2023-01-01)
# =====================================================
START_DATE = "2023-01-01"

enquiry_filtered_df = enquiry_df.filter(
    to_date(col("QuoteDate")) >= F.lit(START_DATE)
).withColumn("QuoteDate", to_date(col("QuoteDate")))

# =====================================================
# STEP 2: LEFT JOINS (Enquiry → Results → Sales)
# =====================================================
joined_df = (
    enquiry_filtered_df.alias("te")
    .join(enquiry_results_df.alias("ter"),
          col("te.TravelEnquiryId") == col("ter.TravelEnquiryId"), "left")
    .join(sales_df.alias("tst"),
          col("ter.ProviderReference") == col("tst.ExternalReference"), "left")
    .select(
        col("te.TravelEnquiryId"),
        col("ter.TravelEnquiryResultId"),
        col("tst.TravelSalesTransactionId"),
        col("te.QuoteDate"),
        col("tst.Duration")
    )
)

# =====================================================
# STEP 3: JOIN DimDate (for DateID)
# =====================================================
joined_df = (
    joined_df.join(
        dim_date_df,
        joined_df["QuoteDate"] == dim_date_df["Date"],
        "left"
    )
    .withColumnRenamed("DateId", "DateID")
    .drop("Date")
)

# =====================================================
# STEP 4: MAP Duration TO DimDuration (for DurationId)
# =====================================================
joined_df = joined_df.withColumn(
    "DurationGroup",
    when((col("Duration") >= 1) & (col("Duration") <= 3), "1 to 3")
    .when((col("Duration") >= 4) & (col("Duration") <= 5), "4 to 5")
    .when((col("Duration") >= 6) & (col("Duration") <= 10), "6 to 10")
    .when((col("Duration") >= 11) & (col("Duration") <= 17), "11 to 17")
    .when((col("Duration") >= 18) & (col("Duration") <= 24), "18 to 24")
    .when((col("Duration") >= 25) & (col("Duration") <= 31), "25 to 31")
    .when(col("Duration") >= 32, "32 +")
    .otherwise("Unknown")
)

joined_df = (
    joined_df.join(
        dim_duration_df.select("DurationId", "Duration"),
        joined_df["DurationGroup"] == dim_duration_df["Duration"],
        "left"
    )
    .withColumn("DurationId", F.when(col("DurationId").isNull(), F.lit(0)).otherwise(col("DurationId")))
)

# =====================================================
# STEP 5: AGGREGATE FACT TABLE
# =====================================================
fact_df = (
    joined_df.groupBy("DateID", "DurationId")
    .agg(
        countDistinct("TravelEnquiryId").alias("DistinctEnquiries"),
        countDistinct("TravelEnquiryResultId").alias("TotalQuotes"),
        F.count("TravelSalesTransactionId").alias("TotalSales")
    )
    .orderBy("DateID", "DurationId")
)


# =====================================================
# STEP 6: WRITE FACT TO GOLD
# =====================================================
fact_target_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/{FACT_TABLE}"

fact_df.write.format("delta").mode("overwrite").save(fact_target_path)
print(f"✅ {FACT_TABLE} successfully written to Gold Layer (from {START_DATE}).")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC SELECT 
# MAGIC     LEFT(CAST(f.DateID AS STRING), 6) AS YearMonth,
# MAGIC     d.Duration,
# MAGIC     SUM(f.DistinctEnquiries) AS TotalEnquiry,
# MAGIC     SUM(f.TotalQuotes) AS TotalQuote,
# MAGIC     SUM(f.TotalSales) AS TotalSales
# MAGIC FROM TaurusGoldLH.FactEnquiryQuoteSales f
# MAGIC LEFT JOIN TaurusGoldLH.DimDuration d 
# MAGIC     ON f.DurationId = d.DurationId
# MAGIC WHERE LEFT(CAST(f.DateID AS STRING), 6) = '202509'
# MAGIC GROUP BY LEFT(CAST(f.DateID AS STRING), 6), d.Duration
# MAGIC ORDER BY d.Duration;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, to_date, countDistinct, when
import pyspark.sql.functions as F

# =====================================================
# CONFIGURATION
# =====================================================
WORKSPACE = "Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = "TaurusSilverLH"
GOLD_LAKEHOUSE = "TaurusGoldLH"

# =====================================================
# SOURCE TABLES
# =====================================================
src_enquiry_table = "silver_travel_enquiry_sales"
src_enquiry_results_table = "silver_travel_enquiry_results_sales"
src_sales_table = "silver_travel_sales_transactions_sales"
dim_date_table = "DimDate"
dim_duration_table = "DimDuration"

# =====================================================
# PATHS
# =====================================================
enquiry_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_enquiry_table}"
enquiry_results_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_enquiry_results_table}"
sales_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_sales_table}"
dim_date_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/{dim_date_table}"
dim_duration_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/{dim_duration_table}"

# =====================================================
# LOAD DATA
# =====================================================
enquiry_df = spark.read.format("delta").load(enquiry_path)
enquiry_results_df = spark.read.format("delta").load(enquiry_results_path)
sales_df = spark.read.format("delta").load(sales_path)
dim_date_df = spark.read.format("delta").load(dim_date_path)
dim_duration_df = spark.read.format("delta").load(dim_duration_path)

# =====================================================
# FILTER: QuoteDate >= '2023-01-01' (include all durations)
# =====================================================
start_date = "2023-01-01"

enquiry_filtered_df = enquiry_df.filter(
    to_date(col("QuoteDate")) >= F.lit(start_date)
).withColumn("QuoteDate", to_date(col("QuoteDate")))

# =====================================================
# LEFT JOINS: Enquiry → Results → Sales
# =====================================================
joined_df = (
    enquiry_filtered_df.alias("te")
    .join(enquiry_results_df.alias("ter"),
          col("te.TravelEnquiryId") == col("ter.TravelEnquiryId"), "left")
    .join(sales_df.alias("tst"),
          col("ter.ProviderReference") == col("tst.ExternalReference"), "left")
    .select(
        col("te.TravelEnquiryId"),
        col("ter.TravelEnquiryResultId"),
        col("tst.TravelSalesTransactionId"),
        col("te.QuoteDate"),
        col("te.Duration")
    )
)

# =====================================================
# JOIN DimDate → Get DateId
# =====================================================
joined_df = (
    joined_df.join(
        dim_date_df,
        joined_df["QuoteDate"] == dim_date_df["Date"],
        "left"
    )
    .withColumnRenamed("DateId", "DateID")
    .drop("Date")
)

# =====================================================
# MAP Duration TO DimDuration → Get DurationId
# =====================================================
joined_df = joined_df.withColumn(
    "DurationGroup",
    when((col("Duration") >= 1) & (col("Duration") <= 3), "1 to 3")
    .when((col("Duration") >= 4) & (col("Duration") <= 5), "4 to 5")
    .when((col("Duration") >= 6) & (col("Duration") <= 10), "6 to 10")
    .when((col("Duration") >= 11) & (col("Duration") <= 17), "11 to 17")
    .when((col("Duration") >= 18) & (col("Duration") <= 24), "18 to 24")
    .when((col("Duration") >= 25) & (col("Duration") <= 31), "25 to 31")
    .when(col("Duration") >= 32, "32 +")
    .otherwise("Unknown")
)

joined_df = (
    joined_df.join(
        dim_duration_df.select("DurationId", "Duration"),
        joined_df["DurationGroup"] == dim_duration_df["Duration"],
        "left"
    )
    .withColumn("DurationId", F.when(col("DurationId").isNull(), F.lit(0)).otherwise(col("DurationId")))
)

# =====================================================
# AGGREGATE (All Durations, from 2023-01-01 onwards)
# =====================================================
fact_df = (
    joined_df.groupBy("DateID", "DurationId")
    .agg(
        countDistinct(col("TravelEnquiryId")).alias("DistinctEnquiries"),
        countDistinct(col("TravelEnquiryResultId")).alias("TotalQuotes"),
        F.count(col("TravelSalesTransactionId")).alias("TotalSales")
    )
    .orderBy("DateID", "DurationId")
)

# =====================================================
# FILTER FOR DISPLAY: 1 Oct → 5 Oct 2025
# =====================================================
display_start = "2025-10-01"
display_end = "2025-10-05"

fact_filtered_df = (
    fact_df.join(dim_date_df.select("DateId", "Date"), fact_df["DateID"] == dim_date_df["DateId"], "left")
    .filter(
        (col("Date") >= F.lit(display_start)) &
        (col("Date") < F.lit(display_end))
    )
    .orderBy("Date")
)

# =====================================================
# DISPLAY FILTERED RESULT
# =====================================================
display(fact_filtered_df.select("Date", "DurationId", "DistinctEnquiries", "TotalQuotes", "TotalSales"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, to_date, countDistinct, when
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# =====================================================
# CONFIGURATION
# =====================================================
WORKSPACE = "Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = "TaurusSilverLH"
GOLD_LAKEHOUSE = "TaurusGoldLH"
FACT_TABLE = "FactEnquiryData"

# =====================================================
# SOURCE TABLES
# =====================================================
src_enquiry_table = "silver_travel_enquiry_sales"
src_enquiry_results_table = "silver_travel_enquiry_results_sales"
src_sales_table = "silver_travel_sales_transactions_sales"
dim_date_table = "DimDate"
dim_duration_table = "DimDuration"

# =====================================================
# PATHS
# =====================================================
enquiry_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_enquiry_table}"
enquiry_results_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_enquiry_results_table}"
sales_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_sales_table}"
dim_date_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/{dim_date_table}"
dim_duration_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/{dim_duration_table}"
fact_target_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/{FACT_TABLE}"

# =====================================================
# LOAD DATA
# =====================================================
enquiry_df = spark.read.format("delta").load(enquiry_path)
enquiry_results_df = spark.read.format("delta").load(enquiry_results_path)
sales_df = spark.read.format("delta").load(sales_path)
dim_date_df = spark.read.format("delta").load(dim_date_path)
dim_duration_df = spark.read.format("delta").load(dim_duration_path)

# =====================================================
# FILTER: QuoteDate >= '2023-01-01' (include all durations)
# =====================================================
start_date = "2023-01-01"

enquiry_filtered_df = enquiry_df.filter(
    to_date(col("QuoteDate")) >= F.lit(start_date)
).withColumn("QuoteDate", to_date(col("QuoteDate")))

# =====================================================
# LEFT JOINS: Enquiry → Results → Sales
# =====================================================
joined_df = (
    enquiry_filtered_df.alias("te")
    .join(enquiry_results_df.alias("ter"),
          col("te.TravelEnquiryId") == col("ter.TravelEnquiryId"), "left")
    .join(sales_df.alias("tst"),
          col("ter.ProviderReference") == col("tst.ExternalReference"), "left")
    .select(
        col("te.TravelEnquiryId"),
        col("ter.TravelEnquiryResultId"),
        col("tst.TravelSalesTransactionId"),
        col("te.QuoteDate"),
        col("te.Duration")
    )
)

# =====================================================
# JOIN DimDate → Get DateId
# =====================================================
joined_df = (
    joined_df.join(
        dim_date_df,
        joined_df["QuoteDate"] == dim_date_df["Date"],
        "left"
    )
    .withColumnRenamed("DateId", "DateID")
    .drop("Date")
)

# =====================================================
# MAP Duration TO DimDuration → Get DurationId
# =====================================================
joined_df = joined_df.withColumn(
    "DurationGroup",
    when((col("Duration") >= 1) & (col("Duration") <= 3), "1 to 3")
    .when((col("Duration") >= 4) & (col("Duration") <= 5), "4 to 5")
    .when((col("Duration") >= 6) & (col("Duration") <= 10), "6 to 10")
    .when((col("Duration") >= 11) & (col("Duration") <= 17), "11 to 17")
    .when((col("Duration") >= 18) & (col("Duration") <= 24), "18 to 24")
    .when((col("Duration") >= 25) & (col("Duration") <= 31), "25 to 31")
    .when(col("Duration") >= 32, "32 +")
    .otherwise("Unknown")
)

joined_df = (
    joined_df.join(
        dim_duration_df.select("DurationId", "Duration"),
        joined_df["DurationGroup"] == dim_duration_df["Duration"],
        "left"
    )
    .withColumn("DurationId", F.when(col("DurationId").isNull(), F.lit(0)).otherwise(col("DurationId")))
)

# =====================================================
# AGGREGATE (All Durations, from 2023-01-01 onwards)
# =====================================================
fact_df = (
    joined_df.groupBy("DateID", "DurationId")
    .agg(
        countDistinct(col("TravelEnquiryId")).alias("DistinctEnquiries"),
        countDistinct(col("TravelEnquiryResultId")).alias("TotalQuotes"),
        F.count(col("TravelSalesTransactionId")).alias("TotalSales")
    )
    .orderBy("DateID", "DurationId")
)

# =====================================================
# WRITE FACT TABLE TO GOLD
# =====================================================
fact_df.write.format("delta").mode("overwrite").save(fact_target_path)
print(f"✅ {FACT_TABLE} successfully written to Gold Layer (from {start_date}).")

# =====================================================
# FILTER FOR DISPLAY: 1 Oct → 5 Oct 2025
# =====================================================
display_start = "2025-10-01"
display_end = "2025-10-05"

fact_filtered_df = (
    fact_df.join(dim_date_df.select("DateId", "Date"), fact_df["DateID"] == dim_date_df["DateId"], "left")
    .filter(
        (col("Date") >= F.lit(display_start)) &
        (col("Date") < F.lit(display_end))
    )
    .orderBy("Date", "DurationId")
)

# =====================================================
# DISPLAY FILTERED RESULT (Preview)
# =====================================================
display(fact_filtered_df.select("Date", "DurationId", "DistinctEnquiries", "TotalQuotes", "TotalSales"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from FactEnquiryData 
# MAGIC where DateId = 20251001

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from  FactEnquiryData 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, to_date, countDistinct, when, lit
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# =====================================================
# CONFIGURATION
# =====================================================
WORKSPACE = "Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = "TaurusSilverLH"
GOLD_LAKEHOUSE = "TaurusGoldLH"
FACT_TABLE = "FactEnquiryData"

# =====================================================
# SOURCE TABLES
# =====================================================
src_enquiry_table = "silver_travel_enquiry_sales"
src_enquiry_results_table = "silver_travel_enquiry_results_sales"
src_sales_table = "silver_travel_sales_transactions_sales"
dim_date_table = "DimDate"
dim_duration_table = "DimDuration"

# =====================================================
# PATHS
# =====================================================
enquiry_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_enquiry_table}"
enquiry_results_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_enquiry_results_table}"
sales_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_sales_table}"
dim_date_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/{dim_date_table}"
dim_duration_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/{dim_duration_table}"
fact_target_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/{FACT_TABLE}"

# =====================================================
# LOAD DATA
# =====================================================
enquiry_df = spark.read.format("delta").load(enquiry_path)
enquiry_results_df = spark.read.format("delta").load(enquiry_results_path)
sales_df = spark.read.format("delta").load(sales_path)
dim_date_df = spark.read.format("delta").load(dim_date_path)
dim_duration_df = spark.read.format("delta").load(dim_duration_path)

# =====================================================
# FILTER: QuoteDate >= '2023-01-01' (include all durations)
# =====================================================
start_date = "2023-01-01"

enquiry_filtered_df = enquiry_df.filter(
    to_date(col("QuoteDate")) >= F.lit(start_date)
).withColumn("QuoteDate", to_date(col("QuoteDate")))

# =====================================================
# LEFT JOINS: Enquiry → Results → Sales
# =====================================================
joined_df = (
    enquiry_filtered_df.alias("te")
    .join(enquiry_results_df.alias("ter"),
          col("te.TravelEnquiryId") == col("ter.TravelEnquiryId"), "left")
    .join(sales_df.alias("tst"),
          col("ter.ProviderReference") == col("tst.ExternalReference"), "left")
    .select(
        col("te.TravelEnquiryId"),
        col("ter.TravelEnquiryResultId"),
        col("tst.TravelSalesTransactionId"),
        col("te.QuoteDate"),
        col("te.Duration")
    )
)

# =====================================================
# JOIN DimDate → Get DateId
# =====================================================
joined_df = (
    joined_df.join(
        dim_date_df,
        joined_df["QuoteDate"] == dim_date_df["Date"],
        "left"
    )
    .withColumnRenamed("DateId", "DateID")
    .drop("Date")
)

# =====================================================
# MAP Duration TO DimDuration → Get DurationId
# =====================================================
joined_df = joined_df.withColumn(
    "DurationGroup",
    when((col("Duration") >= 1) & (col("Duration") <= 3), "1 to 3")
    .when((col("Duration") >= 4) & (col("Duration") <= 5), "4 to 5")
    .when((col("Duration") >= 6) & (col("Duration") <= 10), "6 to 10")
    .when((col("Duration") >= 11) & (col("Duration") <= 17), "11 to 17")
    .when((col("Duration") >= 18) & (col("Duration") <= 24), "18 to 24")
    .when((col("Duration") >= 25) & (col("Duration") <= 31), "25 to 31")
    .when(col("Duration") >= 32, "32 +")
    .otherwise("Unknown")
)

joined_df = (
    joined_df.join(
        dim_duration_df.select("DurationId", "Duration"),
        joined_df["DurationGroup"] == dim_duration_df["Duration"],
        "left"
    )
    .withColumn("DurationId", F.when(col("DurationId").isNull(), F.lit(0)).otherwise(col("DurationId")))
)

# =====================================================
# AGGREGATE (All Durations, from 2023-01-01 onwards)
# =====================================================
fact_df = (
    joined_df.groupBy("DateID", "DurationId")
    .agg(
        countDistinct(col("TravelEnquiryId")).alias("TotalEnquiry"),
        countDistinct(col("TravelEnquiryResultId")).alias("TotalQuote"),
        F.count(col("TravelSalesTransactionId")).alias("TotalSales")
    )
    .orderBy("DateID", "DurationId")
)

# =====================================================
# ADD CONVERSION METRICS
# =====================================================
fact_df = fact_df.withColumn(
    "QuoteToEnquiryRate",
    F.when(col("TotalEnquiry") == 0, lit(None))
     .otherwise((col("TotalQuote") / col("TotalEnquiry")).cast("decimal(10,4)"))
).withColumn(
    "SalesToEnquiryRate",
    F.when(col("TotalEnquiry") == 0, lit(None))
     .otherwise((col("TotalSales") / col("TotalEnquiry")).cast("decimal(10,4)"))
).withColumn(
    "SalesToQuoteRate",
    F.when(col("TotalQuote") == 0, lit(None))
     .otherwise((col("TotalSales") / col("TotalQuote")).cast("decimal(10,4)"))
)

# =====================================================
# WRITE FACT TABLE TO GOLD
# =====================================================
fact_df.write.format("delta").mode("overwrite").save(fact_target_path)
print(f"✅ {FACT_TABLE} successfully written to Gold Layer (from {start_date}).")

# =====================================================
# FILTER FOR DISPLAY: 1 Oct → 5 Oct 2025
# =====================================================
display_start = "2025-10-01"
display_end = "2025-10-05"

fact_filtered_df = (
    fact_df.join(dim_date_df.select("DateId", "Date"), fact_df["DateID"] == dim_date_df["DateId"], "left")
    .filter(
        (col("Date") >= F.lit(display_start)) &
        (col("Date") < F.lit(display_end))
    )
    .orderBy("Date", "DurationId")
)

# =====================================================
# DISPLAY FILTERED RESULT (Preview)
# =====================================================
display(fact_filtered_df.select(
    "Date", "DurationId", "TotalEnquiry", "TotalQuote", "TotalSales",
    "QuoteToEnquiryRate", "SalesToEnquiryRate", "SalesToQuoteRate"
))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
