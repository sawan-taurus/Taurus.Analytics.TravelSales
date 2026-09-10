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
#  FactSalesAndQuotes - Gold Layer Build Script (Duration Only)
#  Corrected Aggregations for Distinct Enquiry / Quote / Sales
#  Filtered for 1–2 October 2025
#  Date: 2025-10-16
# =====================================================

from pyspark.sql.functions import col, when, to_date, countDistinct, sum as _sum
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
src_sales_table = "silver_travel_sales_transactions_sales"
src_enquiry_table = "silver_travel_enquiry_sales"
src_enquiry_results_table = "silver_travel_enquiry_results_sales"

# =====================================================
# PATHS
# =====================================================
sales_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_sales_table}"
enquiry_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_enquiry_table}"
enquiry_results_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_enquiry_results_table}"
dim_duration_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimDuration"

# =====================================================
# LOAD DATA
# =====================================================
enquiry_df = spark.read.format("delta").load(enquiry_path)
enquiry_results_df = spark.read.format("delta").load(enquiry_results_path)
sales_df = spark.read.format("delta").load(sales_path)
dim_duration_df = spark.read.format("delta").load(dim_duration_path)

# =====================================================
# FILTER CONFIG: Only 1st & 2nd October 2025
# =====================================================
TARGET_DATES = ["2025-10-01", "2025-10-02"]
enquiry_filtered_df = enquiry_df.filter(F.to_date(col("QuoteDate")).isin(TARGET_DATES))
enquiry_results_filtered_df = enquiry_results_df.join(
    enquiry_filtered_df.select("TravelEnquiryId").distinct(), "TravelEnquiryId", "inner"
)
sales_dedup_df = sales_df.dropDuplicates(["ExternalReference"])
sales_filtered_df = sales_dedup_df.join(
    enquiry_results_filtered_df.select("ProviderReference").dropDuplicates(),
    sales_dedup_df["ExternalReference"] == enquiry_results_filtered_df["ProviderReference"],
    "left"
)

# =====================================================
# DEFINE DURATION GROUP LOGIC (function for reuse)
# =====================================================
def add_duration_group(df):
    return df.withColumn(
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

# =====================================================
# STEP 1: ENQUIRY-LEVEL COUNT
# =====================================================
enquiry_duration_df = (
    enquiry_filtered_df.alias("te")
    .join(enquiry_results_filtered_df.alias("ter"), "TravelEnquiryId", "left")
    .join(sales_filtered_df.alias("tst"),
          col("ter.ProviderReference") == col("tst.ExternalReference"), "left")
    .select("te.TravelEnquiryId", "tst.Duration")
    .transform(add_duration_group)
    .groupBy("DurationGroup")
    .agg(countDistinct("TravelEnquiryId").alias("TotalEnquiry"))
)

# =====================================================
# STEP 2: QUOTE-LEVEL COUNT
# =====================================================
quote_duration_df = (
    enquiry_results_filtered_df.alias("ter")
    .join(sales_filtered_df.alias("tst"),
          col("ter.ProviderReference") == col("tst.ExternalReference"), "left")
    .select("ter.TravelEnquiryResultId", "tst.Duration")
    .transform(add_duration_group)
    .groupBy("DurationGroup")
    .agg(countDistinct("TravelEnquiryResultId").alias("TotalQuote"))
)

# =====================================================
# STEP 3: SALES-LEVEL COUNT
# =====================================================
sales_duration_df = (
    sales_filtered_df.select("TravelSalesTransactionId", "Duration", "TotalGrossIncIPT", "TotalNetToUnderwriter")
    .transform(add_duration_group)
    .groupBy("DurationGroup")
    .agg(
        countDistinct("TravelSalesTransactionId").alias("TotalSales"),
        _sum("TotalGrossIncIPT").alias("TotalGrossIncIPT"),
        _sum("TotalNetToUnderwriter").alias("TotalNetToUnderwriter")
    )
)

# =====================================================
# STEP 4: MERGE ALL THREE COUNTS BY DURATION
# =====================================================
fact_df = (
    enquiry_duration_df
    .join(quote_duration_df, "DurationGroup", "outer")
    .join(sales_duration_df, "DurationGroup", "outer")
    .fillna(0)
)

# =====================================================
# STEP 5: ADD CONVERSION METRICS
# =====================================================
fact_df = fact_df.withColumn(
    "QuoteToEnquiryRate",
    (col("TotalQuote") / F.when(col("TotalEnquiry") == 0, None).otherwise(col("TotalEnquiry"))).cast("decimal(10,4)")
).withColumn(
    "SalesToEnquiryRate",
    (col("TotalSales") / F.when(col("TotalEnquiry") == 0, None).otherwise(col("TotalEnquiry"))).cast("decimal(10,4)")
).withColumn(
    "SalesToQuoteRate",
    (col("TotalSales") / F.when(col("TotalQuote") == 0, None).otherwise(col("TotalQuote"))).cast("decimal(10,4)")
)

# =====================================================
# STEP 6: ORDER AND DISPLAY
# =====================================================
duration_order = F.when(col("DurationGroup") == "1 to 3", 1)\
    .when(col("DurationGroup") == "4 to 5", 2)\
    .when(col("DurationGroup") == "6 to 10", 3)\
    .when(col("DurationGroup") == "11 to 17", 4)\
    .when(col("DurationGroup") == "18 to 24", 5)\
    .when(col("DurationGroup") == "25 to 31", 6)\
    .when(col("DurationGroup") == "32 +", 7)\
    .otherwise(8)

fact_df = fact_df.orderBy(duration_order)

print("✅ Corrected FactSalesAndQuotes (Duration Only) - Distinct Counts for 1–2 Oct 2025")
display(fact_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(fact_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

