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
#  FactSalesAndQuotes - Gold Layer Build Script (Corrected & Enhanced)
#  Date: 2025-10-09
# =====================================================

from pyspark.sql.functions import (
    col, lit, to_date, datediff, current_date, date_sub,
    sum as _sum, max as _max, countDistinct, when, upper, trim
)
import pyspark.sql.functions as F
from delta.tables import DeltaTable

# =====================================================
# CONFIGURATION
# =====================================================
WORKSPACE = "Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = "TaurusSilverLH"
GOLD_LAKEHOUSE = "TaurusGoldLH"
FACT_TABLE = "FactSalesAndQuotes"

# =====================================================
# SOURCE TABLES
# =====================================================
src_sales_table = "silver_travel_sales_transactions_sales"
src_enquiry_table = "silver_travel_enquiry_sales"
src_enquiry_results_table = "silver_travel_enquiry_results_sales"
src_schemes_table = "silver_schemes_sales"
src_scheme_headers_table = "silver_scheme_headers_sales"
src_persons_table = "silver_travel_sales_transaction_persons_sales"

# =====================================================
# PATHS
# =====================================================
sales_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_sales_table}"
enquiry_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_enquiry_table}"
enquiry_results_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_enquiry_results_table}"
schemes_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_schemes_table}"
scheme_headers_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_scheme_headers_table}"
persons_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_persons_table}"

# =====================================================
# DIMENSION TABLE PATHS
# =====================================================
dim_date_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimDate"
dim_channel_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimChannel"
dim_agent_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimAgent"
dim_campaign_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimCampaign"
dim_lead_time_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimLeadTimeGroup"
dim_duration_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimDuration"
dim_age_group_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimAgeGroup"
dim_medical_score_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimMedicalScore"

# =====================================================
# LOAD DATA
# =====================================================
enquiry_df = spark.read.format("delta").load(enquiry_path)
enquiry_results_df = spark.read.format("delta").load(enquiry_results_path)
sales_df = spark.read.format("delta").load(sales_path)
schemes_df = spark.read.format("delta").load(schemes_path)
scheme_headers_df = spark.read.format("delta").load(scheme_headers_path)
persons_df = spark.read.format("delta").load(persons_path)

dim_date_df = spark.read.format("delta").load(dim_date_path)
dim_channel_df = spark.read.format("delta").load(dim_channel_path)
dim_agent_df = spark.read.format("delta").load(dim_agent_path)
dim_campaign_df = spark.read.format("delta").load(dim_campaign_path)
dim_lead_time_df = spark.read.format("delta").load(dim_lead_time_path)
dim_duration_df = spark.read.format("delta").load(dim_duration_path)
dim_age_group_df = spark.read.format("delta").load(dim_age_group_path)
dim_medical_score_df = spark.read.format("delta").load(dim_medical_score_path)

# =====================================================
# FILTER RANGE CONFIG
# =====================================================
START_DATE = "2023-01-01"
END_DATE = F.date_sub(F.current_date(), 1)  # yesterday dynamically

# =====================================================
# STEP 1: FILTER DATA BY DATE RANGE (1 Jan 2023 → yesterday)
# =====================================================
enquiry_filtered_df = enquiry_df.filter(
    (F.to_date(col("QuoteDate")) >= F.lit(START_DATE)) &
    (F.to_date(col("QuoteDate")) <= END_DATE)
)

enquiry_results_filtered_df = enquiry_results_df.join(
    enquiry_filtered_df.select("TravelEnquiryId").distinct(),
    "TravelEnquiryId", "inner"
)

sales_dedup_df = sales_df.dropDuplicates(["ExternalReference"])
sales_filtered_df = sales_dedup_df.join(
    enquiry_results_filtered_df.select("ProviderReference").dropDuplicates(),
    sales_dedup_df["ExternalReference"] == enquiry_results_filtered_df["ProviderReference"],
    "left"
)

# =====================================================
# STEP 2: JOIN ENQUIRY ↔ RESULTS ↔ SALES
# =====================================================
enquiry_sales_df = (
    enquiry_filtered_df.alias("te")
    .join(enquiry_results_filtered_df.alias("ter"),
          col("te.TravelEnquiryId") == col("ter.TravelEnquiryId"), "left")
    .join(sales_filtered_df.alias("tst"),
          col("ter.ProviderReference") == col("tst.ExternalReference"), "left")
    .select(
        col("te.TravelEnquiryId"),
        col("ter.TravelEnquiryResultId"),
        col("tst.TravelSalesTransactionId"),
        F.to_date(col("te.QuoteDate")).alias("QuoteDateOnly"),
        col("tst.TransactionDate"),
        col("tst.StartDate"),
        col("tst.Duration"),
        col("tst.TotalGrossIncIPT"),
        col("tst.TotalNetToUnderwriter"),
        col("ter.SchemeId"),
        col("ter.SchemeName"),
        col("te.AgentName"),
        col("te.CampaignName")
    )
)

# =====================================================
# STEP 3: JOIN DIMENSIONS
# =====================================================

# Date
enquiry_sales_df = enquiry_sales_df.join(
    dim_date_df, enquiry_sales_df["QuoteDateOnly"] == dim_date_df["Date"], "left"
).withColumnRenamed("DateId", "DateID").drop("QuoteDateOnly")

# Channel
enquiry_sales_df = enquiry_sales_df.withColumn("SchemeName_Clean", upper(trim(col("SchemeName"))))
dim_channel_df = dim_channel_df.withColumn("SchemeName_Clean", upper(trim(col("SchemeName"))))
enquiry_sales_df = enquiry_sales_df.join(
    dim_channel_df.select("ChannelId", "SchemeName_Clean"),
    "SchemeName_Clean", "left"
).withColumn("ChannelId", F.when(col("ChannelId").isNull(), F.lit(0)).otherwise(col("ChannelId")))

# Agent
enquiry_sales_df = enquiry_sales_df.withColumn("AgentName_Clean", upper(trim(col("AgentName"))))
dim_agent_df = dim_agent_df.withColumn("AgentName_Clean", upper(trim(col("AgentName"))))
enquiry_sales_df = enquiry_sales_df.join(
    dim_agent_df.select("AgentId", "AgentName_Clean"),
    "AgentName_Clean", "left"
).withColumn("AgentId", F.when(col("AgentId").isNull(), F.lit(0)).otherwise(col("AgentId")))

# Campaign
enquiry_sales_df = enquiry_sales_df.withColumn("CampaignName_Clean", upper(trim(col("CampaignName"))))
dim_campaign_df = dim_campaign_df.withColumn("CampaignName_Clean", upper(trim(col("CampaignName"))))
enquiry_sales_df = enquiry_sales_df.join(
    dim_campaign_df.select("CampaignId", "CampaignName_Clean"),
    "CampaignName_Clean", "left"
).withColumn("CampaignId", F.when(col("CampaignId").isNull(), F.lit(0)).otherwise(col("CampaignId")))

# Lead Time
enquiry_sales_df = enquiry_sales_df.withColumn("LeadDays", datediff(col("StartDate"), col("TransactionDate")))
enquiry_sales_df = enquiry_sales_df.withColumn(
    "LeadTimeGroup",
    when(col("LeadDays") < 0, "Invalid (<0)")
    .when(col("LeadDays") <= 3, "0 to 3")
    .when(col("LeadDays") <= 8, "4 to 8")
    .when(col("LeadDays") <= 15, "9 to 15")
    .when(col("LeadDays") <= 30, "16 to 30")
    .when(col("LeadDays") <= 60, "31 to 60")
    .when(col("LeadDays") >= 61, "61+")
    .otherwise("Unknown")
)
enquiry_sales_df = enquiry_sales_df.join(
    dim_lead_time_df.select("LeadTimeGroupId", "LeadTimeGroup"),
    "LeadTimeGroup", "left"
).withColumn("LeadTimeGroupId", F.when(col("LeadTimeGroupId").isNull(), F.lit(0)).otherwise(col("LeadTimeGroupId")))

# Duration
enquiry_sales_df = enquiry_sales_df.withColumn(
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
enquiry_sales_df = enquiry_sales_df.join(
    dim_duration_df.select("DurationId", "Duration"),
    enquiry_sales_df["DurationGroup"] == dim_duration_df["Duration"], "left"
).withColumn("DurationId", F.when(col("DurationId").isNull(), F.lit(0)).otherwise(col("DurationId")))

# Age & Medical
max_age_df = persons_df.groupBy("TravelSalesTransactionId").agg(F.max("AgeAtPolicyStart").alias("MaxAgeAtPolicyStart"))
max_med_df = persons_df.groupBy("TravelSalesTransactionId").agg(F.max("ScreeningScore").alias("MaxScreeningScore"))
enquiry_sales_df = enquiry_sales_df.join(max_age_df, "TravelSalesTransactionId", "left").join(max_med_df, "TravelSalesTransactionId", "left")

enquiry_sales_df = enquiry_sales_df.withColumn(
    "AgeGroup",
    when((col("MaxAgeAtPolicyStart") >= 0) & (col("MaxAgeAtPolicyStart") <= 20), "0 to 20")
    .when((col("MaxAgeAtPolicyStart") >= 21) & (col("MaxAgeAtPolicyStart") <= 30), "21 to 30")
    .when((col("MaxAgeAtPolicyStart") >= 31) & (col("MaxAgeAtPolicyStart") <= 40), "31 to 40")
    .when((col("MaxAgeAtPolicyStart") >= 41) & (col("MaxAgeAtPolicyStart") <= 50), "41 to 50")
    .when((col("MaxAgeAtPolicyStart") >= 51) & (col("MaxAgeAtPolicyStart") <= 65), "51 to 65")
    .when((col("MaxAgeAtPolicyStart") >= 66) & (col("MaxAgeAtPolicyStart") <= 75), "66 to 75")
    .when(col("MaxAgeAtPolicyStart") >= 76, "76 +")
    .otherwise("Unknown")
)
enquiry_sales_df = enquiry_sales_df.join(dim_age_group_df.select("AgeGroupId", "AgeGroup"), "AgeGroup", "left") \
    .withColumn("AgeGroupId", F.when(col("AgeGroupId").isNull(), F.lit(0)).otherwise(col("AgeGroupId")))

enquiry_sales_df = enquiry_sales_df.withColumn(
    "MedicalScore",
    when(col("MaxScreeningScore").isNull() | (col("MaxScreeningScore") < 1.00), "0 to 0.99")
    .when(col("MaxScreeningScore") == 1.00, "1.00")
    .when((col("MaxScreeningScore") > 1.00) & (col("MaxScreeningScore") <= 1.5), "1.01 to 1.5")
    .when((col("MaxScreeningScore") > 1.5) & (col("MaxScreeningScore") <= 2.5), "1.51 to 2.5")
    .when((col("MaxScreeningScore") > 2.5) & (col("MaxScreeningScore") <= 6.0), "2.51 to 6.0")
    .when(col("MaxScreeningScore") > 6.0, "6.01 +")
    .otherwise("Unknown")
)
enquiry_sales_df = enquiry_sales_df.join(dim_medical_score_df.select("MedicalScoreId", "MedicalScore"), "MedicalScore", "left") \
    .withColumn("MedicalScoreId", F.when(col("MedicalScoreId").isNull(), F.lit(0)).otherwise(col("MedicalScoreId")))

# =====================================================
# STEP 10: JOIN WITH DimSchemeHeader (CoverLevel)
# =====================================================
enquiry_sales_df = enquiry_sales_df.join(
    schemes_df.select("SchemeId", "SchemeHeaderId"), "SchemeId", "left"
).join(
    scheme_headers_df.select("SchemeHeaderId", "FriendlyName"), "SchemeHeaderId", "left"
).withColumnRenamed("FriendlyName", "CoverLevel")

# Ensure SchemeHeaderId exists
enquiry_sales_df = enquiry_sales_df.withColumn(
    "SchemeHeaderId", F.when(col("SchemeHeaderId").isNull(), F.lit(0)).otherwise(col("SchemeHeaderId"))
)
# =====================================================
# STEP 11: AGGREGATE FACT (ALL DIMENSIONS) - DISTINCT SALES INCLUDED
# =====================================================

# Deduplicate by unique identifiers to avoid overcounting
distinct_enquiry_sales_df = enquiry_sales_df.dropDuplicates([
    "TravelEnquiryId",
    "TravelEnquiryResultId",
    "TravelSalesTransactionId",
    "DateID",
    "ChannelId",
    "AgentId",
    "CampaignId",
    "LeadTimeGroupId",
    "DurationId",
    "AgeGroupId",
    "MedicalScoreId",
    "SchemeHeaderId"
])

fact_df = (
    distinct_enquiry_sales_df
    .groupBy(
        "DateID", "ChannelId", "AgentId", "CampaignId", "LeadTimeGroupId",
        "DurationId", "AgeGroupId", "MedicalScoreId", "SchemeHeaderId"
    )
    .agg(
        countDistinct("TravelEnquiryId").alias("TotalEnquiry"),     # DISTINCT
        F.count("TravelEnquiryResultId").alias("TotalQuote"),       # NON-DISTINCT (updated)
        countDistinct("TravelSalesTransactionId").alias("TotalSales"),  # DISTINCT
        _sum("TotalGrossIncIPT").alias("TotalGrossIncIPT"),
        _sum("TotalNetToUnderwriter").alias("TotalNetToUnderwriter")
    )
)


# =====================================================
# STEP 12: ADD CONVERSION AND DISTRIBUTION METRICS
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




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/{FACT_TABLE}"

fact_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(fact_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

