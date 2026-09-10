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
# META         },
# META         {
# META           "id": "dbfc334d-f83f-43fc-965f-0a38145f4700"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

Source_Type = "Sales"  # or "Atoz" or "Oasis" or "Sales"
Data_Load_Type = "historical"  # or "yesterday"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Policy numbers to track
TRACK_POLICIES = ['SOI-DRNL1000752', 'SOI-DRNL1000765', 'SOI-DRNL1000787', 'SOI-DRNL1000804']

def check_policies(df, checkpoint_name):
    """Helper function to check if tracked policies exist"""
    print("=" * 80)
    print(f"CHECKPOINT: {checkpoint_name}")
    print(f"Total records: {df.count()}")
    print(f"Total distinct PolicyNumbers: {df.select('PolicyNumber').distinct().count()}")
    
    tracked_df = df.filter(col("PolicyNumber").isin(TRACK_POLICIES))
    tracked_count = tracked_df.count()
    tracked_policies = [row['PolicyNumber'] for row in tracked_df.select('PolicyNumber').distinct().collect()]
    
    print(f"\nTracked policies found: {len(tracked_policies)} out of 4")
    print(f"Found policies: {tracked_policies}")
    
    missing = set(TRACK_POLICIES) - set(tracked_policies)
    if missing:
        print(f"⚠️ MISSING policies: {list(missing)}")
    
    if tracked_count > 0:
        print(f"\nDetails of tracked policies ({tracked_count} records):")
        display(tracked_df)
    else:
        print("\n❌ No tracked policies found at this checkpoint!")
    
    print("=" * 80)
    print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

def check_jan_2025(df):
    """Quick check for DateId = 202501, SOI/SOM prefix, and New Issue."""
    
    jan_df = df.filter(
        (col("DateId").substr(1, 6) == "202501") &
        (col("PolicyNumber").substr(1, 3).isin("SOI", "SOM")) &
        (col("TransactionType") == "New Issue")
    )
    
    total_jan = jan_df.count()
    distinct_policies_jan = jan_df.select("PolicyNumber").distinct().count()
    
    print("=" * 60)
    print("📅 CHECK — DateId 202501, SOI/SOM, New Issue")
    print(f"Total records: {total_jan}")
    print(f"Distinct PolicyNumbers: {distinct_policies_jan}")
    print("=" * 60)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    col, lit, to_date, datediff, current_date, date_sub,
    sum as _sum, max as _max, count as _count,
    when, upper, trim, concat_ws, countDistinct, row_number
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

WORKSPACE = "Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = "TaurusSilverLH"
Gold_LAKEHOUSE = "TaurusGoldLH"

if Source_Type == "Sales":
    src_table_name = "silver_travel_sales_transactions_sales"
    options_table_name = "silver_travel_sales_transaction_options_sales"
    persons_table_name = "silver_travel_sales_transaction_persons_sales"
    schemes_sales_table = "silver_schemes_sales"
    schemes_header_table = "silver_scheme_headers_sales"
elif Source_Type == "Oasis":
    src_table_name = "silver_travel_sales_transactions_oasis"
    options_table_name = "silver_travel_sales_transaction_options_oasis"
    persons_table_name = "silver_travel_sales_transaction_persons_oasis"
    schemes_sales_table = "silver_schemes_oasis"
    schemes_header_table = "silver_scheme_headers_oasis"
elif Source_Type == "Atoz":
    src_table_name = "silver_travel_sales_transactions_atoz"
    options_table_name = "silver_travel_sales_transaction_options_atoz"
    persons_table_name = "silver_travel_sales_transaction_persons_atoz"
    schemes_sales_table = "silver_schemes_atoz"
    schemes_header_table = "silver_scheme_headers_atoz"

src_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_table_name}"
options_sales_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{options_table_name}"
persons_sales_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{persons_table_name}"
silver_schemes_sales_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{schemes_sales_table}"
silver_schemes_header_sales_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{schemes_header_table}"

SRC_TABLE = src_table_name
DIM_DATE_TABLE = "DimDate"
DIM_SCHEME_HEADERS = "DimSchemeHeaders"
FACT_TABLE = "FactSalesAnalysis"

silver_df = spark.read.format("delta").load(src_file_path) \
    .filter(
        (to_date(col("TransactionDate")) >= to_date(lit("2023-01-01"))) &
        (to_date(col("TransactionDate")) <= date_sub(current_date(), 1))
    )

options_sales_df = spark.read.format("delta").load(options_sales_path)
silver_persons_df = spark.read.format("delta").load(persons_sales_path)
silver_schemes_sales_df = spark.read.format("delta").load(silver_schemes_sales_path)
silver_schemes_header_sales_df = spark.read.format("delta").load(silver_schemes_header_sales_path)

dim_date_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/{DIM_DATE_TABLE}"
dim_scheme_headers_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/{DIM_SCHEME_HEADERS}"
dim_travel_brand_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimTravelBrand"
dim_lead_time_group_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimLeadTimeGroup"
dim_duration_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimDuration"
dim_policy_type_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimPolicyType"
dim_channel_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimChannel"
dim_family_group_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimFamilyGroup"
dim_age_group_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimAgeGroup"
dim_medical_score_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimMedicalScore"
dim_country_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimCountry"
dim_destination_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimDestination"
dim_option_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimOption"
dim_marketing_channel_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimMarketingChannel"
dim_agent_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimAgent"
dim_campaign_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimCampaign"

fact_table_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/{FACT_TABLE}"

dim_date_df = spark.read.format("delta").load(dim_date_path)
dim_scheme_headers_df = spark.read.format("delta").load(dim_scheme_headers_path)
dim_travel_brand_df = spark.read.format("delta").load(dim_travel_brand_path)
dim_lead_time_group_df = spark.read.format("delta").load(dim_lead_time_group_path)
dim_duration_df = spark.read.format("delta").load(dim_duration_path)
dim_policy_type_df = spark.read.format("delta").load(dim_policy_type_path)
dim_channel_df = spark.read.format("delta").load(dim_channel_path)
dim_destination_df = spark.read.format("delta").load(dim_destination_path)
dim_family_group_df = spark.read.format("delta").load(dim_family_group_path)
dim_option_df = spark.read.format("delta").load(dim_option_path)
dim_age_group_df = spark.read.format("delta").load(dim_age_group_path)
dim_medical_score_df = spark.read.format("delta").load(dim_medical_score_path)
dim_country_df = spark.read.format("delta").load(dim_country_path)
dim_marketing_channel_df = spark.read.format("delta").load(dim_marketing_channel_path)
dim_agent_df = spark.read.format("delta").load(dim_agent_path)
dim_campaign_df = spark.read.format("delta").load(dim_campaign_path)

fact_existing_df = spark.read.format("delta").load(fact_table_path)
# ----------------------------------------------------------------------------------------------------
silver_enriched_df = silver_df.join(
    silver_schemes_sales_df.select("SchemeId", "SchemeHeaderId"),
    on="SchemeId",
    how="left"
)

# Join with silver_schemes_header_sales to get FriendlyName
silver_enriched_df = silver_enriched_df.join(
    silver_schemes_header_sales_df.select("SchemeHeaderId", "FriendlyName"),
    on="SchemeHeaderId",
    how="left"
)

silver_enriched_renamed_df = silver_enriched_df.withColumnRenamed("SchemeHeaderId", "SilverSchemeHeaderId")

# Join silver_enriched_df with DimSchemeHeaders to get surrogate SchemeHeaderId using FriendlyName
silver_enriched_df = silver_enriched_df.join(
    dim_scheme_headers_df.select(
        col("SchemeHeaderId").alias("DimSchemeHeaderId"),
        col("FriendlyName")
    ),
    on="FriendlyName",  # This is okay because both sides have the column
    how="left"
)

# Add derived brand name column using case logic

silver_enriched_df = silver_enriched_df.withColumn(
    "SilverTravelBrand",
    when(col("PolicyNumber").substr(1, 3).isin("SOI", "SOM"), "switched_on")
    .when(col("PolicyNumber").substr(1, 3).isin("STA", "STM"), "start_travel")
    .when(col("PolicyNumber").substr(1, 3) == "TC-", "thomas_cook")
    .when(col("PolicyNumber").substr(1, 3).isin("OI-", "OIM"), "oasis_travel")
    .when(col("PolicyNumber").substr(1, 3).isin("TRU", "TRM"), "trusted_ins")
    .when(col("PolicyNumber").substr(1, 3).isin("VIV", "VIM"), "viva_ins")
    .when(col("PolicyNumber").substr(1, 3).isin("ATO", "Ato"), "atoz_travel")
    .otherwise("UNKNOWN")
)

# Join with dimension table to get TravelBrandId

silver_enriched_df = silver_enriched_df.join(
    dim_travel_brand_df.select("TravelBrandId", "TravelBrand"),
    silver_enriched_df["SilverTravelBrand"] == dim_travel_brand_df["TravelBrand"],
    how="left"
)

silver_enriched_df = silver_enriched_df.withColumn(
    "LeadTimeGroup",
    when(datediff(col("StartDate"), col("TransactionDate")) < 0, "Invalid (<0)")
    .when(datediff(col("StartDate"), col("TransactionDate")).between(0, 3), "0 to 3")
    .when(datediff(col("StartDate"), col("TransactionDate")).between(4, 8), "4 to 8")
    .when(datediff(col("StartDate"), col("TransactionDate")).between(9, 15), "9 to 15")
    .when(datediff(col("StartDate"), col("TransactionDate")).between(16, 30), "16 to 30")
    .when(datediff(col("StartDate"), col("TransactionDate")).between(31, 60), "31 to 60")
    .when(datediff(col("StartDate"), col("TransactionDate")) >= 61, "61+")
    .otherwise("Invalid Dates")
)

silver_enriched_df = silver_enriched_df.join(
    dim_lead_time_group_df.select("LeadTimeGroupId", "LeadTimeGroup"),
    on="LeadTimeGroup",
    how="left"
)

silver_enriched_df = silver_enriched_df.withColumn(
    "Duration",
    when((col("Duration") >= 1) & (col("Duration") <= 3), "1 to 3")
    .when((col("Duration") >= 4) & (col("Duration") <= 5), "4 to 5")
    .when((col("Duration") >= 6) & (col("Duration") <= 10), "6 to 10")
    .when((col("Duration") >= 11) & (col("Duration") <= 17), "11 to 17")
    .when((col("Duration") >= 18) & (col("Duration") <= 24), "18 to 24")
    .when((col("Duration") >= 25) & (col("Duration") <= 31), "25 to 31")
    .when(col("Duration") >= 32, "32 +")
    .otherwise("Invalid Duration")
)

silver_enriched_df = silver_enriched_df.join(
    dim_duration_df.select("DurationId", "Duration"),
    on="Duration",
    how="left"
)

silver_enriched_df = silver_enriched_df.join(
    dim_policy_type_df.select("PolicyTypeId", "PolicyType"),
    on="PolicyType",
    how="left"
)

silver_enriched_df = silver_enriched_df.join(
    dim_channel_df.select("ChannelId", "SchemeName"),
    on="SchemeName",
    how="left"
)



# ------------------------------------------------------------------------------------------------------------
window_spec = Window.partitionBy("TravelSalesTransactionId").orderBy(col("OptionID").desc())

latest_options_df = options_sales_df.withColumn("row_num", row_number().over(window_spec)) \
                                    .filter(col("row_num") == 1) \
                                    .select("TravelSalesTransactionId", "OptionName")

silver_enriched_df = silver_enriched_df.join(
    latest_options_df,
    on="TravelSalesTransactionId",
    how="left"
)

silver_enriched_df = silver_enriched_df.join(
    dim_option_df.select("OptionId", "OptionName"),
    on="OptionName",
    how="left"
)

# silver_enriched_df = silver_enriched_df.join(
#     dim_destination_df.select("DestinationId", "Destination"),
#     on="Destination",
#     how="left"
# )

# dim_destination_brand_df = dim_destination_df.select(
#     col("DestinationId"),
#     col("Destination").alias("_DimDestinationName"),
#     col("Brand").alias("_DestinationBrand")
# )

# silver_enriched_df = silver_enriched_df.join(
#     dim_destination_brand_df,
#     (silver_enriched_df["Destination"] == dim_destination_brand_df["_DimDestinationName"]) &
#     (
#         ((col("TravelBrandId") == 12) & (col("_DestinationBrand") == lit("Trusted"))) |
#         ((col("TravelBrandId") != 12) & (col("_DestinationBrand") == lit("All Other Brand")))
#     ),
#     "left"
# ).drop("_DimDestinationName", "_DestinationBrand")

# Primary: exact Destination match (unchanged logic)
dim_destination_brand_df = dim_destination_df.select(
    col("DestinationId"),
    col("Destination").alias("_DimDestinationName"),
    col("Brand").alias("_DestinationBrand")
)

silver_enriched_df = silver_enriched_df.join(
    dim_destination_brand_df,
    (silver_enriched_df["Destination"] == dim_destination_brand_df["_DimDestinationName"]) &
    (
        ((col("TravelBrandId") == 12) & (col("_DestinationBrand") == lit("Trusted"))) |
        ((col("TravelBrandId") != 12) & (col("_DestinationBrand") == lit("All Other Brand")))
    ),
    "left"
).drop("_DimDestinationName", "_DestinationBrand") \
 .withColumnRenamed("DestinationId", "PrimaryDestinationId")

# Fallback: ONE canonical row per (Region, Brand) — the row where Destination
# literally equals Region (e.g. DestinationId 119 "Europe 1", 120 "Europe 3",
# 114 "Europe 2", 106 "Worldwide"). Deduping like this prevents a single
# silver row from fanning out into multiple matches when several countries
# share the same Region.
region_window = Window.partitionBy("Region", "Brand").orderBy(
    (col("Destination") == col("Region")).cast("int").desc()
)
region_canonical_df = (
    dim_destination_df
    .withColumn("_rn", row_number().over(region_window))
    .filter(col("_rn") == 1)
    .select(
        col("DestinationId").alias("FallbackDestinationId"),
        col("Region").alias("_DimRegionName"),
        col("Brand").alias("_FallbackBrand")
    )
)

silver_enriched_df = silver_enriched_df.join(
    region_canonical_df,
    (silver_enriched_df["Destination"] == region_canonical_df["_DimRegionName"]) &
    (
        ((col("TravelBrandId") == 12) & (col("_FallbackBrand") == lit("Trusted"))) |
        ((col("TravelBrandId") != 12) & (col("_FallbackBrand") == lit("All Other Brand")))
    ),
    "left"
).drop("_DimRegionName", "_FallbackBrand")

# Use the primary match; only fall back to the Region-level match if primary missed
silver_enriched_df = silver_enriched_df.withColumn(
    "DestinationId",
    when(col("PrimaryDestinationId").isNotNull(), col("PrimaryDestinationId"))
    .otherwise(col("FallbackDestinationId"))
).drop("PrimaryDestinationId", "FallbackDestinationId")


# Create unique person identifier
person_df = silver_persons_df.withColumn("PersonKey", concat_ws("|", "FirstName", "Surname"))

max_age_df = silver_persons_df.groupBy("TravelSalesTransactionId").agg(
    _max("AgeAtpolicyissue").alias("MaxAgeAtpolicystart")
)

# --- Medical Score logic ---
# Step 1: Get max ScreeningScore per transaction
max_screening_df = silver_persons_df.groupBy("TravelSalesTransactionId").agg(
    _max("ScreeningScore").alias("MaxScreeningScore")
)



# Step 2: Apply bucketing logic
max_screening_df = max_screening_df.withColumn(
    "MedicalScore",
    when(col("MaxScreeningScore").isNull() | (col("MaxScreeningScore") < 1.00), "0 to 0.99")
    .when(col("MaxScreeningScore") == 1.00, "1.00")
    .when((col("MaxScreeningScore") > 1.00) & (col("MaxScreeningScore") <= 1.5), "1.01 to 1.5")
    .when((col("MaxScreeningScore") > 1.5) & (col("MaxScreeningScore") <= 2.5), "1.51 to 2.5")
    .when((col("MaxScreeningScore") > 2.5) & (col("MaxScreeningScore") <= 6.0), "2.51 to 6.0")
    .when(col("MaxScreeningScore") > 6.0, "6.01 +")
)

# Join with silver_enriched_df to get FamilyGroup
family_group_df = silver_enriched_df.select("TravelSalesTransactionId", "FamilyGroup").distinct() \
    .join(person_df, on="TravelSalesTransactionId", how="inner") \
    .groupBy("TravelSalesTransactionId", "FamilyGroup") \
    .agg(countDistinct("PersonKey").alias("UniquePersons")) \
    .withColumn(
        "FinalFamilyGroup",
        when((col("FamilyGroup") == "Individual") & (col("UniquePersons") > 1), "Group")
        .otherwise(col("FamilyGroup"))
    )

silver_enriched_df = silver_enriched_df.join(
    family_group_df.select("TravelSalesTransactionId", "FamilyGroup"),
    on="TravelSalesTransactionId",
    how="left"
)

silver_enriched_df = silver_enriched_df.join(
    dim_family_group_df.select("FamilyGroupId", col("FamilyGroup").alias("FamilyGroup")),
    on="FamilyGroup",
    how="left"
)

silver_enriched_df = silver_enriched_df.join(
    max_age_df,
    on="TravelSalesTransactionId",
    how="left"
)


# ------------------------------------------------------------------------------------------------


silver_enriched_df = silver_enriched_df.withColumn(
    "AgeGroup",
    when((col("MaxAgeAtpolicystart") >= 0) & (col("MaxAgeAtpolicystart") <= 20), "0 to 20")
    .when((col("MaxAgeAtpolicystart") >= 21) & (col("MaxAgeAtpolicystart") <= 30), "21 to 30")
    .when((col("MaxAgeAtpolicystart") >= 31) & (col("MaxAgeAtpolicystart") <= 40), "31 to 40")
    .when((col("MaxAgeAtpolicystart") >= 41) & (col("MaxAgeAtpolicystart") <= 50), "41 to 50")
    .when((col("MaxAgeAtpolicystart") >= 51) & (col("MaxAgeAtpolicystart") <= 65), "51 to 65")
    .when((col("MaxAgeAtpolicystart") >= 66) & (col("MaxAgeAtpolicystart") <= 75), "66 to 75")
    .when(col("MaxAgeAtpolicystart") >= 76, "76 +")
    .otherwise("Invalid Age")
)

silver_enriched_df = silver_enriched_df.join(
    dim_age_group_df.select("AgeGroupId", "AgeGroup"),
    on="AgeGroup",
    how="left"
)



silver_enriched_df = silver_enriched_df.join(
    max_screening_df.select("TravelSalesTransactionId", "MedicalScore"),
    on="TravelSalesTransactionId",
    how="left"
)

# Ensure no dupes in DimMedicalScore
dim_medical_score_deduped_df = dim_medical_score_df.select("MedicalScoreId", "MedicalScore").dropDuplicates(["MedicalScore"])

silver_enriched_df = silver_enriched_df.join(
    dim_medical_score_deduped_df,
    on="MedicalScore",
    how="left"
)

# Normalize HighestRatedCountry
silver_enriched_df = silver_enriched_df.withColumn("NormalizedCountry", upper(trim(col("HighestRatedCountry"))))

# Alias and normalize DimCountry
dim_country_variant = dim_country_df.withColumn("NormalizedVariant", upper(trim(col("VariantName")))).alias("variant")
dim_country_standard = dim_country_df.withColumn("NormalizedStandard", upper(trim(col("StandardName")))).alias("standard")

# First join on VariantName
silver_enriched_df = silver_enriched_df.alias("silver").join(
    dim_country_variant.select(col("variant.CountryId").alias("VariantCountryId"), col("variant.NormalizedVariant")),
    col("silver.NormalizedCountry") == col("variant.NormalizedVariant"),
    how="left"
)

# Then join on StandardName only if VariantCountryId is null
silver_enriched_df = silver_enriched_df.join(
    dim_country_standard.select(col("standard.CountryId").alias("StandardCountryId"), col("standard.NormalizedStandard")),
    (col("VariantCountryId").isNull()) & (col("silver.NormalizedCountry") == col("standard.NormalizedStandard")),
    how="left"
)

# Final CountryId selection
silver_enriched_df = silver_enriched_df.withColumn(
    "FinalCountryId",
    when(col("VariantCountryId").isNotNull(), col("VariantCountryId"))
    .otherwise(col("StandardCountryId"))
)

# Create the Brand value based on Source_Type
brand_value = when(lit(Source_Type) == "Sales", "Taurus own Brands").otherwise(lit(Source_Type))

silver_enriched_df = silver_enriched_df.join(
    dim_marketing_channel_df.select("MarketingChannelId", "MarketingChannel", "Brand"),
    (silver_enriched_df["MarketingChannel"] == dim_marketing_channel_df["MarketingChannel"]) &
    (dim_marketing_channel_df["Brand"] == brand_value),
    "left"
)


# Drop AgentId from source before joining with DimAgent
silver_enriched_df = silver_enriched_df.drop("AgentId")

silver_enriched_df = silver_enriched_df.join(
    dim_agent_df.select("AgentId", "AgentName"),
    on="AgentName",
    how="left"
)

silver_enriched_df = silver_enriched_df.drop("CampaignId")

silver_enriched_df = silver_enriched_df.join(
    dim_campaign_df.select("CampaignId", "CampaignName"),
    on="CampaignName",   # <-- source field that matches dimension
    how="left"
)

# Extract date and join with DimDate to get DateKey
silver_enriched_df = silver_enriched_df.withColumn("TransactionDateOnly", to_date(col("TransactionDate")))

silver_enriched_df = silver_enriched_df.join(
    dim_date_df,
    silver_enriched_df["TransactionDateOnly"] == dim_date_df["Date"],
    "left"
).withColumnRenamed("DateId", "DateKey")


# display(silver_enriched_df)

yesterday = date_sub(current_date(), 1)

if Data_Load_Type == "historical":
    data_to_load = silver_enriched_df.filter(
        (col("TransactionDateOnly") >= to_date(lit("2026-07-01"))) &   # <-- changed from 2023-01-01
        (col("TransactionDateOnly") <= yesterday)
    )
elif Data_Load_Type == "yesterday":
    data_to_load = silver_enriched_df.filter(
        col("TransactionDateOnly") == yesterday
    )


from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, when, substring

fact_df = data_to_load.groupBy( 
    "TravelSalesTransactionId","PolicyNumber", "DimSchemeHeaderId", "TravelBrandId", 
    "DurationId", "PolicyTypeId", "ChannelId", "OptionId", 
    "DestinationId", "FamilyGroupId", "AgeGroupId", "FinalCountryId",
    "MarketingChannelId", "AgentId","CampaignId",
    "TransactionType", "PolicyStatus",
).agg( 
    _max("DateKey").alias("DateID"), 
    _max("MedicalScoreId").alias("MedicalScoreId"), 
    _max("LeadTimeGroupId").alias("LeadTimeGroupId"), 
    _sum("TotalGrossIncIPT").alias("TotalGrossIncIPT"), 
    _sum("TotalGrossExcIPT").alias("TotalGrossExcIPT"), 
    _sum("TotalNetToUnderwriter").alias("TotalNetToUnderwriter"), 
    _count("*").alias("RecordCount") 
)

from pyspark.sql.functions import col

fact_df = (
    fact_df
    .filter(
        (col("TransactionType") == "New Issue") &
        (col("DateId").substr(1, 6) == "202501") &
        (col("PolicyNumber").substr(1, 3).isin("SOI", "SOM"))
    )
)

from pyspark.sql.functions import max as _max, sum as _sum, count as _count

fact_df = (
    data_to_load
    .groupBy(
        "PolicyNumber", "DimSchemeHeaderId", "TravelBrandId",
        "DurationId", "PolicyTypeId", "ChannelId", "OptionId",
        "DestinationId", "FamilyGroupId", "AgeGroupId", "FinalCountryId",
        "MarketingChannelId", "AgentId", "CampaignId",
        "TransactionType", "PolicyStatus"
    )
    .agg(
        _max("TravelSalesTransactionId").alias("TravelSalesTransactionId"),
        _max("DateKey").alias("DateID"),
        _max("MedicalScoreId").alias("MedicalScoreId"),
        _max("LeadTimeGroupId").alias("LeadTimeGroupId"),
        _max("TotalGrossIncIPT").alias("TotalGrossIncIPT"),
        _max("TotalGrossExcIPT").alias("TotalGrossExcIPT"),
        _max("TotalNetToUnderwriter").alias("TotalNetToUnderwriter"),
        _count("*").alias("RecordCount")
    )
)

from pyspark.sql.functions import col
from pyspark.sql.types import DecimalType

fact_df = fact_df.withColumn(
    "TotalGrossIncIPT", 
    col("TotalGrossIncIPT").cast(DecimalType(18, 2))
).withColumn(
    "TotalGrossExcIPT", 
    col("TotalGrossExcIPT").cast(DecimalType(18, 2))
).withColumn(
    "TotalNetToUnderwriter", 
    col("TotalNetToUnderwriter").cast(DecimalType(18, 2))
)
# Total number of rows
row_count = fact_df.count()
print("Total rows:", row_count)

# fact_table.delete(delete_condition)
fact_df = fact_df.withColumn("SourceType", lit(Source_Type))

fact_table = DeltaTable.forPath(spark, fact_table_path)

# Check if there are any existing records for this SourceType in the fact table
existing_records_count = spark.read.format("delta").load(fact_table_path) \
    .filter(col("SourceType") == Source_Type) \
    .count()

if existing_records_count > 0:
    # Get distinct DateIDs from the new data to delete
    date_keys_to_delete = fact_df.select("DateID").distinct()
    date_ids = [row["DateID"] for row in date_keys_to_delete.collect() if row["DateID"] is not None]
    
    if date_ids:
        date_id_list = ",".join(map(str, date_ids))
        delete_condition = f"DateID IN ({date_id_list}) AND SourceType = '{Source_Type}'"
        fact_table.delete(delete_condition)
        print(f"Deleted existing records for SourceType '{Source_Type}' with DateIDs: {date_id_list}")
    else:
        print(f"No valid DateIDs in new data for SourceType '{Source_Type}'. Skipping delete.")
else:
    print(f"No existing records found for SourceType '{Source_Type}'. Skipping delete operation.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if Data_Load_Type == "historical":
    fact_df.write.format("delta") \
    .mode("append") \
    .save(fact_table_path)
elif Data_Load_Type == "yesterday":
    fact_df.write.format("delta") \
        .mode("append") \
        .save(fact_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Full, self-contained PySpark update script (no MERGE) ---

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, min as f_min

# 1) Load dimensions (dedup to keep one row per business key)
dim_agent = (
    spark.table("TaurusGoldLH.DimAgent")
         .select("AgentId", "AgentName")
         .dropDuplicates(["AgentName"])
)

dim_campaign = (
    spark.table("TaurusGoldLH.DimCampaign")
         .select("CampaignId", "CampaignName")
         .dropDuplicates(["CampaignName"])
)

dim_marketing = (
    spark.table("TaurusGoldLH.DimMarketingChannel")
         .select("MarketingChannelId", "MarketingChannel")
         .dropDuplicates(["MarketingChannel"])
)

# 2) Your mapping rules (AgentName, CampaignName, MarketingChannel)
rules = [
    ("Oasis Travel - IDOL CON",     "CON MED",        "Idol Medical"),
    ("Oasis Travel - IDOL CON",     "CON NON MED",    "Idol Non Medical"),
    ("Oasis Travel - IDOL CTM",     "CTM MED",        "Idol Medical"),
    ("Oasis Travel - IDOL CTM",     "CTM NON MED",    "Idol Non Medical"),
    ("Oasis Travel - IDOL GOC",     "GOC MED",        "Idol Medical"),
    ("Oasis Travel - IDOL GOC",     "GOC NON MED",    "Idol Non Medical"),
    ("Oasis Travel - IDOL MONEY",   "MONEY MED",      "Idol Medical"),
    ("Oasis Travel - IDOL MONEY",   "MONEY NON MED",  "Idol Non Medical"),
    ("Oasis Travel - IDOL USWITCH", "USWITCH MED",    "Idol Medical"),
    ("Oasis Travel - IDOL USWITCH", "USWITCH NON MED","Idol Non Medical"),
    ("Oasis Travel - MSM",          "MSM",            "MSM Non Medical"),
    ("Oasis Travel - MTC",          "MTC MED",        "MSM Medical"),
    ("Oasis Travel - MTC",          "MTC NON MED",    "MSM Non Medical"),
]

rules_df = spark.createDataFrame(rules, ["AgentName", "CampaignName", "MarketingChannel"])

# 3) Attach IDs from dimensions (ensures we only use keys in Fact)
rules_ids = (
    rules_df
    .join(dim_agent,    on="AgentName",    how="left")
    .join(dim_campaign, on="CampaignName", how="left")
    .join(dim_marketing,on="MarketingChannel", how="left")
    .select("AgentName","CampaignName","MarketingChannel",
            "AgentId","CampaignId","MarketingChannelId")
)

# 4) Check for any missing IDs (optional but helpful)
missing = rules_ids.filter(
    col("AgentId").isNull() | col("CampaignId").isNull() | col("MarketingChannelId").isNull()
)
if missing.count() > 0:
    print("⚠️ Some mappings could not be resolved to IDs. Review these rows:")
    missing.show(truncate=False)
    # You can raise an error if you want strict behavior:
    # raise ValueError("Unresolved mappings in rules_ids; fix dimension content or rule text.")
else:
    print("✅ All mapping rules resolved to IDs.")

# 5) Deduplicate to a single MarketingChannelId per (AgentId, CampaignId)
#    (If duplicates exist, take the smallest ID deterministically.)
final_map = (
    rules_ids
    .dropna(subset=["AgentId","CampaignId","MarketingChannelId"])
    .groupBy("AgentId","CampaignId")
    .agg(f_min("MarketingChannelId").alias("MarketingChannelId"))
)

# 6) Update the Fact table using DeltaTable.update per (AgentId, CampaignId)
fact_dt = DeltaTable.forName(spark, "TaurusGoldLH.FactSalesAnalysis")

rows = final_map.collect()
print(f"Found {len(rows)} (AgentId, CampaignId) mapping pairs to update.")

for r in rows:
    agent_id = int(r["AgentId"])
    campaign_id = int(r["CampaignId"])
    mc_id = int(r["MarketingChannelId"])

    condition = (
        f"SourceType = 'Oasis' AND MarketingChannelId IS NULL "
        f"AND AgentId = {agent_id} AND CampaignId = {campaign_id}"
    )

    # Update the FK (dimension key). Change to also set a name column if your fact has it.
    fact_dt.update(
        condition=condition,
        set={"MarketingChannelId": f"{mc_id}"}
    )

    print(f"✅ Updated rows where AgentId={agent_id}, CampaignId={campaign_id} → MarketingChannelId={mc_id}")

# 7) (Optional) Verify remaining nulls after update
remaining = spark.sql("""
SELECT COUNT(*) AS remaining_nulls
FROM TaurusGoldLH.FactSalesAnalysis
WHERE SourceType='Oasis' AND MarketingChannelId IS NULL
""").collect()[0]["remaining_nulls"]

print(f"🔎 Remaining Oasis rows with NULL MarketingChannelId: {remaining}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Full, self-contained PySpark update script (AtoZ full) ---

from delta.tables import DeltaTable
from pyspark.sql.functions import col, min as f_min

# 1) Load dimensions (dedup to keep one row per business key)
dim_agent = (
    spark.table("TaurusGoldLH.DimAgent")
         .select("AgentId", "AgentName")
         .dropDuplicates(["AgentName"])
)
dim_campaign = (
    spark.table("TaurusGoldLH.DimCampaign")
         .select("CampaignId", "CampaignName")
         .dropDuplicates(["CampaignName"])
)
dim_marketing = (
    spark.table("TaurusGoldLH.DimMarketingChannel")
         .select("MarketingChannelId", "MarketingChannel")
         .dropDuplicates(["MarketingChannel"])
)

# 2) All AtoZ mapping rules (AgentName, CampaignName, MarketingChannel)
rules = [
    ("AtoZ Direct AMT Renewal",      "RENEW20",         "Direct Renewal"),
    ("AtoZ IDOL AMT Renewal",        "RENEW20",         "Aggregator Renewal"),
    ("AtoZ Insurance Compare Cover", "AtoZ COMCO MED",  "Idol Medical"),
    ("AtoZ Insurance Compare Cover", "AtoZ COMCO NON",  "Idol Non Medical"),
    ("AtoZ Insurance Confused.com",  "AtoZ CON MED",    "Idol Medical"),
    ("AtoZ Insurance Confused.com",  "AtoZ CON NON",    "Idol Non Medical"),
    ("AtoZ Insurance CYTI-MSM",      "AtoZ CYTI-MSM",       "MSM Medical"),
    ("AtoZ Insurance CYTI-MSM",      "AtoZ MSM White Label","MSM Medical"),
    ("AtoZ Insurance Direct",        "AtoZ Website",    "Direct New"),
    ("AtoZ Insurance Direct",        "CREST10",        "Direct New"),
    ("AtoZ Insurance Direct",        "FOOTY10",         "Direct New"),
    ("AtoZ Insurance Direct",        "GIFT10",          "Direct New"),
    ("AtoZ Insurance Direct",        "JCB10",           "Direct New"),
    ("AtoZ Insurance Direct",        "MATESRATES10",    "Direct New"),
    ("AtoZ Insurance Direct",        "MATESRATES15",    "Direct New"),
    ("AtoZ Insurance Direct",        "NAT05",           "Direct New"),
    ("AtoZ Insurance Direct",        "RENEW20",         "Direct New"),
    ("AtoZ Insurance Direct",        "REPEAT10",        "Direct New"),
    ("AtoZ Insurance Direct",        "SJPP10",          "Direct New"),
    ("AtoZ Insurance Direct",        "Travel Partner",  "Direct New"),
    ("AtoZ Insurance Direct",        "Welcome10",       "Direct New"),
    ("AtoZ Insurance Go Compare",    "AtoZ GOCO MED",   "Idol Medical"),
    ("AtoZ Insurance Go Compare",    "AtoZ GOCO NON",   "Idol Non Medical"),
    ("AtoZ Insurance IDOL CTM",      "AtoZ CTM MED",    "Idol Medical"),
    ("AtoZ Insurance IDOL CTM",      "AtoZ CTM NON",    "Idol Non Medical"),
    ("AtoZ Insurance Money",         "AtoZ Money MED",  "Idol Medical"),
    ("AtoZ Insurance MSM",           "AtoZ MSM",        "MSM Non Medical"),
    ("AtoZ Insurance MTC",           "AtoZ MTC",        "MSM Medical"),
    ("AtoZ Insurance Uswitch",       "AtoZ US MED",     "Idol Medical"),
    ("AtoZ Insurance Uswitch",       "AtoZ US NON",     "Idol Non Medical"),
    ("AtoZ MSM CYTI AMT Renewal",    "RENEW20",         "Aggregator Renewal"),
]

rules_df = spark.createDataFrame(rules, ["AgentName", "CampaignName", "MarketingChannel"])

# 3) Attach IDs from dimensions
rules_ids = (
    rules_df
    .join(dim_agent,    on="AgentName",    how="left")
    .join(dim_campaign, on="CampaignName", how="left")
    .join(dim_marketing,on="MarketingChannel", how="left")
    .select("AgentName","CampaignName","MarketingChannel",
            "AgentId","CampaignId","MarketingChannelId")
)

# 4) Check for any missing IDs
missing = rules_ids.filter(
    col("AgentId").isNull() | col("CampaignId").isNull() | col("MarketingChannelId").isNull()
)
if missing.count() > 0:
    print("⚠️ Some mappings could not be resolved to IDs. Review these rows:")
    missing.show(truncate=False)
else:
    print("✅ All mapping rules resolved to IDs.")

# 5) Deduplicate to one MarketingChannelId per (AgentId, CampaignId)
final_map = (
    rules_ids
    .dropna(subset=["AgentId","CampaignId","MarketingChannelId"])
    .groupBy("AgentId","CampaignId")
    .agg(f_min("MarketingChannelId").alias("MarketingChannelId"))
)

# 6) Update the Fact table with DeltaTable.update
fact_dt = DeltaTable.forName(spark, "TaurusGoldLH.FactSalesAnalysis")

rows = final_map.collect()
print(f"Found {len(rows)} mapping pairs to update.")

for r in rows:
    agent_id = int(r["AgentId"])
    campaign_id = int(r["CampaignId"])
    mc_id = int(r["MarketingChannelId"])

    condition = (
        f"SourceType = 'Atoz' AND MarketingChannelId IS NULL "
        f"AND AgentId = {agent_id} AND CampaignId = {campaign_id}"
    )

    fact_dt.update(
        condition=condition,
        set={"MarketingChannelId": f"{mc_id}"}
    )

    print(f"✅ Updated rows where AgentId={agent_id}, CampaignId={campaign_id} → MarketingChannelId={mc_id}")

# 7) Verify remaining NULLs
# remaining = spark.sql("""
# SELECT COUNT(*) AS remaining_nulls
# FROM TaurusGoldLH.FactSalesAnalysis
# WHERE SourceType='Atoz' AND MarketingChannelId IS NULL
# """).collect()[0]["remaining_nulls"]

print(f"🔎 Remaining Atoz rows with NULL MarketingChannelId: {remaining}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check what Source_Type value you're using
print(f"Source_Type: {Source_Type}")

# Check distinct Brand values in dimension table
print("Brands in DimMarketingChannel:")
dim_marketing_channel_df.select("Brand").distinct().show()

# # Check distinct MarketingChannel values in your source before join
# print("MarketingChannel values in source data:")
# silver_enriched_df.select("MarketingChannel").distinct().show(50, truncate=False)

# # Check what's in the dimension table
# print("MarketingChannel dimension data:")
# dim_marketing_channel_df.select("MarketingChannelId", "MarketingChannel", "Brand").show(50, truncate=False)

# # Check if any matches would occur
# print("Potential matches:")
# silver_enriched_df.select("MarketingChannel") \
#     .join(dim_marketing_channel_df, "MarketingChannel", "inner") \
#     .filter(col("Brand") == lit(Source_Type)) \
#     .show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#!/usr/bin/env python
# coding: utf-8

# ## Verify_SOI_Europe1_July2026_Fixed
#
# Read-only verification. Confirms that after the DimDestination fix, join
# hardening, and full historical backfill, FactSalesAnalysis now correctly
# shows July 2026 SOI Direct / SOI Agg data for Region = 'Europe 1'.

# In[1]:

from pyspark.sql.functions import col, sum as _sum, count as _count

WORKSPACE = "Taurus_TravelInsurance_Dev"
Gold_LAKEHOUSE = "TaurusGoldLH"

fact_table_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/FactSalesAnalysis"
dim_destination_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimDestination"
dim_channel_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{Gold_LAKEHOUSE}.Lakehouse/Tables/DimChannel"

fact_df = spark.read.format("delta").load(fact_table_path)
dim_destination_df = spark.read.format("delta").load(dim_destination_path)
dim_channel_df = spark.read.format("delta").load(dim_channel_path)

print("Loaded FactSalesAnalysis, DimDestination, DimChannel.")


# In[2]:

# ------------------------------------------------------------------
# CHECK 1: Overall — any July 2026 SOI rows in Fact at all?
# ------------------------------------------------------------------
fact_soi_july = fact_df.filter(
    (col("SourceType") == "Sales") &
    (col("DateID").substr(1, 6) == "202607")
)

print("=" * 80)
print("CHECK 1: Total SOI/Sales rows in Fact for July 2026 (DateID like 202607%)")
print(f"Total rows: {fact_soi_july.count()}")
print("=" * 80)


# In[3]:

# ------------------------------------------------------------------
# CHECK 2: Join to DimDestination + DimChannel, filter to Region = 'Europe 1'
#          AND Channel in ('SOI Direct', 'SOI Agg') — this is the exact
#          slice that was previously missing.
# ------------------------------------------------------------------
fact_enriched = fact_soi_july.join(
    dim_destination_df.select(
        col("DestinationId"), col("Region"), col("Destination").alias("DimDestinationName")
    ),
    on="DestinationId",
    how="left"
).join(
    dim_channel_df.select(col("ChannelId"), col("Channel")).dropDuplicates(["ChannelId"]),
    on="ChannelId",
    how="left"
)

europe1_soi_result = fact_enriched.filter(
    (col("Region") == "Europe 1") &
    (col("Channel").isin("SOI Direct", "SOI Agg"))
)

print("=" * 80)
print("CHECK 2: July 2026 Fact rows — Region = 'Europe 1', Channel in (SOI Direct, SOI Agg)")
print(f"Total rows: {europe1_soi_result.count()}")
print("=" * 80)

print("\nBreakdown by Channel:")
europe1_soi_result.groupBy("Channel").agg(
    _count("*").alias("RecordCount"),
    _sum("TotalGrossIncIPT").alias("TotalGrossIncIPT"),
    _sum("TotalGrossExcIPT").alias("TotalGrossExcIPT"),
    _sum("TotalNetToUnderwriter").alias("TotalNetToUnderwriter"),
).orderBy("Channel").show(truncate=False)


# In[4]:

# ------------------------------------------------------------------
# CHECK 3: Show sample rows so you can eyeball the actual data
# ------------------------------------------------------------------
print("=" * 80)
print("CHECK 3: Sample rows (Region = Europe 1, Channel = SOI Direct/SOI Agg, July 2026)")
print("=" * 80)
display(
    europe1_soi_result.select(
        "PolicyNumber", "DateID", "Channel", "Region", "DimDestinationName",
        "TotalGrossIncIPT", "TotalGrossExcIPT", "TotalNetToUnderwriter", "RecordCount"
    ).orderBy(col("DateID").desc())
)


# In[5]:

# ------------------------------------------------------------------
# CHECK 4: Confirm no remaining NULL DestinationId for SOI July 2026
#          (proves the fix, not just that some rows now happen to work)
# ------------------------------------------------------------------
null_dest_count = fact_soi_july.filter(col("DestinationId").isNull()).count()

print("=" * 80)
print(f"CHECK 4: Remaining NULL DestinationId for SOI/Sales, July 2026: {null_dest_count}")
print("(should be 0, or only reflect genuinely new/unmapped destinations)")
print("=" * 80)


# In[6]:

# ------------------------------------------------------------------
# CHECK 5: Whole-table sanity check — any unmatched Destination strings
#          left anywhere in Fact (not just July 2026, not just SOI)
# ------------------------------------------------------------------
WORKSPACE_SILVER = "Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = "TaurusSilverLH"
src_file_path = f"abfss://{WORKSPACE_SILVER}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/silver_travel_sales_transactions_sales"
silver_df = spark.read.format("delta").load(src_file_path)

unmatched_anywhere = silver_df.select("Destination").distinct().join(
    dim_destination_df.select("Destination").distinct(),
    on="Destination",
    how="left_anti"
)

print("=" * 80)
print("CHECK 5: Any Destination values in silver with NO match anywhere in DimDestination")
print(f"Unmatched count: {unmatched_anywhere.count()} (should be 0 after the fix)")
print("=" * 80)
unmatched_anywhere.show(100, truncate=False)


# In[7]:

print("Summary:")
print("- CHECK 1/2/3 confirm July 2026 SOI Direct/Agg data for Europe 1 is now present.")
print("- CHECK 4 confirms the DestinationId gap for this slice is closed.")
print("- CHECK 5 confirms no unmatched Destination strings remain anywhere in source.")
print("If CHECK 5 shows any rows, add them to DimDestination the same way as before,")
print("then re-run the backfill for just those dates.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
