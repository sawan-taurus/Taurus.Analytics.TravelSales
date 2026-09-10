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

Source_Type = "Atoz"  # or "Atoz" or "Atoz"
Data_Load_Type = "historical"  # or "historical"yesterday


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
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import datediff, current_date, floor, col, when
from pyspark.sql.types import DecimalType

WORKSPACE = "Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = "TaurusSilverLH"
Gold_LAKEHOUSE = "TaurusGoldLH"

src_table_name = "silver_travel_enquiry_sales"
options_table_name = "silver_travel_enquiry_options_sales"
persons_table_name = "silver_travel_enquiry_persons_sales"
enquriy_results_table_name = 'silver_travel_enquiry_results_sales'

src_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{src_table_name}"
options_sales_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{options_table_name}"
persons_sales_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{persons_table_name}"
enquriy_results_table_name_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{enquriy_results_table_name}"

SRC_TABLE = src_table_name
DIM_DATE_TABLE = "DimDate"
DIM_SCHEME_HEADERS = "DimSchemeHeaders"
FACT_TABLE = "FactQuoteAnalysis"
yesterday = date_sub(current_date(), 1)

if Data_Load_Type == "historical":
    silver_df = spark.read.format("delta").load(src_file_path)
    silver_df_filtered = silver_df.withColumn("QuoteDateOnly", to_date(col("QuoteDate"))).filter(
        (col("QuoteDateOnly") >= to_date(lit("2023-01-01"))) &
        (col("QuoteDateOnly") <= yesterday)
    )

elif Data_Load_Type == "yesterday":
    silver_df = spark.read.format("delta").load(src_file_path)
    silver_df_filtered = silver_df.withColumn("QuoteDateOnly", to_date(col("QuoteDate"))).filter(
        col("QuoteDateOnly") == yesterday
    )

else:
    silver_df = spark.read.format("delta").load(src_file_path)
    silver_df_filtered = silver_df.withColumn("QuoteDateOnly", to_date(col("QuoteDate")))

options_sales_df = spark.read.format("delta").load(options_sales_path)
silver_persons_df = spark.read.format("delta").load(persons_sales_path)
enquriy_results_persons_df = spark.read.format("delta").load(enquriy_results_table_name_path)
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

fact_existing_df = spark.read.format("delta").load(fact_table_path)
# ----------------------------------------------------------------------------------------------------
silver_df_filtered = silver_df_filtered.withColumnRenamed("SchemeType", "SchemeType_Silver")

# Proceed with join
silver_enriched_df = silver_df_filtered.join(
    enquriy_results_persons_df,
    on="TravelEnquiryID",
    how="inner"
)

silver_enriched_df = silver_enriched_df.drop("SchemeType")
silver_enriched_df = silver_enriched_df.withColumnRenamed("SchemeType_Silver", "SchemeType")
silver_enriched_df = silver_enriched_df.withColumn(
    "SilverTravelBrand",
    when(col("BrandIdentifier") == "ViVA", "viva_ins")
    .when(col("BrandIdentifier") == "Start Travel", "start_travel")
    .when(col("BrandIdentifier") == "SwitchedOn", "switched_on")
    .when(col("BrandIdentifier") == "Trusted", "trusted_ins")
    .otherwise("UNKNOWN")
)

silver_enriched_df = silver_enriched_df.join(
    broadcast(dim_travel_brand_df.select("TravelBrandId", "TravelBrand")),
    silver_enriched_df["SilverTravelBrand"] == dim_travel_brand_df["TravelBrand"],
    how="left"
)

# Ensure QuoteDate is cast to date
silver_enriched_df = silver_enriched_df.withColumn("QuoteDateOnly", to_date(col("QuoteDate")))

# Join with DimDate to get DateId
silver_enriched_df = silver_enriched_df.join(
    broadcast(dim_date_df.select("Date", "DateId")),
    silver_enriched_df["QuoteDateOnly"] == dim_date_df["Date"],
    how="left"
)

silver_enriched_df = silver_enriched_df.withColumn(
    "LeadTimeGroup",
    when(datediff(col("StartDate"), col("QuoteDate")) < 0, "Invalid (<0)")
    .when(datediff(col("StartDate"), col("QuoteDate")).between(0, 3), "0 to 3")
    .when(datediff(col("StartDate"), col("QuoteDate")).between(4, 8), "4 to 8")
    .when(datediff(col("StartDate"), col("QuoteDate")).between(9, 15), "9 to 15")
    .when(datediff(col("StartDate"), col("QuoteDate")).between(16, 30), "16 to 30")
    .when(datediff(col("StartDate"), col("QuoteDate")).between(31, 60), "31 to 60")
    .when(datediff(col("StartDate"), col("QuoteDate")) >= 61, "61+")
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

# Alias the dimension table
dim_policy_type_alias = dim_policy_type_df.select("PolicyTypeId", "PolicyType").alias("dim")

# Drop any existing PolicyType or PolicyTypeId columns to avoid duplication
columns_to_drop = [c for c in ["PolicyType", "PolicyTypeId"] if c in silver_enriched_df.columns]
silver_enriched_df = silver_enriched_df.drop(*columns_to_drop)

# Join using SchemeType and select only required columns
silver_enriched_df = silver_enriched_df.join(
    dim_policy_type_alias,
    silver_enriched_df["SchemeType"] == col("dim.PolicyType"),
    how="left"
).select(
    silver_enriched_df["*"],  # all original columns
    col("dim.PolicyTypeId")   # only the needed column from the dimension
)

silver_enriched_df = silver_enriched_df.join(
    dim_channel_df.select("ChannelId", "SchemeName"),
    on="SchemeName",
    how="left"
)

# Step 1: Get the latest option per TravelEnquiryResultID
window_spec = Window.partitionBy("TravelEnquiryResultID").orderBy(col("OptionID").desc())

latest_options_df = options_sales_df.withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .select("TravelEnquiryResultID", "Option")  # Use 'Option' instead of 'OptionName'

# Step 2: Join latest options to silver_enriched_df
silver_enriched_df = silver_enriched_df.join(
    latest_options_df,
    on="TravelEnquiryResultID",
    how="left"
)

# Step 3: Join with DimOption using Option == OptionName
silver_enriched_df = silver_enriched_df.join(
    dim_option_df.select("OptionId", "OptionName"),
    silver_enriched_df["Option"] == dim_option_df["OptionName"],
    how="left"
)

silver_enriched_df = silver_enriched_df.join(
    dim_destination_df.select("DestinationId", "Destination"),
    silver_enriched_df["MagentaDestination"] == dim_destination_df["Destination"],
    how="left"
)

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

# Step 1: Calculate age from PersonDOB
silver_persons_df = silver_persons_df.withColumn(
    "Age", floor(datediff(current_date(), col("PersonDOB")) / 365.25)
)

# Step 2: Get max age per TravelEnquiryId
max_age_df = silver_persons_df.groupBy("TravelEnquiryId").agg(
    _max("Age").alias("MaxAgeAtpolicystart")
)

# Step 3: Join max age into silver_enriched_df
silver_enriched_df = silver_enriched_df.join(
    max_age_df,
    on="TravelEnquiryId",
    how="left"
)

# Step 4: Create AgeGroup column using case logic
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

# Step 5: Join with DimAgeGroup to get AgeGroupId
silver_enriched_df = silver_enriched_df.join(
    dim_age_group_df.select("AgeGroupId", "AgeGroup"),
    on="AgeGroup",
    how="left"
)

# Step 1: Get max PersonMedicalScreeningScore per transaction
max_screening_df = silver_persons_df.groupBy("TravelEnquiryId").agg(
    _max("PersonMedicalScreeningScore").alias("MaxScreeningScore")
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

# Step 3: Join with silver_enriched_df
silver_enriched_df = silver_enriched_df.join(
    max_screening_df.select("TravelEnquiryId", "MedicalScore"),
    on="TravelEnquiryId",
    how="left"
)

# Step 4: Deduplicate DimMedicalScore
dim_medical_score_deduped_df = dim_medical_score_df.select("MedicalScoreId", "MedicalScore").dropDuplicates(["MedicalScore"])

# Step 5: Join to get MedicalScoreId
silver_enriched_df = silver_enriched_df.join(
    dim_medical_score_deduped_df,
    on="MedicalScore",
    how="left"
)

# Join with DimFamilyGroup to get FamilyGroupId
silver_enriched_df = silver_enriched_df.join(
    dim_family_group_df.select("FamilyGroupId", "FamilyGroup"),
    silver_enriched_df["GroupType"] == dim_family_group_df["FamilyGroup"],
    how="left"
)

silver_enriched_df = silver_enriched_df.join(
    dim_marketing_channel_df.select("MarketingChannelId", "MarketingChannel"),
    on="MarketingChannel",
    how="left"
)

# ------------------------------------------------------------------------------------------------------------
# Load the Delta table
fact_table = DeltaTable.forPath(spark, fact_table_path)

fact_df = silver_enriched_df.groupBy(
    "TravelBrandId", "DateId", "LeadTimeGroupId", "DurationId", "PolicyTypeId",
    "ChannelId", "OptionId", "DestinationId", "AgeGroupId", "MedicalScoreId", "FamilyGroupId","MarketingChannelId"
).agg(
    _sum("TotalGrossIncIPT").alias("TotalGrossIncIPT"),
    (_sum("TotalGrossIncIPT") / lit(1.2)).cast(DecimalType(18, 2)).alias("TotalGrossExcIPT"),
    _count("*").alias("RecordCount")
)

# display(fact_df)

# Get distinct DateIDs from the current batch
date_keys_to_delete = fact_df.select("DateID").distinct()
date_ids = [row["DateID"] for row in date_keys_to_delete.collect()]

# Build the delete condition
date_id_list = ",".join(map(str, date_ids))
delete_condition = f"DateId IN ({date_id_list})"

# Perform the delete
fact_table.delete(delete_condition)

if Data_Load_Type == "historical":
    fact_df.write.format("delta") \
    .mode("overwrite") \
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
