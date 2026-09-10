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

from pyspark.sql.functions import (
    col, lit, to_date, date_sub, current_date, when,
    sum as _sum, countDistinct as _countDistinct,max as _max,
    datediff   # 👉 add this
)

from delta.tables import DeltaTable
from pyspark.sql.functions import broadcast

# --------------------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------------------
# ADD THIS LINE HERE - Fix for reading ancient dates
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")

WORKSPACE = "Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = "TaurusSilverLH"
GOLD_LAKEHOUSE = "TaurusGoldLH"

DATA_LOAD_TYPE = "historical"
HISTORICAL_START_DATE = "2023-01-01"

SRC_TABLE = "silver_travel_enquiry_sales"
RESULTS_TABLE = "silver_travel_enquiry_results_sales"
TRANSACTIONS_TABLE = "silver_travel_sales_transactions_sales"
persons_table_name = "silver_travel_sales_transaction_persons_sales"
enquiry_persons_table_name = "silver_travel_enquiry_persons_sales"  # ADD THIS
FACT_TABLE = "FactEnquiries"

src_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{SRC_TABLE}"
results_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{RESULTS_TABLE}"
transactions_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{TRANSACTIONS_TABLE}"
persons_sales_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{persons_table_name}"
enquiry_persons_sales_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{enquiry_persons_table_name}"  # ADD THIS

path_dim_date = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimDate"
path_dim_travel_brand = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimTravelBrand"
path_dim_duration = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimDuration"
path_dim_policy_type = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimPolicyType"
path_dim_destination = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimDestination"
path_dim_family_group = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimFamilyGroup"
path_dim_marketing = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimMarketingChannel"
path_dim_agent = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimAgent"
path_dim_lead_time_group = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimLeadTimeGroup"
dim_lead_time_group_df = spark.read.format("delta").load(path_dim_lead_time_group).select("LeadTimeGroupId", "LeadTimeGroup")
fact_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/{FACT_TABLE}"
path_dim_medical_score = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimMedicalScore"

dim_medical_score_df = (
    spark.read.format("delta")
    .load(path_dim_medical_score)
    .select("MedicalScoreId", "MedicalScore")
)

path_dim_age_group = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{GOLD_LAKEHOUSE}.Lakehouse/Tables/DimAgeGroup"
dim_age_group_df = (
    spark.read.format("delta")
    .load(path_dim_age_group)
    .select("AgeGroupId", "AgeGroup")
)

src_df = spark.read.format("delta").load(src_path)
src_df = src_df.withColumn("QuoteDateOnly", to_date(col("QuoteDate")))

if DATA_LOAD_TYPE.lower() == "historical":
    src_df = src_df.filter(
        (col("QuoteDateOnly") >= to_date(lit(HISTORICAL_START_DATE)))
        & (col("QuoteDateOnly") <= date_sub(current_date(), 1))
    )
elif DATA_LOAD_TYPE.lower() == "yesterday":
    src_df = src_df.filter(col("QuoteDateOnly") == date_sub(current_date(), 1))

# --------------------------------------------------------------------------------------
# READ DIMENSIONS
# --------------------------------------------------------------------------------------
silver_persons_df = spark.read.format("delta").load(persons_sales_path)
dim_date = spark.read.format("delta").load(path_dim_date).select(col("Date").alias("DimDate"), "DateId")
dim_travel_brand = spark.read.format("delta").load(path_dim_travel_brand).select("TravelBrandId", "TravelBrand")
dim_duration = spark.read.format("delta").load(path_dim_duration).select("DurationId", "Duration")
dim_policy_type = spark.read.format("delta").load(path_dim_policy_type).select("PolicyTypeId", col("PolicyType"))
dim_destination = spark.read.format("delta").load(path_dim_destination).select("DestinationId", "Destination")
dim_family_group = spark.read.format("delta").load(path_dim_family_group).select("FamilyGroupId", "FamilyGroup")
dim_marketing = spark.read.format("delta").load(path_dim_marketing).select("MarketingChannelId", "MarketingChannel", "Brand")
dim_agent = spark.read.format("delta").load(path_dim_agent).select(
    col("AgentId").alias("DimAgentId"), 
    "AgentName"
)
# --------------------------------------------------------------------------------------
# DERIVATIONS IN SOURCE TO MATCH KEYS
# --------------------------------------------------------------------------------------
src_df = src_df.withColumn(
    "_TravelBrandKey",
    when(col("BrandIdentifier") == "ViVA", lit("viva_ins"))
    .when(col("BrandIdentifier") == "Start Travel", lit("start_travel"))
    .when(col("BrandIdentifier") == "SwitchedOn", lit("switched_on"))
    .when(col("BrandIdentifier") == "Trusted", lit("trusted_ins"))
    .otherwise(lit("UNKNOWN"))
)

src_df = src_df.withColumn(
    "_DurationBucket",
    when((col("Duration") >= 1) & (col("Duration") <= 3), lit("1 to 3"))
    .when((col("Duration") >= 4) & (col("Duration") <= 5), lit("4 to 5"))
    .when((col("Duration") >= 6) & (col("Duration") <= 10), lit("6 to 10"))
    .when((col("Duration") >= 11) & (col("Duration") <= 17), lit("11 to 17"))
    .when((col("Duration") >= 18) & (col("Duration") <= 24), lit("18 to 24"))
    .when((col("Duration") >= 25) & (col("Duration") <= 31), lit("25 to 31"))
    .when(col("Duration") >= 32, lit("32 +"))
    .otherwise(lit("Invalid Duration"))
)

# ADD THIS: Join with enquiry persons table
enquiry_persons_df = spark.read.format("delta").load(enquiry_persons_sales_path)
src_df = src_df.join(enquiry_persons_df, on="TravelEnquiryID", how="left")

fact_ready = (
    src_df
    .join(broadcast(dim_date), src_df["QuoteDateOnly"] == dim_date["DimDate"], "left")
    .join(broadcast(dim_travel_brand), col("_TravelBrandKey") == dim_travel_brand["TravelBrand"], "left")
    .join(broadcast(dim_duration), col("_DurationBucket") == dim_duration["Duration"], "left")
    .join(broadcast(dim_policy_type), src_df["SchemeType"] == dim_policy_type["PolicyType"], "left")
    .join(broadcast(dim_destination), src_df["MagentaDestination"] == dim_destination["Destination"], "left")
    .join(broadcast(dim_family_group), src_df["GroupType"] == dim_family_group["FamilyGroup"], "left")
    .join(
        broadcast(
            dim_marketing.select("MarketingChannelId", "MarketingChannel", "Brand")
        ),
        (
            (src_df["MarketingChannel"] == dim_marketing["MarketingChannel"]) &
            (dim_marketing["Brand"] == lit("Sales"))
        ),
        "left"
    )
    .join(broadcast(dim_agent), src_df["AgentName"] == dim_agent["AgentName"], "left")
    .withColumn("AgentId", col("DimAgentId"))
    .drop("DimAgentId", dim_agent["AgentName"])
)
# --------------------------------------------------------------------------------------
# ADD IsQuoted FLAG
# --------------------------------------------------------------------------------------
results_df = spark.read.format("delta").load(results_path).select("TravelEnquiryID", "ProviderReference").distinct()

fact_ready = (
    fact_ready
    .join(results_df.withColumn("IsQuoted", lit(1)), on="TravelEnquiryID", how="left")
    .withColumn("IsQuoted", when(col("IsQuoted").isNull(), lit(0)).otherwise(col("IsQuoted")))
)

# --------------------------------------------------------------------------------------
# ADD IsSold FLAG + TotalTravellers
# --------------------------------------------------------------------------------------
transactions_df = (
    spark.read.format("delta").load(transactions_path)
    .select(
        col("ExternalReference"),
        col("Pax").alias("_TotalTravellers")
    )
)

fact_ready = fact_ready.withColumn(
    "LeadTimeGroup",
    when(datediff(col("StartDate"), to_date(col("QuoteDate"))) < 0, "Invalid (<0)")
    .when(datediff(col("StartDate"), to_date(col("QuoteDate"))).between(0, 3), "0 to 3")
    .when(datediff(col("StartDate"), to_date(col("QuoteDate"))).between(4, 8), "4 to 8")
    .when(datediff(col("StartDate"), to_date(col("QuoteDate"))).between(9, 15), "9 to 15")
    .when(datediff(col("StartDate"), to_date(col("QuoteDate"))).between(16, 30), "16 to 30")
    .when(datediff(col("StartDate"), to_date(col("QuoteDate"))).between(31, 60), "31 to 60")
    .when(datediff(col("StartDate"), to_date(col("QuoteDate"))) >= 61, "61+")
    .otherwise("Invalid Dates")
)

silver_persons_df = spark.read.format("delta").load(persons_sales_path)

max_screening_df = (
    enquiry_persons_df
    .groupBy("TravelEnquiryID")
    .agg(_max("PersonMedicalScreeningScore").alias("MaxScreeningScore"))
    .withColumn(
        "MedicalScore",
        when(col("MaxScreeningScore").isNull() | (col("MaxScreeningScore") < 1.00), lit("0 to 0.99"))
        .when(col("MaxScreeningScore") == 1.00, lit("1.00"))
        .when((col("MaxScreeningScore") > 1.00) & (col("MaxScreeningScore") <= 1.5), lit("1.01 to 1.5"))
        .when((col("MaxScreeningScore") > 1.5) & (col("MaxScreeningScore") <= 2.5), lit("1.51 to 2.5"))
        .when((col("MaxScreeningScore") > 2.5) & (col("MaxScreeningScore") <= 6.0), lit("2.51 to 6.0"))
        .when(col("MaxScreeningScore") > 6.0, lit("6.01 +"))
    )
)


from pyspark.sql.functions import floor, months_between

max_age_df = (
    enquiry_persons_df
    .join(
        src_df.select("TravelEnquiryID", col("StartDate").alias("EnquiryStartDate")),
        on="TravelEnquiryID",
        how="left"
    )
    .withColumn(
        "AgeAtEnquiryStart",
        floor(months_between(col("EnquiryStartDate"), col("PersonDOB")) / 12)
    )
    .groupBy("TravelEnquiryID")
    .agg(_max("AgeAtEnquiryStart").alias("MaxAgeAtEnquiryStart"))
    .withColumn(
        "AgeGroup",
        when((col("MaxAgeAtEnquiryStart") >= 0) & (col("MaxAgeAtEnquiryStart") <= 20), lit("0 to 20"))
        .when((col("MaxAgeAtEnquiryStart") >= 21) & (col("MaxAgeAtEnquiryStart") <= 30), lit("21 to 30"))
        .when((col("MaxAgeAtEnquiryStart") >= 31) & (col("MaxAgeAtEnquiryStart") <= 40), lit("31 to 40"))
        .when((col("MaxAgeAtEnquiryStart") >= 41) & (col("MaxAgeAtEnquiryStart") <= 50), lit("41 to 50"))
        .when((col("MaxAgeAtEnquiryStart") >= 51) & (col("MaxAgeAtEnquiryStart") <= 65), lit("51 to 65"))
        .when((col("MaxAgeAtEnquiryStart") >= 66) & (col("MaxAgeAtEnquiryStart") <= 75), lit("66 to 75"))
        .when(col("MaxAgeAtEnquiryStart") >= 76, lit("76 +"))
        .otherwise(lit("Invalid Age"))
    )
    .select("TravelEnquiryID", "AgeGroup")
)

fact_ready = fact_ready.join(max_screening_df, on="TravelEnquiryID", how="left")

fact_ready = fact_ready.join(max_age_df, on="TravelEnquiryID", how="left")

fact_ready = (
    fact_ready
    .join(
        transactions_df.withColumn("IsSoldFlag", lit(1)),
        fact_ready["ProviderReference"] == transactions_df["ExternalReference"],
        "left"
    )
    .withColumn("IsSold", when((col("IsQuoted") == 1) & (col("IsSoldFlag") == 1), lit(1)).otherwise(lit(0)))
    .withColumn("TotalTravellers", when(col("_TotalTravellers").isNull(), lit(0)).otherwise(col("_TotalTravellers")))
    .drop("IsSoldFlag", "_TotalTravellers", "ExternalReference")
)

from pyspark.sql.functions import count as _count, max as _max
from pyspark.sql.types import IntegerType

fact_agg = (
    fact_ready
    .groupBy(
        "TravelEnquiryID",
        "DateId",
        "TravelBrandId",
        "DurationId",
        "PolicyTypeId",
        "DestinationId",
        "FamilyGroupId",
        "MarketingChannelId",
        "AgentId",
        "IsQuoted"
    )
    .agg(
        _count("*").alias("NoOfQuotes"),
        _max("IsSold").alias("IsSold"),
        _max("TotalTravellers").alias("TotalTravellers"),
        _max("LeadTimeGroup").alias("LeadTimeGroup"),   # NEW
        _max("MedicalScore").alias("MedicalScore"),   # NEW
        _max("AgeGroup").alias("AgeGroup") 
    )
    # join with DimLeadTimeGroup to get LeadTimeGroupId
    .join(dim_lead_time_group_df, on="LeadTimeGroup", how="left")
    .join(dim_medical_score_df, on="MedicalScore", how="left")   # NEW
    .join(dim_age_group_df, on="AgeGroup", how="left")   # ADD THIS
    .withColumn("NoOfQuotes", col("NoOfQuotes").cast(IntegerType()))
    .withColumn("TotalTravellers", when(col("TotalTravellers").isNull(), lit(0)).otherwise(col("TotalTravellers").cast(IntegerType())))
)

fact_agg = fact_agg.select(
    "TravelEnquiryID",
    "DateId",
    "TravelBrandId",
    "DurationId",
    "PolicyTypeId",
    "DestinationId",
    "FamilyGroupId",
    "MarketingChannelId",
    "AgentId",
    "LeadTimeGroupId",   # NEW
    "MedicalScoreId",   # NEW
    "AgeGroupId",   # ADD THIS
    "IsQuoted",
    "NoOfQuotes",
    "IsSold",
    "TotalTravellers"
)


# --------------------------------------------------------------------------------------
# REPLACE NULLs WITH 0 FOR MarketingChannelId and AgentId
# --------------------------------------------------------------------------------------
fact_agg = fact_agg.withColumn(
    "MarketingChannelId",
    when(col("MarketingChannelId").isNull(), lit(0)).otherwise(col("MarketingChannelId"))
)

fact_agg = fact_agg.withColumn(
    "AgentId",
    when(col("AgentId").isNull(), lit(0)).otherwise(col("AgentId"))
)
fact_agg = fact_agg.withColumn(
    "LeadTimeGroupId",
    when(col("LeadTimeGroupId").isNull(), lit(0)).otherwise(col("LeadTimeGroupId"))
)

fact_agg = fact_agg.withColumn(
    "MedicalScoreId",
    when(col("MedicalScoreId").isNull(), lit(0)).otherwise(col("MedicalScoreId"))
)

fact_agg = fact_agg.withColumn(
    "AgeGroupId",
    when(col("AgeGroupId").isNull(), lit(0)).otherwise(col("AgeGroupId"))
)


fact_agg = fact_agg.select(
    "TravelEnquiryID",
    "DateId",
    "TravelBrandId",
    "DurationId",
    "PolicyTypeId",
    "DestinationId",
    "FamilyGroupId",
    "MarketingChannelId",
    "AgentId",
    "LeadTimeGroupId",   # NEW
    "MedicalScoreId",   # NEW
    "AgeGroupId",   # ADD THIS
    "IsQuoted",
    "NoOfQuotes",
    "IsSold",
    "TotalTravellers"
)

# --------------------------------------------------------------------------------------
# DELETE EXISTING DATA FOR OVERLAPPING DateIds
# --------------------------------------------------------------------------------------
batch_dateids = [r[0] for r in fact_agg.select("DateId").distinct().collect()]

try:
    tgt = DeltaTable.forPath(spark, fact_path)
    if batch_dateids:
        clause = f"DateId IN ({','.join(map(str, [d for d in batch_dateids if d is not None]))})"
        if not clause.endswith("())"):
            tgt.delete(clause)
    write_mode = "append"
except Exception:
    write_mode = "overwrite"

# --------------------------------------------------------------------------------------
# WRITE TO FACT TABLE
# --------------------------------------------------------------------------------------
fact_agg.write.format("delta").mode(write_mode).save(fact_path)

print(f"Wrote FactEnquiries with mode={write_mode} to {fact_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Update FactEnquiries TravelBrandId") \
    .getOrCreate()

# Define the mapping dictionary
brand_agent_mapping = {
    11: [
        "AMT Renewal", "Idol Compare Cover", "Idol Confused.com", 
        "Idol Compare the Market", "CYTI Moneysupermarket Medical",
        "CYTI Medical Travel Compared", "Switched On Insurance Direct",
        "Direct AMT Renewal", "Idol Money", "Moneysupermarket",
        "CYTI Medical Travel Compared Non Med", "Quotezone", 
        "Idol Go Compare", "Idol Uswitch",
        "Moneysupermarket",
        "CYTI Moneysupermarket Medical",
        "CYTI Medical Travel Compared",
        "Switched On Insurance Direct",
        "Idol Compare the Market",
        "Idol Confused.com",
        "AMT Renewal",
        "Direct AMT Renewal",
        "Idol Compare Cover",
        "Idol Go Compare",
        "CYTI Medical Travel Compared Non Med",
        "Idol Uswitch",
        "Idol Money"
    ],
    10: [
        "Start AMT Renewal", "Start Direct AMT Renewal", "Start Compare Cover",
        "Start confused.com", "Start Compare the Market", "Start CYTI MSM Medical",
        "Start Direct", "Start Go Compare", "Start Money", "Start Moneysupermarket",
        "Start Medical Travel Compared", "Start Parent", "Start Quotezone", "Start Uswitch",
        "Start confused.com",
        "Start Uswitch",
        "Start AMT Renewal",
        "Start Moneysupermarket",
        "Start Compare Cover",
        "Start Medical Travel Compared",
        "Start Money",
        "Start Compare the Market",
        "Start CYTI MSM Medical",
        "Start Direct AMT Renewal",
        "Start Go Compare",
        "Start Direct"
    ],
    12: [
        "Trusted IDOL Compare Cover", "Trusted IDOL Confused", 
        "Trusted IDOL Compare the Market", "Trusted Direct",
        "Trusted Direct AMT Renewal", "Trusted IDOL Go Compare",
        "Trusted IDOL Money", "Trusted Moneysupermarket",
        "Trusted CYTI Moneysupermarket Med", "Trusted Medical Travel Compared",
        "Trusted AMT Renewal", "Trusted IDOL Uswitch",
        "Trusted IDOL Money",
        "Trusted Moneysupermarket",
        "Trusted IDOL Confused",
        "Trusted IDOL Go Compare",
        "Trusted IDOL Uswitch",
        "Trusted IDOL Compare Cover",
        "Trusted Direct",
        "Trusted IDOL Compare the Market"
    ],
    13: [
        "Viva IDOL Compare Cover", "Viva IDOL Confused",
        "Viva IDOL Compare the Market", "Viva Direct",
        "Viva AMT Direct Renewal", "Viva IDOL Go Compare",
        "Viva IDOL Money", "Viva Moneysupermarket",
        "Viva CYTI Moneysupermarket Med", "Viva Medical Travel Compared",
        "Viva AMT Renewal", "Viva IDOL Uswitch",
        "Viva IDOL Compare the Market",
        "Viva IDOL Compare Cover",
        "Viva IDOL Uswitch",
        "Viva Direct",
        "Viva Medical Travel Compared",
        "Viva CYTI Moneysupermarket Med",
        "Viva IDOL Go Compare",
        "Viva IDOL Confused",
        "Viva Moneysupermarket",
        "Viva IDOL Money"
    ],
    14: [
        "Oasis Travel - Agg Renewal", "Oasis Travel - CYTI",
        "Oasis Travel - Direct", "Oasis Travel - Direct Renewal",
        "Oasis Travel - IDOL COMPARE COVER", "Oasis Travel - IDOL CON",
        "Oasis Travel - IDOL CTM", "Oasis Travel - IDOL GOC",
        "Oasis Travel - IDOL MONEY", "Oasis Travel - IDOL USWITCH",
        "Oasis Travel - MSM", "Oasis Travel - MTC", "Oasis Travel - Quotezone"
    ],
    8: [
        "AtoZ Direct AMT Renewal", "AtoZ IDOL AMT Renewal",
        "AtoZ Insurance Compare Cover", "AtoZ Insurance Confused.com",
        "AtoZ Insurance CYTI-MSM", "AtoZ Insurance Direct",
        "AtoZ Insurance Go Compare", "AtoZ Insurance IDOL CTM",
        "AtoZ Insurance Money", "AtoZ Insurance MSM",
        "AtoZ Insurance MTC", "AtoZ Insurance Uswitch",
        "AtoZ Insurance MSM CYTI AMT Renewal"
    ]
}

# Read the FactEnquiries table
# Replace 'your_database.FactEnquiries' with your actual table path
fact_enquiries_df = spark.table("TaurusGoldLH.FactEnquiries")

# Read the DimAgent table to get AgentName
dim_agent_df = spark.table("TaurusGoldLH.DimAgent")

# Join FactEnquiries with DimAgent to get AgentName
fact_with_agent = fact_enquiries_df.alias("fact").join(
    dim_agent_df.alias("agent"),
    col("fact.AgentID") == col("agent.AgentID"),
    "left"
)

# Create the update logic using when-otherwise chain
update_condition = None
for brand_id, agent_names in brand_agent_mapping.items():
    for agent_name in agent_names:
        if update_condition is None:
            update_condition = when(col("agent.AgentName") == agent_name, lit(brand_id))
        else:
            update_condition = update_condition.when(col("agent.AgentName") == agent_name, lit(brand_id))

# Apply the update - keep existing TravelBrandId if no match found
updated_with_agent = fact_with_agent.withColumn(
    "TravelBrandId_New",
    update_condition.otherwise(col("fact.TravelBrandId"))
)

# Select only the FactEnquiries columns with updated TravelBrandId
fact_columns = fact_enquiries_df.columns
updated_df = updated_with_agent.select(
    *[col(f"fact.{c}").alias(c) if c != "TravelBrandId" else col("TravelBrandId_New").alias("TravelBrandId") 
      for c in fact_columns]
)

# Show sample of updated records
print("Sample of updated records:")
updated_df.select("AgentID", "TravelBrandId").show(20, truncate=False)

# Show records that were updated (where TravelBrandId changed)
print("\nRecords with updated TravelBrandId:")
comparison_df = fact_enquiries_df.alias("old").join(
    updated_df.alias("new"),
    col("old.AgentID") == col("new.AgentID"),
    "inner"
).join(
    dim_agent_df.alias("agent"),
    col("old.AgentID") == col("agent.AgentID"),
    "left"
).where(
    col("old.TravelBrandId") != col("new.TravelBrandId")
).select(
    col("agent.AgentName"),
    col("old.TravelBrandId").alias("Old_TravelBrandId"),
    col("new.TravelBrandId").alias("New_TravelBrandId")
)
comparison_df.show(50, truncate=False)

# Count records by TravelBrandId to verify
print("\nCount of records by TravelBrandId after update:")
updated_df.groupBy("TravelBrandId").count().orderBy("TravelBrandId").show()

from delta.tables import DeltaTable

# Load the existing Delta table
delta_table = DeltaTable.forName(spark, "TaurusGoldLH.FactEnquiries")

# Merge based on the actual PK column (TravelEnquiryID)
delta_table.alias("target").merge(
    updated_df.alias("source"),
    "target.TravelEnquiryID = source.TravelEnquiryID"
).whenMatchedUpdate(
    set={
        "TravelBrandId": "source.TravelBrandId"
    }
).execute()


# Write back to the table (choose one method below)

# Method 1: Overwrite the entire table
# updated_df.write.mode("overwrite").saveAsTable("TaurusGoldLH.FactEnquiries")

# Method 2: Write to a new table first for verification
# updated_df.write.mode("overwrite").saveAsTable("TaurusGoldLH.FactEnquiries_Updated")

# Method 3: Use Delta Lake merge (if using Delta tables)
# from delta.tables import DeltaTable
# delta_table = DeltaTable.forName(spark, "your_database.FactEnquiries")
# delta_table.alias("target").merge(
#     updated_df.alias("source"),
#     "target.id = source.id"  # Replace 'id' with your primary key column
# ).whenMatchedUpdate(set={"TravelBrandId": "source.TravelBrandId"}).execute()

print("\nUpdate completed successfully!")

# Stop Spark session
# spark.stop()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import trim



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --------------------------------------------------------------------------------------
# DEFINE AGENT LISTS FOR ChannelId
# --------------------------------------------------------------------------------------
channel_1_agents = [
    "Trusted IDOL Compare Cover",
    "Trusted IDOL Confused",
    "Trusted IDOL Compare the Market",
    "Trusted IDOL Go Compare",
    "Trusted IDOL Money",
    "Trusted Moneysupermarket",
    "Trusted CYTI Moneysupermarket Med",
    "Trusted Medical Travel Compared",
    "Trusted IDOL Uswitch"
]

channel_2_agents = [
    "Trusted Direct",
    "Trusted Direct AMT Renewal",
    "Trusted AMT Renewal"
]

channel_3_agents = [
    "Start Compare Cover",
    "Start confused.com",
    "Start Compare the Market",
    "Start CYTI MSM Medical",
    "Start Go Compare",
    "Start Money",
    "Start Moneysupermarket",
    "Start Medical Travel Compared",
    "Start Parent",
    "Start Quotezone",
    "Start Uswitch"
]

channel_4_agents = [
    "Start AMT Renewal",
    "Start Direct AMT Renewal",
    "Start Direct"
]

channel_5_agents = [
    "AtoZ Insurance Compare Cover",
    "AtoZ Insurance Confused.com",
    "AtoZ Insurance CYTI-MSM",
    "AtoZ Insurance Go Compare",
    "AtoZ Insurance IDOL CTM",
    "AtoZ Insurance Money",
    "AtoZ Insurance MSM",
    "AtoZ Insurance MTC",
    "AtoZ Insurance Uswitch"
]

channel_6_agents = [
    "AtoZ Direct AMT Renewal",
    "AtoZ IDOL AMT Renewal",
    "AtoZ Insurance Direct",
    "AtoZ Insurance MSM CYTI AMT Renewal"
]

channel_7_agents = [
    "Oasis Travel - CYTI",
    "Oasis Travel - IDOL COMPARE COVER",
    "Oasis Travel - IDOL CON",
    "Oasis Travel - IDOL CTM",
    "Oasis Travel - IDOL GOC",
    "Oasis Travel - IDOL MONEY",
    "Oasis Travel - IDOL USWITCH",
    "Oasis Travel - MSM",
    "Oasis Travel - MTC",
    "Oasis Travel - Quotezone"
]

channel_8_agents = [
    "Oasis Travel - Agg Renewal",
    "Oasis Travel - Direct",
    "Oasis Travel - Direct Renewal"
]

channel_9_agents = [
    "Viva IDOL Compare Cover",
    "Viva IDOL Confused",
    "Viva IDOL Compare the Market",
    "Viva IDOL Go Compare",
    "Viva IDOL Money",
    "Viva Moneysupermarket",
    "Viva CYTI Moneysupermarket Med",
    "Viva Medical Travel Compared",
    "Viva IDOL Uswitch"
]

channel_10_agents = [
    "Viva Direct",
    "Viva AMT Direct Renewal",
    "Viva AMT Renewal"
]

channel_11_agents = [
    "Idol Compare Cover",
    "Idol Confused.com",
    "Idol Compare the Market",
    "CYTI Moneysupermarket Medical",
    "CYTI Medical Travel Compared",
    "Idol Money",
    "Moneysupermarket",
    "CYTI Medical Travel Compared Non Med",
    "Quotezone",
    "Idol Go Compare",
    "Idol Uswitch"
]

channel_12_agents = [
    "AMT Renewal",
    "Switched On Insurance Direct",
    "Direct AMT Renewal"
]

# --------------------------------------------------------------------------------------
# READ DIMENSIONS AND FACT TABLE
# --------------------------------------------------------------------------------------
# Read and trim AgentName to remove any leading/trailing spaces
dim_agent_df = (
    spark.read.format("delta")
    .load(path_dim_agent)
    .select("AgentId", trim(col("AgentName")).alias("AgentName"))
)

# Combine all agents for verification
all_channel_agents = (
    channel_1_agents + channel_2_agents + channel_3_agents + channel_4_agents +
    channel_5_agents + channel_6_agents + channel_7_agents + channel_8_agents +
    channel_9_agents + channel_10_agents + channel_11_agents + channel_12_agents
)

# Display what agents will be mapped
print("=== Agents that will be mapped to ChannelId ===")
mapped_agents = dim_agent_df.filter(col("AgentName").isin(all_channel_agents))
mapped_agents.show(100, truncate=False)
print(f"Total agents to be mapped: {mapped_agents.count()}")

# --------------------------------------------------------------------------------------
# UPDATE ChannelId IN FactEnquiries
# --------------------------------------------------------------------------------------
delta_table = DeltaTable.forPath(spark, fact_path)

# Create a mapping dataframe with AgentId and ChannelId
agent_channel_mapping = dim_agent_df.withColumn(
    "ChannelId",
    when(col("AgentName").isin(channel_1_agents), lit(1))
    .when(col("AgentName").isin(channel_2_agents), lit(2))
    .when(col("AgentName").isin(channel_3_agents), lit(3))
    .when(col("AgentName").isin(channel_4_agents), lit(4))
    .when(col("AgentName").isin(channel_5_agents), lit(5))
    .when(col("AgentName").isin(channel_6_agents), lit(6))
    .when(col("AgentName").isin(channel_7_agents), lit(7))
    .when(col("AgentName").isin(channel_8_agents), lit(8))
    .when(col("AgentName").isin(channel_9_agents), lit(9))
    .when(col("AgentName").isin(channel_10_agents), lit(10))
    .when(col("AgentName").isin(channel_11_agents), lit(11))
    .when(col("AgentName").isin(channel_12_agents), lit(12))
    .otherwise(lit(None))
).select("AgentId", "ChannelId")

# Filter to only agents that should have a ChannelId
agent_channel_mapping = agent_channel_mapping.filter(col("ChannelId").isNotNull())

print(f"\n=== Agent-Channel Mapping ===")
agent_channel_mapping.orderBy("ChannelId").show(100, truncate=False)
print(f"Total mappings created: {agent_channel_mapping.count()}")

# Check how many fact records will be affected
fact_df = spark.read.format("delta").load(fact_path)
affected_records = fact_df.join(agent_channel_mapping, "AgentId", "inner").count()
print(f"\nRecords that will be updated: {affected_records}")

# Update FactEnquiries using merge
delta_table.alias("fact").merge(
    agent_channel_mapping.alias("mapping"),
    "fact.AgentId = mapping.AgentId"
).whenMatchedUpdate(
    set={"ChannelId": "mapping.ChannelId"}
).execute()

print("\nChannelId updated successfully in FactEnquiries table")

# --------------------------------------------------------------------------------------
# VERIFICATION
# --------------------------------------------------------------------------------------
verification_df = spark.read.format("delta").load(fact_path)

print(f"\n=== VERIFICATION RESULTS ===")
total_count = verification_df.count()
null_count = verification_df.filter(col("ChannelId").isNull()).count()
print(f"Total records: {total_count}")

# Count for each ChannelId
for i in range(1, 13):
    channel_count = verification_df.filter(col("ChannelId") == i).count()
    print(f"Records with ChannelId = {i}: {channel_count}")

print(f"Records with NULL ChannelId: {null_count}")

# Show summary by ChannelId
print("\n=== Summary by ChannelId ===")
verification_df.groupBy("ChannelId").count().orderBy("ChannelId").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
