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

spark.conf.set("spark.sql.caseSensitive", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC SELECT 
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
# MAGIC     SUM(CASE WHEN Type = 'GPW' THEN Jan26 ELSE 0 END) AS GPW,
# MAGIC     SUM(CASE WHEN Type = 'Taurus Gross Retained' THEN Jan26 ELSE 0 END) AS `TaurusGrossRetained`,
# MAGIC     SUM(CASE WHEN Type = 'Taurus Net Retained' THEN Jan26 ELSE 0 END) AS `TaurusNetRetained`,
# MAGIC     SUM(CASE WHEN Type = 'Taurus Retained' THEN Jan26 ELSE 0 END) AS `TaurusRetained`
# MAGIC FROM TravelForecastSummary
# MAGIC WHERE Brand IN ('ViVA','Switched On','Start Travel','Trusted','Oasis','A to Z') 
# MAGIC AND Type IN ('GPW', 'Taurus Gross Retained', 'Taurus Net Retained','Taurus Retained')
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

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC drop table TaurusGoldLH.TravelForecastSummary 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.TravelForecastSummary
# MAGIC (
# MAGIC     Brand STRING,
# MAGIC     Type STRING,
# MAGIC     `2024` STRING,
# MAGIC     `2025` STRING,
# MAGIC     `2026` STRING,
# MAGIC     `2027` STRING,
# MAGIC     Jan25 STRING,
# MAGIC     Feb25 STRING,
# MAGIC     Mar25 STRING,
# MAGIC     Apr25 STRING,
# MAGIC     May25 STRING,
# MAGIC     Jun25 STRING,
# MAGIC     Jul25 STRING,
# MAGIC     Aug25 STRING,
# MAGIC     Sep25 STRING,
# MAGIC     Oct25 STRING,
# MAGIC     Nov25 STRING,
# MAGIC     Dec25 STRING,
# MAGIC     Jan26 STRING,
# MAGIC     Feb26 STRING,
# MAGIC     Mar26 STRING,
# MAGIC     Apr26 STRING,
# MAGIC     May26 STRING,
# MAGIC     Jun26 STRING,
# MAGIC     Jul26 STRING,
# MAGIC     Aug26 STRING,
# MAGIC     Sep26 STRING,
# MAGIC     Oct26 STRING,
# MAGIC     Nov26 STRING,
# MAGIC     Dec26 STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Travel Forecast Summary table'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/TravelForecastSummary';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.DimCampaign
# MAGIC (
# MAGIC     CampaignId INT,
# MAGIC     CampaignName STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimension table: Scheme Campaign'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/DimCampaign';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("DimCampaignSetup").getOrCreate()

# Campaign data (your list)
campaigns = [
    "GOC MED",
    "25TRAVEL2022",
    "AQUAPIGS",
    "AtoZ COMCO MED",
    "AtoZ COMCO NON",
    "AtoZ CON MED",
    "AtoZ CON NON",
    "AtoZ CTM MED",
    "AtoZ CTM NON",
    "AtoZ CYTI-MSM",
    "AtoZ GOCO MED",
    "AtoZ GOCO NON",
    "AtoZ MSM",
    "AtoZ MSM White Label",
    "AtoZ MTC",
    "AtoZ US MED",
    "AtoZ US NON",
    "AtoZ Website",
    "Backpacker",
    "BFCM2022",
    "BFCM2325",
    "BFCM2425",
    "BIRTHDAY15",
    "BP40",
    "CMSAVE10",
    "CMSAVE25",
    "COMPARE COVER MED",
    "COMPARE COVER NON MED",
    "CON MED",
    "CON NON MED",
    "Cornmarket",
    "CREST10",
    "CROWN25",
    "CTM MED",
    "CTM NON MED",
    "CYTI",
    "CYTI Medical Travel Compared",
    "CYTI Moneysupermarket Medical",
    "DIR",
    "DIRECTOR20",
    "EASOI2320",
    "EASOI2420",
    "EASOI2520",
    "EASY10",
    "EXPIRED15",
    "EXPIRED25",
    "EXPLORER",
    "GETAWAY20",
    "GETAWAY25",
    "GIFT10",
    "Gift20",
    "GOC MED",
    "GOC NON MED",
    "HolidayExtras",
    "Idol Compare Cover",
    "Idol Compare Cover Medical",
    "Idol Compare the Market",
    "Idol Compare the Market Medical",
    "Idol Confused.com",
    "Idol Confused.com Medical",
    "Idol Go Compare",
    "Idol Go Compare Medical",
    "Idol Money",
    "Idol Money Medical",
    "Idol Uswitch",
    "Idol Uswitch Medical",
    "LECTURER10",
    "MATESRATES10",
    "MATESRATES15",
    "MONEY MED",
    "MONEY NON MED",
    "MONEYFACTS10",
    "Moneysupermarket",
    "MSM",
    "MTC",
    "MTC MED",
    "MTC NON MED",
    "NHS25",
    "Oasis Travel - Direct",
    "OFFER25",
    "PASSPORT10",
    "QZSOTPEM22",
    "RENEW10",
    "RENEW20",
    "RENEW25",
    "RENEWAL10",
    "RENEWAL15",
    "RENEWAL20",
    "RENEWAL25",
    "REPEAT10",
    "RESCUE10",
    "RESCUE25",
    "RTN20",
    "SAFNVC2420",
    "SAFVC2420",
    "SALE10",
    "SAVE10",
    "SAVE20",
    "SAVE25",
    "SIA510",
    "SIAFVC515",
    "SPECIAL25",
    "SRB819F",
    "SRC882N",
    "STAFF30",
    "Start Compare Cover",
    "Start Compare Cover Medical",
    "Start Compare the Market",
    "Start Compare the Market Medical",
    "Start confused.com",
    "Start confused.com Medical",
    "Start Direct",
    "Start Go Compare",
    "Start Go Compare Medical",
    "Start Medical Travel Compared",
    "Start Money",
    "Start Money Medical",
    "Start Moneysupermarket",
    "Start MSM Medical",
    "Start Uswitch",
    "Start Uswitch Money",
    "STAY15",
    "SUMMER23",
    "SUPER25",
    "TAFCB24E",
    "TAFCB24S",
    "TAFS2420",
    "TCSTAFF-AMT",
    "THANKS20",
    "Thomas Cook Direct",
    "Thomas Cook Integrated",
    "TRAVEL20",
    "Trusted CYTI Moneysupermarket Med",
    "Trusted Direct",
    "Trusted IDOL Compare Cover",
    "Trusted IDOL Compare Cover Med",
    "Trusted IDOL Compare the Market",
    "Trusted IDOL Compare the Market Med",
    "Trusted IDOL Confused",
    "Trusted IDOL Confused Med",
    "Trusted IDOL Go Compare",
    "Trusted IDOL Go Compare Med",
    "Trusted IDOL Money",
    "Trusted IDOL Money Med",
    "Trusted IDOL Uswitch",
    "Trusted IDOL Uswitch Med",
    "Trusted Medical Travel Compared",
    "Trusted Medical Travel Compared Non Med",
    "Trusted Moneysupermarket",
    "USWITCH MED",
    "USWITCH NON MED",
    "Vibe Insurance",
    "Vibe Insurance CYTI/MSM",
    "Vibe Insurance CYTI/MSM White Label",
    "Vibe Insurance MTC",
    "Viva CYTI Moneysupermarket Med",
    "Viva Direct",
    "Viva IDOL Compare Cover",
    "Viva IDOL Compare Cover Med",
    "Viva IDOL Compare the Market",
    "Viva IDOL Compare the Market Med",
    "Viva IDOL Confused",
    "Viva IDOL Confused Med",
    "Viva IDOL Go Compare",
    "Viva IDOL Go Compare Med",
    "Viva IDOL Money",
    "Viva IDOL Money Med",
    "Viva IDOL Uswitch",
    "Viva IDOL Uswitch Med",
    "Viva Medical Travel Compared",
    "Viva Medical Travel Compared Non Med",
    "Viva Moneysupermarket",
    "Welcome10",
    "WELCOME15",
    "WSNY23",
    "WSNY2420",
    "WSNY2520",
    "XMNY21"
]

# Create DataFrame from campaign list
campaign_df = spark.createDataFrame([(c,) for c in campaigns], ["CampaignName"])

# Add sequential IDs (starting at 1)
window = Window.orderBy("CampaignName")
campaign_df = campaign_df.withColumn("CampaignId", row_number().over(window).cast(IntegerType()))

# Add the Unknown record (ID = 0, enforced IntegerType)
unknown_df = spark.createDataFrame([(0, "Unknown")], ["CampaignId", "CampaignName"]) \
                  .withColumn("CampaignId", col("CampaignId").cast(IntegerType()))

# Union Unknown + real campaigns
final_df = unknown_df.unionByName(campaign_df.select("CampaignId", "CampaignName"))

# Order by CampaignId
final_df = final_df.orderBy("CampaignId")

final_df.show(10, truncate=False)

# Write to Delta table
final_df.write.mode("overwrite").format("delta").saveAsTable("TaurusGoldLH.DimCampaign")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.DimAgent
# MAGIC (
# MAGIC     AgentId INT,
# MAGIC     AgentName STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimension table: Scheme Agent'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/DimAgent';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("DimAgentSetup").getOrCreate()

# Define schema
schema = StructType([
    StructField("AgentId", IntegerType(), False),
    StructField("AgentName", StringType(), False)
])

# Data with Unknown as 0
data = [
    (0, "Unknown"),
    (1, "Oasis Travel - IDOL GOC"),
    (2, "Oasis Travel - IDOL COMPARE COVER"),
    (3, "Oasis Travel - MTC"),
    (4, "Oasis Travel - IDOL CON"),
    (5, "Oasis Travel - Direct Renewal"),
    (6, "Oasis Travel - IDOL MONEY"),
    (7, "Oasis Travel - IDOL CTM"),
    (8, "Oasis Travel - Direct"),
    (9, "Oasis Travel - MSM"),
    (10, "Oasis Travel - IDOL USWITCH"),
    (11, "Oasis Travel - CYTI"),
    (12, "Oasis Travel - Agg Renewal"),
    (13, "Trusted Moneysupermarket"),
    (14, "Moneysupermarket"),
    (15, "Trusted IDOL Go Compare"),
    (16, "Trusted IDOL Compare the Market"),
    (17, "Viva CYTI Moneysupermarket Med"),
    (18, "Viva IDOL Confused"),
    (19, "Trusted IDOL Compare Cover"),
    (20, "Viva IDOL Compare the Market"),
    (21, "Start Direct"),
    (22, "Viva Medical Travel Compared"),
    (23, "Viva IDOL Compare Cover"),
    (24, "Viva IDOL Uswitch"),
    (25, "Idol Compare Cover"),
    (26, "Idol Confused.com"),
    (27, "Start Moneysupermarket"),
    (28, "Start Go Compare"),
    (29, "Start Money"),
    (30, "Start confused.com"),
    (31, "Viva IDOL Go Compare"),
    (32, "Idol Compare the Market"),
    (33, "Idol Go Compare"),
    (34, "Trusted CYTI Moneysupermarket Med"),
    (35, "Idol Money"),
    (36, "Start CYTI MSM Medical"),
    (37, "AMT Renewal"),
    (38, "CYTI Medical Travel Compared"),
    (39, "Start Direct AMT Renewal"),
    (40, "Thomas Cook"),
    (41, "Viva Moneysupermarket"),
    (42, "Direct AMT Renewal"),
    (43, "Trusted Direct"),
    (44, "Start Compare Cover"),
    (45, "Trusted IDOL Confused"),
    (46, "Switched On Insurance Direct"),
    (47, "Start Compare the Market"),
    (48, "Start Medical Travel Compared"),
    (49, "Idol Uswitch"),
    (50, "Start Uswitch"),
    (51, "Trusted Medical Travel Compared"),
    (52, "Trusted IDOL Uswitch"),
    (53, "Trusted IDOL Money"),
    (54, "Viva Direct"),
    (55, "CYTI Moneysupermarket Medical"),
    (56, "Viva IDOL Money"),
    (57, "Start AMT Renewal"),
    (58, "Vibe Insurance Direct"),
    (59, "AtoZ Insurance Go Compare"),
    (60, "AtoZ Insurance MTC"),
    (61, "Vibe Insurance CYTI/MSM"),
    (62, "AtoZ Insurance Direct"),
    (63, "AtoZ Insurance Confused.com"),
    (64, "AtoZ Insurance IDOL CTM"),
    (65, "AtoZ Insurance Uswitch"),
    (66, "AtoZ Insurance CYTI-MSM"),
    (67, "AtoZ Direct AMT Renewal"),
    (68, "Vibe Insurance MTC"),
    (69, "AtoZ IDOL AMT Renewal"),
    (70, "AtoZ Insurance MSM"),
    (71, "AtoZ MSM CYTI AMT Renewal"),
    (72, "AtoZ Insurance Compare Cover"),
    (73, "Ireland Switched On Insurance Direct"),
    (74, "Switched On TEST"),
    (75, "Trusted AMT Renewal"),
    (76, "CYTI Medical Travel Compared Non Med"),
    (77, "Idol Holiday Ready"),
    (78, "Paying Too Much"),
    (79, "Viva IDOL Holiday Ready"),
    (80, "Viva Paying Too Much"),
    (81, "Viva AMT Renewal"),
    (82, "Viva AMT Direct Renewal"),
    (83, "Trusted IDOL Holiday Ready"),
    (84, "Trusted Direct AMT Renewal")





]

# Create DataFrame
agent_df = spark.createDataFrame(data, schema)

# Preview
agent_df.orderBy("AgentId").show(truncate=False)

# Write to Delta table
agent_df.write.mode("overwrite").saveAsTable("TaurusGoldLH.DimAgent")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.DimSchemeHeaders
# MAGIC (
# MAGIC     SchemeHeaderId INT,
# MAGIC     FriendlyName STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimension table: Scheme Headers'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/DimSchemeHeaders';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.DimLeadTimeGroup
# MAGIC (
# MAGIC     LeadTimeGroupId INT,
# MAGIC     LeadTimeGroup STRING,
# MAGIC     SingleTrip DECIMAL(5,2),
# MAGIC     AMT DECIMAL(5,2)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimension table: Lead Time Group'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/DimLeadTimeGroup';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from decimal import Decimal  # Import Decimal for precise decimal values

# Initialize Spark session
spark = SparkSession.builder.appName("DimLeadTimeGroupSetup").getOrCreate()

# Define schema with appropriate precision and scale
schema = StructType([
    StructField("LeadTimeGroupId", IntegerType(), False),
    StructField("LeadTimeGroup", StringType(), False),
    StructField("SingleTrip", DecimalType(5, 2), False),  # e.g., 15.50%
    StructField("AMT", DecimalType(5, 2), False)  # e.g., 15.50%
])

# Define data using Decimal for numeric values
lead_time_groups = [
    (1, "Invalid (<0)", Decimal("0.00"), Decimal("0.00")),
    (2, "0 to 3", Decimal("9.00"), Decimal("0.00")),
    (3, "4 to 8", Decimal("31.00"), Decimal("0.00")),
    (4, "9 to 15", Decimal("34.00"), Decimal("0.00")),
    (5, "16 to 30", Decimal("15.00"), Decimal("0.00")),
    (6, "31 to 60", Decimal("8.00"), Decimal("0.00")),
    (7, "61+", Decimal("3.00"), Decimal("0.00")),
    (8, "Invalid Dates", Decimal("0.00"), Decimal("0.00"))
]

# Create DataFrame
dim_lead_time_group_df = spark.createDataFrame(lead_time_groups, schema)

# Save as table
dim_lead_time_group_df.orderBy("LeadTimeGroupId").write.mode("overwrite").saveAsTable("DimLeadTimeGroup")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC drop table TaurusGoldLH.DimMedicalScore

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.DimMedicalScore
# MAGIC (
# MAGIC     MedicalScoreId INT,
# MAGIC     MedicalScore STRING,
# MAGIC     SingleTrip DECIMAL(5,2),
# MAGIC     AMT DECIMAL(5,2)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimension table: Medical Score'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/DimMedicalScore';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from decimal import Decimal  # Required for DecimalType values

# Initialize Spark session
spark = SparkSession.builder.appName("DimMedicalScoreSetup").getOrCreate()

# Define schema
schema = StructType([
    StructField("MedicalScoreId", IntegerType(), False),
    StructField("MedicalScore", StringType(), False),
    StructField("SingleTrip", DecimalType(5, 2), False),
    StructField("AMT", DecimalType(5, 2), False)
])

# Define data
medical_score_groups = [
    (1, "0 to 0.99", Decimal("50.00"), Decimal("50.00")),
    (2, "1.00", Decimal("9.00"), Decimal("6.50")),
    (3, "1.01 to 1.5", Decimal("20.00"), Decimal("22.50")),
    (4, "1.51 to 2.5", Decimal("11.00"), Decimal("12.00")),
    (5, "2.51 to 6.0", Decimal("7.00"), Decimal("7.00")),
    (6, "6.01 +", Decimal("3.00"), Decimal("2.00")),
]

# Create DataFrame
dim_medical_score_df = spark.createDataFrame(medical_score_groups, schema)

# Show the DataFrame sorted by ID
dim_medical_score_df.orderBy("MedicalScoreId").show()

# Optionally: Save to Hive table or external storage
dim_medical_score_df.orderBy("MedicalScoreId").write.mode("overwrite").saveAsTable("DimMedicalScore")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.DimDuration
# MAGIC (
# MAGIC     DurationId INT,
# MAGIC     Duration STRING,
# MAGIC     SingleTrip DECIMAL(5,2),
# MAGIC     AMT DECIMAL(5,2)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimension table: Duratione'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/DimDuration';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from decimal import Decimal  # Required for DecimalType values

# Initialize Spark session
spark = SparkSession.builder.appName("DimDurationSetup").getOrCreate()

# Define schema with decimal precision
schema = StructType([
    StructField("DurationId", IntegerType(), False),
    StructField("Duration", StringType(), False),
    StructField("SingleTrip", DecimalType(5, 2), False),
    StructField("AMT", DecimalType(5, 2), False)
])

# Define data using Decimal for numeric values
duration_groups = [
    (1, "1 to 3", Decimal("8.00"), Decimal("0.00")),
    (2, "4 to 5", Decimal("31.00"), Decimal("0.00")),
    (3, "6 to 10", Decimal("34.00"), Decimal("0.00")),
    (4, "11 to 17", Decimal("15.00"), Decimal("0.00")),
    (5, "18 to 24", Decimal("8.00"), Decimal("0.00")),
    (6, "25 to 31", Decimal("3.00"), Decimal("0.00")),
    (7, "32 +", Decimal("1.00"), Decimal("0.00")),
    (8, "Invalid Duration", Decimal("0.00"), Decimal("0.00"))
]

# Create DataFrame
dim_duration_df = spark.createDataFrame(duration_groups, schema)

# Show the DataFrame sorted by ID
dim_duration_df.orderBy("DurationId").show()

# Save to Hive table or external storage
dim_duration_df.orderBy("DurationId").write.mode("overwrite").saveAsTable("DimDuration")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.DimPolicyType
# MAGIC (
# MAGIC     PolicyTypeId INT,
# MAGIC     PolicyType STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimension table: Policy Type'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/DimPolicyType';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("DimPolicyTypeSetup").getOrCreate()

# Define schema
schema = StructType([
    StructField("PolicyTypeId", IntegerType(), False),
    StructField("PolicyType", StringType(), False)
])


data = [
    (1, "Annual"),
    (2, "Single")
]

# Create DataFrame
policy_type_df = spark.createDataFrame(data, schema)

policy_type_df.orderBy("PolicyTypeId").show()


# Optionally: Save to Hive table or external storage
policy_type_df.orderBy("PolicyTypeId").write.mode("overwrite").saveAsTable("DimPolicyType")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC drop table TaurusGoldLH.FactSalesAnalysis 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE TaurusGoldLH.FactSalesAnalysis (
# MAGIC     TravelSalesTransactionId INT,
# MAGIC     PolicyNumber STRING,
# MAGIC     DimSchemeHeaderId INT,
# MAGIC     TravelBrandId INT,
# MAGIC     DurationId INT,
# MAGIC     PolicyTypeId INT,
# MAGIC     ChannelId INT,
# MAGIC     OptionId INT,
# MAGIC     DestinationId INT,
# MAGIC     FamilyGroupId INT,
# MAGIC     AgeGroupId INT,
# MAGIC     FinalCountryId INT,
# MAGIC     DateID BIGINT,
# MAGIC     MedicalScoreId INT,
# MAGIC     LeadTimeGroupId INT,
# MAGIC     MarketingChannelId INT,
# MAGIC     AgentId INT, 
# MAGIC     CampaignId INT,
# MAGIC     TransactionType string, 
# MAGIC     PolicyStatus string ,
# MAGIC     TotalGrossIncIPT DECIMAL(18, 2),
# MAGIC     TotalGrossExcIPT DECIMAL(18, 2),
# MAGIC     TotalNetToUnderwriter DECIMAL(18, 2),
# MAGIC     RecordCount BIGINT,
# MAGIC     SourceType String
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactSalesAnalysis';
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.DimSubChannel
# MAGIC (
# MAGIC     SubChannelId INT,
# MAGIC     SubChannel STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimension table: Channel'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/DimSubChannel';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC INSERT INTO DimSubChannel (SubChannelId, SchemeName, Channel, GenericCoverLevel)
# MAGIC VALUES
# MAGIC 
# MAGIC (0,'Unknown'),
# MAGIC (1,'Trusted Agg'),
# MAGIC (2,'Trusted Direct'),
# MAGIC (3,'Start Agg'),
# MAGIC (4,'Start Direct'),
# MAGIC (5,'Atoz Agg'),
# MAGIC (6,'Atoz Direct'),
# MAGIC (7,'Oasis Agg'),
# MAGIC (8,'Oasis Direct'),
# MAGIC (9,'Viva Agg'),
# MAGIC (10,'Viva Direct'),
# MAGIC (11,'SOI Agg'),
# MAGIC (12,'SOI Direct')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.DimChannel
# MAGIC (
# MAGIC     ChannelId INT,
# MAGIC     SchemeName STRING,
# MAGIC     Channel STRING,
# MAGIC     GenericCoverLevel STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimension table: Channel'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/DimChannel';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC INSERT INTO DimChannel (ChannelId, SchemeName, Channel, GenericCoverLevel)
# MAGIC VALUES
# MAGIC -- Oasis
# MAGIC (0,'Unknown','Unknown','Unknown'),
# MAGIC (1,'MSM Backpacker Silver','Oasis Agg','Two'),
# MAGIC (2,'CYTI Backpacker Gold','Oasis Agg','Three'),
# MAGIC (3,'IDOL NON MED Single Elite','Oasis Agg','Three'),
# MAGIC (4,'IDOL MED Backpacker Silver','Oasis Agg','Two'),
# MAGIC (5,'CYTI Single Elite','Oasis Agg','Three'),
# MAGIC (6,'MSM AMT Bronze','Oasis Agg','One'),
# MAGIC (7,'IDOL NON MED Single Gold','Oasis Agg','Three'),
# MAGIC (8,'MSM AMT Premium','Oasis Agg','Two'),
# MAGIC (9,'MTC MED Single Gold','Oasis Agg','Three'),
# MAGIC (10,'MTC NON MED AMT Silver','Oasis Agg','Two'),
# MAGIC (11,'IDOL NON MED Backpacker Bronze','Oasis Agg','One'),
# MAGIC (12,'IDOL NON MED Single Premium','Oasis Agg','Two'),
# MAGIC (13,'MSM Single Premium','Oasis Agg','Two'),
# MAGIC (14,'MSM Backpacker Gold','Oasis Agg','Three'),
# MAGIC (15,'IDOL MED Single Silver','Oasis Agg','Two'),
# MAGIC (16,'CYTI AMT Gold','Oasis Agg','Three'),
# MAGIC (17,'CYTI Single Premium','Oasis Agg','Two'),
# MAGIC (18,'MTC MED Single Bronze','Oasis Agg','One'),
# MAGIC (19,'CYTI AMT Silver','Oasis Agg','Two'),
# MAGIC (20,'MTC MED AMT Silver','Oasis Agg','Two'),
# MAGIC (21,'IDOL MED Backpacker Bronze','Oasis Agg','One'),
# MAGIC (22,'CYTI Backpacker Silver','Oasis Agg','Two'),
# MAGIC (23,'MSM Backpacker Bronze','Oasis Agg','One'),
# MAGIC (24,'IDOL NON MED Single Classic','Oasis Agg','One'),
# MAGIC (25,'MTC NON MED AMT Bronze','Oasis Agg','One'),
# MAGIC (26,'MSM Single Silver','Oasis Agg','Two'),
# MAGIC (27,'IDOL MED AMT Classic','Oasis Agg','One'),
# MAGIC (28,'MTC MED AMT Bronze','Oasis Agg','One'),
# MAGIC (29,'IDOL MED AMT Elite','Oasis Agg','Three'),
# MAGIC (30,'CYTI AMT Bronze','Oasis Agg','One'),
# MAGIC (31,'IDOL NON MED Backpacker Gold','Oasis Agg','Three'),
# MAGIC (32,'IDOL NON MED AMT Silver','Oasis Agg','Two'),
# MAGIC (33,'IDOL MED Single Elite','Oasis Agg','Three'),
# MAGIC (34,'MSM Single Gold','Oasis Agg','Three'),
# MAGIC (35,'MSM AMT Gold','Oasis Agg','Three'),
# MAGIC (36,'MSM Single Elite','Oasis Agg','Three'),
# MAGIC (37,'IDOL NON MED Single Bronze','Oasis Agg','One'),
# MAGIC (38,'CYTI Single Classic','Oasis Agg','One'),
# MAGIC (39,'IDOL MED Backpacker Gold','Oasis Agg','Three'),
# MAGIC (40,'IDOL MED Single Gold','Oasis Agg','Three'),
# MAGIC (41,'IDOL NON MED AMT Gold','Oasis Agg','Three'),
# MAGIC (42,'MTC NON MED Single Bronze','Oasis Agg','One'),
# MAGIC (43,'IDOL NON MED AMT Premium','Oasis Agg','Two'),
# MAGIC (44,'MSM Single Bronze','Oasis Agg','One'),
# MAGIC (45,'MSM AMT Classic','Oasis Agg','One'),
# MAGIC (46,'CYTI Single Silver','Oasis Agg','Two'),
# MAGIC (47,'IDOL MED Single Classic','Oasis Agg','One'),
# MAGIC (48,'CYTI AMT Premium','Oasis Agg','Two'),
# MAGIC (49,'MTC MED AMT Gold','Oasis Agg','Three'),
# MAGIC (50,'MSM AMT Elite','Oasis Agg','Three'),
# MAGIC (51,'MTC NON MED AMT Gold','Oasis Agg','Three'),
# MAGIC (52,'CYTI AMT Classic','Oasis Agg','One'),
# MAGIC (53,'IDOL MED AMT Premium','Oasis Agg','Two'),
# MAGIC (54,'IDOL MED Single Bronze','Oasis Agg','One'),
# MAGIC (55,'MSM AMT Silver','Oasis Agg','Two'),
# MAGIC (56,'IDOL MED AMT Silver','Oasis Agg','Two'),
# MAGIC (57,'MTC NON MED Single Silver','Oasis Agg','Two'),
# MAGIC (58,'CYTI Single Bronze','Oasis Agg','One'),
# MAGIC (59,'MSM Single Classic','Oasis Agg','One'),
# MAGIC (60,'IDOL NON MED Single Silver','Oasis Agg','Two'),
# MAGIC (61,'IDOL NON MED Backpacker Silver','Oasis Agg','Two'),
# MAGIC (62,'IDOL MED Single Premium','Oasis Agg','Two'),
# MAGIC (63,'MTC NON MED Single Gold','Oasis Agg','Three'),
# MAGIC (64,'CYTI Single Gold','Oasis Agg','Three'),
# MAGIC (65,'IDOL MED AMT Bronze','Oasis Agg','One'),
# MAGIC (66,'IDOL NON MED AMT Bronze','Oasis Agg','One'),
# MAGIC (67,'CYTI Backpacker Bronze','Oasis Agg','One'),
# MAGIC (68,'IDOL MED AMT Gold','Oasis Agg','Three'),
# MAGIC (69,'IDOL NON MED AMT Elite','Oasis Agg','Three'),
# MAGIC (70,'MTC MED Single Silver','Oasis Agg','Two'),
# MAGIC (71,'Direct AMT Emerald','Oasis Direct','One'),
# MAGIC (72,'Agg Renewal AMT Bronze','Oasis Direct','One'),
# MAGIC (73,'Direct Single Gold','Oasis Direct','Three'),
# MAGIC (74,'Direct Single Diamond','Oasis Direct','Three'),
# MAGIC (75,'Direct Backpackers Gold','Oasis Direct','Three'),
# MAGIC (76,'Direct AMT Ruby','Oasis Direct','Two'),
# MAGIC (77,'Direct AMT Bronze','Oasis Direct','One'),
# MAGIC (78,'Direct AMT Gold','Oasis Direct','Three'),
# MAGIC (79,'Direct Renewal AMT Diamond','Oasis Direct','Three'),
# MAGIC (80,'Direct Single Emerald','Oasis Direct','One'),
# MAGIC (81,'Direct AMT Silver','Oasis Direct','Two'),
# MAGIC (82,'Direct Single Ruby','Oasis Direct','Two'),
# MAGIC (83,'Agg Renewal AMT Gold','Oasis Direct','Three'),
# MAGIC (84,'Direct Backpackers Silver','Oasis Direct','Two'),
# MAGIC (85,'Direct Renewal AMT Emerald','Oasis Direct','One'),
# MAGIC (86,'Agg Renewal AMT Silver','Oasis Direct','Two'),
# MAGIC (87,'Direct Renewal AMT Ruby','Oasis Direct','Two'),
# MAGIC (88,'Direct Single Silver','Oasis Direct','Two'),
# MAGIC (89,'Direct AMT Diamond','Oasis Direct','Three'),
# MAGIC (90,'Direct Backpackers Bronze','Oasis Direct','One'),
# MAGIC (91,'Direct Single Bronze','Oasis Direct','One'),
# MAGIC (92,'AtoZ Silver Backpacker CYTI','AtoZ Agg','One'),
# MAGIC (93,'AtoZ Gold ST CYTI','AtoZ Agg','Three'),
# MAGIC (94,'AtoZ Silver Backpacker IDOL.','AtoZ Agg','One'),
# MAGIC (95,'AtoZ Silver Backpacker MSM','AtoZ Agg','One'),
# MAGIC (96,'AtoZ Gold AMT IDOL.','AtoZ Agg','Three'),
# MAGIC (97,'AtoZ Gold AMT CYTI','AtoZ Agg','Three'),
# MAGIC (98,'AtoZ Silver ST IDOL','AtoZ Agg','Two'),
# MAGIC (99,'AtoZ Gold ST MSM','AtoZ Agg','Three'),
# MAGIC (100,'AtoZ Bronze AMT IDOL.','AtoZ Agg','One'),
# MAGIC (101,'AtoZ Gold AMT MSM','AtoZ Agg','Three'),
# MAGIC (102,'AtoZ Silver Backpacker IDOL','AtoZ Agg','One'),
# MAGIC (103,'AtoZ Bronze AMT IDOL','AtoZ Agg','One'),
# MAGIC (104,'AtoZ Silver AMT CYTI','AtoZ Agg','Two'),
# MAGIC (105,'AtoZ Gold Backpacker IDOL.','AtoZ Agg','Two'),
# MAGIC (106,'AtoZ Bronze AMT MSM','AtoZ Agg','One'),
# MAGIC (107,'AtoZ Silver ST IDOL.','AtoZ Agg','Two'),
# MAGIC (108,'AtoZ Silver ST CYTI','AtoZ Agg','Two'),
# MAGIC (109,'AtoZ Silver AMT IDOL.','AtoZ Agg','Two'),
# MAGIC (110,'AtoZ Bronze ST IDOL.','AtoZ Agg','One'),
# MAGIC (111,'AtoZ Bronze AMT CYTI','AtoZ Agg','One'),
# MAGIC (112,'AtoZ Gold AMT IDOL','AtoZ Agg','Three'),
# MAGIC (113,'AtoZ Silver AMT MSM','AtoZ Agg','Two'),
# MAGIC (114,'AtoZ Silver ST MSM','AtoZ Agg','Two'),
# MAGIC (115,'AtoZ Bronze ST IDOL','AtoZ Agg','One'),
# MAGIC (116,'AtoZ Bronze ST CYTI','AtoZ Agg','One'),
# MAGIC (117,'AtoZ Gold Backpacker IDOL','AtoZ Agg','Two'),
# MAGIC (118,'AtoZ Gold ST IDOL','AtoZ Agg','Three'),
# MAGIC (119,'AtoZ Silver AMT IDOL','AtoZ Agg','Two'),
# MAGIC (120,'AtoZ Gold ST IDOL.','AtoZ Agg','Three'),
# MAGIC (121,'AtoZ Bronze ST MSM','AtoZ Agg','One'),
# MAGIC (122,'Direct AtoZ Essential ST','AtoZ Direct','One'),
# MAGIC (123,'MSM CYTI Renewal AMT Silver','AtoZ Direct','Two'),
# MAGIC (124,'Direct AtoZ Premium ST','AtoZ Direct','Three'),
# MAGIC (125,'AtoZ Renewal Gold IDOL MED AMT','AtoZ Direct','Three'),
# MAGIC (126,'Direct  AtoZ Standard ST','AtoZ Direct','Two'),
# MAGIC (127,'Direct AtoZ Essential AMT','AtoZ Direct','One'),
# MAGIC (128,'AtoZ Renewal Direct AMT Standard','AtoZ Direct','Two'),
# MAGIC (129,'AtoZ Renewal Silver IDOL MED AMT','AtoZ Direct','Two'),
# MAGIC (130,'MSM CYTI Renewal AMT Gold','AtoZ Direct','Three'),
# MAGIC (131,'Direct AtoZ Premium AMT','AtoZ Direct','Three'),
# MAGIC (132,'AtoZ Renewal Direct AMT Essential','AtoZ Direct','One'),
# MAGIC (133,'Direct AtoZ Standard AMT','AtoZ Direct','Two'),
# MAGIC (134,'MSM CYTI Renewal AMT Bronze','AtoZ Direct','One'),
# MAGIC (135,'Viva IDOL AMT Platinum','Viva Agg','Three'),
# MAGIC (136,'Viva MSM AMT Platinum','Viva Agg','Three'),
# MAGIC (137,'Viva IDOL AMT Silver','Viva Agg','One'),
# MAGIC (138,'Viva CYTI AMT Silver','Viva Agg','One'),
# MAGIC (139,'Viva MSM AMT Silver','Viva Agg','One'),
# MAGIC (140,'Viva CYTI AMT Platinum','Viva Agg','Three'),
# MAGIC (141,'Viva IDOL Single Platinum','Viva Agg','Three'),
# MAGIC (142,'Viva CYTI Single Gold','Viva Agg','Two'),
# MAGIC (143,'Viva IDOL Single Silver','Viva Agg','One'),
# MAGIC (144,'Viva IDOL Single Gold','Viva Agg','Two'),
# MAGIC (145,'Viva MSM Single Platinum','Viva Agg','Three'),
# MAGIC (146,'Viva MSM AMT Gold','Viva Agg','Two'),
# MAGIC (147,'Viva CYTI Single Silver','Viva Agg','One'),
# MAGIC (148,'Viva CYTI Single Platinum','Viva Agg','Three'),
# MAGIC (149,'Viva MSM Single Gold','Viva Agg','Two'),
# MAGIC (150,'Viva IDOL AMT Gold','Viva Agg','Two'),
# MAGIC (151,'Viva CYTI AMT Gold','Viva Agg','Two'),
# MAGIC (152,'Viva MSM Single Silver','Viva Agg','One'),
# MAGIC (153,'Viva Direct AMT Gold','Viva Direct','Two'),
# MAGIC (154,'Viva Direct Single Platinum','Viva Direct','Three'),
# MAGIC (155,'Viva Direct Single Gold','Viva Direct','Two'),
# MAGIC (156,'Viva Direct AMT Silver','Viva Direct','One'),
# MAGIC (157,'Viva Direct AMT Platinum','Viva Direct','Three'),
# MAGIC (158,'Viva Direct Single Silver','Viva Direct','One'),
# MAGIC (159,'Trusted MSM Single Ultimate','Trusted Agg','Three'),
# MAGIC (160,'Trusted IDOL AMT Ultimate','Trusted Agg','Three'),
# MAGIC (161,'Trusted MSM Single Essential','Trusted Agg','One'),
# MAGIC (162,'Trusted MSM AMT Essential','Trusted Agg','One'),
# MAGIC (163,'Trusted IDOL Single Ultimate','Trusted Agg','Three'),
# MAGIC (164,'Trusted MSM Single Classic','Trusted Agg','Two'),
# MAGIC (165,'Trusted MSM AMT Classic','Trusted Agg','Two'),
# MAGIC (166,'Trusted MSM AMT Ultimate','Trusted Agg','Three'),
# MAGIC (167,'Trusted IDOL Single Essential','Trusted Agg','One'),
# MAGIC (168,'Trusted IDOL Single Classic','Trusted Agg','Two'),
# MAGIC (169,'Trusted IDOL AMT Classic','Trusted Agg','Two'),
# MAGIC (170,'Trusted IDOL AMT Essential','Trusted Agg','One'),
# MAGIC (171,'Trusted Direct AMT Emerald','Trusted Direct','One'),
# MAGIC (172,'Trusted Direct Single Emerald','Trusted Direct','One'),
# MAGIC (173,'Trusted Direct Single Ruby','Trusted Direct','Two'),
# MAGIC (174,'Trusted Direct AMT Diamond','Trusted Direct','Three'),
# MAGIC (175,'IDOL Backpacker Ultimate','SOI Agg','Three'),
# MAGIC (176,'CYTI Backpacker Ultimate','SOI Agg','Three'),
# MAGIC (177,'IDOL Backpacker Standard','SOI Agg','One'),
# MAGIC (178,'CYTI AMT Three','SOI Agg','Three'),
# MAGIC (179,'IDOL AMT Two','SOI Agg','Two'),
# MAGIC (180,'CYTI Backpacker Standard','SOI Agg','One'),
# MAGIC (181,'CYTI Backpacker Premium','SOI Agg','Two'),
# MAGIC (182,'MSM Backpacker Standard','SOI Agg','One'),
# MAGIC (183,'MSM Single Three','SOI Agg','Three'),
# MAGIC (184,'IDOL Single One','SOI Agg','One'),
# MAGIC (185,'IDOL AMT One','SOI Agg','One'),
# MAGIC (186,'MSM AMT Three','SOI Agg','Three'),
# MAGIC (187,'MSM AMT Two','SOI Agg','Two'),
# MAGIC (188,'IDOL Single Two','SOI Agg','Two'),
# MAGIC (189,'MSM Backpacker Premium','SOI Agg','Two'),
# MAGIC (190,'MSM Single One','SOI Agg','One'),
# MAGIC (191,'CYTI Single One','SOI Agg','One'),
# MAGIC (192,'CYTI AMT One','SOI Agg','One'),
# MAGIC (193,'MSM AMT One','SOI Agg','One'),
# MAGIC (194,'IDOL AMT Three','SOI Agg','Three'),
# MAGIC (195,'CYTI AMT Two','SOI Agg','Two'),
# MAGIC (196,'MSM Backpacker Ultimate','SOI Agg','Three'),
# MAGIC (197,'IDOL Backpacker Premium','SOI Agg','Two'),
# MAGIC (198,'IDOL Single Three','SOI Agg','Three'),
# MAGIC (199,'MSM Single Two','SOI Agg','Two'),
# MAGIC (200,'CYTI Single Two','SOI Agg','Two'),
# MAGIC (201,'CYTI Single Three','SOI Agg','Three'),
# MAGIC (202,'Direct Backpacker Premium','SOI Direct','Two'),
# MAGIC (203,'PCW Renewal One','SOI Direct','One'),
# MAGIC (204,'Direct Backpacker Ultimate','SOI Direct','Three'),
# MAGIC (205,'SOI Direct Renewal Standard','SOI Direct','One'),
# MAGIC (206,'Direct Single Ultimate','SOI Direct','Three'),
# MAGIC (207,'PCW Renewal Two','SOI Direct','Two'),
# MAGIC (208,'Direct Single Premium','SOI Direct','Two'),
# MAGIC (209,'PCW Renewal Three','SOI Direct','Three'),
# MAGIC (210,'Direct Single Standard','SOI Direct','One'),
# MAGIC (211,'SOI Direct Renewal Premium','SOI Direct','Two'),
# MAGIC (212,'Direct Backpacker Standard','SOI Direct','One'),
# MAGIC (213,'Direct AMT Ultimate','SOI Direct','Three'),
# MAGIC (214,'Direct AMT Premium','SOI Direct','Two'),
# MAGIC (215,'Direct AMT Standard','SOI Direct','One'),
# MAGIC (216,'SOI Direct Renewal Ultimate','SOI Direct','Three'),
# MAGIC (217,'Start MSM Single Premier','Start Agg','Three'),
# MAGIC (218,'Start CYTI Backpacker Classic','Start Agg','Two'),
# MAGIC (219,'Start CYTI AMT Premier','Start Agg','Three'),
# MAGIC (220,'Start IDOL Single Premier.','Start Agg','Three'),
# MAGIC (221,'Start IDOL AMT Essential','Start Agg','One'),
# MAGIC (222,'Start IDOL AMT Premier.','Start Agg','Three'),
# MAGIC (223,'Start IDOL Single Classic.','Start Agg','Two'),
# MAGIC (224,'Start CYTI AMT Classic','Start Agg','Two'),
# MAGIC (225,'Start CYTI Backpacker Essential','Start Agg','One'),
# MAGIC (226,'Start CYTI Single Premier','Start Agg','Three'),
# MAGIC (227,'Start IDOL Backpacker Premier.','Start Agg','Three'),
# MAGIC (228,'Start IDOL Backpacker Essential','Start Agg','One'),
# MAGIC (229,'Start IDOL Backpacker Premier','Start Agg','Three'),
# MAGIC (230,'Start CYTI Backpacker Premier','Start Agg','Three'),
# MAGIC (231,'Start MSM AMT Essential','Start Agg','One'),
# MAGIC (232,'Start IDOL Single Classic','Start Agg','Two'),
# MAGIC (233,'Start MSM AMT Classic','Start Agg','Two'),
# MAGIC (234,'Start IDOL Single Essential.','Start Agg','One'),
# MAGIC (235,'Start IDOL Backpacker Classic','Start Agg','Two'),
# MAGIC (236,'Start CYTI Single Classic','Start Agg','Two'),
# MAGIC (237,'Start IDOL AMT Classic.','Start Agg','Two'),
# MAGIC (238,'Start MSM Single Classic','Start Agg','Two'),
# MAGIC (239,'Start IDOL AMT Premier','Start Agg','Three'),
# MAGIC (240,'Start IDOL AMT Classic','Start Agg','Two'),
# MAGIC (241,'Start CYTI Single Essential','Start Agg','One'),
# MAGIC (242,'Start MSM Backpacker Classic','Start Agg','Two'),
# MAGIC (243,'Start IDOL Backpacker Classic.','Start Agg','Two'),
# MAGIC (244,'Start IDOL AMT Essential.','Start Agg','One'),
# MAGIC (245,'Start MSM AMT Premier','Start Agg','Three'),
# MAGIC (246,'Start IDOL Single Premier','Start Agg','Three'),
# MAGIC (247,'Start IDOL Backpacker Essential.','Start Agg','One'),
# MAGIC (248,'Start IDOL Single Essential','Start Agg','One'),
# MAGIC (249,'Start CYTI AMT Essential','Start Agg','One'),
# MAGIC (250,'Start MSM Backpacker Premier','Start Agg','Three'),
# MAGIC (251,'Start MSM Single Essential','Start Agg','One'),
# MAGIC (252,'Start Direct Renewal 5 Star','Start Direct','Three'),
# MAGIC (253,'Start PCW Renewal Essential','Start Direct','One'),
# MAGIC (254,'Start Direct AMT 4 Star','Start Direct','Two'),
# MAGIC (255,'Start Direct Backpacker Classic','Start Direct','Two'),
# MAGIC (256,'Start Direct Single 4 Star','Start Direct','Two'),
# MAGIC (257,'Start AGG Renewal Classic','Start Direct','Two'),
# MAGIC (258,'Start PCW Renewal Classic','Start Direct','Two'),
# MAGIC (259,'Start AGG Renewal Premier','Start Direct','Three'),
# MAGIC (260,'Start Direct Single 5 Star','Start Direct','Three'),
# MAGIC (261,'Start Direct Backpacker Essential','Start Direct','One'),
# MAGIC (262,'Start Direct Renewal 4 Star','Start Direct','Two'),
# MAGIC (263,'Start Direct AMT 3 Star','Start Direct','One'),
# MAGIC (264,'Start AGG Renewal Essential','Start Direct','One'),
# MAGIC (265,'Start Direct AMT 5 Star','Start Direct','Three'),
# MAGIC (266,'Start Direct Single 3 Star','Start Direct','One'),
# MAGIC (267,'Start Direct Backpacker Premier','Start Direct','Three'),
# MAGIC (268,'Start Direct Renewal 3 Star','Start Direct','One'),
# MAGIC (269,'Start PCW Renewal Premier','Start Direct','Three'),
# MAGIC (270,'MTC NM Single One','SOI Agg','One'),
# MAGIC (271,'MTC NM Single Two','SOI Agg','Two'),
# MAGIC (272,'MTC NM Single Three','SOI Agg','Three'),
# MAGIC (273,'MTC NM AMT One','SOI Agg','One'),
# MAGIC (274,'MTC NM AMT Two','SOI Agg','Two'),
# MAGIC (275,'MTC NM AMT Three','SOI Agg','Three'),
# MAGIC (276,'Viva Direct Backpacker Silver','Viva Direct','One'),
# MAGIC (277,'Viva Direct Backpacker Gold','Viva Direct','Two'),
# MAGIC (278,'Viva Direct Backpacker Platinum','Viva Direct','Three'),
# MAGIC (279,'Viva IDOL Backpacker Silver','Viva Agg','One'),
# MAGIC (280,'Viva IDOL Backpacker Gold','Viva Agg','Two'),
# MAGIC (281,'Viva IDOL Backpacker Platinum','Viva Agg','Three'),
# MAGIC (282,'Viva MSM Backpacker Silver','Viva Agg','One'),
# MAGIC (283,'Viva MSM Backpacker Gold','Viva Agg','Two'),
# MAGIC (284,'Viva MSM Backpacker Platinum','Viva Agg','Three'),
# MAGIC (285,'Viva CYTI Backpacker Silver','Viva Agg','One'),
# MAGIC (286,'Viva CYTI Backpacker Gold','Viva Agg','Two'),
# MAGIC (287,'Viva CYTI Backpacker Platinum','Viva Agg','Three'),
# MAGIC (288,'Viva MTC NM Single Silver','Viva Agg','One'),
# MAGIC (289,'Viva MTC NM Single Gold','Viva Agg','Two'),
# MAGIC (290,'Viva MTC NM Single Platinum','Viva Agg','Three'),
# MAGIC (291,'Viva MTC NM AMT Silver','Viva Agg','One'),
# MAGIC (292,'Viva MTC NM AMT Gold','Viva Agg','Two'),
# MAGIC (293,'Viva MTC NM AMT Platinum','Viva Agg','Three'),
# MAGIC (294,'Viva PCW Renewal Silver','Viva Direct','One'),
# MAGIC (295,'Viva PCW Renewal Gold','Viva Direct','Two'),
# MAGIC (296,'Viva PCW Renewal Platinum','Viva Direct','Three'),
# MAGIC (297,'Viva Direct Renewal Silver','Viva Direct','One'),
# MAGIC (298,'Viva Direct Renewal Gold','Viva Direct','Two'),
# MAGIC (299,'Viva Direct Renewal Platinum','Viva Direct','Three'),
# MAGIC (300,'IDOL NON MED AMT Classic','Oasis Agg','One'),
# MAGIC (301,'CYTI AMT Elite','Oasis Agg','Three'),
# MAGIC (302,'MTC NON MED Backpacker Bronze','Oasis Agg','One'),
# MAGIC (303,'MTC NON MED Backpacker Silver','Oasis Agg','Two'),
# MAGIC (304,'MTC NON MED Backpacker Gold','Oasis Agg','Three'),
# MAGIC (305,'MTC MED Backpacker Bronze','Oasis Agg','One'),
# MAGIC (306,'MTC MED Backpacker Silver','Oasis Agg','Two'),
# MAGIC (307,'MTC MED Backpacker Gold','Oasis Agg','Three'),
# MAGIC (308,'Direct AtoZ Essential Backpacker','Atoz Direct','One'),
# MAGIC (309,'Direct AtoZ Standard Backpacker','Atoz Direct','Two'),
# MAGIC (310,'Direct AtoZ Premium Backpacker','Atoz Direct','Three'),
# MAGIC (311,'Direct AtoZ Silver ST','Atoz Direct','One'),
# MAGIC (312,'Direct AtoZ Gold ST','Atoz Direct','Two'),
# MAGIC (313,'Direct AtoZ Platinum ST','Atoz Direct','Three'),
# MAGIC (314,'Direct AtoZ Silver AMT','Atoz Direct','One'),
# MAGIC (315,'Direct AtoZ Gold AMT','Atoz Direct','Two'),
# MAGIC (316,'Direct AtoZ Platinum AMT','Atoz Direct','Three'),
# MAGIC (317,'Direct AtoZ Silver Backpacker','Atoz Direct','One'),
# MAGIC (318,'Direct AtoZ Gold Backpacker','Atoz Direct','Two'),
# MAGIC (319,'Direct AtoZ Platinum Backpacker','Atoz Direct','Three'),
# MAGIC (320,'AtoZ Platinum Backpacker IDOL','Atoz Agg','Three'),
# MAGIC (321,'AtoZ Platinum Backpacker IDOL.','Atoz Agg','Three'),
# MAGIC (322,'AtoZ Gold Backpacker MSM','Atoz Agg','Two'),
# MAGIC (323,'AtoZ Platinum Backpacker MSM','Atoz Agg','Three'),
# MAGIC (324,'AtoZ Gold Backpacker CYTI','Atoz Agg','Two'),
# MAGIC (325,'AtoZ Platinum Backpacker CYTI','Atoz Agg','Three'),
# MAGIC (326,'AtoZ Renewal Direct AMT Premium','Atoz Direct','Three'),
# MAGIC (327,'AtoZ Renewal Bronze IDOL MED AMT','Atoz Direct','One'),
# MAGIC (328,'Start MSM Backpacker Essential','Start Agg','One'),
# MAGIC (329,'Start MTC NM Single Essential','Start Agg','One'),
# MAGIC (330,'Start MTC NM Single Classic','Start Agg','Two'),
# MAGIC (331,'Start MTC NM Single Premier','Start Agg','Three'),
# MAGIC (332,'Start MTC NM AMT Essential','Start Agg','One'),
# MAGIC (333,'Start MTC NM AMT Classic','Start Agg','Two'),
# MAGIC (334,'Start MTC NM AMT Premier','Start Agg','Three'),
# MAGIC (335,'Direct Single Emerld','Trusted Direct','One'),
# MAGIC (336,'IDOL Single Essential','Trusted Agg','One'),
# MAGIC (337,'IDOL Single Classic','Trusted Agg','Two'),
# MAGIC (338,'IDOL Single Ultimate','Trusted Agg','Three'),
# MAGIC (339,'IDOL AMT Essential','Trusted Agg','One'),
# MAGIC (340,'IDOL AMT Classic','Trusted Agg','Two'),
# MAGIC (341,'IDOL AMT Ultimate','Trusted Agg','Three'),
# MAGIC (342,'IDOL Single Essential','Trusted Agg','One'),
# MAGIC (343,'IDOL Single Classic','Trusted Agg','Two'),
# MAGIC (344,'IDOL AMT Essential','Trusted Agg','One'),
# MAGIC (345,'IDOL AMT Classic','Trusted Agg','Two'),
# MAGIC (346,'IDOL AMT Ultimate','Trusted Agg','Three'),
# MAGIC (347,'MSM Single Essential','Trusted Agg','One'),
# MAGIC (348,'MSM Single Ultimate','Trusted Agg','Three'),
# MAGIC (349,'MSM AMT Essential','Trusted Agg','One'),
# MAGIC (350,'MSM AMT Ultimate','Trusted Agg','Three'),
# MAGIC (351,'CYTI Single Essential','Trusted Agg','One'),
# MAGIC (352,'CYTI Single Ultimate','Trusted Agg','Three'),
# MAGIC (353,'CYTI AMT Essential','Trusted Agg','One'),
# MAGIC (354,'CYTI AMT Ultimate','Trusted Agg','Three'),
# MAGIC (355,'MTC NM Single Essential','Trusted Agg','One'),
# MAGIC (356,'MTC NM Single Classic','Trusted Agg','Two'),
# MAGIC (357,'MTC NM Single Ultimate','Trusted Agg','Three'),
# MAGIC (358,'MTC NM AMT Essential','Trusted Agg','One'),
# MAGIC (359,'MTC NM AMT Classic','Trusted Agg','Two'),
# MAGIC (360,'MTC NM AMT Ultimate','Trusted Agg','Three'),
# MAGIC (361,'PCW Renewal Essential','Trusted Direct','One'),
# MAGIC (362,'PCW Renewal Classic','Trusted Direct','Two'),
# MAGIC (363,'PCW Renewal Ultimate','Trusted Direct','Three'),
# MAGIC (364,'Trusted Direct Renewal Emerald','Trusted Direct','One'),
# MAGIC (365,'Trusted Direct Renewal Ruby','Trusted Direct','Two'),
# MAGIC (366,'Trusted Direct Renewal Diamond','Trusted Direct','Three')
# MAGIC (367, 'Trusted CYTI AMT Classic', 'Trusted Agg', 'Classic'),
# MAGIC (368, 'Trusted CYTI AMT Essential', 'Trusted Agg', 'Essential'),
# MAGIC (369, 'Trusted CYTI AMT Ultimate', 'Trusted Agg', 'Ultimate'),
# MAGIC (370, 'Trusted CYTI Single Classic', 'Trusted Agg', 'Classic'),
# MAGIC (371, 'Trusted CYTI Single Essential', 'Trusted Agg', 'Essential'),
# MAGIC (372, 'Trusted CYTI Single Ultimate', 'Trusted Agg', 'Ultimate'),
# MAGIC (373, 'Trusted Direct AMT Ruby', 'Trusted Direct', 'Ruby'),
# MAGIC (374, 'Trusted Direct Single Diamond', 'Trusted Direct', 'Diamond'),
# MAGIC (375, 'Trusted MTC NM AMT Classic', 'Trusted Agg', 'Classic'),
# MAGIC (376, 'Trusted MTC NM AMT Essential', 'Trusted Agg', 'Essential'),
# MAGIC (377, 'Trusted MTC NM AMT Ultimate', 'Trusted Agg', 'Ultimate'),
# MAGIC (378, 'Trusted MTC NM Single Classic', 'Trusted Agg', 'Classic'),
# MAGIC (379, 'Trusted MTC NM Single Essential', 'Trusted Agg', 'Essential'),
# MAGIC (380, 'Trusted MTC NM Single Ultimate', 'Trusted Agg', 'Ultimate');
# MAGIC 
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.DimOption
# MAGIC (
# MAGIC     OptionId INT,
# MAGIC     OptionName STRING,
# MAGIC     OptionCategory STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimension table: Option'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/DimOption';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC INSERT INTO DimOption (OptionId, OptionName, OptionCategory)
# MAGIC VALUES
# MAGIC (1, 'Gadget cover Extended £3,000', 'Gadget'),
# MAGIC (2, 'Gadget Cover', 'Gadget'),
# MAGIC (3, 'Winter Sports', 'Winter Sports'),
# MAGIC (4, 'Enhanced COVID-19 Protect Cover', 'Other'),
# MAGIC (5, 'Gadget Cover Extended £2,000', 'Gadget'),
# MAGIC (6, 'Business Travel', 'Other'),
# MAGIC (7, 'AMT Cruise - 3+ trips', 'Cruise'),
# MAGIC (8, 'AMT Cruise - 2 trips', 'Cruise'),
# MAGIC (9, 'Wedding Cover', 'Other'),
# MAGIC (10, 'Golf Cover', 'Winter Sports'),
# MAGIC (11, 'Gadget Cover Extended £1,000', 'Gadget'),
# MAGIC (12, 'Extended Travel Disruption', 'Other'),
# MAGIC (13, 'AMT Cruise - 1 trip', 'Cruise'),
# MAGIC (14, 'Cruise Cover', 'Cruise'),
# MAGIC (15, 'Excess Waiver', 'Excess Waiver'),
# MAGIC (16, 'Gadget cover Extended £2,000', 'Gadget'),
# MAGIC (17, 'Standard Sports & Activities', 'Other'),
# MAGIC (18, 'Gadget cover Extended £1,000', 'Gadget'),
# MAGIC (19, 'Gadget Cover Extended £3,000', 'Gadget'),
# MAGIC (20, 'Enhanced Gadget Cover', 'Gadget'),
# MAGIC (21, 'Sports & Activities', 'Other'),
# MAGIC (22, 'Rental Vehicle Excess Waiver Cover', 'Excess Waiver');


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC drop table  TaurusGoldLH.DimDestination

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.DimDestination
# MAGIC (
# MAGIC     DestinationId INT,
# MAGIC     Destination STRING,
# MAGIC     Region STRING,
# MAGIC     Brand STRING,
# MAGIC     SingleTrip DECIMAL(5,2),
# MAGIC     AMT DECIMAL(5,2)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimension table: Destination'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/DimDestination';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC -- INSERT INTO DimDestination (DestinationId, Destination, Region, Brand, SingleTrip, AMT)
# MAGIC -- VALUES
# MAGIC -- (1, 'Unknown', 'Unknown', 0.00, 0.00),
# MAGIC -- (2, 'Western Europe (No EHIC)', 'Europe 1', 45.00, 0.00),
# MAGIC -- (3, 'Turkey', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (4, 'Central Europe (NO EHIC)', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (5, 'Europe', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (6, 'Germany', 'Europe 1', 45.00, 0.00),
# MAGIC -- (7, 'Eastern Europe', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (8, 'Europe1', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (9, 'Europe 1 & 2', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (10, 'France', 'Europe 1', 45.00, 0.00),
# MAGIC -- (11, 'Greece', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (12, 'Worldwide excluding USA,Canada & Caribbean', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (13, 'Africa', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (14, 'Europe (Andora, Gibraltar, Monaco, Vatican City State)', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (15, 'Worldwide including USA, Canada, the Caribbean & Mexico', 'WW Including', 10.00, 35.00),
# MAGIC -- (16, 'Belgium', 'Europe 1', 45.00, 0.00),
# MAGIC -- (17, 'Worldwide excluding USA, Canada, the Caribbean & Mexico', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (18, 'Worldwide excluding USA,Canada, the Caribbean & Mexico', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (19, 'United States', 'WW Including', 10.00, 35.00),
# MAGIC -- (20, 'India', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (21, 'China', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (22, 'Malta', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (23, 'Croatia', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (24, 'Italy', 'Europe 1', 45.00, 0.00),
# MAGIC -- (25, 'Europe 2', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (26, 'Spain', 'Europe 1', 45.00, 0.00),
# MAGIC -- (27, 'Ireland', 'Europe 1', 45.00, 0.00),
# MAGIC -- (28, 'Thailand', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (29, 'Caribbean', 'WW Including', 10.00, 35.00),
# MAGIC -- (30, 'Morocco', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (31, 'Worldwide', 'WW Including', 10.00, 35.00),
# MAGIC -- (32, 'Western Europe (EHIC)', 'Europe 1', 45.00, 0.00),
# MAGIC -- (33, 'Europe (Bulgaria, Czech Republic, Estonia, Hungary, Latvia, Lithuania, Poland, Romania, Slovakia, Slovenia)', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (34, 'South/Central America', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (35, 'Cyprus', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (36, 'Mexico', 'WW Including', 10.00, 35.00),
# MAGIC -- (37, 'WorldwideExcludingUSACanadaAndCaribbean', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (38, 'Europe (Albania, Bosnia and Herzegovina, Macedonia, Montenegro, Serbia)', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (39, 'Tunisia', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (40, 'UK', 'UK', 3.00, 0.00),
# MAGIC -- (41, 'Middle East', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (42, 'Switzerland', 'Europe 1', 45.00, 0.00),
# MAGIC -- (43, 'Pacific/Indian/Atlantic Ocean', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (44, 'United Arab Emirates', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (45, 'Australia/New Zealand', 'Australia/New Zealand', 1.50, 0.00),
# MAGIC -- (46, 'Canada', 'WW Including', 10.00, 35.00),
# MAGIC -- (47, 'Asia', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (48, 'Central Europe (EHIC)', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (49, 'Worldwide including USA,Canada & Caribbean', 'WW Including', 10.00, 35.00),
# MAGIC -- (50, 'Worldwide including USA,Canada, the Caribbean & Mexico', 'WW Including', 10.00, 35.00),
# MAGIC -- (51, 'Portugal', 'Europe 1', 45.00, 0.00),
# MAGIC -- (52, 'Europe (Denmark, Faroe Islands, Finland, Iceland, Liechtenstein, Luxembourg, Norway, Svalbard And Jan Mayen, Sweden)', 'Europe 1', 45.00, 0.00),
# MAGIC -- (53, 'Austria', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (54, 'Egypt', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (55, 'South Africa', 'WW Excluding', 10.50, 20.00),
# MAGIC -- (56, 'ANZ', 'Australia/New Zealand', 1.50, 0.00),
# MAGIC -- (57, 'Europe 1 and 2', 'Europe 2 (Incl where shows as Europe)', 30.00, 45.00),
# MAGIC -- (58, 'United Kingdom', 'UK', 3.00, 0.00),
# MAGIC -- (59, 'Netherlands', 'Europe 1', 45.00, 0.00);


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC INSERT INTO DimDestination (DestinationId, Destination, Region, Brand, SingleTrip, AMT)
# MAGIC VALUES
# MAGIC (1, 'WorldwideExcludingUSACanadaAndCaribbean', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (2, 'Europe (Albania, Bosnia and Herzegovina, Macedonia, Montenegro, Serbia)', 'Europe 2', 'Trusted', 30, 45),
# MAGIC (3, 'Tunisia', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (4, 'UK', 'UK', 'Trusted', 3, 0),
# MAGIC (5, 'Middle East', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (6, 'Switzerland', 'Europe 2', 'Trusted', 45, 0),
# MAGIC (7, 'Pacific/Indian/Atlantic Ocean', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (8, 'United Arab Emirates', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (9, 'Australia/New Zealand', 'Australia/New Zealand', 'Trusted', 1.5, 0),
# MAGIC (10, 'Canada', 'WW Including', 'Trusted', 10, 35),
# MAGIC (11, 'Asia', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (12, 'Central Europe (EHIC)', 'Europe 2', 'Trusted', 30, 45),
# MAGIC (13, 'Worldwide including USA,Canada & Caribbean', 'WW Including', 'Trusted', 10, 35),
# MAGIC (14, 'Worldwide including USA,Canada, the Caribbean & Mexico', 'WW Including', 'Trusted', 10, 35),
# MAGIC (15, 'Portugal', 'Europe 1', 'Trusted', 45, 0),
# MAGIC (16, 'Worldwide including USA, Canada, the Caribbean & Mexico', 'WW Including', 'Trusted', 10, 35),
# MAGIC (17, 'Belgium', 'Europe 1', 'Trusted', 45, 0),
# MAGIC (18, 'Worldwide excluding USA, Canada, the Caribbean & Mexico', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (19, 'Worldwide excluding USA,Canada, the Caribbean & Mexico', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (20, 'United States', 'WW Including', 'Trusted', 10, 35),
# MAGIC (21, 'India', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (22, 'China', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (23, 'Malta', 'Europe 2', 'Trusted', 30, 45),
# MAGIC (24, 'Europe (Denmark, Faroe Islands, Finland, Iceland, Liechtenstein, Luxembourg, Norway, Svalbard And Jan Mayen, Sweden)', 'Europe 1', 'Trusted', 45, 0),
# MAGIC (25, 'Austria', 'Europe 1', 'Trusted', 30, 45),
# MAGIC (26, 'Egypt', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (27, 'South Africa', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (28, 'ANZ', 'Australia/New Zealand', 'Trusted', 1.5, 0),
# MAGIC (29, 'Europe 1 and 2', 'Europe 2', 'Trusted', 30, 45),
# MAGIC (30, 'United Kingdom', 'UK', 'Trusted', 3, 0),
# MAGIC (31, 'Netherlands', 'Europe 1', 'Trusted', 45, 0),
# MAGIC (32, 'Europe1', 'Europe 1', 'Trusted', 30, 45),
# MAGIC (33, 'Europe 1 & 2', 'Europe 2', 'Trusted', 30, 45),
# MAGIC (34, 'France', 'Europe 1', 'Trusted', 45, 0),
# MAGIC (35, 'Greece', 'Europe 2', 'Trusted', 30, 45),
# MAGIC (36, 'Worldwide excluding USA,Canada & Caribbean', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (37, 'Africa', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (38, 'Europe (Andora, Gibraltar, Monaco, Vatican City State)', 'Europe 2', 'Trusted', 30, 45),
# MAGIC (39, 'Unknown', 'Unknown', 'Trusted', 0, 0),
# MAGIC (40, 'Western Europe (No EHIC)', 'Europe 1', 'Trusted', 45, 0),
# MAGIC (41, 'Turkey', 'Europe 2', 'Trusted', 10.5, 20),
# MAGIC (42, 'Central Europe (NO EHIC)', 'Europe 2', 'Trusted', 30, 45),
# MAGIC (43, 'Europe', 'Europe 2', 'Trusted', 30, 45),
# MAGIC (44, 'Germany', 'Europe 1', 'Trusted', 45, 0),
# MAGIC (45, 'Eastern Europe', 'Europe 1', 'Trusted', 30, 45),
# MAGIC (46, 'Morocco', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (47, 'Worldwide', 'WW Including', 'Trusted', 10, 35),
# MAGIC (48, 'Western Europe (EHIC)', 'Europe 1', 'Trusted', 45, 0),
# MAGIC (49, 'Europe (Bulgaria, Czech Republic, Estonia, Hungary, Latvia, Lithuania, Poland, Romania, Slovakia, Slovenia)', 'Europe 1', 'Trusted', 30, 45),
# MAGIC (50, 'South/Central America', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (51, 'Cyprus', 'Europe 2', 'Trusted', 30, 45),
# MAGIC (52, 'Mexico', 'WW Including', 'Trusted', 10, 35),
# MAGIC (53, 'Croatia', 'Europe 1', 'Trusted', 30, 45),
# MAGIC (54, 'Italy', 'Europe 1', 'Trusted', 45, 0),
# MAGIC (55, 'Europe 2', 'Europe 2', 'Trusted', 30, 45),
# MAGIC (56, 'Spain', 'Europe 2', 'Trusted', 45, 0),
# MAGIC (57, 'Ireland', 'Europe 1', 'Trusted', 45, 0),
# MAGIC (58, 'Thailand', 'WW Excluding', 'Trusted', 10.5, 20),
# MAGIC (59, 'Caribbean', 'WW Including', 'Trusted', 10, 35),
# MAGIC (60, 'WorldwideExcludingUSACanadaAndCaribbean', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (61, 'Europe (Albania, Bosnia and Herzegovina, Macedonia, Montenegro, Serbia)', 'Europe 2', 'All Other Brand', 30, 45),
# MAGIC (62, 'Tunisia', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (63, 'UK', 'UK', 'All Other Brand', 3, 0),
# MAGIC (64, 'Middle East', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (65, 'Switzerland', 'Europe 2', 'All Other Brand', 45, 0),
# MAGIC (66, 'Pacific/Indian/Atlantic Ocean', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (67, 'United Arab Emirates', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (68, 'Australia/New Zealand', 'Australia/New Zealand', 'All Other Brand', 1.5, 0),
# MAGIC (69, 'Canada', 'WW Including', 'All Other Brand', 10, 35),
# MAGIC (70, 'Asia', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (71, 'Central Europe (EHIC)', 'Europe 2', 'All Other Brand', 30, 45),
# MAGIC (72, 'Worldwide including USA,Canada & Caribbean', 'WW Including', 'All Other Brand', 10, 35),
# MAGIC (73, 'Worldwide including USA,Canada, the Caribbean & Mexico', 'WW Including', 'All Other Brand', 10, 35),
# MAGIC (74, 'Portugal', 'Europe 1', 'All Other Brand', 45, 0),
# MAGIC (75, 'Worldwide including USA, Canada, the Caribbean & Mexico', 'WW Including', 'All Other Brand', 10, 35),
# MAGIC (76, 'Belgium', 'Europe 1', 'All Other Brand', 45, 0),
# MAGIC (77, 'Worldwide excluding USA, Canada, the Caribbean & Mexico', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (78, 'Worldwide excluding USA,Canada, the Caribbean & Mexico', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (79, 'United States', 'WW Including', 'All Other Brand', 10, 35),
# MAGIC (80, 'India', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (81, 'China', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (82, 'Malta', 'Europe 2', 'All Other Brand', 30, 45),
# MAGIC (83, 'Europe (Denmark, Faroe Islands, Finland, Iceland, Liechtenstein, Luxembourg, Norway, Svalbard And Jan Mayen, Sweden)', 'Europe 1', 'All Other Brand', 45, 0),
# MAGIC (84, 'Austria', 'Europe 1', 'All Other Brand', 30, 45),
# MAGIC (85, 'Egypt', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (86, 'South Africa', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (87, 'ANZ', 'Australia/New Zealand', 'All Other Brand', 1.5, 0),
# MAGIC (88, 'Europe 1 and 2', 'Europe 2', 'All Other Brand', 30, 45),
# MAGIC (89, 'United Kingdom', 'UK', 'All Other Brand', 3, 0),
# MAGIC (90, 'Netherlands', 'Europe 1', 'All Other Brand', 45, 0),
# MAGIC (91, 'Europe1', 'Europe 1', 'All Other Brand', 30, 45),
# MAGIC (92, 'Europe 1 & 2', 'Europe 2', 'All Other Brand', 30, 45),
# MAGIC (93, 'France', 'Europe 1', 'All Other Brand', 45, 0),
# MAGIC (94, 'Greece', 'Europe 2', 'All Other Brand', 30, 45),
# MAGIC (95, 'Worldwide excluding USA,Canada & Caribbean', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (96, 'Africa', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (97, 'Europe (Andora, Gibraltar, Monaco, Vatican City State)', 'Europe 2', 'All Other Brand', 30, 45),
# MAGIC (98, 'Unknown', 'Unknown', 'All Other Brand', 0, 0),
# MAGIC (99, 'Western Europe (No EHIC)', 'Europe 1', 'All Other Brand', 45, 0),
# MAGIC (100, 'Turkey', 'Europe 2', 'All Other Brand', 10.5, 20),
# MAGIC (101, 'Central Europe (NO EHIC)', 'Europe 2', 'All Other Brand', 30, 45),
# MAGIC (102, 'Europe', 'Europe 2', 'All Other Brand', 30, 45),
# MAGIC (103, 'Germany', 'Europe 1', 'All Other Brand', 45, 0),
# MAGIC (104, 'Eastern Europe', 'Europe 1', 'All Other Brand', 30, 45),
# MAGIC (105, 'Morocco', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (106, 'Worldwide', 'WW Including', 'All Other Brand', 10, 35),
# MAGIC (107, 'Western Europe (EHIC)', 'Europe 1', 'All Other Brand', 45, 0),
# MAGIC (108, 'Europe (Bulgaria, Czech Republic, Estonia, Hungary, Latvia, Lithuania, Poland, Romania, Slovakia, Slovenia)', 'Europe 1', 'All Other Brand', 30, 45),
# MAGIC (109, 'South/Central America', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (110, 'Cyprus', 'Europe 2', 'All Other Brand', 30, 45),
# MAGIC (111, 'Mexico', 'WW Including', 'All Other Brand', 10, 35),
# MAGIC (112, 'Croatia', 'Europe 1', 'All Other Brand', 30, 45),
# MAGIC (113, 'Italy', 'Europe 1', 'All Other Brand', 45, 0),
# MAGIC (114, 'Europe 2', 'Europe 2', 'All Other Brand', 30, 45),
# MAGIC (115, 'Spain', 'Europe 2', 'All Other Brand', 45, 0),
# MAGIC (116, 'Ireland', 'Europe 1', 'All Other Brand', 45, 0),
# MAGIC (117, 'Thailand', 'WW Excluding', 'All Other Brand', 10.5, 20),
# MAGIC (118, 'Caribbean', 'WW Including', 'All Other Brand', 10, 35),
# MAGIC     (119,"Europe 1", "Europe 1", "All Other Brand", 45.0, 0.0),
# MAGIC     (120,"Europe 3", "Europe 3", "All Other Brand", 30.0, 45.0),  
# MAGIC     (121,"Worldwide excluding USA, Canada, Mexico, the Caribbean and Egypt", "WW Excluding", "All Other Brand", 10.5, 20.0),
# MAGIC     (122,"Worldwide including USA, Canada, the Caribbean and Mexico", "WW Including", "All Other Brand", 10.0, 35.0),
# MAGIC     (123,"Australia & New Zealand", "Australia/New Zealand", "All Other Brand", 1.5, 0.0)
# MAGIC 
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from TaurusGoldLH.DimTravelBrand

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT
# MAGIC     f.TravelBrandId,
# MAGIC     f.DestinationId,
# MAGIC     d.Destination,
# MAGIC     d.Brand,
# MAGIC     COUNT(*) AS MismatchCount
# MAGIC FROM TaurusGoldLH.FactEnquiries f
# MAGIC JOIN TaurusGoldLH.DimDestination d
# MAGIC     ON f.DestinationId = d.DestinationId
# MAGIC WHERE
# MAGIC     (f.TravelBrandId = 12 AND d.Brand <> 'Trusted')
# MAGIC     OR (f.TravelBrandId <> 12 AND d.Brand <> 'All Other Brand')
# MAGIC GROUP BY f.TravelBrandId, f.DestinationId, d.Destination, d.Brand
# MAGIC ORDER BY MismatchCount DESC;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select distinct DestinationId from TaurusGoldLH.FactEnquiries
# MAGIC where TravelBrandId = 12


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from TaurusGoldLH.DimDestination

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.DimFamilyGroup
# MAGIC (
# MAGIC     FamilyGroupId INT,
# MAGIC     FamilyGroup STRING,
# MAGIC     SingleTrip DECIMAL(5,2),
# MAGIC     AMT DECIMAL(5,2)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimension table: Family Group'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/DimFamilyGroup';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC INSERT INTO DimFamilyGroup (FamilyGroupId, FamilyGroup, SingleTrip, AMT)
# MAGIC VALUES 
# MAGIC     (1, 'Individual', 60.00, 45.00),
# MAGIC     (2, 'Group', 10.00, 5.00),
# MAGIC     (3, 'Couple', 15.00, 30.00),
# MAGIC     (4, 'Family', 12.50, 17.00),
# MAGIC     (5, 'Single Parent Family', 2.50, 3.00);
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.DimAgeGroup
# MAGIC (
# MAGIC     AgeGroupId INT,
# MAGIC     AgeGroup STRING,
# MAGIC     SingleTrip DECIMAL(5,2),
# MAGIC     AMT DECIMAL(5,2)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimension table: Age Group'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/DimAgeGroup';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from decimal import Decimal  # Required for DecimalType values

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Insert DimAgeGroup") \
    .getOrCreate()

# Define schema with decimal precision
schema = StructType([
    StructField("AgeGroupId", IntegerType(), False),
    StructField("AgeGroup", StringType(), False),
    StructField("SingleTrip", DecimalType(5, 2), False),
    StructField("AMT", DecimalType(5, 2), False)
])

# Define age group data with percentage values
age_group_data = [
    (1, "0 to 20", Decimal("5.00"), Decimal("5.00")),
    (2, "21 to 30", Decimal("15.00"), Decimal("15.00")),
    (3, "31 to 40", Decimal("20.00"), Decimal("20.00")),
    (4, "41 to 50", Decimal("20.00"), Decimal("20.00")),
    (5, "51 to 65", Decimal("22.50"), Decimal("25.00")),
    (6, "66 to 75", Decimal("12.50"), Decimal("12.50")),
    (7, "76 +", Decimal("5.00"), Decimal("2.50"))
]

# Create DataFrame
df_age_group = spark.createDataFrame(age_group_data, schema)

# Show the DataFrame sorted by ID
df_age_group.orderBy("AgeGroupId").show()

# Save to Hive table or external storage
df_age_group.orderBy("AgeGroupId").write.mode("overwrite").saveAsTable("DimAgeGroup")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.DimCountry
# MAGIC (
# MAGIC     CountryID INT,
# MAGIC     VariantName STRING,
# MAGIC     StandardName STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimension table: Country'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/DimCountry';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (1, 'SOUTH AFRICA', 'South Africa');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (2, 'BAHAMAS', 'Bahamas');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (3, 'ARMENIA', 'Armenia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (4, 'CAMBODIA', 'Cambodia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (5, 'JAPAN', 'Japan');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (6, 'UGANDA', 'Uganda');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (7, 'BANGLADESH', 'Bangladesh');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (8, 'TIMOR-LESTE', 'Timor-Leste');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (9, 'TENERIFE', 'Tenerife');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (10, 'CAPE VERDE', 'Cape Verde');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (11, 'MAURITANIA', 'Mauritania');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (12, 'Balearics', 'Balearics');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (13, 'JERSEY', 'Jersey');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (14, 'SAINT MARTIN', 'Saint Martin');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (15, 'MALDIVES', 'Maldives');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (16, 'Corsica', 'Corsica');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (17, 'SAO TOME AND PRINCIPE', 'Sao Tome And Principe');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (18, 'GREEK ISLANDS', 'Greek Islands');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (19, 'JORDAN', 'Jordan');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (20, 'Zaire', 'Zaire');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (21, 'RUSSIAN FEDERATION', 'Russian Federation');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (22, 'LESOTHO', 'Lesotho');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (23, 'MALTA', 'Malta');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (24, 'ALBANIA', 'Albania');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (25, 'BELARUS', 'Belarus');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (26, 'EQUATORIAL GUINEA', 'Equatorial Guinea');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (27, 'ANTIGUA AND BARBUDA', 'Antigua And Barbuda');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (28, 'MAURITIUS', 'Mauritius');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (29, 'MINORCA', 'Minorca');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (30, 'KUWAIT', 'Kuwait');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (31, 'VATICAN CITY STATE', 'Vatican City State');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (32, 'St Kitts And Nevis', 'St Kitts And Nevis');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (33, 'LITHUANIA', 'Lithuania');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (34, 'Zanzibar', 'Zanzibar');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (35, 'UNITED KINGDOM', 'United Kingdom');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (36, 'AZERBAIJAN', 'Azerbaijan');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (37, 'PHILIPPINES', 'Philippines');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (38, 'SAINT KITTS AND NEVIS', 'Saint Kitts And Nevis');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (39, 'ANDORRA', 'Andorra');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (40, 'FALKLAND ISLANDS (MALVINAS)', 'Falkland Islands (Malvinas)');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (41, 'CZECH REPUBLIC', 'Czech Republic');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (42, 'SENEGAL', 'Senegal');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (43, 'MAJORCA', 'Majorca');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (44, 'Sardinia', 'Sardinia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (45, 'OMAN', 'Oman');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (46, 'Macau', 'Macau');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (47, 'AMERICAN SAMOA', 'American Samoa');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (48, 'SAMOA', 'Samoa');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (49, 'FRENCH GUIANA', 'French Guiana');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (50, 'SOMALIA', 'Somalia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (51, 'MONTSERRAT', 'Montserrat');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (52, 'ESTONIA', 'Estonia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (53, 'CÔTE D''IVOIRE', 'Côte D''Ivoire');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (54, 'KIRIBATI', 'Kiribati');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (55, 'FIJI', 'Fiji');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (56, 'Lapland', 'Lapland');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (57, 'COSTA RICA', 'Costa Rica');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (58, 'BHUTAN', 'Bhutan');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (59, 'CANADA', 'Canada');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (60, 'LAO PEOPLE''S DEMOCRATIC REPUBLIC', 'Lao People''S Democratic Republic');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (61, 'POLAND', 'Poland');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (62, 'ALASKA', 'Alaska');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (63, 'PUERTO RICO', 'Puerto Rico');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (64, 'Curacao', 'Curacao');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (65, 'GRENADA', 'Grenada');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (66, 'SLOVAKIA', 'Slovakia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (67, 'COLOMBIA', 'Colombia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (68, 'SRI LANKA', 'Sri Lanka');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (69, 'IBIZA', 'Ibiza');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (70, 'VANUATU', 'Vanuatu');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (71, 'GUATEMALA', 'Guatemala');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (72, 'UNITED ARAB EMIRATES', 'United Arab Emirates');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (73, 'DUBAI', 'Dubai');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (74, 'ALGERIA', 'Algeria');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (75, 'GIBRALTAR', 'Gibraltar');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (76, 'GUERNSEY', 'Guernsey');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (77, 'HONDURAS', 'Honduras');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (78, 'TURKMENISTAN', 'Turkmenistan');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (79, 'ARUBA', 'Aruba');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (80, 'DOMINICA', 'Dominica');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (81, 'EL SALVADOR', 'El Salvador');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (82, 'BENIN', 'Benin');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (83, 'LIBERIA', 'Liberia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (84, 'MOROCCO', 'Morocco');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (85, 'BOSNIA AND HERZEGOVINA', 'Bosnia And Herzegovina');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (86, 'MEXICO', 'Mexico');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (87, 'CAMEROON', 'Cameroon');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (88, 'PAKISTAN', 'Pakistan');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (89, 'BURUNDI', 'Burundi');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (90, 'ANGUILLA', 'Anguilla');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (91, 'FRANCE', 'France');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (92, 'GAMBIA', 'Gambia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (93, 'CAYMAN ISLANDS', 'Cayman Islands');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (94, 'GABON', 'Gabon');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (95, 'BRUNEI DARUSSALAM', 'Brunei Darussalam');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (96, 'FRENCH POLYNESIA', 'French Polynesia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (97, 'SAINT LUCIA', 'Saint Lucia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (98, 'SINGAPORE', 'Singapore');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (99, 'MOZAMBIQUE', 'Mozambique');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (100, 'MADAGASCAR', 'Madagascar');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (101, 'NIGERIA', 'Nigeria');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (102, 'CHINA', 'China');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (103, 'RHODES', 'Rhodes');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (104, 'AUSTRIA', 'Austria');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (105, 'MALAYSIA', 'Malaysia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (106, 'LATVIA', 'Latvia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (107, 'Bali', 'Bali');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (108, 'ZAMBIA', 'Zambia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (109, 'NICARAGUA', 'Nicaragua');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (110, 'BOTSWANA', 'Botswana');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (111, 'NEPAL', 'Nepal');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (112, 'MALI', 'Mali');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (113, 'GRAN CANARIA', 'Gran Canaria');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (114, 'KYRGYZSTAN', 'Kyrgyzstan');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (115, 'ZIMBABWE', 'Zimbabwe');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (116, 'Laos', 'Laos');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (117, 'BELIZE', 'Belize');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (118, 'SAUDI ARABIA', 'Saudi Arabia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (119, 'KOREA, REPUBLIC OF', 'Korea, Republic Of');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (120, 'CROATIA', 'Croatia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (121, 'SURINAME', 'Suriname');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (122, 'RWANDA', 'Rwanda');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (123, 'BULGARIA', 'Bulgaria');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (124, 'SWITZERLAND', 'Switzerland');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (125, 'ARGENTINA', 'Argentina');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (126, 'KAZAKHSTAN', 'Kazakhstan');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (127, 'HONG KONG', 'Hong Kong');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (128, 'LANZAROTE', 'Lanzarote');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (129, 'GREENLAND', 'Greenland');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (130, 'GUAM', 'Guam');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (131, 'UKRAINE', 'Ukraine');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (132, 'SERBIA', 'Serbia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (133, 'SOLOMON ISLANDS', 'Solomon Islands');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (134, 'NORWAY', 'Norway');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (135, 'FINLAND', 'Finland');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (136, 'MENORCA', 'Menorca');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (137, 'MACAO', 'Macao');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (138, 'MALAWI', 'Malawi');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (139, 'MONGOLIA', 'Mongolia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (140, 'MARTINIQUE', 'Martinique');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (141, 'KOS (GREEK ISLAND)', 'Kos (Greek Island)');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (142, 'NETHERLANDS', 'Netherlands');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (143, 'DOMINICAN REPUBLIC', 'Dominican Republic');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (144, 'ANGOLA', 'Angola');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (145, 'SLOVENIA', 'Slovenia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (146, 'GUYANA', 'Guyana');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (147, 'GRAND CANARIA', 'Grand Canaria');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (148, 'SIERRA LEONE', 'Sierra Leone');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (149, 'UZBEKISTAN', 'Uzbekistan');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (150, 'TUNISIA', 'Tunisia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (151, 'CHANNEL ISLANDS', 'Channel Islands');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (152, 'HOLLAND', 'Holland');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (153, 'SWEDEN', 'Sweden');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (154, 'ECUADOR', 'Ecuador');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (155, 'ANTARCTICA', 'Antarctica');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (156, 'BRITISH VIRGIN ISLES', 'British Virgin Isles');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (157, 'BELGIUM', 'Belgium');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (158, 'BOLIVIA', 'Bolivia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (159, 'TONGA', 'Tonga');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (160, 'NAMIBIA', 'Namibia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (161, 'TRINIDAD AND TOBAGO', 'Trinidad And Tobago');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (162, 'CONGO, THE DEMOCRATIC REPUBLIC OF THE', 'Congo, The Democratic Republic Of The');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (163, 'NEW ZEALAND', 'New Zealand');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (164, 'SAINT HELENA', 'Saint Helena');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (165, 'SPAIN', 'Spain');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (166, 'MOLDOVA', 'Moldova');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (167, 'PARAGUAY', 'Paraguay');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (168, 'LEBANON', 'Lebanon');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (169, 'GREECE', 'Greece');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (170, 'PALAU', 'Palau');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (171, 'SWAZILAND', 'Swaziland');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (172, 'GUINEA', 'Guinea');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (173, 'INDIA', 'India');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (174, 'BERMUDA', 'Bermuda');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (175, 'KENYA', 'Kenya');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (176, 'ICELAND', 'Iceland');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (177, 'Azores', 'Azores');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (178, 'TURKEY', 'Turkey');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (179, 'COOK ISLANDS', 'Cook Islands');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (180, 'FAROE ISLANDS', 'Faroe Islands');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (181, 'LUXEMBOURG', 'Luxembourg');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (182, 'CORFU', 'Corfu');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (183, 'TAJIKISTAN', 'Tajikistan');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (184, 'ABU DHABI', 'Abu Dhabi');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (185, 'MACEDONIA, THE FORMER YUGOSLAV REPUBLIC OF', 'Macedonia, The Former Yugoslav Republic Of');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (186, 'ZANTE', 'Zante');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (187, 'TURKS AND CAICOS ISLANDS', 'Turks And Caicos Islands');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (188, 'URUGUAY', 'Uruguay');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (189, 'VIRGIN ISLANDS, BRITISH', 'Virgin Islands, British');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (190, 'NETHERLANDS ANTILLES', 'Netherlands Antilles');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (191, 'ITALY', 'Italy');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (192, 'AUSTRALIA', 'Australia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (193, 'MONACO', 'Monaco');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (194, 'PANAMA', 'Panama');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (195, 'CHILE', 'Chile');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (196, 'TAIWAN, PROVINCE OF CHINA', 'Taiwan, Province Of China');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (197, 'THAILAND', 'Thailand');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (198, 'HUNGARY', 'Hungary');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (199, 'DENMARK', 'Denmark');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (200, 'QATAR', 'Qatar');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (201, 'SAINT VINCENT AND THE GRENADINES', 'Saint Vincent And The Grenadines');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (202, 'JAMAICA', 'Jamaica');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (203, 'TOGO', 'Togo');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (204, 'CONGO', 'Congo');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (205, 'SVALBARD AND JAN MAYEN', 'Svalbard And Jan Mayen');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (206, 'VIET NAM', 'Viet Nam');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (207, 'MONTENEGRO', 'Montenegro');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (208, 'HAITI', 'Haiti');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (209, 'SAN MARINO', 'San Marino');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (210, 'DJIBOUTI', 'Djibouti');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (211, 'CYPRUS', 'Cyprus');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (212, 'SEYCHELLES', 'Seychelles');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (213, 'COMOROS', 'Comoros');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (214, 'ETHIOPIA', 'Ethiopia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (215, 'ISRAEL', 'Israel');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (216, 'TANZANIA, UNITED REPUBLIC OF', 'Tanzania, United Republic Of');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (217, 'PORTUGAL', 'Portugal');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (218, 'PAPUA NEW GUINEA', 'Papua New Guinea');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (219, 'INDONESIA', 'Indonesia');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (220, 'CRETE', 'Crete');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (221, 'IRELAND', 'Ireland');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (222, 'BURKINA FASO', 'Burkina Faso');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (223, 'UNITED STATES', 'United States');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (224, 'ROMANIA', 'Romania');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (225, 'ERITREA', 'Eritrea');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (226, 'CANARY ISLANDS', 'Canary Islands');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (227, 'GERMANY', 'Germany');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (228, 'VIRGIN ISLANDS, U.S.', 'Virgin Islands, U.S.');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (229, 'GHANA', 'Ghana');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (230, 'BRAZIL', 'Brazil');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (231, 'ISLE OF MAN', 'Isle Of Man');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (232, 'GUINEA-BISSAU', 'Guinea-Bissau');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (233, 'BARBADOS', 'Barbados');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (234, 'Madeira', 'Madeira');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (235, 'EGYPT', 'Egypt');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (236, 'VENEZUELA', 'Venezuela');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (237, 'BAHRAIN', 'Bahrain');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (238, 'CUBA', 'Cuba');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (239, 'Sarawak', 'Sarawak');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (240, 'PERU', 'Peru');
# MAGIC INSERT INTO DimCountry (CountryID, VariantName, StandardName) VALUES (241, 'GEORGIA', 'Georgia');

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE FactQuoteAnalysis (
# MAGIC     TravelEnquiryID INT,
# MAGIC     TravelBrandId INT,
# MAGIC     DurationId INT,
# MAGIC     PolicyTypeId INT,
# MAGIC     ChannelId INT,
# MAGIC     OptionId INT,
# MAGIC     DestinationId INT,
# MAGIC     FamilyGroupId INT,
# MAGIC     AgeGroupId INT,
# MAGIC     DateID BIGINT,
# MAGIC     MedicalScoreId INT,
# MAGIC     LeadTimeGroupId INT,
# MAGIC     MarketingChannelId INT,
# MAGIC     TotalGrossIncIPT DECIMAL(18, 2),
# MAGIC     TotalGrossExcIPT DECIMAL(18, 2),
# MAGIC     RecordCount BIGINT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactQuoteAnalysis';
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC drop table TaurusGoldLH.FactEnquiries 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from  TaurusGoldLH.FactEnquiries

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE TaurusGoldLH.FactEnquiries (
# MAGIC     TravelEnquiryID INT,
# MAGIC     DateID BIGINT,
# MAGIC     TravelBrandId INT,
# MAGIC     ChannelId INT,
# MAGIC     DurationId INT,
# MAGIC     PolicyTypeId INT,
# MAGIC     DestinationId INT,
# MAGIC     FamilyGroupId INT, 
# MAGIC     LeadTimeGroupId INT,
# MAGIC     MedicalScoreId INT,
# MAGIC     AgeGroupId INT,
# MAGIC     MarketingChannelId INT,
# MAGIC     AgentID INT,
# MAGIC     IsQuoted INT,
# MAGIC     NoOfQuotes INT,
# MAGIC     IsSold INT,
# MAGIC     TotalTravellers INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactEnquiries';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC drop table TaurusGoldLH.DimMarketingChannel


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.DimMarketingChannel
# MAGIC (
# MAGIC     MarketingChannelID INT, 
# MAGIC     MarketingChannel STRING,
# MAGIC     Brand STRING,
# MAGIC     AggComm DECIMAL(5,2),
# MAGIC     Payment DECIMAL(5,2),
# MAGIC     Verisk DECIMAL(5,2),
# MAGIC     System DECIMAL(5,2),
# MAGIC     TotalCost DECIMAL(5,2)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Dimension table: Marketing Channel Costs'
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/DimMarketingChannel';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC INSERT INTO DimMarketingChannel (MarketingChannelID, MarketingChannel, Brand, AggComm, Payment, Verisk, System, TotalCost)
# MAGIC VALUES 
# MAGIC     (0, 'Unknown','Unknown', 0.00, 0.00, 0.00, 0.00, 0.00),
# MAGIC     (1, 'Direct New','Taurus own Brands', 0.00, 1.50, 1.75, 2.00, 5.25),
# MAGIC     (2, 'Direct Renewal', 'Taurus own Brands',0.00, 1.50, 1.75, 2.00, 5.25),
# MAGIC     (3, 'Aggregator Renewal', 'Taurus own Brands',0.00, 1.50, 1.75, 2.00, 5.25),
# MAGIC     (4, 'Idol Medical', 'Taurus own Brands',39.00, 1.50, 1.75, 2.00, 44.25),
# MAGIC     (5, 'Idol Non Medical', 'Taurus own Brands',36.00, 1.50, 0.00, 2.00, 39.50),
# MAGIC     (6, 'MSM Medical', 'Taurus own Brands',39.00, 1.50, 1.75, 2.00, 44.25),
# MAGIC     (7, 'MSM Non Medical', 'Taurus own Brands',36.00, 0.00, 0.00, 2.00, 38.00),
# MAGIC 
# MAGIC     (8, 'Direct New','Oasis', 25.00, 1.50, 1.75, 2.00, 5.25),
# MAGIC     (9, 'Direct Renewal', 'Oasis',25.00, 1.50, 1.75, 2.00, 5.25),
# MAGIC     (10, 'Aggregator Renewal', 'Oasis',25.00, 1.50, 1.75, 2.00, 5.25),
# MAGIC     (11, 'Idol Medical', 'Oasis',6.00, 1.50, 1.75, 2.00, 44.25),
# MAGIC     (12, 'Idol Non Medical', 'Oasis',6.00, 1.50, 0.00, 2.00, 39.50),
# MAGIC     (13, 'MSM Medical', 'Oasis',6.00, 1.50, 1.75, 2.00, 44.25),
# MAGIC     (14, 'MSM Non Medical', 'Oasis',6.00, 0.00, 0.00, 2.00, 38.00),
# MAGIC 
# MAGIC     (15, 'Direct New','Atoz', 25.00, 1.50, 1.75, 2.00, 5.25),
# MAGIC     (16, 'Direct Renewal', 'AtoZ',25.00, 1.50, 1.75, 2.00, 5.25),
# MAGIC     (17, 'Aggregator Renewal', 'AtoZ',25.00, 1.50, 1.75, 2.00, 5.25),
# MAGIC     (18, 'Idol Medical', 'AtoZ',5.00, 1.50, 1.75, 2.00, 44.25),
# MAGIC     (19, 'Idol Non Medical', 'AtoZ',5.00, 1.50, 0.00, 2.00, 39.50),
# MAGIC     (20, 'MSM Medical', 'AtoZ',5.00, 1.50, 1.75, 2.00, 44.25),
# MAGIC     (21, 'MSM Non Medical', 'AtoZ',5.00, 0.00, 0.00, 2.00, 38.00);


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from DimMarketingChannel

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC drop table TaurusGoldLH.FactSalesAndQuotes 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.FactSalesAndQuotes
# MAGIC (
# MAGIC     DateID               INT,
# MAGIC     ChannelId            INT,
# MAGIC     AgentId              INT,
# MAGIC     CampaignId           INT,
# MAGIC     SchemeHeaderId       INT,
# MAGIC     LeadTimeGroupId      INT,
# MAGIC     DurationId           INT,
# MAGIC     AgeGroupId           INT,
# MAGIC     MedicalScoreId       INT,
# MAGIC     TotalEnquiry         BIGINT,
# MAGIC     TotalQuote           BIGINT,
# MAGIC     TotalSales           BIGINT,
# MAGIC     ConversionRate       DECIMAL(10,4),
# MAGIC     TotalGrossIncIPT     DECIMAL(18,2),
# MAGIC     TotalNetToUnderwriter DECIMAL(18,2),
# MAGIC     SourceType           STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactSalesAndQuotes';


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC drop table DimForecastByChannel 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE DimForecastByChannel (
# MAGIC     BrandID INT,
# MAGIC     `Jan-25` DECIMAL(18,4),
# MAGIC     `Feb-25` DECIMAL(18,4),
# MAGIC     `Mar-25` DECIMAL(18,4),
# MAGIC     `Apr-25` DECIMAL(18,4),
# MAGIC     `May-25` DECIMAL(18,4),
# MAGIC     `Jun-25` DECIMAL(18,4),
# MAGIC     `Jul-25` DECIMAL(18,4),
# MAGIC     `Aug-25` DECIMAL(18,4),
# MAGIC     `Sep-25` DECIMAL(18,4),
# MAGIC     `Oct-25` DECIMAL(18,6),
# MAGIC     `Nov-25` DECIMAL(18,6),
# MAGIC     `Dec-25` DECIMAL(18,6),
# MAGIC     `Jan-26` DECIMAL(18,4),
# MAGIC     `Feb-26` DECIMAL(18,4),
# MAGIC     `Mar-26` DECIMAL(18,4),
# MAGIC     `Apr-26` DECIMAL(18,4),
# MAGIC     `May-26` DECIMAL(18,4),
# MAGIC     `Jun-26` DECIMAL(18,4),
# MAGIC     `Jul-26` DECIMAL(18,4),
# MAGIC     `Aug-26` DECIMAL(18,4),
# MAGIC     `Sep-26` DECIMAL(18,4),
# MAGIC     `Oct-26` DECIMAL(18,6),
# MAGIC     `Nov-26` DECIMAL(18,6),
# MAGIC     `Dec-26` DECIMAL(18,6)
# MAGIC );
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# 1️⃣ Define all month columns (as in DimForecastByChannel)
months = [
    "Jan-25","Feb-25","Mar-25","Apr-25","May-25","Jun-25",
    "Jul-25","Aug-25","Sep-25","Oct-25","Nov-25","Dec-25",
    "Jan-26","Feb-26","Mar-26","Apr-26","May-26","Jun-26",
    "Jul-26","Aug-26","Sep-26","Oct-26","Nov-26","Dec-26"
]

# 2️⃣ Full forecast data (all brands, all months)
data = [
    {"TravelBrand": "SOIAgg", "Jan-25": 4348.05, "Feb-25": 4002.6, "Mar-25": 4732.35, "Apr-25": 3856.65, "May-25": 4375.35, "Jun-25": 4782.75, "Jul-25": 2629.2, "Aug-25": 3336.9, "Sep-25": 5096.7, "Oct-25": 3419.682515, "Nov-25": 2279.788344, "Dec-25": 2735.746012, "Jan-26": 4565.4525, "Feb-26": 4202.73, "Mar-26": 4968.9675, "Apr-26": 4049.4825, "May-26": 4594.1175, "Jun-26": 5021.8875, "Jul-26": 2760.66, "Aug-26": 3503.745, "Sep-26": 5351.535, "Oct-26": 3590.666641, "Nov-26": 2393.777761, "Dec-26": 2872.533313},
    {"TravelBrand": "SOIDirect", "Jan-25": 728.12, "Feb-25": 532.39, "Mar-25": 584.0725, "Apr-25": 488.26, "May-25": 587.9125, "Jun-25": 588.47, "Jul-25": 596.3025, "Aug-25": 512.8525, "Sep-25": 507.19, "Oct-25": 538.1659509, "Nov-25": 358.7773006, "Dec-25": 430.5327607, "Jan-26": 681.871336, "Feb-26": 495.8993526, "Mar-26": 534.1831553, "Apr-26": 451.9144408, "May-26": 543.2996568, "Jun-26": 557.9206441, "Jul-26": 551.4125436, "Aug-26": 483.0208604, "Sep-26": 498.9476821, "Oct-26": 513.3616569, "Nov-26": 342.2411046, "Dec-26": 410.6893255},
    {"TravelBrand": "TrustedAgg", "Jan-25": 1056, "Feb-25": 1056, "Mar-25": 1524, "Apr-25": 2861, "May-25": 7650, "Jun-25": 7113, "Jul-25": 9106, "Aug-25": 8419, "Sep-25": 5776, "Oct-25": 4040, "Nov-25": 2694, "Dec-25": 3232, "Jan-26": 1162, "Feb-26": 1162, "Mar-26": 1676, "Apr-26": 3147, "May-26": 8415, "Jun-26": 7825, "Jul-26": 10016, "Aug-26": 9261, "Sep-26": 6353, "Oct-26": 4444, "Nov-26": 2963, "Dec-26": 3556},
    {"TravelBrand": "TrustedDirect", "Jan-25": 2, "Feb-25": 2, "Mar-25": 8, "Apr-25": 10, "May-25": 27, "Jun-25": 38, "Jul-25": 35, "Aug-25": 46, "Sep-25": 46, "Oct-25": 20, "Nov-25": 13, "Dec-25": 16, "Jan-26": 3, "Feb-26": 3, "Mar-26": 9, "Apr-26": 9, "May-26": 29, "Jun-26": 41, "Jul-26": 39, "Aug-26": 50, "Sep-26": 48, "Oct-26": 21, "Nov-26": 14, "Dec-26": 17},
    {"TravelBrand": "StartAgg", "Jan-25": 1306, "Feb-25": 1150, "Mar-25": 1277},
    {"TravelBrand": "StartDirect", "Jan-25": 227, "Feb-25": 200, "Mar-25": 328},
    {"TravelBrand": "VivaAgg", "Jan-25": 2542, "Feb-25": 4477, "Mar-25": 5230, "Apr-25": 6138, "May-25": 4514, "Jun-25": 7156, "Jul-25": 15068, "Aug-25": 16913, "Sep-25": 14387, "Oct-25": 7193, "Nov-25": 4795, "Dec-25": 5754, "Jan-26": 2796, "Feb-26": 4925, "Mar-26": 5753, "Apr-26": 6752, "May-26": 4965, "Jun-26": 7871, "Jul-26": 16575, "Aug-26": 18604, "Sep-26": 15826, "Oct-26": 7912, "Nov-26": 5275, "Dec-26": 6329},
    {"TravelBrand": "VivaDirect", "Jan-25": 139, "Feb-25": 139, "Mar-25": 175, "Apr-25": 136, "May-25": 167, "Jun-25": 217, "Jul-25": 203, "Aug-25": 211, "Sep-25": 216, "Oct-25": 147, "Nov-25": 98, "Dec-25": 118, "Jan-26": 139, "Feb-26": 139, "Mar-26": 175, "Apr-26": 137, "May-26": 167, "Jun-26": 218, "Jul-26": 205, "Aug-26": 214, "Sep-26": 218, "Oct-26": 148, "Nov-26": 99, "Dec-26": 119},
    {"TravelBrand": "OasisAgg", "Jan-25": 1370, "Feb-25": 1967, "Mar-25": 2175, "Apr-25": 2443, "May-25": 8610, "Jun-25": 11374, "Jul-25": 10768, "Aug-25": 10735, "Sep-25": 11418, "Oct-25": 5601, "Nov-25": 3734, "Dec-25": 4480, "Jan-26": 1439, "Feb-26": 2065, "Mar-26": 2283, "Apr-26": 2566, "May-26": 9041, "Jun-26": 11942, "Jul-26": 11306, "Aug-26": 11272, "Sep-26": 11989, "Oct-26": 5881, "Nov-26": 3920, "Dec-26": 4704},
    {"TravelBrand": "OasisDirect", "Jan-25": 696, "Feb-25": 617, "Mar-25": 707, "Apr-25": 595, "May-25": 897, "Jun-25": 1007, "Jul-25": 1105, "Aug-25": 860, "Sep-25": 917, "Oct-25": 681, "Nov-25": 454, "Dec-25": 545, "Jan-26": 657, "Feb-26": 588, "Mar-26": 686, "Apr-26": 599, "May-26": 926, "Jun-26": 1067, "Jul-26": 1132, "Aug-26": 919, "Sep-26": 986, "Oct-26": 696, "Nov-26": 464, "Dec-26": 557},
    {"TravelBrand": "AtozAgg", "Jan-25": 872, "Feb-25": 992, "Mar-25": 1131, "Apr-25": 1385, "May-25": 2788, "Jun-25": 3898, "Jul-25": 3793, "Aug-25": 3807, "Sep-25": 4407, "Oct-25": 2015, "Nov-25": 1344, "Dec-25": 1612, "Jan-26": 916, "Feb-26": 1042, "Mar-26": 1187, "Apr-26": 1454, "May-26": 2927, "Jun-26": 4092, "Jul-26": 3982, "Aug-26": 3998, "Sep-26": 4627, "Oct-26": 2116, "Nov-26": 1411, "Dec-26": 1693},
    {"TravelBrand": "AtozDirect", "Jan-25": 68, "Feb-25": 25, "Mar-25": 55, "Apr-25": 52, "May-25": 103, "Jun-25": 298, "Jul-25": 376, "Aug-25": 367, "Sep-25": 385, "Oct-25": 159, "Nov-25": 106, "Dec-25": 127, "Jan-26": 71, "Feb-26": 29, "Mar-26": 57, "Apr-26": 57, "May-26": 118, "Jun-26": 332, "Jul-26": 409, "Aug-26": 410, "Sep-26": 436, "Oct-26": 176, "Nov-26": 117, "Dec-26": 141}
]

# 3️⃣ Convert all numbers to float
for row in data:
    for k, v in row.items():
        if k != "TravelBrand" and v is not None:
            row[k] = float(v)

# 4️⃣ Create DataFrame
df_forecast = spark.createDataFrame(data)

# 5️⃣ Join with DimTravelBrand to get BrandID
df_dim = spark.sql("SELECT TravelBrandID AS BrandID, TravelBrand FROM DimTravelBrand")

df_joined = (
    df_forecast.join(df_dim, on="TravelBrand", how="inner")
    .drop("TravelBrand")
)

# 6️⃣ Cast all columns to match DECIMAL types
for m in months:
    if m in df_joined.columns:
        scale = 6 if m.endswith(("Oct-25","Nov-25","Dec-25","Oct-26","Nov-26","Dec-26")) else 4
        df_joined = df_joined.withColumn(m, col(m).cast(DecimalType(18, scale)))

# 7️⃣ Reorder columns to match table schema
cols_final = ["BrandID"] + [m for m in months if m in df_joined.columns]
df_final = df_joined.select(*cols_final)

# 8️⃣ Write to table
df_final.write.mode("append").saveAsTable("DimForecastByChannel")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from DimForecastByChannel 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC -- drop table FactForecastByChannel

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC 
# MAGIC select * from TaurusGoldLH.FactForecastByChannel f


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC 
# MAGIC select distinct b.TravelBrandId,b.TravelBrand from TaurusGoldLH.FactForecastByChannel f
# MAGIC join DimTravelBrand b on f.TravelBrandId=b.TravelBrandId

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE FactForecastByChannel (
# MAGIC     TravelBrandId INT,
# MAGIC     DateId INT,
# MAGIC     ForecastVolume DECIMAL(18,4)
# MAGIC );
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, udf, concat_ws, lpad
from pyspark.sql.types import DecimalType, StringType, IntegerType
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

# 1️⃣ Full forecast dataset (Jan-25 to Dec-26)
data = [
    {"TravelBrand": "SOIAgg", "Jan-25": 4348.05, "Feb-25": 4002.6, "Mar-25": 4732.35, "Apr-25": 3856.65, "May-25": 4375.35, "Jun-25": 4782.75, "Jul-25": 2629.2, "Aug-25": 3336.9, "Sep-25": 5096.7, "Oct-25": 3419.682515, "Nov-25": 2279.788344, "Dec-25": 2735.746012, "Jan-26": 4565.4525, "Feb-26": 4202.73, "Mar-26": 4968.9675, "Apr-26": 4049.4825, "May-26": 4594.1175, "Jun-26": 5021.8875, "Jul-26": 2760.66, "Aug-26": 3503.745, "Sep-26": 5351.535, "Oct-26": 3590.666641, "Nov-26": 2393.777761, "Dec-26": 2872.533313},
    {"TravelBrand": "SOIDirect", "Jan-25": 728.12, "Feb-25": 532.39, "Mar-25": 584.0725, "Apr-25": 488.26, "May-25": 587.9125, "Jun-25": 588.47, "Jul-25": 596.3025, "Aug-25": 512.8525, "Sep-25": 507.19, "Oct-25": 538.1659509, "Nov-25": 358.7773006, "Dec-25": 430.5327607, "Jan-26": 681.871336, "Feb-26": 495.8993526, "Mar-26": 534.1831553, "Apr-26": 451.9144408, "May-26": 543.2996568, "Jun-26": 557.9206441, "Jul-26": 551.4125436, "Aug-26": 483.0208604, "Sep-26": 498.9476821, "Oct-26": 513.3616569, "Nov-26": 342.2411046, "Dec-26": 410.6893255},
    {"TravelBrand": "TrustedAgg", "Jan-25": 1056, "Feb-25": 1056, "Mar-25": 1524, "Apr-25": 2861, "May-25": 7650, "Jun-25": 7113, "Jul-25": 9106, "Aug-25": 8419, "Sep-25": 5776, "Oct-25": 4040, "Nov-25": 2694, "Dec-25": 3232, "Jan-26": 1162, "Feb-26": 1162, "Mar-26": 1676, "Apr-26": 3147, "May-26": 8415, "Jun-26": 7825, "Jul-26": 10016, "Aug-26": 9261, "Sep-26": 6353, "Oct-26": 4444, "Nov-26": 2963, "Dec-26": 3556},
    {"TravelBrand": "TrustedDirect", "Jan-25": 2, "Feb-25": 2, "Mar-25": 8, "Apr-25": 10, "May-25": 27, "Jun-25": 38, "Jul-25": 35, "Aug-25": 46, "Sep-25": 46, "Oct-25": 20, "Nov-25": 13, "Dec-25": 16, "Jan-26": 3, "Feb-26": 3, "Mar-26": 9, "Apr-26": 9, "May-26": 29, "Jun-26": 41, "Jul-26": 39, "Aug-26": 50, "Sep-26": 48, "Oct-26": 21, "Nov-26": 14, "Dec-26": 17},
    {"TravelBrand": "StartAgg", "Jan-25": 1306, "Feb-25": 1150, "Mar-25": 1277},
    {"TravelBrand": "StartDirect", "Jan-25": 227, "Feb-25": 200, "Mar-25": 328},
    {"TravelBrand": "VivaAgg", "Jan-25": 2542, "Feb-25": 4477, "Mar-25": 5230, "Apr-25": 6138, "May-25": 4514, "Jun-25": 7156, "Jul-25": 15068, "Aug-25": 16913, "Sep-25": 14387, "Oct-25": 7193, "Nov-25": 4795, "Dec-25": 5754, "Jan-26": 2796, "Feb-26": 4925, "Mar-26": 5753, "Apr-26": 6752, "May-26": 4965, "Jun-26": 7871, "Jul-26": 16575, "Aug-26": 18604, "Sep-26": 15826, "Oct-26": 7912, "Nov-26": 5275, "Dec-26": 6329},
    {"TravelBrand": "VivaDirect", "Jan-25": 139, "Feb-25": 139, "Mar-25": 175, "Apr-25": 136, "May-25": 167, "Jun-25": 217, "Jul-25": 203, "Aug-25": 211, "Sep-25": 216, "Oct-25": 147, "Nov-25": 98, "Dec-25": 118, "Jan-26": 139, "Feb-26": 139, "Mar-26": 175, "Apr-26": 137, "May-26": 167, "Jun-26": 218, "Jul-26": 205, "Aug-26": 214, "Sep-26": 218, "Oct-26": 148, "Nov-26": 99, "Dec-26": 119},
    {"TravelBrand": "OasisAgg", "Jan-25": 1370, "Feb-25": 1967, "Mar-25": 2175, "Apr-25": 2443, "May-25": 8610, "Jun-25": 11374, "Jul-25": 10768, "Aug-25": 10735, "Sep-25": 11418, "Oct-25": 5601, "Nov-25": 3734, "Dec-25": 4480, "Jan-26": 1439, "Feb-26": 2065, "Mar-26": 2283, "Apr-26": 2566, "May-26": 9041, "Jun-26": 11942, "Jul-26": 11306, "Aug-26": 11272, "Sep-26": 11989, "Oct-26": 5881, "Nov-26": 3920, "Dec-26": 4704},
    {"TravelBrand": "OasisDirect", "Jan-25": 696, "Feb-25": 617, "Mar-25": 707, "Apr-25": 595, "May-25": 897, "Jun-25": 1007, "Jul-25": 1105, "Aug-25": 860, "Sep-25": 917, "Oct-25": 681, "Nov-25": 454, "Dec-25": 545, "Jan-26": 657, "Feb-26": 588, "Mar-26": 686, "Apr-26": 599, "May-26": 926, "Jun-26": 1067, "Jul-26": 1132, "Aug-26": 919, "Sep-26": 986, "Oct-26": 696, "Nov-26": 464, "Dec-26": 557},
    {"TravelBrand": "AtozAgg", "Jan-25": 872, "Feb-25": 992, "Mar-25": 1131, "Apr-25": 1385, "May-25": 2788, "Jun-25": 3898, "Jul-25": 3793, "Aug-25": 3807, "Sep-25": 4407, "Oct-25": 2015, "Nov-25": 1344, "Dec-25": 1612, "Jan-26": 916, "Feb-26": 1042, "Mar-26": 1187, "Apr-26": 1454, "May-26": 2927, "Jun-26": 4092, "Jul-26": 3982, "Aug-26": 3998, "Sep-26": 4627, "Oct-26": 2116, "Nov-26": 1411, "Dec-26": 1693},
    {"TravelBrand": "AtozDirect", "Jan-25": 68, "Feb-25": 25, "Mar-25": 55, "Apr-25": 52, "May-25": 103, "Jun-25": 298, "Jul-25": 376, "Aug-25": 367, "Sep-25": 385, "Oct-25": 159, "Nov-25": 106, "Dec-25": 127, "Jan-26": 71, "Feb-26": 29, "Mar-26": 57, "Apr-26": 57, "May-26": 118, "Jun-26": 332, "Jul-26": 409, "Aug-26": 410, "Sep-26": 436, "Oct-26": 176, "Nov-26": 117, "Dec-26": 141}
]

# 2️⃣ Convert all numeric values to float (avoid LongType)
for row in data:
    for k, v in row.items():
        if k != "TravelBrand" and v is not None:
            row[k] = float(v)

# 3️⃣ Create DataFrame
df = spark.createDataFrame(data)

# 4️⃣ Join with DimTravelBrand for IDs
dim_brand = spark.sql("SELECT TravelBrandId, TravelBrand FROM DimTravelBrand")
df = df.join(dim_brand, on="TravelBrand", how="inner")

# 5️⃣ Unpivot (wide → long)
month_cols = [c for c in df.columns if '-' in c]
pairs = ", ".join([f"'{m}', `{m}`" for m in month_cols])
stack_expr = f"stack({len(month_cols)}, {pairs}) as (MonthName, ForecastVolume)"
df_unpivot = df.select("TravelBrandId", expr(stack_expr))

# 6️⃣ Convert MonthName → YearMonth (YYYY-MM)
month_map = {'Jan': '01','Feb': '02','Mar': '03','Apr': '04','May': '05','Jun': '06',
             'Jul': '07','Aug': '08','Sep': '09','Oct': '10','Nov': '11','Dec': '12'}

def convert_period(m):
    try:
        mon, yr = m.split('-')
        return f"20{yr}-{month_map[mon]}"
    except:
        return None

convert_period_udf = udf(convert_period, StringType())
df_unpivot = df_unpivot.withColumn("YearMonth", convert_period_udf(col("MonthName")))

# 7️⃣ Prepare DimDate YearMonth mapping (using Date column)
dim_date = (
    spark.table("DimDate")
    .withColumn("YearMonth", concat_ws("-", col("Year").cast("string"), lpad(col("Month"), 2, "0")))
    .groupBy("YearMonth")
    .agg(F.min("DateId").alias("DateId"))  # first DateId of month
)

# 8️⃣ Join Forecast with DimDate
df_joined = (
    df_unpivot.join(dim_date, on="YearMonth", how="left")
    .drop("YearMonth", "MonthName")
)

# 9️⃣ Cast ForecastVolume + DateId properly
df_final = (
    df_joined
    .withColumn("ForecastVolume", col("ForecastVolume").cast(DecimalType(18,4)))
    .withColumn("DateId", col("DateId").cast(IntegerType()))
)

# 🔟 Select final columns
df_final = df_final.select("TravelBrandId", "DateId", "ForecastVolume")

# ✅ Optional: Preview
display(df_final.limit(10))

# 11️⃣ Insert with schema alignment
df_final.write.option("mergeSchema", "true").mode("append").saveAsTable("FactForecastByChannel")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS TaurusGoldLH.FactSalesQuoteAnalysis
# MAGIC (
# MAGIC     DateID               INT,
# MAGIC     DurationId           INT,
# MAGIC     
# MAGIC     TotalEnquiry         BIGINT,
# MAGIC     TotalQuote           BIGINT,
# MAGIC     TotalSales           BIGINT,
# MAGIC     ConversionRate       DECIMAL(10,4),
# MAGIC     TotalGrossIncIPT     DECIMAL(18,2),
# MAGIC     TotalNetToUnderwriter DECIMAL(18,2)
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://Taurus_TravelInsurance_Dev@onelake.dfs.fabric.microsoft.com/TaurusGoldLH.Lakehouse/Tables/FactSalesQuoteAnalysis';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# New Forecast table by Brand****

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC drop table FactForecastByBrand

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE FactForecastByBrand (
# MAGIC     BrandId INT,
# MAGIC     SubBrandId INT,
# MAGIC     DateId INT,
# MAGIC     ForecastVolume DECIMAL(18,4)
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, udf, concat_ws, lpad
from pyspark.sql.types import DecimalType, StringType, IntegerType
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

data = [
    {"BrandId": 11, "SubBrandId": 7, "Jan-25": 4348.05, "Feb-25": 4002.6, "Mar-25": 4732.35, "Apr-25": 3856.65, "May-25": 4375.35, "Jun-25": 4782.75, "Jul-25": 2629.2, "Aug-25": 3336.9, "Sep-25": 5096.7, "Oct-25": 3419.682515, "Nov-25": 2279.788344, "Dec-25": 2735.746012, "Jan-26": 4565.4525, "Feb-26": 4202.73, "Mar-26": 4968.9675, "Apr-26": 4049.4825, "May-26": 4594.1175, "Jun-26": 5021.8875, "Jul-26": 2760.66, "Aug-26": 3503.745, "Sep-26": 5351.535, "Oct-26": 3590.666641, "Nov-26": 2393.777761, "Dec-26": 2872.533313},
    {"BrandId": 11, "SubBrandId": 9, "Jan-25": 728.12, "Feb-25": 532.39, "Mar-25": 584.0725, "Apr-25": 488.26, "May-25": 587.9125, "Jun-25": 588.47, "Jul-25": 596.3025, "Aug-25": 512.8525, "Sep-25": 507.19, "Oct-25": 538.1659509, "Nov-25": 358.7773006, "Dec-25": 430.5327607, "Jan-26": 681.871336, "Feb-26": 495.8993526, "Mar-26": 534.1831553, "Apr-26": 451.9144408, "May-26": 543.2996568, "Jun-26": 557.9206441, "Jul-26": 551.4125436, "Aug-26": 483.0208604, "Sep-26": 498.9476821, "Oct-26": 513.3616569, "Nov-26": 342.2411046, "Dec-26": 410.6893255},
    {"BrandId": 12, "SubBrandId": 17, "Jan-25": 1056, "Feb-25": 1056, "Mar-25": 1524, "Apr-25": 2861, "May-25": 7650, "Jun-25": 7113, "Jul-25": 9106, "Aug-25": 8419, "Sep-25": 5776, "Oct-25": 4040, "Nov-25": 2694, "Dec-25": 3232, "Jan-26": 1162, "Feb-26": 1162, "Mar-26": 1676, "Apr-26": 3147, "May-26": 8415, "Jun-26": 7825, "Jul-26": 10016, "Aug-26": 9261, "Sep-26": 6353, "Oct-26": 4444, "Nov-26": 2963, "Dec-26": 3556},
    {"BrandId": 12, "SubBrandId": 1, "Jan-25": 2, "Feb-25": 2, "Mar-25": 8, "Apr-25": 10, "May-25": 27, "Jun-25": 38, "Jul-25": 35, "Aug-25": 46, "Sep-25": 46, "Oct-25": 20, "Nov-25": 13, "Dec-25": 16, "Jan-26": 3, "Feb-26": 3, "Mar-26": 9, "Apr-26": 9, "May-26": 29, "Jun-26": 41, "Jul-26": 39, "Aug-26": 50, "Sep-26": 48, "Oct-26": 21, "Nov-26": 14, "Dec-26": 17},
    {"BrandId": 10, "SubBrandId": 19, "Jan-25": 1306, "Feb-25": 1150, "Mar-25": 1277},
    {"BrandId": 10, "SubBrandId": 20, "Jan-25": 227, "Feb-25": 200, "Mar-25": 328},
    {"BrandId": 13, "SubBrandId": 15, "Jan-25": 2542, "Feb-25": 4477, "Mar-25": 5230, "Apr-25": 6138, "May-25": 4514, "Jun-25": 7156, "Jul-25": 15068, "Aug-25": 16913, "Sep-25": 14387, "Oct-25": 7193, "Nov-25": 4795, "Dec-25": 5754, "Jan-26": 2796, "Feb-26": 4925, "Mar-26": 5753, "Apr-26": 6752, "May-26": 4965, "Jun-26": 7871, "Jul-26": 16575, "Aug-26": 18604, "Sep-26": 15826, "Oct-26": 7912, "Nov-26": 5275, "Dec-26": 6329},
    {"BrandId": 13, "SubBrandId": 16, "Jan-25": 139, "Feb-25": 139, "Mar-25": 175, "Apr-25": 136, "May-25": 167, "Jun-25": 217, "Jul-25": 203, "Aug-25": 211, "Sep-25": 216, "Oct-25": 147, "Nov-25": 98, "Dec-25": 118, "Jan-26": 139, "Feb-26": 139, "Mar-26": 175, "Apr-26": 137, "May-26": 167, "Jun-26": 218, "Jul-26": 205, "Aug-26": 214, "Sep-26": 218, "Oct-26": 148, "Nov-26": 99, "Dec-26": 119},
    {"BrandId": 14, "SubBrandId": 4, "Jan-25": 1370, "Feb-25": 1967, "Mar-25": 2175, "Apr-25": 2443, "May-25": 8610, "Jun-25": 11374, "Jul-25": 10768, "Aug-25": 10735, "Sep-25": 11418, "Oct-25": 5601, "Nov-25": 3734, "Dec-25": 4480, "Jan-26": 1439, "Feb-26": 2065, "Mar-26": 2283, "Apr-26": 2566, "May-26": 9041, "Jun-26": 11942, "Jul-26": 11306, "Aug-26": 11272, "Sep-26": 11989, "Oct-26": 5881, "Nov-26": 3920, "Dec-26": 4704},
    {"BrandId": 14, "SubBrandId": 5, "Jan-25": 696, "Feb-25": 617, "Mar-25": 707, "Apr-25": 595, "May-25": 897, "Jun-25": 1007, "Jul-25": 1105, "Aug-25": 860, "Sep-25": 917, "Oct-25": 681, "Nov-25": 454, "Dec-25": 545, "Jan-26": 657, "Feb-26": 588, "Mar-26": 686, "Apr-26": 599, "May-26": 926, "Jun-26": 1067, "Jul-26": 1132, "Aug-26": 919, "Sep-26": 986, "Oct-26": 696, "Nov-26": 464, "Dec-26": 557},
    {"BrandId": 8, "SubBrandId": 18, "Jan-25": 872, "Feb-25": 992, "Mar-25": 1131, "Apr-25": 1385, "May-25": 2788, "Jun-25": 3898, "Jul-25": 3793, "Aug-25": 3807, "Sep-25": 4407, "Oct-25": 2015, "Nov-25": 1344, "Dec-25": 1612, "Jan-26": 916, "Feb-26": 1042, "Mar-26": 1187, "Apr-26": 1454, "May-26": 2927, "Jun-26": 4092, "Jul-26": 3982, "Aug-26": 3998, "Sep-26": 4627, "Oct-26": 2116, "Nov-26": 1411, "Dec-26": 1693},
    {"BrandId": 8, "SubBrandId": 2, "Jan-25": 68, "Feb-25": 25, "Mar-25": 55, "Apr-25": 52, "May-25": 103, "Jun-25": 298, "Jul-25": 376, "Aug-25": 367, "Sep-25": 385, "Oct-25": 159, "Nov-25": 106, "Dec-25": 127, "Jan-26": 71, "Feb-26": 29, "Mar-26": 57, "Apr-26": 57, "May-26": 118, "Jun-26": 332, "Jul-26": 409, "Aug-26": 410, "Sep-26": 436, "Oct-26": 176, "Nov-26": 117, "Dec-26": 141}
]

# 2️⃣ Convert all numeric values to float
for row in data:
    for k, v in row.items():
        if k not in ["BrandId", "SubBrandId"] and v is not None:
            row[k] = float(v)

# 3️⃣ Create DataFrame
df = spark.createDataFrame(data)

# 4️⃣ Unpivot (wide → long)
month_cols = [c for c in df.columns if '-' in c]
pairs = ", ".join([f"'{m}', `{m}`" for m in month_cols])
stack_expr = f"stack({len(month_cols)}, {pairs}) as (MonthName, ForecastVolume)"
df_unpivot = df.select("BrandId", "SubBrandId", expr(stack_expr))

# 5️⃣ Convert MonthName → YearMonth (YYYY-MM)
month_map = {'Jan': '01','Feb': '02','Mar': '03','Apr': '04','May': '05','Jun': '06',
             'Jul': '07','Aug': '08','Sep': '09','Oct': '10','Nov': '11','Dec': '12'}

def convert_period(m):
    try:
        mon, yr = m.split('-')
        return f"20{yr}-{month_map[mon]}"
    except:
        return None

convert_period_udf = udf(convert_period, StringType())
df_unpivot = df_unpivot.withColumn("YearMonth", convert_period_udf(col("MonthName")))

# 6️⃣ Prepare DimDate YearMonth mapping
dim_date = (
    spark.table("DimDate")
    .withColumn("YearMonth", concat_ws("-", col("Year").cast("string"), lpad(col("Month"), 2, "0")))
    .groupBy("YearMonth")
    .agg(F.min("DateId").alias("DateId"))
)

# 7️⃣ Join Forecast with DimDate
df_joined = (
    df_unpivot.join(dim_date, on="YearMonth", how="left")
    .drop("YearMonth", "MonthName")
)

# 8️⃣ Get the schema of the existing table
existing_schema = spark.table("FactForecastByBrand").schema

# 9️⃣ Cast columns to match the existing table schema
df_final = df_joined
for field in existing_schema:
    if field.name in df_final.columns:
        df_final = df_final.withColumn(field.name, col(field.name).cast(field.dataType))

# 🔟 Select final columns in the same order as the existing table
df_final = df_final.select(*[field.name for field in existing_schema])

# 1️⃣1️⃣ Insert into FactForecastByBrand
df_final.write.mode("append").saveAsTable("FactForecastByBrand")

# ✅ Preview
display(df_final.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from TaurusGoldLH.FactForecastByBrand

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, udf, concat_ws, lpad
from pyspark.sql.types import DecimalType, StringType, IntegerType
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

# 1️⃣ Full forecast dataset (Jan-25 to Dec-26)
data = [
    {"TravelBrand": 11, 7, "Jan-25": 4348.05, "Feb-25": 4002.6, "Mar-25": 4732.35, "Apr-25": 3856.65, "May-25": 4375.35, "Jun-25": 4782.75, "Jul-25": 2629.2, "Aug-25": 3336.9, "Sep-25": 5096.7, "Oct-25": 3419.682515, "Nov-25": 2279.788344, "Dec-25": 2735.746012, "Jan-26": 4565.4525, "Feb-26": 4202.73, "Mar-26": 4968.9675, "Apr-26": 4049.4825, "May-26": 4594.1175, "Jun-26": 5021.8875, "Jul-26": 2760.66, "Aug-26": 3503.745, "Sep-26": 5351.535, "Oct-26": 3590.666641, "Nov-26": 2393.777761, "Dec-26": 2872.533313},
    {"TravelBrand": 11, 9, "Jan-25": 728.12, "Feb-25": 532.39, "Mar-25": 584.0725, "Apr-25": 488.26, "May-25": 587.9125, "Jun-25": 588.47, "Jul-25": 596.3025, "Aug-25": 512.8525, "Sep-25": 507.19, "Oct-25": 538.1659509, "Nov-25": 358.7773006, "Dec-25": 430.5327607, "Jan-26": 681.871336, "Feb-26": 495.8993526, "Mar-26": 534.1831553, "Apr-26": 451.9144408, "May-26": 543.2996568, "Jun-26": 557.9206441, "Jul-26": 551.4125436, "Aug-26": 483.0208604, "Sep-26": 498.9476821, "Oct-26": 513.3616569, "Nov-26": 342.2411046, "Dec-26": 410.6893255},
    {"TravelBrand": 12, 17, "Jan-25": 1056, "Feb-25": 1056, "Mar-25": 1524, "Apr-25": 2861, "May-25": 7650, "Jun-25": 7113, "Jul-25": 9106, "Aug-25": 8419, "Sep-25": 5776, "Oct-25": 4040, "Nov-25": 2694, "Dec-25": 3232, "Jan-26": 1162, "Feb-26": 1162, "Mar-26": 1676, "Apr-26": 3147, "May-26": 8415, "Jun-26": 7825, "Jul-26": 10016, "Aug-26": 9261, "Sep-26": 6353, "Oct-26": 4444, "Nov-26": 2963, "Dec-26": 3556},
    {"TravelBrand": 12, 1, "Jan-25": 2, "Feb-25": 2, "Mar-25": 8, "Apr-25": 10, "May-25": 27, "Jun-25": 38, "Jul-25": 35, "Aug-25": 46, "Sep-25": 46, "Oct-25": 20, "Nov-25": 13, "Dec-25": 16, "Jan-26": 3, "Feb-26": 3, "Mar-26": 9, "Apr-26": 9, "May-26": 29, "Jun-26": 41, "Jul-26": 39, "Aug-26": 50, "Sep-26": 48, "Oct-26": 21, "Nov-26": 14, "Dec-26": 17},
    {"TravelBrand": 10, 19, "Jan-25": 1306, "Feb-25": 1150, "Mar-25": 1277},
    {"TravelBrand": 10, 20, "Jan-25": 227, "Feb-25": 200, "Mar-25": 328},
    {"TravelBrand": 13, 15, "Jan-25": 2542, "Feb-25": 4477, "Mar-25": 5230, "Apr-25": 6138, "May-25": 4514, "Jun-25": 7156, "Jul-25": 15068, "Aug-25": 16913, "Sep-25": 14387, "Oct-25": 7193, "Nov-25": 4795, "Dec-25": 5754, "Jan-26": 2796, "Feb-26": 4925, "Mar-26": 5753, "Apr-26": 6752, "May-26": 4965, "Jun-26": 7871, "Jul-26": 16575, "Aug-26": 18604, "Sep-26": 15826, "Oct-26": 7912, "Nov-26": 5275, "Dec-26": 6329},
    {"TravelBrand": 13, 16, "Jan-25": 139, "Feb-25": 139, "Mar-25": 175, "Apr-25": 136, "May-25": 167, "Jun-25": 217, "Jul-25": 203, "Aug-25": 211, "Sep-25": 216, "Oct-25": 147, "Nov-25": 98, "Dec-25": 118, "Jan-26": 139, "Feb-26": 139, "Mar-26": 175, "Apr-26": 137, "May-26": 167, "Jun-26": 218, "Jul-26": 205, "Aug-26": 214, "Sep-26": 218, "Oct-26": 148, "Nov-26": 99, "Dec-26": 119},
    {"TravelBrand": 14, 4, "Jan-25": 1370, "Feb-25": 1967, "Mar-25": 2175, "Apr-25": 2443, "May-25": 8610, "Jun-25": 11374, "Jul-25": 10768, "Aug-25": 10735, "Sep-25": 11418, "Oct-25": 5601, "Nov-25": 3734, "Dec-25": 4480, "Jan-26": 1439, "Feb-26": 2065, "Mar-26": 2283, "Apr-26": 2566, "May-26": 9041, "Jun-26": 11942, "Jul-26": 11306, "Aug-26": 11272, "Sep-26": 11989, "Oct-26": 5881, "Nov-26": 3920, "Dec-26": 4704},
    {"TravelBrand": 14, 5, "Jan-25": 696, "Feb-25": 617, "Mar-25": 707, "Apr-25": 595, "May-25": 897, "Jun-25": 1007, "Jul-25": 1105, "Aug-25": 860, "Sep-25": 917, "Oct-25": 681, "Nov-25": 454, "Dec-25": 545, "Jan-26": 657, "Feb-26": 588, "Mar-26": 686, "Apr-26": 599, "May-26": 926, "Jun-26": 1067, "Jul-26": 1132, "Aug-26": 919, "Sep-26": 986, "Oct-26": 696, "Nov-26": 464, "Dec-26": 557},
    {"TravelBrand": 8, 18, "Jan-25": 872, "Feb-25": 992, "Mar-25": 1131, "Apr-25": 1385, "May-25": 2788, "Jun-25": 3898, "Jul-25": 3793, "Aug-25": 3807, "Sep-25": 4407, "Oct-25": 2015, "Nov-25": 1344, "Dec-25": 1612, "Jan-26": 916, "Feb-26": 1042, "Mar-26": 1187, "Apr-26": 1454, "May-26": 2927, "Jun-26": 4092, "Jul-26": 3982, "Aug-26": 3998, "Sep-26": 4627, "Oct-26": 2116, "Nov-26": 1411, "Dec-26": 1693},
    {"TravelBrand": 8, 2, "Jan-25": 68, "Feb-25": 25, "Mar-25": 55, "Apr-25": 52, "May-25": 103, "Jun-25": 298, "Jul-25": 376, "Aug-25": 367, "Sep-25": 385, "Oct-25": 159, "Nov-25": 106, "Dec-25": 127, "Jan-26": 71, "Feb-26": 29, "Mar-26": 57, "Apr-26": 57, "May-26": 118, "Jun-26": 332, "Jul-26": 409, "Aug-26": 410, "Sep-26": 436, "Oct-26": 176, "Nov-26": 117, "Dec-26": 141}
]

# 2️⃣ Convert all numeric values to float (avoid LongType)
for row in data:
    for k, v in row.items():
        if k != "TravelBrand" and v is not None:
            row[k] = float(v)

# 3️⃣ Create DataFrame
df = spark.createDataFrame(data)

# 4️⃣ Join with DimTravelBrand for IDs
dim_brand = spark.sql("SELECT TravelBrandId, TravelBrand FROM DimTravelBrand")
df = df.join(dim_brand, on="TravelBrand", how="inner")

# 5️⃣ Unpivot (wide → long)
month_cols = [c for c in df.columns if '-' in c]
pairs = ", ".join([f"'{m}', `{m}`" for m in month_cols])
stack_expr = f"stack({len(month_cols)}, {pairs}) as (MonthName, ForecastVolume)"
df_unpivot = df.select("TravelBrandId", expr(stack_expr))

# 6️⃣ Convert MonthName → YearMonth (YYYY-MM)
month_map = {'Jan': '01','Feb': '02','Mar': '03','Apr': '04','May': '05','Jun': '06',
             'Jul': '07','Aug': '08','Sep': '09','Oct': '10','Nov': '11','Dec': '12'}

def convert_period(m):
    try:
        mon, yr = m.split('-')
        return f"20{yr}-{month_map[mon]}"
    except:
        return None

convert_period_udf = udf(convert_period, StringType())
df_unpivot = df_unpivot.withColumn("YearMonth", convert_period_udf(col("MonthName")))

# 7️⃣ Prepare DimDate YearMonth mapping (using Date column)
dim_date = (
    spark.table("DimDate")
    .withColumn("YearMonth", concat_ws("-", col("Year").cast("string"), lpad(col("Month"), 2, "0")))
    .groupBy("YearMonth")
    .agg(F.min("DateId").alias("DateId"))  # first DateId of month
)

# 8️⃣ Join Forecast with DimDate
df_joined = (
    df_unpivot.join(dim_date, on="YearMonth", how="left")
    .drop("YearMonth", "MonthName")
)

# 9️⃣ Cast ForecastVolume + DateId properly
df_final = (
    df_joined
    .withColumn("ForecastVolume", col("ForecastVolume").cast(DecimalType(18,4)))
    .withColumn("DateId", col("DateId").cast(IntegerType()))
)

# 🔟 Select final columns
df_final = df_final.select("TravelBrandId", "DateId", "ForecastVolume")

# ✅ Optional: Preview
display(df_final.limit(10))

# 11️⃣ Insert with schema alignment
df_final.write.option("mergeSchema", "true").mode("append").saveAsTable("FactForecastByChannel")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from FactForecastByBrand 
# MAGIC 
# MAGIC -- # data = [
# MAGIC -- #     {"TravelBrand": "switched_on","SOIAgg", "Jan-25": 4348.05, "Feb-25": 4002.6, "Mar-25": 4732.35, "Apr-25": 3856.65, "May-25": 4375.35, "Jun-25": 4782.75, "Jul-25": 2629.2, "Aug-25": 3336.9, "Sep-25": 5096.7, "Oct-25": 3419.682515, "Nov-25": 2279.788344, "Dec-25": 2735.746012, "Jan-26": 4565.4525, "Feb-26": 4202.73, "Mar-26": 4968.9675, "Apr-26": 4049.4825, "May-26": 4594.1175, "Jun-26": 5021.8875, "Jul-26": 2760.66, "Aug-26": 3503.745, "Sep-26": 5351.535, "Oct-26": 3590.666641, "Nov-26": 2393.777761, "Dec-26": 2872.533313},
# MAGIC -- #     {"TravelBrand": "switched_on","SOIDirect", "Jan-25": 728.12, "Feb-25": 532.39, "Mar-25": 584.0725, "Apr-25": 488.26, "May-25": 587.9125, "Jun-25": 588.47, "Jul-25": 596.3025, "Aug-25": 512.8525, "Sep-25": 507.19, "Oct-25": 538.1659509, "Nov-25": 358.7773006, "Dec-25": 430.5327607, "Jan-26": 681.871336, "Feb-26": 495.8993526, "Mar-26": 534.1831553, "Apr-26": 451.9144408, "May-26": 543.2996568, "Jun-26": 557.9206441, "Jul-26": 551.4125436, "Aug-26": 483.0208604, "Sep-26": 498.9476821, "Oct-26": 513.3616569, "Nov-26": 342.2411046, "Dec-26": 410.6893255},
# MAGIC -- #     {"TravelBrand": "trusted_ins","TrustedAgg", "Jan-25": 1056, "Feb-25": 1056, "Mar-25": 1524, "Apr-25": 2861, "May-25": 7650, "Jun-25": 7113, "Jul-25": 9106, "Aug-25": 8419, "Sep-25": 5776, "Oct-25": 4040, "Nov-25": 2694, "Dec-25": 3232, "Jan-26": 1162, "Feb-26": 1162, "Mar-26": 1676, "Apr-26": 3147, "May-26": 8415, "Jun-26": 7825, "Jul-26": 10016, "Aug-26": 9261, "Sep-26": 6353, "Oct-26": 4444, "Nov-26": 2963, "Dec-26": 3556},
# MAGIC -- #     {"TravelBrand": "trusted_ins","TrustedDirect", "Jan-25": 2, "Feb-25": 2, "Mar-25": 8, "Apr-25": 10, "May-25": 27, "Jun-25": 38, "Jul-25": 35, "Aug-25": 46, "Sep-25": 46, "Oct-25": 20, "Nov-25": 13, "Dec-25": 16, "Jan-26": 3, "Feb-26": 3, "Mar-26": 9, "Apr-26": 9, "May-26": 29, "Jun-26": 41, "Jul-26": 39, "Aug-26": 50, "Sep-26": 48, "Oct-26": 21, "Nov-26": 14, "Dec-26": 17},
# MAGIC -- #     {"TravelBrand": "start_travel","StartAgg", "Jan-25": 1306, "Feb-25": 1150, "Mar-25": 1277},
# MAGIC -- #     {"TravelBrand": "start_travel","StartDirect", "Jan-25": 227, "Feb-25": 200, "Mar-25": 328},
# MAGIC -- #     {"TravelBrand": "viva_ins","VivaAgg", "Jan-25": 2542, "Feb-25": 4477, "Mar-25": 5230, "Apr-25": 6138, "May-25": 4514, "Jun-25": 7156, "Jul-25": 15068, "Aug-25": 16913, "Sep-25": 14387, "Oct-25": 7193, "Nov-25": 4795, "Dec-25": 5754, "Jan-26": 2796, "Feb-26": 4925, "Mar-26": 5753, "Apr-26": 6752, "May-26": 4965, "Jun-26": 7871, "Jul-26": 16575, "Aug-26": 18604, "Sep-26": 15826, "Oct-26": 7912, "Nov-26": 5275, "Dec-26": 6329},
# MAGIC -- #     {"TravelBrand": "viva_ins","VivaDirect", "Jan-25": 139, "Feb-25": 139, "Mar-25": 175, "Apr-25": 136, "May-25": 167, "Jun-25": 217, "Jul-25": 203, "Aug-25": 211, "Sep-25": 216, "Oct-25": 147, "Nov-25": 98, "Dec-25": 118, "Jan-26": 139, "Feb-26": 139, "Mar-26": 175, "Apr-26": 137, "May-26": 167, "Jun-26": 218, "Jul-26": 205, "Aug-26": 214, "Sep-26": 218, "Oct-26": 148, "Nov-26": 99, "Dec-26": 119},
# MAGIC -- #     {"TravelBrand": "oasis_travel","OasisAgg", "Jan-25": 1370, "Feb-25": 1967, "Mar-25": 2175, "Apr-25": 2443, "May-25": 8610, "Jun-25": 11374, "Jul-25": 10768, "Aug-25": 10735, "Sep-25": 11418, "Oct-25": 5601, "Nov-25": 3734, "Dec-25": 4480, "Jan-26": 1439, "Feb-26": 2065, "Mar-26": 2283, "Apr-26": 2566, "May-26": 9041, "Jun-26": 11942, "Jul-26": 11306, "Aug-26": 11272, "Sep-26": 11989, "Oct-26": 5881, "Nov-26": 3920, "Dec-26": 4704},
# MAGIC -- #     {"TravelBrand": "oasis_travel","OasisDirect", "Jan-25": 696, "Feb-25": 617, "Mar-25": 707, "Apr-25": 595, "May-25": 897, "Jun-25": 1007, "Jul-25": 1105, "Aug-25": 860, "Sep-25": 917, "Oct-25": 681, "Nov-25": 454, "Dec-25": 545, "Jan-26": 657, "Feb-26": 588, "Mar-26": 686, "Apr-26": 599, "May-26": 926, "Jun-26": 1067, "Jul-26": 1132, "Aug-26": 919, "Sep-26": 986, "Oct-26": 696, "Nov-26": 464, "Dec-26": 557},
# MAGIC -- #     {"TravelBrand": "atoz_travel","AtozAgg", "Jan-25": 872, "Feb-25": 992, "Mar-25": 1131, "Apr-25": 1385, "May-25": 2788, "Jun-25": 3898, "Jul-25": 3793, "Aug-25": 3807, "Sep-25": 4407, "Oct-25": 2015, "Nov-25": 1344, "Dec-25": 1612, "Jan-26": 916, "Feb-26": 1042, "Mar-26": 1187, "Apr-26": 1454, "May-26": 2927, "Jun-26": 4092, "Jul-26": 3982, "Aug-26": 3998, "Sep-26": 4627, "Oct-26": 2116, "Nov-26": 1411, "Dec-26": 1693},
# MAGIC -- #     {"TravelBrand": "atoz_travel","AtozDirect", "Jan-25": 68, "Feb-25": 25, "Mar-25": 55, "Apr-25": 52, "May-25": 103, "Jun-25": 298, "Jul-25": 376, "Aug-25": 367, "Sep-25": 385, "Oct-25": 159, "Nov-25": 106, "Dec-25": 127, "Jan-26": 71, "Feb-26": 29, "Mar-26": 57, "Apr-26": 57, "May-26": 118, "Jun-26": 332, "Jul-26": 409, "Aug-26": 410, "Sep-26": 436, "Oct-26": 176, "Nov-26": 117, "Dec-26": 141}
# MAGIC -- # ]
# MAGIC 
# MAGIC -- # 10,start_travel
# MAGIC -- # 11,switched_on
# MAGIC -- # 12,trusted_ins
# MAGIC -- # 13,viva_ins
# MAGIC -- # 8,atoz_travel
# MAGIC -- # 14,oasis_travel
# MAGIC 
# MAGIC -- # 18	AtozAgg
# MAGIC -- # 2	AtozDirect
# MAGIC -- # 17	TrustedAgg
# MAGIC -- # 1	TrustedDirect
# MAGIC -- # 4	OasisAgg
# MAGIC -- # 5	OasisDirect
# MAGIC -- # 7	SOIAgg
# MAGIC -- # 9	SOIDirect
# MAGIC -- # 15	VivaAgg
# MAGIC -- # 16	VivaDirect
# MAGIC -- # 19	StartAgg
# MAGIC -- # 20	StartDirect
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select 
# MAGIC     count(*) NoOfPolicyNumbers, 
# MAGIC     sum(TotalGrossExcIPT) TotalGrossExcIPT 
# MAGIC from TaurusGoldLH.FactSalesAnalysis 
# MAGIC where left(DateId,6) = 202501   -- Jan2025
# MAGIC -- and PolicyTypeId = 2            -- Single
# MAGIC and TravelBrandId = 11          -- SOI
# MAGIC and TransactionType <> 'MTA Debit'
# MAGIC -- and policynumber not in         -- endorsements 
# MAGIC -- ('SOI-CTM1134180',	'SOI-CON1013796',	'SOM-MSM1043370',	'SOI-CON1014472',	'SOI-CTM1140858',	'SOI-GOC1039446',	
# MAGIC -- 'SOI-CON1013568',	'SOI-CTM1135881',	'SOI-CTM1135557',	'SOI-CON1014765',	'SOI-GOC1039483',	'SOM-MTC1007715',	
# MAGIC -- 'SOI-MSM1143047',	'SOM-MSM1043284',	'SOI-CTM1141012',	'SOI-CTM1138152',	'SOI-CON1013682',	'SOM-MSM1045534',	
# MAGIC -- 'SOI-GOC1039933',	'SOI-CTM1134917',	'SOI-CTM1141181',	'SOI-CTM1140640',	'SOI-CTM1138330',	'SOM-MSM1042560',	
# MAGIC -- 'SOI-CTM1131407',	'SOI-GOC1037450',	'SOI-CTM1140063',	'SOI-CTM1139073',	'SOI-CTM1138016',	'SOI-DIR1019302',	
# MAGIC -- 'SOM-MTC1007870',	'SOI-CTM1133610',	'SOI-MSM1149819',	'SOI-CTM1135875',	'SOI-DIR1022779',	'SOI-GOC1037367',	
# MAGIC -- 'SOI-CON1014722',	'SOM-MTC1007626',	'SOM-MSM1045069',	'SOI-CTM1133615',	'SOI-MSM1149783',	'SOI-CTM1135531',	
# MAGIC -- 'SOI-CTM1139828',	'SOI-CTM1135186',	'SOI-MSM1151028',	'SOI-CTM1140857',	'SOI-CON1014052',	'SOI-CTM1141159',	
# MAGIC -- 'SOM-MSM1043729',	'SOM-MSM1043742',	'SOI-CTM1135972',	'SOI-CON1013273',	'SOI-CON1014373',	'SOM-MSM1044176',	
# MAGIC -- 'SOI-CON1013452',	'SOI-CTM1141190',	'SOI-GOC1037083',	'SOI-GOC1039900',	'SOI-CTM1119913',	'SOI-CTM1134136',	
# MAGIC -- 'SOI-CON1013965')


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select TravelSalesTransactionID,* from TaurusSilverLH.silver_travel_sales_transaction_persons_sales
# MAGIC limit 100 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select MedicalScoreId,count(*) from TaurusGoldLH.FactEnquiries 
# MAGIC group by MedicalScoreId 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from TaurusGoldLH.DimDuration

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select distinct DurationId from TaurusGoldLH.FactEnquiries 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select count(*) from TaurusGoldLH.FactEnquiries 
# MAGIC --217565770 -- without medicalscore and age group


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from TaurusGoldLH.DimAgeGroup

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from TaurusGoldLH.DimTravelBrand

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select *  from TaurusGoldLH.FactSalesAnalysis
# MAGIC where PolicyNumber = 'SOI-DRNL1000779'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select 
# MAGIC count(*)
# MAGIC --PolicyNumber,count(*) 
# MAGIC from TaurusGoldLH.FactSalesAnalysis
# MAGIC where left(DateId,6)= 202501
# MAGIC and TravelBrandId = 11
# MAGIC and TransactionType = 'New Issue'
# MAGIC -- group by PolicyNumber 
# MAGIC -- having count(*)>1
# MAGIC and AgeGroupId = 1
# MAGIC and PolicyTypeId = 1
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC --COUNT(DISTINCT PolicyNumber)
# MAGIC select PolicyNumber,DateId,TravelBrandId,AgeGroupId,TransactionType,PolicyTypeId,* from TaurusGoldLH.FactSalesAnalysis
# MAGIC where PolicyNumber in 
# MAGIC (
# MAGIC 'SOI-MSM1152185'
# MAGIC )
# MAGIC -- where left(DateId,6)= 202501--
# MAGIC -- and TravelBrandId = 11--
# MAGIC -- and AgeGroupId = 7--
# MAGIC -- and TransactionType = 'New Issue'--
# MAGIC -- and PolicyTypeId = 1--

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select TotalGrossExcIPT,* from TaurusSilverLH.silver_travel_sales_transactions_sales t 
# MAGIC where PolicyNumber = 'SOI-CON1014921'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select AgeAtpolicystart,TotalGrossExcIPT,* from TaurusSilverLH.silver_travel_sales_transactions_sales t 
# MAGIC left join TaurusSilverLH.silver_travel_sales_transaction_persons_sales p on t.TravelSalesTransactionID = p.TravelSalesTransactionID
# MAGIC where PolicyNumber in 
# MAGIC (
# MAGIC 'SOI-MSM1152185'
# MAGIC )
# MAGIC and  TransactionType = 'New Issue'
# MAGIC --2025-04-19T00:00:00Z -- start date
# MAGIC --2004-03-06T00:00:00Z date of birth
# MAGIC -- -- and 
# MAGIC -- PolicyType = 'Annual'AND
# MAGIC --  TransactionDate >= '2025-01-01'
# MAGIC -- AND TransactionDate < '2025-02-01'
# MAGIC -- and t.BrandIdentifier = 'SwitchedOn'
# MAGIC -- -- limit 100 
# MAGIC -- -- and AgeGroupId = 7
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select *  from TaurusGoldLH.DimPolicyType

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select *  from TaurusGoldLH.DimLeadTimeGroup

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select *
# MAGIC --count(*),sum(TotalGrossExcIPT)
# MAGIC from TaurusGoldLH.FactSalesAnalysis
# MAGIC where  left(DateId,6)= 202501
# MAGIC and TravelBrandId = 11
# MAGIC and PolicyTypeId = 2
# MAGIC  and TransactionType = 'New Issue'
# MAGIC  and LeadTimeGroupId = 2
# MAGIC and PolicyNumber = 'SOI-CON1014993'
# MAGIC 
# MAGIC 
# MAGIC -- and TravelBrandId = 11
# MAGIC --and AgeGroupId = 1 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select StartDate,* from TaurusSilverLH.silver_travel_sales_transactions_sales t 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC SELECT * FROM TaurusSilverLH.silver_travel_enquiry_options_sales LIMIT 1000

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM TaurusSilverLH.silver_travel_enquiry_persons_sales LIMIT 1000

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC 
# MAGIC SELECT * FROM TaurusSilverLH.silver_travel_enquiry_sales LIMIT 1000

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select AgeGroupId,count(*) from TaurusGoldLH.FactEnquiries
# MAGIC group by AgeGroupId 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
