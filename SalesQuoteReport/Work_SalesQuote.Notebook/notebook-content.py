# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3b2d8490-dd7d-4046-a7c8-21da791cfcdd",
# META       "default_lakehouse_name": "TaurusBronzeLH",
# META       "default_lakehouse_workspace_id": "9c958852-649b-48e2-ac6a-f03167ff64dc",
# META       "known_lakehouses": [
# META         {
# META           "id": "3b2d8490-dd7d-4046-a7c8-21da791cfcdd"
# META         },
# META         {
# META           "id": "dbfc334d-f83f-43fc-965f-0a38145f4700"
# META         },
# META         {
# META           "id": "d69a91e4-0efe-4e5a-81c6-43b1d5fab183"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM TaurusSilverLH.silver_travel_enquiry_sales 
# MAGIC where AgentName like '%Paying Too Much%' 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT
# MAGIC     AgentName,
# MAGIC     COUNT(*)          AS TotalRows,
# MAGIC     MIN(QuoteDate)    AS EarliestQuoteDate,
# MAGIC     MAX(QuoteDate)    AS LatestQuoteDate
# MAGIC FROM TaurusSilverLH.silver_travel_enquiry_sales
# MAGIC WHERE AgentName IN (
# MAGIC     'Idol Holiday Ready',
# MAGIC     'Paying Too Much',
# MAGIC     'Viva IDOL Holiday Ready',
# MAGIC     'Viva Paying Too Much',
# MAGIC     'Viva AMT Renewal',
# MAGIC     'Viva AMT Direct Renewal',
# MAGIC     'Trusted IDOL Holiday Ready',
# MAGIC     'Trusted Direct AMT Renewal'
# MAGIC )
# MAGIC GROUP BY AgentName
# MAGIC ORDER BY TotalRows DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT
# MAGIC     AgentName,
# MAGIC     COUNT(*)              AS TotalRows,
# MAGIC     MIN(TransactionDate)  AS EarliestTransactionDate,
# MAGIC     MAX(TransactionDate)  AS LatestTransactionDate
# MAGIC FROM TaurusSilverLH.silver_travel_sales_transactions_sales
# MAGIC WHERE AgentName IN (
# MAGIC     'Idol Holiday Ready',
# MAGIC     'Paying Too Much',
# MAGIC     'Viva IDOL Holiday Ready',
# MAGIC     'Viva Paying Too Much',
# MAGIC     'Viva AMT Renewal',
# MAGIC     'Viva AMT Direct Renewal',
# MAGIC     'Trusted IDOL Holiday Ready',
# MAGIC     'Trusted Direct AMT Renewal'
# MAGIC )
# MAGIC GROUP BY AgentName
# MAGIC ORDER BY TotalRows DESC;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql import functions as F

# # Define scheme name tuples
# scheme_oasis_agg = (
#     "MSM Backpacker Silver", "CYTI Backpacker Gold", "IDOL NON MED Single Elite", "IDOL MED Backpacker Silver",
#     "CYTI Single Elite", "MSM AMT Bronze", "IDOL NON MED Single Gold", "MSM AMT Premium", "MTC MED Single Gold",
#     "MTC NON MED AMT Silver", "IDOL NON MED Backpacker Bronze", "IDOL NON MED Single Premium", "MSM Single Premium",
#     "MSM Backpacker Gold", "IDOL MED Single Silver", "CYTI AMT Gold", "CYTI Single Premium", "MTC MED Single Bronze",
#     "CYTI AMT Silver", "MTC MED AMT Silver", "IDOL MED Backpacker Bronze", "CYTI Backpacker Silver",
#     "MSM Backpacker Bronze", "IDOL NON MED Single Classic", "MTC NON MED AMT Bronze", "MSM Single Silver",
#     "IDOL MED AMT Classic", "MTC MED AMT Bronze", "IDOL MED AMT Elite", "CYTI AMT Bronze", "IDOL NON MED Backpacker Gold",
#     "IDOL NON MED AMT Silver", "IDOL MED Single Elite", "MSM Single Gold", "MSM AMT Gold", "MSM Single Elite",
#     "IDOL NON MED Single Bronze", "CYTI Single Classic", "IDOL MED Backpacker Gold", "IDOL MED Single Gold",
#     "IDOL NON MED AMT Gold", "MTC NON MED Single Bronze", "IDOL NON MED AMT Premium", "MSM Single Bronze",
#     "MSM AMT Classic", "CYTI Single Silver", "IDOL MED Single Classic", "CYTI AMT Premium", "MTC MED AMT Gold",
#     "MSM AMT Elite", "MTC NON MED AMT Gold", "CYTI AMT Classic", "IDOL MED AMT Premium", "IDOL MED Single Bronze",
#     "MSM AMT Silver", "IDOL MED AMT Silver", "MTC NON MED Single Silver", "CYTI Single Bronze", "MSM Single Classic",
#     "IDOL NON MED Single Silver", "IDOL NON MED Backpacker Silver", "IDOL MED Single Premium", "MTC NON MED Single Gold",
#     "CYTI Single Gold", "IDOL MED AMT Bronze", "IDOL NON MED AMT Bronze", "CYTI Backpacker Bronze",
#     "IDOL MED AMT Gold", "IDOL NON MED AMT Elite", "MTC MED Single Silver"
# )

# scheme_oasis_direct = (
#     "Direct AMT Emerald", "Agg Renewal AMT Bronze", "Direct Single Gold", "Direct Single Diamond",
#     "AGG RENEWAL AMT SILVER", "Direct Backpackers Gold", "Direct AMT Ruby", "AGG RENEWAL AMT BRONZE",
#     "Direct AMT Bronze", "Direct AMT Gold", "Direct Renewal AMT Diamond", "Direct Single Emerald",
#     "Direct AMT Silver", "Direct Single Ruby", "Agg Renewal AMT Gold", "Direct Backpackers Silver",
#     "Direct Renewal AMT Emerald", "Agg Renewal AMT Silver", "Direct Renewal AMT Ruby", "Direct Single Silver",
#     "Direct AMT Diamond", "AGG RENEWAL AMT GOLD", "Direct Backpackers Bronze", "Direct Single Bronze"
# )

# scheme_atoz_agg = [
#     "AtoZ Silver Backpacker CYTI", "AtoZ Gold ST CYTI", "AtoZ Silver Backpacker IDOL.",
#     "AtoZ Silver Backpacker MSM", "AtoZ Gold AMT IDOL.", "AtoZ Gold AMT CYTI", "AtoZ Silver ST IDOL",
#     "AtoZ Gold ST MSM", "AtoZ Bronze AMT IDOL.", "AtoZ Gold AMT MSM", "AtoZ Silver Backpacker IDOL",
#     "AtoZ Bronze AMT IDOL", "AtoZ Silver AMT CYTI", "AtoZ Gold Backpacker IDOL.", "AtoZ Bronze AMT MSM",
#     "AtoZ Silver ST IDOL.", "AtoZ Silver ST CYTI", "AtoZ Silver AMT IDOL.", "AtoZ Bronze ST IDOL.",
#     "AtoZ Bronze AMT CYTI", "AtoZ Gold AMT IDOL", "AtoZ Silver AMT MSM", "AtoZ Silver ST MSM",
#     "AtoZ Bronze ST IDOL", "AtoZ Bronze ST CYTI", "AtoZ Gold Backpacker IDOL", "AtoZ Gold ST IDOL",
#     "AtoZ Silver AMT IDOL", "AtoZ Gold ST IDOL.", "AtoZ Bronze ST MSM"
# ]

# scheme_atoz_direct = [
# "Direct AtoZ Essential ST","MSM CYTI Renewal AMT Silver","Direct AtoZ Premium ST","AtoZ Renewal Gold IDOL MED AMT",
# "Direct  AtoZ Standard ST","Direct AtoZ Essential AMT","AtoZ Renewal Direct AMT Standard","AtoZ Renewal Silver IDOL MED AMT",
# "MSM CYTI Renewal AMT Gold","Direct AtoZ Premium AMT","AtoZ Renewal Direct AMT Essential","Direct AtoZ Standard AMT","MSM CYTI Renewal AMT Bronze"
# ]


# scheme_viva_direct = [
# "Viva Direct AMT Gold","Viva Direct Single Platinum","Viva Direct Single Gold","Viva Direct AMT Silver","Viva Direct AMT Platinum","Viva Direct Single Silver",
# ]

# scheme_viva_agg = [
# "Viva IDOL AMT Platinum","Viva MSM AMT Platinum","Viva IDOL AMT Silver","Viva CYTI AMT Silver","Viva MSM AMT Silver","Viva CYTI AMT Platinum","Viva IDOL Single Platinum",
# "Viva CYTI Single Gold","Viva IDOL Single Silver","Viva IDOL Single Gold","Viva MSM Single Platinum","Viva MSM AMT Gold","Viva CYTI Single Silver","Viva CYTI Single Platinum",
# "Viva MSM Single Gold","Viva IDOL AMT Gold","Viva CYTI AMT Gold","Viva MSM Single Silver",
# ]

# scheme_trusted_direct = [
# "Trusted Direct AMT Emerald","Trusted Direct Single Emerald","Trusted Direct Single Ruby","Trusted Direct AMT Diamond",
# ]

# scheme_trusted_agg = [
# "Trusted MSM Single Ultimate","Trusted IDOL AMT Ultimate","Trusted MSM Single Essential","Trusted MSM AMT Essential","Trusted IDOL Single Ultimate","Trusted MSM Single Classic",
# "Trusted MSM AMT Classic","Trusted MSM AMT Ultimate","Trusted IDOL Single Essential","Trusted IDOL Single Classic","Trusted IDOL AMT Classic","Trusted IDOL AMT Essential",
# ]

# scheme_soi_agg = [
# "IDOL Backpacker Ultimate","CYTI Backpacker Ultimate","IDOL Backpacker Standard","CYTI AMT Three","IDOL AMT Two","CYTI Backpacker Standard","CYTI Backpacker Premium","MSM Backpacker Standard",
# "MSM Single Three","IDOL Single One","IDOL AMT One","MSM AMT Three","MSM AMT Two","IDOL Single Two","MSM Backpacker Premium","MSM Single One","CYTI Single One","CYTI AMT One","MSM AMT One",
# "IDOL AMT Three","CYTI AMT Two","MSM Backpacker Ultimate","IDOL Backpacker Premium","IDOL Single Three","MSM Single Two","CYTI Single Two","CYTI Single Three",
# ]

# scheme_soi_direct = [
# "Direct Backpacker Premium","PCW Renewal One","Direct Backpacker Ultimate","SOI Direct Renewal Standard","Direct Single Ultimate","PCW Renewal Two","Direct Single Premium",
# "PCW Renewal Three","Direct Single Standard","SOI Direct Renewal Premium","Direct Backpacker Standard","Direct AMT Ultimate","Direct AMT Premium","Direct AMT Standard",
# "SOI Direct Renewal Ultimate",
# ]

# scheme_start_agg = [
# "Start MSM Single Premier","Start CYTI Backpacker Classic","Start CYTI AMT Premier","Start IDOL Single Premier.","Start IDOL AMT Essential","Start IDOL AMT Premier.",
# "Start IDOL Single Classic.","Start CYTI AMT Classic","Start CYTI Backpacker Essential","Start CYTI Single Premier","Start IDOL Backpacker Premier.","Start IDOL Backpacker Essential",
# "Start IDOL Backpacker Premier","Start CYTI Backpacker Premier","Start MSM AMT Essential","Start IDOL Single Classic","Start MSM AMT Classic","Start IDOL Single Essential.",
# "Start IDOL Backpacker Classic","Start CYTI Single Classic","Start IDOL AMT Classic.","Start MSM Single Classic","Start IDOL AMT Premier","Start IDOL AMT Classic","Start CYTI Single Essential",
# "Start MSM Backpacker Classic","Start IDOL Backpacker Classic.","Start IDOL AMT Essential.","Start MSM AMT Premier","Start IDOL Single Premier","Start IDOL Backpacker Essential.",
# "Start IDOL Single Essential","Start CYTI AMT Essential","Start MSM Backpacker Premier","Start MSM Single Essential",
# ]

# scheme_start_direct = [
# "Start Direct Renewal 5 Star","Start PCW Renewal Essential","Start Direct AMT 4 Star","Start Direct Backpacker Classic","Start Direct Single 4 Star","Start AGG Renewal Classic",
# "Start PCW Renewal Classic","Start AGG Renewal Premier","Start Direct Single 5 Star","Start Direct Backpacker Essential","Start Direct Renewal 4 Star","Start Direct AMT 3 Star",
# "Start AGG Renewal Essential","Start Direct AMT 5 Star","Start Direct Single 3 Star","Start Direct Backpacker Premier","Start Direct Renewal 3 Star","Start PCW Renewal Premier",

# ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# agg_oasis_list_sql = ", ".join(f"'{s}'" for s in scheme_oasis_agg)
# direct_oasis_list_sql = ", ".join(f"'{s}'" for s in scheme_oasis_direct)

# agg_atoz_list_sql = ", ".join(f"'{s}'" for s in scheme_atoz_agg)
# direct_atoz_list_sql = ", ".join(f"'{s}'" for s in scheme_atoz_direct)

# agg_viva_list_sql = ", ".join(f"'{s}'" for s in scheme_viva_agg)
# direct_viva_list_sql = ", ".join(f"'{s}'" for s in scheme_viva_direct)

# agg_trusted_list_sql = ", ".join(f"'{s}'" for s in scheme_trusted_agg)
# direct_trusted_list_sql = ", ".join(f"'{s}'" for s in scheme_trusted_direct)

# agg_soi_list_sql = ", ".join(f"'{s}'" for s in scheme_soi_agg)
# direct_soi_list_sql = ", ".join(f"'{s}'" for s in scheme_soi_direct)

# agg_start_list_sql = ", ".join(f"'{s}'" for s in scheme_start_agg)
# direct_start_list_sql = ", ".join(f"'{s}'" for s in scheme_start_direct)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from TaurusSilverLH.silver_travel_sales_transactions_sales
# MAGIC limit 10 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC SELECT * FROM TaurusSilverLH.silver_travel_enquiry_sales limit 100 
# MAGIC -- silver_travel_enquiry_results_sales - TravelEnquiryResultID and TravelEnquiryID
# MAGIC -- silver_travel_enquiry_sales - TravelEnquiryID


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC SELECT * FROM TaurusSilverLH.silver_travel_enquiry_results_sales limit 100 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select destination,PolicyType,SchemeID,TravelSalesTransactionID,Destination,HighestRatedCountry,TotalGrossIncIPT,TotalGrossExcIPT,TotalNetToUnderwriter,* 
# MAGIC from tblTravelSalesTransactionsSales
# MAGIC where PolicyNumber = 'SOI-GOC1045267'
# MAGIC limit 10 
# MAGIC --check cancellation
# MAGIC -- SOI-CTM1144942

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC select TotalGrossIncIPT/1.2 as TotalGrossExcIPT,* from tblTravelEnquiryResults  limit 100 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select AgeAtpolicystart,* from TaurusSilverLH.silver_travel_sales_transaction_persons_sales 
# MAGIC where TravelSalesTransactionID IN (select TravelSalesTransactionID 
# MAGIC from tblTravelSalesTransactionsSales
# MAGIC where PolicyNumber = 'SOI-CTM1144942'
# MAGIC )
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * 
# MAGIC from tblTravelSalesTransactionsSales
# MAGIC where PolicyNumber = 'SOI-CTM1144942'


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 
# MAGIC     t.PolicyNumber,
# MAGIC     t.FamilyGroup,
# MAGIC     t.TravelSalesTransactionId,
# MAGIC     COUNT(DISTINCT CONCAT(p.FirstName, '|', p.Surname)) AS UniquePersons,
# MAGIC     CASE 
# MAGIC         WHEN t.FamilyGroup = 'Individual' AND COUNT(DISTINCT CONCAT(p.FirstName, '|', p.Surname)) > 1 THEN 'Group'
# MAGIC         ELSE t.FamilyGroup
# MAGIC     END AS FinalFamilyGroup
# MAGIC FROM 
# MAGIC     TaurusSilverLH.silver_travel_sales_transactions_sales t
# MAGIC JOIN 
# MAGIC     TaurusSilverLH.silver_travel_sales_transaction_persons_sales p 
# MAGIC     ON t.TravelSalesTransactionId = p.TravelSalesTransactionId
# MAGIC WHERE 
# MAGIC     t.PolicyNumber = 'SOI-CTM1144942'
# MAGIC GROUP BY 
# MAGIC     t.PolicyNumber, t.FamilyGroup, t.TravelSalesTransactionId;
# MAGIC 
# MAGIC CASE 
# MAGIC         WHEN AgeAtPolicyIssue BETWEEN 0 AND 20 THEN '0–20'
# MAGIC         WHEN AgeAtPolicyIssue BETWEEN 21 AND 30 THEN '21–30'
# MAGIC         WHEN AgeAtPolicyIssue BETWEEN 31 AND 40 THEN '31–40'
# MAGIC         WHEN AgeAtPolicyIssue BETWEEN 41 AND 50 THEN '41–50'
# MAGIC         WHEN AgeAtPolicyIssue BETWEEN 51 AND 65 THEN '51–65'
# MAGIC         WHEN AgeAtPolicyIssue BETWEEN 66 AND 75 THEN '66–75'
# MAGIC         WHEN AgeAtPolicyIssue >= 76 THEN '76+'
# MAGIC         ELSE 'Invalid Age'
# MAGIC     END AS AgeGroup

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC SELECT sum(TotalGrossExcIPT) FROM TaurusSilverLH.silver_travel_sales_transactions_atoz LIMIT 1000

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select sum(TotalGrossExcIPT) from TaurusGoldLH.FactSalesAnalysis 
# MAGIC where TravelBrandId = 8
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
# MAGIC select * from TaurusGoldLH.factdailysalesreport
# MAGIC where DateID = 20250901

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select distinct DestinationID,count(*) from TaurusGoldLH.FactSalesAnalysis
# MAGIC group by DestinationID 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC 
# MAGIC select * from TaurusGoldLH.DimMarketingChannel

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select MarketingChannelId, COunt(*) from TaurusGoldLH.FactSalesAnalysis 
# MAGIC where left(DateId,4) = 2025
# MAGIC group by MarketingChannelId 
# MAGIC 
# MAGIC limit 10 
# MAGIC -- where marketingchannel 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select count(*) From FactSalesAnalysis
# MAGIC where marketingchannelid is null 
# MAGIC and sourcetype = 'Atoz'
# MAGIC --21530
# MAGIC --12645

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select SchemeName,* from TaurusSilverLH.silver_travel_enquiry_sales
# MAGIC limit 10 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC SELECT * FROM TaurusSilverLH.silver_travel_sales_transactions_atoz 
# MAGIC where AgentName = 'AtoZ Direct AMT Renewal'
# MAGIC and CampaignName = 'RENEW20' and MarketingChannel is null

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC SELECT CampaignId, CampaignName
# MAGIC FROM TaurusGoldLH.DimCampaign
# MAGIC WHERE CampaignName like '%Partner%'
# MAGIC 
# MAGIC -- 'CREST10'
# MAGIC 
# MAGIC --     'FOOTY10',
# MAGIC --     'JCB10',
# MAGIC --     'NAT05',
# MAGIC --     'SJPP10',
# MAGIC --     'Travel Partner'
# MAGIC -- )
# MAGIC ORDER BY CampaignName;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC SELECT AgentId, AgentName
# MAGIC FROM TaurusGoldLH.DimAgent
# MAGIC WHERE AgentName = 'AtoZ Insurance IDOL CTM'
# MAGIC -- like '%CTM%'
# MAGIC     
# MAGIC ORDER BY AgentName;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT 
# MAGIC     COUNT(DISTINCT PolicyNumber) AS DistinctPolicyCount
# MAGIC     --,policystatus
# MAGIC FROM TaurusBronzeLH.tblTravelSalesTransactions_1
# MAGIC WHERE TransactionDate >= '2025-01-01'
# MAGIC   AND TransactionDate < '2025-02-01'
# MAGIC   AND (PolicyNumber LIKE 'SOI%' OR PolicyNumber LIKE 'SOM%')
# MAGIC   AND TransactionType NOT IN ('Client Details Update','Cancellation')
# MAGIC   AND PolicyStatus = 'Live'
# MAGIC   --group by policystatus
# MAGIC   -- policystatus

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC SELECT 
# MAGIC     distinct PolicyStatus,PolicyNumber
# MAGIC FROM TaurusBronzeLH.tblTravelSalesTransactions_1
# MAGIC WHERE TransactionDate >= '2025-01-01'
# MAGIC   AND TransactionDate < '2025-02-01'
# MAGIC   AND (PolicyNumber LIKE 'SOI%' OR PolicyNumber LIKE 'SOM%')
# MAGIC   --AND TransactionType NOT IN ('Client Details Update','Cancellation')
# MAGIC   --AND PolicyStatus = 'Live'
# MAGIC   and PolicyNumber in ('SOI-DIR1022975',
# MAGIC 'SOI-DIR1023143',
# MAGIC 'SOI-DRNL1000733',
# MAGIC 'SOI-DRNL1000734',
# MAGIC 'SOI-DRNL1000735',
# MAGIC 'SOI-DRNL1000737',
# MAGIC 'SOI-DRNL1000738',
# MAGIC 'SOI-DRNL1000739',
# MAGIC 'SOI-DRNL1000742',
# MAGIC 'SOI-DRNL1000746',
# MAGIC 'SOI-DRNL1000747',
# MAGIC 'SOI-DRNL1000750',
# MAGIC 'SOI-DRNL1000751',
# MAGIC 'SOI-DRNL1000752',
# MAGIC 'SOI-DRNL1000754',
# MAGIC 'SOI-DRNL1000755',
# MAGIC 'SOI-DRNL1000759',
# MAGIC 'SOI-DRNL1000760',
# MAGIC 'SOI-DRNL1000761',
# MAGIC 'SOI-DRNL1000762',
# MAGIC 'SOI-DRNL1000764',
# MAGIC 'SOI-DRNL1000765',
# MAGIC 'SOI-DRNL1000766',
# MAGIC 'SOI-DRNL1000767',
# MAGIC 'SOI-DRNL1000768',
# MAGIC 'SOI-DRNL1000769',
# MAGIC 'SOI-DRNL1000771',
# MAGIC 'SOI-DRNL1000773',
# MAGIC 'SOI-DRNL1000775',
# MAGIC 'SOI-DRNL1000777',
# MAGIC 'SOI-DRNL1000778',
# MAGIC 'SOI-DRNL1000779',
# MAGIC 'SOI-DRNL1000782',
# MAGIC 'SOI-DRNL1000783',
# MAGIC 'SOI-DRNL1000784',
# MAGIC 'SOI-DRNL1000785',
# MAGIC 'SOI-DRNL1000787',
# MAGIC 'SOI-DRNL1000789',
# MAGIC 'SOI-DRNL1000790',
# MAGIC 'SOI-DRNL1000791',
# MAGIC 'SOI-DRNL1000792',
# MAGIC 'SOI-DRNL1000793',
# MAGIC 'SOI-DRNL1000794',
# MAGIC 'SOI-DRNL1000795',
# MAGIC 'SOI-DRNL1000797',
# MAGIC 'SOI-DRNL1000798',
# MAGIC 'SOI-DRNL1000799',
# MAGIC 'SOI-DRNL1000800',
# MAGIC 'SOI-DRNL1000801',
# MAGIC 'SOI-DRNL1000803',
# MAGIC 'SOI-DRNL1000804',
# MAGIC 'SOI-DRNL1000805',
# MAGIC 'SOI-RNL1001227',
# MAGIC 'SOI-RNL1001228',
# MAGIC 'SOI-RNL1001229',
# MAGIC 'SOI-RNL1001230',
# MAGIC 'SOI-RNL1001233',
# MAGIC 'SOI-RNL1001234',
# MAGIC 'SOI-RNL1001235',
# MAGIC 'SOI-RNL1001236',
# MAGIC 'SOI-RNL1001237',
# MAGIC 'SOI-RNL1001238',
# MAGIC 'SOI-RNL1001239',
# MAGIC 'SOI-RNL1001240',
# MAGIC 'SOI-RNL1001241',
# MAGIC 'SOI-RNL1001243',
# MAGIC 'SOI-RNL1001245',
# MAGIC 'SOI-RNL1001247',
# MAGIC 'SOI-RNL1001250',
# MAGIC 'SOI-RNL1001251',
# MAGIC 'SOI-RNL1001252',
# MAGIC 'SOI-RNL1001253',
# MAGIC 'SOI-RNL1001254',
# MAGIC 'SOI-RNL1001255',
# MAGIC 'SOI-RNL1001256',
# MAGIC 'SOI-RNL1001257',
# MAGIC 'SOI-RNL1001258',
# MAGIC 'SOI-RNL1001259',
# MAGIC 'SOI-RNL1001260',
# MAGIC 'SOI-RNL1001261',
# MAGIC 'SOI-RNL1001262',
# MAGIC 'SOI-RNL1001263',
# MAGIC 'SOI-RNL1001264',
# MAGIC 'SOI-RNL1001265',
# MAGIC 'SOI-RNL1001266',
# MAGIC 'SOI-RNL1001267',
# MAGIC 'SOI-RNL1001268',
# MAGIC 'SOI-RNL1001269',
# MAGIC 'SOI-RNL1001270',
# MAGIC 'SOI-RNL1001271',
# MAGIC 'SOI-RNL1001272',
# MAGIC 'SOI-RNL1001274',
# MAGIC 'SOI-RNL1001275',
# MAGIC 'SOI-RNL1001277',
# MAGIC 'SOI-RNL1001278',
# MAGIC 'SOI-RNL1001279')
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC --with policy status
# MAGIC select distinct policystatus,count(*)from TaurusBronzeLH.tblTravelSalesTransactions_1
# MAGIC WHERE TransactionDate >= '2025-01-01'
# MAGIC   AND TransactionDate < '2025-02-01'
# MAGIC   AND (PolicyNumber LIKE 'SOI%' OR PolicyNumber LIKE 'SOM%')
# MAGIC   AND TransactionType NOT IN ('Client Details Update')
# MAGIC group by policystatus 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC --wihtout
# MAGIC select count(*)from TaurusBronzeLH.tblTravelSalesTransactions_1
# MAGIC WHERE TransactionDate >= '2025-01-01'
# MAGIC   AND TransactionDate < '2025-02-01'
# MAGIC   AND (PolicyNumber LIKE 'SOI%' OR PolicyNumber LIKE 'SOM%')
# MAGIC   AND TransactionType NOT IN ('Client Details Update')
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC SELECT 
# MAGIC    --COUNT(DISTINCT PolicyNumber) AS DistinctPolicyCount
# MAGIC    COUNT(*)
# MAGIC FROM TaurusGoldLH.factsalesanalysis
# MAGIC WHERE Left(DateId,6)= 202501
# MAGIC AND LEFT(PolicyNumber, 3) IN ('SOI', 'SOM')
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC SELECT 
# MAGIC    DISTINCT PolicyNumber
# MAGIC FROM TaurusGoldLH.factsalesanalysis
# MAGIC WHERE Left(DateId,6)= 202501
# MAGIC AND LEFT(PolicyNumber, 3) IN ('SOI', 'SOM')
# MAGIC AND TransactionType NOT IN ('Client Details Update','Cancellation')
# MAGIC and PolicyStatus = 'Live'
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC select * from 
# MAGIC TaurusGoldLH.factsalesanalysis
# MAGIC where policynumber
# MAGIC in (
# MAGIC select policynumber
# MAGIC FROM TaurusGoldLH.factsalesanalysis
# MAGIC WHERE Left(DateId,6)= 202501
# MAGIC AND LEFT(PolicyNumber, 3) IN ('SOI', 'SOM')
# MAGIC AND TransactionType NOT IN ('Client Details Update','Cancellation')
# MAGIC and PolicyStatus = 'Live'
# MAGIC group by policynumber 
# MAGIC having count(*)>1
# MAGIC )
# MAGIC and Left(DateId,6)= 202501 and 
# MAGIC LEFT(PolicyNumber, 3) IN ('SOI', 'SOM')
# MAGIC AND TransactionType NOT IN ('Client Details Update','Cancellation')
# MAGIC and PolicyStatus = 'Live'
# MAGIC order by policynumber

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT 
# MAGIC *
# MAGIC FROM TaurusGoldLH.factsalesanalysis
# MAGIC where policynumber = 'SOI-CTM1142544'
# MAGIC  and PolicyStatus = 'Live' 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from TaurusGoldLH.DimMarketingChannel

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select distinct TravelBrandId,count(*) from TaurusGoldLH.factsalesanalysis
# MAGIC group by TravelBrandId

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
# MAGIC select * from TaurusGoldLH.DimTravelBrand
# MAGIC limit 100 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from TaurusGoldLH.DimTravelBrand
# MAGIC order by 1 
# MAGIC 
# MAGIC start_travel 10
# MAGIC switched_on 11
# MAGIC trusted_ins 12
# MAGIC viva_ins 13
# MAGIC 
# MAGIC oasis_travel 14
# MAGIC atoz_travel 8

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC %%sql 
# MAGIC 
# MAGIC select TravelBrand,a.AgentName,count(*) from TaurusGoldLH.FactEnquiries f
# MAGIC left join DimTravelBrand t on f.TravelBrandId = t.TravelBrandID
# MAGIC left join DimAgent a on f.AgentId = a.AgentId
# MAGIC where TravelBrand = 'UNKNOWN'
# MAGIC group by TravelBrand,a.AgentName 
# MAGIC -- Trusted IDOL Money


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
# MAGIC select * from TaurusGoldLH.DimTravelBrand

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select * from TaurusGoldLH.FactSalesAnalysis 
# MAGIC limit 10 

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

# MAGIC %%%sql
# MAGIC 
# MAGIC select policynumber,* from TaurusGoldLH.FactSalesAnalysis 
# MAGIC where left(DateId,6) = 202501
# MAGIC and PolicyTypeId = 2
# MAGIC and TravelBrandId = 11


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select policynumber,* from TaurusGoldLH.FactSalesAnalysis 
# MAGIC where left(DateId,6) = 202501
# MAGIC and PolicyTypeId = 2
# MAGIC and TravelBrandId = 11
# MAGIC and policynumber not in 
# MAGIC 
# MAGIC ('SOI-CON1014929',	'SOI-CTM1141468',	'SOI-MSM1151169',	'SOM-MSM1045795',	'SOI-GOC1040048',	'SOI-CTM1144356',	'SOI-CTM1141600',	'SOM-MSM1045775',	'SOI-DIR1022890',	'SOI-CTM1141554',	'SOI-DIR1022886',	'SOI-CTM1141555',	'SOI-CTM1141509',	'SOM-MTC1007892',	'SOI-CTM1141469',	'SOI-GOC1040050',	'SOI-CTM1141426',	'SOI-CTM1141640',	'SOI-GOC1040064',	'SOI-GOC1040115',	'SOI-DIR1022894',	'SOI-CTM1141556',	'SOI-CON1014963',	'SOI-CON1014962',	'SOI-CTM1141601',	'SOI-GOC1040079',	'SOI-CTM1141427',	'SOI-GOC1040051',	'SOM-MSM1045777',	'SOI-MSM1151182',	'SOM-MSM1045821',	'SOI-CTM1141641',	'SOI-GOC1040116',	'SOI-CTM1141613',	'SOI-CON1014964',	'SOI-CTM1141603',	'SOM-MSM1045796',	'SOI-MSM1151157',	'SOI-CTM1141558',	'SOI-GOC1040093',	'SOI-DIR1022900',	'SOI-GOC1040052',	'SOI-MSM1151202',	'SOI-CTM1141470',	'SOI-CTM1141510',	'SOI-MSM1151191',	'SOI-CTM1144357',	'SOI-GOC1040053',	'SOI-CTM1141511',	'SOI-GOC1040065',	'SOI-CON1014930',	'SOI-CTM1141642',	'SOI-MSM1151203',	'SOI-CTM1141471',	'SOI-CTM1141602',	'SOI-CON1014953',	'SOI-DIR1022941',	'SOI-CTM1141643',	'SOI-CON1014952',	'SOI-CTM1141512',	'SOM-MTC1007880',	'SOI-CTM1141397',	'SOI-DIR1022885',	'SOI-CTM1141428',	'SOM-MSM1045778',	'SOI-DIR1022944',	'SOI-GOC1040066',	'SOI-CTM1141472',	'SOI-GOC1040094',	'SOI-DIR1022884',	'SOI-GOC1040054',	'SOI-CON1014931',	'SOI-CTM1141513',	'SOI-CTM1141473',	'SOI-MSM1151170',	'SOI-CTM1141394',	'SOI-MSM1151136',	'SOI-GOC1040095',	'SOI-CTM1141604',	'SOI-DIR1022901',	'SOI-CTM1141559',	'SOI-MSM1151192',	'SOI-CTM1141605',	'SOM-MSM1045822',	'SOI-CTM1141395',	'SOI-MSM1151158',	'SOI-GOC1040096',	'SOI-CTM1141560',	'SOI-CTM1141514',	'SOI-GOC1040128',	'SOI-CTM1141474',	'SOI-CTM1141396',	'SOI-GOC1040032',	'SOI-CTM1141606',	'SOI-MSM1151193',	'SOI-CTM1141515',	'SOI-CTM1141516',	'SOI-CTM1141607',	'SOI-CTM1141429',	'SOI-CTM1141399',	'SOI-GOC1040068',	'SOI-CTM1141398',	'SOI-GOC1040067',	'SOI-MSM1151211',	'SOI-CTM1141561',	'SOI-CON1014921',	'SOI-MSM1151137',	'SOI-DIR1022895',	'SOI-GOC1040033',	'SOI-CTM1141400',	'SOI-GOC1040034',	'SOI-CTM1141517',	'SOM-MSM1045797',	'SOI-GOC1040069',	'SOM-MSM1045763',	'SOI-DIR1023053',	'SOI-CON1014922',	'SOI-GOC1040035',	'SOI-CTM1141401',	'SOI-CTM1141644',	'SOI-CTM1141609',	'SOM-MSM1045779',	'SOI-CON1014923',	'SOI-CON1014924',	'SOI-MSM1151139',	'SOM-MSM1045780',	'SOI-MSM1151212',	'SOI-CTM1141563',	'SOI-CTM1141564',	'SOI-MSM1151171',	'SOI-CTM1141518',	'SOI-GOC1040036',	'SOI-CTM1141566',	'SOI-CTM1141565',	'SOM-MSM1045836',	'SOI-MSM1151172',	'SOI-DIR1022887',	'SOI-CTM1141430',	'SOI-CTM1141610',	'SOI-CTM1141646',	'SOI-CTM1141402',	'SOI-MSM1151160',	'SOI-GOC1040117',	'SOI-CTM1141669',	'SOI-CTM1141519',	'SOI-CTM1141645',	'SOI-GOC1040097',	'SOI-CTM1141567',	'SOI-CTM1141568',	'SOI-CTM1141611',	'SOM-MSM1045850',	'SOI-MSM1151204',	'SOI-CTM1141617',	'SOM-MSM1045849',	'SOI-CTM1141476',	'SOI-CON1014954',	'SOM-MSM1045798',	'SOI-CON1014972',	'SOI-CTM1141520',	'SOI-CTM1141647',	'SOM-MTC1007875',	'SOI-CTM1141477',	'SOI-CTM1141521',	'SOI-CTM1141569',	'SOI-MSM1151173',	'SOI-CON1014932',	'SOM-MSM1045765',	'SOI-CTM1141612',	'SOM-MSM1045799',	'SOI-CTM1141432',	'SOI-CTM1141478',	'SOI-CTM1141403',	'SOI-CON1014944',	'SOI-CTM1141522',	'SOM-MSM1045800',	'SOI-CTM1141523',	'SOI-MSM1151183',	'SOI-CTM1141670',	'SOI-GOC1040129',	'SOM-MSM1045801',	'SOI-MSM1151140',	'SOI-GOC1040055',	'SOI-CTM1141479',	'SOI-CTM1141671',	'SOI-CTM1141648',	'SOM-MSM1045856',	'SOM-MSM1045766',	'SOI-CTM1141614',	'SOI-CTM1141525',	'SOI-CON1014973',	'SOI-CTM1141480',	'SOI-CTM1141404',	'SOI-CON1014974',	'SOI-CTM1141524',	'SOI-CTM1141672',	'SOM-MSM1045857',	'SOI-CTM1141433',	'SOI-CTM1141434',	'SOI-CTM1141615',	'SOI-GOC1040098',	'SOI-CTM1141435',	'SOI-GOC1040141',	'SOI-CTM1141673',	'SOI-MSM1151141',	'SOM-MTC1007877',	'SOI-CTM1141405',	'SOI-CTM1141526',	'SOM-MSM1045810',	'SOI-CTM1141437',	'SOI-CTM1141438',	'SOI-CTM1141674',	'SOI-GOC1040099',	'SOI-CON1014945',	'SOI-CTM1141527',	'SOI-CTM1141571',	'SOI-DIR1022993',	'SOI-MSM1151213',	'SOI-CTM1141439',	'SOM-MSM1045837',	'SOI-GOC1040082',	'SOI-MSM1151142',	'SOI-CTM1141406',	'SOI-CTM1141440',	'SOI-CTM1141572',	'SOI-CTM1141573',	'SOI-CTM1141574',	'SOI-CTM1141481',	'SOI-CTM1141482',	'SOM-MTC1007878',	'SOI-GOC1040119',	'SOI-CTM1141484',	'SOI-CTM1141483',	'SOI-DIR1022902',	'SOI-GOC1040056',	'SOI-GOC1040120',	'SOI-CTM1141675',	'SOI-GOC1040143',	'SOI-CTM1141485',	'SOI-CON1014975',	'SOI-GOC1040100',	'SOI-CTM1141441',	'SOI-CTM1141575',	'SOI-CON1014955',	'SOI-CTM1141442',	'SOI-CON1014933',	'SOI-GOC1040083',	'SOI-CTM1141649',	'SOI-GOC1040037',	'SOM-MSM1045768',	'SOI-CON1014939',	'SOI-CON1014934',	'SOI-DIR1022888',	'SOI-GOC1040038',	'SOI-MSM1151143',	'SOI-CTM1141676',	'SOM-MSM1045823',	'SOM-MTC1007895',	'SOM-MSM1045781',	'SOI-CTM1141407',	'SOI-CTM1141650',	'SOI-CTM1141579',	'SOI-CTM1141408',	'SOI-DIR1022896',	'SOI-CTM1141651',	'SOI-GOC1040039',	'SOI-CTM1141444',	'SOI-CTM1141576',	'SOI-CTM1141528',	'SOM-MTC1007888',	'SOI-GOC1040040',	'SOI-GOC1040041',	'SOI-CTM1141443',	'SOM-MSM1045802',	'SOI-CTM1141577',	'SOI-GOC1040121',	'SOI-CTM1141677',	'SOI-CON1014976',	'SOI-CTM1141578',	'SOI-CON1014935',	'SOM-MSM1045782',	'SOI-MSM1151144',	'SOI-CTM1141618',	'SOI-GOC1040058',	'SOI-CTM1141410',	'SOI-GOC1040144',	'SOI-GOC1040057',	'SOI-CTM1141486',	'SOI-CTM1141487',	'SOI-CTM1141445',	'SOI-CTM1141580',	'SOI-CTM1141411',	'SOI-CTM1141488',	'SOI-MSM1151206',	'SOI-GOC1040059',	'SOI-CTM1141581',	'SOI-GOC1040085',	'SOI-CTM1141529',	'SOI-CON1014946',	'SOI-CTM1141619',	'SOM-MSM1045783',	'SOI-CTM1141620',	'SOI-CTM1141412',	'SOI-CTM1141678',	'SOI-CON1014977',	'SOI-CTM1141530',	'SOI-GOC1040042',	'SOI-GOC1040086',	'SOI-GOC1040087',	'SOM-MSM1045824',	'SOI-CTM1141679',	'SOI-MSM1151146',	'SOI-CTM1141680',	'SOM-MSM1045839',	'SOI-CTM1141652',	'SOI-CTM1141489',	'SOI-GOC1040044',	'SOI-CTM1141582',	'SOI-CTM1141447',	'SOI-CTM1141490',	'SOI-GOC1040101',	'SOI-CON1014956',	'SOM-MSM1045825',	'SOM-MTC1007881',	'SOI-CTM1141531',	'SOI-CTM1141532',	'SOI-CTM1141491',	'SOI-DIR1022889',	'SOI-CTM1141448',	'SOI-CTM1141583',	'SOI-CTM1141492',	'SOI-GOC1040070',	'SOI-CON1014957',	'SOI-CTM1141681',	'SOI-CTM1141653',	'SOI-DIR1022905',	'SOI-GOC1040104',	'SOI-CTM1141682',	'SOI-CTM1141449',	'SOM-MSM1045813',	'SOI-CON1014967',	'SOI-GOC1040105',	'SOM-MSM1045826',	'SOI-CON1014926',	'SOI-CTM1141684',	'SOI-CTM1141450',	'SOI-GOC1040110',	'SOI-CTM1141415',	'SOM-MSM1045814',	'SOM-MSM1045803',	'SOM-MSM1045827',	'SOI-CTM1141414',	'SOI-CTM1141416',	'SOI-CTM1141533',	'SOI-CTM1141451',	'SOM-MTC1007897',	'SOI-CTM1141683',	'SOM-MTC1007887',	'SOI-CON1014968',	'SOI-CTM1141685',	'SOI-CTM1141621',	'SOI-CON1014927',	'SOI-CTM1141453',	'SOI-CTM1141534',	'SOI-CTM1141584',	'SOM-MSM1045804',	'SOI-CTM1141494',	'SOI-GOC1040071',	'SOM-MSM1045840',	'SOI-GOC1040145',	'SOI-GOC1040146',	'SOI-CTM1141417',	'SOI-CTM1141585',	'SOI-CON1014940',	'SOI-CTM1141495',	'SOI-CTM1141654',	'SOI-GOC1040130',	'SOI-CTM1141686',	'SOI-DIR1022897',	'SOI-CTM1141535',	'SOI-CON1014947',	'SOI-MSM1151185',	'SOI-GOC1040147',	'SOI-CTM1141655',	'SOI-GOC1040131',	'SOI-GOC1040123',	'SOM-MSM1045841',	'SOI-GOC1040045',	'SOI-MSM1151149',	'SOI-MSM1151195',	'SOI-DIR1022898',	'SOM-MTC1007882',	'SOI-CTM1141419',	'SOI-CTM1141623',	'SOI-CTM1141497',	'SOI-GOC1040132',	'SOI-GOC1040124',	'SOI-GOC1040046',	'SOI-CON1014958',	'SOI-CTM1141420',	'SOM-MSM1045785',	'SOI-CTM1141498',	'SOI-DIR1022907',	'SOI-DIR1022918',	'SOI-CTM1141624',	'SOI-MSM1151208',	'SOI-MSM1151177',	'SOM-MSM1045771',	'SOI-CON1014948',	'SOM-MSM1045772',	'SOI-CTM1141625',	'SOM-MSM1045858',	'SOI-CTM1141499',	'SOI-CTM1141454',	'SOI-GOC1040133',	'SOI-GOC1040134',	'SOI-MSM1151214',	'SOI-CTM1141687',	'SOI-MSM1151151',	'SOI-CTM1141421',	'SOM-MSM1045853',	'SOM-MTC1007879',	'SOI-GOC1040125',	'SOI-CTM1141422',	'SOI-GOC1040106',	'SOI-GOC1040148',	'SOI-CTM1141423',	'SOI-CON1014936',	'SOI-CTM1141455',	'SOI-GOC1040072',	'SOM-MSM1045805',	'SOI-CTM1141656',	'SOI-CTM1141424',	'SOI-CTM1141586',	'SOI-CTM1141688',	'SOI-GOC1040073',	'SOI-MSM1151196',	'SOI-GOC1040108',	'SOM-MSM1045828',	'SOI-DIR1022899',	'SOI-CON1014969',	'SOI-CTM1141657',	'SOI-CTM1141626',	'SOI-MSM1151198',	'SOI-MSM1151199',	'SOI-CTM1141658',	'SOI-CTM1141500',	'SOI-MSM1151162',	'SOI-CTM1141456',	'SOI-CON1014937',	'SOI-MSM1151153',	'SOI-CTM1141536',	'SOI-CTM1141537',	'SOI-CTM1141457',	'SOI-GOC1040047',	'SOI-CTM1141459',	'SOI-CTM1141458',	'SOI-CTM1141425',	'SOI-CTM1141588',	'SOI-MSM1151187',	'SOI-GOC1040149',	'SOI-CTM1141589',	'SOI-GOC1040126',	'SOI-CON1014928',	'SOI-CTM1141538',	'SOI-CON1014970',	'SOI-CTM1141659',	'SOI-CTM1141501',	'SOI-CON1014941',	'SOM-MSM1045773',	'SOI-CTM1141660',	'SOI-CTM1141661',	'SOI-GOC1040074',	'SOI-CTM1141502',	'SOI-CTM1141539',	'SOI-MSM1151156',	'SOM-MSM1045869',	'SOM-MSM1045815',	'SOM-MSM1045788',	'SOI-CTM1141689',	'SOM-MSM1045868',	'SOM-MSM1045829',	'SOI-CTM1141690',	'SOI-CON1014942',	'SOI-CTM1141503',	'SOI-CON1014971',	'SOM-MSM1045842',	'SOI-CTM1141590',	'SOI-CON1014959',	'SOI-MSM1151215',	'SOI-GOC1040060',	'SOI-CON1014960',	'SOI-MSM1151209',	'SOI-GOC1040075',	'SOI-GOC1040127',	'SOM-MSM1045843',	'SOI-CON1014943',	'SOI-GOC1040076',	'SOI-CTM1141504',	'SOI-CTM1141540',	'SOI-CTM1141662',	'SOI-MSM1151163',	'SOI-CTM1141460',	'SOI-CTM1141663',	'SOI-GOC1040136',	'SOI-GOC1040111',	'SOI-CTM1141591',	'SOI-CTM1141592',	'SOI-CTM1141705',	'SOI-CTM1141461',	'SOI-GOC1040112',	'SOI-GOC1040157',	'SOI-MSM1151164',	'SOM-MSM1045862',	'SOI-CTM1141541',	'SOI-CTM1141629',	'SOI-MSM1151216',	'SOM-MSM1045870',	'SOI-CTM1141505',	'SOI-CTM1141506',	'SOM-MSM1045806',	'SOI-CTM1141462',	'SOI-MSM1151179',	'SOI-CTM1141593',	'SOI-CTM1141691',	'SOI-MSM1151231',	'SOI-CTM1141706',	'SOI-CTM1141594',	'SOI-CTM1141630',	'SOI-CTM1141542',	'SOI-CTM1141544',	'SOI-MSM1151210',	'SOI-CTM1141543',	'SOI-CTM1141463',	'SOM-MSM1045790',	'SOI-GOC1040077',	'SOI-DIR1022903',	'SOI-GOC1040150',	'SOI-CTM1141707',	'SOI-DIR1022906',	'SOI-CTM1141464',	'SOI-CTM1141665',	'SOI-CTM1141546',	'SOI-CTM1141631',	'SOI-CTM1141692',	'SOI-GOC1040151',	'SOI-MSM1151217',	'SOM-MSM1045792',	'SOM-MSM1045807',	'SOI-MSM1151233',	'SOI-CTM1141545',	'SOM-MTC1007884',	'SOM-MSM1045830',	'SOM-MSM1045831',	'SOI-CTM1141507',	'SOI-GOC1040089',	'SOI-CTM1141632',	'SOM-MSM1045817',	'SOI-GOC1040061',	'SOI-GOC1040062',	'SOI-CON1014985',	'SOI-GOC1040159',	'SOI-CTM1141508',	'SOI-CTM1141465',	'SOI-CON1014982',	'SOI-CTM1141708',	'SOI-CTM1141547',	'SOI-CTM1141466',	'SOI-CTM1141633',	'SOI-MSM1151166',	'SOM-MSM1045793',	'SOI-GOC1040078',	'SOI-CTM1141666',	'SOI-MSM1151218',	'SOM-MSM1045794',	'SOM-MSM1045832',	'SOI-CTM1141694',	'SOM-MSM1045863',	'SOI-CTM1141667',	'SOI-CTM1141695',	'SOM-MSM1045871',	'SOI-CTM1141696',	'SOI-CTM1141595',	'SOM-MSM1045844',	'SOI-CON1014961',	'SOI-CON1014949',	'SOI-GOC1040137',	'SOI-GOC1040138',	'SOI-MSM1151180',	'SOI-MSM1151219',	'SOI-GOC1040139',	'SOI-CTM1141697',	'SOI-MSM1151227',	'SOI-CTM1141548',	'SOI-MSM1151201',	'SOI-CTM1141634',	'SOI-MSM1151167',	'SOM-MSM1045808',	'SOM-MSM1045872',	'SOI-MSM1151189',	'SOI-CTM1141598',	'SOI-CTM1141636',	'SOI-GOC1040090',	'SOI-CON1014979',	'SOI-DIR1022893',	'SOI-GOC1040160',	'SOI-DIR1022919',	'SOI-CTM1141668',	'SOI-CTM1141635',	'SOI-GOC1040063',	'SOI-CTM1141698',	'SOM-MSM1045855',	'SOI-CTM1141553',	'SOI-GOC1040140',	'SOI-GOC1040161',	'SOI-CTM1141709',	'SOI-CTM1141710',	'SOI-CTM1141637',	'SOI-CTM1141744',	'SOI-CTM1141467',	'SOI-CTM1141597',	'SOI-CON1014980',	'SOI-MSM1151228',	'SOI-CTM1141745',	'SOI-CON1014984',	'SOI-DIR1022909',	'SOI-GOC1040162',	'SOI-DIR1022913',	'SOI-GOC1040163',	'SOI-GOC1040164',	'SOI-GOC1040114',	'SOI-CTM1141786',	'SOI-CTM1141549',	'SOI-GOC1040091',	'SOI-CON1014990',	'SOM-MSM1045864',	'SOI-GOC1040165',	'SOI-GOC1040166',	'SOM-MTC1007891',	'SOI-MSM1151229',	'SOI-CTM1141712',	'SOM-MSM1045879',	'SOI-MSM1151254',	'SOI-CTM1141746',	'SOI-CTM1141788',	'SOI-CTM1141787',	'SOI-GOC1040167',	'SOM-MSM1045833',	'SOM-MSM1045847',	'SOI-DIR1022922',	'SOI-CTM1141713',	'SOI-GOC1040324',	'SOI-CTM1141747',	'SOI-MSM1151190',	'SOM-MSM1046013',	'SOI-GOC1040325',	'SOI-CTM1141638',	'SOI-CTM1141550',	'SOI-DIR1022914',	'SOI-DIR1022967',	'SOM-MSM1045874',	'SOM-MSM1045818',	'SOI-CTM1142107',	'SOI-CON1014966',	'SOM-MSM1045846',	'SOI-GOC1040169',	'SOI-GOC1040092',	'SOI-GOC1040168',	'SOM-MSM1045885',	'SOI-CTM1142458',	'SOI-GOC1040199',	'SOI-CTM1142755',	'SOI-CTM1141748',	'SOI-CTM1142464',	'SOI-GOC1040518',	'SOM-MSM1045819',	'SOM-MSM1045901',	'SOI-CTM1141789',	'SOM-MSM1046241',	'SOI-GOC1040152',	'SOI-MSM1151241',	'SOI-GOC1040170',	'SOM-MSM1045900',	'SOI-DIR1023064',	'SOI-CTM1141715',	'SOI-CON1014950',	'SOI-GOC1040326',	'SOM-MSM1046242',	'SOM-MSM1045886',	'SOI-CTM1142459',	'SOI-CON1014951',	'SOI-CTM1141552',	'SOI-CTM1143070',	'SOM-MSM1046014',	'SOM-MTC1007928',	'SOI-CTM1141749',	'SOI-GOC1040153',	'SOM-MSM1046357',	'SOM-MSM1046132',	'SOM-MSM1046016',	'SOI-GOC1040185',	'SOI-MSM1151489',	'SOI-CTM1143071',	'SOI-CTM1141750',	'SOI-GOC1040519',	'SOI-CTM1142461',	'SOI-MSM1151255',	'SOI-MSM1151708',	'SOI-MSM1151243',	'SOI-CTM1141751',	'SOI-CTM1141752',	'SOI-CON1014986',	'SOI-GOC1040420',	'SOM-MSM1046243',	'SOI-CTM1142108',	'SOI-CTM1143086',	'SOI-CON1014987',	'SOI-CTM1142462',	'SOI-CTM1142756',	'SOI-CTM1142757',	'SOI-CTM1141716',	'SOI-CON1015056',	'SOI-MSM1151244',	'SOI-CTM1143072',	'SOI-MSM1151710',	'SOI-GOC1040619',	'SOI-DIR1022921',	'SOI-CTM1141700',	'SOI-GOC1040520',	'SOI-CTM1141721',	'SOI-MSM1151245',	'SOI-DIR1022923',	'SOI-MSM1151711',	'SOI-CTM1142758',	'SOI-DIR1022925',	'SOM-MSM1045887',	'SOI-CON1015150',	'SOI-CTM1142759',	'SOI-GOC1040154',	'SOI-CTM1142110',	'SOI-CTM1142760',	'SOI-CTM1141702',	'SOM-MSM1046244',	'SOI-GOC1040186',	'SOI-CTM1143073',	'SOI-CTM1141717',	'SOI-GOC1040187',	'SOI-CTM1142761',	'SOM-MSM1046245',	'SOI-MSM1151246',	'SOI-CTM1141701',	'SOI-CTM1142762',	'SOI-CON1014981',	'SOI-CTM1142763',	'SOI-GOC1040188',	'SOM-MSM1045903',	'SOI-GOC1040200',	'SOI-CTM1141718',	'SOI-GOC1040155',	'SOI-CTM1141753',	'SOI-MSM1151257',	'SOI-CTM1141754',	'SOM-MSM1046133',	'SOI-CTM1142114',	'SOI-CTM1142764',	'SOI-CTM1142463',	'SOI-CTM1142765',	'SOI-CON1015057',	'SOI-CTM1142766',	'SOI-CTM1142767',	'SOI-CTM1141704',	'SOI-GOC1040156',	'SOI-CTM1141703',	'SOM-MSM1045888',	'SOI-CTM1142111',	'SOI-DIR1022968',	'SOI-CTM1141790',	'SOI-CTM1141719',	'SOM-MSM1045867',	'SOI-CTM1142465',	'SOI-CTM1141720',	'SOI-CON1015101',	'SOI-DIR1023025',	'SOI-DIR1022910',	'SOI-DIR1022911',	'SOM-MSM1046445',	'SOI-GOC1040189',	'SOM-MTC1007900',	'SOI-CTM1142112',	'SOI-CTM1143365',	'SOI-CTM1142466',	'SOI-CON1014991',	'SOI-CTM1141755',	'SOI-GOC1040192',	'SOI-GOC1040190',	'SOI-GOC1040697',	'SOI-GOC1040328',	'SOM-MSM1045904',	'SOI-CON1014988',	'SOI-CTM1143367',	'SOI-CTM1143368',	'SOI-MSM1151370',	'SOI-CTM1143369',	'SOI-CTM1142768',	'SOI-CTM1142467',	'SOI-CTM1141722',	'SOI-MSM1151236',	'SOI-MSM1151247',	'SOM-MSM1046247',	'SOM-MSM1046446',	'SOI-GOC1040171',	'SOI-MSM1151712',	'SOI-MSM1151713',	'SOM-MSM1045905',	'SOI-GOC1040172',	'SOM-MSM1046135',	'SOI-DIR1022926',	'SOI-MSM1151248',	'SOI-DIR1022916',	'SOI-CTM1142113',	'SOI-GOC1040191',	'SOI-CTM1142116',	'SOI-CTM1141756',	'SOI-CTM1142468',	'SOI-CTM1141758',	'SOI-CTM1141791',	'SOM-MSM1046358',	'SOI-GOC1040620',	'SOI-CTM1141792',	'SOM-MSM1046359',	'SOI-CTM1142769',	'SOI-CTM1142771',	'SOI-CTM1141759',	'SOM-MSM1046447',	'SOI-CTM1142117',	'SOI-GOC1040330',	'SOI-CTM1141723',	'SOI-CTM1143370',	'SOI-CTM1142118',	'SOI-GOC1040173',	'SOM-MSM1045876',	'SOI-MSM1151714',	'SOI-CTM1142770',	'SOI-CTM1143074',	'SOI-DIR1023107',	'SOM-MSM1046248',	'SOM-MSM1046360',	'SOI-GOC1040201',	'SOI-CTM1143371',	'SOI-MSM1151249',	'SOM-MTC1007929',	'SOI-CTM1141725',	'SOM-MSM1046176',	'SOI-CTM1141760',	'SOI-CON1015194',	'SOM-MTC1008017',	'SOM-MSM1046249',	'SOM-MSM1046137',	'SOI-CTM1141724',	'SOI-CTM1142772',	'SOM-MSM1046136',	'SOM-MSM1046448',	'SOM-MSM1046449',	'SOI-GOC1040174',	'SOI-CTM1142119',	'SOI-GOC1040621',	'SOI-GOC1040175',	'SOI-CTM1142773',	'SOI-GOC1040698',	'SOI-CTM1143075',	'SOI-GOC1040193',	'SOI-CTM1142776',	'SOM-MTC1007904',	'SOI-MSM1151579',	'SOI-CTM1143076',	'SOM-MSM1046017',	'SOI-DIR1022917',	'SOI-GOC1040202',	'SOI-CTM1142120',	'SOI-CTM1143373',	'SOI-CTM1143077',	'SOI-GOC1040194',	'SOI-CTM1142469',	'SOM-MSM1046250',	'SOI-CTM1143372',	'SOI-CTM1141727',	'SOI-GOC1040176',	'SOI-CON1014992',	'SOI-CTM1142775',	'SOI-GOC1040699',	'SOI-CTM1143078',	'SOI-CTM1142121',	'SOI-GOC1040421',	'SOI-DIR1023026',	'SOM-MSM1045878',	'SOI-CTM1141728',	'SOI-MSM1151802',	'SOI-MSM1151716',	'SOI-DIR1022924',	'SOI-CTM1143079',	'SOI-GOC1040329',	'SOI-GOC1040195',	'SOI-CTM1143374',	'SOI-CTM1143080',	'SOI-MSM1151803',	'SOI-CON1015228',	'SOI-MSM1151238',	'SOI-CON1014989',	'SOI-GOC1040177',	'SOI-CTM1141761',	'SOI-CTM1141762',	'SOI-CTM1141729',	'SOI-CTM1141764',	'SOI-CTM1141731',	'SOI-CTM1141730',	'SOI-GOC1040422',	'SOI-CTM1141763',	'SOI-CON1015102',	'SOM-MSM1045891',	'SOI-CTM1142470',	'SOI-CTM1142471',	'SOM-MSM1045906',	'SOM-MSM1046139',	'SOI-CTM1143081',	'SOI-CTM1143082',	'SOI-CTM1141793',	'SOI-MSM1151717',	'SOI-CTM1142472',	'SOI-GOC1040179',	'SOI-GOC1040178',	'SOI-GOC1040521',	'SOI-DIR1022969',	'SOI-CTM1141732',	'SOI-MSM1151258',	'SOI-DIR1023138',	'SOI-CTM1141735',	'SOI-CTM1143375',	'SOI-CTM1143083',	'SOI-GOC1040180',	'SOI-MSM1151580',	'SOI-CTM1143376',	'SOI-CTM1143085',	'SOI-CTM1143084',	'SOM-MSM1046251',	'SOM-MSM1046361',	'SOI-CTM1141733',	'SOI-CTM1141734',	'SOI-GOC1040196',	'SOI-GOC1040331',	'SOI-MSM1151372',	'SOI-CTM1141736',	'SOM-MSM1046140',	'SOI-GOC1040700',	'SOI-MSM1151805',	'SOI-CTM1143087',	'SOI-CTM1142777',	'SOI-CTM1141766',	'SOM-MSM1046018',	'SOI-MSM1151259',	'SOM-MSM1045882',	'SOI-CTM1142473',	'SOM-MSM1046252',	'SOI-MSM1151373',	'SOI-MSM1151374',	'SOI-CTM1141767',	'SOI-CTM1143377',	'SOM-MSM1046453',	'SOI-CTM1141768',	'SOI-GOC1040701',	'SOI-CTM1143378',	'SOI-DIR1022927',	'SOI-CTM1141794',	'SOM-MTC1007930',	'SOI-GOC1040702',	'SOI-MSM1151807',	'SOM-MSM1045892',	'SOI-MSM1151375',	'SOI-CTM1143088',	'SOM-MSM1046454',	'SOI-CTM1141795',	'SOI-CTM1141796',	'SOI-CON1015151',	'SOI-GOC1040197',	'SOM-MSM1045893',	'SOI-MSM1151250',	'SOI-CTM1142474',	'SOI-MSM1151251',	'SOI-CTM1141769',	'SOI-CTM1142778',	'SOI-CTM1143379',	'SOI-CTM1141770',	'SOI-CTM1143089',	'SOI-CTM1143091',	'SOI-CTM1141774',	'SOI-CTM1143092',	'SOI-CTM1142779',	'SOI-MSM1151239',	'SOI-CTM1141797',	'SOI-CTM1143090',	'SOI-CTM1142475',	'SOI-CTM1142780',	'SOI-GOC1040423',	'SOI-MSM1151493',	'SOI-GOC1040424',	'SOI-GOC1040332',	'SOM-MSM1045883',	'SOI-GOC1040182',	'SOI-CTM1142781',	'SOI-MSM1151718',	'SOI-GOC1040522',	'SOI-CTM1141737',	'SOI-MSM1151260',	'SOI-DIR1023066',	'SOI-CON1014993',	'SOM-MSM1045894',	'SOM-MSM1046020',	'SOI-CTM1141738',	'SOI-CTM1141771',	'SOI-CTM1141739',	'SOI-GOC1040183',	'SOI-CTM1142782',	'SOI-MSM1151240',	'SOI-CTM1141741',	'SOI-CTM1141772',	'SOI-CTM1142122',	'SOI-CTM1141798',	'SOI-CTM1142783',	'SOI-GOC1040198',	'SOM-MTC1008018',	'SOI-MSM1151376',	'SOI-MSM1151581',	'SOI-CTM1142784',	'SOI-GOC1040622',	'SOI-CTM1141773',	'SOI-CTM1141740',	'SOM-MSM1045907',	'SOI-CTM1143380',	'SOM-MSM1046362',	'SOI-CTM1142795',	'SOM-MSM1045895',	'SOI-GOC1040184',	'SOI-GOC1040333',	'SOI-CTM1141780',	'SOI-DIR1022971',	'SOI-MSM1151261',	'SOM-MTC1007898',	'SOI-MSM1151252',	'SOI-CTM1141775',	'SOI-CTM1143093',	'SOI-MSM1151253',	'SOI-CTM1142123',	'SOI-DIR1023096',	'SOI-CON1015152',	'SOM-MTC1007902',	'SOI-CTM1141776',	'SOI-CON1014995',	'SOI-CTM1142124',	'SOI-CTM1143094',	'SOI-CTM1142786',	'SOI-CTM1143095',	'SOM-MSM1046141',	'SOI-CTM1143096',	'SOI-CTM1142125',	'SOM-MSM1046021',	'SOM-MSM1046540',	'SOI-CTM1142787',	'SOM-MTC1007997',	'SOM-MSM1045897',	'SOI-MSM1151377',	'SOI-MSM1151582',	'SOI-CTM1141800',	'SOM-MSM1046363',	'SOI-DIR1022972',	'SOI-CTM1141801',	'SOI-CTM1141802',	'SOI-GOC1040703',	'SOI-CTM1143381',	'SOI-MSM1151378',	'SOI-CTM1142127',	'SOM-MTC1007931',	'SOM-MSM1045908',	'SOI-MSM1151808',	'SOI-GOC1040523',	'SOI-DIR1023167',	'SOI-CTM1141777',	'SOM-MSM1046364',	'SOI-CTM1141803',	'SOI-CTM1142477',	'SOI-CTM1141778',	'SOM-MSM1045898',	'SOM-MSM1046142',	'SOI-CTM1142478',	'SOI-CTM1141804',	'SOI-MSM1151902',	'SOM-MSM1046541',	'SOI-DIR1023068',	'SOI-CTM1142128',	'SOI-CTM1142788',	'SOI-CTM1141779',	'SOM-MSM1046023',	'SOI-MSM1151810',	'SOM-MSM1046542',	'SOM-MSM1046024',	'SOI-CTM1143099',	'SOM-MSM1046456',	'SOI-CTM1143383',	'SOM-MSM1045909',	'SOI-CON1014996',	'SOI-GOC1040752',	'SOI-CTM1143384',	'SOI-CTM1141783',	'SOI-CTM1142789',	'SOI-CTM1142790',	'SOI-MSM1151379',	'SOI-CON1014997',	'SOI-CTM1141782',	'SOI-CTM1142792',	'SOI-CTM1141781',	'SOI-CTM1142791',	'SOI-CON1015058',	'SOI-CTM1142131',	'SOM-MSM1045899',	'SOI-GOC1040426',	'SOM-MSM1046143',	'SOI-CTM1142479',	'SOI-CTM1141784',	'SOM-MSM1046256',	'SOI-CTM1143101',	'SOI-CON1015229',	'SOI-CTM1142132',	'SOI-CTM1143572',	'SOI-CON1014994',	'SOI-CTM1141805',	'SOI-CTM1141785',	'SOI-CON1014998',	'SOI-CON1014999',	'SOI-CTM1141806',	'SOI-CTM1142480',	'SOI-CTM1143385',	'SOM-MTC1008019',	'SOI-CTM1142793',	'SOI-DIR1023202',	'SOI-CTM1143754',	'SOI-CTM1143386',	'SOI-GOC1040427',	'SOI-CTM1143103',	'SOI-MSM1151381',	'SOI-CTM1142133',	'SOM-MSM1046257',	'SOI-CON1015059',	'SOI-MSM1151382',	'SOI-MSM1151811',	'SOI-CTM1142794',	'SOI-CTM1143102',	'SOM-MSM1046635',	'SOM-MSM1046457',	'SOI-MSM1151583',	'SOI-CON1015195',	'SOM-MSM1046144',	'SOI-CTM1141807',	'SOM-MSM1046367',	'SOI-CTM1143573',	'SOI-MSM1151812',	'SOI-GOC1040704',	'SOI-CTM1142796',	'SOM-MSM1045910',	'SOI-CTM1143387',	'SOI-CTM1142134',	'SOI-GOC1040525',	'SOI-CTM1143755',	'SOI-CTM1142135',	'SOM-MSM1046145',	'SOI-CTM1143104',	'SOI-CTM1141808',	'SOI-CON1015153',	'SOI-DIR1023067',	'SOM-MTC1007973',	'SOI-CTM1142797',	'SOI-CTM1142798',	'SOI-MSM1151584',	'SOI-CTM1142799',	'SOI-MSM1151813',	'SOI-CTM1143105',	'SOI-CTM1143106',	'SOM-MSM1046146',	'SOI-CON1015196',	'SOI-CTM1142136',	'SOM-MSM1046458',	'SOI-MSM1151494',	'SOI-CTM1142481',	'SOI-CTM1142137',	'SOI-CTM1143574',	'SOM-MTC1007974',	'SOI-CTM1142138',	'SOI-GOC1040705',	'SOI-CTM1142483',	'SOI-CTM1143388',	'SOI-GOC1040753',	'SOI-CTM1141809',	'SOM-MSM1046258',	'SOI-GOC1040204',	'SOI-DIR1023276',	'SOI-DIR1023027',	'SOI-CTM1142801',	'SOI-GOC1040706',	'SOI-MSM1151814',	'SOI-GOC1040707',	'SOI-GOC1040428',	'SOM-MSM1046368',	'SOI-MSM1151585',	'SOM-MSM1046545',	'SOI-MSM1151263',	'SOI-DIR1022973',	'SOI-DIR1022974',	'SOI-CTM1142802',	'SOM-MSM1046369',	'SOM-MSM1046025',	'SOM-MSM1046026',	'SOI-CTM1141810',	'SOI-MSM1151719',	'SOI-CTM1142484',	'SOI-CTM1142142',	'SOI-CTM1142486',	'SOI-CTM1142485',	'SOI-CTM1143575',	'SOI-CTM1143107',	'SOI-CTM1142141',	'SOI-CON1015103',	'SOI-GOC1040429',	'SOI-MSM1151496',	'SOI-CTM1143389',	'SOI-CTM1142140',	'SOI-CTM1142143',	'SOM-MSM1046259',	'SOI-GOC1040334',	'SOI-CTM1143390',	'SOM-MSM1046371',	'SOM-MSM1046027',	'SOI-CTM1141811',	'SOI-CON1015104',	'SOI-GOC1040526',	'SOI-CTM1143391',	'SOI-CTM1143392',	'SOI-GOC1040708',	'SOI-GOC1040205',	'SOI-CTM1142487',	'SOI-CTM1143576',	'SOI-CON1015252',	'SOM-MTC1007951',	'SOI-CTM1143393',	'SOI-CTM1143394',	'SOI-GOC1040206',	'SOI-CTM1141812',	'SOI-CTM1142488',	'SOI-CON1015000',	'SOI-DIR1022929',	'SOI-CTM1143108',	'SOI-GOC1040335',	'SOI-CTM1142804',	'SOI-MSM1151720',	'SOI-CTM1141813',	'SOI-CTM1142489',	'SOI-MSM1151815',	'SOM-MSM1045912',	'SOI-GOC1040625',	'SOI-MSM1151816',	'SOI-CTM1143396',	'SOI-CTM1143395',	'SOM-MTC1007999',	'SOM-MSM1046459',	'SOM-MTC1007952',	'SOI-GOC1040336',	'SOI-CTM1142144',	'SOM-MTC1008000',	'SOM-MSM1046028',	'SOI-DIR1023205',	'SOM-MSM1046029',	'SOI-CTM1142490',	'SOM-MSM1045913',	'SOI-CON1015290',	'SOI-GOC1040754',	'SOI-GOC1040430',	'SOI-CTM1143397',	'SOI-CTM1142805',	'SOI-CTM1142145',	'SOI-CTM1143398',	'SOI-MSM1151817',	'SOI-DIR1023139',	'SOI-GOC1040626',	'SOI-DIR1023111',	'SOM-MTC1007975',	'SOI-CTM1143109',	'SOI-GOC1040709',	'SOI-CTM1143400',	'SOI-CTM1141814',	'SOI-CTM1143757',	'SOI-CON1015154',	'SOI-DIR1023204',	'SOI-MSM1152006',	'SOM-MSM1046030',	'SOI-CTM1143110',	'SOI-CTM1143111',	'SOI-CTM1143401',	'SOI-CTM1143402',	'SOI-CON1015253',	'SOI-CTM1143112',	'SOI-GOC1040627',	'SOI-CTM1143113',	'SOI-GOC1040710',	'SOI-CON1015197',	'SOI-CTM1143758',	'SOI-CTM1143577',	'SOI-CTM1142491',	'SOI-CTM1143759',	'SOI-DIR1023113',	'SOI-GOC1040207',	'SOI-CTM1141815',	'SOI-GOC1040337',	'SOM-MSM1046547',	'SOI-DIR1023069',	'SOI-CON1015230',	'SOI-CON1015231',	'SOM-MSM1046372',	'SOI-GOC1040431',	'SOI-GOC1040208',	'SOI-CTM1142806',	'SOI-CTM1143114',	'SOI-DIR1023112',	'SOI-CTM1143115',	'SOI-MSM1151265',	'SOI-MSM1151905',	'SOI-CTM1142492',	'SOI-DIR1022976',	'SOI-CTM1143578',	'SOI-CTM1142808',	'SOI-CTM1141816',	'SOI-CTM1142807',	'SOI-CTM1142493',	'SOI-GOC1040711',	'SOM-MSM1046031',	'SOI-MSM1151383',	'SOI-CTM1142146',	'SOI-CTM1143117',	'SOI-CTM1143116',	'SOI-DIR1023114',	'SOI-GOC1040712',	'SOI-CON1015001',	'SOM-MSM1045914',	'SOI-CTM1143118',	'SOI-MSM1151497',	'SOM-MSM1045915',	'SOI-GOC1040629',	'SOI-MSM1151384',	'SOI-CTM1141817',	'SOM-MTC1007976',	'SOI-CTM1143403',	'SOI-CTM1142810',	'SOI-CTM1142494',	'SOM-MSM1046032',	'SOI-DIR1023070',	'SOI-GOC1040209',	'SOI-CTM1141818',	'SOI-CTM1143120',	'SOI-CTM1142809',	'SOI-CTM1142811',	'SOI-CTM1142812',	'SOI-GOC1040338',	'SOI-CTM1143404',	'SOI-CON1015105',	'SOI-CTM1142495',	'SOM-MSM1045916',	'SOI-MSM1151906',	'SOI-CON1015254',	'SOI-MSM1151818',	'SOI-GOC1040713',	'SOM-MSM1046460',	'SOI-CTM1142148',	'SOI-CTM1142496',	'SOM-MTC1007905',	'SOI-DIR1022977',	'SOI-MSM1151498',	'SOI-CTM1141819',	'SOI-CTM1143760',	'SOI-CTM1141820',	'SOM-MTC1007953',	'SOM-MSM1046373',	'SOI-GOC1040824',	'SOI-DIR1022978',	'SOI-CTM1141821',	'SOI-MSM1151500',	'SOI-MSM1151819',	'SOI-DIR1022979',	'SOI-CTM1143579',	'SOI-CTM1141824',	'SOM-MSM1046033',	'SOI-CTM1142813',	'SOI-MSM1151820',	'SOI-MSM1151821',	'SOI-MSM1151588',	'SOI-CTM1142149',	'SOI-CON1015198',	'SOI-GOC1040527',	'SOI-CTM1142814',	'SOI-CTM1141822',	'SOI-CTM1142498',	'SOI-CTM1142499',	'SOI-CTM1142815',	'SOI-CTM1142816',	'SOI-MSM1151501',	'SOI-CTM1141823',	'SOM-MSM1046374',	'SOI-GOC1040755',	'SOI-CTM1143761',	'SOI-MSM1151502',	'SOI-CTM1143406',	'SOI-MSM1151907',	'SOI-MSM1151908',	'SOI-CTM1143762',	'SOM-MSM1046637',	'SOI-CTM1143405',	'SOI-CTM1143580',	'SOI-MSM1151385',	'SOM-MSM1046550',	'SOI-CON1015002',	'SOI-DIR1023206',	'SOM-MTC1008003',	'SOI-MSM1151386',	'SOI-CTM1142500',	'SOI-MSM1151503',	'SOI-GOC1040339',	'SOI-CTM1142501',	'SOI-CON1015106',	'SOI-CTM1142817',	'SOM-MSM1046034',	'SOI-MSM1151909',	'SOM-MSM1046461',	'SOI-CTM1142818',	'SOI-CTM1142151',	'SOI-CTM1142502',	'SOI-CON1015199',	'SOI-CTM1142150',	'SOI-CTM1141825',	'SOI-CTM1142819',	'SOI-CTM1142503',	'SOI-CTM1142504',	'SOI-CTM1142820',	'SOI-CON1015003',	'SOI-CTM1142505',	'SOI-CON1015107',	'SOI-MSM1151266',	'SOI-CTM1142506',	'SOI-MSM1151822',	'SOI-CTM1143407',	'SOI-CTM1142821',	'SOI-GOC1040714',	'SOI-GOC1040211',	'SOI-MSM1152008',	'SOI-GOC1040631',	'SOI-GOC1040528',	'SOI-CTM1143763',	'SOM-MSM1046462',	'SOI-CON1015061',	'SOM-MSM1046147',	'SOM-MSM1046261',	'SOI-CTM1141826',	'SOI-CTM1143764',	'SOI-CTM1142507',	'SOI-MSM1151823',	'SOI-CTM1143408',	'SOI-MSM1151505',	'SOI-MSM1151721',	'SOI-CTM1142823',	'SOI-GOC1040715',	'SOI-CTM1142824',	'SOI-CTM1143581',	'SOI-CTM1142918',	'SOI-GOC1040632',	'SOI-CTM1143765',	'SOI-CTM1142825',	'SOI-CTM1142826',	'SOI-CTM1142827',	'SOI-MSM1152009',	'SOI-CON1015232',	'SOI-GOC1040530',	'SOI-GOC1040212',	'SOI-DIR1022930',	'SOI-MSM1151910',	'SOI-CTM1143409',	'SOI-GOC1040529',	'SOI-CTM1141827',	'SOI-GOC1040634',	'SOM-MSM1046639',	'SOI-CTM1143582',	'SOI-GOC1040756',	'SOI-DIR1023074',	'SOI-CTM1143766',	'SOI-CTM1142152',	'SOI-CTM1143583',	'SOI-GOC1040531',	'SOI-CTM1142829',	'SOM-MSM1046378',	'SOI-DIR1022931',	'SOI-CTM1141828',	'SOI-CTM1143767',	'SOI-CTM1143121',	'SOI-CTM1142508',	'SOI-CTM1142509',	'SOI-CTM1143126',	'SOI-DIR1023028',	'SOI-CTM1142511',	'SOI-CTM1143768',	'SOI-CTM1142828',	'SOI-GOC1040716',	'SOI-CTM1142830',	'SOI-CTM1143122',	'SOI-CTM1143769',	'SOI-CTM1142153',	'SOI-CTM1143585',	'SOI-CTM1142154',	'SOI-DIR1022981',	'SOI-CTM1143771',	'SOI-CTM1143770',	'SOI-CTM1142510',	'SOI-CON1015292',	'SOI-CTM1143773',	'SOI-GOC1040532',	'SOI-GOC1040635',	'SOI-GOC1040533',	'SOI-CON1015156',	'SOI-CTM1143772',	'SOI-DIR1022985',	'SOM-MSM1046552',	'SOI-CTM1143411',	'SOM-MSM1046463',	'SOI-CTM1143123',	'SOM-MSM1046640',	'SOI-GOC1040213',	'SOM-MSM1046379',	'SOI-CTM1143124',	'SOI-CTM1141829',	'SOI-CTM1143774',	'SOM-MSM1046262',	'SOI-CTM1143125',	'SOI-CTM1143127',	'SOM-MTC1008020',	'SOI-MSM1151507',	'SOM-MSM1046149',	'SOI-MSM1151591',	'SOI-GOC1040432',	'SOI-MSM1151267',	'SOI-CTM1142155',	'SOI-CTM1143128',	'SOI-CTM1142512',	'SOI-DIR1023115',	'SOI-CTM1141830',	'SOI-CTM1143412',	'SOI-CTM1143130',	'SOI-MSM1151268',	'SOI-DIR1023029',	'SOI-CON1015157',	'SOI-CTM1142156',	'SOI-GOC1040214',	'SOI-CTM1143775',	'SOI-CTM1141832',	'SOM-MSM1046036',	'SOM-MSM1045918',	'SOI-CTM1143129',	'SOI-CTM1141831',	'SOI-GOC1040433',	'SOI-CTM1143586',	'SOM-MSM1046037',	'SOI-CON1015255',	'SOM-MSM1046464',	'SOM-MSM1046263',	'SOI-CON1015109',	'SOI-CTM1141833',	'SOI-GOC1040757',	'SOI-GOC1040341',	'SOI-CTM1141834',	'SOI-DIR1023116',	'SOI-DIR1023140',	'SOI-CON1015158',	'SOM-MSM1046553',	'SOI-GOC1040216',	'SOI-MSM1151724',	'SOM-MSM1046381',	'SOM-MSM1046038',	'SOI-CTM1143131',	'SOI-CTM1143777',	'SOI-CTM1143776',	'SOI-CTM1141835',	'SOI-DIR1023072',	'SOI-GOC1040534',	'SOI-CTM1143132',	'SOI-CON1015293',	'SOI-GOC1040215',	'SOI-GOC1040535',	'SOI-CTM1143413',	'SOI-CTM1142831',	'SOM-MSM1046151',	'SOM-MSM1045921',	'SOM-MSM1046039',	'SOI-MSM1151725',	'SOI-CTM1143133',	'SOI-DIR1023030',	'SOI-CTM1142832',	'SOM-MSM1045920',	'SOI-MSM1151824',	'SOI-CTM1143414',	'SOI-CTM1142513',	'SOI-CTM1143415',	'SOI-CTM1143416',	'SOI-GOC1040434',	'SOI-CTM1142514',	'SOI-MSM1151592',	'SOI-CTM1141836',	'SOI-MSM1151270',	'SOI-MSM1152010',	'SOI-GOC1040758',	'SOI-CTM1143587',	'SOI-CTM1141837',	'SOM-MSM1046554',	'SOI-GOC1040449',	'SOI-CTM1142515',	'SOI-MSM1151508',	'SOM-MSM1045922',	'SOI-CTM1143588',	'SOI-CTM1142833',	'SOI-CTM1143417',	'SOI-DIR1023031',	'SOI-MSM1151825',	'SOI-CTM1141838',	'SOI-CTM1143418',	'SOI-CTM1143134',	'SOI-CTM1141839',	'SOM-MSM1046152',	'SOI-GOC1040717',	'SOI-CTM1143135',	'SOI-CTM1143591',	'SOI-CTM1143590',	'SOI-CON1015160',	'SOI-CON1015294',	'SOI-CTM1142158',	'SOI-CON1015159',	'SOI-MSM1151390',	'SOI-CTM1141840',	'SOI-GOC1040342',	'SOI-CTM1142835',	'SOM-MSM1046555',	'SOI-CTM1141841',	'SOI-CTM1142531',	'SOM-MSM1046041',	'SOI-CTM1142159',	'SOI-CON1015005',	'SOI-DIR1023207',	'SOI-CON1015006',	'SOM-MSM1046153',	'SOM-MSM1046264',	'SOI-CTM1143136',	'SOI-GOC1040343',	'SOI-CTM1142516',	'SOI-MSM1151594',	'SOI-CTM1143137',	'SOM-MSM1046383',	'SOI-MSM1152011',	'SOM-MSM1046042',	'SOI-GOC1040436',	'SOM-MTC1008034',	'SOI-CTM1143138',	'SOI-CTM1141843',	'SOI-GOC1040435',	'SOM-MSM1046155',	'SOI-CTM1143592',	'SOI-CTM1143779',	'SOM-MSM1046265',	'SOI-GOC1040217',	'SOI-CTM1141844',	'SOI-CTM1143593',	'SOI-GOC1040344',	'SOI-CTM1142517',	'SOI-GOC1040825',	'SOI-GOC1040345',	'SOI-CTM1142518',	'SOI-CTM1143594',	'SOI-CTM1143419',	'SOM-MSM1046384',	'SOI-CON1015258',	'SOM-MSM1046465',	'SOI-CTM1143595',	'SOM-MSM1046043',	'SOI-GOC1040346',	'SOM-MSM1046266',	'SOI-MSM1151595',	'SOI-CTM1142160',	'SOI-GOC1040219',	'SOI-CTM1143420',	'SOI-DIR1022983',	'SOI-MSM1151391',	'SOM-MSM1046556',	'SOI-CTM1143780',	'SOI-CTM1142527',	'SOI-MSM1151274',	'SOI-MSM1151510',	'SOI-CTM1143596',	'SOI-CTM1143781',	'SOI-CTM1142521',	'SOI-CTM1142161',	'SOI-MSM1152014',	'SOI-GOC1040636',	'SOI-CTM1142522',	'SOI-CTM1143597',	'SOI-GOC1040637',	'SOI-CTM1142523',	'SOI-CTM1143139',	'SOI-MSM1151728',	'SOI-DIR1023169',	'SOI-GOC1040220',	'SOI-CTM1142524',	'SOI-MSM1151393',	'SOM-MSM1046044',	'SOI-CTM1142162',	'SOI-CTM1141845',	'SOI-DIR1023032',	'SOI-GOC1040348',	'SOM-MSM1045923',	'SOI-MSM1151911',	'SOI-CTM1142163',	'SOM-MSM1046156',	'SOM-MTC1008035',	'SOI-CTM1142525',	'SOM-MSM1046157',	'SOI-DIR1023073',	'SOI-GOC1040437',	'SOI-CTM1142164',	'SOI-GOC1040349',	'SOI-GOC1040350',	'SOI-CTM1142165',	'SOI-GOC1040439',	'SOI-GOC1040438',	'SOI-CTM1142526',	'SOI-CTM1142166',	'SOI-DIR1023141',	'SOM-MSM1045924',	'SOI-CTM1142836',	'SOI-CTM1143598',	'SOI-CTM1141846',	'SOI-MSM1151275',	'SOI-CTM1142837',	'SOI-MSM1151394',	'SOI-CTM1143141',	'SOI-GOC1040440',	'SOM-MSM1046046',	'SOI-MSM1151276',	'SOI-CTM1143782',	'SOI-CTM1141847',	'SOI-GOC1040759',	'SOI-CTM1142528',	'SOI-MSM1151596',	'SOI-GOC1040638',	'SOM-MTC1007908',	'SOI-CTM1143421',	'SOI-CTM1142529',	'SOM-MSM1046385',	'SOI-CTM1141849',	'SOI-CTM1142168',	'SOI-MSM1151511',	'SOM-MSM1046159',	'SOI-GOC1040639',	'SOI-CON1015110',	'SOI-CTM1142167',	'SOI-CTM1142838',	'SOM-MSM1045925',	'SOI-CTM1142169',	'SOI-CTM1143601',	'SOI-GOC1040222',	'SOI-CTM1142839',	'SOI-CTM1143600',	'SOM-MSM1046557',	'SOI-GOC1040441',	'SOI-CTM1142170',	'SOI-DIR1023145',	'SOI-CTM1143602',	'SOI-CTM1143142',	'SOI-CTM1143143',	'SOI-MSM1151598',	'SOI-MSM1151827',	'SOI-CTM1141851',	'SOI-CTM1143603',	'SOI-GOC1040442',	'SOI-CTM1142840',	'SOI-GOC1040351',	'SOI-CTM1143422',	'SOI-CTM1142530',	'SOI-CON1015111',	'SOI-GOC1040443',	'SOI-CTM1143604',	'SOI-CTM1142171',	'SOI-CTM1141850',	'SOI-CTM1143144',	'SOI-CTM1142172',	'SOI-CON1015063',	'SOI-MSM1151396',	'SOM-MTC1008006',	'SOI-GOC1040537',	'SOI-CTM1142532',	'SOI-CON1015112',	'SOI-GOC1040223',	'SOI-CTM1143605',	'SOI-CTM1142533',	'SOI-DIR1023142',	'SOI-GOC1040445',	'SOI-CON1015113',	'SOI-CTM1143423',	'SOI-MSM1151828',	'SOM-MSM1046160',	'SOI-CTM1142173',	'SOI-CTM1142175',	'SOI-CTM1143783',	'SOI-CTM1141852',	'SOI-GOC1040353',	'SOI-CTM1142174',	'SOI-CTM1141853',	'SOI-CTM1143145',	'SOI-CTM1142534',	'SOI-GOC1040536',	'SOM-MSM1046268',	'SOI-GOC1040760',	'SOI-DIR1023171',	'SOI-CTM1143146',	'SOI-CON1015161',	'SOI-CTM1142841',	'SOI-DIR1022932',	'SOI-MSM1151730',	'SOI-DIR1023170',	'SOI-MSM1151599',	'SOI-GOC1040538',	'SOM-MTC1007909',	'SOI-GOC1040467',	'SOI-CON1015114',	'SOI-MSM1151513',	'SOI-CTM1141855',	'SOI-GOC1040640',	'SOI-CTM1143147',	'SOI-GOC1040641',	'SOI-GOC1040354',	'SOI-DIR1022933',	'SOM-MTC1008007',	'SOI-GOC1040539',	'SOI-CTM1143148',	'SOM-MSM1046161',	'SOI-MSM1152015',	'SOI-CTM1143607',	'SOI-CTM1142537',	'SOI-CTM1143149',	'SOI-CTM1143150',	'SOI-CTM1143608',	'SOM-MSM1046559',	'SOI-GOC1040447',	'SOI-CTM1141856',	'SOI-CTM1142842',	'SOI-CON1015007',	'SOI-CTM1142535',	'SOI-CTM1142843',	'SOI-CTM1143609',	'SOI-CTM1142178',	'SOM-MTC1007910',	'SOI-CTM1143151',	'SOI-CTM1142844',	'SOM-MSM1046469',	'SOM-MSM1046642',	'SOI-CON1015008',	'SOM-MSM1046049',	'SOI-MSM1151514',	'SOI-CTM1142538',	'SOI-GOC1040761',	'SOI-CTM1142845',	'SOM-MSM1046048',	'SOM-MSM1046471',	'SOI-CTM1143424',	'SOI-CTM1141858',	'SOI-GOC1040225',	'SOI-CTM1142846',	'SOI-CTM1142179',	'SOI-MSM1151732',	'SOM-MSM1046162',	'SOI-MSM1151600',	'SOI-GOC1040226',	'SOI-CON1015009',	'SOI-MSM1151733',	'SOI-MSM1151734',	'SOI-CTM1143784',	'SOI-MSM1151831',	'SOI-MSM1151277',	'SOI-MSM1151735',	'SOI-CON1015115',	'SOI-CTM1142542',	'SOI-CON1015010',	'SOI-CTM1143152',	'SOI-CTM1142540',	'SOI-CTM1142847',	'SOI-CTM1142848',	'SOM-MSM1046472',	'SOI-CTM1143611',	'SOI-CTM1141859',	'SOI-CTM1142849',	'SOI-CTM1142539',	'SOI-MSM1152017',	'SOI-CTM1143786',	'SOI-GOC1040826',	'SOI-CTM1142180',	'SOI-CTM1142181',	'SOM-MSM1046473',	'SOI-CTM1142541',	'SOI-CTM1142182',	'SOI-CON1015116',	'SOI-DIR1023034',	'SOI-MSM1151278',	'SOM-MSM1045929',	'SOI-MSM1152018',	'SOI-GOC1040762',	'SOI-GOC1040642',	'SOI-MSM1152019',	'SOI-CTM1143612',	'SOI-CTM1142183',	'SOI-CTM1142543',	'SOI-GOC1040827',	'SOI-CTM1143788',	'SOI-MSM1151279',	'SOI-CTM1141860',	'SOI-CTM1142184',	'SOI-CTM1141862',	'SOI-CTM1143153',	'SOM-MSM1046560',	'SOI-CTM1141861',	'SOI-MSM1151736',	'SOI-MSM1151737',	'SOM-MSM1046561',	'SOI-CTM1142544',	'SOI-CTM1143425',	'SOI-MSM1151833',	'SOI-CTM1143614',	'SOI-CTM1142850',	'SOI-MSM1151399',	'SOI-CTM1143613',	'SOI-MSM1151601',	'SOM-MTC1008036',	'SOI-MSM1151400',	'SOM-MSM1045930',	'SOI-GOC1040355',	'SOM-MSM1046643',	'SOI-CTM1143790',	'SOI-CTM1141863',	'SOI-CTM1142546',	'SOI-MSM1151834',	'SOI-GOC1040828',	'SOI-MSM1151602',	'SOI-MSM1151603',	'SOI-CTM1142851',	'SOI-CTM1141864',	'SOM-MSM1046050',	'SOI-CTM1142545',	'SOI-GOC1040643',	'SOI-CTM1141865',	'SOI-CON1015295',	'SOI-CTM1142547',	'SOI-CTM1143154',	'SOI-MSM1151738',	'SOM-MTC1007912',	'SOI-CTM1143426',	'SOM-MSM1046474',	'SOI-CTM1141866',	'SOM-MSM1046166',	'SOI-MSM1151835',	'SOI-DIR1023208',	'SOM-MSM1046475',	'SOI-CTM1142185',	'SOI-MSM1151917',	'SOI-MSM1151836',	'SOI-MSM1151516',	'SOM-MSM1045931',	'SOI-CTM1142852',	'SOI-CTM1142548',	'SOI-GOC1040356',	'SOI-DIR1023172',	'SOI-CON1015117',	'SOI-CTM1142853',	'SOI-GOC1040763',	'SOM-MSM1046645',	'SOI-CTM1143427',	'SOI-MSM1151517',	'SOI-CTM1142854',	'SOM-MSM1046562',	'SOM-MSM1046168',	'SOI-DIR1023144',	'SOI-CTM1143156',	'SOI-CTM1142186',	'SOM-MSM1046052',	'SOI-MSM1152020',	'SOM-MSM1046169',	'SOI-CTM1143791',	'SOM-MSM1045934',	'SOM-MSM1046051',	'SOI-CTM1143428',	'SOI-CTM1143157',	'SOI-GOC1040227',	'SOI-CTM1142549',	'SOI-CTM1143615',	'SOI-CON1015233',	'SOI-CTM1143158',	'SOM-MSM1046646',	'SOI-MSM1151837',	'SOI-MSM1151838',	'SOI-GOC1040764',	'SOI-GOC1040644',	'SOI-CTM1143159',	'SOI-CTM1143160',	'SOI-MSM1151519',	'SOI-MSM1151280',	'SOI-CTM1141869',	'SOM-MSM1046053',	'SOI-CTM1142550',	'SOI-GOC1040765',	'SOM-MSM1046054',	'SOI-CTM1143161',	'SOI-CTM1141870',	'SOM-MSM1046564',	'SOI-CTM1142187',	'SOI-CTM1143792',	'SOI-CTM1142551',	'SOM-MTC1008022',	'SOI-CTM1143162',	'SOI-CTM1142188',	'SOM-MSM1046647',	'SOI-MSM1152021',	'SOM-MSM1046170',	'SOI-CTM1143616',	'SOI-CTM1142552',	'SOI-CTM1141871',	'SOM-MSM1046386',	'SOI-GOC1040357',	'SOI-MSM1151282',	'SOI-MSM1152022',	'SOI-GOC1040228',	'SOI-CTM1142189',	'SOM-MSM1046565',	'SOI-GOC1040540',	'SOI-CTM1142553',	'SOI-MSM1151606',	'SOI-CTM1142856',	'SOM-MSM1046476',	'SOI-GOC1040358',	'SOI-MSM1151401',	'SOM-MSM1045935',	'SOM-MSM1046269',	'SOI-CTM1143429',	'SOI-CTM1143795',	'SOI-CTM1143794',	'SOI-CTM1143164',	'SOI-CTM1143163',	'SOI-DIR1023210',	'SOI-CTM1143165',	'SOI-CTM1142191',	'SOI-CTM1142555',	'SOM-MSM1045936',	'SOI-MSM1152023',	'SOI-GOC1040229',	'SOI-MSM1151919',	'SOM-MSM1046171',	'SOI-DIR1022984',	'SOI-CTM1142192',	'SOI-MSM1152024',	'SOM-MSM1045937',	'SOI-CON1015064',	'SOI-CTM1143617',	'SOI-CTM1142554',	'SOI-GOC1040230',	'SOM-MSM1046566',	'SOI-MSM1151283',	'SOI-CTM1142857',	'SOI-CTM1142193',	'SOI-DIR1023035',	'SOI-CTM1141872',	'SOI-CTM1142194',	'SOI-CON1015012',	'SOI-CTM1143167',	'SOI-CTM1143796',	'SOI-CTM1143430',	'SOI-CTM1141873',	'SOM-MSM1046387',	'SOI-CTM1143618',	'SOI-GOC1040453',	'SOI-CTM1143166',	'SOI-MSM1151402',	'SOI-GOC1040451',	'SOI-GOC1040359',	'SOM-MSM1045938',	'SOI-CTM1142556',	'SOM-MSM1046567',	'SOI-CTM1142859',	'SOI-CON1015118',	'SOI-CTM1142858',	'SOI-GOC1040360',	'SOI-CTM1141874',	'SOI-CTM1141875',	'SOI-CTM1143619',	'SOI-GOC1040452',	'SOI-CTM1143620',	'SOI-DIR1023173',	'SOI-CON1015260',	'SOI-CTM1141876',	'SOM-MTC1007913',	'SOI-DIR1023047',	'SOI-CTM1143169',	'SOI-CTM1143797',	'SOI-CTM1142860',	'SOM-MSM1046270',	'SOI-CTM1143621',	'SOM-MTC1008008',	'SOI-CTM1141877',	'SOM-MSM1046271',	'SOM-MSM1046478',	'SOI-GOC1040231',	'SOI-CTM1141878',	'SOM-MSM1046479',	'SOI-CTM1142862',	'SOM-MSM1046056',	'SOI-GOC1040233',	'SOI-CTM1143171',	'SOM-MSM1046057',	'SOI-CTM1141879',	'SOI-CON1015261',	'SOI-DIR1023174',	'SOI-GOC1040646',	'SOI-GOC1040232',	'SOI-GOC1040234',	'SOI-GOC1040454',	'SOI-CON1015262',	'SOI-MSM1151403',	'SOI-CTM1142861',	'SOI-CTM1142863',	'SOI-CTM1143798',	'SOI-MSM1152025',	'SOI-CTM1141880',	'SOI-CTM1142557',	'SOI-GOC1040829',	'SOI-DIR1023175',	'SOI-CTM1143170',	'SOI-GOC1040456',	'SOI-MSM1152026',	'SOI-MSM1151284',	'SOI-CTM1142195',	'SOI-GOC1040718',	'SOM-MSM1046172',	'SOM-MSM1046568',	'SOI-GOC1040830',	'SOI-DIR1023176',	'SOI-GOC1040541',	'SOI-CTM1143431',	'SOI-GOC1040236',	'SOI-GOC1040235',	'SOI-CON1015162',	'SOI-MSM1151607',	'SOM-MSM1046569',	'SOI-CTM1143173',	'SOI-MSM1151608',	'SOI-CTM1142865',	'SOI-DIR1023036',	'SOI-CTM1143432',	'SOI-DIR1023189',	'SOI-GOC1040831',	'SOI-DIR1023213',	'SOI-CTM1142559',	'SOI-CON1015234',	'SOI-CTM1143434',	'SOI-CTM1143433',	'SOM-MSM1045941',	'SOI-CTM1142558',	'SOI-MSM1152027',	'SOI-GOC1040361',	'SOI-MSM1152028',	'SOI-GOC1040542',	'SOI-CTM1141881',	'SOI-CTM1142196',	'SOI-DIR1023177',	'SOM-MSM1046273',	'SOI-CTM1142197',	'SOI-GOC1040237',	'SOI-CTM1141882',	'SOI-CON1015119',	'SOI-CTM1142198',	'SOI-MSM1151841',	'SOI-CTM1141883',	'SOI-GOC1040362',	'SOI-CTM1142200',	'SOI-GOC1040719',	'SOI-CTM1142201',	'SOI-GOC1040238',	'SOI-CTM1142560',	'SOI-MSM1151920',	'SOI-MSM1151842',	'SOI-CON1015120',	'SOM-MSM1045942',	'SOI-MSM1151405',	'SOI-CTM1142561',	'SOM-MSM1046173',	'SOI-CTM1141884',	'SOI-CTM1143622',	'SOI-CTM1142202',	'SOM-MSM1046174',	'SOI-CTM1142866',	'SOI-CTM1142867',	'SOI-CTM1142203',	'SOI-MSM1151921',	'SOI-GOC1040766',	'SOI-CTM1142868',	'SOI-CON1015263',	'SOI-GOC1040239',	'SOI-CTM1143624',	'SOI-MSM1152029',	'SOI-DIR1022986',	'SOI-GOC1040720',	'SOI-GOC1040363',	'SOI-DIR1023037',	'SOI-MSM1152030',	'SOI-CTM1141885',	'SOI-CON1015121',	'SOI-CTM1141886',	'SOM-MSM1046175',	'SOI-CTM1143174',	'SOI-GOC1040767',	'SOI-CTM1143435',	'SOI-CTM1142204',	'SOM-MSM1046571',	'SOI-CTM1142869',	'SOI-CTM1142870',	'SOI-CTM1142563',	'SOI-CTM1143799',	'SOI-CTM1143175',	'SOM-MSM1046572',	'SOI-CTM1142562',	'SOI-CTM1143625',	'SOI-CTM1143436',	'SOI-MSM1152031',	'SOI-GOC1040364',	'SOM-MTC1007938',	'SOM-MSM1045945',	'SOI-MSM1151844',	'SOM-MSM1046058',	'SOI-MSM1152032',	'SOI-GOC1040832',	'SOI-CTM1143800',	'SOI-CON1015264',	'SOI-CTM1142205',	'SOI-MSM1151845',	'SOI-CTM1142564',	'SOI-MSM1151406',	'SOI-DIR1023119',	'SOI-DIR1022935',	'SOI-CTM1143801',	'SOM-MSM1046480',	'SOI-MSM1151520',	'SOI-CTM1143806',	'SOI-DIR1023178',	'SOI-CTM1143437',	'SOI-CTM1142206',	'SOM-MSM1046277',	'SOI-CTM1143627',	'SOI-CTM1142566',	'SOI-CTM1143438',	'SOM-MSM1046177',	'SOM-MSM1045946',	'SOI-GOC1040458',	'SOI-CTM1143176',	'SOI-CON1015122',	'SOM-MSM1046178',	'SOI-GOC1040647',	'SOI-MSM1151740',	'SOI-GOC1040365',	'SOI-GOC1040721',	'SOM-MTC1008025',	'SOI-MSM1151407',	'SOI-CTM1143802',	'SOI-CTM1143803',	'SOI-MSM1152033',	'SOI-CTM1143177',	'SOI-CTM1142871',	'SOI-GOC1040240',	'SOI-GOC1040543',	'SOI-CTM1143440',	'SOI-GOC1040241',	'SOM-MSM1046278',	'SOI-GOC1040544',	'SOI-GOC1040834',	'SOI-GOC1040459',	'SOI-GOC1040768',	'SOI-CTM1143805',	'SOI-CTM1141888',	'SOI-DIR1023190',	'SOI-DIR1022987',	'SOM-MSM1046279',	'SOI-MSM1152034',	'SOI-CTM1141889',	'SOI-CTM1142208',	'SOI-CON1015123',	'SOI-CTM1143804',	'SOI-GOC1040769',	'SOI-CTM1142872',	'SOM-MSM1046280',	'SOI-CON1015163',	'SOI-CTM1142873',	'SOM-MSM1046388',	'SOI-CTM1142568',	'SOI-CON1015124',	'SOM-MSM1046180',	'SOI-CTM1143441',	'SOI-CON1015236',	'SOM-MTC1008026',	'SOI-GOC1040770',	'SOI-CON1015164',	'SOI-CTM1143629',	'SOM-MSM1046649',	'SOI-CTM1141890',	'SOI-CON1015065',	'SOI-CTM1142209',	'SOM-MSM1046574',	'SOI-MSM1152035',	'SOI-CON1015296',	'SOI-CTM1143807',	'SOI-MSM1151846',	'SOM-MSM1046181',	'SOI-CON1015017',	'SOI-CTM1143808',	'SOI-MSM1151925',	'SOI-CTM1143442',	'SOI-CTM1143178',	'SOI-DIR1023146',	'SOM-MSM1046483',	'SOI-GOC1040835',	'SOI-GOC1040836',	'SOM-MSM1045948',	'SOI-CTM1143179',	'SOI-CTM1142569',	'SOI-CTM1141892',	'SOI-MSM1152036',	'SOI-GOC1040461',	'SOI-CON1015265',	'SOM-MSM1045947',	'SOI-CTM1143809',	'SOI-CTM1143810',	'SOI-DIR1022988',	'SOI-CTM1143180',	'SOM-MSM1046182',	'SOM-MSM1046061',	'SOM-MSM1046062',	'SOI-CON1015013',	'SOI-CTM1141893',	'SOI-CON1015014',	'SOI-CTM1142570',	'SOM-MSM1046183',	'SOI-GOC1040242',	'SOM-MSM1046282',	'SOI-CTM1142571',	'SOI-CTM1143443',	'SOI-GOC1040722',	'SOI-GOC1040462',	'SOI-CTM1142573',	'SOI-GOC1040648',	'SOI-GOC1040463',	'SOI-CTM1142572',	'SOI-DIR1022936',	'SOI-GOC1040837',	'SOI-CTM1142574',	'SOI-CTM1142210',	'SOI-DIR1023038',	'SOI-CON1015016',	'SOI-CON1015015',	'SOI-DIR1022989',	'SOI-CON1015297',	'SOI-CTM1142874',	'SOI-CTM1143631',	'SOM-MSM1045952',	'SOM-MSM1046484',	'SOI-MSM1151287',	'SOI-CON1015066',	'SOI-MSM1151610',	'SOI-CTM1143444',	'SOI-DIR1023121',	'SOI-MSM1151611',	'SOI-MSM1152037',	'SOI-GOC1040546',	'SOI-MSM1151521',	'SOM-MSM1046284',	'SOI-GOC1040771',	'SOI-CON1015237',	'SOM-MSM1046184',	'SOM-MSM1046576',	'SOI-CTM1141894',	'SOI-MSM1151612',	'SOM-MSM1045950',	'SOM-MSM1046287',	'SOI-CTM1142877',	'SOM-MSM1045951',	'SOI-CTM1143181',	'SOI-CTM1142875',	'SOI-CTM1142876',	'SOI-MSM1151926',	'SOI-CTM1142212',	'SOI-GOC1040243',	'SOI-MSM1151522',	'SOI-MSM1151523',	'SOI-CTM1142575',	'SOI-CTM1143812',	'SOI-CON1015125',	'SOI-CON1015238',	'SOI-CTM1142878',	'SOI-DIR1023179',	'SOI-CTM1143813',	'SOI-CTM1142213',	'SOM-MSM1046286',	'SOI-GOC1040464',	'SOI-MSM1151849',	'SOI-CTM1142576',	'SOM-MSM1046288',	'SOM-MSM1046285',	'SOI-CON1015266',	'SOM-MSM1046579',	'SOI-CTM1141895',	'SOM-MSM1046485',	'SOM-MSM1046289',	'SOI-CTM1142214',	'SOI-CTM1143446',	'SOI-CTM1142879',	'SOM-MSM1045953',	'SOI-CTM1142215',	'SOI-CON1015067',	'SOM-MSM1045954',	'SOI-CTM1143445',	'SOI-CTM1143633',	'SOI-CTM1143182',	'SOI-CTM1142217',	'SOM-MTC1008049',	'SOI-CTM1142218',	'SOI-MSM1151614',	'SOI-CTM1141896',	'SOI-CTM1143634',	'SOI-GOC1040465',	'SOI-CTM1143183',	'SOI-MSM1152038',	'SOI-CTM1143447',	'SOI-CTM1141897',	'SOI-MSM1151615',	'SOI-CTM1143635',	'SOI-DIR1023147',	'SOI-CON1015200',	'SOM-MSM1046290',	'SOM-MSM1046486',	'SOI-DIR1022990',	'SOI-CTM1142219',	'SOI-CTM1143814',	'SOM-MSM1046650',	'SOI-MSM1151409',	'SOM-MSM1046651',	'SOI-CTM1143636',	'SOI-CTM1142220',	'SOI-CTM1142577',	'SOI-GOC1040548',	'SOI-CTM1143184',	'SOM-MSM1046580',	'SOI-GOC1040547',	'SOI-CTM1142578',	'SOI-GOC1040839',	'SOI-MSM1151927',	'SOI-CTM1141898',	'SOI-CTM1141899',	'SOI-DIR1023180',	'SOI-GOC1040466',	'SOM-MSM1046391',	'SOM-MSM1045956',	'SOM-MSM1046291',	'SOM-MSM1046292',	'SOI-CTM1142580',	'SOM-MSM1046487',	'SOI-MSM1151616',	'SOI-CTM1142579',	'SOI-CTM1141900',	'SOI-CTM1142880',	'SOI-CTM1142583',	'SOI-CTM1142221',	'SOI-MSM1151410',	'SOI-MSM1151928',	'SOI-CTM1143185',	'SOI-MSM1151526',	'SOI-CON1015126',	'SOM-MTC1008050',	'SOM-MTC1008027',	'SOI-CTM1142881',	'SOI-CTM1141901',	'SOI-CTM1143449',	'SOI-CTM1143186',	'SOI-DIR1022991',	'SOM-MSM1046293',	'SOI-CTM1142222',	'SOI-MSM1151411',	'SOI-CTM1142582',	'SOI-CTM1142581',	'SOM-MSM1046294',	'SOI-DIR1022992',	'SOI-GOC1040772',	'SOI-CTM1142882',	'SOI-CTM1141903',	'SOI-MSM1151617',	'SOI-CTM1141902',	'SOI-MSM1151618',	'SOI-CTM1142883',	'SOM-MSM1046652',	'SOI-CTM1143450',	'SOI-CTM1142884',	'SOI-CTM1143187',	'SOI-DIR1023039',	'SOI-CTM1141905',	'SOI-MSM1151850',	'SOI-CTM1143451',	'SOI-GOC1040773',	'SOI-CTM1142584',	'SOI-CTM1142224',	'SOI-CTM1143815',	'SOI-CON1015239',	'SOI-CTM1142886',	'SOI-CTM1142887',	'SOI-MSM1151621',	'SOI-CTM1142888',	'SOI-CTM1143188',	'SOM-MSM1046187',	'SOI-CON1015165',	'SOI-CTM1142585',	'SOI-CTM1142889',	'SOI-GOC1040368',	'SOI-CTM1141906',	'SOI-CTM1142586',	'SOM-MSM1046188',	'SOI-CON1015166',	'SOI-CTM1143638',	'SOM-MSM1046582',	'SOM-MTC1007914',	'SOI-CTM1142890',	'SOI-CTM1142225',	'SOI-CTM1143639',	'SOI-MSM1151528',	'SOI-CTM1142226',	'SOI-CTM1141907',	'SOI-MSM1151929',	'SOI-CTM1143189',	'SOI-MSM1152039',	'SOI-CON1015127',	'SOI-MSM1151743',	'SOM-MSM1046653',	'SOI-CTM1142587',	'SOI-DIR1023082',	'SOI-CTM1142893',	'SOI-CTM1143190',	'SOI-CTM1142588',	'SOI-CTM1142589',	'SOI-CON1015298',	'SOM-MSM1046654',	'SOI-GOC1040369',	'SOM-MSM1046063',	'SOI-GOC1040723',	'SOI-CTM1143640',	'SOI-CON1015267',	'SOI-CTM1143817',	'SOI-GOC1040649',	'SOI-MSM1151622',	'SOI-CTM1142894',	'SOM-MSM1045957',	'SOI-MSM1151930',	'SOI-CTM1143641',	'SOI-CTM1143452',	'SOM-MSM1046655',	'SOI-MSM1151744',	'SOI-MSM1151851',	'SOI-CTM1141910',	'SOI-CTM1143642',	'SOI-CTM1142590',	'SOI-CON1015128',	'SOI-CTM1142591',	'SOI-CON1015202',	'SOI-GOC1040244',	'SOI-CTM1143643',	'SOI-CTM1142227',	'SOI-CTM1143818',	'SOI-MSM1151852',	'SOI-GOC1040724',	'SOI-CTM1143453',	'SOI-CTM1142895',	'SOI-CTM1143644',	'SOI-MSM1151931',	'SOI-CTM1141911',	'SOM-MSM1046190',	'SOI-CTM1142228',	'SOI-CTM1143454',	'SOM-MSM1046191',	'SOI-GOC1040550',	'SOI-CTM1143191',	'SOI-MSM1151745',	'SOI-CON1015240',	'SOM-MSM1046584',	'SOI-CON1015129',	'SOI-CTM1143193',	'SOI-CTM1141913',	'SOI-CTM1142896',	'SOI-CTM1142592',	'SOI-MSM1152041',	'SOM-MSM1046192',	'SOI-CTM1141912',	'SOI-CTM1142897',	'SOI-CTM1143645',	'SOI-CON1015167',	'SOI-MSM1151626',	'SOM-MSM1046064',	'SOI-CTM1142899',	'SOI-CTM1142230',	'SOI-CTM1142231',	'SOI-CTM1141914',	'SOI-CTM1143647',	'SOI-CTM1143646',	'SOI-GOC1040842',	'SOM-MSM1045958',	'SOM-MSM1045959',	'SOI-GOC1040468',	'SOI-MSM1151932',	'SOI-GOC1040469',	'SOI-MSM1151529',	'SOI-DIR1023040',	'SOI-CTM1143192',	'SOM-MTC1008009',	'SOI-CTM1141915',	'SOI-CTM1143819',	'SOI-CTM1142593',	'SOI-CTM1143648',	'SOI-GOC1040725',	'SOI-CTM1143194',	'SOI-DIR1023148',	'SOM-MTC1007939',	'SOI-CTM1142232',	'SOI-MSM1152042',	'SOI-GOC1040776',	'SOI-GOC1040775',	'SOI-CTM1142594',	'SOI-DIR1023181',	'SOI-GOC1040843',	'SOI-CTM1142900',	'SOI-CTM1143649',	'SOI-CTM1142595',	'SOI-CTM1141916',	'SOI-GOC1040652',	'SOI-GOC1040651',	'SOI-CTM1142234',	'SOI-GOC1040844',	'SOI-MSM1152043',	'SOI-CTM1143455',	'SOI-CON1015204',	'SOI-CTM1141917',	'SOI-CTM1143456',	'SOI-CTM1142233',	'SOI-CTM1142901',	'SOI-DIR1023214',	'SOI-GOC1040845',	'SOM-MSM1046066',	'SOI-GOC1040726',	'SOI-DIR1023078',	'SOI-CTM1142596',	'SOI-CON1015268',	'SOM-MTC1007980',	'SOI-CTM1143820',	'SOI-CTM1142597',	'SOI-CTM1142902',	'SOI-GOC1040553',	'SOI-MSM1151746',	'SOI-CTM1143457',	'SOI-CTM1142598',	'SOM-MSM1046585',	'SOI-CTM1143196',	'SOI-CON1015018',	'SOI-CTM1141918',	'SOI-GOC1040470',	'SOI-CTM1143197',	'SOI-CON1015205',	'SOI-CON1015168',	'SOI-CTM1142235',	'SOM-MSM1046065',	'SOI-CTM1142599',	'SOI-CTM1143821',	'SOI-MSM1151627',	'SOI-GOC1040245',	'SOI-GOC1040777',	'SOI-CTM1143650',	'SOM-MSM1046298',	'SOI-CTM1143198',	'SOI-CTM1143651',	'SOI-CTM1143199',	'SOI-CTM1142906',	'SOI-GOC1040846',	'SOI-CTM1143200',	'SOM-MSM1046488',	'SOM-MSM1046657',	'SOM-MSM1046490',	'SOI-CTM1142903',	'SOI-CTM1143652',	'SOI-CTM1142905',	'SOM-MSM1046299',	'SOI-CTM1141919',	'SOI-MSM1151531',	'SOM-MSM1046193',	'SOI-DIR1022939',	'SOM-MSM1046489',	'SOI-CTM1142600',	'SOI-CTM1142904',	'SOI-CON1015169',	'SOI-GOC1040554',	'SOM-MSM1045960',	'SOI-CTM1142236',	'SOI-DIR1023079',	'SOI-CTM1143201',	'SOI-CON1015130',	'SOM-MSM1046491',	'SOI-GOC1040653',	'SOI-MSM1151290',	'SOI-MSM1151291',	'SOI-GOC1040555',	'SOI-CTM1142237',	'SOM-MSM1046659',	'SOI-CTM1141920',	'SOI-MSM1151292',	'SOI-CTM1143653',	'SOI-CTM1143654',	'SOI-CTM1142907',	'SOI-MSM1151415',	'SOM-MSM1046586',	'SOI-CTM1143823',	'SOI-MSM1152044',	'SOI-CTM1143824',	'SOI-MSM1151532',	'SOI-CTM1142601',	'SOI-CTM1142238',	'SOI-CTM1143825',	'SOM-MSM1046194',	'SOI-MSM1151933',	'SOI-MSM1151747',	'SOI-MSM1151934',	'SOI-CTM1142240',	'SOI-MSM1151629',	'SOM-MSM1046492',	'SOI-MSM1151293',	'SOI-CTM1142910',	'SOM-MSM1046587',	'SOI-MSM1151748',	'SOM-MSM1046660',	'SOI-MSM1151534',	'SOM-MSM1046661',	'SOI-CTM1142908',	'SOI-CTM1143655',	'SOI-MSM1151935',	'SOI-CTM1143202',	'SOI-MSM1152045',	'SOI-GOC1040727',	'SOI-CTM1142602',	'SOI-CTM1141921',	'SOI-CTM1142909',	'SOI-MSM1151854',	'SOI-GOC1040847',	'SOI-CTM1143458',	'SOI-CTM1141925',	'SOI-GOC1040471',	'SOI-CTM1142603',	'SOI-CTM1142604',	'SOI-CTM1142241',	'SOM-MSM1046393',	'SOM-MSM1046067',	'SOI-MSM1151535',	'SOI-CON1015170',	'SOI-GOC1040472',	'SOI-MSM1151536',	'SOI-GOC1040473',	'SOI-CTM1142605',	'SOI-GOC1040848',	'SOI-CTM1142242',	'SOI-DIR1023215',	'SOM-MSM1046394',	'SOI-GOC1040556',	'SOI-DIR1022995',	'SOM-MSM1046301',	'SOI-CTM1142606',	'SOI-CTM1142607',	'SOI-GOC1040372',	'SOI-MSM1151936',	'SOI-GOC1040557',	'SOM-MTC1008010',	'SOI-CTM1142911',	'SOI-CON1015069',	'SOI-CTM1142243',	'SOI-CTM1143826',	'SOM-MSM1046662',	'SOI-GOC1040728',	'SOI-CTM1142913',	'SOI-CTM1143459',	'SOI-CON1015269',	'SOI-CON1015070',	'SOI-CTM1143460',	'SOI-CTM1142608',	'SOI-CTM1143827',	'SOI-CTM1143461',	'SOM-MSM1046588',	'SOI-CTM1142912',	'SOI-CTM1142609',	'SOI-GOC1040729',	'SOI-CTM1142610',	'SOI-CTM1142611',	'SOI-MSM1151537',	'SOI-CTM1142914',	'SOI-CTM1142244',	'SOI-DIR1023150',	'SOI-CTM1143203',	'SOM-MSM1046663',	'SOM-MSM1046195',	'SOM-MSM1046069',	'SOM-MTC1008039',	'SOI-DIR1023041',	'SOI-GOC1040654',	'SOI-CON1015206',	'SOI-MSM1151538',	'SOI-CTM1142612',	'SOI-CTM1143204',	'SOI-CTM1143828',	'SOI-GOC1040474',	'SOI-CTM1142613',	'SOI-CTM1142915',	'SOI-DIR1023080',	'SOI-CON1015241',	'SOI-DIR1023149',	'SOI-CTM1142245',	'SOM-MSM1046396',	'SOI-MSM1151937',	'SOI-CTM1142246',	'SOI-CTM1142614',	'SOI-MSM1151539',	'SOI-CTM1142615',	'SOI-CTM1142616',	'SOI-GOC1040849',	'SOI-CTM1143463',	'SOI-CTM1142617',	'SOI-CTM1143205',	'SOI-CTM1143462',	'SOI-CTM1143464',	'SOI-CTM1143465',	'SOI-MSM1151416',	'SOI-GOC1040558',	'SOI-CTM1142618',	'SOI-CTM1142620',	'SOI-CTM1143206',	'SOI-CTM1143657',	'SOI-CON1015019',	'SOI-CTM1141922',	'SOI-CTM1142621',	'SOI-CTM1143207',	'SOM-MSM1045961',	'SOI-CTM1143208',	'SOM-MSM1046589',	'SOI-MSM1151417',	'SOI-MSM1151939',	'SOI-CTM1142916',	'SOI-CTM1142917',	'SOI-CTM1143211',	'SOI-GOC1040850',	'SOI-CTM1142248',	'SOI-CON1015131',	'SOM-MSM1046665',	'SOI-GOC1040373',	'SOI-CTM1143658',	'SOI-CTM1141923',	'SOI-CTM1142919',	'SOI-MSM1151940',	'SOI-CTM1142920',	'SOI-MSM1151630',	'SOI-GOC1040476',	'SOI-CTM1142251',	'SOI-MSM1151419',	'SOI-MSM1151941',	'SOM-MSM1046494',	'SOI-CTM1142252',	'SOI-CTM1142921',	'SOI-CTM1142922',	'SOM-MSM1045962',	'SOI-GOC1040656',	'SOI-GOC1040851',	'SOM-MSM1046666',	'SOM-MSM1046397',	'SOI-MSM1151294',	'SOM-MTC1008012',	'SOI-DIR1023151',	'SOI-CON1015242',	'SOI-CTM1142622',	'SOI-CON1015299',	'SOI-CTM1143829',	'SOI-DIR1023125',	'SOI-CTM1142923',	'SOM-MTC1007982',	'SOM-MSM1046070',	'SOM-MSM1046304',	'SOI-CTM1142623',	'SOM-MSM1046590',	'SOM-MSM1046305',	'SOI-CTM1142255',	'SOI-GOC1040730',	'SOI-CTM1142254',	'SOM-MSM1046591',	'SOI-CTM1142924',	'SOI-DIR1023126',	'SOI-DIR1023183',	'SOI-CTM1143212',	'SOI-CTM1143214',	'SOI-CTM1142624',	'SOI-CTM1141924',	'SOI-CTM1143830',	'SOI-GOC1040477',	'SOI-MSM1152046',	'SOI-MSM1151420',	'SOI-CTM1142925',	'SOI-CTM1142926',	'SOI-MSM1151631',	'SOI-CTM1143213',	'SOI-CTM1143466',	'SOI-DIR1023081',	'SOI-CTM1142927',	'SOI-MSM1151540',	'SOI-CTM1143831',	'SOM-MTC1008052',	'SOI-GOC1040246',	'SOI-GOC1040852',	'SOI-CTM1143835',	'SOI-GOC1040853',	'SOI-CTM1142256',	'SOI-CTM1142257',	'SOM-MSM1046667',	'SOM-MSM1046668',	'SOI-MSM1152047',	'SOI-CTM1142929',	'SOI-CTM1142928',	'SOI-CTM1141926',	'SOI-GOC1040778',	'SOI-CTM1142258',	'SOI-CTM1143832',	'SOM-MSM1046306',	'SOI-MSM1151752',	'SOI-CTM1143215',	'SOI-CTM1143833',	'SOI-GOC1040374',	'SOM-MTC1007958',	'SOI-CTM1143467',	'SOI-GOC1040559',	'SOI-MSM1151421',	'SOI-DIR1022998',	'SOI-GOC1040560',	'SOI-GOC1040561',	'SOI-CTM1143216',	'SOI-CTM1143468',	'SOI-MSM1151943',	'SOI-MSM1151632',	'SOI-CTM1143217',	'SOI-CTM1143469',	'SOI-CTM1142931',	'SOI-CTM1141927',	'SOI-CTM1142930',	'SOI-GOC1040479',	'SOM-MTC1008040',	'SOI-CTM1142932',	'SOI-CON1015243',	'SOI-CTM1141929',	'SOI-CTM1143218',	'SOI-GOC1040375',	'SOM-MTC1007917',	'SOM-MSM1046071',	'SOI-MSM1151856',	'SOI-CTM1143470',	'SOI-CTM1143834',	'SOI-CTM1143471',	'SOI-GOC1040731',	'SOI-CTM1141930',	'SOI-GOC1040563',	'SOI-GOC1040854',	'SOI-CTM1142263',	'SOI-MSM1151423',	'SOI-DIR1023127',	'SOI-CON1015271',	'SOI-GOC1040562',	'SOI-CON1015073',	'SOI-GOC1040247',	'SOI-GOC1040657',	'SOI-CON1015208',	'SOI-CTM1142264',	'SOI-CTM1143219',	'SOM-MSM1046307',	'SOI-CTM1143836',	'SOI-DIR1023235',	'SOI-CTM1143220',	'SOI-CTM1141931',	'SOI-CON1015172',	'SOI-CTM1142626',	'SOI-GOC1040660',	'SOI-GOC1040732',	'SOI-DIR1023217',	'SOI-GOC1040659',	'SOI-CTM1141932',	'SOI-DIR1023042',	'SOI-CTM1143472',	'SOI-CTM1142265',	'SOI-CTM1143473',	'SOI-GOC1040658',	'SOI-CON1015300',	'SOI-MSM1151424',	'SOI-CTM1143474',	'SOI-CTM1142266',	'SOM-MSM1046308',	'SOI-CTM1143221',	'SOM-MSM1046398',	'SOI-MSM1151945',	'SOI-MSM1151946',	'SOM-MSM1046593',	'SOI-DIR1023216',	'SOI-CTM1141933',	'SOI-CTM1143475',	'SOI-CTM1142268',	'SOM-MSM1046670',	'SOI-CTM1143476',	'SOI-CTM1141934',	'SOI-CTM1142933',	'SOI-CTM1143837',	'SOM-MSM1046309',	'SOI-GOC1040855',	'SOI-CTM1143477',	'SOI-MSM1152050',	'SOM-MSM1046310',	'SOI-CTM1143478',	'SOI-MSM1151753',	'SOI-CTM1142271',	'SOI-GOC1040376',	'SOI-GOC1040780',	'SOI-GOC1040480',	'SOI-GOC1040481',	'SOM-MSM1046594',	'SOI-DIR1023152',	'SOI-CTM1143838',	'SOI-CTM1143479',	'SOI-MSM1151857',	'SOI-DIR1023083',	'SOM-MSM1045963',	'SOI-CTM1143840',	'SOM-MTC1007983',	'SOI-CTM1143480',	'SOI-CTM1142272',	'SOI-CTM1143839',	'SOI-CTM1143223',	'SOI-GOC1040377',	'SOI-MSM1151425',	'SOI-MSM1151542',	'SOM-MTC1007918',	'SOI-CTM1142934',	'SOM-MSM1046673',	'SOI-GOC1040856',	'SOI-CTM1142935',	'SOI-GOC1040564',	'SOI-CTM1142274',	'SOI-CTM1141935',	'SOI-CTM1143222',	'SOI-MSM1151858',	'SOI-MSM1151634',	'SOI-CTM1143660',	'SOI-CTM1141936',	'SOI-CTM1143224',	'SOI-GOC1040565',	'SOI-CTM1142937',	'SOI-CTM1142275',	'SOI-MSM1151297',	'SOM-MSM1046496',	'SOM-MSM1046196',	'SOI-CTM1143481',	'SOI-DIR1023185',	'SOI-CTM1141937',	'SOI-CTM1143226',	'SOI-CTM1143225',	'SOI-GOC1040248',	'SOI-GOC1040781',	'SOI-CTM1143227',	'SOI-CTM1143228',	'SOI-GOC1040482',	'SOI-MSM1151635',	'SOI-MSM1151543',	'SOM-MSM1046311',	'SOI-CTM1143230',	'SOI-CON1015173',	'SOI-CTM1143229',	'SOI-MSM1151298',	'SOI-CTM1141938',	'SOI-GOC1040249',	'SOI-CON1015174',	'SOI-MSM1151636',	'SOI-CTM1142938',	'SOI-GOC1040250',	'SOI-CTM1141939',	'SOI-MSM1152053',	'SOM-MSM1046399',	'SOI-MSM1151299',	'SOM-MTC1008013',	'SOI-CTM1141940',	'SOI-GOC1040251',	'SOI-CON1015301',	'SOI-GOC1040782',	'SOI-DIR1023187',	'SOI-CON1015132',	'SOI-DIR1023044',	'SOI-CTM1142276',	'SOI-GOC1040859',	'SOM-MSM1046400',	'SOI-MSM1151860',	'SOM-MTC1008041',	'SOI-CTM1143482',	'SOI-CTM1142627',	'SOI-CON1015133',	'SOI-CTM1142277',	'SOI-MSM1151300',	'SOI-CTM1141941',	'SOI-GOC1040858',	'SOI-CTM1142939',	'SOM-MSM1046072',	'SOI-CTM1142628',	'SOI-DIR1022999',	'SOI-GOC1040378',	'SOM-MSM1046198',	'SOM-MTC1008042',	'SOI-CTM1143841',	'SOI-CTM1142278',	'SOI-MSM1151301',	'SOI-CTM1142279',	'SOI-CTM1142280',	'SOI-CTM1143483',	'SOI-CTM1143843',	'SOI-CTM1141942',	'SOI-CTM1143844',	'SOM-MSM1045965',	'SOI-GOC1040566',	'SOI-CTM1141943',	'SOI-CTM1143845',	'SOI-CTM1142281',	'SOM-MSM1046073',	'SOM-MSM1046199',	'SOI-CTM1142282',	'SOI-MSM1151545',	'SOI-CTM1141944',	'SOM-MSM1046596',	'SOI-CTM1143485',	'SOI-DIR1023094',	'SOI-GOC1040379',	'SOI-CTM1143484',	'SOI-GOC1040661',	'SOI-MSM1151638',	'SOM-MTC1007960',	'SOI-CTM1142283',	'SOI-CTM1143846',	'SOI-CTM1143661',	'SOI-CTM1143486',	'SOI-CTM1142630',	'SOI-CTM1142631',	'SOI-CTM1141945',	'SOI-CTM1142632',	'SOI-CTM1141946',	'SOI-CTM1142284',	'SOI-GOC1040381',	'SOI-MSM1151426',	'SOI-CTM1143662',	'SOI-CTM1143232',	'SOI-GOC1040860',	'SOI-CTM1143847',	'SOI-CTM1142285',	'SOI-GOC1040382',	'SOI-GOC1040483',	'SOI-CTM1142633',	'SOI-GOC1040733',	'SOM-MSM1046597',	'SOI-CTM1142940',	'SOI-CTM1142634',	'SOI-CTM1142635',	'SOI-GOC1040662',	'SOI-CTM1143233',	'SOI-CON1015134',	'SOI-MSM1151950',	'SOI-MSM1152055',	'SOI-MSM1151951',	'SOI-GOC1040567',	'SOI-MSM1151754',	'SOI-CON1015244',	'SOI-CTM1142942',	'SOI-DIR1023000',	'SOI-GOC1040252',	'SOI-MSM1151862',	'SOI-CTM1143234',	'SOI-CTM1143235',	'SOI-GOC1040253',	'SOI-CTM1143488',	'SOM-MSM1046200',	'SOI-CTM1141947',	'SOI-CTM1142636',	'SOI-CTM1142637',	'SOI-MSM1151427',	'SOI-CON1015245',	'SOI-CTM1143663',	'SOI-CON1015074',	'SOI-GOC1040254',	'SOI-DIR1023084',	'SOM-MSM1046313',	'SOI-CON1015210',	'SOI-GOC1040486',	'SOI-CTM1141949',	'SOI-CTM1143848',	'SOM-MSM1046074',	'SOM-MTC1008029',	'SOI-CTM1141948',	'SOI-CTM1142638',	'SOI-MSM1151547',	'SOI-GOC1040734',	'SOI-CTM1142287',	'SOI-MSM1151548',	'SOI-CTM1143489',	'SOM-MSM1046401',	'SOI-GOC1040783',	'SOI-CTM1143849',	'SOI-MSM1151755',	'SOM-MSM1045967',	'SOI-CTM1142289',	'SOI-GOC1040569',	'SOI-MSM1151756',	'SOI-CTM1143236',	'SOM-MSM1046402',	'SOI-MSM1151640',	'SOI-CON1015175',	'SOI-MSM1151641',	'SOI-GOC1040784',	'SOI-CTM1143490',	'SOI-DIR1022942',	'SOI-GOC1040663',	'SOI-CTM1142290',	'SOI-MSM1151642',	'SOI-CTM1143237',	'SOM-MSM1046075',	'SOI-MSM1151863',	'SOI-CON1015211',	'SOI-CON1015212',	'SOI-GOC1040255',	'SOI-GOC1040384',	'SOI-CTM1143238',	'SOI-CTM1142943',	'SOI-CTM1143239',	'SOI-MSM1151952',	'SOI-GOC1040735',	'SOI-CTM1143664',	'SOI-GOC1040785',	'SOI-DIR1023128',	'SOI-CTM1143850',	'SOI-CTM1143665',	'SOI-GOC1040570',	'SOI-GOC1040256',	'SOM-MSM1046600',	'SOI-GOC1040571',	'SOI-DIR1023085',	'SOI-GOC1040862',	'SOI-GOC1040861',	'SOI-GOC1040385',	'SOI-CTM1142639',	'SOI-MSM1151304',	'SOI-DIR1022943',	'SOI-MSM1151549',	'SOI-CON1015246',	'SOM-MSM1046498',	'SOI-CTM1142944',	'SOI-CTM1142291',	'SOI-GOC1040573',	'SOI-CON1015075',	'SOI-MSM1151643',	'SOI-MSM1152057',	'SOI-DIR1023045',	'SOI-CTM1141950',	'SOI-CON1015247',	'SOM-MSM1046499',	'SOM-MSM1046314',	'SOI-CTM1142945',	'SOM-MSM1046076',	'SOI-GOC1040664',	'SOM-MSM1046201',	'SOI-CTM1143666',	'SOI-GOC1040257',	'SOI-GOC1040574',	'SOI-DIR1023086',	'SOI-CTM1142946',	'SOI-CTM1142947',	'SOI-CTM1143240',	'SOI-GOC1040665',	'SOI-CON1015076',	'SOI-CTM1142948',	'SOI-GOC1040386',	'SOI-CTM1143492',	'SOI-CTM1141952',	'SOM-MSM1046077',	'SOI-CTM1143241',	'SOI-CTM1142949',	'SOI-CTM1141953',	'SOI-CTM1143851',	'SOI-CTM1143667',	'SOI-CTM1142640',	'SOI-CTM1142641',	'SOI-CTM1142950',	'SOI-DIR1023046',	'SOM-MSM1045969',	'SOI-GOC1040487',	'SOI-CTM1142643',	'SOI-GOC1040258',	'SOM-MSM1046500',	'SOI-GOC1040786',	'SOI-GOC1040787',	'SOM-MSM1046202',	'SOI-CON1015274',	'SOI-MSM1152058',	'SOI-CTM1142951',	'SOI-CTM1143491',	'SOI-DIR1023095',	'SOI-GOC1040864',	'SOI-CTM1142645',	'SOI-CTM1142952',	'SOI-GOC1040387',	'SOI-MSM1151645',	'SOI-GOC1040488',	'SOM-MSM1046315',	'SOI-CTM1142953',	'SOI-MSM1151864',	'SOI-CTM1142646',	'SOI-CON1015136',	'SOI-CTM1143853',	'SOI-GOC1040863',	'SOI-CTM1142954',	'SOI-CTM1141954',	'SOI-CTM1142294',	'SOI-GOC1040259',	'SOM-MSM1046404',	'SOI-GOC1040260',	'SOI-GOC1040489',	'SOI-GOC1040575',	'SOI-GOC1040576',	'SOI-CON1015176',	'SOI-CTM1143242',	'SOI-CTM1143243',	'SOM-MSM1046403',	'SOI-GOC1040577',	'SOI-GOC1040388',	'SOI-CTM1142955',	'SOI-CTM1142647',	'SOI-MSM1151550',	'SOM-MSM1046078',	'SOM-MSM1046316',	'SOI-GOC1040261',	'SOI-CTM1141955',	'SOI-CTM1142956',	'SOI-DIR1023048',	'SOM-MSM1046203',	'SOI-GOC1040262',	'SOI-CTM1142648',	'SOI-GOC1040490',	'SOI-DIR1023087',	'SOI-CTM1141956',	'SOI-DIR1023237',	'SOI-CTM1143854',	'SOI-CON1015177',	'SOI-MSM1151649',	'SOI-CTM1143855',	'SOI-CTM1142649',	'SOI-CTM1143494',	'SOI-GOC1040389',	'SOI-CTM1143856',	'SOI-CTM1143668',	'SOI-MSM1151650',	'SOM-MSM1046079',	'SOI-DIR1023097',	'SOM-MSM1046679',	'SOI-MSM1151865',	'SOI-CTM1143875',	'SOI-DIR1023049',	'SOI-CTM1143246',	'SOM-MSM1046080',	'SOI-DIR1023153',	'SOM-MSM1046405',	'SOI-GOC1040737',	'SOI-MSM1151430',	'SOI-CON1015213',	'SOI-CON1015138',	'SOI-CTM1142650',	'SOM-MSM1046602',	'SOI-CTM1143857',	'SOI-CTM1143864',	'SOI-CTM1142651',	'SOI-CTM1143669',	'SOI-GOC1040263',	'SOI-DIR1022946',	'SOM-MTC1007961',	'SOI-GOC1040788',	'SOI-CTM1143244',	'SOI-CTM1143670',	'SOI-GOC1040391',	'SOI-MSM1152060',	'SOM-MSM1046680',	'SOI-GOC1040264',	'SOI-CON1015214',	'SOI-CTM1142652',	'SOI-CON1015275',	'SOI-MSM1151954',	'SOI-CTM1142653',	'SOI-MSM1151652',	'SOM-MSM1046205',	'SOM-MSM1046502',	'SOI-CON1015023',	'SOI-CTM1142298',	'SOI-CTM1143671',	'SOM-MTC1007962',	'SOI-CTM1143672',	'SOM-MSM1046501',	'SOI-CTM1143858',	'SOI-CTM1142958',	'SOI-CTM1141958',	'SOI-CTM1142300',	'SOM-MSM1046406',	'SOI-MSM1151654',	'SOM-MSM1046503',	'SOM-MSM1045971',	'SOM-MTC1007963',	'SOI-DIR1023089',	'SOI-CTM1143673',	'SOI-GOC1040789',	'SOI-CTM1142654',	'SOI-GOC1040492',	'SOI-DIR1023154',	'SOM-MSM1046206',	'SOI-DIR1023088',	'SOI-MSM1151956',	'SOM-MTC1007920',	'SOI-CTM1142655',	'SOI-CTM1142301',	'SOI-CTM1143495',	'SOI-GOC1040578',	'SOI-MSM1151655',	'SOI-MSM1152061',	'SOI-GOC1040666',	'SOI-CTM1143247',	'SOI-CTM1142959',	'SOM-MSM1046082',	'SOI-GOC1040493',	'SOM-MTC1007964',	'SOI-CON1015216',	'SOM-MTC1007965',	'SOI-GOC1040667',	'SOI-CON1015302',	'SOM-MSM1046207',	'SOM-MSM1046318',	'SOI-CTM1141960',	'SOI-CON1015303',	'SOI-CTM1142303',	'SOM-MSM1046208',	'SOM-MSM1046319',	'SOI-DIR1023051',	'SOI-CTM1143859',	'SOM-MSM1046603',	'SOI-DIR1023155',	'SOI-CTM1143496',	'SOI-CTM1143861',	'SOI-CTM1143249',	'SOI-CTM1143250',	'SOM-MSM1046209',	'SOI-MSM1151656',	'SOI-GOC1040579',	'SOI-DIR1023063',	'SOI-CTM1143860',	'SOI-GOC1040265',	'SOI-CTM1142656',	'SOM-MSM1046083',	'SOI-DIR1023123',	'SOI-CTM1143862',	'SOI-DIR1023218',	'SOI-GOC1040580',	'SOI-MSM1151866',	'SOI-CTM1142305',	'SOM-MSM1046408',	'SOI-DIR1023052',	'SOI-CTM1143863',	'SOI-MSM1151657',	'SOI-CTM1143497',	'SOI-DIR1023003',	'SOM-MSM1045972',	'SOI-DIR1023091',	'SOM-MSM1046084',	'SOI-CTM1142657',	'SOM-MSM1046505',	'SOI-CON1015304',	'SOM-MSM1046605',	'SOM-MSM1046606',	'SOI-CTM1142658',	'SOI-DIR1023090',	'SOM-MSM1045973',	'SOI-GOC1040668',	'SOI-MSM1151867',	'SOI-GOC1040581',	'SOI-GOC1040582',	'SOI-CTM1141962',	'SOI-CTM1143865',	'SOI-CTM1143498',	'SOM-MTC1007985',	'SOI-CON1015077',	'SOI-CTM1142306',	'SOI-CTM1143499',	'SOI-CON1015078',	'SOI-GOC1040494',	'SOI-CTM1143251',	'SOI-MSM1151658',	'SOI-CTM1142961',	'SOI-MSM1151434',	'SOI-CTM1142309',	'SOM-MTC1007921',	'SOI-GOC1040495',	'SOM-MSM1046607',	'SOI-CTM1143252',	'SOI-CTM1142308',	'SOI-GOC1040792',	'SOI-MSM1151957',	'SOM-MSM1046211',	'SOM-MSM1046212',	'SOI-GOC1040496',	'SOI-DIR1023156',	'SOI-CTM1141964',	'SOM-MSM1045974',	'SOI-CON1015024',	'SOM-MSM1046410',	'SOI-CTM1143674',	'SOI-CTM1143500',	'SOI-MSM1151868',	'SOI-CTM1143867',	'SOI-CTM1143866',	'SOI-CTM1141965',	'SOI-CON1015025',	'SOI-DIR1022947',	'SOI-MSM1151869',	'SOI-MSM1152062',	'SOI-GOC1040790',	'SOI-CTM1142310',	'SOI-CON1015139',	'SOI-MSM1151311',	'SOM-MSM1046085',	'SOI-MSM1151958',	'SOI-CTM1143675',	'SOM-MTC1007942',	'SOI-CTM1143253',	'SOM-MSM1046506',	'SOI-MSM1151959',	'SOI-MSM1151659',	'SOI-CON1015026',	'SOI-GOC1040497',	'SOI-CON1015276',	'SOI-CTM1142311',	'SOI-CTM1142963',	'SOI-DIR1023129',	'SOI-MSM1151437',	'SOI-CTM1142661',	'SOI-GOC1040266',	'SOI-CTM1142660',	'SOI-MSM1151313',	'SOI-GOC1040267',	'SOI-GOC1040865',	'SOI-CON1015305',	'SOI-CON1015217',	'SOI-MSM1151763',	'SOM-MSM1046086',	'SOI-GOC1040669',	'SOM-MSM1046411',	'SOI-MSM1151438',	'SOI-CTM1143254',	'SOI-CTM1142969',	'SOI-GOC1040739',	'SOI-CTM1142662',	'SOI-GOC1040793',	'SOI-CTM1142964',	'SOI-MSM1152063',	'SOI-DIR1023093',	'SOI-MSM1151314',	'SOI-CTM1142312',	'SOI-GOC1040393',	'SOI-GOC1040498',	'SOI-CTM1142663',	'SOI-CON1015218',	'SOI-CTM1141966',	'SOI-CTM1142313',	'SOI-GOC1040268',	'SOI-CTM1141967',	'SOI-CTM1142965',	'SOI-GOC1040269',	'SOI-MSM1151660',	'SOI-CTM1143868',	'SOM-MSM1046087',	'SOI-CTM1141968',	'SOI-CTM1143869',	'SOI-GOC1040583',	'SOM-MSM1046507',	'SOI-CON1015219',	'SOI-DIR1023219',	'SOI-MSM1151661',	'SOM-MSM1046214',	'SOI-CTM1143871',	'SOI-GOC1040270',	'SOI-CTM1141969',	'SOI-MSM1151315',	'SOM-MSM1046088',	'SOM-MSM1046320',	'SOM-MSM1046413',	'SOM-MSM1046412',	'SOI-CTM1143502',	'SOI-CTM1142967',	'SOI-CTM1143255',	'SOI-CTM1143676',	'SOI-DIR1022958',	'SOI-CTM1143870',	'SOI-CTM1143501',	'SOM-MSM1046213',	'SOM-MSM1046608',	'SOI-MSM1151662',	'SOI-GOC1040394',	'SOI-MSM1151663',	'SOI-GOC1040499',	'SOI-CTM1143256',	'SOI-MSM1151664',	'SOI-MSM1151317',	'SOI-CTM1143872',	'SOI-CTM1142667',	'SOI-CTM1142968',	'SOI-CTM1143873',	'SOI-CTM1141971',	'SOM-MSM1046508',	'SOI-CON1015079',	'SOI-MSM1151439',	'SOI-GOC1040740',	'SOI-DIR1022948',	'SOI-DIR1023130',	'SOI-CTM1143874',	'SOI-MSM1151961',	'SOI-MSM1151962',	'SOI-CTM1141973',	'SOM-MSM1046321',	'SOM-MSM1046089',	'SOI-CTM1141972',	'SOI-CTM1143259',	'SOI-CON1015080',	'SOI-CTM1142314',	'SOI-GOC1040741',	'SOI-GOC1040396',	'SOI-CTM1143503',	'SOM-MSM1046322',	'SOI-CTM1143257',	'SOI-CTM1142315',	'SOI-GOC1040271',	'SOI-CTM1141974',	'SOI-MSM1151318',	'SOI-CTM1143258',	'SOM-MSM1046510',	'SOI-CTM1143677',	'SOI-GOC1040584',	'SOI-GOC1040272',	'SOI-CTM1141975',	'SOM-MTC1008030',	'SOI-MSM1151665',	'SOI-CTM1143678',	'SOM-MSM1046216',	'SOI-CTM1142970',	'SOI-GOC1040273',	'SOI-CON1015179',	'SOI-CTM1142316',	'SOI-CON1015140',	'SOI-CTM1142971',	'SOI-GOC1040500',	'SOI-CTM1143876',	'SOI-MSM1151319',	'SOI-GOC1040274',	'SOI-CTM1143877',	'SOI-CTM1142666',	'SOI-CTM1142317',	'SOI-CTM1142318',	'SOI-CTM1143679',	'SOI-GOC1040395',	'SOM-MSM1046090',	'SOI-CTM1142972',	'SOM-MSM1046215',	'SOI-GOC1040866',	'SOI-CTM1143680',	'SOM-MSM1045976',	'SOI-CTM1143504',	'SOI-CTM1143878',	'SOI-CTM1143260',	'SOI-CTM1141976',	'SOI-DIR1022949',	'SOI-CTM1141977',	'SOM-MSM1046217',	'SOI-MSM1152064',	'SOI-MSM1151872',	'SOI-DIR1023157',	'SOI-CTM1141979',	'SOI-MSM1151666',	'SOI-GOC1040795',	'SOI-DIR1023191',	'SOI-CTM1143505',	'SOI-CON1015141',	'SOI-DIR1022950',	'SOI-CTM1141981',	'SOM-MSM1046091',	'SOI-CON1015081',	'SOM-MSM1046609',	'SOI-CTM1142319',	'SOI-GOC1040796',	'SOI-CTM1142973',	'SOI-CTM1142321',	'SOI-CTM1143681',	'SOI-CTM1143879',	'SOI-CTM1143261',	'SOI-CTM1141980',	'SOI-CTM1143262',	'SOI-MSM1151765',	'SOI-CON1015180',	'SOI-MSM1151440',	'SOI-CTM1142322',	'SOI-CTM1142323',	'SOI-CTM1143263',	'SOI-MSM1151963',	'SOI-CTM1141982',	'SOI-GOC1040797',	'SOI-CTM1143506',	'SOM-MSM1046610',	'SOI-CTM1141983',	'SOI-CTM1143264',	'SOM-MSM1046219',	'SOM-MSM1046324',	'SOI-CTM1141984',	'SOI-GOC1040867',	'SOI-GOC1040501',	'SOI-GOC1040397',	'SOM-MSM1046509',	'SOM-MSM1046681',	'SOM-MSM1046325',	'SOI-MSM1151667',	'SOI-CTM1143880',	'SOI-CTM1142974',	'SOI-CTM1143881',	'SOI-CTM1142324',	'SOI-CTM1143883',	'SOI-GOC1040398',	'SOI-DIR1023004',	'SOI-CTM1143882',	'SOI-CTM1142668',	'SOI-CTM1143682',	'SOI-DIR1023105',	'SOI-MSM1152065',	'SOI-MSM1152066',	'SOM-MSM1046512',	'SOI-GOC1040670',	'SOI-GOC1040868',	'SOI-CTM1143884',	'SOM-MSM1046414',	'SOI-CTM1141985',	'SOI-GOC1040399',	'SOI-CTM1141986',	'SOI-CTM1143885',	'SOM-MSM1046415',	'SOI-CTM1143886',	'SOI-CON1015248',	'SOI-CON1015306',	'SOI-CTM1143683',	'SOI-MSM1151668',	'SOI-MSM1151669',	'SOI-CTM1142975',	'SOI-DIR1023192',	'SOI-CTM1143887',	'SOI-GOC1040277',	'SOI-CTM1143888',	'SOI-CTM1142669',	'SOM-MTC1007986',	'SOI-GOC1040276',	'SOI-GOC1040275',	'SOI-CON1015082',	'SOI-CTM1142671',	'SOI-CTM1141987',	'SOI-MSM1151670',	'SOI-CTM1143265',	'SOI-CTM1141988',	'SOI-CON1015028',	'SOI-CTM1142670',	'SOM-MSM1046612',	'SOI-MSM1151555',	'SOM-MSM1046326',	'SOI-CTM1143889',	'SOM-MSM1046613',	'SOI-CTM1143508',	'SOI-MSM1151965',	'SOI-MSM1152068',	'SOI-DIR1023054',	'SOI-MSM1151671',	'SOI-CTM1143507',	'SOM-MSM1046513',	'SOI-CON1015142',	'SOI-CTM1142325',	'SOI-CTM1142326',	'SOI-CTM1143266',	'SOI-MSM1151766',	'SOI-GOC1040671',	'SOI-GOC1040400',	'SOM-MSM1046514',	'SOI-GOC1040278',	'SOI-DIR1022951',	'SOM-MSM1046092',	'SOI-CTM1142327',	'SOM-MSM1046221',	'SOM-MSM1046416',	'SOI-MSM1151966',	'SOI-CTM1143890',	'SOI-MSM1151556',	'SOI-CTM1142977',	'SOI-CTM1143684',	'SOI-CTM1142672',	'SOI-CTM1142328',	'SOM-MSM1046222',	'SOI-MSM1151967',	'SOI-MSM1151968',	'SOI-CTM1141989',	'SOM-MSM1046327',	'SOI-CTM1142329',	'SOI-CTM1143267',	'SOI-CTM1142976',	'SOI-MSM1151672',	'SOI-CTM1142673',	'SOI-CTM1142978',	'SOI-CTM1143891',	'SOI-MSM1151873',	'SOI-CTM1141990',	'SOI-CTM1141991',	'SOI-CTM1141992',	'SOI-CTM1143268',	'SOI-GOC1040586',	'SOI-CTM1143509',	'SOI-CTM1142674',	'SOI-CTM1142675',	'SOM-MTC1007967',	'SOI-MSM1152069',	'SOI-MSM1151969',	'SOM-MSM1046328',	'SOI-CTM1142330',	'SOI-GOC1040589',	'SOM-MSM1045978',	'SOM-MSM1046329',	'SOI-GOC1040280',	'SOI-GOC1040588',	'SOM-MSM1046515',	'SOI-CTM1141994',	'SOI-CTM1142979',	'SOI-CON1015249',	'SOI-CTM1143270',	'SOI-GOC1040869',	'SOI-GOC1040587',	'SOI-CTM1143271',	'SOI-CTM1142677',	'SOI-CTM1143272',	'SOI-CTM1142678',	'SOM-MSM1046224',	'SOI-GOC1040798',	'SOI-GOC1040800',	'SOI-GOC1040799',	'SOI-CTM1142980',	'SOM-MSM1046093',	'SOM-MSM1046517',	'SOI-GOC1040672',	'SOI-CTM1143273',	'SOI-MSM1151557',	'SOI-MSM1151767',	'SOI-MSM1151768',	'SOI-CTM1142679',	'SOI-MSM1151970',	'SOI-CTM1142680',	'SOI-GOC1040590',	'SOI-GOC1040591',	'SOI-CON1015029',	'SOI-DIR1023158',	'SOI-CTM1142981',	'SOI-MSM1152070',	'SOI-CTM1142982',	'SOI-GOC1040743',	'SOI-CTM1141995',	'SOI-MSM1151769',	'SOI-CTM1143510',	'SOI-CTM1142681',	'SOI-CTM1142682',	'SOI-CTM1142683',	'SOI-GOC1040870',	'SOI-CTM1143685',	'SOI-GOC1040592',	'SOI-DIR1022953',	'SOI-MSM1151874',	'SOI-GOC1040282',	'SOI-GOC1040801',	'SOI-CTM1143274',	'SOM-MSM1046518',	'SOI-CTM1142331',	'SOM-MSM1045979',	'SOI-GOC1040871',	'SOM-MSM1046682',	'SOI-CTM1143275',	'SOI-CTM1142684',	'SOI-GOC1040744',	'SOI-CTM1143276',	'SOI-CTM1141998',	'SOI-CTM1142685',	'SOI-CTM1143277',	'SOI-GOC1040802',	'SOI-MSM1152071',	'SOI-GOC1040803',	'SOI-MSM1151674',	'SOI-MSM1151443',	'SOM-MSM1046330',	'SOM-MSM1046614',	'SOI-CTM1141999',	'SOI-CTM1143278',	'SOI-CTM1142983',	'SOI-CTM1143279',	'SOI-CTM1142984',	'SOM-MSM1046519',	'SOI-CTM1142000',	'SOI-MSM1152072',	'SOI-CTM1143892',	'SOM-MSM1046094',	'SOI-GOC1040593',	'SOI-CON1015250',	'SOI-MSM1151971',	'SOI-CTM1142686',	'SOI-CTM1142001',	'SOI-MSM1151972',	'SOI-MSM1151444',	'SOM-MSM1045980',	'SOI-CTM1143511',	'SOI-CTM1143512',	'SOI-CTM1143280',	'SOI-GOC1040804',	'SOI-CTM1142985',	'SOI-CTM1142332',	'SOI-CTM1142040',	'SOI-CON1015084',	'SOI-MSM1151770',	'SOI-CTM1142987',	'SOM-MSM1046520',	'SOI-CTM1142986',	'SOI-GOC1040283',	'SOI-CTM1142687',	'SOI-MSM1151558',	'SOM-MSM1046521',	'SOI-CTM1143281',	'SOI-DIR1022970',	'SOI-DIR1023055',	'SOI-MSM1151675',	'SOI-CTM1143893',	'SOI-MSM1151676',	'SOI-CTM1142333',	'SOI-CTM1142988',	'SOM-MSM1046522',	'SOI-CTM1143282',	'SOM-MTC1008031',	'SOM-MSM1046615',	'SOI-CON1015030',	'SOI-CTM1143894',	'SOI-CTM1143895',	'SOI-CTM1142989',	'SOI-CTM1143283',	'SOI-CTM1143284',	'SOI-CON1015085',	'SOI-GOC1040872',	'SOI-CTM1142990',	'SOI-MSM1152074',	'SOI-CTM1142334',	'SOI-CON1015143',	'SOI-CTM1142688',	'SOM-MSM1046417',	'SOI-CTM1143513',	'SOI-CTM1142002',	'SOI-CTM1143896',	'SOI-CTM1142991',	'SOI-CTM1143686',	'SOI-CTM1143687',	'SOI-CTM1143897',	'SOI-CTM1142003',	'SOI-MSM1151322',	'SOI-CTM1142004',	'SOI-GOC1040805',	'SOI-GOC1040284',	'SOI-DIR1023109',	'SOI-CTM1142335',	'SOI-MSM1151974',	'SOI-DIR1023005',	'SOI-CTM1142689',	'SOI-GOC1040285',	'SOI-CON1015181',	'SOM-MSM1046616',	'SOI-DIR1023159',	'SOI-CON1015031',	'SOI-GOC1040286',	'SOM-MSM1046418',	'SOI-CTM1143515',	'SOI-CTM1143898',	'SOI-MSM1151446',	'SOI-GOC1040594',	'SOI-CON1015032',	'SOM-MSM1045981',	'SOI-CTM1142690',	'SOI-MSM1151876',	'SOI-CTM1142005',	'SOI-CTM1143286',	'SOI-MSM1152075',	'SOI-CTM1142691',	'SOI-GOC1040503',	'SOI-GOC1040288',	'SOI-GOC1040287',	'SOI-GOC1040289',	'SOI-CTM1143900',	'SOI-CTM1142336',	'SOI-GOC1040595',	'SOI-CTM1143516',	'SOI-DIR1023220',	'SOI-GOC1040290',	'SOI-DIR1023006',	'SOI-CTM1143287',	'SOI-CTM1142992',	'SOI-GOC1040873',	'SOI-MSM1151975',	'SOM-MSM1046683',	'SOI-CON1015033',	'SOI-CON1015182',	'SOI-CTM1143688',	'SOI-CTM1142006',	'SOI-CON1015034',	'SOI-CTM1142008',	'SOI-CTM1142692',	'SOI-CTM1143901',	'SOI-MSM1151448',	'SOI-CTM1142007',	'SOI-CON1015035',	'SOI-DIR1023193',	'SOM-MSM1045982',	'SOI-CTM1142993',	'SOM-MTC1008033',	'SOI-MSM1152076',	'SOM-MSM1045983',	'SOI-CTM1142337',	'SOI-CTM1142995',	'SOI-CTM1142996',	'SOI-MSM1151449',	'SOI-CON1015277',	'SOM-MSM1045984',	'SOI-GOC1040504',	'SOI-MSM1152077',	'SOI-MSM1151450',	'SOI-MSM1151877',	'SOM-MSM1046095',	'SOI-CTM1142998',	'SOI-MSM1151325',	'SOM-MSM1046096',	'SOI-MSM1151679',	'SOI-CTM1143288',	'SOI-CTM1142338',	'SOI-MSM1151680',	'SOI-CTM1142997',	'SOI-CTM1143289',	'SOI-MSM1151771',	'SOI-CTM1143290',	'SOI-MSM1151451',	'SOI-CTM1143902',	'SOM-MSM1046331',	'SOM-MSM1046419',	'SOI-CTM1142339',	'SOI-CTM1143291',	'SOM-MSM1046097',	'SOI-CTM1143517',	'SOM-MSM1045985',	'SOI-CTM1142340',	'SOI-MSM1151772',	'SOM-MSM1046420',	'SOI-CTM1143292',	'SOI-CTM1142693',	'SOI-CTM1143904',	'SOI-CTM1143689',	'SOI-GOC1040506',	'SOM-MSM1046525',	'SOI-CTM1142999',	'SOI-MSM1152080',	'SOM-MSM1046684',	'SOI-GOC1040806',	'SOI-CTM1143518',	'SOI-CTM1143691',	'SOI-CTM1143692',	'SOI-CTM1143293',	'SOM-MSM1046332',	'SOI-CTM1143905',	'SOI-MSM1151879',	'SOI-MSM1151773',	'SOI-CTM1143294',	'SOI-CTM1143000',	'SOI-CTM1143295',	'SOI-GOC1040673',	'SOM-MSM1046098',	'SOI-DIR1022954',	'SOI-GOC1040505',	'SOI-CTM1143519',	'SOI-CTM1142341',	'SOI-CTM1142694',	'SOI-CTM1142343',	'SOI-MSM1151979',	'SOI-CTM1143296',	'SOI-CTM1142342',	'SOI-CTM1142011',	'SOI-CON1015036',	'SOI-CTM1143001',	'SOI-GOC1040874',	'SOI-MSM1151454',	'SOM-MSM1046099',	'SOI-CTM1143906',	'SOI-DIR1023007',	'SOI-CTM1143297',	'SOI-CTM1142012',	'SOI-GOC1040291',	'SOI-CTM1142695',	'SOI-MSM1152081',	'SOM-MSM1046685',	'SOI-CON1015037',	'SOI-CTM1142013',	'SOI-MSM1151881',	'SOI-CTM1143693',	'SOM-MTC1007923',	'SOI-CON1015144',	'SOI-CTM1143520',	'SOI-CTM1143298',	'SOI-MSM1151681',	'SOI-CTM1142696',	'SOI-CTM1142344',	'SOM-MSM1046421',	'SOI-DIR1023160',	'SOI-CTM1142014',	'SOM-MSM1045989',	'SOI-GOC1040507',	'SOI-GOC1040809',	'SOM-MSM1046334',	'SOM-MTC1008054',	'SOM-MSM1046526',	'SOM-MSM1046100',	'SOM-MTC1007989',	'SOM-MTC1007944',	'SOI-CTM1143521',	'SOI-GOC1040807',	'SOM-MSM1046527',	'SOI-CON1015038',	'SOI-MSM1151682',	'SOI-CTM1143002',	'SOI-CON1015145',	'SOI-DIR1023009',	'SOI-GOC1040808',	'SOM-MSM1046528',	'SOI-CTM1143300',	'SOI-CON1015307',	'SOI-CTM1143301',	'SOI-GOC1040675',	'SOI-CON1015039',	'SOI-MSM1151683',	'SOI-GOC1040597',	'SOI-CTM1143907',	'SOI-GOC1040810',	'SOI-MSM1151774',	'SOI-CTM1142697',	'SOM-MSM1046101',	'SOM-MSM1046423',	'SOI-CTM1143302',	'SOM-MSM1046424',	'SOI-CTM1143694',	'SOI-CTM1142015',	'SOI-CTM1142017',	'SOI-CTM1143908',	'SOI-CON1015040',	'SOI-CTM1143003',	'SOI-GOC1040745',	'SOI-CON1015086',	'SOI-MSM1151328',	'SOI-CTM1142019',	'SOI-CTM1142018',	'SOI-MSM1151980',	'SOI-CTM1143522',	'SOI-CON1015278',	'SOI-CTM1142345',	'SOI-CTM1143695',	'SOI-CTM1143696',	'SOM-MSM1046687',	'SOI-MSM1151882',	'SOI-MSM1151883',	'SOI-CTM1142698',	'SOM-MSM1046618',	'SOM-MSM1046425',	'SOI-GOC1040598',	'SOI-GOC1040676',	'SOI-CTM1143303',	'SOI-GOC1040599',	'SOI-CTM1142020',	'SOI-CTM1142699',	'SOM-MSM1046335',	'SOI-DIR1023008',	'SOI-MSM1151455',	'SOI-CTM1142346',	'SOI-MSM1151981',	'SOI-MSM1151982',	'SOI-GOC1040508',	'SOI-MSM1151983',	'SOI-CTM1143006',	'SOM-MSM1046688',	'SOI-CTM1142347',	'SOI-CTM1143007',	'SOI-CTM1142701',	'SOM-MSM1046529',	'SOI-CON1015183',	'SOI-CTM1142348',	'SOI-MSM1151562',	'SOI-CTM1142702',	'SOI-CTM1143304',	'SOI-CTM1142349',	'SOI-CTM1143699',	'SOI-CTM1143305',	'SOI-GOC1040746',	'SOI-GOC1040601',	'SOI-CTM1142703',	'SOI-CTM1142350',	'SOI-CTM1143698',	'SOI-GOC1040293',	'SOI-CTM1143700',	'SOI-CTM1143976',	'SOI-CON1015308',	'SOM-MSM1046226',	'SOM-MSM1046227',	'SOI-GOC1040294',	'SOI-CTM1143523',	'SOI-CTM1143524',	'SOI-CTM1143701',	'SOI-DIR1023098',	'SOI-GOC1040295',	'SOI-CTM1143909',	'SOI-MSM1151884',	'SOI-CTM1142351',	'SOI-CTM1143307',	'SOI-GOC1040600',	'SOI-CTM1143525',	'SOI-CON1015222',	'SOI-CON1015041',	'SOI-CTM1142352',	'SOI-CTM1142353',	'SOI-CTM1143526',	'SOM-MTC1008056',	'SOI-CTM1142354',	'SOI-MSM1151457',	'SOI-CON1015184',	'SOI-MSM1151685',	'SOI-GOC1040296',	'SOI-CTM1142704',	'SOI-DIR1023056',	'SOI-GOC1040677',	'SOM-MTC1008057',	'SOM-MSM1046102',	'SOI-CTM1143910',	'SOM-MSM1046103',	'SOI-CTM1143008',	'SOI-GOC1040811',	'SOI-CTM1143527',	'SOI-CTM1143528',	'SOI-GOC1040747',	'SOI-MSM1151458',	'SOI-CTM1142355',	'SOI-CTM1142023',	'SOI-CTM1143308',	'SOM-MSM1046689',	'SOI-MSM1151984',	'SOI-MSM1151885',	'SOI-GOC1040748',	'SOI-CTM1143009',	'SOI-MSM1151459',	'SOI-GOC1040297',	'SOI-CTM1142024',	'SOI-CTM1143309',	'SOI-CON1015279',	'SOI-MSM1151985',	'SOI-CTM1142026',	'SOI-CTM1142356',	'SOI-CTM1142025',	'SOI-CTM1143702',	'SOI-GOC1040602',	'SOI-MSM1151886',	'SOM-MSM1046531',	'SOI-CTM1143310',	'SOI-CTM1143010',	'SOM-MSM1045992',	'SOI-CTM1143011',	'SOM-MSM1046690',	'SOI-CTM1143529',	'SOI-GOC1040298',	'SOI-CTM1142357',	'SOI-DIR1023057',	'SOI-CTM1142705',	'SOI-CTM1143012',	'SOI-CON1015043',	'SOI-CTM1142027',	'SOI-CTM1143703',	'SOI-CTM1143013',	'SOI-GOC1040299',	'SOM-MSM1046530',	'SOI-GOC1040603',	'SOI-CTM1143311',	'SOI-CTM1142358',	'SOI-CTM1142706',	'SOI-CTM1143911',	'SOI-CTM1143312',	'SOI-CTM1142359',	'SOI-CTM1142360',	'SOI-CON1015223',	'SOI-CTM1142361',	'SOI-MSM1152082',	'SOI-CTM1142707',	'SOI-CTM1142029',	'SOI-CTM1143530',	'SOM-MSM1046104',	'SOI-MSM1151986',	'SOI-GOC1040875',	'SOI-CTM1142709',	'SOM-MSM1046691',	'SOI-CTM1143014',	'SOI-CTM1143313',	'SOI-MSM1151987',	'SOM-MSM1046336',	'SOI-CTM1143314',	'SOM-MSM1046532',	'SOI-CTM1143705',	'SOI-CTM1142028',	'SOI-MSM1151563',	'SOI-CTM1143315',	'SOI-GOC1040301',	'SOI-CTM1142710',	'SOI-GOC1040300',	'SOI-CTM1143015',	'SOI-CTM1143531',	'SOI-MSM1151777',	'SOI-DIR1023132',	'SOI-CTM1143316',	'SOI-CTM1143533',	'SOI-CTM1143017',	'SOI-CTM1143532',	'SOI-MSM1151778',	'SOI-GOC1040678',	'SOI-CON1015045',	'SOI-MSM1151330',	'SOM-MSM1046337',	'SOI-CTM1143534',	'SOM-MSM1046228',	'SOI-CTM1143706',	'SOI-DIR1022959',	'SOI-CON1015087',	'SOM-MSM1046428',	'SOI-CTM1142362',	'SOI-CTM1143018',	'SOI-CTM1143019',	'SOI-CTM1143535',	'SOI-CTM1143707',	'SOI-MSM1151332',	'SOI-CTM1143912',	'SOI-CTM1142363',	'SOI-CTM1142711',	'SOI-MSM1151888',	'SOI-CTM1143708',	'SOI-CTM1143709',	'SOI-MSM1151688',	'SOI-CTM1143536',	'SOI-CTM1142030',	'SOI-DIR1023133',	'SOI-CON1015044',	'SOI-CTM1143020',	'SOI-CTM1143913',	'SOI-DIR1023162',	'SOI-CTM1142713',	'SOI-GOC1040812',	'SOI-MSM1151988',	'SOI-GOC1040813',	'SOI-MSM1152084',	'SOI-CTM1143914',	'SOI-MSM1151889',	'SOI-CTM1143710',	'SOI-CTM1143021',	'SOI-CTM1143317',	'SOI-CTM1143539',	'SOI-CTM1142031',	'SOI-GOC1040877',	'SOM-MSM1046339',	'SOI-CTM1143538',	'SOI-CTM1143915',	'SOI-CTM1143022',	'SOI-GOC1040604',	'SOI-DIR1023134',	'SOI-MSM1151989',	'SOI-CTM1142032',	'SOI-MSM1151333',	'SOM-MSM1046429',	'SOM-MSM1046620',	'SOI-CTM1142712',	'SOI-CON1015280',	'SOI-GOC1040679',	'SOI-MSM1151779',	'SOI-CTM1143318',	'SOM-MSM1046430',	'SOI-CTM1143916',	'SOI-CTM1143319',	'SOI-CON1015281',	'SOI-MSM1151334',	'SOI-CTM1142364',	'SOI-CTM1142714',	'SOI-GOC1040680',	'SOI-CTM1142715',	'SOI-CTM1143541',	'SOI-CON1015310',	'SOI-CTM1143023',	'SOI-CTM1143024',	'SOI-CTM1143025',	'SOI-CTM1142033',	'SOI-MSM1151690',	'SOI-MSM1151890',	'SOM-MSM1046230',	'SOI-CTM1142036',	'SOI-DIR1023222',	'SOI-CTM1143320',	'SOI-CTM1142035',	'SOI-CON1015046',	'SOM-MSM1046431',	'SOI-MSM1151781',	'SOI-CTM1143321',	'SOI-MSM1151782',	'SOI-GOC1040879',	'SOI-DIR1023251',	'SOI-MSM1152085',	'SOI-CTM1143917',	'SOM-MSM1046340',	'SOM-MSM1046533',	'SOI-MSM1151335',	'SOM-MSM1046692',	'SOI-CTM1142716',	'SOI-CON1015047',	'SOI-CTM1143322',	'SOI-CTM1143323',	'SOI-MSM1151783',	'SOI-GOC1040814',	'SOM-MTC1007970',	'SOI-CTM1143918',	'SOI-DIR1023102',	'SOI-CTM1142037',	'SOI-MSM1151784',	'SOI-CTM1143542',	'SOI-CTM1142038',	'SOI-MSM1151336',	'SOI-CTM1143026',	'SOI-DIR1023103',	'SOI-CTM1142717',	'SOI-CTM1143711',	'SOI-CTM1143324',	'SOI-GOC1040749',	'SOI-CTM1143027',	'SOI-MSM1151691',	'SOI-CTM1142718',	'SOI-CON1015311',	'SOI-CTM1143712',	'SOM-MSM1046105',	'SOM-MSM1045993',	'SOI-GOC1040401',	'SOI-CTM1142365',	'SOI-CTM1143028',	'SOI-GOC1040815',	'SOI-CTM1143543',	'SOI-GOC1040681',	'SOI-GOC1040682',	'SOI-GOC1040302',	'SOI-MSM1151991',	'SOI-CTM1143029',	'SOI-CTM1143713',	'SOI-CTM1143919',	'SOI-CTM1143030',	'SOI-DIR1023058',	'SOI-GOC1040402',	'SOI-CTM1142039',	'SOI-CON1015146',	'SOI-MSM1151337',	'SOI-MSM1151564',	'SOI-CTM1142366',	'SOI-CTM1143920',	'SOM-MTC1007971',	'SOI-CTM1143714',	'SOI-GOC1040605',	'SOI-CON1015088',	'SOI-MSM1151891',	'SOI-DIR1023164',	'SOI-CTM1142041',	'SOI-CTM1143325',	'SOM-MSM1045994',	'SOI-DIR1023059',	'SOM-MSM1046342',	'SOM-MSM1046534',	'SOM-MTC1008058',	'SOI-DIR1023060',	'SOI-CTM1143544',	'SOI-CTM1143921',	'SOI-GOC1040303',	'SOI-CTM1142367',	'SOI-MSM1151460',	'SOI-CTM1143715',	'SOM-MSM1046106',	'SOI-GOC1040684',	'SOI-MSM1151785',	'SOI-CTM1143031',	'SOI-CTM1143032',	'SOI-GOC1040403',	'SOI-GOC1040404',	'SOI-MSM1151992',	'SOI-CTM1143326',	'SOI-CON1015186',	'SOI-CTM1143922',	'SOI-GOC1040606',	'SOI-CTM1142721',	'SOI-CTM1142369',	'SOI-MSM1151993',	'SOI-CTM1143923',	'SOI-CTM1142370',	'SOI-CTM1142044',	'SOI-CTM1142720',	'SOM-MSM1046621',	'SOI-CTM1142043',	'SOI-MSM1151339',	'SOI-MSM1151340',	'SOM-MSM1046694',	'SOI-CTM1143716',	'SOI-MSM1151565',	'SOI-CTM1143034',	'SOI-DIR1023135',	'SOI-MSM1151461',	'SOI-CTM1143924',	'SOI-CTM1143327',	'SOI-CTM1142371',	'SOI-CTM1143545',	'SOI-CTM1143328',	'SOI-CTM1142045',	'SOI-CTM1142722',	'SOI-CTM1143546',	'SOI-GOC1040881',	'SOI-CTM1143717',	'SOM-MSM1046107',	'SOI-CON1015282',	'SOI-CTM1142724',	'SOI-MSM1151893',	'SOI-MSM1151894',	'SOI-GOC1040607',	'SOM-MSM1046343',	'SOI-CTM1143035',	'SOI-CON1015147',	'SOM-MSM1046232',	'SOI-CTM1142725',	'SOI-CTM1142046',	'SOM-MSM1045995',	'SOI-CTM1143925',	'SOI-CTM1142373',	'SOM-MSM1045996',	'SOI-CTM1143548',	'SOI-GOC1040882',	'SOI-CTM1143718',	'SOI-GOC1040304',	'SOI-CTM1142726',	'SOI-CTM1143549',	'SOI-DIR1023165',	'SOI-CTM1142372',	'SOI-CTM1143037',	'SOI-MSM1151786',	'SOI-CTM1142727',	'SOI-CTM1143926',	'SOM-MSM1046623',	'SOI-CTM1143038',	'SOI-GOC1040685',	'SOI-CTM1143329',	'SOI-GOC1040510',	'SOI-CTM1142374',	'SOI-CTM1143330',	'SOM-MSM1046344',	'SOI-CTM1142728',	'SOM-MSM1045997',	'SOI-CTM1143927',	'SOI-MSM1151895',	'SOI-CTM1143039',	'SOM-MSM1046345',	'SOI-GOC1040686',	'SOM-MSM1046695',	'SOI-MSM1151566',	'SOI-CTM1142048',	'SOI-GOC1040608',	'SOI-DIR1023223',	'SOI-CTM1142729',	'SOI-MSM1151567',	'SOM-MSM1045998',	'SOI-CON1015312',	'SOI-CTM1143928',	'SOI-CTM1143929',	'SOI-MSM1151787',	'SOI-GOC1040305',	'SOI-CTM1143931',	'SOI-CTM1142730',	'SOM-MSM1046432',	'SOI-GOC1040817',	'SOI-MSM1151995',	'SOI-CTM1143550',	'SOI-CTM1142376',	'SOI-MSM1151692',	'SOI-CTM1142049',	'SOI-CTM1143331',	'SOI-GOC1040306',	'SOI-CTM1142378',	'SOI-CTM1143332',	'SOI-CTM1142731',	'SOI-CTM1143930',	'SOI-CTM1143040',	'SOI-MSM1151788',	'SOI-CTM1142050',	'SOM-MSM1046535',	'SOI-GOC1040307',	'SOM-MSM1046108',	'SOI-CTM1142732',	'SOI-GOC1040883',	'SOI-DIR1023104',	'SOI-DIR1023196',	'SOI-MSM1151463',	'SOI-CTM1142377',	'SOM-MSM1046233',	'SOI-CTM1143932',	'SOI-CTM1142733',	'SOI-GOC1040511',	'SOI-CTM1143551',	'SOI-CTM1143719',	'SOI-CTM1142734',	'SOI-CON1015187',	'SOI-CTM1142735',	'SOI-GOC1040308',	'SOI-MSM1151896',	'SOI-CTM1143933',	'SOI-GOC1040512',	'SOM-MSM1046234',	'SOI-MSM1151790',	'SOI-CTM1143934',	'SOM-MSM1046697',	'SOI-DIR1023018',	'SOI-MSM1151568',	'SOI-MSM1151569',	'SOI-CTM1143552',	'SOM-MSM1046000',	'SOI-CON1015049',	'SOI-DIR1023011',	'SOM-MSM1046435',	'SOI-CON1015224',	'SOI-CTM1143935',	'SOI-CTM1142379',	'SOI-MSM1151464',	'SOI-CTM1143553',	'SOI-CTM1142380',	'SOI-CTM1142381',	'SOI-CTM1143936',	'SOI-CTM1142382',	'SOI-MSM1151344',	'SOI-CTM1142736',	'SOI-CTM1142383',	'SOI-MSM1151345',	'SOM-MSM1046001',	'SOI-MSM1151693',	'SOI-CTM1143041',	'SOM-MSM1046624',	'SOI-CTM1142384',	'SOI-MSM1151694',	'SOM-MSM1046346',	'SOI-GOC1040610',	'SOI-CTM1143042',	'SOI-GOC1040611',	'SOI-CTM1143721',	'SOI-CTM1143333',	'SOI-CTM1143937',	'SOI-CTM1142052',	'SOI-CTM1143554',	'SOM-MSM1046536',	'SOI-MSM1151695',	'SOM-MSM1046110',	'SOM-MSM1046537',	'SOI-CTM1143043',	'SOI-CON1015089',	'SOM-MSM1046348',	'SOI-CTM1142053',	'SOI-CON1015313',	'SOI-CTM1143044',	'SOI-MSM1151571',	'SOI-CTM1142385',	'SOI-CTM1143938',	'SOI-CTM1143334',	'SOI-MSM1151572',	'SOI-MSM1151696',	'SOI-CTM1142737',	'SOI-CTM1143939',	'SOI-CTM1143045',	'SOI-CTM1143335',	'SOM-MSM1046698',	'SOI-CTM1143722',	'SOI-CTM1143557',	'SOM-MSM1046433',	'SOM-MSM1046625',	'SOI-CTM1143336',	'SOI-CTM1142386',	'SOM-MSM1046434',	'SOI-GOC1040884',	'SOI-MSM1151897',	'SOI-CTM1143555',	'SOI-CTM1142055',	'SOI-CTM1143723',	'SOM-MTC1008046',	'SOI-GOC1040687',	'SOI-CTM1142054',	'SOI-CTM1143046',	'SOI-CTM1142056',	'SOI-CTM1142057',	'SOI-CTM1143724',	'SOI-GOC1040688',	'SOI-CTM1143556',	'SOM-MSM1046699',	'SOI-CTM1143558',	'SOI-GOC1040310',	'SOI-CTM1142738',	'SOI-GOC1040513',	'SOI-CTM1143439',	'SOM-MSM1046349',	'SOI-CTM1142388',	'SOI-GOC1040885',	'SOI-CTM1142058',	'SOM-MSM1046350',	'SOI-GOC1040407',	'SOI-CTM1143725',	'SOI-CTM1143726',	'SOI-CTM1143727',	'SOM-MSM1046436',	'SOI-CTM1143728',	'SOI-CTM1142739',	'SOM-MSM1046112',	'SOI-GOC1040750',	'SOM-MSM1046111',	'SOI-MSM1152089',	'SOI-CTM1142389',	'SOI-CTM1142059',	'SOM-MSM1046003',	'SOI-CTM1143337',	'SOI-CON1015050',	'SOI-CTM1143940',	'SOI-DIR1023197',	'SOM-MSM1046700',	'SOM-MSM1046351',	'SOI-CTM1142390',	'SOI-CTM1143729',	'SOI-CTM1142391',	'SOI-GOC1040409',	'SOI-CTM1142060',	'SOI-CTM1142740',	'SOI-GOC1040408',	'SOI-GOC1040689',	'SOI-GOC1040821',	'SOI-MSM1152091',	'SOI-CTM1142741',	'SOI-CTM1142742',	'SOM-MSM1046539',	'SOI-CTM1142743',	'SOI-CTM1143560',	'SOI-CON1015283',	'SOM-MSM1046236',	'SOI-CTM1143338',	'SOI-GOC1040822',	'SOI-CTM1143730',	'SOI-CON1015315',	'SOI-CTM1143339',	'SOM-MTC1008059',	'SOI-MSM1151346',	'SOM-MSM1046626',	'SOI-GOC1040612',	'SOM-MTC1007993',	'SOI-DIR1022961',	'SOI-GOC1040311',	'SOI-CTM1143340',	'SOM-MSM1046701',	'SOI-MSM1151466',	'SOI-CTM1143341',	'SOI-CTM1142392',	'SOI-CTM1143562',	'SOI-DIR1023198',	'SOI-CTM1143941',	'SOI-CTM1142393',	'SOI-MSM1151347',	'SOI-CTM1142062',	'SOI-CTM1143732',	'SOI-DIR1023013',	'SOI-CTM1143342',	'SOM-MSM1046237',	'SOM-MSM1046113',	'SOM-MSM1046437',	'SOI-CTM1142063',	'SOM-MSM1046702',	'SOI-CTM1143942',	'SOI-DIR1023012',	'SOI-CTM1143563',	'SOI-GOC1040613',	'SOI-MSM1152092',	'SOM-MSM1046627',	'SOI-GOC1040312',	'SOI-GOC1040313',	'SOI-GOC1040751',	'SOI-CTM1143047',	'SOI-CTM1143564',	'SOI-MSM1151898',	'SOI-GOC1040515',	'SOI-GOC1040614',	'SOI-MSM1151573',	'SOI-MSM1151349',	'SOI-GOC1040615',	'SOI-GOC1040314',	'SOI-CTM1143943',	'SOI-MSM1151997',	'SOI-GOC1040886',	'SOI-CTM1143343',	'SOI-MSM1151468',	'SOI-CTM1142395',	'SOI-CTM1143944',	'SOI-DIR1022962',	'SOM-MSM1046352',	'SOI-CTM1142064',	'SOI-CTM1143565',	'SOM-MTC1007946',	'SOM-MSM1046628',	'SOI-CTM1143945',	'SOM-MSM1046114',	'SOI-MSM1151350',	'SOI-CTM1142065',	'SOI-MSM1151698',	'SOI-CON1015148',	'SOI-MSM1151351',	'SOI-DIR1023166',	'SOI-MSM1151469',	'SOI-CTM1143052',	'SOI-CON1015051',	'SOM-MTC1007926',	'SOI-DIR1023224',	'SOI-CTM1143048',	'SOI-GOC1040516',	'SOI-CTM1142396',	'SOI-MSM1151899',	'SOM-MSM1046703',	'SOI-CTM1142066',	'SOI-CTM1143049',	'SOM-MSM1046704',	'SOI-CTM1142397',	'SOM-MSM1046439',	'SOI-CTM1143566',	'SOI-CTM1143567',	'SOM-MSM1046705',	'SOM-MSM1046440',	'SOI-CON1015052',	'SOI-GOC1040315',	'SOI-MSM1151998',	'SOI-CTM1143050',	'SOI-CTM1143568',	'SOI-CTM1143051',	'SOI-MSM1151700',	'SOI-CTM1142068',	'SOI-CTM1143569',	'SOI-MSM1151574',	'SOI-CTM1143733',	'SOM-MSM1046353',	'SOI-CTM1142398',	'SOI-CTM1142744',	'SOI-MSM1151575',	'SOI-GOC1040412',	'SOI-CTM1142745',	'SOI-CTM1143344',	'SOM-MTC1008016',	'SOM-MSM1046115',	'SOM-MSM1046354',	'SOI-MSM1151576',	'SOI-MSM1151471',	'SOM-MTC1007947',	'SOI-DIR1023106',	'SOI-CTM1143053',	'SOI-GOC1040316',	'SOI-MSM1151791',	'SOI-MSM1151792',	'SOI-CTM1143054',	'SOI-GOC1040887',	'SOI-MSM1151352',	'SOI-CTM1143570',	'SOM-MSM1046238',	'SOI-CTM1143345',	'SOI-CTM1142399',	'SOI-CTM1142069',	'SOM-MSM1046706',	'SOI-CTM1143055',	'SOI-CTM1143946',	'SOI-CTM1142070',	'SOI-CTM1143571',	'SOM-MSM1046239',	'SOI-CON1015316',	'SOI-CTM1142746',	'SOI-CTM1142747',	'SOI-CTM1143056',	'SOM-MSM1046355',	'SOI-CTM1142748',	'SOI-MSM1151701',	'SOI-CTM1142071',	'SOI-CTM1142400',	'SOI-MSM1151793',	'SOI-CTM1143947',	'SOM-MSM1046712',	'SOI-DIR1023015',	'SOI-CON1015149',	'SOI-CTM1142072',	'SOI-DIR1023200',	'SOI-CTM1142401',	'SOM-MSM1046116',	'SOI-CTM1143346',	'SOI-CTM1142402',	'SOI-MSM1151353',	'SOI-MSM1151354',	'SOI-CTM1143057',	'SOI-CTM1143735',	'SOI-CTM1142749',	'SOI-CTM1142750',	'SOI-GOC1040517',	'SOM-MTC1008061',	'SOI-CON1015285',	'SOI-CTM1143948',	'SOI-CTM1143347',	'SOI-GOC1040413',	'SOM-MSM1046240',	'SOI-MSM1151473',	'SOI-CTM1143949',	'SOI-MSM1151794',	'SOI-CTM1142073',	'SOI-GOC1040317',	'SOI-CTM1142751',	'SOI-DIR1023016',	'SOI-CTM1142404',	'SOM-MSM1046117',	'SOI-CTM1143348',	'SOI-CTM1142403',	'SOI-MSM1151702',	'SOI-GOC1040894',	'SOI-CTM1142074',	'SOI-CTM1142752',	'SOM-MSM1046629',	'SOI-CON1015189',	'SOI-MSM1151703',	'SOI-MSM1151474',	'SOI-CTM1143969',	'SOI-GOC1040895',	'SOI-MSM1151475',	'SOI-CTM1142753',	'SOI-CTM1142754',	'SOI-CTM1142405',	'SOM-MSM1046707',	'SOM-MSM1046630',	'SOI-MSM1151355',	'SOI-MSM1152101',	'SOI-GOC1040896',	'SOI-GOC1040414',	'SOI-CON1015317',	'SOI-MSM1151999',	'SOI-CTM1143737',	'SOM-MTC1008063',	'SOM-MTC1007948',	'SOI-MSM1151356',	'SOI-GOC1040949',	'SOI-MSM1151357',	'SOM-MSM1046713',	'SOI-CTM1143058',	'SOI-CTM1142075',	'SOI-CTM1143736',	'SOI-MSM1151476',	'SOI-GOC1040888',	'SOI-CON1015090',	'SOI-MSM1151477',	'SOI-CTM1143739',	'SOI-CTM1143738',	'SOM-MSM1046791',	'SOI-GOC1040616',	'SOI-MSM1151706',	'SOI-CTM1143970',	'SOM-MTC1008064',	'SOI-CTM1143350',	'SOI-CTM1143972',	'SOI-CTM1143971',	'SOI-GOC1040318',	'SOM-MSM1046006',	'SOI-GOC1040319',	'SOM-MSM1046442',	'SOM-MSM1046118',	'SOM-MTC1007994',	'SOI-CTM1142076',	'SOI-GOC1040320',	'SOM-MSM1046714',	'SOI-GOC1040321',	'SOI-CON1015192',	'SOI-CON1015225',	'SOI-GOC1040690',	'SOI-CON1015286',	'SOI-MSM1152203',	'SOI-MSM1152103',	'SOI-CTM1143973',	'SOI-CON1015054',	'SOI-GOC1040889',	'SOI-MSM1151795',	'SOI-CTM1143952',	'SOI-CTM1142406',	'SOI-CTM1143953',	'SOI-CTM1142077',	'SOI-CTM1144148',	'SOI-GOC1040691',	'SOI-CON1015342',	'SOI-CTM1142407',	'SOI-CTM1143062',	'SOI-CTM1143974',	'SOI-MSM1152204',	'SOI-CTM1142078',	'SOI-CTM1142079',	'SOI-CTM1143060',	'SOI-MSM1151358',	'SOM-MSM1046792',	'SOI-MSM1152000',	'SOI-CTM1143059',	'SOI-GOC1040617',	'SOI-CON1015191',	'SOM-MSM1046443',	'SOI-MSM1152205',	'SOI-CTM1143740',	'SOI-DIR1023227',	'SOI-MSM1152001',	'SOI-DIR1022963',	'SOI-CTM1143954',	'SOI-MSM1152094',	'SOI-CTM1143955',	'SOI-CTM1143064',	'SOI-MSM1151707',	'SOI-CTM1143956',	'SOI-CTM1143065',	'SOI-MSM1151478',	'SOI-CTM1143975',	'SOM-MSM1046356',	'SOI-CTM1142409',	'SOI-GOC1040692',	'SOI-CTM1143741',	'SOI-CTM1142410',	'SOI-CTM1142411',	'SOI-GOC1040891',	'SOI-MSM1152095',	'SOI-CTM1143957',	'SOI-CTM1143958',	'SOI-CTM1143066',	'SOI-CTM1142412',	'SOI-CTM1143959',	'SOI-CTM1142416',	'SOI-MSM1151479',	'SOI-CON1015287',	'SOI-GOC1040890',	'SOI-MSM1152206',	'SOI-MSM1151480',	'SOI-CTM1143961',	'SOI-CTM1142413',	'SOI-GOC1040618',	'SOI-CTM1143068',	'SOI-CON1015193',	'SOI-CTM1143069',	'SOI-GOC1040897',	'SOI-CTM1144332',	'SOI-MSM1152207',	'SOI-MSM1151360',	'SOI-CTM1144149',	'SOI-CTM1142414',	'SOM-MSM1046007',	'SOI-GOC1040898',	'SOI-GOC1040996',	'SOI-CTM1142415',	'SOI-GOC1040899',	'SOI-CTM1142417',	'SOM-MSM1046708',	'SOI-MSM1152290',	'SOM-MSM1046793',	'SOI-CTM1143962',	'SOI-CTM1142426',	'SOI-MSM1152105',	'SOI-CTM1142080',	'SOM-MSM1046709',	'SOI-MSM1152002',	'SOI-CTM1143742',	'SOI-MSM1152106',	'SOI-CTM1143977',	'SOI-CTM1144331',	'SOI-MSM1152097',	'SOI-CTM1142082',	'SOI-CTM1142081',	'SOI-CTM1143353',	'SOI-CTM1142418',	'SOI-CON1015343',	'SOM-MSM1046889',	'SOI-CTM1144150',	'SOI-CTM1142419',	'SOI-CTM1144151',	'SOI-CTM1143978',	'SOI-MSM1151361',	'SOM-MSM1046710',	'SOM-MSM1046008',	'SOI-CON1015374',	'SOI-MSM1152098',	'SOM-MSM1046711',	'SOM-MSM1046794',	'SOI-CTM1144333',	'SOI-CTM1144152',	'SOI-CON1015226',	'SOI-CTM1143743',	'SOI-MSM1151482',	'SOM-MSM1046119',	'SOI-MSM1151796',	'SOI-CTM1143744',	'SOI-CTM1142083',	'SOI-CON1015091',	'SOI-MSM1151797',	'SOM-MTC1008060',	'SOI-CTM1142084',	'SOI-MSM1151362',	'SOI-CTM1143745',	'SOI-CTM1142421',	'SOI-CTM1143979',	'SOI-GOC1040322',	'SOI-CTM1143963',	'SOI-MSM1152107',	'SOI-CTM1143980',	'SOI-CTM1144153',	'SOI-CTM1142420',	'SOI-MSM1152100',	'SOM-MSM1046795',	'SOI-CTM1143748',	'SOI-MSM1152108',	'SOI-CTM1143981',	'SOI-GOC1040997',	'SOM-MSM1046796',	'SOI-CTM1143355',	'SOI-CON1015055',	'SOM-MSM1046634',	'SOI-MSM1152109',	'SOI-MSM1152291',	'SOI-GOC1040893',	'SOI-CTM1143964',	'SOM-MSM1046009',	'SOM-MSM1046120',	'SOI-CTM1142085',	'SOI-CTM1144155',	'SOI-CON1015288',	'SOM-MSM1046715',	'SOI-CTM1143356',	'SOI-CTM1144154',	'SOM-MSM1046444',	'SOI-CTM1143965',	'SOI-MSM1152208',	'SOI-DIR1023225',	'SOI-DIR1023136',	'SOI-CTM1142422',	'SOI-CON1015092',	'SOI-CTM1144334',	'SOI-CTM1142086',	'SOI-MSM1151363',	'SOI-CTM1143357',	'SOI-MSM1151483',	'SOI-CTM1142423',	'SOI-DIR1023278',	'SOI-CTM1142424',	'SOI-CTM1142425',	'SOI-CTM1143966',	'SOI-GOC1040998',	'SOI-GOC1040999',	'SOI-MSM1152209',	'SOM-MSM1046716',	'SOI-CTM1142088',	'SOI-CTM1142089',	'SOI-CTM1142427',	'SOI-MSM1151798',	'SOI-CTM1142090',	'SOI-CTM1142091',	'SOI-CTM1143750',	'SOI-MSM1152111',	'SOI-CTM1143968',	'SOI-CON1015318',	'SOI-CTM1144526',	'SOI-MSM1151365',	'SOI-CTM1143982',	'SOI-MSM1152003',	'SOI-CTM1144528',	'SOI-DIR1023203',	'SOI-GOC1040900',	'SOI-CTM1144527',	'SOI-CTM1143751',	'SOI-CTM1143752',	'SOI-CON1015319',	'SOI-CON1015375',	'SOM-MTC1007949',	'SOM-MSM1046717',	'SOI-CTM1143983',	'SOI-CTM1143753',	'SOM-MSM1046983',	'SOI-CTM1143984',	'SOM-MSM1046124',	'SOI-DIR1023294',	'SOI-GOC1040823',	'SOI-CON1015289',	'SOI-CTM1143985',	'SOI-CTM1143358',	'SOI-GOC1040323',	'SOI-CTM1142106',	'SOI-DIR1022965',	'SOI-CTM1142428',	'SOM-MSM1046718',	'SOI-CTM1142092',	'SOI-CON1015344',	'SOI-CTM1142105',	'SOI-GOC1041000',	'SOI-MSM1152292',	'SOI-MSM1151799',	'SOI-CTM1142093',	'SOI-CTM1143360',	'SOM-MSM1046798',	'SOI-DIR1023229',	'SOI-CTM1142094',	'SOM-MSM1046125',	'SOI-CTM1144156',	'SOI-CTM1144157',	'SOI-CTM1144158',	'SOI-CTM1142429',	'SOI-CTM1144335',	'SOI-GOC1040416',	'SOI-GOC1040695',	'SOI-CTM1142095',	'SOI-DIR1022966',	'SOI-CTM1142430',	'SOI-CTM1142096',	'SOI-CTM1142097',	'SOM-MSM1046010',	'SOI-MSM1151485',	'SOI-CTM1142098',	'SOM-MSM1046985',	'SOI-DIR1023247',	'SOM-MSM1046719',	'SOI-DIR1023021',	'SOI-CTM1142099',	'SOI-CTM1143361',	'SOI-MSM1151801',	'SOI-CTM1144159',	'SOI-CTM1143362',	'SOI-CTM1142100',	'SOI-CTM1143986',	'SOI-CTM1142431',	'SOI-CTM1144336',	'SOI-CTM1143363',	'SOI-CTM1143364',	'SOI-CTM1144160',	'SOI-MSM1151367',	'SOI-CTM1142432',	'SOI-CTM1142101',	'SOI-CTM1143366',	'SOI-CTM1142433',	'SOI-CON1015345',	'SOI-CTM1142434',	'SOI-MSM1151486',	'SOI-CON1015094',	'SOI-CTM1143987',	'SOM-MSM1046126',	'SOI-CTM1144337',	'SOI-CTM1142435',	'SOI-MSM1152112',	'SOI-MSM1152113',	'SOI-GOC1040901',	'SOM-MSM1046128',	'SOI-GOC1040696',	'SOI-CON1015227',	'SOI-CTM1144338',	'SOM-MSM1046011',	'SOI-MSM1151368',	'SOI-CTM1144161',	'SOM-MSM1046987',	'SOI-CTM1144162',	'SOM-MSM1046127',	'SOI-MSM1152211',	'SOI-CTM1142436',	'SOI-CTM1142103',	'SOM-MSM1046891',	'SOM-MSM1046129',	'SOI-CTM1144529',	'SOI-MSM1152213',	'SOI-CTM1144530',	'SOI-DIR1023022',	'SOI-GOC1040950',	'SOI-CON1015095',	'SOI-CTM1142437',	'SOM-MSM1046130',	'SOM-MTC1008078',	'SOI-CTM1143988',	'SOM-MSM1046800',	'SOI-CTM1142438',	'SOI-GOC1040902',	'SOI-CTM1143989',	'SOI-GOC1040903',	'SOM-MSM1046989',	'SOI-CTM1142439',	'SOM-MSM1046892',	'SOI-DIR1023248',	'SOI-MSM1152367',	'SOI-CTM1144163',	'SOM-MSM1046802',	'SOI-GOC1040904',	'SOM-MSM1046720',	'SOM-MSM1046991',	'SOM-MSM1046992',	'SOM-MSM1046803',	'SOI-CTM1144164',	'SOI-GOC1040905',	'SOM-MSM1046893',	'SOI-MSM1152293',	'SOI-GOC1041035',	'SOI-CTM1144531',	'SOM-MSM1046804',	'SOI-MSM1152214',	'SOI-CON1015096',	'SOI-CTM1142440',	'SOI-DIR1023230',	'SOI-CTM1143990',	'SOM-MSM1046805',	'SOI-MSM1152115',	'SOI-CTM1144165',	'SOI-MSM1152116',	'SOI-CON1015097',	'SOI-CTM1142441',	'SOI-CTM1142442',	'SOI-CON1015098',	'SOI-GOC1040417',	'SOI-CON1015400',	'SOI-CTM1142444',	'SOI-MSM1152215',	'SOI-GOC1040418',	'SOI-CON1015347',	'SOI-GOC1040906',	'SOI-CTM1143991',	'SOM-MSM1046895',	'SOI-CTM1144340',	'SOM-MSM1046994',	'SOI-CTM1143998',	'SOI-CTM1144532',	'SOI-CTM1143992',	'SOI-MSM1152294',	'SOI-CTM1144533',	'SOI-CTM1144534',	'SOI-CTM1142443',	'SOI-CTM1142446',	'SOI-CTM1143993',	'SOI-CTM1144167',	'SOI-CTM1142447',	'SOI-CTM1144341',	'SOI-MSM1151488',	'SOI-CTM1142445',	'SOI-MSM1152368',	'SOI-MSM1152118',	'SOI-CTM1143995',	'SOI-DIR1023296',	'SOI-MSM1152119',	'SOI-MSM1152120',	'SOI-CTM1142454',	'SOI-CON1015348',	'SOM-MSM1046995',	'SOI-CTM1143997',	'SOI-CTM1144169',	'SOI-CTM1143996',	'SOI-CON1015099',	'SOI-CON1015320',	'SOI-MSM1152121',	'SOI-CTM1144535',	'SOM-MSM1046996',	'SOM-MSM1046806',	'SOI-MSM1152369',	'SOI-CTM1142448',	'SOI-MSM1152122',	'SOI-GOC1041036',	'SOI-CTM1142449',	'SOI-MSM1152123',	'SOI-CTM1144171',	'SOI-CTM1144170',	'SOI-CTM1142452',	'SOI-CTM1142450',	'SOI-CTM1142451',	'SOI-CTM1142453',	'SOM-MSM1046896',	'SOI-CON1015100',	'SOI-CTM1142455',	'SOI-CTM1144172',	'SOM-MTC1008102',	'SOI-CTM1142456',	'SOI-MSM1152125',	'SOI-MSM1152295',	'SOI-CON1015376',	'SOI-CTM1144173',	'SOI-GOC1041037',	'SOI-CTM1144536',	'SOI-CTM1144537',	'SOI-MSM1152216',	'SOI-CTM1144174',	'SOI-GOC1041038',	'SOI-DIR1023249',	'SOI-CTM1144175',	'SOI-MSM1152296',	'SOM-MSM1046898',	'SOI-DIR1023279',	'SOI-CTM1144343',	'SOM-MSM1046997',	'SOI-CON1015401',	'SOI-CTM1143999',	'SOM-MSM1046899',	'SOI-GOC1041040',	'SOI-GOC1041041',	'SOI-CTM1144538',	'SOI-MSM1152127',	'SOI-CTM1144539',	'SOM-MSM1046721',	'SOI-CTM1144345',	'SOI-CTM1144344',	'SOI-GOC1040908',	'SOM-MSM1046998',	'SOI-MSM1152370',	'SOI-CTM1144000',	'SOI-CTM1144001',	'SOI-CON1015402',	'SOM-MSM1046722',	'SOI-CTM1144176',	'SOI-GOC1041042',	'SOI-MSM1152371',	'SOI-GOC1040909',	'SOI-CTM1144346',	'SOM-MSM1046900',	'SOI-GOC1041001',	'SOI-CTM1144540',	'SOI-CTM1144002',	'SOI-CTM1144541',	'SOI-CTM1144178',	'SOI-CTM1144347',	'SOM-MSM1046901',	'SOI-CTM1144542',	'SOI-CTM1144348',	'SOI-CTM1144543',	'SOI-CTM1144179',	'SOI-CTM1144177',	'SOI-MSM1152297',	'SOI-CTM1144349',	'SOI-CTM1144544',	'SOM-MSM1046902',	'SOI-CON1015349',	'SOI-CTM1144003',	'SOI-DIR1023280',	'SOI-DIR1023298',	'SOI-DIR1023240',	'SOI-CON1015403',	'SOI-CON1015377',	'SOM-MSM1046904',	'SOI-MSM1152129',	'SOI-CTM1144545',	'SOM-MSM1046807',	'SOI-CTM1144350',	'SOI-CTM1144352',	'SOI-MSM1152373',	'SOI-CTM1144351',	'SOI-GOC1040910',	'SOI-CTM1144546',	'SOI-CTM1144006',	'SOI-CTM1144005',	'SOI-CTM1144353',	'SOI-MSM1152300',	'SOI-CTM1144354',	'SOM-MTC1008066',	'SOM-MSM1046905',	'SOI-CTM1144355',	'SOM-MTC1008103',	'SOI-CTM1144547',	'SOM-MSM1046723',	'SOI-GOC1041006',	'SOM-MSM1046907',	'SOI-CON1015378',	'SOI-CTM1144548',	'SOI-CON1015321',	'SOI-CON1015379',	'SOI-GOC1041002',	'SOI-CTM1144358',	'SOM-MSM1046809',	'SOI-CTM1144359',	'SOI-CTM1144181',	'SOI-CTM1144182',	'SOI-MSM1152217',	'SOI-MSM1152377',	'SOI-CTM1144550',	'SOI-CTM1144360',	'SOI-CTM1144551',	'SOM-MSM1046724',	'SOI-MSM1152130',	'SOI-MSM1152301',	'SOI-CTM1144552',	'SOI-CTM1144183',	'SOM-MSM1046908',	'SOI-MSM1152218',	'SOI-CTM1144007',	'SOI-GOC1041004',	'SOI-GOC1041003',	'SOM-MSM1046812',	'SOI-CTM1144553',	'SOI-CTM1144555',	'SOI-CTM1144554',	'SOM-MSM1046811',	'SOI-CTM1144008',	'SOI-CTM1144556',	'SOI-CTM1144361',	'SOI-MSM1152379',	'SOI-DIR1023250',	'SOI-CTM1144010',	'SOI-DIR1023299',	'SOI-CTM1144557',	'SOM-MSM1046813',	'SOI-MSM1152219',	'SOI-CTM1144362',	'SOI-CTM1144011',	'SOI-CTM1144363',	'SOI-CTM1144364',	'SOI-CTM1144184',	'SOI-GOC1041005',	'SOM-MSM1046910',	'SOI-MSM1152131',	'SOI-MSM1152132',	'SOM-MSM1046725',	'SOM-MSM1046726',	'SOI-CTM1144185',	'SOI-CTM1144365',	'SOI-MSM1152133',	'SOI-CTM1144012',	'SOI-CTM1144186',	'SOI-GOC1041043',	'SOM-MSM1046727',	'SOI-CON1015323',	'SOI-GOC1040951',	'SOI-CTM1144187',	'SOM-MSM1047001',	'SOM-MSM1046728',	'SOI-CTM1144558',	'SOI-CTM1144188',	'SOI-CTM1144559',	'SOI-CTM1144366',	'SOI-CTM1144189',	'SOI-CTM1144190',	'SOI-MSM1152134',	'SOI-GOC1040912',	'SOM-MSM1046729',	'SOI-CTM1144367',	'SOM-MSM1046814',	'SOI-CON1015350',	'SOI-MSM1152221',	'SOI-CTM1144368',	'SOI-CTM1144369',	'SOI-CTM1144370',	'SOI-CTM1144372',	'SOI-CTM1144371',	'SOI-CTM1144013',	'SOI-MSM1152135',	'SOM-MSM1047002',	'SOI-CTM1144560',	'SOI-GOC1040913',	'SOM-MSM1046730',	'SOI-CON1015405',	'SOI-CTM1144561',	'SOM-MSM1047003',	'SOI-CTM1144373',	'SOI-CTM1144014',	'SOI-GOC1040914',	'SOI-CTM1144562',	'SOI-CTM1144374',	'SOI-MSM1152136',	'SOI-CTM1144375',	'SOM-MSM1046911',	'SOI-CTM1144376',	'SOI-CTM1144015',	'SOI-CON1015380',	'SOI-CTM1144377',	'SOM-MSM1046731',	'SOI-MSM1152137',	'SOI-DIR1023232',	'SOI-CTM1144563',	'SOI-CTM1144016',	'SOM-MSM1046815',	'SOI-GOC1040915',	'SOI-GOC1041044',	'SOI-CON1015351',	'SOM-MSM1047004',	'SOI-CTM1144017',	'SOM-MSM1047005',	'SOM-MTC1008067',	'SOI-CTM1144191',	'SOI-GOC1040952',	'SOI-CTM1144564',	'SOI-MSM1152302',	'SOI-MSM1152222',	'SOM-MTC1008104',	'SOI-CTM1144378',	'SOI-CTM1144565',	'SOM-MSM1046732',	'SOI-CTM1144379',	'SOI-CTM1144192',	'SOI-CTM1144380',	'SOM-MSM1046816',	'SOI-MSM1152381',	'SOI-CTM1144566',	'SOI-CTM1144018',	'SOI-GOC1040916',	'SOI-GOC1041007',	'SOI-CTM1144381',	'SOI-CTM1144382',	'SOI-CTM1144383',	'SOI-CON1015324',	'SOM-MSM1047006',	'SOI-CTM1144019',	'SOI-GOC1041008',	'SOI-CTM1144193',	'SOI-CTM1144385',	'SOI-CTM1144194',	'SOI-CTM1144020',	'SOI-CON1015325',	'SOI-GOC1040917',	'SOI-MSM1152303',	'SOM-MSM1046914',	'SOI-GOC1041045',	'SOM-MTC1008068',	'SOM-MSM1046915',	'SOM-MSM1046733',	'SOI-CTM1144021',	'SOI-CTM1144022',	'SOI-MSM1152223',	'SOI-CTM1144567',	'SOM-MSM1046917',	'SOM-MSM1046734',	'SOM-MSM1046916',	'SOM-MSM1046819',	'SOM-MSM1046735',	'SOI-CTM1144568',	'SOI-CTM1144386',	'SOM-MSM1046919',	'SOI-CTM1144195',	'SOI-CTM1144387',	'SOI-CTM1144569',	'SOI-CTM1144570',	'SOI-MSM1152305',	'SOI-CTM1144571',	'SOI-CTM1144389',	'SOI-CTM1144196',	'SOI-CTM1144388',	'SOI-DIR1023281',	'SOI-CTM1144572',	'SOI-MSM1152382',	'SOI-CTM1144390',	'SOI-MSM1152306',	'SOI-CTM1144391',	'SOI-GOC1041010',	'SOI-CTM1144392',	'SOI-GOC1040953',	'SOI-CTM1144573',	'SOM-MSM1047007',	'SOI-MSM1152383',	'SOI-MSM1152140',	'SOI-MSM1152308',	'SOI-CTM1144197',	'SOM-MSM1046821',	'SOI-GOC1041011',	'SOM-MSM1046736',	'SOI-CTM1144198',	'SOI-CTM1144393',	'SOI-CTM1144023',	'SOI-GOC1041047',	'SOI-CTM1144394',	'SOI-MSM1152310',	'SOI-CTM1144200',	'SOI-CTM1144199',	'SOI-CTM1144574',	'SOM-MSM1047008',	'SOM-MSM1046920',	'SOI-MSM1152141',	'SOI-CTM1144024',	'SOI-CTM1144202',	'SOI-MSM1152142',	'SOI-GOC1041048',	'SOI-DIR1023236',	'SOI-CTM1144203',	'SOI-CTM1144395',	'SOI-MSM1152227',	'SOI-MSM1152143',	'SOI-MSM1152144',	'SOI-GOC1040954',	'SOI-CTM1144396',	'SOI-CTM1144397',	'SOM-MSM1046737',	'SOM-MSM1046738',	'SOM-MTC1008069',	'SOI-MSM1152145',	'SOM-MSM1046739',	'SOI-CTM1144204',	'SOI-GOC1041049',	'SOI-CTM1144398',	'SOM-MSM1046824',	'SOI-CTM1144025',	'SOI-CTM1144026',	'SOI-CTM1144206',	'SOI-GOC1041050',	'SOI-CTM1144399',	'SOM-MSM1046741',	'SOI-GOC1040955',	'SOI-MSM1152384',	'SOM-MSM1047009',	'SOI-CTM1144575',	'SOI-GOC1040956',	'SOM-MSM1046922',	'SOI-CTM1144576',	'SOI-CTM1144207',	'SOI-GOC1040918',	'SOI-CTM1144027',	'SOM-MSM1047010',	'SOI-CTM1144577',	'SOI-CTM1144578',	'SOI-CON1015352',	'SOI-GOC1040957',	'SOI-CON1015353',	'SOI-MSM1152228',	'SOI-MSM1152312',	'SOI-CTM1144400',	'SOI-MSM1152229',	'SOI-GOC1041012',	'SOI-CTM1144208',	'SOI-MSM1152385',	'SOI-CTM1144401',	'SOM-MSM1046825',	'SOM-MSM1046923',	'SOI-GOC1040919',	'SOI-CTM1144209',	'SOI-CON1015406',	'SOI-MSM1152147',	'SOI-CTM1144028',	'SOI-CTM1144579',	'SOM-MTC1008071',	'SOI-CTM1144403',	'SOI-CTM1144580',	'SOI-CTM1144210',	'SOI-CTM1144211',	'SOI-GOC1041051',	'SOI-CON1015326',	'SOM-MSM1047011',	'SOI-CTM1144404',	'SOI-GOC1040959',	'SOM-MSM1046826',	'SOI-CON1015354',	'SOI-CTM1144405',	'SOI-MSM1152231',	'SOI-CTM1144212',	'SOI-CTM1144029',	'SOI-CTM1144213',	'SOI-CTM1144030',	'SOI-CTM1144214',	'SOI-CTM1144581',	'SOI-CTM1144582',	'SOI-MSM1152387',	'SOM-MSM1046924',	'SOI-CON1015407',	'SOI-CTM1144583',	'SOI-MSM1152314',	'SOI-CTM1144584',	'SOI-MSM1152149',	'SOM-MSM1046743',	'SOI-CTM1144406',	'SOI-MSM1152151',	'SOI-CTM1144031',	'SOI-MSM1152152',	'SOI-GOC1041052',	'SOI-CTM1144407',	'SOI-CTM1144408',	'SOI-MSM1152153',	'SOI-GOC1040920',	'SOI-MSM1152315',	'SOI-CTM1144215',	'SOI-MSM1152154',	'SOI-CTM1144409',	'SOI-CTM1144032',	'SOM-MSM1046925',	'SOM-MSM1046744',	'SOI-CTM1144033',	'SOM-MSM1046926',	'SOI-CTM1144216',	'SOI-CTM1144034',	'SOI-CTM1144585',	'SOI-CTM1144035',	'SOM-MTC1008080',	'SOI-MSM1152316',	'SOI-MSM1152317',	'SOI-CTM1144586',	'SOI-CTM1144036',	'SOM-MSM1046827',	'SOI-CTM1144587',	'SOI-GOC1041013',	'SOI-CTM1144588',	'SOI-CTM1144037',	'SOM-MSM1047013',	'SOI-CON1015355',	'SOI-GOC1041014',	'SOM-MSM1046928',	'SOM-MSM1046929',	'SOI-CTM1144038',	'SOI-MSM1152234',	'SOI-GOC1041053',	'SOM-MSM1047014',	'SOI-MSM1152155',	'SOI-CTM1144218',	'SOM-MSM1046746',	'SOI-CTM1144042',	'SOI-CTM1144040',	'SOM-MSM1046931',	'SOI-CTM1144410',	'SOI-GOC1040961',	'SOI-CTM1144041',	'SOI-CTM1144219',	'SOI-CTM1144589',	'SOM-MSM1046748',	'SOI-MSM1152388',	'SOI-GOC1041054',	'SOI-CTM1144590',	'SOM-MSM1046828',	'SOI-CTM1144591',	'SOI-GOC1040922',	'SOI-CTM1144043',	'SOI-CTM1144411',	'SOM-MTC1008072',	'SOI-GOC1040962',	'SOI-CTM1144412',	'SOI-CTM1144220',	'SOI-MSM1152156',	'SOI-CTM1144413',	'SOI-GOC1041015',	'SOI-CTM1144044',	'SOI-CTM1144221',	'SOI-CON1015327',	'SOM-MSM1047015',	'SOM-MSM1046932',	'SOI-DIR1023300',	'SOI-CON1015328',	'SOI-DIR1023255',	'SOI-GOC1040923',	'SOI-CTM1144415',	'SOI-MSM1152157',	'SOI-CTM1144592',	'SOI-CON1015410',	'SOI-MSM1152235',	'SOI-MSM1152320',	'SOI-CTM1144593',	'SOI-GOC1040963',	'SOI-MSM1152321',	'SOI-MSM1152322',	'SOM-MSM1046829',	'SOM-MSM1046750',	'SOI-CTM1144416',	'SOI-MSM1152390',	'SOI-CTM1144594',	'SOI-CTM1144045',	'SOI-CTM1144595',	'SOM-MTC1008081',	'SOI-CTM1144417',	'SOI-CTM1144046',	'SOI-DIR1023257',	'SOI-MSM1152158',	'SOI-GOC1040964',	'SOM-MSM1046830',	'SOI-CON1015411',	'SOI-GOC1040965',	'SOI-GOC1041016',	'SOI-CTM1144419',	'SOI-DIR1023239',	'SOI-CTM1144597',	'SOI-CTM1144598',	'SOI-CTM1144599',	'SOI-CTM1144222',	'SOI-CTM1144600',	'SOI-DIR1023302',	'SOI-CTM1144611',	'SOI-CTM1144420',	'SOI-CON1015381',	'SOI-CTM1144047',	'SOI-CON1015329',	'SOI-CON1015356',	'SOI-MSM1152237',	'SOI-DIR1023258',	'SOI-CTM1144048',	'SOI-CTM1144049',	'SOI-CTM1144422',	'SOI-CTM1144421',	'SOM-MSM1046751',	'SOM-MSM1046831',	'SOI-GOC1041017',	'SOI-CTM1144601',	'SOI-CTM1144050',	'SOI-CON1015330',	'SOM-MSM1046832',	'SOI-CTM1144051',	'SOI-CTM1144602',	'SOI-MSM1152238',	'SOI-DIR1023238',	'SOI-CTM1144423',	'SOM-MSM1046752',	'SOM-MSM1046933',	'SOI-CTM1144223',	'SOI-CTM1144603',	'SOI-CTM1144224',	'SOI-CTM1144225',	'SOI-CTM1144226',	'SOI-MSM1152324',	'SOI-CTM1144227',	'SOI-CTM1144228',	'SOI-CTM1144229',	'SOI-CTM1144604',	'SOI-CTM1144052',	'SOI-CTM1144053',	'SOI-CTM1144230',	'SOI-MSM1152160',	'SOI-CTM1144054',	'SOI-CTM1144055',	'SOI-CTM1144056',	'SOI-CTM1144605',	'SOI-MSM1152325',	'SOM-MSM1046934',	'SOI-CTM1144231',	'SOI-CTM1144233',	'SOI-CTM1144232',	'SOI-CTM1144111',	'SOI-CON1015357',	'SOI-GOC1040924',	'SOI-CTM1144606',	'SOI-MSM1152240',	'SOI-CTM1144424',	'SOI-CTM1144234',	'SOI-MSM1152161',	'SOI-CTM1144425',	'SOI-CON1015382',	'SOM-MSM1046935',	'SOI-CTM1144057',	'SOI-MSM1152326',	'SOI-CTM1144235',	'SOI-CTM1144058',	'SOI-MSM1152162',	'SOI-MSM1152241',	'SOI-CTM1144608',	'SOI-CTM1144607',	'SOI-CTM1144236',	'SOI-GOC1040967',	'SOI-CTM1144237',	'SOM-MSM1046753',	'SOI-CTM1144609',	'SOI-MSM1152391',	'SOM-MSM1047017',	'SOI-CTM1144610',	'SOI-GOC1040925',	'SOI-MSM1152242',	'SOM-MSM1046936',	'SOI-CTM1144059',	'SOI-CTM1144427',	'SOI-CTM1144238',	'SOM-MSM1046937',	'SOM-MSM1047018',	'SOI-MSM1152243',	'SOM-MTC1008113',	'SOI-CTM1144239',	'SOM-MSM1046938',	'SOI-GOC1041018',	'SOI-CON1015358',	'SOI-MSM1152244',	'SOM-MSM1047019',	'SOI-CTM1144612',	'SOI-CTM1144061',	'SOM-MSM1046834',	'SOI-CTM1144429',	'SOI-CTM1144062',	'SOI-CTM1144430',	'SOI-CON1015383',	'SOI-GOC1040926',	'SOI-CTM1144063',	'SOI-MSM1152163',	'SOI-CTM1144431',	'SOI-CTM1144613',	'SOI-CTM1144064',	'SOI-CTM1144065',	'SOI-DIR1023282',	'SOI-GOC1040927',	'SOM-MSM1046835',	'SOI-CON1015331',	'SOI-CTM1144066',	'SOI-CTM1144432',	'SOI-CON1015412',	'SOI-GOC1041055',	'SOI-CON1015332',	'SOI-CTM1144067',	'SOI-CTM1144240',	'SOI-MSM1152327',	'SOI-CTM1144068',	'SOI-CTM1144433',	'SOM-MTC1008105',	'SOM-MSM1046836',	'SOM-MTC1008106',	'SOI-GOC1040968',	'SOI-CTM1144069',	'SOI-CTM1144241',	'SOM-MTC1008082',	'SOI-CTM1144070',	'SOI-CTM1144614',	'SOM-MSM1047020',	'SOI-CTM1144242',	'SOI-CTM1144243',	'SOI-CTM1144244',	'SOI-GOC1040969',	'SOI-CON1015359',	'SOI-CTM1144071',	'SOI-MSM1152164',	'SOI-MSM1152247',	'SOI-GOC1040970',	'SOI-CTM1144246',	'SOI-MSM1152166',	'SOI-CTM1144245',	'SOI-MSM1152167',	'SOI-CTM1144072',	'SOI-MSM1152393',	'SOI-CTM1144615',	'SOI-CTM1144616',	'SOI-GOC1041056',	'SOI-CTM1144617',	'SOI-CTM1144074',	'SOI-CTM1144073',	'SOM-MSM1046755',	'SOI-CON1015360',	'SOI-GOC1041019',	'SOM-MSM1046756',	'SOI-MSM1152168',	'SOI-CTM1144075',	'SOI-CTM1144434',	'SOI-CTM1144076',	'SOI-CTM1144247',	'SOI-MSM1152394',	'SOM-MSM1046839',	'SOI-GOC1040971',	'SOI-CTM1144248',	'SOI-MSM1152169',	'SOI-CTM1144618',	'SOI-CTM1144437',	'SOI-CON1015361',	'SOM-MSM1046941',	'SOI-CTM1144077',	'SOI-CTM1144250',	'SOI-GOC1040972',	'SOM-MSM1046840',	'SOI-CTM1144439',	'SOI-CON1015413',	'SOI-MSM1152170',	'SOI-CTM1144438',	'SOI-MSM1152171',	'SOM-MSM1046757',	'SOM-MTC1008083',	'SOI-CTM1144440',	'SOI-GOC1040973',	'SOI-CTM1144251',	'SOI-CTM1144619',	'SOI-CTM1144620',	'SOM-MSM1046758',	'SOI-CTM1144622',	'SOI-MSM1152172',	'SOI-CTM1144079',	'SOI-MSM1152173',	'SOI-CTM1144621',	'SOI-CTM1144253',	'SOM-MSM1046759',	'SOM-MSM1046942',	'SOI-GOC1041021',	'SOI-CTM1144252',	'SOI-CON1015384',	'SOI-CTM1144080',	'SOI-DIR1023304',	'SOI-CTM1144624',	'SOI-CTM1144623',	'SOI-CTM1144442',	'SOI-CON1015333',	'SOI-CON1015385',	'SOM-MSM1046760',	'SOI-CTM1144254',	'SOM-MSM1047022',	'SOI-CTM1144625',	'SOI-GOC1040928',	'SOI-MSM1152396',	'SOI-DIR1023261',	'SOM-MSM1046841',	'SOI-CTM1144081',	'SOI-CTM1144082',	'SOI-MSM1152249',	'SOI-CTM1144443',	'SOM-MSM1046943',	'SOM-MTC1008108',	'SOM-MSM1046842',	'SOI-GOC1041022',	'SOI-GOC1040974',	'SOI-CON1015362',	'SOI-CTM1144444',	'SOI-DIR1023305',	'SOI-GOC1041058',	'SOI-CTM1144626',	'SOI-MSM1152328',	'SOI-CTM1144083',	'SOI-CTM1144255',	'SOI-CTM1144627',	'SOI-CTM1144256',	'SOI-CTM1144257',	'SOI-CTM1144445',	'SOM-MSM1047023',	'SOM-MSM1046761',	'SOI-CTM1144258',	'SOI-DIR1023263',	'SOI-CON1015334',	'SOI-GOC1040929',	'SOI-CTM1144084',	'SOI-GOC1041023',	'SOI-MSM1152397',	'SOM-MSM1046944',	'SOI-MSM1152329',	'SOI-CTM1144628',	'SOM-MSM1046843',	'SOI-GOC1040975',	'SOI-GOC1040930',	'SOI-CTM1144085',	'SOI-CON1015335',	'SOI-CON1015414',	'SOI-CTM1144446',	'SOI-DIR1023264',	'SOI-CTM1144259',	'SOI-MSM1152174',	'SOI-MSM1152330',	'SOI-DIR1023265',	'SOI-CTM1144086',	'SOI-CON1015387',	'SOI-CTM1144088',	'SOI-CTM1144087',	'SOI-CON1015386',	'SOI-CTM1144629',	'SOI-CTM1144089',	'SOI-MSM1152331',	'SOM-MSM1046763',	'SOI-CTM1144447',	'SOI-GOC1040931',	'SOM-MSM1046766',	'SOI-CON1015336',	'SOM-MSM1046765',	'SOI-GOC1041059',	'SOM-MSM1046764',	'SOI-CTM1144630',	'SOI-CON1015388',	'SOI-MSM1152332',	'SOI-CTM1144261',	'SOI-CTM1144631',	'SOI-CTM1144262',	'SOI-CTM1144260',	'SOI-CTM1144090',	'SOI-CON1015337',	'SOI-CTM1144448',	'SOI-MSM1152398',	'SOI-CTM1144632',	'SOI-GOC1040932',	'SOI-CTM1144449',	'SOM-MTC1008084',	'SOI-CTM1144633',	'SOI-MSM1152333',	'SOI-CTM1144450',	'SOM-MSM1046767',	'SOI-DIR1023241',	'SOM-MTC1008114',	'SOM-MSM1047025',	'SOI-CTM1144452',	'SOI-CTM1144451',	'SOI-CTM1144091',	'SOI-CTM1144453',	'SOI-CTM1144263',	'SOM-MTC1008115',	'SOI-DIR1023283',	'SOI-CTM1144454',	'SOI-GOC1041024',	'SOI-CON1015338',	'SOM-MSM1046947',	'SOM-MSM1047026',	'SOM-MSM1047027',	'SOM-MSM1046948',	'SOM-MSM1046768',	'SOI-GOC1040933',	'SOI-MSM1152176',	'SOI-MSM1152334',	'SOI-CTM1144635',	'SOM-MSM1047028',	'SOM-MSM1046769',	'SOI-GOC1040934',	'SOI-MSM1152399',	'SOI-CTM1144093',	'SOI-GOC1041060',	'SOI-CTM1144636',	'SOI-CTM1144455',	'SOI-CTM1144456',	'SOI-DIR1023306',	'SOI-MSM1152177',	'SOI-CTM1144094',	'SOI-CTM1144457',	'SOM-MSM1047029',	'SOI-CTM1144095',	'SOM-MSM1046949',	'SOM-MSM1047030',	'SOI-CON1015339',	'SOI-CON1015389',	'SOI-CTM1144096',	'SOI-MSM1152178',	'SOM-MSM1047031',	'SOI-CTM1144458',	'SOM-MSM1046770',	'SOI-GOC1040935',	'SOM-MTC1008116',	'SOI-CTM1144099',	'SOI-CTM1144459',	'SOI-MSM1152179',	'SOI-CTM1144098',	'SOI-GOC1040936',	'SOM-MSM1046771',	'SOI-DIR1023307',	'SOI-MSM1152180',	'SOI-CTM1144100',	'SOI-GOC1041025',	'SOI-CTM1144638',	'SOM-MSM1046950',	'SOI-CTM1144101',	'SOI-CTM1144102',	'SOM-MSM1047032',	'SOI-CTM1144639',	'SOI-CTM1144103',	'SOM-MTC1008074',	'SOM-MSM1046772',	'SOI-MSM1152181',	'SOI-CTM1144640',	'SOI-GOC1041061',	'SOI-CTM1144104',	'SOM-MSM1047033',	'SOI-CTM1144105',	'SOI-MSM1152182',	'SOI-MSM1152183',	'SOI-CTM1144106',	'SOI-CTM1144107',	'SOI-CTM1144641',	'SOI-MSM1152184',	'SOI-CTM1144642',	'SOM-MSM1047034',	'SOM-MSM1046951',	'SOI-MSM1152400',	'SOI-MSM1152250',	'SOI-GOC1041062',	'SOI-CTM1144462',	'SOI-CTM1144463',	'SOM-MSM1046952',	'SOI-CTM1144109',	'SOI-CTM1144110',	'SOI-CTM1144112',	'SOI-CTM1144464',	'SOI-CTM1144644',	'SOI-MSM1152401',	'SOI-GOC1041063',	'SOI-GOC1040937',	'SOI-MSM1152402',	'SOI-MSM1152336',	'SOI-GOC1041026',	'SOI-MSM1152186',	'SOI-MSM1152187',	'SOI-CTM1144465',	'SOI-MSM1152337',	'SOI-CTM1144114',	'SOI-DIR1023284',	'SOI-CTM1144646',	'SOI-CTM1144647',	'SOI-MSM1152188',	'SOI-CTM1144115',	'SOI-GOC1041064',	'SOI-GOC1041065',	'SOI-CTM1144648',	'SOI-GOC1040938',	'SOI-CTM1144116',	'SOI-CTM1144649',	'SOI-CTM1144117',	'SOI-CTM1144468',	'SOI-CTM1144466',	'SOI-CTM1144467',	'SOM-MSM1046773',	'SOI-GOC1041066',	'SOI-CTM1144118',	'SOI-MSM1152338',	'SOM-MSM1046774',	'SOM-MSM1047035',	'SOI-MSM1152340',	'SOI-GOC1041067',	'SOI-MSM1152190',	'SOI-CTM1144119',	'SOM-MSM1047036',	'SOI-MSM1152403',	'SOI-CTM1144651',	'SOI-GOC1040939',	'SOI-GOC1041068',	'SOI-CTM1144469',	'SOM-MSM1046954',	'SOM-MSM1046776',	'SOM-MSM1046775',	'SOI-CTM1144120',	'SOM-MSM1046777',	'SOI-CTM1144652',	'SOI-MSM1152191',	'SOM-MTC1008076',	'SOI-CTM1144470',	'SOI-CTM1144653',	'SOI-CTM1144121',	'SOI-CTM1144471',	'SOI-MSM1152404',	'SOI-CTM1144654',	'SOI-CTM1144122',	'SOM-MSM1046778',	'SOI-CTM1144656',	'SOI-CTM1144124',	'SOI-MSM1152405',	'SOM-MSM1046956',	'SOI-CTM1144125',	'SOI-CTM1144657',	'SOI-MSM1152406',	'SOI-CTM1144658',	'SOM-MSM1047037',	'SOM-MSM1046957',	'SOI-GOC1040940',	'SOI-CTM1144472',	'SOI-GOC1041027',	'SOI-DIR1023285',	'SOI-CTM1144659',	'SOM-MSM1046958',	'SOI-DIR1023286',	'SOM-MSM1046779',	'SOI-CTM1144660',	'SOI-CTM1144473',	'SOI-DIR1023242',	'SOI-CTM1144127',	'SOI-CTM1144126',	'SOI-MSM1152193',	'SOM-MSM1047039',	'SOI-CTM1144128',	'SOI-CTM1144662',	'SOI-MSM1152407',	'SOI-GOC1041028',	'SOI-CTM1144129',	'SOI-CTM1144663',	'SOI-GOC1040941',	'SOM-MSM1046780',	'SOI-CTM1144664',	'SOI-CTM1144475',	'SOI-CON1015415',	'SOI-CTM1144474',	'SOI-CTM1144130',	'SOI-DIR1023288',	'SOI-DIR1023287',	'SOI-CTM1144665',	'SOI-CTM1144477',	'SOI-CTM1144131',	'SOI-CTM1144666',	'SOI-CTM1144667',	'SOI-CTM1144478',	'SOI-CTM1144668',	'SOM-MSM1046783',	'SOI-CTM1144479',	'SOI-CTM1144669',	'SOI-DIR1023243',	'SOI-GOC1041069',	'SOI-CTM1144132',	'SOI-CTM1144670',	'SOI-CTM1144133',	'SOM-MSM1047040',	'SOI-DIR1023309',	'SOI-GOC1040942',	'SOI-CTM1144264',	'SOI-CTM1144265',	'SOI-GOC1041029',	'SOM-MSM1047041',	'SOI-CTM1144480',	'SOI-CTM1144672',	'SOI-DIR1023289',	'SOI-CTM1144266',	'SOM-MSM1046784',	'SOM-MSM1047042',	'SOI-MSM1152195',	'SOI-GOC1041070',	'SOM-MSM1047043',	'SOI-MSM1152409',	'SOI-MSM1152196',	'SOI-MSM1152251',	'SOI-CTM1144481',	'SOM-MTC1008109',	'SOI-CTM1144267',	'SOM-MTC1008086',	'SOI-CTM1144268',	'SOI-CTM1144270',	'SOI-CTM1144269',	'SOM-MTC1008087',	'SOI-CTM1144673',	'SOM-MTC1008117',	'SOI-CTM1144271',	'SOM-MSM1046961',	'SOI-CTM1144134',	'SOI-CTM1144272',	'SOM-MSM1046848',	'SOI-CTM1144135',	'SOI-CTM1144674',	'SOI-CTM1144136',	'SOI-MSM1152252',	'SOI-CTM1144482',	'SOI-CTM1144483',	'SOM-MSM1046962',	'SOM-MSM1046963',	'SOI-GOC1040944',	'SOI-GOC1041030',	'SOI-CON1015341',	'SOI-MSM1152253',	'SOI-MSM1152254',	'SOI-GOC1040976',	'SOI-CTM1144676',	'SOM-MSM1046964',	'SOI-MSM1152197',	'SOI-CTM1144484',	'SOI-CTM1144485',	'SOI-CTM1144486',	'SOM-MSM1046785',	'SOI-GOC1041071',	'SOI-MSM1152255',	'SOI-CON1015390',	'SOI-CTM1144677',	'SOI-MSM1152198',	'SOI-CTM1144137',	'SOM-MSM1046965',	'SOI-CTM1144488',	'SOM-MSM1046966',	'SOM-MSM1046786',	'SOI-MSM1152199',	'SOI-CTM1144489',	'SOI-MSM1152344',	'SOI-CTM1144490',	'SOI-CON1015391',	'SOI-MSM1152345',	'SOI-CTM1144491',	'SOI-GOC1041031',	'SOI-CTM1144274',	'SOI-CTM1144273',	'SOI-CTM1144492',	'SOI-CTM1144138',	'SOI-CTM1144139',	'SOI-GOC1041032',	'SOI-CON1015392',	'SOI-CTM1144140',	'SOI-CTM1144493',	'SOI-MSM1152346',	'SOI-CTM1144494',	'SOI-MSM1152347',	'SOI-CTM1144275',	'SOI-DIR1023245',	'SOM-MSM1046967',	'SOI-CTM1144495',	'SOI-DIR1023290',	'SOI-CTM1144496',	'SOI-MSM1152348',	'SOI-CON1015363',	'SOM-MTC1008089',	'SOI-GOC1040977',	'SOI-CTM1144141',	'SOI-CTM1144497',	'SOI-CTM1144276',	'SOI-CTM1144498',	'SOI-CTM1144499',	'SOI-MSM1152349',	'SOI-MSM1152350',	'SOM-MSM1046969',	'SOI-MSM1152256',	'SOI-GOC1040945',	'SOI-GOC1040946',	'SOI-DIR1023252',	'SOI-CTM1144143',	'SOI-GOC1040947',	'SOI-CTM1144500',	'SOM-MSM1046787',	'SOI-GOC1040948',	'SOI-CTM1144277',	'SOI-CTM1144278',	'SOI-MSM1152351',	'SOI-CON1015393',	'SOM-MSM1046850',	'SOM-MSM1046851',	'SOI-CTM1144144',	'SOI-MSM1152257',	'SOI-CTM1144145',	'SOM-MSM1046788',	'SOI-CTM1144147',	'SOM-MSM1046789',	'SOI-CTM1144146',	'SOI-MSM1152352',	'SOI-CON1015394',	'SOI-CTM1144279',	'SOI-CTM1144501',	'SOI-CTM1144280',	'SOI-MSM1152353',	'SOI-CON1015364',	'SOI-CTM1144502',	'SOM-MSM1046852',	'SOI-DIR1023269',	'SOI-CTM1144281',	'SOI-DIR1023292',	'SOI-CTM1144503',	'SOM-MTC1008090',	'SOM-MTC1008091',	'SOI-MSM1152354',	'SOI-CTM1144504',	'SOM-MSM1046855',	'SOI-DIR1023271',	'SOM-MSM1046970',	'SOI-CON1015395',	'SOI-MSM1152258',	'SOI-CTM1144505',	'SOI-CON1015396',	'SOI-MSM1152355',	'SOI-MSM1152356',	'SOI-GOC1041033',	'SOI-CTM1144506',	'SOM-MSM1046972',	'SOI-CTM1144283',	'SOI-CTM1144507',	'SOI-CTM1144508',	'SOI-CTM1144509',	'SOI-CTM1144510',	'SOI-CTM1144284',	'SOI-GOC1041034',	'SOI-CTM1144285',	'SOI-GOC1040979',	'SOI-CTM1144286',	'SOM-MSM1046973',	'SOM-MTC1008110',	'SOM-MTC1008092',	'SOM-MSM1046856',	'SOI-CTM1144511',	'SOI-MSM1152260',	'SOM-MSM1046857',	'SOM-MSM1046974',	'SOM-MSM1046858',	'SOI-MSM1152261',	'SOI-CTM1144512',	'SOI-CON1015365',	'SOM-MSM1046975',	'SOI-DIR1023293',	'SOI-CTM1144513',	'SOM-MSM1046859',	'SOI-CTM1144514',	'SOM-MSM1046860',	'SOI-MSM1152358',	'SOI-MSM1152262',	'SOI-CTM1144515',	'SOI-MSM1152263',	'SOI-CTM1144287',	'SOI-CTM1144516',	'SOI-CTM1144517',	'SOI-CON1015366',	'SOM-MSM1046861',	'SOM-MTC1008093',	'SOM-MSM1046976',	'SOM-MSM1046978',	'SOI-MSM1152359',	'SOI-CTM1144288',	'SOI-MSM1152360',	'SOI-CTM1144289',	'SOM-MSM1046979',	'SOI-MSM1152264',	'SOI-CON1015397',	'SOI-MSM1152361',	'SOI-MSM1152266',	'SOI-GOC1040980',	'SOI-CTM1144518',	'SOI-CTM1144519',	'SOM-MSM1046863',	'SOI-CTM1144520',	'SOI-CTM1144290',	'SOI-MSM1152362',	'SOI-MSM1152267',	'SOI-GOC1040981',	'SOI-CTM1144521',	'SOI-CON1015398',	'SOM-MSM1046982',	'SOI-CTM1144522',	'SOI-MSM1152269',	'SOI-MSM1152270',	'SOI-GOC1040982',	'SOI-MSM1152364',	'SOI-GOC1040984',	'SOI-CTM1144523',	'SOI-GOC1040983',	'SOM-MSM1046866',	'SOI-CTM1144291',	'SOI-MSM1152365',	'SOM-MSM1046868',	'SOI-MSM1152271',	'SOM-MSM1046869',	'SOI-CTM1144524',	'SOI-CTM1144292',	'SOI-CTM1144293',	'SOM-MTC1008095',	'SOI-CTM1144525',	'SOI-CTM1144294',	'SOI-GOC1040985',	'SOI-CTM1144295',	'SOI-CON1015367',	'SOI-CTM1144296',	'SOI-CTM1144297',	'SOI-CTM1144298',	'SOM-MSM1046870',	'SOM-MSM1046871',	'SOI-CTM1144299',	'SOI-GOC1040986',	'SOI-CON1015368',	'SOI-CON1015369',	'SOI-CTM1144300',	'SOI-CTM1144302',	'SOI-CTM1144303',	'SOI-CTM1144304',	'SOM-MTC1008097',	'SOI-CTM1144305',	'SOI-CTM1144306',	'SOI-CTM1144307',	'SOI-GOC1040989',	'SOI-CTM1144308',	'SOI-MSM1152272',	'SOI-CON1015370',	'SOI-GOC1040987',	'SOI-GOC1040988',	'SOI-CTM1144309',	'SOI-DIR1023274',	'SOM-MTC1008098',	'SOI-CON1015371',	'SOI-CTM1144310',	'SOI-MSM1152273',	'SOM-MSM1046872',	'SOI-MSM1152274',	'SOI-CTM1144311',	'SOI-GOC1040990',	'SOI-CTM1144312',	'SOI-DIR1023275',	'SOM-MSM1046874',	'SOI-CTM1144313',	'SOI-CON1015373',	'SOI-GOC1040991',	'SOI-GOC1040992',	'SOI-CTM1144314',	'SOI-CTM1144315',	'SOM-MSM1046875',	'SOI-GOC1040993',	'SOI-MSM1152278',	'SOI-CTM1144316',	'SOI-CTM1144317',	'SOI-CTM1144318',	'SOI-CTM1144319',	'SOI-CTM1144320',	'SOI-MSM1152279',	'SOI-MSM1152280',	'SOI-MSM1152281',	'SOI-MSM1152282',	'SOI-GOC1040994',	'SOM-MSM1046876',	'SOM-MSM1046877',	'SOI-CTM1144321',	'SOI-MSM1152283',	'SOI-CTM1144323',	'SOI-CTM1144324',	'SOM-MSM1046878',	'SOM-MSM1046879',	'SOI-CTM1144325',	'SOI-MSM1152284',	'SOI-MSM1152285',	'SOM-MSM1046882',	'SOM-MSM1046881',	'SOM-MSM1046884',	'SOI-MSM1152286',	'SOI-GOC1040995',	'SOI-CTM1144327',	'SOI-CTM1144326',	'SOI-MSM1152287',	'SOI-CTM1144328',	'SOI-MSM1152288',	'SOI-MSM1152289',	'SOM-MSM1046886',	'SOM-MSM1046887',	'SOI-CTM1144330',	'SOM-MTC1008051',	'SOI-DIR1023195')

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC 'SOI-CTM1134180',
# MAGIC 'SOI-CON1013796',
# MAGIC 'SOM-MSM1043370',
# MAGIC 'SOI-CON1014472',
# MAGIC 'SOI-CTM1140858',
# MAGIC 'SOI-GOC1039446',
# MAGIC 'SOI-CON1013568',
# MAGIC 'SOI-CTM1135881',
# MAGIC 'SOI-CTM1135557',
# MAGIC 'SOI-CON1014765',
# MAGIC 'SOI-GOC1039483',
# MAGIC 'SOM-MTC1007715',
# MAGIC 'SOI-MSM1143047',
# MAGIC 'SOM-MSM1043284',
# MAGIC 'SOI-CTM1141012',
# MAGIC 'SOI-CTM1138152',
# MAGIC 'SOI-CON1013682',
# MAGIC 'SOM-MSM1045534',
# MAGIC 'SOI-GOC1039933',
# MAGIC 'SOI-CTM1134917',
# MAGIC 'SOI-CTM1141181',
# MAGIC 'SOI-CTM1140640',
# MAGIC 'SOI-CTM1138330',
# MAGIC 'SOM-MSM1042560',
# MAGIC 'SOI-CTM1131407',
# MAGIC 'SOI-GOC1037450',
# MAGIC 'SOI-CTM1140063',
# MAGIC 'SOI-CTM1139073',
# MAGIC 'SOI-CTM1138016',
# MAGIC 'SOI-DIR1019302',
# MAGIC 'SOM-MTC1007870',
# MAGIC 'SOI-CTM1133610',
# MAGIC 'SOI-MSM1149819',
# MAGIC 'SOI-CTM1135875',
# MAGIC 'SOI-DIR1022779',
# MAGIC 'SOI-GOC1037367',
# MAGIC 'SOI-CON1014722',
# MAGIC 'SOM-MTC1007626',
# MAGIC 'SOM-MSM1045069',
# MAGIC 'SOI-CTM1133615',
# MAGIC 'SOI-MSM1149783',
# MAGIC 'SOI-CTM1135531',
# MAGIC 'SOI-CTM1139828',
# MAGIC 'SOI-CTM1135186',
# MAGIC 'SOI-MSM1151028',
# MAGIC 'SOI-CTM1140857',
# MAGIC 'SOI-CON1014052',
# MAGIC 'SOI-CTM1141159',
# MAGIC 'SOM-MSM1043729',
# MAGIC 'SOM-MSM1043742',
# MAGIC 'SOI-CTM1135972',
# MAGIC 'SOI-CON1013273',
# MAGIC 'SOI-CON1014373',
# MAGIC 'SOM-MSM1044176',
# MAGIC 'SOI-CON1013452',
# MAGIC 'SOI-CTM1141190',
# MAGIC 'SOI-GOC1037083',
# MAGIC 'SOI-GOC1039900',
# MAGIC 'SOI-CTM1119913',
# MAGIC 'SOI-CTM1134136',
# MAGIC 'SOI-CON1013965',


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC 
# MAGIC select * from TaurusSilverLH.silver_travel_enquiry_sales
# MAGIC limit 100

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
