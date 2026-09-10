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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# SelectedDate = "2026-08-11"
# SourceType = "SOI"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if SourceType == "Atoz":
    base_table = "tblTravelSalesTransactions_2"
elif SourceType == "AtozAgg":
    base_table = "tblTravelSalesTransactions_2"
elif SourceType == "AtozDirect":
    base_table = "tblTravelSalesTransactions_2"
elif SourceType == "Oasis":
    base_table = "tblTravelSalesTransactions"
elif SourceType == "OasisAgg":
    base_table = "tblTravelSalesTransactions"
elif SourceType == "OasisDirect":
    base_table = "tblTravelSalesTransactions"
elif SourceType == "SOI":
    base_table = "tblTravelSalesTransactions_1"
elif SourceType == "SOIAgg":
    base_table = "tblTravelSalesTransactions_1"
elif SourceType == "SOIDirect":
    base_table = "tblTravelSalesTransactions_1"
elif SourceType == "Start":
    base_table = "tblTravelSalesTransactions_1"
elif SourceType == "StartAgg":
    base_table = "tblTravelSalesTransactions_1"
elif SourceType == "StartDirect":
    base_table = "tblTravelSalesTransactions_1"
elif SourceType == "Trusted":
    base_table = "tblTravelSalesTransactions_1"
elif SourceType == "TrustedAgg":
    base_table = "tblTravelSalesTransactions_1"
elif SourceType == "TrustedDirect":
    base_table = "tblTravelSalesTransactions_1"
elif SourceType == "Viva":
    base_table = "tblTravelSalesTransactions_1"
elif SourceType == "VivaAgg":
    base_table = "tblTravelSalesTransactions_1"
elif SourceType == "VivaDirect":
    base_table = "tblTravelSalesTransactions_1"
else:
    raise ValueError(f"Unsupported SourceType: {SourceType}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.selected_date", SelectedDate)
spark.conf.set("spark.sql.source_type", SourceType)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


target_table = "TaurusBronzeLH.Daily" + SourceType
selected_date = SelectedDate  


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

scheme_atoz_agg = [
    "AtoZ Silver Backpacker CYTI", "AtoZ Gold ST CYTI", "AtoZ Silver Backpacker IDOL.",
    "AtoZ Silver Backpacker MSM", "AtoZ Gold AMT IDOL.", "AtoZ Gold AMT CYTI", "AtoZ Silver ST IDOL",
    "AtoZ Gold ST MSM", "AtoZ Bronze AMT IDOL.", "AtoZ Gold AMT MSM", "AtoZ Silver Backpacker IDOL",
    "AtoZ Bronze AMT IDOL", "AtoZ Silver AMT CYTI", "AtoZ Gold Backpacker IDOL.", "AtoZ Bronze AMT MSM",
    "AtoZ Silver ST IDOL.", "AtoZ Silver ST CYTI", "AtoZ Silver AMT IDOL.", "AtoZ Bronze ST IDOL.",
    "AtoZ Bronze AMT CYTI", "AtoZ Gold AMT IDOL", "AtoZ Silver AMT MSM", "AtoZ Silver ST MSM",
    "AtoZ Bronze ST IDOL", "AtoZ Bronze ST CYTI", "AtoZ Gold Backpacker IDOL", "AtoZ Gold ST IDOL",
    "AtoZ Silver AMT IDOL", "AtoZ Gold ST IDOL.", "AtoZ Bronze ST MSM"
]

scheme_atoz_direct = [
"Direct AtoZ Essential ST","MSM CYTI Renewal AMT Silver","Direct AtoZ Premium ST","AtoZ Renewal Gold IDOL MED AMT",
"Direct  AtoZ Standard ST","Direct AtoZ Essential AMT","AtoZ Renewal Direct AMT Standard","AtoZ Renewal Silver IDOL MED AMT",
"MSM CYTI Renewal AMT Gold","Direct AtoZ Premium AMT","AtoZ Renewal Direct AMT Essential","Direct AtoZ Standard AMT","MSM CYTI Renewal AMT Bronze"
]

scheme_oasis_direct = [
"Direct AMT Emerald","Agg Renewal AMT Bronze","Direct Single Gold","Direct Single Diamond","AGG RENEWAL AMT SILVER","Direct Backpackers Gold","Direct AMT Ruby",
"AGG RENEWAL AMT BRONZE","Direct AMT Bronze","Direct AMT Gold","Direct Renewal AMT Diamond","Direct Single Emerald","Direct AMT Silver","Direct Single Ruby",
"Agg Renewal AMT Gold","Direct Backpackers Silver","Direct Renewal AMT Emerald","Agg Renewal AMT Silver","Direct Renewal AMT Ruby","Direct Single Silver",
"Direct AMT Diamond","AGG RENEWAL AMT GOLD","Direct Backpackers Bronze","Direct Single Bronze",
]

scheme_oasis_agg = [
"MSM Backpacker Silver","CYTI Backpacker Gold","IDOL NON MED Single Elite","IDOL MED Backpacker Silver","CYTI Single Elite","MSM AMT Bronze","IDOL NON MED Single Gold","MSM AMT Premium",
"MTC MED Single Gold","MTC NON MED AMT Silver","IDOL NON MED Backpacker Bronze","IDOL NON MED Single Premium","MSM Single Premium","MSM Backpacker Gold","IDOL MED Single Silver",
"CYTI AMT Gold","CYTI Single Premium","MTC MED Single Bronze","CYTI AMT Silver","MTC MED AMT Silver","IDOL MED Backpacker Bronze","CYTI Backpacker Silver","MSM Backpacker Bronze",
"IDOL NON MED Single Classic","MTC NON MED AMT Bronze","MSM Single Silver","IDOL MED AMT Classic","MTC MED AMT Bronze","IDOL MED AMT Elite","CYTI AMT Bronze","IDOL NON MED Backpacker Gold",
"IDOL NON MED AMT Silver","IDOL MED Single Elite","MSM Single Gold","MSM AMT Gold","MSM Single Elite","IDOL NON MED Single Bronze","CYTI Single Classic","IDOL MED Backpacker Gold",
"IDOL MED Single Gold","IDOL NON MED AMT Gold","MTC NON MED Single Bronze","IDOL NON MED AMT Premium","MSM Single Bronze","MSM AMT Classic","CYTI Single Silver","IDOL MED Single Classic",
"CYTI AMT Premium","MTC MED AMT Gold","MSM AMT Elite","MTC NON MED AMT Gold","CYTI AMT Classic","IDOL MED AMT Premium","IDOL MED Single Bronze","MSM AMT Silver","IDOL MED AMT Silver",
"MTC NON MED Single Silver","CYTI Single Bronze","MSM Single Classic","IDOL NON MED Single Silver","IDOL NON MED Backpacker Silver","IDOL MED Single Premium","MTC NON MED Single Gold",
"CYTI Single Gold","IDOL MED AMT Bronze","IDOL NON MED AMT Bronze","CYTI Backpacker Bronze","IDOL MED AMT Gold","IDOL NON MED AMT Elite","MTC MED Single Silver",
"Viva IDOL Backpacker Silver","Viva IDOL Backpacker Gold","Viva IDOL Backpacker Platinum","Viva MSM Backpacker Silver","Viva MSM Backpacker Gold",
"Viva MSM Backpacker Platinum","Viva CYTI Backpacker Silver","Viva CYTI Backpacker Gold","Viva CYTI Backpacker Platinum",
"Viva MTC NM Single Silver","Viva MTC NM Single Gold","Viva MTC NM Single Platinum","Viva MTC NM AMT Silver","Viva MTC NM AMT Gold",
"Viva MTC NM AMT Platinum","Viva MTC NM Backpacker Silver","Viva MTC NM Backpacker Gold","Viva MTC NM Backpacker Platinum",
"Viva PTM Single Premium","Viva PTM Single Ultimate","Viva PTM AMT Basic","Viva PTM AMT Premium","Viva PTM AMT Ultimate",
]

scheme_viva_direct = [
"Viva Direct AMT Gold","Viva Direct Single Platinum","Viva Direct Single Gold","Viva Direct AMT Silver","Viva Direct AMT Platinum","Viva Direct Single Silver",
"Viva PTM Single Basic",
"Viva Direct Backpacker Silver","Viva Direct Backpacker Gold","Viva Direct Backpacker Platinum",
"Viva PCW Renewal Silver","Viva PCW Renewal Gold","Viva PCW Renewal Platinum","Viva Direct Renewal Silver","Viva Direct Renewal Gold",
"Viva Direct Renewal Platinum",
]

scheme_viva_agg = [
"Viva IDOL AMT Platinum","Viva MSM AMT Platinum","Viva IDOL AMT Silver","Viva CYTI AMT Silver","Viva MSM AMT Silver","Viva CYTI AMT Platinum","Viva IDOL Single Platinum",
"Viva CYTI Single Gold","Viva IDOL Single Silver","Viva IDOL Single Gold","Viva MSM Single Platinum","Viva MSM AMT Gold","Viva CYTI Single Silver","Viva CYTI Single Platinum",
"Viva MSM Single Gold","Viva IDOL AMT Gold","Viva CYTI AMT Gold","Viva MSM Single Silver",
"Viva PTM Single Basic",
]

scheme_trusted_direct = [
"Trusted Direct AMT Emerald","Trusted Direct Single Emerald","Trusted Direct Single Ruby","Trusted Direct AMT Diamond",
"Trusted Direct AMT Ruby","Trusted Direct Single Diamond","Trusted Direct Renewal Emerald","Trusted Direct Renewal Ruby","Trusted Direct Renewal Diamond",
"Trusted PCW Renewal Essential","Trusted PCW Renewal Classic","Trusted PCW Renewal Ultimate",
]

scheme_trusted_agg = [
"Trusted MSM Single Ultimate","Trusted IDOL AMT Ultimate","Trusted MSM Single Essential","Trusted MSM AMT Essential","Trusted IDOL Single Ultimate","Trusted MSM Single Classic",
"Trusted MSM AMT Classic","Trusted MSM AMT Ultimate","Trusted IDOL Single Essential","Trusted IDOL Single Classic","Trusted IDOL AMT Classic","Trusted IDOL AMT Essential",
'Trusted CYTI Single Classic','Trusted CYTI Single Essential','Trusted CYTI Single Ultimate','Trusted CYTI AMT Essential',
'Trusted CYTI AMT Classic','Trusted CYTI AMT Ultimate',
"Trusted MTC NM Single Essential","Trusted MTC NM Single Classic","Trusted MTC NM Single Ultimate","Trusted MTC NM AMT Essential","Trusted MTC NM AMT Classic",
"Trusted MTC NM AMT Ultimate",
]

scheme_soi_agg = [
"IDOL Backpacker Ultimate","CYTI Backpacker Ultimate","IDOL Backpacker Standard","CYTI AMT Three","IDOL AMT Two","CYTI Backpacker Standard","CYTI Backpacker Premium","MSM Backpacker Standard",
"MSM Single Three","IDOL Single One","IDOL AMT One","MSM AMT Three","MSM AMT Two","IDOL Single Two","MSM Backpacker Premium","MSM Single One","CYTI Single One","CYTI AMT One","MSM AMT One",
"IDOL AMT Three","CYTI AMT Two","MSM Backpacker Ultimate","IDOL Backpacker Premium","IDOL Single Three","MSM Single Two","CYTI Single Two","CYTI Single Three",
"PTM Single Essential","PTM AMT Essential","PTM Single Classic","MTC NM Single One","MTC NM Single Two","MTC NM Single Three","MTC NM AMT One",
"MTC NM AMT Two","MTC NM AMT Three","PTM Single Premier","PTM AMT Classic","PTM AMT Premier",
]




scheme_soi_direct = [
"Direct Backpacker Premium","PCW Renewal One","Direct Backpacker Ultimate","SOI Direct Renewal Standard","Direct Single Ultimate","PCW Renewal Two","Direct Single Premium",
"PCW Renewal Three","Direct Single Standard","SOI Direct Renewal Premium","Direct Backpacker Standard","Direct AMT Ultimate","Direct AMT Premium","Direct AMT Standard",
"SOI Direct Renewal Ultimate",
]

scheme_start_agg = [
"Start MSM Single Premier","Start CYTI Backpacker Classic","Start CYTI AMT Premier","Start IDOL Single Premier.","Start IDOL AMT Essential","Start IDOL AMT Premier.",
"Start IDOL Single Classic.","Start CYTI AMT Classic","Start CYTI Backpacker Essential","Start CYTI Single Premier","Start IDOL Backpacker Premier.","Start IDOL Backpacker Essential",
"Start IDOL Backpacker Premier","Start CYTI Backpacker Premier","Start MSM AMT Essential","Start IDOL Single Classic","Start MSM AMT Classic","Start IDOL Single Essential.",
"Start IDOL Backpacker Classic","Start CYTI Single Classic","Start IDOL AMT Classic.","Start MSM Single Classic","Start IDOL AMT Premier","Start IDOL AMT Classic","Start CYTI Single Essential",
"Start MSM Backpacker Classic","Start IDOL Backpacker Classic.","Start IDOL AMT Essential.","Start MSM AMT Premier","Start IDOL Single Premier","Start IDOL Backpacker Essential.",
"Start IDOL Single Essential","Start CYTI AMT Essential","Start MSM Backpacker Premier","Start MSM Single Essential",
]

scheme_start_direct = [
"Start Direct Renewal 5 Star","Start PCW Renewal Essential","Start Direct AMT 4 Star","Start Direct Backpacker Classic","Start Direct Single 4 Star","Start AGG Renewal Classic",
"Start PCW Renewal Classic","Start AGG Renewal Premier","Start Direct Single 5 Star","Start Direct Backpacker Essential","Start Direct Renewal 4 Star","Start Direct AMT 3 Star",
"Start AGG Renewal Essential","Start Direct AMT 5 Star","Start Direct Single 3 Star","Start Direct Backpacker Premier","Start Direct Renewal 3 Star","Start PCW Renewal Premier",

]

scheme_name_tuple_atoz_agg = tuple(scheme_atoz_agg)
scheme_name_tuple_atoz_direct = tuple(scheme_atoz_direct)

scheme_name_tuple_oasis_agg = tuple(scheme_oasis_agg)
scheme_name_tuple_oasis_direct = tuple(scheme_oasis_direct)

scheme_name_tuple_viva_agg = tuple(scheme_viva_agg)
scheme_name_tuple_viva_direct = tuple(scheme_viva_direct)

scheme_name_tuple_trusted_agg = tuple(scheme_trusted_agg)
scheme_name_tuple_trusted_direct = tuple(scheme_trusted_direct)

scheme_name_tuple_soi_agg = tuple(scheme_soi_agg)
scheme_name_tuple_soi_direct = tuple(scheme_soi_direct)

scheme_name_tuple_start_agg = tuple(scheme_start_agg)
scheme_name_tuple_start_direct = tuple(scheme_start_direct)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if SourceType == 'AtozAgg':
    travel_brand_expr = "'AtozAgg'"
    additional_filter = "AND SchemeName IN {}".format(scheme_name_tuple_atoz_agg)
elif SourceType == 'AtozDirect':
    travel_brand_expr = "'AtozDirect'"
    additional_filter = "AND SchemeName IN {}".format(scheme_name_tuple_atoz_direct)
elif SourceType == 'OasisAgg':
    travel_brand_expr = "'OasisAgg'"
    additional_filter = "AND SchemeName IN {}".format(scheme_name_tuple_oasis_agg)
elif SourceType == 'OasisDirect':
    travel_brand_expr = "'OasisDirect'"
    additional_filter = "AND SchemeName IN {}".format(scheme_name_tuple_oasis_direct)
elif SourceType == 'VivaAgg':
    travel_brand_expr = "'VivaAgg'"
    additional_filter = "AND SchemeName IN {}".format(scheme_name_tuple_viva_agg)
elif SourceType == 'VivaDirect':
    travel_brand_expr = "'VivaDirect'"
    additional_filter = "AND SchemeName IN {}".format(scheme_name_tuple_viva_direct)
elif SourceType == 'TrustedAgg':
    travel_brand_expr = "'TrustedAgg'"
    additional_filter = "AND SchemeName IN {}".format(scheme_name_tuple_trusted_agg)
elif SourceType == 'TrustedDirect':
    travel_brand_expr = "'TrustedDirect'"
    additional_filter = "AND SchemeName IN {}".format(scheme_name_tuple_trusted_direct)
elif SourceType == 'SOIAgg':
    travel_brand_expr = "'SOIAgg'"
    additional_filter = "AND SchemeName IN {}".format(scheme_name_tuple_soi_agg)
elif SourceType == 'SOIDirect':
    travel_brand_expr = "'SOIDirect'"
    additional_filter = "AND SchemeName IN {}".format(scheme_name_tuple_soi_direct)
elif SourceType == 'StartAgg':
    travel_brand_expr = "'StartAgg'"
    additional_filter = "AND SchemeName IN {}".format(scheme_name_tuple_start_agg)
elif SourceType == 'StartDirect':
    travel_brand_expr = "'StartDirect'"
    additional_filter = "AND SchemeName IN {}".format(scheme_name_tuple_start_direct)
elif SourceType == 'Atoz':
    travel_brand_expr = """
    CASE
        WHEN LEFT(PolicyNumber, 3) IN ('ATO','Ato') THEN 'atoz_travel'
        ELSE 'UNKNOWN'
    END
    """
    additional_filter = "AND (SchemeName IN {} OR SchemeName IN {})".format(scheme_name_tuple_atoz_agg, scheme_name_tuple_atoz_direct)
elif SourceType == 'Trusted':
    travel_brand_expr = """
    CASE
        WHEN LEFT(PolicyNumber, 3) IN ('TRU','TRM') THEN 'trusted_ins'
        ELSE 'UNKNOWN'
    END
    """
    additional_filter = "AND (SchemeName IN {} OR SchemeName IN {})".format(scheme_name_tuple_trusted_agg, scheme_name_tuple_trusted_direct)
elif SourceType == 'SOI':
    travel_brand_expr = """
    CASE
        WHEN LEFT(PolicyNumber, 3) IN ('SOI','SOM') THEN 'switched_on'
        ELSE 'UNKNOWN'
    END
    """
    additional_filter = "AND (SchemeName IN {} OR SchemeName IN {})".format(scheme_name_tuple_soi_agg, scheme_name_tuple_soi_direct)
elif SourceType == 'Start':
    travel_brand_expr = """
    CASE
        WHEN LEFT(PolicyNumber, 3) IN ('STA','STM') THEN 'start_travel'
        ELSE 'UNKNOWN'
    END
    """
    additional_filter = "AND (SchemeName IN {} OR SchemeName IN {})".format(scheme_name_tuple_start_agg, scheme_name_tuple_start_direct)
elif SourceType == 'Viva':
    travel_brand_expr = """
    CASE
        WHEN LEFT(PolicyNumber, 3) IN ('VIV','VIM') THEN 'viva_ins'
        ELSE 'UNKNOWN'
    END
    """
    additional_filter = "AND (SchemeName IN {} OR SchemeName IN {})".format(scheme_name_tuple_viva_agg, scheme_name_tuple_viva_direct)
elif SourceType == 'Oasis':
    travel_brand_expr = """
    CASE
        WHEN LEFT(PolicyNumber, 3) IN ('OI-','OIM') THEN 'oasis_travel'
        ELSE 'UNKNOWN'
    END
    """
    additional_filter = "AND (SchemeName IN {} OR SchemeName IN {})".format(scheme_name_tuple_oasis_agg, scheme_name_tuple_oasis_direct)
else:
    travel_brand_expr = """
    CASE
        WHEN LEFT(PolicyNumber, 3) IN ('SOI','SOM') THEN 'switched_on'
        WHEN LEFT(PolicyNumber, 3) IN ('STA','STM') THEN 'start_travel'
        WHEN LEFT(PolicyNumber, 3) IN ('TC-') THEN 'thomas_cook'
        WHEN LEFT(PolicyNumber, 3) IN ('OI-','OIM') THEN 'oasis_travel'
        WHEN LEFT(PolicyNumber, 3) IN ('TRU','TRM') THEN 'trusted_ins'
        WHEN LEFT(PolicyNumber, 3) IN ('VIV','VIM') THEN 'viva_ins'
        WHEN LEFT(PolicyNumber, 3) IN ('ATO','Ato') THEN 'atoz_travel'
        ELSE 'UNKNOWN'
    END
    """
    additional_filter = ""
    
query = f"""
WITH MAGENTA_TRAVEL_BRAND AS 
(
    SELECT 
        {travel_brand_expr} AS travel_brand,
        CAST(TransactionDate AS DATE) AS Date,
        SUM(CASE WHEN TransactionType = 'New Issue' THEN 1 
                 WHEN TransactionType = 'Cancellation' THEN -1 ELSE 0 END) AS Volume,
        SUM(CASE WHEN TransactionType = 'New Issue' THEN TotalGrossIncIPT 
                 WHEN TransactionType = 'Cancellation' THEN -TotalGrossIncIPT ELSE 0 END) AS GWPincIPTDay,
        SUM(CASE WHEN TransactionType = 'New Issue' THEN TotalGrossExcIPT 
                 WHEN TransactionType = 'Cancellation' THEN -TotalGrossExcIPT ELSE 0 END) AS GWPexIPTDay
    FROM  {base_table}
    WHERE 
        CAST(TransactionDate AS DATE) = DATE('{SelectedDate}')
        {additional_filter}
    GROUP BY
        {travel_brand_expr},
        CAST(TransactionDate AS DATE)
),
MONTHLY_TOTALS AS 
(
    SELECT 
        {travel_brand_expr} AS travel_brand,
        SUM(CASE WHEN TransactionType = 'New Issue' THEN 1 
                 WHEN TransactionType = 'Cancellation' THEN -1 ELSE 0 END) AS VolumeMonth,
        SUM(CASE WHEN TransactionType = 'New Issue' THEN TotalGrossIncIPT 
                 WHEN TransactionType = 'Cancellation' THEN -TotalGrossIncIPT ELSE 0 END) AS TotalGrossIncIPTMonth,
        SUM(CASE WHEN TransactionType = 'New Issue' THEN TotalGrossExcIPT 
                 WHEN TransactionType = 'Cancellation' THEN -TotalGrossExcIPT ELSE 0 END) AS TotalGrossExcIPTMonth
    FROM  {base_table}
    WHERE 
        CAST(TransactionDate AS DATE) >= DATE_TRUNC('MONTH', DATE('{SelectedDate}')) AND
        CAST(TransactionDate AS DATE) <= DATE('{SelectedDate}')
        {additional_filter}
    GROUP BY
        {travel_brand_expr}
)
SELECT 
    daily.travel_brand,
    daily.Date,
    daily.Volume,
    daily.GWPincIPTDay,
    daily.GWPexIPTDay,
    monthly.VolumeMonth,
    monthly.TotalGrossIncIPTMonth,
    monthly.TotalGrossExcIPTMonth
FROM MAGENTA_TRAVEL_BRAND daily
LEFT JOIN MONTHLY_TOTALS monthly
    ON daily.travel_brand = monthly.travel_brand
WHERE daily.travel_brand <> 'UNKNOWN'
"""



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql(query)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# if SourceType in ['Trusted', 'Viva', 'SOI', 'Start']:
#     SourceType = 'Sales'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df = df.select(
    col("travel_brand").cast("string"),
    col("Date").cast("date"),
    col("Volume").cast("double"),  # Force to match target type
    col("GWPincIPTDay").cast("double"),
    col("GWPexIPTDay").cast("double"),
    col("VolumeMonth").cast("double"),  # Also likely came from COUNT()
    col("TotalGrossIncIPTMonth").cast("double"),
    col("TotalGrossExcIPTMonth").cast("double")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode("overwrite").saveAsTable("TaurusBronzeLH.Daily" + SourceType)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
