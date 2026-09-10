# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dbfc334d-f83f-43fc-965f-0a38145f4700",
# META       "default_lakehouse_name": "TaurusSilverLH",
# META       "default_lakehouse_workspace_id": "9c958852-649b-48e2-ac6a-f03167ff64dc",
# META       "known_lakehouses": [
# META         {
# META           "id": "dbfc334d-f83f-43fc-965f-0a38145f4700"
# META         },
# META         {
# META           "id": "3b2d8490-dd7d-4046-a7c8-21da791cfcdd"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# File_Type = "Viva"
# SelectedDate = "2025-07-04"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StringType, DateType, DoubleType, StructType, StructField


if File_Type in ['Trusted', 'Viva', 'SOI', 'Start']:
    Target_File_Type = 'Sales'
else:
    Target_File_Type = File_Type

WORKSPACE = f"Taurus_TravelInsurance_Dev"
SRC_LAKEHOUSE = f"TaurusBronzeLH"
SRC_TABLE = SRC_LAKEHOUSE + '.Daily' + File_Type

TRG_LAKEHOUSE = f"TaurusSilverLH"
TRG_SILVER_TABLE = 'Daily' + Target_File_Type

column_mappings_sales = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_oasis = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_atoz = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_new = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_direct = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_renewal = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_oasis_agg = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_oasis_direct = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_atoz_agg = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_atoz_direct = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_viva_agg = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_viva_direct = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_soi_agg = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_soi_direct = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_start_agg = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_start_direct = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_trusted_agg = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}

column_mappings_trusted_direct = {
    "travel_brand": ("travel_brand", StringType()),
    "Date": ("Date", DateType()),
    "Volume": ("Volume", DoubleType()),
    "GWPincIPTDay": ("GWPincIPTDay", DoubleType()),
    "GWPexIPTDay": ("GWPexIPTDay", DoubleType()),
    "VolumeMonth": ("VolumeMonth", DoubleType()),
    "TotalGrossIncIPTMonth": ("TotalGrossIncIPTMonth", DoubleType()),
    "TotalGrossExcIPTMonth": ("TotalGrossExcIPTMonth", DoubleType())
}


# Mapping the source table to the appropriate column mappings
mapping_files = {
    "sales_source_file": column_mappings_sales,
    "oasis_source_file": column_mappings_oasis,
    "atoz_source_file": column_mappings_atoz,
    "atoz_source_file": column_mappings_direct,
    "atoz_source_file": column_mappings_new,
    "atoz_source_file": column_mappings_renewal,

    # Add more mappings as needed
}

# Function to dynamically pick the column mappings based on the source file
def get_column_mappings(SRC_TABLE):
    # Logic to determine the key for the source file
    if SRC_TABLE == SRC_LAKEHOUSE + '.DailyTrusted':
        return column_mappings_sales
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailySales':
        return column_mappings_sales
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyViva':
        return column_mappings_sales
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailySOI':
        return column_mappings_sales
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyStart':
        return column_mappings_sales
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyOasis':
        return column_mappings_oasis
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyAtoz':
        return column_mappings_atoz
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyNew':
        return column_mappings_new
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyRenewal':
        return column_mappings_renewal
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyDirect':
        return column_mappings_direct
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyAtozAgg':
        return column_mappings_atoz_agg
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyAtozDirect':
        return column_mappings_atoz_direct
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyOasisAgg':
        return column_mappings_oasis_agg
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyOasisDirect':
        return column_mappings_oasis_direct
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyVivaAgg':
        return column_mappings_viva_agg
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyVivaDirect':
        return column_mappings_viva_direct
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailySOIAgg':
        return column_mappings_soi_agg
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailySOIDirect':
        return column_mappings_soi_direct
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyTrustedAgg':
        return column_mappings_trusted_agg
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyTrustedDirect':
        return column_mappings_trusted_direct
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyStartAgg':
        return column_mappings_start_agg
    elif SRC_TABLE == SRC_LAKEHOUSE + '.DailyStartDirect':
        return column_mappings_start_direct
    else:
        raise ValueError(f"Mapping not defined for source file: {src_file_path}")

# def create_silver_layer(src_file_path: str, dest_file_path: str, column_mappings: dict):
def create_silver_layer(dest_file_path: str, column_mappings: dict):
    # Read the Bronze Delta table
    bronze_df = spark.read.table(SRC_TABLE)
    display(bronze_df)

    # Extract distinct Date values from the source file
    date_column = column_mappings["Date"][0]  # Get the new column name for Date
    distinct_dates = bronze_df.select(to_date(col("Date"), "dd/MM/yyyy").alias(date_column)).distinct()

    # Check if the destination path exists
    try:
        # Load the Silver Delta table if it exists
        silver_df = spark.read.format("Delta").load(dest_file_path)
    except:
        # If the path does not exist, create an empty DataFrame with the proper schema
        silver_schema = StructType([
            StructField(new_column_name, new_data_type, True)
            for _, (new_column_name, new_data_type) in column_mappings.items()
        ])
        silver_df = spark.createDataFrame([], silver_schema)
    
    # Delete records in the Silver Delta table for matching dates
    # for row in distinct_dates.collect():
    #     date_to_delete = row[date_column]
    #     silver_df = silver_df.filter(col(date_column) != date_to_delete)

    if File_Type == 'Viva':
        travel_brand_literal = 'viva_ins'
    elif File_Type == 'Trusted':
        travel_brand_literal = 'trusted_ins'
    elif File_Type == 'Start':
        travel_brand_literal = 'start_travel'
    elif File_Type == 'SOI':
        travel_brand_literal = 'switched_on'
    elif File_Type == 'Atoz':
        travel_brand_literal = 'atoz_travel'
    elif File_Type == 'Oasis':
        travel_brand_literal = 'oasis_travel'
    else: 
        travel_brand_literal = File_Type
    print(File_Type)
    for row in distinct_dates.collect():
        date_to_delete = row[date_column]
        silver_df = silver_df.filter(~((col(date_column) == date_to_delete) & (col("travel_brand") == travel_brand_literal))
    )

    # Apply transformations (cast to appropriate data type) and rename columns
    for column, (new_column_name, new_data_type) in column_mappings.items():
        if column == "Date":  # Handle Date column specifically with correct format
            bronze_df = bronze_df.withColumn(new_column_name, to_date(col(column), "dd/MM/yyyy"))
        elif new_data_type == DateType():
            bronze_df = bronze_df.withColumn(new_column_name, to_date(col(column), "yyyy-MM-dd"))
        else:
            bronze_df = bronze_df.withColumn(new_column_name, col(column).cast(new_data_type))
    
    # Select transformed columns based on new names
    silver_columns = [new_column_name for _, (new_column_name, _) in column_mappings.items()]
    transformed_df = bronze_df.select(*silver_columns)

    # Append the new transformed data to the updated Silver Delta table
    updated_silver_df = silver_df.union(transformed_df)

    # Write the updated Silver Delta table back to the destination path in append mode
    updated_silver_df.write.format("Delta").mode("overwrite").save(dest_file_path)

    # Print schema for debugging purposes
    updated_silver_df.printSchema()

# Example Usage
# src_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SRC_LAKEHOUSE}.Lakehouse/Tables/{SRC_TABLE}"
dest_file_path = f"abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{TRG_LAKEHOUSE}.Lakehouse/Tables/{TRG_SILVER_TABLE}"

# Dynamically fetch the column mappings based on the source file
column_mappings = get_column_mappings(SRC_TABLE)





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

create_silver_layer(dest_file_path, column_mappings)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC select * from DailySales
# MAGIC where date = '2025-09-02'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
