CREATE TABLE [mtd].[control_events] (

	[source_type] varchar(20) NOT NULL, 
	[source_container_name] varchar(50) NULL, 
	[source_folder_path] varchar(100) NULL, 
	[source_database_name] varchar(100) NULL, 
	[source_schema_name] varchar(50) NULL, 
	[source_table_name] varchar(100) NULL, 
	[source_column_list] varchar(1000) NULL, 
	[source_watermark_column] varchar(100) NULL, 
	[target_container_name] varchar(50) NOT NULL, 
	[target_folder_path] varchar(100) NOT NULL, 
	[load_type] varchar(100) NOT NULL, 
	[enable_flag] int NOT NULL, 
	[processing_frequency] varchar(100) NULL, 
	[data_load_type] varchar(255) NULL, 
	[last_process_id] int NULL, 
	[source_date_column] varchar(100) NULL
);