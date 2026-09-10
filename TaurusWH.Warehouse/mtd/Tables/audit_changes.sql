CREATE TABLE [mtd].[audit_changes] (

	[source_type] varchar(20) NULL, 
	[event_run_id] varchar(50) NULL, 
	[event_activity_run_id] varchar(50) NULL, 
	[item_name] varchar(150) NULL, 
	[data_read] bigint NULL, 
	[data_written] bigint NULL, 
	[files_read] int NULL, 
	[files_written] int NULL, 
	[rows_read] bigint NULL, 
	[rows_written] bigint NULL, 
	[data_consistency_verification] varchar(50) NULL, 
	[copy_duration] int NULL, 
	[event_start_time] datetime2(6) NULL, 
	[event_end_time] datetime2(6) NULL, 
	[source_cutoff_time] datetime2(6) NULL, 
	[load_type] varchar(100) NULL, 
	[status] varchar(20) NULL, 
	[event_triggered_by] varchar(20) NULL, 
	[error_details] varchar(1500) NULL, 
	[pipeline_url] varchar(500) NULL
);