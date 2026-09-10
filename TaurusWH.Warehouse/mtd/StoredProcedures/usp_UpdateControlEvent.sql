CREATE PROCEDURE mtd.usp_UpdateControlEvent
    @SourceTableName NVARCHAR(255),
    @DataLoadType NVARCHAR(50),
    @SourceContainerName NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE mtd.control_events
    SET 
        data_load_type = @DataLoadType
    WHERE 
        source_table_name = @SourceTableName and 
        enable_flag = 1 and 
        source_container_name = @SourceContainerName;
END;