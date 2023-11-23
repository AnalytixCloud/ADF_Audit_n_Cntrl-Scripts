create PROCEDURE [dbo].[sp_UpdateWatermark]
    @Source_Object_Name NVARCHAR(255),
    @Watermark_Column NVARCHAR(255),
    @Watermark_Value NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Sql NVARCHAR(MAX);

    -- Construct the SQL to update the watermark in the control table
    SET @Sql = '
        UPDATE [dbo].[controltable]
        SET watermark_value = ''' + @Watermark_Value + '''
        WHERE Source_Object_Name = ''' + @Source_Object_Name + '''
          AND Watermark_Column = ''' + @Watermark_Column + '''
    ';

    -- Execute the dynamic SQL
    EXEC sp_executesql @Sql;
END;