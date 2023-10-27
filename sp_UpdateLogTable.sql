/****** Object:  StoredProcedure [dbo].[sp_UpdateLogTable]    Script Date: 27-10-2023 17:02:49 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[sp_UpdateLogTable]
@DataFactory_Name VARCHAR(250),
@Pipeline_Name VARCHAR(250),
@RunID VARCHAR(250),
@Source VARCHAR(300),
@Destination VARCHAR(300),
@TriggerType VARCHAR(300),
@TriggerId VARCHAR(300),
@TriggerName VARCHAR(300),
@TriggerTime VARCHAR(500),
@rowsCopied VARCHAR(300),
@DataRead INT,
@copyDuration_in_secs VARCHAR(300),
@effectiveIntegrationRuntime VARCHAR(300),
@Source_Type VARCHAR(300),
@Sink_Type VARCHAR(300),
@Execution_Status VARCHAR(300),
@CopyActivity_Start_Time VARCHAR(500),
@CopyActivity_End_Time VARCHAR(500),
@CopyActivity_queuingDuration_in_secs VARCHAR(500),
@CopyActivity_transferDuration_in_secs VARCHAR(500)
AS
INSERT INTO [pipeline_log]
(
      [DataFactory_Name]
      ,[Pipeline_Name]
      ,[RunId]
      ,[Source]
      ,[Destination]
      ,[TriggerType]
      ,[TriggerId]
      ,[TriggerName]
      ,[TriggerTime]
      ,[rowsCopied]
      ,[DataRead]
      ,[copyDuration_in_secs]
      ,[effectiveIntegrationRuntime]
      ,[Source_Type]
      ,[Sink_Type]
      ,[Execution_Status]
      ,[CopyActivity_Start_Time]
      ,[CopyActivity_End_Time]
      ,[CopyActivity_queuingDuration_in_secs]
      ,[CopyActivity_transferDuration_in_secs]
)
VALUES
(
@DataFactory_Name
      ,@Pipeline_Name
      ,@RunId
      ,@Source
      ,@Destination
      ,@TriggerType
      ,@TriggerId
      ,@TriggerName
      ,@TriggerTime
      ,@rowsCopied
      ,@DataRead
      ,@copyDuration_in_secs
      ,@effectiveIntegrationRuntime
      ,@Source_Type
      ,@Sink_Type
      ,@Execution_Status
      ,@CopyActivity_Start_Time
      ,@CopyActivity_End_Time
      ,@CopyActivity_queuingDuration_in_secs
      ,@CopyActivity_transferDuration_in_secs
)
GO


