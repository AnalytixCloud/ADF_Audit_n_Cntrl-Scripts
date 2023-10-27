/****** Object:  Table [dbo].[pipeline_log]    Script Date: 27-10-2023 17:01:42 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[pipeline_log](
	[LOG_ID] [int] IDENTITY(1,1) NOT NULL,
	[PARAMETER_ID] [int] NULL,
	[DataFactory_Name] [nvarchar](500) NULL,
	[Pipeline_Name] [nvarchar](500) NULL,
	[RunId] [nvarchar](500) NULL,
	[Source] [nvarchar](500) NULL,
	[Destination] [nvarchar](500) NULL,
	[TriggerType] [nvarchar](500) NULL,
	[TriggerId] [nvarchar](500) NULL,
	[TriggerName] [nvarchar](500) NULL,
	[TriggerTime] [nvarchar](500) NULL,
	[rowsCopied] [nvarchar](500) NULL,
	[DataRead] [int] NULL,
	[No_ParallelCopies] [int] NULL,
	[copyDuration_in_secs] [nvarchar](500) NULL,
	[effectiveIntegrationRuntime] [nvarchar](500) NULL,
	[Source_Type] [nvarchar](500) NULL,
	[Sink_Type] [nvarchar](500) NULL,
	[Execution_Status] [nvarchar](500) NULL,
	[CopyActivity_Start_Time] [nvarchar](500) NULL,
	[CopyActivity_End_Time] [nvarchar](500) NULL,
	[CopyActivity_queuingDuration_in_secs] [nvarchar](500) NULL,
	[CopyActivity_transferDuration_in_secs] [nvarchar](500) NULL,
 CONSTRAINT [PK_pipeline_logs] PRIMARY KEY CLUSTERED 
(
	[LOG_ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


