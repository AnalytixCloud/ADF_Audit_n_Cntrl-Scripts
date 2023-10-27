/****** Object:  Table [dbo].[pipeline_errors]    Script Date: 27-10-2023 17:00:56 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[pipeline_errors](
	[error_id] [int] IDENTITY(1,1) NOT NULL,
	[parameter_id] [int] NULL,
	[DataFactory_Name] [nvarchar](500) NULL,
	[Pipeline_Name] [nvarchar](500) NULL,
	[RunId] [nvarchar](500) NULL,
	[Source] [nvarchar](500) NULL,
	[Destination] [nvarchar](500) NULL,
	[TriggerType] [nvarchar](500) NULL,
	[TriggerId] [nvarchar](500) NULL,
	[TriggerName] [nvarchar](500) NULL,
	[TriggerTime] [nvarchar](500) NULL,
	[No_ParallelCopies] [int] NULL,
	[copyDuration_in_secs] [nvarchar](500) NULL,
	[effectiveIntegrationRuntime] [nvarchar](500) NULL,
	[Source_Type] [nvarchar](500) NULL,
	[Sink_Type] [nvarchar](500) NULL,
	[Execution_Status] [nvarchar](500) NULL,
	[ErrorDescription] [nvarchar](max) NULL,
	[ErrorCode] [nvarchar](500) NULL,
	[ErrorLoggedTime] [nvarchar](500) NULL,
	[FailureType] [nvarchar](500) NULL,
 CONSTRAINT [PK_pipeline_error] PRIMARY KEY CLUSTERED 
(
	[error_id] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO


