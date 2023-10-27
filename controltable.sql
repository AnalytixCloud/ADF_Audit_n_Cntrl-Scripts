/****** Object:  Table [dbo].[controltable]    Script Date: 27-10-2023 16:59:45 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[controltable](
	[Source_System_Name] [nvarchar](255) NULL,
	[Business_Area] [nvarchar](255) NULL,
	[Source_Object_Type] [nvarchar](255) NULL,
	[Source_Object_Schema] [nvarchar](255) NULL,
	[Source_Object_Name] [nvarchar](255) NULL,
	[Source_File_Location] [nvarchar](255) NULL,
	[Target_Location] [nvarchar](255) NULL,
	[Target_Object_Name] [nvarchar](255) NULL,
	[File_Format] [nvarchar](255) NULL,
	[Partition_Column] [nvarchar](255) NULL,
	[Load_Type] [nvarchar](255) NULL,
	[Watermark_Column] [nvarchar](255) NULL,
	[Creation_Time] [datetime] NULL,
	[Indicator] [float] NULL,
	[Status] [nvarchar](255) NULL,
	[Last_Successful_date] [datetime] NULL,
	[watermark_value] [nvarchar](100) NULL
) ON [PRIMARY]
GO


