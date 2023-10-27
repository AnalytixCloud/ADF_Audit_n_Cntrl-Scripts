USE [SUPPLYCHAIN]
GO

/****** Object:  Table [dbo].[supplier]    Script Date: 27-10-2023 17:17:11 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[supplier](
	[S_ID] [float] NULL,
	[Tin_No] [nvarchar](255) NULL,
	[Company_Name] [nvarchar](255) NULL,
	[Date_of_Reg] [datetime] NULL,
	[SubCity] [nvarchar](255) NULL,
	[Town] [nvarchar](255) NULL,
	[Telephone] [float] NULL,
	[Fax] [float] NULL,
	[EMail] [nvarchar](255) NULL,
	[Business_License_No] [nvarchar](255) NULL,
	[Business_Type] [nvarchar](255) NULL
) ON [PRIMARY]
GO


