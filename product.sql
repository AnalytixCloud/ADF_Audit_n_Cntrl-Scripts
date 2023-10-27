USE [SUPPLYCHAIN]
GO

/****** Object:  Table [dbo].[product]    Script Date: 27-10-2023 17:03:50 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[product](
	[P_ID] [nvarchar](255) NULL,
	[Product_type] [nvarchar](255) NULL,
	[Price] [float] NULL
) ON [PRIMARY]
GO


