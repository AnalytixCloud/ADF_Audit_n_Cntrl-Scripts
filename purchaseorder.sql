USE [SUPPLYCHAIN]
GO

/****** Object:  Table [dbo].[purchaseorder]    Script Date: 27-10-2023 17:16:28 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[purchaseorder](
	[PO_ID] [int] IDENTITY(1,1) NOT NULL,
	[P_ID] [nvarchar](255) NULL,
	[Availability] [float] NULL,
	[Number_of_products_sold] [float] NULL,
	[Revenue_generated] [float] NULL,
	[Stock_levels] [float] NULL,
	[Lead_times] [float] NULL,
	[Order_quantities] [float] NULL,
	[Shipping_times] [float] NULL,
	[Shipping_carriers] [nvarchar](255) NULL,
	[Shipping_costs] [float] NULL,
	[Supplier_Id] [float] NULL,
	[Location] [nvarchar](255) NULL,
	[Lead_time] [float] NULL,
	[Production_volumes] [float] NULL,
	[Manufacturing_lead_time] [float] NULL,
	[Manufacturing_costs] [float] NULL,
	[Inspection_results] [nvarchar](255) NULL,
	[Defect_rates] [float] NULL,
	[Transportation_id] [float] NULL,
	[Routes] [nvarchar](255) NULL,
	[Costs] [float] NULL
) ON [PRIMARY]
GO


