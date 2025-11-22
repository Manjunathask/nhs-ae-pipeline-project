USE [NHS_A_E_Warehouse];
GO

-- Dimension for Organisations
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[gold].[Dim_Organisation]') AND type in (N'U'))
BEGIN
    CREATE TABLE [gold].[Dim_Organisation](
        [OrganisationKey] INT IDENTITY(1,1) PRIMARY KEY,
        [OrgCode] NVARCHAR(10) NOT NULL,
        [OrgName] NVARCHAR(255) NULL
    );
END
GO

-- Fact table for monthly benchmark data
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[gold].[Fact_Monthly_Benchmark]') AND type in (N'U'))
BEGIN
    CREATE TABLE [gold].[Fact_Monthly_Benchmark](
        [BenchmarkKey] INT IDENTITY(1,1) PRIMARY KEY,
        [DateKey] INT NOT NULL, -- FK to Dim_Date
        [OrganisationKey] INT NOT NULL, -- FK to Dim_Organisation
        
        -- Measures
        [Attendances_Type_1] INT NULL,
        [Attendances_Over_4hrs_Type_1] INT NULL,
        [Waited_4_12_hrs_DTA] INT NULL,
        [Waited_12_plus_hrs_DTA] INT NULL,
        [Emergency_Admissions_Type_1] INT NULL,
        
        -- Constraints
        CONSTRAINT [FK_Fact_Benchmark_Dim_Date] FOREIGN KEY ([DateKey]) REFERENCES [gold].[Dim_Date] ([DateKey]),
        CONSTRAINT [FK_Fact_Benchmark_Dim_Organisation] FOREIGN KEY ([OrganisationKey]) REFERENCES [gold].[Dim_Organisation] ([OrganisationKey])
    );
END
GO