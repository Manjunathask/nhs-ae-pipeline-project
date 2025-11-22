USE [NHS_A_E_Warehouse];
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[silver].[Clean_External_Benchmarks]') AND type in (N'U'))
BEGIN
    CREATE TABLE [silver].[Clean_External_Benchmarks](
        [Period] DATE NOT NULL,
        [OrgCode] NVARCHAR(10) NOT NULL,
        [OrgName] NVARCHAR(255) NULL,
        [Attendances_Type_1] INT NULL,
        [Attendances_Over_4hrs_Type_1] INT NULL,
        [Waited_4_12_hrs_DTA] INT NULL,
        [Waited_12_plus_hrs_DTA] INT NULL,
        [Emergency_Admissions_Type_1] INT NULL
        -- Add any other key measures you want to analyze --
    );
END
GO