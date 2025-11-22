USE [NHS_A_E_Warehouse];
GO

CREATE OR ALTER PROCEDURE [gold].[sp_Load_Fact_Monthly_Benchmark]
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @RunStartTime DATETIME = GETDATE();
    PRINT 'Starting Silver-to-Gold Benchmark Load...';

    --================================================================================
    -- STEP 1: Update Dim_Organisation (SCD Type 1)
    -- Use MERGE to insert new organisations or update names for existing ones.
    --================================================================================

    PRINT 'Updating gold.Dim_Organisation...';

    MERGE INTO [gold].[Dim_Organisation] AS target
    USING (
        -- Get the unique list of orgs from the silver table
        SELECT DISTINCT [OrgCode], [OrgName]
        FROM [silver].[Clean_External_Benchmarks]
        WHERE [OrgCode] IS NOT NULL
    ) AS source
    ON target.[OrgCode] = source.[OrgCode]

    -- WHEN MATCHED: Update the name if it has changed
    WHEN MATCHED AND (ISNULL(target.[OrgName], '') <> ISNULL(source.[OrgName], '')) THEN
        UPDATE SET
            target.[OrgName] = source.[OrgName]

    -- WHEN NOT MATCHED: Insert the new organisation
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            [OrgCode],
            [OrgName]
        )
        VALUES (
            source.[OrgCode],
            source.[OrgName]
        );
    
    PRINT 'gold.Dim_Organisation update complete.';

    --================================================================================
    -- STEP 2: Ensure 'Unknown' Record Exists in Dim_Organisation
    -- Best practice to handle any facts that might have a NULL OrgCode.
    --================================================================================
    
    PRINT 'Ensuring -1 (Unknown) OrganisationKey exists...';

    SET IDENTITY_INSERT [gold].[Dim_Organisation] ON;

    IF NOT EXISTS (SELECT 1 FROM [gold].[Dim_Organisation] WHERE [OrganisationKey] = -1)
    BEGIN
        INSERT INTO [gold].[Dim_Organisation] (
            [OrganisationKey],
            [OrgCode],
            [OrgName]
        )
        VALUES (
            -1,
            'UNK',
            'Unknown'
        );
    END
    
    SET IDENTITY_INSERT [gold].[Dim_Organisation] OFF;


    --================================================================================
    -- STEP 3: Rebuild Fact_Monthly_Benchmark
    -- Since the silver benchmark table is truncated and reloaded,
    -- the correct idempotent pattern is to truncate and reload this fact table.
    -- The volume is small, so this is very fast and ensures data is always correct.
    --================================================================================

    PRINT 'Truncating gold.Fact_Monthly_Benchmark...';
    TRUNCATE TABLE [gold].[Fact_Monthly_Benchmark];

    PRINT 'Loading data into gold.Fact_Monthly_Benchmark...';

    INSERT INTO [gold].[Fact_Monthly_Benchmark] (
        [DateKey],
        [OrganisationKey],
        [Attendances_Type_1],
        [Attendances_Over_4hrs_Type_1],
        [Waited_4_12_hrs_DTA],
        [Waited_12_plus_hrs_DTA],
        [Emergency_Admissions_Type_1]
    )
    SELECT
        -- Look up the DateKey from Dim_Date
        CONVERT(INT, FORMAT(s.[Period], 'yyyyMMdd')) AS [DateKey],
        
        -- Look up the OrganisationKey. Default to -1 if not found.
        COALESCE(o.[OrganisationKey], -1) AS [OrganisationKey],

        -- Measures
        s.[Attendances_Type_1],
        s.[Attendances_Over_4hrs_Type_1],
        s.[Waited_4_12_hrs_DTA],
        s.[Waited_12_plus_hrs_DTA],
        s.[Emergency_Admissions_Type_1]
    FROM
        [silver].[Clean_External_Benchmarks] AS s
    LEFT JOIN
        [gold].[Dim_Organisation] AS o ON s.[OrgCode] = o.[OrgCode]
    WHERE
        -- Ensure we have a valid date to join to Dim_Date
        s.[Period] IS NOT NULL;

    PRINT 'Fact_Monthly_Benchmark load complete.';
    PRINT 'Silver-to-Gold Benchmark Load Finished. Run time: ' + CAST(DATEDIFF(SECOND, @RunStartTime, GETDATE()) AS VARCHAR) + ' seconds.';

END
GO

-- To run this procedure in Airflow, you'll just call(Not Implemented Yet)
-- EXEC [gold].[sp_Load_Fact_Monthly_Benchmark];