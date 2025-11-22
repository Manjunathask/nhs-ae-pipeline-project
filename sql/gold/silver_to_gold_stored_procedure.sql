USE [NHS_A_E_Warehouse];
GO

CREATE OR ALTER PROCEDURE [gold].[sp_Load_Fact_PatientJourney_Incremental]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RunStartTime DATETIME = GETDATE();
    PRINT 'Starting Silver-to-Gold Load...';

    --================================================================================
    -- STEP 1: Update Dim_Ward (SCD Type 1)
    -- We use MERGE to insert new wards or update existing ones (e.g., if TotalBeds changes).
    --================================================================================

    PRINT 'Updating gold.Dim_Ward...';

    MERGE INTO [gold].[Dim_Ward] AS target
    USING [silver].[Clean_Ward_Occupancy] AS source
    ON target.[WardID] = source.[WardID]

    -- WHEN MATCHED: Update if any details have changed (SCD Type 1)
    WHEN MATCHED AND (
        ISNULL(target.[WardName], '') <> ISNULL(source.[WardName], '') OR
        ISNULL(target.[Specialty], '') <> ISNULL(source.[Specialty], '') OR
        ISNULL(target.[TotalBeds], 0) <> ISNULL(source.[TotalBeds], 0)
    ) THEN
        UPDATE SET
            target.[WardName] = source.[WardName],
            target.[Specialty] = source.[Specialty],
            target.[TotalBeds] = source.[TotalBeds]

    -- WHEN NOT MATCHED: Insert new ward
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            [WardID],
            [WardName],
            [Specialty],
            [TotalBeds]
        )
        VALUES (
            source.[WardID],
            source.[WardName],
            source.[Specialty],
            source.[TotalBeds]
        );
    
    PRINT 'gold.Dim_Ward update complete.';


    --================================================================================
    -- STEP 2: Ensure 'Unknown' Record Exists in Dim_Ward
    -- This is a data warehousing best practice. It prevents our load from
    -- failing if a fact record has a WardID we've never seen before.
    -- We use -1 as the key to avoid conflicts with the IDENTITY(1,1) column.
    --================================================================================
    
    PRINT 'Ensuring -1 (Unknown) WardKey exists...';

    SET IDENTITY_INSERT [gold].[Dim_Ward] ON;

    IF NOT EXISTS (SELECT 1 FROM [gold].[Dim_Ward] WHERE [WardKey] = -1)
    BEGIN
        INSERT INTO [gold].[Dim_Ward] (
            [WardKey],
            [WardID],
            [WardName],
            [Specialty],
            [TotalBeds]
        )
        VALUES (
            -1,
            'UNK',
            'Unknown',
            'Unknown',
            0
        );
    END
    
    SET IDENTITY_INSERT [gold].[Dim_Ward] OFF;


    --================================================================================
    -- STEP 3: Load NEW records into Fact_PatientJourney
    -- We join Silver to Gold Dims to get surrogate keys.
    -- We use WHERE NOT EXISTS to ensure we only insert new rows.
    --================================================================================

    PRINT 'Loading new records into gold.Fact_PatientJourney...';

    INSERT INTO [gold].[Fact_PatientJourney] (
        [PatientJourneyKey],
        [Arrival_DateKey],
        [Arrival_TimeKey],
        [DTA_DateKey],
        [DTA_TimeKey],
        [Discharge_DateKey],
        [Discharge_TimeKey],
        [WardKey],
        [Mins_Arrival_to_Triage],
        [MMins_Triage_to_Doctor], -- Note: Mapping Silver.Mins_Triage_to_Doctor to this column
        [Mins_Doctor_to_DTA],
        [Mins_DTA_to_Ward],
        [Total_Mins_in_AE],
        [Breached_4Hr_Flag],
        [Ward_Occupancy_Percent_at_DTA]
    )
    SELECT
        s.[PatientJourneyKey],

        -- Convert timestamps to dimension keys
        -- We trust the Dim_Date/Dim_Time tables are pre-populated
        CONVERT(INT, FORMAT(s.[Arrival_Timestamp], 'yyyyMMdd')) AS [Arrival_DateKey],
        CONVERT(INT, FORMAT(s.[Arrival_Timestamp], 'HHmm')) AS [Arrival_TimeKey],
        CONVERT(INT, FORMAT(s.[DecisionToAdmit_Timestamp], 'yyyyMMdd')) AS [DTA_DateKey],
        CONVERT(INT, FORMAT(s.[DecisionToAdmit_Timestamp], 'HHmm')) AS [DTA_TimeKey],
        CONVERT(INT, FORMAT(s.[Discharge_Timestamp], 'yyyyMMdd')) AS [Discharge_DateKey],
        CONVERT(INT, FORMAT(s.[Discharge_Timestamp], 'HHmm')) AS [Discharge_TimeKey],

        -- Look up the Ward surrogate key. If not found, use -1 (Unknown)
        COALESCE(w.[WardKey], -1) AS [WardKey],

        -- Direct Measures
        s.[Mins_Arrival_to_Triage],
        s.[Mins_Triage_to_Doctor], -- This maps to gold column [MMins_Triage_to_Doctor]
        s.[Mins_Doctor_to_DTA],
        s.[Mins_DTA_to_Ward],
        s.[Total_Mins_in_AE],
        s.[Breached_4Hr_Flag],
        s.[Ward_Occupancy_Percent_at_DTA]

    FROM
        [silver].[Clean_Patient_Journeys] AS s
    LEFT JOIN
        [gold].[Dim_Ward] AS w ON s.[WardID_at_DTA] = w.[WardID]
    
    WHERE NOT EXISTS (
        SELECT 1
        FROM [gold].[Fact_PatientJourney] AS f
        WHERE f.[PatientJourneyKey] = s.[PatientJourneyKey]
    );

    PRINT 'Fact_PatientJourney load complete.';
    PRINT 'Silver-to-Gold Load Finished. Run time: ' + CAST(DATEDIFF(SECOND, @RunStartTime, GETDATE()) AS VARCHAR) + ' seconds.';

END



---EXEC [gold].[sp_Load_Fact_PatientJourney_Incremental];