USE [NHS_A_E_Warehouse];
GO

PRINT 'Running Gold Layer DQ Checks...';

-- Check 1: Fact table foreign keys should not be NULL
IF EXISTS (
    SELECT 1 FROM [gold].[Fact_PatientJourney]
    WHERE [Arrival_DateKey] IS NULL
       OR [Arrival_TimeKey] IS NULL
       OR [WardKey] IS NULL
)
BEGIN
    RAISERROR ('DQ Check FAILED: NULL Foreign Keys found in gold.Fact_PatientJourney', 16, 1);
END
ELSE
BEGIN
    PRINT 'DQ Check PASSED: No NULL Foreign Keys in Fact table.';
END

-- Check 2: Check for referential integrity (e.g., all Arrival_DateKey exist in Dim_Date)
IF EXISTS (
    SELECT 1
    FROM [gold].[Fact_PatientJourney] f
    LEFT JOIN [gold].[Dim_Date] d ON f.[Arrival_DateKey] = d.[DateKey]
    WHERE d.[DateKey] IS NULL
)
BEGIN
    RAISERROR ('DQ Check FAILED: Orphan records found in Fact_PatientJourney (Dim_Date)', 16, 1);
END
ELSE
BEGIN
    PRINT 'DQ Check PASSED: Referential integrity for Dim_Date is valid.';
END

-- Check 3: Check for duplicate keys in Dimensions
IF EXISTS (
    SELECT [DateKey], COUNT(1)
    FROM [gold].[Dim_Date]
    GROUP BY [DateKey]
    HAVING COUNT(1) > 1
)
BEGIN
    RAISERROR ('DQ Check FAILED: Duplicate keys found in gold.Dim_Date', 16, 1);
END
ELSE
BEGIN
    PRINT 'DQ Check PASSED: No duplicate keys in Dim_Date.';
END


PRINT 'Gold Layer DQ Checks Complete.';
