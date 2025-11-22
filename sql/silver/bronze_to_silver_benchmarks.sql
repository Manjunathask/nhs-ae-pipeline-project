USE [NHS_A_E_Warehouse];
GO

-- TRUNCATE and RELOAD this table each time
TRUNCATE TABLE [silver].[Clean_External_Benchmarks];
GO

INSERT INTO [silver].[Clean_External_Benchmarks] (
    [Period],
    [OrgCode],
    [OrgName],
    [Attendances_Type_1],
    [Attendances_Over_4hrs_Type_1],
    [Waited_4_12_hrs_DTA],
    [Waited_12_plus_hrs_DTA],
    [Emergency_Admissions_Type_1]
)
SELECT
    --Robustly parse the date
    TRY_CAST(
        parsed.YearString + '-' +
        CASE parsed.MonthString
            WHEN 'APRIL' THEN '04'
            WHEN 'MAY' THEN '05'
            WHEN 'JUNE' THEN '06'
            WHEN 'JULY' THEN '07'
            WHEN 'AUGUST' THEN '08'
            WHEN 'SEPTEMBER' THEN '09'
            -- Add any other months you find
        END + '-01'
    AS DATE) AS [Period],
    
    b.[org_code] AS [OrgCode],
    b.[org_name] AS [OrgName],
    
    -- Convert text to numbers, replacing NULLs with 0
    ISNULL(TRY_CAST(b.[aande_attendances_type_1] AS INT), 0) AS [Attendances_Type_1],
    ISNULL(TRY_CAST(b.[attendances_over_4hrs_type_1] AS INT), 0) AS [Attendances_Over_4hrs_Type_1],
    ISNULL(TRY_CAST(b.[patients_who_have_waited_4_12_hs_from_dta_to_admission] AS INT), 0) AS [Waited_4_12_hrs_DTA],
    ISNULL(TRY_CAST(b.[patients_who_have_waited_12+_hrs_from_dta_to_admission] AS INT), 0) AS [Waited_12_plus_hrs_DTA],
    ISNULL(TRY_CAST(b.[emergency_admissions_via_aande___type_1] AS INT), 0) AS [Emergency_Admissions_Type_1]

FROM
    [bronze].[External_Benchmarks] AS b
-- Use CROSS APPLY to define the parsing logic once.
-- This makes the SELECT statement much cleaner.
CROSS APPLY (
    SELECT
        -- Find the position of the 2nd hyphen (the one after the month)
        -- We start searching from char 8 (just after 'MSitAE-')
        CHARINDEX('-', b.period, 8) AS SecondHyphenPos
) AS ca1
CROSS APPLY (
    SELECT
        -- Extract Month
        -- Starts at char 8, length is (Position of 2nd hyphen) - 8
        SUBSTRING(b.period, 8, ca1.SecondHyphenPos - 8) AS MonthString,
        
        -- Extract Year
        -- Starts 1 char after the 2nd hyphen, for 4 chars
        SUBSTRING(b.period, ca1.SecondHyphenPos + 1, 4) AS YearString
) AS parsed
WHERE
    -- Filter out bad rows
    b.[org_code] <> 'TOTAL'
    AND b.[org_code] IS NOT NULL
    -- This filter ensures the string has the format we expect
    -- and prevents the CHARINDEX/SUBSTRING error
    AND b.period LIKE 'MSitAE-%-%' 
    AND ca1.SecondHyphenPos > 8; -- Ensures the 2nd hyphen was found successfully
GO