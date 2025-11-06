USE [NHS_A_E_Warehouse];
GO

PRINT 'Running Silver Layer DQ Checks...';

-- Check 1: No negative wait times
IF EXISTS (
    SELECT 1 FROM [silver].[Clean_Patient_Journeys]
    WHERE [Mins_Arrival_to_Triage] < 0
       OR [Mins_Triage_to_Doctor] < 0
       OR [Mins_Doctor_to_DTA] < 0
       OR [Mins_DTA_to_Ward] < 0
)
BEGIN
    RAISERROR ('DQ Check FAILED: Negative wait times found in silver.Clean_Patient_Journeys', 16, 1);
END
ELSE
BEGIN
    PRINT 'DQ Check PASSED: No negative wait times.';
END

-- Check 2: Occupancy percentage within bounds
IF EXISTS (
    SELECT 1 FROM [silver].[Clean_Ward_Occupancy]
    WHERE [OccupancyPercentage] < 0 OR [OccupancyPercentage] > 100
)
BEGIN
    RAISERROR ('DQ Check FAILED: Occupancy percentage out of bounds (0-100) in silver.Clean_Ward_Occupancy', 16, 1);
END
ELSE
BEGIN
    PRINT 'DQ Check PASSED: Occupancy percentages are valid.';
END

-- Check 3: No NULL primary keys
IF EXISTS (
    SELECT 1 FROM [silver].[Clean_Patient_Journeys] WHERE [PatientJourneyKey] IS NULL
)
BEGIN
    RAISERROR ('DQ Check FAILED: NULL Primary Key found in silver.Clean_Patient_Journeys', 16, 1);
END
ELSE
BEGIN
    PRINT 'DQ Check PASSED: No NULL Primary Keys in Silver.';
END

PRINT 'Silver Layer DQ Checks Complete.';
