USE [NHS_A_E_Warehouse];
GO

-- Upsert/Merge raw patient journeys from bronze.Patient_AE_Journeys to silver.Clean_Patient_Journeys
MERGE INTO silver.Clean_Patient_Journeys AS target
USING (
    SELECT
        CONCAT(PatientID, '_', FORMAT(Arrival_Timestamp, 'yyyyMMddHHmmss')) AS PatientJourneyKey,
        PatientID,
        Arrival_Timestamp,
        Triage_Timestamp,
        SeenByDoctor_Timestamp,
        DecisionToAdmit_Timestamp,
        WardAdmission_Timestamp,
        Discharge_Timestamp,
        Discharge_Reason,
        CurrentLocation,
        DATEDIFF(MINUTE, Arrival_Timestamp, Triage_Timestamp) AS Mins_Arrival_to_Triage,
        DATEDIFF(MINUTE, Triage_Timestamp, SeenByDoctor_Timestamp) AS Mins_Triage_to_Doctor,
        DATEDIFF(MINUTE, SeenByDoctor_Timestamp, DecisionToAdmit_Timestamp) AS Mins_Doctor_to_DTA,
        DATEDIFF(MINUTE, DecisionToAdmit_Timestamp, WardAdmission_Timestamp) AS Mins_DTA_to_Ward,
        DATEDIFF(MINUTE, Arrival_Timestamp, Discharge_Timestamp) AS Total_Mins_in_AE,
        CASE 
            WHEN DATEDIFF(MINUTE, Arrival_Timestamp, Discharge_Timestamp) > 240 THEN 1 ELSE 0 
        END AS Breached_4Hr_Flag,
        NULL AS WardID_at_DTA,
        NULL AS Ward_Occupancy_Percent_at_DTA
    FROM bronze.Patient_AE_Journeys
    WHERE PatientID IS NOT NULL AND Arrival_Timestamp IS NOT NULL
) AS source
ON target.PatientJourneyKey = source.PatientJourneyKey

-- Only insert new records
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        PatientJourneyKey, PatientID, Arrival_Timestamp, Triage_Timestamp, SeenByDoctor_Timestamp,
        DecisionToAdmit_Timestamp, WardAdmission_Timestamp, Discharge_Timestamp, Discharge_Reason, CurrentLocation,
        Mins_Arrival_to_Triage, Mins_Triage_to_Doctor, Mins_Doctor_to_DTA, Mins_DTA_to_Ward, Total_Mins_in_AE,
        Breached_4Hr_Flag, WardID_at_DTA, Ward_Occupancy_Percent_at_DTA
    )
    VALUES (
        source.PatientJourneyKey, source.PatientID, source.Arrival_Timestamp, source.Triage_Timestamp, source.SeenByDoctor_Timestamp,
        source.DecisionToAdmit_Timestamp, source.WardAdmission_Timestamp, source.Discharge_Timestamp, source.Discharge_Reason, source.CurrentLocation,
        source.Mins_Arrival_to_Triage, source.Mins_Triage_to_Doctor, source.Mins_Doctor_to_DTA, source.Mins_DTA_to_Ward, source.Total_Mins_in_AE,
        source.Breached_4Hr_Flag, source.WardID_at_DTA, source.Ward_Occupancy_Percent_at_DTA
    );

GO

-- Step 1: Truncate silver table
TRUNCATE TABLE silver.Clean_Ward_Occupancy;

-- Step 2 & 3: Insert only the latest snapshot
INSERT INTO silver.Clean_Ward_Occupancy (
    WardID, WardName, Specialty, TotalBeds, OccupiedBeds, OccupancyPercentage, LastUpdatedTimestamp
)
SELECT
    WardID,
    WardName,
    Specialty,
    TotalBeds,
    OccupiedBeds,
    CASE 
        WHEN TotalBeds > 0 THEN CAST(OccupiedBeds AS DECIMAL(10,2)) / TotalBeds * 100 
        ELSE 0 
    END AS OccupancyPercentage,
    LastUpdatedTimestamp
FROM bronze.Ward_Occupancy
WHERE LastUpdatedTimestamp = (
    SELECT MAX(LastUpdatedTimestamp) FROM bronze.Ward_Occupancy
)
AND WardID IS NOT NULL;
GO
