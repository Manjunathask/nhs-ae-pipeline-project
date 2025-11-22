--USE [NHS_A_E_Warehouse];
--GO

--================================================================================
-- STEP 1: Populate Ward Occupancy Snapshot
-- This must run FIRST, so we have the latest occupancy data to join to.
--================================================================================

-- Truncate the silver table to only hold the LATEST snapshot
TRUNCATE TABLE silver.Clean_Ward_Occupancy;

-- Insert the latest snapshot of ward data from Bronze
INSERT INTO silver.Clean_Ward_Occupancy (
    WardID, WardName, Specialty, TotalBeds, OccupiedBeds, OccupancyPercentage, LastUpdatedTimestamp
)
SELECT
    WardID,
    WardName,
    Specialty,
    TotalBeds,
    OccupiedBeds,
    -- Calculate Occupancy Percentage
    CASE 
        WHEN TotalBeds > 0 THEN CAST(OccupiedBeds AS DECIMAL(10,2)) / TotalBeds * 100 
        ELSE 0 
    END AS OccupancyPercentage,
    LastUpdatedTimestamp
FROM bronze.Ward_Occupancy
WHERE LastUpdatedTimestamp = (
    -- This subquery ensures we only get the most recent snapshot
    SELECT MAX(LastUpdatedTimestamp) FROM bronze.Ward_Occupancy
)
AND WardID IS NOT NULL;
--GO

--================================================================================
-- STEP 2: Upsert Patient Journeys AND Enrich
-- This runs SECOND. We can now join to the populated silver.Clean_Ward_Occupancy
--================================================================================

MERGE INTO silver.Clean_Patient_Journeys AS target
USING (
    SELECT
        -- Primary Key
        CONCAT(j.PatientID, '_', FORMAT(j.Arrival_Timestamp, 'yyyyMMddHHmmss')) AS PatientJourneyKey,
        
        -- Dimensions
        j.PatientID,
        j.Arrival_Timestamp,
        j.Triage_Timestamp,
        j.SeenByDoctor_Timestamp,
        j.DecisionToAdmit_Timestamp,
        j.WardAdmission_Timestamp,
        j.Discharge_Timestamp,
        j.Discharge_Reason,
        j.CurrentLocation,
        
        -- Calculated Measures
        DATEDIFF(MINUTE, j.Arrival_Timestamp, j.Triage_Timestamp) AS Mins_Arrival_to_Triage,
        DATEDIFF(MINUTE, j.Triage_Timestamp, j.SeenByDoctor_Timestamp) AS Mins_Triage_to_Doctor,
        DATEDIFF(MINUTE, j.SeenByDoctor_Timestamp, j.DecisionToAdmit_Timestamp) AS Mins_Doctor_to_DTA,
        DATEDIFF(MINUTE, j.DecisionToAdmit_Timestamp, j.WardAdmission_Timestamp) AS Mins_DTA_to_Ward,
        DATEDIFF(MINUTE, j.Arrival_Timestamp, j.Discharge_Timestamp) AS Total_Mins_in_AE,
        CASE 
            WHEN DATEDIFF(MINUTE, j.Arrival_Timestamp, j.Discharge_Timestamp) > 240 THEN 1 ELSE 0 
        END AS Breached_4Hr_Flag,

        -- *** THIS IS THE NEW ENRICHMENT LOGIC *** --
        
        -- 1. Parse the WardID from the raw 'CurrentLocation' text
        -- (e.g., 'Ward-4' becomes 'W4' to match the Ward table)
        CASE 
            WHEN j.Discharge_Reason = 'Admitted' THEN REPLACE(j.CurrentLocation, 'Ward-', 'W') 
            ELSE NULL 
        END AS WardID_at_DTA,

        -- 2. Look up the occupancy percentage from the table we populated in Step 1
        w.OccupancyPercentage AS Ward_Occupancy_Percent_at_DTA

    FROM 
        bronze.Patient_AE_Journeys AS j
    LEFT JOIN 
        -- Join to the silver ward table (populated in Step 1) to get occupancy
        silver.Clean_Ward_Occupancy AS w 
        -- We parse the key on-the-fly to match
        ON w.WardID = REPLACE(j.CurrentLocation, 'Ward-', 'W')
    WHERE 
        j.PatientID IS NOT NULL AND j.Arrival_Timestamp IS NOT NULL
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
--GO