USE [NHS_A_E_Warehouse];
GO

-- Table for cleaned, transformed, and enriched patient journeys
-- Note: Table is created in the [silver] schema
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[silver].[Clean_Patient_Journeys]') AND type in (N'U'))
BEGIN
    CREATE TABLE [silver].[Clean_Patient_Journeys](
        [PatientJourneyKey] NVARCHAR(100) PRIMARY KEY, -- A unique key, e.g., PatientID + Arrival_Timestamp
        [PatientID] NVARCHAR(100) NOT NULL,
        [Arrival_Timestamp] DATETIME2(0) NULL,
        [Triage_Timestamp] DATETIME2(0) NULL,
        [SeenByDoctor_Timestamp] DATETIME2(0) NULL,
        [DecisionToAdmit_Timestamp] DATETIME2(0) NULL,
        [WardAdmission_Timestamp] DATETIME2(0) NULL,
        [Discharge_Timestamp] DATETIME2(0) NULL,
        [Discharge_Reason] NVARCHAR(100) NULL,
        [CurrentLocation] NVARCHAR(255) NULL,

        -- Calculated Duration Columns (in Minutes)
        [Mins_Arrival_to_Triage] INT NULL,
        [Mins_Triage_to_Doctor] INT NULL,
        [Mins_Doctor_to_DTA] INT NULL,
        [Mins_DTA_to_Ward] INT NULL,
        [Total_Mins_in_AE] INT NULL,

        -- Calculated Flag
        [Breached_4Hr_Flag] BIT NULL,

        -- Enriched Data
        [WardID_at_DTA] NVARCHAR(50) NULL, -- The target ward
        [Ward_Occupancy_Percent_at_DTA] DECIMAL(5, 2) NULL
    );
END
GO

-- Table for cleaned ward occupancy
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[silver].[Clean_Ward_Occupancy]') AND type in (N'U'))
BEGIN
    CREATE TABLE [silver].[Clean_Ward_Occupancy](
        [WardID] NVARCHAR(50) PRIMARY KEY,
        [WardName] NVARCHAR(100) NULL,
        [Specialty] NVARCHAR(100) NULL,
        [TotalBeds] INT NULL,
        [OccupiedBeds] INT NULL,
        [OccupancyPercentage] DECIMAL(5, 2) NULL, -- Calculated: (OccupiedBeds / TotalBeds) * 100
        [LastUpdatedTimestamp] DATETIME2(0) NULL
    );
END
GO