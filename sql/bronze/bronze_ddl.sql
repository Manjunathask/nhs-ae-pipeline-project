USE [NHS_A_E_Warehouse];
GO

-- Table for raw patient journey data (from simulated EPR)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[Patient_AE_Journeys]') AND type in (N'U'))
BEGIN
    CREATE TABLE [bronze].[Patient_AE_Journeys](
        [PatientID] NVARCHAR(100) NOT NULL,
        [Arrival_Timestamp] DATETIME2(0) NULL,
        [Triage_Timestamp] DATETIME2(0) NULL,
        [SeenByDoctor_Timestamp] DATETIME2(0) NULL,
        [DecisionToAdmit_Timestamp] DATETIME2(0) NULL,
        [WardAdmission_Timestamp] DATETIME2(0) NULL,
        [Discharge_Timestamp] DATETIME2(0) NULL,
        [Discharge_Reason] NVARCHAR(100) NULL,
        [CurrentLocation] NVARCHAR(255) NULL,
        [IngestionTimestamp] DATETIME DEFAULT GETDATE()
    );
END
GO

-- Table for raw ward occupancy data (from simulated Bed Management)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[bronze].[Ward_Occupancy]') AND type in (N'U'))
BEGIN
    CREATE TABLE [bronze].[Ward_Occupancy](
        [WardID] NVARCHAR(50) NOT NULL,
        [WardName] NVARCHAR(100) NULL,
        [Specialty] NVARCHAR(100) NULL,
        [TotalBeds] INT NULL,
        [OccupiedBeds] INT NULL,
        [LastUpdatedTimestamp] DATETIME2(0) DEFAULT GETDATE()
    );
END
GO

-- Table for raw external benchmarks (from the 6 CSVs)
-- Drop the table if it exists
IF OBJECT_ID(N'[bronze].[External_Benchmarks]', 'U') IS NOT NULL
    DROP TABLE bronze.External_Benchmarks;
GO
-- Create the new table with updated columns
CREATE TABLE [bronze].[External_Benchmarks](
    [period] NVARCHAR(MAX) NULL,
    [org_code] NVARCHAR(MAX) NULL,
    [parent_org] NVARCHAR(MAX) NULL,
    [org_name] NVARCHAR(MAX) NULL,
    [aande_attendances_type_1] NVARCHAR(MAX) NULL,
    [aande_attendances_type_2] NVARCHAR(MAX) NULL,
    [aande_attendances_other_aande_department] NVARCHAR(MAX) NULL,
    [aande_attendances_booked_appointments_type_1] NVARCHAR(MAX) NULL,
    [aande_attendances_booked_appointments_type_2] NVARCHAR(MAX) NULL,
    [aande_attendances_booked_appointments_other_department] NVARCHAR(MAX) NULL,
    [attendances_over_4hrs_type_1] NVARCHAR(MAX) NULL,
    [attendances_over_4hrs_type_2] NVARCHAR(MAX) NULL,
    [attendances_over_4hrs_other_department] NVARCHAR(MAX) NULL,
    [attendances_over_4hrs_booked_appointments_type_1] NVARCHAR(MAX) NULL,
    [attendances_over_4hrs_booked_appointments_type_2] NVARCHAR(MAX) NULL,
    [attendances_over_4hrs_booked_appointments_other_department] NVARCHAR(MAX) NULL,
    [patients_who_have_waited_4_12_hs_from_dta_to_admission] NVARCHAR(MAX) NULL,
    [patients_who_have_waited_12+_hrs_from_dta_to_admission] NVARCHAR(MAX) NULL,
    [emergency_admissions_via_aande___type_1] NVARCHAR(MAX) NULL,
    [emergency_admissions_via_aande___type_2] NVARCHAR(MAX) NULL,
    [emergency_admissions_via_aande___other_aande_department] NVARCHAR(MAX) NULL,
    [other_emergency_admissions] NVARCHAR(MAX) NULL,
    );
GO
