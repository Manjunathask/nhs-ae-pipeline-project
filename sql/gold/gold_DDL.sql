USE [NHS_A_E_Warehouse];
GO

-- Dimension Table for Date
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[gold].[Dim_Date]') AND type in (N'U'))
BEGIN
    CREATE TABLE [gold].[Dim_Date](
        [DateKey] INT PRIMARY KEY,
        [FullDate] DATE NOT NULL,
        [DayOfMonth] INT NOT NULL,
        [DayName] NVARCHAR(20) NOT NULL,
        [DayOfWeek] INT NOT NULL,
        [MonthName] NVARCHAR(20) NOT NULL,
        [MonthOfYear] INT NOT NULL,
        [Quarter] INT NOT NULL,
        [Year] INT NOT NULL
    );
END
GO

-- Dimension Table for Time (of day)
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[gold].[Dim_Time]') AND type in (N'U'))
BEGIN
    CREATE TABLE [gold].[Dim_Time](
        [TimeKey] INT PRIMARY KEY, -- Format: HHMM (e.g., 1430)
        [Hour] INT NOT NULL,
        [Minute] INT NOT NULL,
        [Time_of_Day_Band] NVARCHAR(50) NOT NULL -- e.g., 'Morning', 'Afternoon', 'Evening', 'Night'
    );
END
GO

-- Dimension Table for Wards
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[gold].[Dim_Ward]') AND type in (N'U'))
BEGIN
    CREATE TABLE [gold].[Dim_Ward](
        [WardKey] INT IDENTITY(1,1) PRIMARY KEY,
        [WardID] NVARCHAR(50) NOT NULL,
        [WardName] NVARCHAR(100) NULL,
        [Specialty] NVARCHAR(100) NULL,
        [TotalBeds] INT NULL
        -- Sourced from silver.Clean_Ward_Occupancy
    );
END
GO

-- Fact Table for Patient Journeys
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[gold].[Fact_PatientJourney]') AND type in (N'U'))
BEGIN
    CREATE TABLE [gold].[Fact_PatientJourney](
        [PatientJourneyKey] NVARCHAR(100) PRIMARY KEY, -- Sourced from Silver
        
        -- Foreign Keys to Dimensions
        [Arrival_DateKey] INT NOT NULL,
        [Arrival_TimeKey] INT NOT NULL,
        [DTA_DateKey] INT NULL,
        [DTA_TimeKey] INT NULL,
        [Discharge_DateKey] INT NULL,
        [Discharge_TimeKey] INT NULL,
        [WardKey] INT NOT NULL,

        -- Measures (from Silver)
        [Mins_Arrival_to_Triage] INT NULL,
        [MMins_Triage_to_Doctor] INT NULL,
        [Mins_Doctor_to_DTA] INT NULL,
        [Mins_DTA_to_Ward] INT NULL,
        [Total_Mins_in_AE] INT NULL,
        [Breached_4Hr_Flag] BIT NULL,
        [Ward_Occupancy_Percent_at_DTA] DECIMAL(5, 2) NULL
        
        -- Add Foreign Key Constraints
        ,CONSTRAINT [FK_Fact_PatientJourney_Dim_Date_Arrival] FOREIGN KEY ([Arrival_DateKey]) REFERENCES [gold].[Dim_Date] ([DateKey])
        ,CONSTRAINT [FK_Fact_PatientJourney_Dim_Time_Arrival] FOREIGN KEY ([Arrival_TimeKey]) REFERENCES [gold].[Dim_Time] ([TimeKey])
        ,CONSTRAINT [FK_Fact_PatientJourney_Dim_Ward] FOREIGN KEY ([WardKey]) REFERENCES [gold].[Dim_Ward] ([WardKey])
    );
END
GO
