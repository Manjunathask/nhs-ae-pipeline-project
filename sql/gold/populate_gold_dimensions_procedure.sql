USE [NHS_A_E_Warehouse];
GO

CREATE OR ALTER PROCEDURE [gold].[sp_Populate_Dim_Date]
AS
BEGIN
    -- Populate gold.Dim_Date
    -- This script populates Dim_Date for 10 years (2020-2030).
    TRUNCATE TABLE [gold].[Dim_Date];
    
    DECLARE @StartDate DATE = '2020-01-01';
    DECLARE @EndDate DATE = '2030-12-31';

    WHILE @StartDate <= @EndDate
    BEGIN
        INSERT INTO [gold].[Dim_Date] (
            [DateKey],
            [FullDate],
            [DayOfMonth],
            [DayName],
            [DayOfWeek],
            [MonthName],
            [MonthOfYear],
            [Quarter],
            [Year]
        )
        VALUES (
            CONVERT(INT, FORMAT(@StartDate, 'yyyyMMdd')),
            @StartDate,
            DATEPART(DAY, @StartDate),
            DATENAME(WEEKDAY, @StartDate),
            DATEPART(WEEKDAY, @StartDate),
            DATENAME(MONTH, @StartDate),
            DATEPART(MONTH, @StartDate),
            DATEPART(QUARTER, @StartDate),
            DATEPART(YEAR, @StartDate)
        );

        SET @StartDate = DATEADD(DAY, 1, @StartDate);
    END
END
GO

CREATE OR ALTER PROCEDURE [gold].[sp_Populate_Dim_Time]
AS
BEGIN
    -- Populate gold.Dim_Time
    -- This script populates Dim_Time for every minute of the day.
    TRUNCATE TABLE [gold].[Dim_Time];

    DECLARE @Hour INT = 0;
    DECLARE @Minute INT = 0;
    DECLARE @TimeKey INT;
    DECLARE @Time_of_Day_Band NVARCHAR(50);

    WHILE @Hour < 24
    BEGIN
        SET @Minute = 0;
        WHILE @Minute < 60
        BEGIN
            SET @TimeKey = (@Hour * 100) + @Minute;

            SET @Time_of_Day_Band = CASE
                WHEN @Hour BETWEEN 6 AND 11 THEN 'Morning'
                WHEN @Hour BETWEEN 12 AND 17 THEN 'Afternoon'
                WHEN @Hour BETWEEN 18 AND 21 THEN 'Evening'
                ELSE 'Night'
            END;

            INSERT INTO [gold].[Dim_Time] (
                [TimeKey],
                [Hour],
                [Minute],
                [Time_of_Day_Band]
            )
            VALUES (
                @TimeKey,
                @Hour,
                @Minute,
                @Time_of_Day_Band
            );

            SET @Minute = @Minute + 1;
        END
        SET @Hour = @Hour + 1;
    END
END
GO

-- Execute the procedures to populate the dimensions
EXEC [gold].[sp_Populate_Dim_Date];
EXEC [gold].[sp_Populate_Dim_Time];
GO
