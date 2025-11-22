USE [NHS_A_E_Warehouse];
GO

CREATE OR ALTER PROCEDURE [gold].[sp_Populate_Dim_Date]
AS
BEGIN
    SET NOCOUNT ON;

    -- Check if the table is already populated.
    -- If it has rows, do nothing and exit the procedure.
    IF (SELECT COUNT(1) FROM [gold].[Dim_Date]) > 0
    BEGIN
        PRINT 'gold.Dim_Date is already populated. No action taken.';
        RETURN;
    END

    -- If the table IS empty, proceed with the one-time population.
    PRINT 'Populating gold.Dim_Date for the first time...';
    
    DECLARE @StartDate DATE = '2020-01-01';
    DECLARE @EndDate DATE = '2030-12-31';

    WHILE @StartDate <= @EndDate
    BEGIN
        INSERT INTO [gold].[Dim_Date] (
            [DateKey], [FullDate], [DayOfMonth], [DayName], [DayOfWeek],
            [MonthName], [MonthOfYear], [Quarter], [Year]
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
    END;
    PRINT 'gold.Dim_Date population complete.';
END
GO

CREATE OR ALTER PROCEDURE [gold].[sp_Populate_Dim_Time]
AS
BEGIN
    SET NOCOUNT ON;

    -- Check if the table is already populated.
    -- If it has rows, do nothing and exit the procedure.
    IF (SELECT COUNT(1) FROM [gold].[Dim_Time]) > 0
    BEGIN
        PRINT 'gold.Dim_Time is already populated. No action taken.';
        RETURN;
    END

    -- If the table IS empty, proceed with the one-time population.
    PRINT 'Populating gold.Dim_Time for the first time...';

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
                [TimeKey], [Hour], [Minute], [Time_of_Day_Band]
            )
            VALUES (
                @TimeKey, @Hour, @Minute, @Time_of_Day_Band
            );

            SET @Minute = @Minute + 1;
        END
        SET @Hour = @Hour + 1;
    END;
    PRINT 'gold.Dim_Time population complete.';
END
GO

-- You can still safely run these.
-- If the tables are full, they will just print a message and exit.
-- If you ever TRUNCATE (and drop/disable the constraints) manually,
-- these procedures will correctly repopulate the tables.
EXEC [gold].[sp_Populate_Dim_Date];
EXEC [gold].[sp_Populate_Dim_Time];
GO