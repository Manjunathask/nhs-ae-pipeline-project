-- List all your CSV file names here (just the file names, not full paths)
DECLARE @Files TABLE (FileName NVARCHAR(255));
INSERT INTO @Files (FileName)
VALUES
    ('August-2025-CSV-g5tyh-v2.0.csv'),
    ('July-2025-CSV-9jfy4.csv'),
    ('Monthly-AE-April-2025.csv'),
    ('Monthly-AE-June-2025-1-1.csv'),
    ('Monthly-AE-May-2025.csv'),
    ('September-2025-Csv-7hjr2.csv');  -- Add or remove lines as needed

-- Folder path (update this once)
DECLARE @FolderPath NVARCHAR(500) = 
    'C:\Users\skman\Pictures\UK masters\Data Science\Projects\NHS\Data Source CSV\';

-- Variables for looping
DECLARE @FileName NVARCHAR(255);
DECLARE @FullPath NVARCHAR(1000);
DECLARE @SQL NVARCHAR(MAX);

DECLARE file_cursor CURSOR FOR
SELECT FileName FROM @Files;

OPEN file_cursor;
FETCH NEXT FROM file_cursor INTO @FileName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @FullPath = @FolderPath + @FileName;

    SET @SQL = '
        BULK INSERT bronze.External_Benchmarks
        FROM ''' + @FullPath + '''
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''\n'',
            TABLOCK
        );';

    PRINT 'Loading file: ' + @FileName; -- shows progress in Messages tab
    EXEC (@SQL);

    FETCH NEXT FROM file_cursor INTO @FileName;
END

CLOSE file_cursor;
DEALLOCATE file_cursor;

SELECT * FROM bronze.External_Benchmarks