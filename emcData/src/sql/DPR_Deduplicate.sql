-- Step 1: Identify the columns that define a duplicate row
-- Assuming columns that define duplicate rows are col1, col2, col3, col4, and col5
-- Adjust the column names accordingly based on your schema

-- Step 2: Write a query to select distinct rows based on those columns. 
--         Create a new table to hold the deduplicated data.
DROP TABLE IF EXISTS public."RealTimeDPR";

CREATE TABLE public."RealTimeDPR" AS
SELECT DISTINCT ON ("Date", "Period") *
FROM emcdata."RealTimeDPR"
ORDER BY "Date", "Period";

-- Step 3: Drop the original table
DROP TABLE emcdata."RealTimeDPR";

-- Step 4: Rename the new table to match the original name
ALTER TABLE emcdata."RealTimeDPR" RENAME TO "RealTimeDPR";

-- Step 7: Create an index on the reordered columns for better performance, if necessary
-- Example:
-- CREATE INDEX idx_reorder ON RealTimeLar (Date, Period, ForecastDate, ForecastPeriod, LoadScenario);
