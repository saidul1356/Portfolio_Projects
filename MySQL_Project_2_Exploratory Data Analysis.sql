-- Project_1: Data Cleaning in MySQL
SELECT * FROM layoffs_raw;

-- 1. Remove Duplicates
-- 1.1 Finding Duplicates

-- Create a staging table 'layoffs_staging' as a copy of 'layoffs_raw'
-- This table will be used to identify and remove duplicates

DROP TABLE IF EXISTS layoffs_staging;

CREATE TABLE layoffs_staging
LIKE layoffs_raw;

SELECT * 
  FROM layoffs_staging;

-- Copy data from 'layoffs_raw' into the staging table 'layoffs_staging'
INSERT layoffs_staging
SELECT * 
  FROM layoffs_raw;

-- Use a CTE to identify duplicate rows in the staging table
-- The ROW_NUMBER() function assigns a unique integer to each row within a partition
WITH duplicate_cte AS 
(
  SELECT *,
  ROW_NUMBER () OVER (
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,  ``date`` , stage, country, funds_raised_millions -- Used backticks ` `, to call `date` column as `date` itself is also a keyword in MySql.
					 ) AS row_num
   FROM layoffs_staging
)

-- Select rows from the CTE where row_num > 1 (indicating duplicates)
SELECT *
  FROM duplicate_cte
 WHERE row_num > 1;
 
 -- View data for a specific company (example: 'Casper') in the staging table
SELECT * 
FROM layoffs_staging
WHERE company = 'Casper';

-- **1.2 Deleting Duplicates**
-- Create a new table 'layoffs_staging_2' to store deduplicated data
DROP TABLE IF EXISTS layoffs_staging_2;
CREATE TABLE `layoffs_staging_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  ``date`` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT -- Additional column for identifying duplicates.
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging_2;

-- Copy data from the CTE (including row numbers) into the new table 'layoffs_staging_2'
INSERT INTO layoffs_staging_2

  SELECT *,
  ROW_NUMBER () OVER (
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,  ``date`` , stage, country, funds_raised_millions -- Used backticks ` `, to call `date` column as `date` itself is also a keyword in MySql.
					 ) AS row_num
   FROM layoffs_staging;

-- View the contents of layoffs_staging_2 table with row numbers
SELECT *
FROM layoffs_staging_2
WHERE row_num > 1; -- Display only duplicate rows (row_num > 1)

-- Delete duplicate rows from layoffs_staging_2 table
-- Rows with row_num > 1 represent duplicates, keeping only the first occurrence of each group.
DELETE
FROM layoffs_staging_2
WHERE row_num > 1;

-- **Summary 1.**
-- Data has been inserted into 'layoffs_staging_2' table with row numbers assigned
-- The ROW_NUMBER() function assigns a unique integer to each row within a partition, helping to identify and remove duplicate rows later.
 
 
 
-- **2. Standardize the Data**
-- 2.1 Standardize the Company Names:
-- Select the company column along with its trimmed version to remove leading and trailing whitespace.
SELECT company, TRIM(company)
FROM layoffs_staging_2;

-- Up`date` the company column in the layoffs_staging_2 table with the trimmed company names.
UP`date` layoffs_staging_2
SET company = TRIM(company);

-- 2.2 Standardize the Industry Names (Crypto):
-- Select all rows where the industry column starts with 'Crypto'.
SELECT *
FROM layoffs_staging_2
WHERE industry LIKE 'Crypto%';

-- Up`date` the industry column to 'Crypto' for rows where the industry starts with 'Crypto'.
UP`date` layoffs_staging_2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- 2.3 Standardize the Country Names (United States):
-- Select distinct country names along with their trailing periods removed to ensure consistency.
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging_2;

-- Up`date` the country column in the layoffs_staging_2 table with the standardized country names.
-- Trailing periods are removed from country names where they start with 'United States'.
UP`date` layoffs_staging_2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- 2.4 Standardize the `date` Format:
-- Select the `date` column along with its values converted to the '%m/%d/%Y' format.
SELECT ``date``,
STR_TO_`date`(``date``, '%m/%d/%Y')
FROM layoffs_staging_2;

-- Up`date` the `date` column in the layoffs_staging_2 table with values converted to the '%m/%d/%Y' format.
UP`date` layoffs_staging_2
SET ``date`` = STR_TO_`date`(``date``, '%m/%d/%Y');

-- Alter the data type of the `date` column in the layoffs_staging_2 table to `date` for consistency.
ALTER TABLE layoffs_staging_2
MODIFY COLUMN ``date`` `date`;

-- 3. Null Values Or Blank Values
-- 3.1 Identify rows with NULL values in total_laid_off and percentage_laid_off columns
SELECT *
FROM layoffs_staging_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Up`date` industry column to NULL where it has blank values
UP`date` layoffs_staging_2
SET industry = NULL
WHERE industry = '';

-- View all records for the company 'Airbnb'
SELECT *
FROM layoffs_staging_2
WHERE company = 'Airbnb';

-- 3.2 Identify rows with blank or NULL values in the industry column
SELECT *
FROM layoffs_staging_2
WHERE industry = ''
OR industry IS NULL;

-- Join the table with itself to find records with missing industry information but with the same company
SELECT *
FROM layoffs_staging_2 ls1
JOIN layoffs_staging_2 ls2
ON ls1.company = ls2.company
WHERE (ls1.industry IS NULL OR ls1.industry = '')
AND ls2.industry IS NOT NULL;

-- 3.3 Up`date` missing industry information by matching with records from the same company
UP`date` layoffs_staging_2 ls1
JOIN layoffs_staging_2 ls2
ON ls1.company = ls2.company
SET ls1.industry = ls2.industry
WHERE (ls1.industry IS NULL OR ls1.industry = '')
AND ls2.industry IS NOT NULL;

-- **Summary 3.**
-- NULL values in total_laid_off and percentage_laid_off columns have been identified and handled.
-- Blank values in the industry column have been up`date`d to NULL.
-- Records for the company 'Airbnb' have been displayed.
-- Records with blank or NULL values in the industry column have been identified.
-- Missing industry information for records with the same company has been up`date`d based on available data.


-- 4. Remove Any Unnecessary Rows And Columns
-- 4.1 Selecting rows where both total_laid_off and percentage_laid_off are NULL
SELECT *
FROM layoffs_staging_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Deleting rows where both total_laid_off and percentage_laid_off are NULL
DELETE
FROM layoffs_staging_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- 4.2 Dropping the column row_num as it's unnecessary
ALTER TABLE layoffs_staging_2
DROP COLUMN row_num;

-- **Summary 4.**
-- Rows with missing values for both total_laid_off and percentage_laid_off columns have been removed.
-- The row_num column has been dropped as it's unnecessary for further analysis.

-- Project_2_Exploratory Data Analysis: Data Cleaning in MySQL
-- Exploring trends, patterns, and outliers in layoff data

-- Selecting all data from the 'layoffs_staging_2' table to start the analysis
SELECT * 
FROM world_layoffs.layoffs_staging_2;

-- Finding the maximum total laid off to understand the extent of layoffs
SELECT MAX(total_laid_off)
FROM world_layoffs.layoffs_staging_2;

-- Examining the maximum percentage laid off to assess the severity of layoffs
SELECT MAX(percentage_laid_off),  MAX(percentage_laid_off)
FROM world_layoffs.layoffs_staging_2
WHERE  percentage_laid_off IS NOT NULL;

-- Identifying companies where 100% of employees were laid off, possibly indicating business closures
SELECT *
FROM world_layoffs.layoffs_staging_2
WHERE  percentage_laid_off = 1;

-- Further examining companies with 100% layoffs by their funds raised, indicating company size
SELECT *
FROM world_layoffs.layoffs_staging_2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Aggregating layoffs by country and industry to identify heavily affected regions and sectors
SELECT country, industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging_2
GROUP BY country, industry
ORDER BY country ASC, SUM(total_laid_off) DESC;

-- Identifying companies with the largest single-day layoffs
SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY 2 DESC
LIMIT 5;

-- Identifying companies with the most total layoffs over time
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging_2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- Analyzing layoffs by location to understand geographical trends
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging_2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- Calculating total layoffs by country to understand global impact
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging_2
GROUP BY country
ORDER BY 2 DESC;

-- Analyzing layoffs over time by year to identify trends
SELECT YEAR(`date`), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging_2
GROUP BY YEAR(`date`)
ORDER BY 1 ASC;

-- Analyzing layoffs by industry to understand sector-specific impact
SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging_2
GROUP BY industry
ORDER BY 2 DESC;

-- Analyzing layoffs by company stage to understand impact across different company sizes
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging_2
GROUP BY stage
ORDER BY 2 DESC;

-- Identifying top companies with the most layoffs each year
WITH Company_Year AS (
  SELECT company, YEAR(`date`) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging_2
  GROUP BY company, YEAR(`date`)
)
, 
Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 5
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- Calculating the rolling total of layoffs per month
-- to visualize layoffs progression over time
WITH date_CTE AS (
SELECT SUBSTRING(`date`,1,7) as `date`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging_2
GROUP BY `date`
ORDER BY `date` ASC
)
SELECT `date`, SUM(total_laid_off) OVER (ORDER BY `date` ASC) as rolling_total_layoffs
FROM date_CTE
ORDER BY `date` ASC;

