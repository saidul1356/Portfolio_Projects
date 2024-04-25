-- Project: Data Cleaning in MySQL
SELECT * FROM layoffs;

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
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,  `date` , stage, country, funds_raised_millions -- Used backticks ` `, to call date column as date itself is also a keyword in MySql.
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
CREATE TABLE `layoffs_staging_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
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
  PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,  `date` , stage, country, funds_raised_millions -- Used backticks ` `, to call date column as date itself is also a keyword in MySql.
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

-- Update the company column in the layoffs_staging_2 table with the trimmed company names.
UPDATE layoffs_staging_2
SET company = TRIM(company);

-- 2.2 Standardize the Industry Names (Crypto):
-- Select all rows where the industry column starts with 'Crypto'.
SELECT *
FROM layoffs_staging_2
WHERE industry LIKE 'Crypto%';

-- Update the industry column to 'Crypto' for rows where the industry starts with 'Crypto'.
UPDATE layoffs_staging_2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- 2.3 Standardize the Country Names (United States):
-- Select distinct country names along with their trailing periods removed to ensure consistency.
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging_2;

-- Update the country column in the layoffs_staging_2 table with the standardized country names.
-- Trailing periods are removed from country names where they start with 'United States'.
UPDATE layoffs_staging_2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- 2.4 Standardize the Date Format:
-- Select the date column along with its values converted to the '%m/%d/%Y' format.
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging_2;

-- Update the date column in the layoffs_staging_2 table with values converted to the '%m/%d/%Y' format.
UPDATE layoffs_staging_2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Alter the data type of the date column in the layoffs_staging_2 table to DATE for consistency.
ALTER TABLE layoffs_staging_2
MODIFY COLUMN `date` DATE;

-- 3. Null Values Or Blank Values
-- 3.1 Identify rows with NULL values in total_laid_off and percentage_laid_off columns
SELECT *
FROM layoffs_staging_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Update industry column to NULL where it has blank values
UPDATE layoffs_staging_2
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

-- 3.3 Update missing industry information by matching with records from the same company
UPDATE layoffs_staging_2 ls1
JOIN layoffs_staging_2 ls2
ON ls1.company = ls2.company
SET ls1.industry = ls2.industry
WHERE (ls1.industry IS NULL OR ls1.industry = '')
AND ls2.industry IS NOT NULL;

-- **Summary 3.**
-- NULL values in total_laid_off and percentage_laid_off columns have been identified and handled.
-- Blank values in the industry column have been updated to NULL.
-- Records for the company 'Airbnb' have been displayed.
-- Records with blank or NULL values in the industry column have been identified.
-- Missing industry information for records with the same company has been updated based on available data.


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