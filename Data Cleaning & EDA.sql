# DATA CLEANING
# 1. Remove Duplicates
-- Check for duplicates
-- Creating a copy of table layoffs with a new column row_num so the original data 
-- remains unaltered
CREATE TABLE layoffs_copy (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO `world_layoffs`.layoffs_copy
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs;


SELECT *
FROM layoffs_copy;

# Remove duplicates
-- Check for duplicates
SELECT * FROM layoffs_copy
WHERE row_num > 1;


-- Delete duplicate rows
DELETE FROM layoffs_copy 
WHERE row_num > 1;


# 2. Standardize Data
-- Fixing data entry errors
-- Remove blank spaces from company name
SELECT company, TRIM(company)
FROM layoffs_copy;

UPDATE layoffs_copy 
SET company = TRIM(company);


SELECT *
FROM layoffs_copy
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_copy
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


SELECT DISTINCT country
FROM layoffs_copy
WHERE country LIKE 'United States%';

UPDATE layoffs_copy
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


-- Changing data type of column `date`
SELECT date,
	STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_copy;

--  Update the date format
UPDATE layoffs_copy
SET date = STR_TO_DATE(`date`, '%m/%d/%Y');


-- Change the `date` datatype from Text to Date
ALTER TABLE layoffs_copy
MODIFY COLUMN `date` DATE;


# 3. Handling NULL/Blank values
SELECT *
FROM layoffs_copy
WHERE industry IS NULL
	OR industry = '';
    
-- Updating ‘blank’ values as NULL
UPDATE layoffs_copy
SET industry = NULL
WHERE industry = '';


-- Doing a self join on the table to identify possible values to populate missing values
SELECT t1.industry, t2.industry
FROM layoffs_copy t1
JOIN layoffs_copy t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL 
	AND t2.industry IS NOT NULL;

-- Doing an update to populate the missing values in 'industry'
UPDATE layoffs_copy t1
JOIN layoffs_copy t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
	AND t2.industry IS NOT NULL;


# 4. Remove unwanted rows / columns
-- Since we will be doing an EDA on total_laid_off, rows where both total_laid_off and
-- percentage_laid_off are NULL will be of no help
-- Hence, deleting those rows where both the above column values are NULL
DELETE FROM layoffs_copy
WHERE total_laid_off IS NULL 
	AND percentage_laid_off IS NULL;
    

ALTER TABLE layoffs_copy
DROP COLUMN row_num;

    
# EXPLORATORY DATA ANALYSIS
-- Maximum number and percentage of laypffs by date
SELECT `date`, MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_copy
GROUP BY `date`
ORDER BY 2 DESC;


-- Exploring data with 100% layoffs
SELECT *
FROM layoffs_copy
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;


-- Ordering the companies by total layoffs
SELECT company, SUM(total_laid_off) 
FROM layoffs_copy
GROUP BY company
ORDER BY 2 DESC;


-- Date range of available data
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging_2;


-- Total and average layoffs per country by industry
SELECT country, industry, 
	SUM(total_laid_off) total_laidoff, 
    ROUND(AVG(percentage_laid_off), 2) avg_percentage_laid_off
FROM layoffs_copy
GROUP BY country, industry
ORDER BY 1, 2 DESC;


-- Calculate running total by month
WITH cte1 AS(
SELECT SUBSTRING(`date`, 1, 7) AS `month`, 
	SUM(total_laid_off) AS total_layoffs
	FROM layoffs_copy
	GROUP BY `month`
	HAVING `month` IS NOT NULL
	ORDER BY 1 ASC
)
SELECT *,
	SUM(total_layoffs) OVER(ORDER BY `month`) AS running_total
FROM cte1;


-- Ranking company by year based on total layoffs
WITH cte1 AS(
	SELECT company, 
		YEAR(`date`) AS `year`, 
        SUM(total_laid_off) AS running_total,
        DENSE_RANK() OVER(PARTITION BY YEAR(`date`) ORDER BY SUM(total_laid_off) DESC) AS ranking
	FROM layoffs_copy
	GROUP BY company, `year`
)
SELECT *
FROM cte1
WHERE `year` IS NOT NULL
	AND ranking <= 5;


