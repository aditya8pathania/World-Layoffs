-- -------------------- Copy of Data to Prevent Faults -------------------

SELECT * FROM world_layoff.layoff_staging;

-- ------------------------ Removing Duplicates --------------------------

with cte as(
select *,row_number() over (PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,
`date`,stage,country,funds_raised_millions) as rn from world_layoff.layoff_staging
) 
select * from cte where rn > 1;

CREATE TABLE `layoffs_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_2
select *,row_number() over (PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,
`date`,stage,country,funds_raised_millions) as rn from world_layoff.layoff_staging;

SELECT * FROM world_layoff.layoffs_2
WHERE row_num > 1;

DELETE FROM world_layoff.layoffs_2
WHERE row_num > 1;

-- ------------------------------------------------------------------------------------------------
-- --------------------------------- Standardizing Data -------------------------------------------

UPDATE world_layoff.layoffs_2
SET company = TRIM(Company);

SELECT DISTINCT(industry)
FROM layoffs_2
ORDER BY 1;

SELECT * FROM layoffs_2
WHERE industry LIKE ('Crypto%');

UPDATE layoffs_2
SET industry = 'Crypto'
WHERE industry LIKE ('Crypto%');

SELECT COUNT(DISTINCT (industry)) from layoffs_2;

-- ------------------------------------------------------------------------------------------------

SELECT DISTINCT(COUNTRY) 
FROM layoffs_2
ORDER BY 1;

UPDATE layoffs_2
SET COUNTRY = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- -------------------------------------------------------------------------------------------------

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y') AS `Date`
FROM layoffs_2;

UPDATE layoffs_2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_2 
MODIFY COLUMN `date` DATE;

-- --------------------------------------------------------------------------------------------------
-- --------------------------------- Removing Null Values -------------------------------------------

SELECT * FROM layoffs_2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

UPDATE layoffs_2 t1
JOIN layoffs_2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- -----------------------------------------------------------------------------------------------------
-- ---------------------------- Rest We Delete the null values which are unessary ----------------------
