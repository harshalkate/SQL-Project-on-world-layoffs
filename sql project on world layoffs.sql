-- Using the database world_layoffs
use word_layoff;

-- selecting all for the table viewing
SELECT 
    *
FROM
    layoffs_staging;


-- Q1 Identifying the duplicates values in the data
with duplicate_cte as
(
select *,
row_number() over( 
partition by company, location, industry, total_laid_off, 
percentage_laid_off, 'date', stage, 
country, funds_raised_millions) as row_num
from layoffs_staging
)
select * 
from duplicate_cte
where row_num > 1;

-- Q2 selecting the data of the company Casper and identify the duplicates
SELECT 
    *
FROM
    layoffs_staging
WHERE
    company = 'Casper';


-- Q3 created the copy of the table layoffs as `layoffs_staging2`.
CREATE TABLE `layoffs_staging2` (
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

-- viewing the copy table 
SELECT 
    *
FROM
    layoffs_staging2;

-- inserting the data into the copy table 

insert into layoffs_staging2
select *,
row_number() over( 
partition by company, location, industry, 
total_laid_off, percentage_laid_off,'date', 
stage, country, funds_raised_millions) as row_num
from layoffs_staging;

SET SQL_SAFE_UPDATES = 0;

-- Q4 deleting the values from the rows where greater then 1  
DELETE FROM layoffs_staging2 
WHERE
    row_num > 1;

-- viewing the updated table
SELECT 
    *
FROM
    layoffs_staging2;

-- Q5 Triming the space and gaps between in the columns and the rows
SELECT 
    company, TRIM(company)
FROM
    layoffs_staging2;

-- update the changes in the company column.
UPDATE layoffs_staging2 
SET 
    company = TRIM(company);

-- Q6 selecting the  distinct values from the 
-- column Industry from layoffs_staging2.
SELECT DISTINCT
    industry
FROM
    layoffs_staging2
ORDER BY 1;

-- Updating the column industry and setting it to crypto
UPDATE layoffs_staging2 
SET 
    industry = 'Crypto'
WHERE
    industry LIKE 'Crypto%';
    
select * 
industry
from layoffs_staging2;

-- Q7 selecting the  distinct values from the column Location from layoffs_staging2.
SELECT DISTINCT
    location
FROM
    layoffs_staging2
ORDER BY 1;

-- Q8 selecting the  distinct values from the column country from layoffs_staging2.
SELECT DISTINCT
    country
FROM
    layoffs_staging2
ORDER BY 1;

-- Q9 identifying the distinct values and trimming the exact column.
SELECT DISTINCT
    country, TRIM(TRAILING '.' FROM country)
FROM
    layoffs_staging2
ORDER BY 1;

-- updating and triming the country column 
-- and the removing the special characters from it.
UPDATE layoffs_staging2 
SET 
    country = TRIM(TRAILING '.' FROM country)
WHERE
    country LIKE 'United States%';


-- Q10 Converting the format of the date column for better results.
SELECT 
    `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM
    layoffs_staging2;

-- and updating it accordingyly.
UPDATE layoffs_staging2 
SET 
    `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Modifying the column date.
SELECT 
    *
FROM
    layoffs_staging2;

alter table layoffs_staging2
modify column `date` date;

-- Q11 Identifying  the null values which are present in the 
-- total_laid_off and percentage_laid_off column.
SELECT 
    *
FROM
    layoffs_staging2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

-- Q12 Identifying the null values which are present in the industry column.
SELECT 
    *
FROM
    layoffs_staging2
WHERE
    industry IS NULL OR industry = '';


-- Q13 viewing the company column where company is equal to the Airbnb.
SELECT 
    *
FROM
    layoffs_staging2
WHERE
    company = 'Airbnb';

-- Q14 Joining the cloumn industry from the table itself by using only join.
SELECT 
    t1.industry, t2.industry
FROM
    layoffs_staging2 t1
        JOIN
    layoffs_staging2 t2 ON t1.company = t2.company
WHERE
    (t1.industry IS NULL OR t1.industry = '')
        AND t2.industry IS NOT NULL;    

-- selecting where the company contians bally word in it .
SELECT 
    *
FROM
    layoffs_staging2
WHERE
    company LIKE 'Bally%';

-- viewing the total null values in the column total_laid_off and percentage_laid_off.
SELECT 
    *
FROM
    layoffs_staging2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

-- deleting the null values which are present in the table
DELETE FROM layoffs_staging2 
WHERE
    total_laid_off IS NULL
    AND percentage_laid_off IS NULL;

-- selecting everything from the table 
SELECT 
    *
FROM
    layoffs_staging2

-- Alter the table layoffs_staging2 and the dropped the column row_num.
alter table 
layoffs_staging2
drop column row_num;


-- EDA of the World layoffs.

select max(total_laid_off),max(percentage_laid_off)
from layoffs_staging2;

-- selecting the percentage lay off above 1 percentile with the order.
select * 
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

-- Total laid off by the company accordingly.
SELECT 
    company, SUM(total_laid_off)
FROM
    layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- date of Layoff started by the companies 
SELECT 
    MIN(`date`), MAX(`date`)
FROM
    layoffs_staging2;


-- Total laid off by the country
SELECT 
    country, SUM(total_laid_off)
FROM
    layoffs_staging2
GROUP BY country
ORDER BY country DESC;

-- Total laid offs by the years and sum of it and in sequence.
SELECT 
    YEAR(`date`), SUM(total_laid_off)
FROM
    layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- sum of the total laid off by the stage.
SELECT 
    stage, SUM(total_laid_off)
FROM
    layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Average of the percentage_laid_off by country
SELECT 
    country, AVG(percentage_laid_off)
FROM
    layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Laid off according to the months using substring
SELECT 
    SUBSTRING(`date`, 1, 7) AS Months, SUM(total_laid_off)
FROM
    layoffs_staging2
WHERE
    SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY Months
ORDER BY 1 ASC;


-- rolling total of totatotal_laid_off by year and months.
with Rolling_total as
(
select substring(`date`,1,7) as `Month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `Month`
order by 1 asc
)
select `Month`,total_off
,sum(total_off) over(order by `Month`) as Rolling_total
from Rolling_total;	

-- Total laid off by the company
SELECT 
    company, SUM(total_laid_off)
FROM
    layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


-- Total laid off by the company with the years and rankings
with Company_Year (company,years,total_laid_off) as
(
select company, year(`date`),sum(total_laid_off)
from layoffs_staging2
group by company,year(`date`)
), Company_rank as
(select *, dense_rank() over(partition by years order by total_laid_off desc) as Ranking
from company_year
where years is not null
)
select * from 
Company_rank
where ranking <=5;

-- Total layoffs by the year 2022,23
SELECT 
    YEAR(`date`) AS year, COUNT(*) AS total_layoffs
FROM
    layoffs_staging2
WHERE
    YEAR(`date`) BETWEEN 2022 AND 2023
GROUP BY YEAR(`date`)
ORDER BY year;

select *
from layoffs_staging2