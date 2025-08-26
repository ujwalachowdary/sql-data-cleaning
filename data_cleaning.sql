select  *
from layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;
WITH duplicate_cte as 
(
select  *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions ) AS row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num >1;
select *
from layoffs_staging
where company = 'AllyO';











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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

INSERT INTO layoffs_staging2
select  *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions ) AS row_num
from layoffs_staging;

select *
from layoffs_staging2
WHERE row_num >1;
SET SQL_SAFE_UPDATES =0;
delete
from layoffs_staging2
WHERE row_num >1;


-- STANDARDIZING DATA

-- look in the company there are some unwanted spaces are there 
select *
from layoffs_staging2;


select company ,TRIM(company)
from layoffs_staging2;

-- updating

update layoffs_staging2
SET company = TRIM(company);

select  distinct industry
from layoffs_staging2
ORDER BY 1;

select *
from layoffs_staging2
where industry like 'crypto%';

-- here we updated all the crypto% to crypto in industry column

update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';

select distinct industry
from layoffs_staging2;
-- we will clean the country column
select distinct country
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where country like 'united states%'
order by 1;

select distinct country ,TRIM(TRAILING '.' from country )
from layoffs_staging2
order by 1;

update layoffs_staging2
set country =TRIM(TRAILING '.' from country )
where country like  'united states%';


-- changing string to date   in date column

--- all the dates  are in the text form

select `date`
from layoffs_staging2;

select `date` ,str_to_date(`date` ,'%m/%d/%Y')
from layoffs_staging2;

-- updating 
update layoffs_staging2
set `date` =str_to_date(`date` ,'%m/%d/%Y');


-- now we can convert the data type of date

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


SELECT *
from layoffs_staging2;

-- lets checks the nulls in the total_laid_off and percentage_laid_off
SELECT *
from layoffs_staging2
WHERE total_laid_off is null
AND percentage_laid_off IS NULL
;


-- there are some null values in industry column to

SELECT *
from layoffs_staging2
WHERE industry is null
or  industry = '';
-- here there are some null values  in airbnb 
SELECT *
from layoffs_staging2
where company = 'Airbnb';

-- the Airbnb is a travel industry so we try to update   the another airbub into  travel industry
--  we can use joins  
-- here its shows the both industry  t1 is null and t2 has industry  that has no null so we are populated into t2 to t1
select t1.industry ,t2.industry
from layoffs_staging2 t1 
JOIN layoffs_staging2 t2
   on t1.company = t2.company
where( t1.industry is null or t1.industry ='')
and t2.industry is not null;

-- here we will change the  blanks to null
update layoffs_staging2
set industry = null
where industry = '';

-- now we will update 

update layoffs_staging2 t1 
JOIN layoffs_staging2 t2
  on t1.company = t2.company
SET t1.industry = t2.industry
where( t1.industry is null or t1.industry ='')
and t2.industry is not null;
  
  
-- lets take a  look at null values
-- so in both total_laid_off and percenatge_laid_off  there are null values if we want  we can delete them
select *  
from layoffs_staging2;

select *  
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

 -- delete those  useless dat awe dont use
delete 
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

-- now lastly we can drop the column row_num
alter table layoffs_staging2
drop column row_num;


 -- now lets check with every thing
 select * 
 from layoffs_staging2;
