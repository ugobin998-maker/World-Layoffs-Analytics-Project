	-- SQL Project - Data Cleaning
    
    
    -- Getting data ready for cleaning. 
    -- First thing to do is create a staging table for layoffs_table where we will clean the data
    -- We want to keep a table with the raw data in case something happens

use layoffs_database;
select * from layoffs_table; 

create table layoffs_staging
like layoffs_table;           

insert into layoffs_staging
select * from layoffs_table;  

select * from layoffs_staging;


	 -- Now, we follow these steps for data cleaning:
     -- 1. Check for duplicates and remove them
     -- 2. Standardize our data and fix errors
     -- 3. Look for null values and see how we can handle them
    
-- 1. Remove Duplicates
  
select *,
row_number() over(partition by company, 
location, 
industry, 
total_laid_off, 
percentage_laid_off, 
date, 
stage,  
country, 
funds_raised_millions) as row_num
from layoffs_staging;                -- Creating a column to number the rows, partitioned by each column

with duplicate_cte as
(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage,  country, funds_raised_millions) as row_num
from layoffs_staging
) -- Creating a CTE to insert the above query(lines 18-28)

select * from duplicate_cte -- Identifying duplicates only
where row_num > 1; -- Cannot perform an update/delete operation from a CTE, must create another table for this purpose

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; -- Creating a separate table with row_number column to remove duplicates

insert into layoffs_staging2
select *,
row_number() over(partition by company, 
location, 
industry, 
total_laid_off, 
percentage_laid_off, 
date, stage,  
country, 
funds_raised_millions) as row_num
from layoffs_staging;             -- Inserting above query(lines 18-28) to new table created

delete from layoffs_staging2
where row_num > 1; -- Permanently removing duplicates from new table created

alter table layoffs_staging2
drop column row_num; -- Dropping row_num column since there is no further use


  -- 2. Standardizing Data
        
        
select * from layoffs_staging2;
	
select distinct company, trim(company) from layoffs_staging2;
update layoffs_staging2 
set company = trim(company); -- Removing unnecessary spaces in cells

select distinct industry 
from layoffs_staging2;

select * from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Cryptocurrency'
where industry like 'Crypto%';  -- Grouping all crypto-related categories into the same industry

select date, str_to_date(date, '%m/%d/%Y') from layoffs_staging2; 

update layoffs_staging2
set date = str_to_date(date, '%m/%d/%Y');

alter table layoffs_staging2
modify column date date;-- Converting date column format from string to date

select distinct country from layoffs_staging2;

update layoffs_staging2
set country = 'United States'
where country like 'United States%'; -- Grouping all countries similar to 'United States' in the same category


  -- Handling blank/null values


select * from layoffs_staging2;

select * 
from layoffs_staging2
where industry is null; -- Null Values

select * 
from layoffs_staging2
where industry = '';   -- Blank Cells

select * 
from layoffs_staging2
where company = 'Airbnb';  -- Finding out what if the industry of 'Airbnb' is available

update layoffs_staging2
set industry = 'Travel'
where company = 'Airbnb';   -- Replacing blank cell with 'Travel' as the industry of 'Airbnb' is available

select * 
from layoffs_staging2
where company = 'Juul';

update layoffs_staging2
set industry = 'Consumer'
where company = 'Juul';

select * 
from layoffs_staging2
where company = 'Carvana';

update layoffs_staging2
set industry = 'transportation'
where company = 'Carvana';

  
select * from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null; -- Selecting all rows which give limited insights

delete from layoffs_staging2
where total_laid_off is null and percentage_laid_off is null; -- Removing the rows with limited insights











