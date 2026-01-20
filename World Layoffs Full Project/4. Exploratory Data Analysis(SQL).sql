-- Data Exploration of World Layoffs



use world_layoffs;
select * from `layoffs_staging2`;

select max(total_laid_off), max(percentage_laid_off) 	-- Finding out the highest layoff occured + highest layoff percentage
from layoffs_staging2;

select * 
from `layoffs_staging2`
where percentage_laid_off = 1
order by total_laid_off desc; 			-- Finding out the company with highest number of layoffs to go under

select company, sum(total_laid_off)
from `layoffs_staging2`
group by company; 					-- Finding out which company had the greatest layoff in total

select min(date), max(date)
from `layoffs_staging2`; 		-- Finding out the time span of this layoff dataset

select industry, sum(total_laid_off)
from `layoffs_staging2`
group by industry; 			-- Finding out which industry had the most layoff in that time span

select country, sum(total_laid_off)
from `layoffs_staging2` 
group by country; 		-- Finding out which country had the most layoff in that time span

select year(`date`), sum(total_laid_off)
from `layoffs_staging2`
group by year(`date`); 		-- Finding out how the number of layoffs vary among years

select stage, sum(total_laid_off)
from `layoffs_staging2`
group by stage; 		-- Finding out how the number of layoffs is spread among stages

select date from `layoffs_staging2`;

select substring(date, 1, 7) as `Month`, sum(total_laid_off) as total_monthly_layoffs 	-- Extracting only the year-month out of the date
from `layoffs_staging2`
where substring(date, 1, 7) is not null 	-- Not considering records with no dates
group by `Month`
order by 1; 	-- Finding out the number of layoffs for each month

with rolling_total as 		-- Creating CTE to find out the cummulative monthly layoffs
(
select substring(date, 1, 7) as `Month`, sum(total_laid_off) as total_monthly_layoffs
from `layoffs_staging2`
where substring(date, 1, 7) is not null
group by `Month`
order by 1
)

select `Month`, total_monthly_layoffs, sum(total_monthly_layoffs) over(order by `Month`) as rolling_total
from rolling_total
group by `Month`; 		-- Query to obtain cumulative layoffs under rolling_total column

select company, year(`date`), sum(total_laid_off)
from `layoffs_staging2`
group by company, year(`date`); 		-- Listing the company and the number of layoffs the year it happened

with company_year(company, `year`, total_layoffs) as 	-- Creating first CTE
(
select company, year(`date`), sum(total_laid_off)
from `layoffs_staging2`
where year(`date`) is not null
group by company, year(`date`)
), company_year_rank as 		-- Creating second CTE to add a ranking function
(select *,
dense_rank() over(partition by `year` order by total_layoffs desc) as ranking
from company_year) 		-- Ranking for each year the number of layoffs made by a company from highest to lowest

select * 
from company_year_rank
where ranking <= 5; 		-- finding out the top 5 companies with most layoffs in each year

