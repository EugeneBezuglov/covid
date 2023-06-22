/*
Covid data exploration.

Skills used: window (analytic) functions, CTEs, aggregate functions, converting data types, joins, temp tables.
*/

-- I used Python to import CSV into PostgreSQL and `date` column was imported as TEXT type, so I change it to DATE type.

ALTER TABLE covid
ALTER COLUMN date TYPE date
USING date::date;

---- Informative queries
-- Select the overview Data and save it in a View.

DROP VIEW IF EXISTS covid_overview;

CREATE VIEW covid_overview AS
WITH first_death AS 
(
	SELECT
		location,
		MIN(date) as first_death
	FROM covid
	WHERE total_deaths IS NOT NULL
	GROUP BY location
), first_vaccination AS 
(
	SELECT
		location,
		MIN(date) as first_vaccination
	FROM covid
	WHERE total_vaccinations IS NOT NULL
	GROUP BY location
)

SELECT
	r.continent,
	r.location,
	MAX(total_cases) AS total_cases,
	MAX(total_deaths) AS total_deaths,
	ROUND(CAST(100 * MAX(total_deaths) / MAX(total_cases) AS numeric), 2) AS death_percentage,
	FLOOR(1000000 * MAX(total_deaths) / MAX(population)) AS "deaths / million",
	ROUND(CAST(100 * MAX(total_cases) / MAX(population) AS numeric), 2) AS infected_population_percent,
	MIN(date) AS first_case,
	first_death,
	first_vaccination,
	first_death - MIN(date) AS days_to_first_death,
	first_vaccination - MIN(date) AS days_to_first_vaccination,
	MAX(median_age) AS median_age
FROM covid r
	INNER JOIN first_death fd ON r.location = fd.location
	INNER JOIN first_vaccination fv ON r.location = fv.location
WHERE
	total_cases IS NOT NULL
	AND continent IS NOT NULL
GROUP BY
	r.continent,
	r.location,
	first_death,
	first_vaccination
ORDER BY
	total_cases DESC;

SELECT * FROM covid_overview;


-- Correlations.
WITH cte_1 AS
(
	SELECT
		continent,
		location,
		MAX(population) AS population,
		MAX(total_cases) AS total_cases,
		(100 * MAX(total_cases) / MAX(population))::numeric as infection_rate,
		MAX(median_age) AS median_age,
		MAX(total_deaths) / MAX(total_cases) AS death_rate,
		MAX(total_vaccinations) AS total_vaccinations,
		MAX(total_vaccinations) / MAX(population) AS vaccination_rate,
		MAX(diabetes_prevalence) AS diabetes_prevalence,
		MAX(female_smokers) AS female_smokers,
		MAX(male_smokers) AS male_smokers,
		MAX(life_expectancy) AS life_expectancy,
		MAX(human_development_index) AS human_development_index,
		MAX(cardiovasc_death_rate) AS cardiovasc_death_rate,
		MAX(gdp_per_capita) AS gdp_per_capita,
		MAX(total_tests_per_thousand) AS total_tests_per_thousand,
		MAX(extreme_poverty) AS extreme_poverty,
		MAX(aged_70_older) AS aged_70_older
	FROM covid
	WHERE continent IS NOT NULL
	GROUP BY 
		continent,
		location
)

SELECT
	continent,
	CORR(total_cases, population) AS "total_cases/population",
	CORR(death_rate, population) AS "death_rate/population",
	CORR(infection_rate, population) AS "infection_rate/population",
	CORR(death_rate, median_age) AS "death_rate/median_age",
	CORR(infection_rate, median_age) AS "infection_rate/median_age",
	CORR(death_rate, aged_70_older) AS "death_rate/aged_70_older",
	CORR(infection_rate, aged_70_older) AS "infection_rate/aged_70_older",
	CORR(death_rate, vaccination_rate) AS "death_rate/vaccination_rate",
	CORR(infection_rate, vaccination_rate) AS "infection_rate/vaccination_rate",
	CORR(death_rate, female_smokers) AS "death_rate/female_smokers",
	CORR(infection_rate, female_smokers) AS "infection_rate/female_smokers",
	CORR(death_rate, male_smokers) AS "death_rate/male_smokers",
	CORR(infection_rate, male_smokers) AS "infection_rate/male_smokers",
	CORR(death_rate, diabetes_prevalence) AS "death_rate/diabetes_prevalence",
	CORR(infection_rate, diabetes_prevalence) AS "infection_rate/diabetes_prevalence",
	CORR(death_rate, life_expectancy) AS "death_rate/life_expectancy",
	CORR(infection_rate, life_expectancy) AS "infection_rate/life_expectancy",
	CORR(death_rate, human_development_index) AS "death_rate/human_development_index",
	CORR(infection_rate, human_development_index) AS "infection_rate/human_development_index",
	CORR(death_rate, cardiovasc_death_rate) AS "death_rate/cardiovasc_death_rate",
	CORR(infection_rate, cardiovasc_death_rate) AS "infection_rate/cardiovasc_death_rate",
	CORR(death_rate, gdp_per_capita) AS "death_rate/gdp_per_capita",
	CORR(infection_rate, gdp_per_capita) AS "infection_rate/gdp_per_capita",
	CORR(death_rate, extreme_poverty) AS "death_rate/extreme_poverty",
	CORR(infection_rate, extreme_poverty) AS "infection_rate/extreme_poverty",
	CORR(death_rate, total_tests_per_thousand) AS "death_rate/total_tests_per_thousand",
	CORR(infection_rate, total_tests_per_thousand) AS "infection_rate/total_tests_per_thousand"
FROM cte_1
GROUP BY continent;


---- Basic queries

-- Rolling vaccinations

WITH rolling AS 
(
	SELECT
		date,
		continent,
		location,
		population,
		new_vaccinations,
		SUM(new_vaccinations) OVER(PARTITION BY location ORDER BY date) rolling_vaccinations
	FROM covid
	WHERE continent IS NOT NULL
	ORDER BY
		continent DESC,
		location,
		date
)

SELECT
	*,
	ROUND(CAST(100 * rolling_vaccinations / population AS NUMERIC), 2) AS percent_vaccinated
FROM rolling

-- Moving average of new deaths

SELECT
	date,
	continent,
	location,
	population,
	new_deaths,
	ROUND(AVG(new_deaths) OVER(PARTITION BY location ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS rolling_average_new_deaths
FROM covid
ORDER BY
	continent,
	location,
	date;

-- Infection rate compared to population

SELECT
	location,
	population,
	MAX(total_cases) as total_cases,
	ROUND((100 * MAX(total_cases) / population)::numeric, 2) as infection_rate
FROM covid
WHERE total_cases IS NOT NULL
GROUP BY
	location,
	population
ORDER BY
	infection_rate DESC

-- How many people died in each country

SELECT
	location,
	MAX(total_deaths) as total_deaths
FROM covid
WHERE 
	total_deaths IS NOT NULL
	AND continent IS NOT NULL -- because location includes World, Europe, Asia, and so on, with corresponding continent NULL, so I exclude it.
GROUP BY location
ORDER BY total_deaths DESC

-- Total deaths world. I check it using the two possible ways and then union tables.

WITH country AS
(
	SELECT
		location,
		MAX(total_deaths) as total_deaths
	FROM covid
	WHERE 
		total_deaths IS NOT NULL
		AND continent IS NOT NULL -- because location includes World, Europe, Asia, and so on, with continent NULL, so I exclude it.
	GROUP BY location
	ORDER BY total_deaths DESC
)

SELECT
	SUM(total_deaths) AS world_death_count
FROM country
UNION ALL
SELECT MAX(total_deaths)
FROM covid
WHERE location = 'World'

-- How many people died by continent.

SELECT
	continent,
	SUM(total_deaths) AS total_deaths
FROM covid_overview
GROUP BY continent
ORDER BY total_deaths DESC;


-- Top 5 countries on each continent with the highest total deaths count.

WITH ranked AS
(
	SELECT
		continent,
		location,
		MAX(total_deaths) AS total_deaths,
		RANK() OVER(PARTITION BY continent ORDER BY MAX(total_deaths) DESC) AS rank
	FROM covid
	WHERE continent IS NOT null
		AND total_deaths IS NOT null
	GROUP BY
		continent,
		location
)

SELECT
	continent,
	location,
	total_deaths
FROM ranked
WHERE rank <= 5;

-- Top 5 countries on each continent with highest population death percentage.

WITH ranked AS
(
	SELECT
		continent,
		location,
		ROUND(CAST(100* MAX(total_deaths) / population AS numeric), 2) AS percent_of_population_died,
		RANK() OVER(PARTITION BY continent ORDER BY 100* MAX(total_deaths) / population DESC) AS rank
	FROM covid
	WHERE continent IS NOT null
		AND total_deaths IS NOT null
	GROUP BY
		continent,
		location,
		population
)

SELECT
	continent,
	location,
	percent_of_population_died
FROM ranked
WHERE rank <= 5;


-- Compare top 5 countries by death count and top 5 by death rate per population.
-- Essentially, I union the previous two queries horizontally, adding extra rank columns for clarity.
-- Not the best query though, it's only good for an initial overview before diving further.

WITH ranked_1 AS
(
	SELECT
		continent,
		location,
		MAX(total_deaths) AS total_deaths,
		RANK() OVER(PARTITION BY continent ORDER BY MAX(total_deaths) DESC) AS rank
	FROM covid
	WHERE continent IS NOT null
		AND total_deaths IS NOT null
	GROUP BY
		continent,
		location

), ranked_2 AS

(
	SELECT
		continent,
		location,
		ROUND(CAST(100* MAX(total_deaths) / population AS numeric), 2) AS percent_of_population_died,
		RANK() OVER(PARTITION BY continent ORDER BY 100* MAX(total_deaths) / population DESC) AS rank
	FROM covid
	WHERE continent IS NOT null
		AND total_deaths IS NOT null
	GROUP BY
		continent,
		location,
		population
)

SELECT
	r1.continent,
	r1.rank AS rank_total_deaths,
	r3.rank AS rank_death_percent,
	r1.location,
	r1.total_deaths,
	r2.rank AS rank_death_percent,
	r4.rank AS rank_total_deaths, 
	r2.location,
	r2.percent_of_population_died
FROM ranked_1 r1
	INNER JOIN ranked_2 r2
		ON r1.rank = r2.rank
		AND r1.continent = r2.continent
		AND r1.rank <=5
	INNER JOIN ranked_2 r3
		ON r1.location = r3.location
	INNER JOIN ranked_1 r4
		ON r4.location = r2.location
	
ORDER BY r1.continent, r1.rank


-- Create temp table for data on Russia (for future use).

DROP TABLE IF EXISTS russia;

SELECT *
INTO TEMP russia
FROM covid
WHERE location = 'Russia';

SELECT *
FROM russia;
	