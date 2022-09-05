USE country_vaccinations;

-- 1. What is the total population in Asia?
SELECT continent, SUM(population) AS "Total Population" 
FROM countrydata
WHERE continent = "Asia";


-- 2. What is the total population among the ten ASEAN countries?
SELECT SUM(population) AS "Total Population in ASEAN" 
FROM countrydata
WHERE location IN ("Brunei", "Myanmar", "Cambodia", "Indonesia", "Laos", 
			"Malaysia", "Philippines", "Singapore", "Thailand", "Vietnam");


-- 3. Generate a list of unique data sources (source_name).
SELECT DISTINCT source_name
FROM country_sources;


-- 4. Specific to Singapore, display the daily total_vaccinations starting (inclusive) March-1 2021 through (inclusive) May-31 2021. 
SELECT date, total_vaccinations
FROM country_vaccinations
WHERE country = 'Singapore'
AND date >=  '3/1/2021' AND date <  '6/1/2021';


-- 5. When is the first batch of vaccinations recorded in Singapore?
SELECT MIN(STR_TO_DATE(date, '%m/%d/%Y')) AS first_date FROM country_vaccinations
WHERE country = "Singapore"
AND total_vaccinations>0;


-- 6. Based on the date identified in (5), specific to Singapore, compute the total number of new cases thereafter. 
-- For instance, if the date identified in (5) is Jan-1 2021, the total number of new cases will be the sum of new cases starting from (inclusive) Jan-1 to the last date in the dataset.
SELECT SUM(new_cases) AS "Total New Cases" FROM country_casesbydate
WHERE iso_code = (SELECT iso_code FROM countrydata WHERE location = "Singapore")
AND date >=
(SELECT MIN(STR_TO_DATE(date, '%m/%d/%Y')) AS first_date FROM country_vaccinations
WHERE country = "Singapore"
AND total_vaccinations>0);


-- 7. Compute the total number of new cases in Singapore before the date identified in (5).
-- For instance, if the date identified in (5) is Jan-1 2021 and the first date recorded (in Singapore) in the dataset is Feb-1 2020, the total number of new cases will be the sum of new cases starting from (inclusive) Feb-1 2020 through (inclusive) Dec-31 2020.
SELECT SUM(new_cases) AS Total_New_Cases FROM country_casesbydate
WHERE iso_code = (SELECT iso_code FROM countrydata WHERE location = "Singapore")
AND date <
(SELECT MIN(STR_TO_DATE(date, '%m/%d/%Y')) AS first_date FROM country_vaccinations
WHERE country = "Singapore"
AND total_vaccinations>0);


-- 8. Herd immunity estimation. On a daily basis, specific to Germany, calculate the percentage of new cases (i.e., percentage of new cases = new cases / populations) and total vaccinations on each available vaccine in relation to its population.
SELECT t1.date, t1.new_cases/t3.population AS "percentage of new cases", t2.vaccine, 
t2.total_vaccinations/t3.population AS "percentage of total vaccinations"
FROM 
(
	SELECT new_cases, date FROM country_casesbydate
	WHERE iso_code = (SELECT iso_code FROM countrydata WHERE location = "Germany")
) t1
INNER JOIN
(
	SELECT vaccine, total_vaccinations, date
	FROM country_vaccinations_by_manufacturer
	WHERE location = "Germany"
) t2
ON t1.date = t2.date,
(SELECT population FROM countrydata WHERE location = "Germany") t3;


-- 9. Vaccination Drivers. Specific to Germany, based on each daily new case, display the total vaccinations of each available vaccines after 20 days, 30 days, and 40 days.
SELECT n.date, n.new_cases,
COALESCE(m.vaccine, a.vaccine, b.vaccine, c.vaccine) AS vaccine, 
m.total_vaccinations,
a.date AS aft20_days, a.total_vaccinations AS vac_aft20days,
b.date AS aft30_days, b.total_vaccinations AS vac_aft30days,
c.date AS aft40_days, c.total_vaccinations AS vac_aft40days
FROM country_casesbydate AS n 
LEFT JOIN 
country_vaccinations_by_manufacturer AS m 
ON n.date = m.date AND n.iso_code = m.iso_code
LEFT JOIN 
country_vaccinations_by_manufacturer AS c 
ON date_add(n.date, INTERVAL 40 DAY) = c.date 
AND c.iso_code = n.iso_code AND c.vaccine = COALESCE(m.vaccine, c.vaccine)
LEFT JOIN 
country_vaccinations_by_manufacturer AS b 
ON date_add(n.date, INTERVAL 30 DAY) = b.date AND b.iso_code = n.iso_code 
AND b.vaccine = COALESCE(m.vaccine, c.vaccine)
LEFT JOIN 
country_vaccinations_by_manufacturer AS a 
ON date_add(n.date, INTERVAL 20 DAY) = a.date AND a.iso_code = n.iso_code 
AND a.vaccine = COALESCE(m.vaccine, c.vaccine)
WHERE new_cases>0  AND n.iso_code = (SELECT iso_code FROM countrydata WHERE location = "Germany")
ORDER BY n.date;


-- 10. Vaccination Effects. Specific to Germany, on a daily basis, based on the total number of accumulated vaccinations (sum of total_vaccinations of each vaccine in a day), generate the daily new cases after 21 days, 60 days, and 120 days.
SELECT a.date, b.total_vac, 
LEAD(a.new_cases,21) OVER (ORDER BY date) new_cases_aft21days,
LEAD(a.new_cases,60) OVER (ORDER BY date) new_cases_aft60days, 
LEAD(a.new_cases,120) OVER (ORDER BY date) new_cases_aft120days 
FROM country_casesbydate a, 
(
	SELECT location, date,sum(total_vaccinations) total_vac
	FROM country_vaccinations_by_manufacturer 
	WHERE location = 'Germany'
	GROUP BY date
) b
WHERE iso_code = (SELECT iso_code FROM countrydata WHERE location = "Germany") 
AND a.date = b.date;