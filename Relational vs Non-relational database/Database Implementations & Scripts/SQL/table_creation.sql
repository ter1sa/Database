USE country_vaccinations;

# countrydata table
DROP TABLE IF EXISTS `countrydata`;
CREATE TABLE IF NOT EXISTS countrydata AS
  SELECT distinct(iso_code), location, continent, population, population_density, median_age, aged_65_older, aged_70_older
  handwashing_facilities, hospital_beds_per_thousand, gdp_per_capita, human_development_index, extreme_poverty
  FROM country_vaccinations.covid19data;
  
# country_sources table
DROP TABLE IF EXISTS `country_sources`;
CREATE TABLE IF NOT EXISTS country_sources AS
  SELECT distinct(iso_code), source_name, source_website
  FROM country_vaccinations.country_vaccinations;
  
# Drop sources columns from country_vaccinations
ALTER TABLE country_vaccinations.country_vaccinations
DROP COLUMN source_name,
DROP COLUMN source_website;
  
# country_healthdata table
DROP TABLE IF EXISTS `country_healthdata`;
CREATE TABLE IF NOT EXISTS country_healthdata AS
  SELECT distinct(iso_code), cardiovasc_death_rate, diabetes_prevalence, female_smokers,male_smokers,
  life_expectancy
  FROM country_vaccinations.covid19data;
  
# country_c19totals table
DROP TABLE IF EXISTS `country_c19totals`;
CREATE TABLE IF NOT EXISTS country_c19totals AS
  SELECT iso_code, date, total_cases, total_cases_per_million, total_deaths,
  total_deaths_per_million, total_tests,total_tests_per_thousand, tests_units
  FROM country_vaccinations.covid19data;
  
# country_casesbydate table
DROP TABLE IF EXISTS `country_casesbydate`;
CREATE TABLE IF NOT EXISTS country_casesbydate AS
  SELECT iso_code, date, new_cases, new_cases_smoothed, new_cases_per_million, new_cases_smoothed_per_million
  FROM country_vaccinations.covid19data;
  
# covid19_mortalitybydate table
DROP TABLE IF EXISTS `covid19_mortalitybydate`;
CREATE TABLE IF NOT EXISTS covid19_mortalitybydate AS
  SELECT iso_code, date, new_deaths, new_deaths_smoothed, new_deaths_per_million
  new_deaths_smoothed_per_million, excess_mortality
  FROM country_vaccinations.covid19data;
  
# covid19_hospbydate table
DROP TABLE IF EXISTS `covid19_hospbydate`;
CREATE TABLE IF NOT EXISTS covid19_hospbydate AS
  SELECT iso_code, date, hosp_patients, hosp_patients_per_million, icu_patients,
  icu_patients_per_million, weekly_hosp_admissions, weekly_hosp_admissions_per_million
  weekly_icu_admissions, weekly_icu_admissions_per_million
  FROM country_vaccinations.covid19data;
  
# covid19_testingbydate table
DROP TABLE IF EXISTS `covid19_testingbydate`;
CREATE TABLE IF NOT EXISTS covid19_testingbydate AS
  SELECT iso_code, date, new_tests, new_tests_per_thousand, new_tests_smoothed, 
  new_tests_smoothed_per_thousand, tests_per_case, positive_rate
  FROM country_vaccinations.covid19data;
  
# covid19_vaccinations table
DROP TABLE IF EXISTS `covid19_vaccinations`;
CREATE TABLE IF NOT EXISTS covid19_vaccinations AS
  SELECT iso_code, date, new_vaccinations, new_vaccinations_smoothed, 
  new_vaccinations_smoothed_per_million, total_vaccinations, total_vaccinations_per_hundred
  FROM country_vaccinations.covid19data;
  
# covid19_stringency table
DROP TABLE IF EXISTS `covid19_stringency`;
CREATE TABLE IF NOT EXISTS covid19_stringency AS
  SELECT iso_code, date, stringency_index
  FROM country_vaccinations.covid19data;
  
# covid19_reproduction table
DROP TABLE IF EXISTS `covid19_reproduction`;
CREATE TABLE IF NOT EXISTS covid19_reproduction AS
  SELECT iso_code, date, reproduction_rate
  FROM country_vaccinations.covid19data;
  
# Create iso_code column in country_vaccinations_by_manufacturer
SET SQL_SAFE_UPDATES = 0;
ALTER TABLE country_vaccinations_by_manufacturer ADD COLUMN iso_code TEXT;
UPDATE country_vaccinations_by_manufacturer t1
INNER JOIN countrydata t2 ON t1.location = t2.location
SET t1.iso_code = t2.iso_code;
SET SQL_SAFE_UPDATES = 1;
  
# Drop covid19data
DROP TABLE `country_vaccinations`.`covid19data`;
