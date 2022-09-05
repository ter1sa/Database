#Question1
select date, total_vaccinations 
from country_vaccinations
where country = "Singapore";

#Question2
select vaccines, sum(daily_vaccinations) as total_administrated
from country_vaccinations
where country in ("Brunei", "Myanmar", "Cambodia", "Indonesia", "Laos", 
				  "Malaysia", "Philippines", "Singapore", "Thailand", "Vietnam")
group by vaccines
order by total_administrated DESC;

#Question3
select country, max(daily_vaccinations_per_million) 
from country_vaccinations
group by country
order by max(daily_vaccinations_per_million) DESC;

#Question4
select vaccine, sum(total_vaccinations) as total_administrated 
from country_vaccinations_by_manufacturer
group by vaccine
order by total_administrated DESC;

#Question5
select vaccine, min(date) as date 
from country_vaccinations_by_manufacturer
where location = "Italy"
group by vaccine;

select datediff(max(date), min(date)) as Difference_In_Days 
from (
	select vaccine, min(date) as date from country_vaccinations_by_manufacturer
	where location = "Italy"
	group by vaccine
	) q5;

#Question6
select distinct location, vaccine from country_vaccinations_by_manufacturer
where location = (
	select location from country_vaccinations_by_manufacturer
	group by location
	order by count(DISTINCT vaccine) DESC
	limit 1
    );

#Question7
select country, vaccines, max(people_fully_vaccinated_per_hundred) as vaccinated_percentage 
from country_vaccinations
group by country, vaccines
having vaccinated_percentage > 60
order by vaccinated_percentage DESC;

#Question8
select date_format(date, '%b') as month #format %b: Abbreviated month name (Jan to Dec)
, vaccine, max(total_vaccinations) as monthly_total_vaccinations 
from country_vaccinations_by_manufacturer
where location = "United States"
group by month, vaccine;

#Question9
select c1.country, datediff(c2.achieved_date, c1.start_date)+1 as `Days_to_over_50%` 
from (
	select country, min(str_to_date(date, '%m/%d/%Y')) as start_date 
    #format %m: Month name as a numeric value (01 to 12), 
    #format %d: Day of the month as a numeric value (01 to 31),
    #format %Y: Year as a numeric, 4-digit value
	from country_vaccinations
	group by country
	) c1
inner join
	(
	select country, min(str_to_date(date, '%m/%d/%Y')) as achieved_date 
	from country_vaccinations
	where total_vaccinations_per_hundred > 50
	group by country
	) c2
on c1.country = c2.country;


#Question10
select vaccine, sum(total) as global_total 
from (
	select location, vaccine, max(total_vaccinations) as total 
	from country_vaccinations_by_manufacturer
	group by vaccine, location
	) q10
group by vaccine
order by global_total DESC;