use "noSQLproj"

// 1. Display a list of total vaccinations per day in Singapore. 
// [source table: country_vaccinations]
db.countryVac.aggregate([
    {$match:{"country":"Singapore"}},
    {$project:{_id:0, date:1, total_vaccinations:1}}
])

// 2. Display the sum of daily vaccinations among ASEAN countries.
// [source table: country_vaccinations]
db.countryVac.aggregate([ 
    {$project: {_id:0,country:1, daily_vaccinations: {$convert: {input:"$daily_vaccinations",  to: "double"}}}},
    {$match: {"country":{$in: ["Brunei", "Cambodia", "Indonesia", "Laos", "Malaysia", "Myanmar", "Philippines", "Singapore", "Thailand", "Vietnam"]}}},
    {$group: {_id:{groupByCountry: "$country"},  total_administrated:{$sum:"$daily_vaccinations"}}},
    {$project: {_id:0, country:"$_id.groupByCountry", total_administrated:1}},
    {$sort: {total_administrated: -1}}
])

// 3. Identify the maximum daily vaccinations per million of each country. Sort the list based on daily vaccinations per million in a descending order.
// [source table: country_vaccinations]
db.countryVac.aggregate([
    {$project: {_id:0, country:1, daily_vaccinations_per_million: {$convert: {input: "$daily_vaccinations_per_million", to: "double"}}}},
    {$group: {_id:{groupByCountry: "$country"}, Max_Daily: {$max: "$daily_vaccinations_per_million"}}},
    {$project: {_id:0, country:"$_id.groupByCountry", Max_Daily:1}},
    {$sort:{Max_Daily: -1}
])

// 4. Which is the most administrated vaccine? Display a list of total administration (i.e., sum of total vaccinations) per vaccine.
// [source table: country_vaccinations_by_manufacturer]
db.countryVacManu.aggregate([
    {$project: {_id:0, location:1, vaccine:1, total_vaccinations: {$convert:{input: "$total_vaccinations", to: "double"}}}},
    {$group: {_id: {location: "$location", vaccine: "$vaccine"}, max_administration_per_country: {$max: "$total_vaccinations"}}},
    {$group: {_id: {vaccine : "$_id.vaccine"}, total_administration: {$sum: "$max_administration_per_country"}}},
    {$project: {_id:0, vaccine: "$_id.vaccine", total_administration:1}},
    {$sort: {total_administration:-1}}
])

// 5. Italy has commenced administrating various vaccines to its populations as a 
// vaccine becomes available. Identify the first dates of each vaccine being administrated,
// then compute the difference in days between the earliest date and the 4th date.
// [source table: country_vaccinations_by_manufacturer]
db.countryVacManu.aggregate([
    {$match: {location: "Italy"}},
    {$project: {_id: 0, vaccine: 1, date: {$convert: {input: "$date", to: "date"}}}},
    {$group: {_id: {vaccine: "$vaccine"}, min_date: {$min: "$date"}}},
    {$project: {_id:0, vaccine: "$_id.vaccine", min_date:1}},
    {$sort: {min_date: -1}},
    {$group: {_id: {}, max_date: {$max: "$min_date"}, min_date: {$min: "$min_date"}}},
    {$project: {_id: 0, date_difference: {$divide: [{$subtract: ["$max_date", "$min_date"]}, 1000*60*60*24]}}}
])

// 6. What is the country with the most types of administrated vaccine?
// [source table: country_vaccinations_by_manufacturer]
db.countryVacManu.aggregate([
    {$group: {_id: {location: "$location"}, uniquevaccine: {$addToSet: "$vaccine"}}},
    {$project: {_id:0, country: "$_id.location", uniquevaccine:1, vaccineCount: {$size: "$uniquevaccine"}}},
    {$sort: {vaccineCount: -1}},
    {$limit:1},
    {$project: {country:1, uniquevaccine:1}}
])

// 7. What are the countries that have fully vaccinated more than 60% of its people? For each country, display the vaccines administrated.
// [source table: country_vaccinations]
db.countryVac.aggregate([
    {$project: {_id: 0, country: 1, vaccines: 1, people_fully_vaccinated_per_hundred: {$convert: {input: "$people_fully_vaccinated_per_hundred", to: "double"}}}},
    {$group: {_id: {country: "$country", vaccine: "$vaccines"}, vaccinated_percentage: {$max: "$people_fully_vaccinated_per_hundred"}}},
    {$match:{vaccinated_percentage:{$gt:60}}},
    {$project: {_id: 0,country:"$_id.country", vaccine: "$_id.vaccine", vaccinated_percentage:1}},
    {$sort: {vaccinated_percentage: -1}}
])

// 8. Monthly vaccination insight – display the monthly total vaccination amount of each vaccine per month in the United States.
// [source table: country_vaccinations_by_manufacturer]
db.countryVacManu.aggregate([
    {$match: {location: "United States"}},
    {$project: {_id: 0, vaccine: 1, total_vaccinations: {$convert: {input: "$total_vaccinations", to: "double"}}, date: {$convert: {input: "$date", to: "date"}}}},
    {$group: {_id: {vaccine: "$vaccine", month_date: {$month: "$date"}}, monthly_total: {$max: "$total_vaccinations"}}},
    {$project: {_id: 0, vaccine: "$_id.vaccine", month_date: "$_id.month_date", monthly_total:1}},
    {$sort: {month_date: 1}}
])

// 9. Days to 50 percent. Compute the number of days (i.e., using the first 
// available date on records of a country) that each country takes to go above 
// the 50% threshold of vaccination administration 
// (i.e., total_vaccinations_per_hundred > 50)
// [source table: country_vaccinations]
db.countryVac.aggregate([
    {$project: {_id:0, country:1, date: {$convert: {input:"$date",to:"date"}}, total_vaccinations_per_hundred: {$convert: {input:"$total_vaccinations_per_hundred",to:"double"}}}},
    {$match: {total_vaccinations_per_hundred: {$gt:50}}},
    {$group: {_id: {"country": "$country"}, threshold_date: {$min: "$date"}}}
    {$lookup: {
        from: "countryVac",
        localField: "_id.country", 
        foreignField: "country", 
        pipeline: [
            {$project: {_id:0, country:1, date: {$convert: {input:"$date",to:"date"}}}},
            {$group: {_id: {"country": "country"}, min_date: {$min: "$date"}}}],
        as: "VacInfo" 
    }},
    {$unwind: {path: "$VacInfo"}},
    {$project: {_id:0, country: "$_id.country", Days_to_Over_50: {$sum: [{$divide: [{$subtract: ["$threshold_date","$VacInfo.min_date"]},1000*60*60*24]},1]}}}, 
    {$sort: {country:1}}
])


// 10. Compute the global total of vaccinations per vaccine.
// [source table: country_vaccinations_by_manufacturer]
db.countryVacManu.aggregate([
    {$project: {_id: 0, location: 1, vaccine: 1, total_vaccinations: {$convert:{input: "$total_vaccinations", to: "double"}}}},
    {$group: {_id: {location: "$location", vaccine: "$vaccine"}, max_administrationpc: {$max: "$total_vaccinations"}}},
    {$group: {_id: {vaccine : "$_id.vaccine"}, total_administration: {$sum: "$max_administrationpc"}}},
    {$project: {_id: 0, vaccine: "$_id.vaccine", total_administration: 1}},
    {$sort: {total_administration: -1}}
])

// 11. What is the total population in Asia?
db.covid19data.aggregate([
    {$match: {continent: "Asia"}},
    {$project: {_id: 0, continent: 1, location: 1, date: {$convert: {input: "$date", to: "date"}}, population: {$convert: {input: "$population", to: "double"}}}},
    {$sort: {date:1}},
    {$group: {_id: {location: "$location",continent:"$continent"}, latest_pop: {$last:"$population"}}},
    {$group: {_id: {continent:"$_id.continent"},asia_total:{$sum:"$latest_pop"}}},
    {$project:{_id:0, continent:"$_id.continent", asia_total:1}}
])

// 12. What is the total population among the ten ASEAN countries?
db.covid19data.aggregate([
    {$match: {"location":{$in: ["Brunei", "Cambodia", "Indonesia", "Laos", "Malaysia", "Myanmar", "Philippines", "Singapore", "Thailand", "Vietnam"]}}},
    {$project: {_id:0, location:1, date:{$convert:{input:"$date",to:"date"}}, population: {$convert: {input: "$population", to: "double"}}}},
    {$sort: {date:1}},
    {$group: {_id: {location: "$location"}, latest_pop: {$last: "$population"}}},
    {$group: {_id: {}, asean_total:{$sum:"$latest_pop"}}},
    {$project: {_id:0, asean_total:1}}
])

// 13. Generate a list of unique data sources (source_name).
db.countryVac.aggregate([
    {$group: {_id:{}, uniquedatasource: {$addToSet: "$source_name"}}},
    {$project: {_id:0, uniquedatasource:1}}
])

// 14. Specific to Singapore, display the daily total_vaccinations starting (inclusive) March-1 2021 through (inclusive) May-31 2021. 
db.countryVac.aggregate([
    {$match: {country: "Singapore"}},
    {$project: {_id:0, daily_vaccinations: {$convert: {input: "$total_vaccinations", to: "double"}}, date: {$convert: {input: "$date", to: "date"}}}},
    {$match: {date: {$gte: ISODate("2021-03-01"), $lte: ISODate("2021-05-31")}}}
    {$sort: {date: 1}}
])


// 15. When is the first batch of vaccinations recorded in Singapore?
db.countryVac.aggregate([
     {$project: {_id:0, country:1, date: {$convert: {input: "$date", to: "date"}}, total_vaccinations: {$convert: {input: "$total_vaccinations", to: "double"}}}},
     {$match: {country: "Singapore"}},
     {$match: {total_vaccinations: {$gt:0}}},
     {$sort: {date:1}},
     {$limit: 1},
     {$project: {_id:0, total_vaccinations: 0}}
])


// 16. Based on the date identified in (5), specific to Singapore, compute the total number of new cases thereafter. 
// For instance, if the date identified in (5) is Jan-1 2021, the total number of new cases will be the sum of new cases starting from (inclusive) Jan-1 to the last date in the dataset.
db.covid19data.aggregate([
    {$match: {location: "Singapore"}},
    {$project: {_id:0, date: {$convert: {input: "$date", to: "date"}}, new_cases: {$convert: {input: "$new_cases", to: "double"}}}},
    {$match: {date: {$gte: ISODate("2021-01-11")}}},
    {$group: {_id: {}, total_cases:{$sum:"$new_cases"}}},
    {$project: {_id:0, total_cases:1}}
])

// 17. Compute the total number of new cases in Singapore before the date identified in (5).
// For instance, if the date identified in (5) is Jan-1 2021 and the first date recorded (in Singapore) in the dataset is Feb-1 2020, the total number of new cases will be the sum of new cases starting from (inclusive) Feb-1 2020 through (inclusive) Dec-31 2020.
db.covid19data.aggregate([
    {$match: {location: "Singapore"}},
    {$project: {_id:0, date: {$convert: {input: "$date", to: "date"}}, new_cases: {$convert: {input: "$new_cases", to: "double"}}}},
    {$match: {date: {$lt: ISODate("2021-01-11")}}},
    {$group: {_id: {}, total_cases:{$sum:"$new_cases"}}},
    {$project: {_id:0, total_cases:1}}
])

// 18. Herd immunity estimation. On a daily basis, specific to Germany, 
// calculate the percentage of new cases and total vaccinations on each 
// available vaccine in relation to its population.
db.covid19data.aggregate([
    {$match: {"location" :"Germany"}},
    {$project: {_id:0, date: 1, 
                new_cases: {$convert: {input:"$new_cases", to: "double"}}, 
                population: {$convert: {input:"$population", to: "double"}}}},
    {$lookup: 
        {
            from: "countryVacManu",
            localField: "date",
            foreignField: "date",
            pipeline: 
            [
                {$match: {"location" :"Germany"}},
                {$project: {_id:0, date: 1, vaccine:1, 
                            total_vaccinations: {$convert: {input:"$total_vaccinations", to: "double"}}}},
            ]
            as: "vaccInfo"
        }
    }
    {$unwind:"$vaccInfo"},
    {$project: {date:1, percentage_new_cases: {$divide: ["$new_cases", "$population"]}, 
                percentage_vaccinated: {$divide: ["$vaccInfo.total_vaccinations", "$population"]}, vaccine:"$vaccInfo.vaccine"}},
    {$sort: {date:1, vaccine:1}}
])

// 19. Vaccination Drivers. Specific to Germany, based on each daily new case, 
// display the total vaccinations of each available vaccines after 20 days, 
// 30 days, and 40 days.
db.covid19data.createIndex({"date":1})
db.countryVacManu.createIndex({"date":1})

db.covid19data.aggregate([
    {$project: {_id: 0, location:1, date: {$convert: {input: "$date", to: "date"}}, new_cases: {$convert: {input:"$new_cases", to: "double"}}}},
    {$match: {location: "Germany", new_cases: {$gt:0}}},
    {$project: {date: 1, "20daysAfter": {$add: ["$date",20*1000*60*60*24]}, 
                "30daysAfter": {$add: ["$date",30*1000*60*60*24]},
                "40daysAfter": {$add: ["$date",40*1000*60*60*24]}, new_cases: 1}},
    {$project: {date: 1, "20daysAfter": {$substr: [{$convert: {input: "$20daysAfter", to: "string"}}, 0, 10]}, 
                "30daysAfter": {$substr: [{$convert: {input: "$30daysAfter", to: "string"}}, 0, 10]}, 
                "40daysAfter": {$substr: [{$convert: {input: "$40daysAfter", to: "string"}}, 0, 10]}, new_cases: 1}},
    {$lookup: 
        {
            from: "countryVacManu",
            localField: "20daysAfter"
            foreignField: "date"
            pipeline: 
            [
                {$match: {"location" :"Germany"}},
                {$project: {_id:0, vaccine:1, date: {$convert: {input: "$date", to: "date"}},
                            total_vaccinations: {$convert: {input:"$total_vaccinations", to: "double"}}}}
            ]
            as: "vacc_after_20"
        }
    },
    {$lookup:
        {
            from: "countryVacManu",
            localField: "30daysAfter"
            foreignField: "date"
            pipeline: 
            [
                {$match: {"location" :"Germany"}},
                {$project: {_id:0, vaccine:1, date: {$convert: {input: "$date", to: "date"}},
                            total_vaccinations: {$convert: {input:"$total_vaccinations", to: "double"}}}}
            ]
            as: "vacc_after_30"
        }
    }
    {$lookup:
        {
            from: "countryVacManu",
            localField: "40daysAfter"
            foreignField: "date"
            pipeline: 
            [
                {$match: {"location" :"Germany"}},
                {$project: {_id:0, vaccine:1, date: {$convert: {input: "$date", to: "date"}},
                            total_vaccinations: {$convert: {input:"$total_vaccinations", to: "double"}}}}
            ]
            as: "vacc_after_40"
        }
    }
    {$project: {date:1, new_cases:1, vacc_after_20:1, vacc_after_30:1, vacc_after_40:1}}
])

// 20. Vaccination Effects. Specific to Germany, on a daily basis, 
// based on the total number of accumulated vaccinations 
// (sum of total_vaccinations of each vaccine in a day), 
// generate the daily new cases after 21 days, 60 days, and 120 days.
db.covid19data.createIndex({"date":1})
db.countryVacManu.createIndex({"date":1})

db.countryVacManu.aggregate([
    {$project: {_id: 0, location:1, date: {$convert: {input: "$date", to: "date"}}, total_vaccinations: {$convert: {input:"$total_vaccinations", to: "double"}}}},
    {$match: {location: "Germany"}},
    {$group: {_id: {date: "$date"}, accumulated_vaccinations:{$sum:"$total_vaccinations"}}},
    {$sort: {"_id.date":1}}
    {$project: {_id:0, date: "$_id.date", "21daysAfter": {$add: ["$_id.date",21*1000*60*60*24]}, 
                "60daysAfter": {$add: ["$_id.date",60*1000*60*60*24]},
                "120daysAfter": {$add: ["$_id.date",120*1000*60*60*24]}, accumulated_vaccinations:1}},
    {$project: {date: 1, "21daysAfter": {$substr: [{$convert: {input: "$21daysAfter", to: "string"}}, 0, 10]}, 
                "60daysAfter": {$substr: [{$convert: {input: "$60daysAfter", to: "string"}}, 0, 10]}, 
                "120daysAfter": {$substr: [{$convert: {input: "$120daysAfter", to: "string"}}, 0, 10]}, accumulated_vaccinations: 1}},
    {$lookup: 
        {
            from: "covid19data",
            localField: "21daysAfter"
            foreignField: "date"
            pipeline: 
            [
                {$match: {"location" :"Germany"}},
                {$project: {_id:0, date: {$convert: {input: "$date", to: "date"}},
                            new_cases: {$convert: {input:"$new_cases", to: "double"}}}}
            ]
            as: "new_cases_after_21"
        }
    },
    {$lookup:
        {
            from: "covid19data",
            localField: "60daysAfter"
            foreignField: "date"
            pipeline: 
            [
                {$match: {"location" :"Germany"}},
                {$project: {_id:0, date: {$convert: {input: "$date", to: "date"}},
                            new_cases: {$convert: {input:"$new_cases", to: "double"}}}}
            ]
            as: "new_cases_after_60"
        }
    }
    {$lookup:
        {
            from: "covid19data",
            localField: "120daysAfter"
            foreignField: "date"
            pipeline: 
            [
                {$match: {"location" :"Germany"}},
                {$project: {_id:0, date: {$convert: {input: "$date", to: "date"}},
                            new_cases: {$convert: {input:"$new_cases", to: "double"}}}}
            ]
            as: "new_cases_after_120"
        }
    },
    {$project: {_id:0, date: 1, new_cases_after_21: 1, new_cases_after_60: 1, new_cases_after_120: 1, accumulated_vaccinations:1}}
])