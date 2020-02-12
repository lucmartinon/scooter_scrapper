create index spl_index on scooter_position_logs(city, provider, scooter_id)

CREATE MATERIALIZED VIEW city_providers AS (
select provider, city, min(date(timestamp)) cp_min_date, max(date(timestamp)) cp_max_date
from scooter_position_logs
where provider <> 'lime' and city <> ''
group by 1,2);

refresh materialized view city_providers;
refresh materialized view scooters;

create table city_providers (
provider VARCHAR(50) NOT NULL,
city VARCHAR(50) NOT NULL,
cp_min_date date not null,
cp_max_date date not null
);


create materialized view scooters as (
select distinct
    city, provider, scooter_id,
    first_value(date(timestamp)) over win scooter_first_seen,
    last_value(date(timestamp)) over win scooter_last_seen,
    last_value(timestamp) over win scooter_last_seen_ts,
    last_value(lng) over win last_lng,
    last_value(lat) over win last_lat
from scooter_position_logs
where provider <> 'lime' and city <> ''
WINDOW win as (partition by city, provider, scooter_id order by timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )
);

create or replace view scooter_summary as (
select
provider, city, scooter_id,
first_seen,
last_seen,
(cp_max_date - scooter_last_seen > 14) seen_death,
(scooter_first_seen - cp_min_date > 7) seen_birth,
case when cp_max_date - scooter_last_seen > 14 then scooter_last_seen  end  death_date,
case when scooter_first_seen - cp_min_date > 7 then scooter_first_seen end birth_date,
(scooter_last_seen - scooter_first_seen) observed_life_days,
case when (cp_max_date - scooter_last_seen > 14 and scooter_first_seen - cp_min_date > 7) then (scooter_last_seen - scooter_first_seen) end tot_life_days,
case when cp_max_date - scooter_last_seen > 14 then last_lat end last_lat_before_death,
case when cp_max_date - scooter_last_seen > 14 then last_lng end last_lng_before_death,
cp_max_date,
cp_min_date
from scooters
join city_providers using (city, provider)
);

create table scooter_summary (
provider VARCHAR(50) NOT NULL,
city VARCHAR(50) NOT NULL,
scooter_id VARCHAR(50) NOT NULL,
scooter_first_seen DATE NULL,
scooter_last_seen DATE NULL,
seen_death BOOLEAN NULL,
seen_birth BOOLEAN NULL,
death_date DATE NULL,
birth_date DATE NULL,
observed_life_days INT NOT NULL,
tot_life_days INT NULL,
last_lat_before_death FLOAT8 NULL,
last_lng_before_death FLOAT8 NULL,
cp_max_date date not null,
cp_min_date date not null
);


create materialized view spl_with_last_postition as (
select distinct
    city, provider, scooter_id,
    LAG(timestamp) over win previously_seen,
    round(extract(epoch from timestamp) - extract(epoch from LAG(timestamp) over win) / 3600) hours_since_previously_seen
    from scooter_position_logs
where provider <> 'lime'
WINDOW win as (partition by city, provider, scooter_id order by timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
);


select
provider,
city,
count(*) tot_scooters,
count(distinct city) cities,
sum(case when seen_death then 1 else 0 end) seen_death,
sum(case when seen_birth then 1 else 0 end) seen_birth,
sum(case when seen_birth and seen_death then 1 else 0 end) seen_full_life,
sum(case when not seen_birth and not seen_death then 1 else 0 end) stalone_scooters,

avg(observed_life_days) avg_lifetime_days_ALL,
percentile_disc(0.5) within group (order by observed_life_days),

avg(case when seen_birth then observed_life_days else NULL end) avg_lifetime_seen_birth,
avg(case when seen_birth and seen_death then observed_life_days else NULL end) avg_lifetime_seen_full_life
from scooter_summary where city <> ''
group by 1,2
having count(*) > 10
order by 2 desc;

#RETENTION D90
with
numbers as (select * from generate_series(0,90) as day),

scooters as (
	select *,  (count(*) over (partition by city, provider)) cp_tot
	from scooter_summary
	left join mass_extinctions using (city, provider, death_date)
	where seen_birth and observed_life_days > 0 and cp_max_date - birth_date >= 90 and provider in ('tier' , 'voi' , 'circ' , 'hive' )
	and mass_extinctions.death_date is null
)
select
	day ,
	provider ,
	city,
  cp_tot,
	cast (sum (case when observed_life_days >= day then 1 else 0 end) as Integer) remaining_scooters,
  cast (100 * sum (case when observed_life_days >= day then 1 else 0 end) / cp_tot as Integer) remaining_scooter_perc
from numbers join scooters on true
where cp_tot >= 60
group by 1,2,3,4


#observable scooter per provider per day


select
provider,
observed_life_days osbserved_days,
count(*) scooters,
sum(count(*)) over (partition by provider) provider_total,
sum(count(*)) over (partition by provider order by (cp_max_date - birth_date) RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cum_sum,
sum(count(*)) over (partition by provider) - sum(count(*)) over (partition by provider order by observed_life_days RANGE BETWEEN UNBOUNDED PRECEDING AND current row) remaining_scooters,
round((sum(count(*)) over (partition by provider) - sum(count(*)) over (partition by provider order by observed_life_daysRANGE BETWEEN UNBOUNDED PRECEDING AND current row)) / sum(count(*)) over (partition by provider),2) remaining_ratio
from scooter_summary
where seen_birth and observed_life_days > 0 and (cp_max_date - birth_date) >= 90
group by 1,2
order by 1,2

same but with a privot

select * from crosstab(
'select * from (
select
observed_life_days row_name,
provider cat,
round(
	100 * (sum(count(*)) over (partition by provider) -
	 sum(count(*)) over (partition by provider order by observed_life_days RANGE BETWEEN UNBOUNDED PRECEDING AND current row)
	) / sum(count(*)) over (partition by provider),2)  as value
from scooter_summary
where seen_birth and observed_life_days > 0 and (cp_max_date - birth_date) >= 90
group by 1,2
order by 1,2
) a where row_name <= 90
',
'SELECT DISTINCT provider cat FROM scooter_summary where provider in (''tier'' , ''voi'' , ''circ'' , ''hive''  ) order by 1;'
) AS
(osbserved_days int, "circ" float,  "hive" float, "tier" float, "voi" float );



detecting mass extinctions
with dates as (SELECT generate_series(date '2019-10-01', date '2020-01-31', '1 day') date )
select
city,
provider,
date,
sum(case when date between scooter_first_seen and scooter_last_seen then 1 else 0 end) alive,
sum(case when date = death_date then 1 else 0 end) deaths,
sum(case when date = birth_date then 1 else 0 end) birth,
avg(case when date = death_date then tot_life_days else null end) avg_life_amoung_todays_deaths,
case
	when sum(case when date between scooter_first_seen and scooter_last_seen then 1 else 0 end) > 0
	then 100 * sum(case when date = death_date then 1 else 0 end) / sum(case when date between scooter_first_seen and scooter_last_seen then 1 else 0 end)
	else 0 end death_perc
from dates join scooter_summary on (date between cp_min_date and cp_max_date)
--where city = 'Marseille' and provider = 'voi'
group by 1,2,3
order by 8 desc, 5 desc



deciding at which ratio we should categorize a death a mass extinctionswith dates as (SELECT generate_series(date '2019-10-01', date '2020-01-31', '1 day') date )
with dates as (SELECT generate_series(date '2019-10-01', date '2020-01-31', '1 day') date )
select
death_perc, sum(deaths) cum_death, count(*) cases
from (
select
city,
provider,
date,
sum(case when date between scooter_first_seen and scooter_last_seen then 1 else 0 end) alive,
sum(case when date = death_date then 1 else 0 end) deaths,
sum(case when date = birth_date then 1 else 0 end) birth,
avg(case when date = death_date then scooter_last_seen - scooter_first_seen else null end) avg_life_todays_deaths,
case
	when sum(case when date between scooter_first_seen and scooter_last_seen then 1 else 0 end) > 0
	then 100 * sum(case when date = death_date then 1 else 0 end) / sum(case when date between scooter_first_seen and scooter_last_seen then 1 else 0 end)
	else 0 end death_perc
from dates join scooter_summary on (date between cp_min_date and cp_max_date)
group by 1,2,3
)a
where deaths > 5
group by 1
order by 1

--> based on this I take 25%
create materialized view mass_extinctions as (
with dates as (SELECT cast(generate_series(date '2019-10-01', date '2020-01-31', '1 day') as date) date)
select
city,
provider,
date death_date
from (
select
city,
provider,
date,
sum(case when date between scooter_first_seen and scooter_last_seen then 1 else 0 end) alive,
sum(case when date = death_date then 1 else 0 end) deaths,
sum(case when date = birth_date then 1 else 0 end) births,
case
	when sum(case when date between scooter_first_seen and scooter_last_seen then 1 else 0 end) > 0
	then 100 * sum(case when date = death_date then 1 else 0 end) / sum(case when date between scooter_first_seen and scooter_last_seen then 1 else 0 end)
	else 0 end death_perc
from dates join scooter_summary on (date between cp_min_date and cp_max_date)
group by 1,2,3
)a
where deaths > 5 and (death_perc >=25 or (death_perc >= 10 and deaths > 50))
);
