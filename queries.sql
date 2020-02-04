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
select
*,
round((provider_total - cum_death) / provider_total,2) still_alive_perc
from (
	select
	provider,
	observed_life_days ,
	sum(case when seen_death then 1 else 0 end) deaths,
	sum(count(*)) over (partition by provider) provider_total,
	sum(sum(case when seen_death then 1 else 0 end)) over (partition by provider order by observed_life_days RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cum_death
	from scooter_summary
	where city <> '' and seen_birth and cp_max_date - birth_date >= 90 and observed_life_days > 0
	group by 1,2
	order by 1,2
) a
where observed_life_days <= 90


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
