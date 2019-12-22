select
provider,
min(max_ts) earliest_max,
max(min_ts) latest_min,
count(distinct id)
from (
select
city, provider, id, min(timestamp) min_ts, max(timestamp) max_ts, count(*) seen
from scooter_position_logs
where provider != 'lime'
group by 1,2,3
) a
group by 1;


-- see the number of scooter per "last seen on" for each provider.
select
provider,
max_date,
count(distinct id) scooters from (
select
city, provider, id, min(timestamp) min_ts, max(date(timestamp)) max_date, count(*) seen
from scooter_position_logs
where provider != 'lime' and not (provider = 'circ' and city = 'Marseille')
group by 1,2,3
) a
group by 1,2;




focus on circ
select
city,
max_date,
count(distinct id) scooters from (
select
city, provider, id, min(timestamp) min_ts, max(date(timestamp)) max_date, count(*) seen
from scooter_position_logs
where provider = 'circ'
group by 1,2,3
) a
group by 1,2;


select timestamp, count(*) from scooter_position_logs
where provider = 'circ' and city = 'Berlin'
group by 1
order by 1



-- scooters dead since 14 days
with
spl as (select provider, city, scooter_id, timestamp, location from scooter_position_logs where provider <> 'lime'),

city_providers as (
    select provider, city, min(date(timestamp)) cp_min_date, max(date(timestamp)) cp_max_date from spl group by 1,2
),

scooter_postitions as (
select
    city, provider, scooter_id, timestamp, location,
    cast (first_value(location) over (partition by city, provider, scooter_id order by timestamp desc) as varchar(40)) last_location
    from spl
),


scooters as (
select
    city, provider, scooter_id, last_location,
    min(date(timestamp)) scooter_first_seen, max(date(timestamp)) scooter_last_seen
    from scooter_postitions group by 1,2,3,4
),

fully_observed_scooters as (

)

select
city, provider, count(*) fully_observed_scooters, min(lifetime_days), max(lifetime_days), avg(lifetime_days)
from fully_observed_scooters
group by 1,2
order by 3 desc
;

create index spl_index on scooter_position_logs(city, provider, scooter_id)


CREATE MATERIALIZED VIEW city_providers AS (
select provider, city, min(date(timestamp)) cp_min_date, max(date(timestamp)) cp_max_date
from scooter_position_logs
where provider <> 'lime'
group by 1,2);


create materialized view scooters as (
select distinct
    city, provider, scooter_id,
    first_value(date(timestamp)) over win scooter_first_seen,
    last_value(date(timestamp)) over win scooter_last_seen,
    last_value(cast (location as varchar(64))) over win last_location
    from scooter_position_logs
where provider <> 'lime'
WINDOW win as (partition by city, provider, scooter_id order by timestamp RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )
);

select
provider, city, cp_min_date, cp_max_date,
count(*) scooters,
sum(case when cp_max_date - scooter_last_seen > 14 then 1 else 0 end) seen_death,
sum(case when scooter_first_seen - cp_min_date > 7 then 1 else 0 end) seen_birth,
sum(case when scooter_first_seen - cp_min_date > 7 and cp_max_date - scooter_last_seen > 14 then 1 else 0 end) seen_full_life,
sum(case when scooter_first_seen - cp_min_date <= 1 and cp_max_date - scooter_last_seen <= 1 then 1 else 0 end) scooter_all_period,
avg(scooter_last_seen - scooter_first_seen) avg_lifetime_days,
avg(case when scooter_first_seen - cp_min_date > 7 and cp_max_date - scooter_last_seen > 14 then scooter_last_seen - scooter_first_seen else null end) avg_lifetime_days_seen_full_life
from scooters
join city_providers using (city, provider)
where city = 'Berlin'
group by 1,2,3,4
having count(*) > 10
order by 5 desc;





select
scooters.*, scooter_last_seen - scooter_first_seen as lifetime_days
from
scooters join city_providers using (city, provider)
where  cp_max_date - scooter_last_seen > 21 and scooter_first_seen - cp_min_date > 7;

select * from scooter_position_logs where city = 'Berlin' and provider = 'Circ' and scooter_id = '' and timestamp = ''
