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



-- cooters dead since 14 days
with
spl as (select provider, city, scooter_id, timestamp, location from scooter_position_logs where provider <> 'lime' and city = 'Berlin'),

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
)


select
scooters.*
from
scooters join city_providers using (city, provider)
where  cp_max_date - scooter_last_seen > 14
;



