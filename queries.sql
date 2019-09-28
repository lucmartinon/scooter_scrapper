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
where provider != 'lime'
group by 1,2,3
) a
group by 1,2;
