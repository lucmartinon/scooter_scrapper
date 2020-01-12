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

create  view scooter_summary as (
select
provider, city, scooter_id,
scooter_first_seen,
scooter_last_seen,
(cp_max_date - scooter_last_seen > 14) seen_death,
(scooter_first_seen - cp_min_date > 3) seen_birth,
case when cp_max_date - scooter_last_seen > 14 then scooter_last_seen  end  death_date,
case when scooter_first_seen - cp_min_date > 3 then scooter_first_seen end birth_date,
(scooter_last_seen - scooter_first_seen) observed_life_days,
case when cp_max_date - scooter_last_seen > 14 then last_location end last_location_before_death
from scooters
join city_providers using (city, provider)
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
group by 1,2,3,4
having count(*) > 10
order by 5 desc;





select
scooters.*, scooter_last_seen - scooter_first_seen as lifetime_days
from
scooters join city_providers using (city, provider)
where  cp_max_date - scooter_last_seen > 21 and scooter_first_seen - cp_min_date > 7;

select * from scooter_position_logs where city = 'Berlin' and provider = 'Circ' and scooter_id = '' and timestamp = ''


psql scooter_scrapper -c "COPY (select * from scooter_position_logs where date(timestamp) = '2019-12-21') TO stdout DELIMITER ',' CSV HEADER" | gzip > /tmp/dec_21.csv.gz

psql scooter_scrapper -c "COPY (select * from scooter_position_logs where date(timestamp) between '2019-10-01' and '2019-10-31') TO stdout DELIMITER ',' CSV HEADER" | gzip > /tmp/201910.csv.gz