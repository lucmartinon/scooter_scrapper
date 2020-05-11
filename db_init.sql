create database scooter_scrapper;

CREATE TABLE scooter_position_logs (
   city VARCHAR(100) NOT NULL,
   provider  VARCHAR(100) NOT NULL,
   scooter_id  VARCHAR(100) NULL,
   secondary_id VARCHAR(100) NULL,
   location POINT NOT NULL,
   timestamp TIMESTAMP NOT NULL,
   battery_level VARCHAR(50) NULL,
   licence_plate VARCHAR(50) NULL
);

create index scooter_index_alpha on scooter_position_logs_alpha (city, provider, scooter_id);

CREATE TABLE scooter_position_logs (
	battery_level VARCHAR(50) NULL,
	city VARCHAR(100) NOT NULL,
	scooter_id  VARCHAR(100) NULL,
	lat FLOAT8 NOT NULL,
	licence_plate VARCHAR(50) NULL,
	lng FLOAT8 NOT NULL,
	provider  VARCHAR(100) NOT NULL,
	secondary_id VARCHAR(100) NULL,
	timestamp TIMESTAMP NOT NULL
);

CREATE TABLE new_spl (
	battery_level VARCHAR(50) NULL,
	city VARCHAR(100) NOT NULL,
	scooter_id  VARCHAR(100) NULL,
	lat FLOAT8 NOT NULL,
	licence_plate VARCHAR(50) NULL,
	lng FLOAT8 NOT NULL,
	provider  VARCHAR(100) NOT NULL,
	secondary_id VARCHAR(100) NULL,
	timestamp TIMESTAMP NOT NULL
);

create index scooter_index_alpha on scooter_position_logs_alpha (city, provider, scooter_id);


INSERT INTO scooter_position_logs_alpha (battery_level,city,scooter_id,lat,licence_plate,lng,provider,secondary_id,timestamp)
SELECT battery_level,city,scooter_id, location[1]lat,licence_plate, location[0] lng,provider,secondary_id,timestamp FROM scooter_position_logs
where timestamp < '2019-11-01' and timestamp >= '2019-10-01';



-- on server creating user for observableHQ

CREATE USER observable WITH PASSWORD 'this is my very strong password';
GRANT CONNECT ON DATABASE scooter_scrapper TO observable;
GRANT USAGE ON SCHEMA public TO observable;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO observable;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO observable;


CREATE EXTENSION tablefunc;