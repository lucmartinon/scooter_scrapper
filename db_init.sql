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


insert into scooter_position_logs_2
    select city, provider, id as scooter_id, secondary_id, location, timestamp, battery_level, licence_plate from scooter_position_logs where provider = 'voi'



insert into scooter_position_logs_2
    select city, provider, id as scooter_id, secondary_id, point(location[1], location[0]), timestamp, battery_level, licence_plate from scooter_position_logs where provider != 'voi'
