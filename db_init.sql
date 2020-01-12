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

create index scooter_index on scooter_position_logs (city, provider, scooter_id);



