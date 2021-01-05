CREATE KEYSPACE IF NOT EXISTS Test
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE Test;

CREATE TABLE Users (
  companyName varchar,
  name varchar,
  phone int,
  street varchar,
  pets list<varchar>,
  PRIMARY KEY (companyName, name)
);

CREATE INDEX streetIndex ON Users (street);

CREATE TABLE MeteoData (
  stationName varchar, 
  measurement timestamp, 
  temperature double,
  pressure double,
  PRIMARY KEY (stationName, measurement)
);

CREATE TABLE MeteoData2 (
  stationName varchar, 
  mDay varchar,
  measurement timestamp, 
  temperature double,
  pressure double,
  PRIMARY KEY ((stationName, mDay), measurement)
);

CREATE TABLE PageViewCounts (
  url varchar,
  views counter,
  PRIMARY KEY(url)
);