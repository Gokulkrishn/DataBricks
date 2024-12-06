-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

USE f1_raw;

-- COMMAND ----------

DROP table if exists f1_raw.circuits;

-- COMMAND ----------

create table if not exists f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
using csv
options (path "/mnt/databrickudemy/raw/circuits.csv",header true)

-- COMMAND ----------

select * from circuits;

-- COMMAND ----------

DROP TABLE if EXISTS f1_raw.races;

-- COMMAND ----------

create table if not exists f1_raw.races (
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING, 
  url STRING
)
using csv
options (path "/mnt/databrickudemy/raw/races.csv", header "true")

-- COMMAND ----------

select * from races;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING)
USING json
OPTIONS(path "/mnt/formula1dl/raw/constructors.json")


-- COMMAND ----------


SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code STRING,
name STRUCT<forename: STRING, surname: STRING>,
dob DATE,
nationality STRING,
url STRING)
USING json
OPTIONS (path "/mnt/formula1dl/raw/drivers.json")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.results(
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed FLOAT,
statusId STRING)
USING json
OPTIONS(path "/mnt/databrickudemy/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
driverId INT,
duration STRING,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time STRING)
USING json
OPTIONS(path "/mnt/databrickudemy/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

select * from pit_stops;

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
raceId INT,
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/databrickudemy/raw/lap_times")

-- COMMAND ----------

select * from lap_times;

-- COMMAND ----------


DROP TABLE IF EXISTS f1_raw.qualifying;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
constructorId INT,
driverId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING,
qualifyId INT,
raceId INT)
USING json
OPTIONS (path "/mnt/databrickudemy/raw/qualifying", multiLine true)


-- COMMAND ----------

select * from qualifying;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('')

-- COMMAND ----------


