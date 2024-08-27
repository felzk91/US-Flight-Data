--Creating an empty table to store airport data.
CREATE TABLE airports(
iata varchar(10),
airport varchar(100),
city varchar(100),
state varchar(10),
country varchar(50),
lat float,
long float,
PRIMARY KEY (iata)
);

--Review imported data for airports.
SELECT * FROM airports

--Creating an empty table to store carrier data.
CREATE TABLE carriers(
Code varchar(10),
Description varchar(100),
PRIMARY KEY(Code)
)

--Review imported data for carriers.
SELECT * FROM carriers

--Creating an empty table to store plane-data.
CREATE TABLE plane_data(
tailnum varchar(50),
type varchar(50),
manufacturer varchar(50),
issue_date varchar(50),
model varchar(50),
status varchar(50),
aircraft_type varchar(50),
engine_type varchar(50),
year smallint
)


--Review imported data for plane data.
SELECT * FROM plane_data

--Creating an empty table to store flights data.
CREATE TABLE flights(
Year smallint,
Month smallint,
DayofMonth smallint,
DayOfWeek smallint,
DepTime smallint,
CRSDepTime smallint,
ArrTime smallint,
CRSArrTime smallint,
UniqueCarrier varchar(50),
FlightNum smallint,
TailNum varchar(50),
ActualElaspedTime smallint,
CRSElaspedTime smallint,
AirTime smallint,
ArrDelay smallint,
DepDelay smallint,
Origin varchar(50),
Dest varchar(50),
Distance smallint,
TaxiIn smallint,
TaxiOut smallint,
Cancelled smallint,
CancellationCode varchar(50),
Diverted smallint,
CarrierDelay smallint,
WeatherDelay smallint,
NasDelay smallint,
SecurityDelay smallint,
LateAircraftDelay smallint
)

--Review imported data for flights
SELECT DISTINCT Year FROM flights

--Creating a duplicate flights data to preserve the original raw data

CREATE TABLE flights_staging AS (SELECT * FROM flights);

--Check data for duplicates
WITH duplicate_cte AS
(
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY Year, Month, DayofMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime, UniqueCarrier, FlightNum, TailNum,
ActualElaspedTime, CRSElaspedTime, AirTime, ArrDelay, DepDelay, Origin, Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode,
Diverted, CarrierDelay, WeatherDelay, NasDelay, SecurityDelay, LateAircraftDelay
) AS row_num 
FROM flights
)
SELECT* FROM duplicate_cte
WHERE row_num > 1

--Creating an empty table to store updated table without duplicates.
CREATE TABLE flights_staging2(
Year smallint,
Month smallint,
DayofMonth smallint,
DayOfWeek smallint,
DepTime smallint,
CRSDepTime smallint,
ArrTime smallint,
CRSArrTime smallint,
UniqueCarrier varchar(50),
FlightNum smallint,
TailNum varchar(50),
ActualElaspedTime smallint,
CRSElaspedTime smallint,
AirTime smallint,
ArrDelay smallint,
DepDelay smallint,
Origin varchar(50),
Dest varchar(50),
Distance smallint,
TaxiIn smallint,
TaxiOut smallint,
Cancelled smallint,
CancellationCode varchar(50),
Diverted smallint,
CarrierDelay smallint,
WeatherDelay smallint,
NasDelay smallint,
SecurityDelay smallint,
LateAircraftDelay smallint,
row_num smallint
)

INSERT INTO flights_staging2
SELECT*,
ROW_NUMBER() OVER(
PARTITION BY Year, Month, DayofMonth, DayOfWeek, DepTime, CRSDepTime, ArrTime, CRSArrTime, UniqueCarrier, FlightNum, TailNum,
ActualElaspedTime, CRSElaspedTime, AirTime, ArrDelay, DepDelay, Origin, Dest, Distance, TaxiIn, TaxiOut, Cancelled, CancellationCode,
Diverted, CarrierDelay, WeatherDelay, NasDelay, SecurityDelay, LateAircraftDelay
) AS row_num 
FROM flights

DELETE
FROM flights_staging2
WHERE row_num > 1

--We remove rows that are not required for our analysis to enhance query performance.
ALTER TABLE flights_staging2
DROP COLUMN CancellationCode,
DROP COLUMN row_num,
DROP COLUMN TaxiIn,
DROP COLUMN TaxiOut;

--We add a new column called average delay that calculates the average of depdelay and arrdelay.
ALTER TABLE flights_staging2
ADD COLUMN average_delay FLOAT;

UPDATE flights_staging2
SET average_delay = (DepDelay + ArrDelay) / 2;

SELECT * FROM flights_staging2
LIMIT 10

--Creating a sample of the flights data.
CREATE TABLE sample(
Year smallint,
Month smallint,
DayofMonth smallint,
DayOfWeek smallint,
DepTime smallint,
CRSDepTime smallint,
ArrTime smallint,
CRSArrTime smallint,
UniqueCarrier varchar(50),
FlightNum smallint,
TailNum varchar(50),
ActualElaspedTime smallint,
CRSElaspedTime smallint,
AirTime smallint,
ArrDelay smallint,
DepDelay smallint,
Origin varchar(50),
Dest varchar(50),
Distance smallint,
Cancelled smallint,
Diverted smallint,
CarrierDelay smallint,
WeatherDelay smallint,
NasDelay smallint,
SecurityDelay smallint,
LateAircraftDelay smallint,
average_delay float
)

--We create a sample to speed up the cleaning process
INSERT INTO sample
SELECT * FROM flights_staging2
TABLESAMPLE BERNOULLI(5); 

--Converting Time columns into time format
ALTER TABLE flights_staging2
ADD COLUMN CRSDepTime_timeformat time

UPDATE flights_staging2
SET CRSDepTime_timeformat = CAST(CRSDepTime / 100 || ':' || CRSDepTime % 100 as time)
WHERE CRSDepTime IS NOT NULL

ALTER TABLE flights_staging2
ADD COLUMN CRSArrTime_timeformat time

UPDATE flights_staging2
SET CRSArrTime_timeformat = CAST(CRSArrTime / 100 || ':' || CRSArrTime % 100 as time)
WHERE CRSArrTime IS NOT NULL

ALTER TABLE flights_staging2
ADD COLUMN DepTime_timeformat time,
ADD COLUMN ArrTime_timeformat time

UPDATE flights_staging2
SET DepTime = DepTime - 2400
WHERE DepTime > 2400

UPDATE flights_staging2
SET ArrTime = ArrTime - 2400
WHERE ArrTime > 2400

--Review the number of columns with data entry error where the departure minute is more than 60.
SELECT DepTime FROM flights_staging2
WHERE DepTime%100 > 60

--Removing the rows with data entry error.
DELETE FROM flights_staging2
WHERE DepTime%100 >= 60

UPDATE flights_staging2
SET DepTime_timeformat = CAST(DepTime / 100 || ':' || DepTime % 100 as time)
WHERE DepTime IS NOT NULL

UPDATE flights_staging2
SET ArrTime_timeformat = CAST(ArrTime / 100 || ':' || ArrTime % 100 as time)
WHERE ArrTime IS NOT NULL

--Review newly created time columns
SELECT DepTime_timeformat, ArrTime_timeformat, CRSDepTime_timeformat, CRSArrTime_timeformat
FROM flights_staging2

--Drop existing time columns
ALTER TABLE flights_staging2
DROP COLUMN DepTime,
DROP COLUMN ArrTime,
DROP COLUMN CRSDepTime,
DROP COLUMN CRSArrTime

--Creating a new column Date
ALTER TABLE flights_staging2
ADD COLUMN FullDate DATE;

UPDATE flights_staging2
SET FullDate = MAKE_DATE(Year,Month,DayofMonth)

SELECT Year, Month, DayofMonth, FullDate
FROM flights_staging2

--Analysis 1.1: When is the best time to travel with minimum delay?
CREATE TABLE analysis_1_1 (
Hour smallint,
MeanDelay float
);

--Review Aggregated Table
SELECT EXTRACT(HOUR FROM CRSDepTime_timeformat) as Hour, AVG(average_delay) as MeanDelay
FROM flights_staging2
GROUP BY Hour;

--Update table to convert 12am to 00:00 instead of 24:00.
SELECT CRSDepTime_timeformat
FROM flights_staging2
WHERE CRSDepTime_timeformat >= '24:00'

UPDATE flights_staging2
SET CRSDepTime_timeformat = '00:00'
WHERE CRSDepTime_timeformat >= '24:00'

INSERT INTO analysis_1_1
SELECT EXTRACT(HOUR FROM CRSDepTime_timeformat) as Hour, AVG(average_delay) as MeanDelay
FROM flights_staging2
GROUP BY Hour;

SELECT * FROM analysis_1_1

--Analysis 1.2: Is the best time to travel consistent throughout the years?

SELECT year,
CASE
        WHEN EXTRACT(HOUR FROM CRSDepTime_timeformat) BETWEEN 5 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM CRSDepTime_timeformat) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM CRSDepTime_timeformat) BETWEEN 17 AND 20 THEN 'Evening'
        WHEN EXTRACT(HOUR FROM CRSDepTime_timeformat) BETWEEN 21 AND 23 THEN 'Night'
        ELSE 'Late Night'
    END AS TimeOfDay,
AVG(average_delay) AS MeanDelay
FROM flights_staging2
GROUP BY  year, TimeOfDay

CREATE TABLE analysis_1_2 (
year varchar(10),
TimeOfDay varchar(50),
MeanDelay float
);

INSERT INTO analysis_1_2
SELECT year,
CASE
        WHEN EXTRACT(HOUR FROM CRSDepTime_timeformat) BETWEEN 5 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM CRSDepTime_timeformat) BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM CRSDepTime_timeformat) BETWEEN 17 AND 20 THEN 'Evening'
        WHEN EXTRACT(HOUR FROM CRSDepTime_timeformat) BETWEEN 21 AND 23 THEN 'Night'
        ELSE 'Late Night'
    END AS TimeOfDay,
AVG(average_delay) AS MeanDelay
FROM flights_staging2
GROUP BY  year, TimeOfDay;

--Analysis 1.3: When is the best season to travel?
SELECT
CASE
		WHEN month BETWEEN 3 AND 5 THEN 'spring'
		WHEN month BETWEEN 6 and 8 THEN 'summer'
		WHEN month BETWEEN 9 and 11 THEN 'autumn'
		ELSE 'winter'
	END AS Season,
AVG(average_delay) AS MeanDelay
FROM flights_staging2
GROUP BY Season

CREATE TABLE analysis_1_3(
Season varchar(10),
MeanDelay float
);

INSERT INTO analysis_1_3
SELECT
CASE
		WHEN month BETWEEN 3 AND 5 THEN 'spring'
		WHEN month BETWEEN 6 and 8 THEN 'summer'
		WHEN month BETWEEN 9 and 11 THEN 'autumn'
		ELSE 'winter'
	END AS Season,
AVG(average_delay) AS MeanDelay
FROM flights_staging2
GROUP BY Season

SELECT * FROM analysis_1_3

--Analysis 2: Does older aircrafts suffer more delays?

--Certain year columns contain null values. We will use the issue date year as the manufactured year.
SELECT CAST(SPLIT_PART(issue_date, '/', 3) AS INTEGER) as year_part, year
FROM plane_data
WHERE issue_date IS NOT NULL;

--Creating a new column to store the year of issue date
ALTER TABLE plane_data
ADD COLUMN issue_date_year smallint;

UPDATE plane_data
SET issue_date_year = CAST(SPLIT_PART(issue_date, '/', 3) AS INTEGER)
WHERE issue_date IS NOT NULL;

--Certain issue dates are recorded in a two-digit year format. To standardize the data, all years are converted to a four-digit format (yyyy).
UPDATE plane_data
SET issue_date_year = 
	CASE
			WHEN issue_date_year < 10 THEN issue_date_year+2000
			WHEN issue_date_year BETWEEN 10 AND 99 THEN issue_date_year+1900
			ELSE issue_date_year
		END;

--Check if there are still issue dates that are recorded in two-digit year format.
SELECT issue_date_year
FROM plane_data
WheRE issue_date_year < 100;

--Substitute year with issue_date_year for year with null or 0 value.
UPDATE plane_data
SET year = issue_date_year
WHERE year IS NULL AND year = 0;

--Check if there are any discrepencies in year column.
SELECT DISTINCT year FROM plane_data
ORDER BY year desc

SELECT f.Year - p.year AS PlaneAge, AVG(average_delay) AS MeanDelay
FROM flights_staging2 AS f
JOIN plane_data AS p
on f.TailNum = p.TailNum
GROUP BY PlaneAge;

SELECT *, f.Year - p.year AS PlaneAge
FROM flights_staging2 AS f
JOIN plane_data AS p
on f.TailNum = p.TailNum
WHERE f.Year - p.year NOT BETWEEN 0 AND 51


--Creating an empty table to store aggregated data for analysis 2.
CREATE TABLE analysis_2(
PlaneAge smallint,
MeanDelay float,
Frequency int
)

INSERT INTO analysis_2
SELECT f.Year - p.year AS PlaneAge, AVG(average_delay) AS MeanDelay, COUNT(*) AS Frequency
FROM flights_staging2 AS f
JOIN plane_data AS p
on f.TailNum = p.TailNum
GROUP BY PlaneAge;

SELECT * FROM analysis_2
ORDER BY PlaneAge

--Calculating the Pearson's correlation coefficient for plane age and average delay.
WITH analysis_2_cte AS
(SELECT f.average_delay AS Delay, f.Year - p.year AS PlaneAge
FROM flights_staging2 AS f
JOIN plane_data AS p
on f.TailNum = p.TailNum
WHERE f.Year - p.year NOT BETWEEN 0 AND 51)
SELECT corr(Delay, PlaneAge) AS Correlation
FROM analysis_4_cte


--Analysis 3: How does the popularity of flight routes change over time?
CREATE TABLE analysis_3
(
PathID varchar(50),
Year smallint,
Frequency integer,
RowNum integer,
FlightPercentage Float
)

INSERT INTO analysis_3
WITH YearlyFlightFreqCTE AS
(SELECT Year, COUNT(*) AS YearTotal
FROM flights_staging2
GROUP BY Year)
SELECT a.Pathid, a.Year, a.Frequency, ROW_NUMBER() OVER (PARTITION BY a.Year ORDER BY a.Year Desc, a.Frequency Desc) AS RowNum, a.Frequency / b.YearTotal AS FlightPercentage
FROM(
SELECT Pathid, Year, COUNT(*) AS Frequency
FROM flights_staging2
GROUP BY Year, Pathid) AS a
JOIN YearlyFlightFreqCTE AS b
ON a.Year = b.Year

--Analysis 4: How do the top diverted flight routes compare to the average diversion rate of all flights?
CREATE TABLE analysis_4 (
Diverted smallint,
Frequency integer
);

--Creating a contingency table that counts the number of diverted and non-diverted flights.
INSERT INTO analysis_4
SELECT Diverted, COUNT(*) as Frequency
FROM flights_Staging2
GROUP BY Diverted

--Creating a table to store the aggregated data for divert proportion and frequency by PathID.
CREATE TABLE analysis_4_1(
PathId Varchar(50),
DivertProportion Float,
Frequency integer,
Origin varchar(50),
Dest varchar(50),
OriginLat smallint,
OriginLong smallint,
DestLat smallint,
DestLong smallint
)

INSERT INTO analysis_4_1
SELECT f.PathID, f.DivertedProportion, f.Frequency, f.Origin, f.Dest, a1.lat AS OriginLat, a1.long AS OriginLong, a2.lat AS DestLat, a2.long AS DestLong
FROM (
SELECT Origin || 'to' || Dest AS PathId, AVG(Diverted) AS DivertedProportion, COUNT(*) AS Frequency, Origin, Dest
FROM flights_staging2
GROUP BY PathID, Origin, Dest
) AS f
JOIN airports AS a1
ON f.Origin = a1.iata
JOIN airports AS a2
ON f.Dest = a2.iata

--Creating a new column that stores the PathID (Origin - Dest Flight Path).
ALTER TABLE flights_staging2
ADD COLUMN PathID varchar(50);

UPDATE flights_staging2
SET PathID = Origin || ' to ' || Dest;

--Review flight information for PathID 'SUN to TWF'
SELECT * FROM flights_staging2
WHERE PathID = 'SUN to TWF'

--Analysis 5: Do flight delays cause a ripple effect across subsequent flights?
ALTER TABLE flights_staging2
ADD COLUMN depdatetime timestamp;

UPDATE flights_staging2
SET depdatetime = make_timestamp(
        EXTRACT(YEAR FROM fulldate)::int,
        EXTRACT(MONTH FROM fulldate)::int,
        EXTRACT(DAY FROM fulldate)::int,
        EXTRACT(HOUR FROM CRSDeptime_timeformat)::int,
        EXTRACT(MINUTE FROM CRSDeptime_timeformat)::int,
        EXTRACT(SECOND FROM CRSDeptime_timeformat)::int)

CREATE TABLE analysis_5 (
TailNum varchar(50),
PathID varchar(50),
CurrentFlightDelay integer,
SubsequentFlightDelay integer
)

INSERT INTO analysis_5
SELECT a.TailNum AS TailNum, a.PathID AS PathID, a.Average_Delay AS CurrentFlightDelay, b.DepDelay AS SubsequentFlightDelay FROM
(
SELECT ROW_NUMBER() OVER(PARTITION BY TailNum ORDER BY depdatetime ASC) AS RowNum, PathID, TailNum, Average_Delay, DepDelay, ArrDelay
FROM flights_staging2
) AS a
JOIN (
SELECT ROW_NUMBER() OVER(PARTITION BY TailNum ORDER BY depdatetime ASC) AS RowNum, PathID, TailNum, Average_Delay, DepDelay, ArrDelay
FROM flights_staging2
) AS b
ON a.TailNum = b.TailNum AND a.RowNum + 1 = b.RowNum
WHERE a.DepDelay >= 15 AND a.ArrDelay >= 15

SELECT * FROM analysis_5
WHERE currentflightdelay < 0

--Analysis 6: What is the popularity of carriers over the years?
SELECT UniqueCarrier, Year, SUM(COUNT(*)) OVER (PARTITION BY UniqueCarrier ORDER BY Year) AS CumulativeFrequency
FROM flights_staging2
GROUP BY UniqueCarrier, Year
ORDER BY UniqueCarrier, Year

CREATE TABLE analysis_6(
UniqueCarrier varchar(50),
Year smallint,
CumulativeFrequency integer
);

INSERT INTO analysis_6
SELECT UniqueCarrier, Year, SUM(COUNT(*)) OVER (PARTITION BY UniqueCarrier ORDER BY Year) AS CumulativeFrequency
FROM flights_staging2
GROUP BY UniqueCarrier, Year
ORDER BY UniqueCarrier, Year