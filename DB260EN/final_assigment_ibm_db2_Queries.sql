

----------------------------------------- Task 13-----------------------
SELECT a.stationid, b.trucktype,sum(a.wastecollected) AS total_waste_collected
FROM FACTTRIPS a
LEFT JOIN DIMTRUCK b
ON a.truckid=b.truckid
GROUP BY grouping sets(stationid,trucktype);

----------------------------------------- Task 14------------------------
SELECT b.year, c.city, a.stationid, sum(a.wastecollected) AS total_waste_collected
FROM FACTTRIPS a
LEFT JOIN DIMDATE  b
ON a.dateid =b.dateid 
LEFT JOIN DIMSTATION c 
ON a.stationid=c.stationid 
GROUP BY ROLLUP (b.year, c.city, a.stationid);

----------------------------------------- Task 15------------------------
SELECT b.year, c.city, a.stationid, avg(a.wastecollected) AS average_waste_collected
FROM FACTTRIPS a
LEFT JOIN DIMDATE   b
ON a.dateid =b.dateid 
LEFT JOIN DIMSTATION c 
ON a.stationid=c.stationid 
GROUP BY CUBE (b.year, c.city, a.stationid);




-- 16 Materialized Query Tables (MQT)
DROP TABLE max_waste_stats;

CREATE TABLE max_waste_stats AS 
  (SELECT 
    b.city,			
    a.stationid, 
    c.trucktype, 
    MAX(a.wastecollected) AS max_waste_collected   
    FROM FACTTRIPS a
        LEFT JOIN DIMSTATION b
        ON a.stationid=b.stationid 
            LEFT JOIN DIMTRUCK c
            ON a.truckid=c.truckid
    GROUP BY CUBE (b.city,a.stationid, c.trucktype)
    ) 
           
DATA INITIALLY DEFERRED
REFRESH DEFERRED 

;
-----------------------------
REFRESH TABLE max_waste_stats ;


-------------------IBM Cognos-----------------------------------

case extract(month, Date_ )
when 1 then 'January'
when 2 then 'February'
when 3 then 'March'
when 4 then 'April'
when 5 then 'May'
when 6 then 'June'
when 7 then 'July'
when 8 then 'August'
when 9 then 'September'
when 10 then 'October'
when 11 then 'November'
when 12 then 'December'
else 'error'
end

case extract(month, Date_ )
when 1 then '1|January'
when 2 then '2|February'
when 3 then '3|March'
when 4 then '4|April'
when 5 then '5|May'
when 6 then '6|June'
when 7 then '7|July'
when 8 then '8|August'
when 9 then '9|September'
when 10 then '10|October'
when 11 then '11|November'
when 12 then '12|December'
else 'error'
end





















DROP TABLE max_waste_stats;

CREATE TABLE max_waste_stats AS 
  (SELECT 
    b.city,			
    a.stationid, 
    c.trucktype, 
    MAX(a.wastecollected) AS max_waste_collected   
    FROM FACTTRIPS a
        LEFT JOIN DIMSTATION b
        ON a.stationid=b.stationid 
            LEFT JOIN DIMTRUCK c
            ON a.truckid=c.truckid
    GROUP BY CUBE (b.city,a.stationid, c.trucktype)
    ) 
           
DATA INITIALLY DEFERRED
REFRESH DEFERRED 
ENABLE QUERY OPTIMIZATION
MAINTAINED BY USER;
-----------------------------
REFRESH TABLE max_waste_stats ;

----------
-----------



CREATE TABLE max_waste_stats AS 
  (SELECT 
    b.city,			
    a.stationid, 
    c.trucktype, 
    MAX(a.wastecollected) AS max_waste_collected   
    FROM FACTTRIPS a
        LEFT JOIN DIMSTATION b
        ON a.stationid=b.stationid 
            LEFT JOIN DIMTRUCK c
            ON a.truckid=c.truckid
    GROUP BY CUBE (b.city,a.stationid, c.trucktype)
    ) 
           
DATA INITIALLY IMMEDIATE 
REFRESH DEFERRED 
DISABLE QUERY OPTIMIZATION
MAINTAINED BY USER;

CREATE MATERIALIZED VIEW max_waste_stats
AS SELECT 
b.city,			
a.stationid, 
c.trucktype, 
MAX(a.wastecollected) AS max_waste_collected   
FROM FACTTRIPS a
    LEFT JOIN DIMSTATION b
    ON a.stationid=b.stationid 
        LEFT JOIN DIMTRUCK c
        ON a.truckid=c.truckid
GROUP BY CUBE (b.city;,a.stationid, c.trucktype)
WITH DATA;