DROP TABLE "MyDimDate";
DROP TABLE "MyDimWaste";
DROP TABLE  "MyDimZone";
DROP TABLE "MyFactTrips";

/*
dateid
date
day
weekday
weekdayname
month
monthname
quarter
quartername
year
*/

CREATE TABLE "MyDimDate" (
	dateid	integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    "date"  date NOT NULL,
    day  integer NOT NULL,
    Weekday    integer	 NOT NULL,
    Weekdayname	varchar(10)	 NOT NULL,
	"month"	 integer	 NOT NULL,
	monthname varchar(10) NOT NULL,
	Quarter	integer	 NOT NULL,
	Quartername	varchar(2)	 NOT NULL,
    "Year"	integer	 NOT NULL,

	CONSTRAINT "MyDimDate_pkey" PRIMARY KEY (dateid)
);

CREATE TABLE "MyDimWaste" (
	waste_id 	integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    waste_type	 varchar(10) 	 NOT NULL,
	CONSTRAINT "MyDimWaste_pkey" PRIMARY KEY (waste_id)
);

/*
zone_id
collection_zone
city
*/
CREATE TABLE "MyDimZone" (
	zone_id	integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    collection_zone	 varchar(10) 	NOT NULL,
    city	 varchar(50) 	NOT NULL,
	CONSTRAINT "MyDimZone_pkey" PRIMARY KEY (Zone_id)
);

CREATE TABLE "MyFactTrips" (
	trip_id	integer NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    dateid	integer	 NOT NULL,
    zone_id	integer	 NOT NULL,
    waste_id	integer	 NOT NULL,
    waste_collected	numeric(4, 2) NOT NULL,
    
	CONSTRAINT "MyFactTrips_pkey" PRIMARY KEY (trip_id),
    CONSTRAINT fk_dateid FOREIGN KEY(dateid)  REFERENCES "MyDimDate"(dateid),
    CONSTRAINT fk_zone_id FOREIGN KEY(zone_id)  REFERENCES "MyDimZone"(zone_id),
    CONSTRAINT fk_waste_id FOREIGN KEY(waste_id)  REFERENCES "MyDimWaste"(waste_id)
);

