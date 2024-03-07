
--Import wild_animal.csv into cassandra server into a keyspace named wildlife and a table named animals

--cqlsh  -u cassandra -p cassandra 

CREATE KEYSPACE wildlife   
WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

use wildlife; 

CREATE TABLE animals(
id text PRIMARY KEY,
class text,
diet text,
latin_name text
);

COPY wildlife.animals (id,class,diet,latin_name) FROM 'wild_animal.csv' WITH DELIMITER=',' AND HEADER=TRUE;

--Write a cql query to count the number of rows in the animals table

SELECT count(*) FROM animals;

--Create an index for the name column in the animals table using cql

CREATE INDEX index_name ON animals (latin_name);

--Write a cql query to count the number of animals that have a herbivore diet.

UPDATE animals SET diet  = 'Herbivore' WHERE id IN ( '7', '4');
select count(*) from animals WHERE diet = 'Herbivore' ALLOW FILTERING;

