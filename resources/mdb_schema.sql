DROP TABLE IF EXISTS facts_table CASCADE;
DROP TABLE IF EXISTS survey_dimension CASCADE;
DROP TABLE IF EXISTS type_dimension CASCADE;
DROP TABLE IF EXISTS neighborhood_dimension CASCADE;
DROP TABLE IF EXISTS borough_dimension CASCADE;
DROP TABLE IF EXISTS city_dimension CASCADE;
DROP TABLE IF EXISTS country_dimension CASCADE;

CREATE TABLE facts_table
(id SERIAL NOT NULL PRIMARY KEY,
price float8 NOT NULL,
minstay int4,
accommodates int4,
bedrooms int4,
bathrooms int4,
overall_satisfaction float8,
reviews int4 NOT NULL,
location_id int4 NOT NULL,
type_id int4 NOT NULL,
survey_id int4 NOT NULL,
room_id int4 NOT NULL);

CREATE TABLE survey_dimension
(id SERIAL NOT NULL PRIMARY KEY,
"date" date NOT NULL,
day int4 NOT NULL,
month int4 NOT NULL,
year int4 NOT NULL);

CREATE TABLE type_dimension
(id SERIAL NOT NULL PRIMARY KEY,
name varchar(255) NOT NULL);

CREATE TABLE neighborhood_dimension
(id SERIAL NOT NULL PRIMARY KEY,
neighborhood varchar(255) NOT NULL,
borough_id int4 NOT NULL);

CREATE TABLE borough_dimension
(id SERIAL NOT NULL PRIMARY KEY,
borough varchar(255),
city_id int4 NOT NULL);

CREATE TABLE city_dimension
(id SERIAL NOT NULL PRIMARY KEY,
city varchar(255) NOT NULL,
country_id int4 NOT NULL);

CREATE TABLE country_dimension
(id SERIAL NOT NULL PRIMARY KEY,
country varchar(255));

ALTER TABLE facts_table ADD CONSTRAINT FKfacts_tabl820611 FOREIGN KEY (survey_id) REFERENCES survey_dimension (id);
ALTER TABLE facts_table ADD CONSTRAINT FKfacts_tabl982935 FOREIGN KEY (type_id) REFERENCES type_dimension (id);

ALTER TABLE facts_table ADD CONSTRAINT FKfacts_tabl804217 FOREIGN KEY (location_id) REFERENCES neighborhood_dimension (id);
ALTER TABLE neighborhood_dimension ADD CONSTRAINT FKneighborho146082 FOREIGN KEY (borough_id) REFERENCES borough_dimension (id);
ALTER TABLE borough_dimension ADD CONSTRAINT FKborough_di512403 FOREIGN KEY (city_id) REFERENCES city_dimension (id);
ALTER TABLE city_dimension ADD CONSTRAINT FKcity_dimen882122 FOREIGN KEY (country_id) REFERENCES country_dimension (id);
