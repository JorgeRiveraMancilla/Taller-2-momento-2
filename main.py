from Connect import Connect
from DataFrame import DataFrame

drop_table_statements = [
    'DROP TABLE IF EXISTS rooms CASCADE;',
    'DROP TABLE IF EXISTS surveys CASCADE;',
    'DROP TABLE IF EXISTS hosts CASCADE;',
    'DROP TABLE IF EXISTS property_types CASCADE;',
    'DROP TABLE IF EXISTS types CASCADE;',
    'DROP TABLE IF EXISTS locations CASCADE;',
    'DROP TABLE IF EXISTS neighborhoods CASCADE;',
    'DROP TABLE IF EXISTS attractions CASCADE;',
    'DROP TABLE IF EXISTS boroughs CASCADE;',
    'DROP TABLE IF EXISTS cities CASCADE;',
    'DROP TABLE IF EXISTS countries CASCADE;']
create_table_statements = [
    'CREATE TABLE rooms'
    '(id SERIAL NOT NULL,'
    'overall_satisfaction float4,'
    'reviews int4 NOT NULL,'
    'accommodates int4,'
    'bedrooms float4,'
    'bathrooms int4,'
    'price float4 NOT NULL,'
    'minstay int4,'
    'name varchar(255),'
    'last_modified timestamp NOT NULL,'
    'latitude int4 NOT NULL,'
    'longitude float8 NOT NULL,'
    'id_location int4 NOT NULL,'
    'id_host int4 NOT NULL,'
    'id_property_type int4 NOT NULL,'
    'id_type int4 NOT NULL,'
    'id_survey int4 NOT NULL,'
    'PRIMARY KEY (id));',

    'CREATE TABLE surveys'
    '(id SERIAL NOT NULL,'
    'PRIMARY KEY (id));',

    'CREATE TABLE hosts'
    '(id SERIAL NOT NULL,'
    'PRIMARY KEY (id));',

    'CREATE TABLE property_types'
    '(id SERIAL NOT NULL,'
    'name varchar(255) NOT NULL,'
    'PRIMARY KEY (id));',

    'CREATE TABLE types'
    '(id SERIAL NOT NULL,'
    'name varchar(255) NOT NULL,'
    'PRIMARY KEY (id));',

    'CREATE TABLE locations'
    '(id SERIAL NOT NULL,'
    'id_neighborhood int4 NOT NULL,'
    'id_borough int4 NOT NULL,'
    'id_city int4 NOT NULL,'
    'id_country int4 NOT NULL,'
    'PRIMARY KEY (id));',

    'CREATE TABLE neighborhoods'
    '(id SERIAL NOT NULL,'
    'name varchar(255) NOT NULL,'
    'id_borough int4 NOT NULL,'
    'PRIMARY KEY (id));',

    'CREATE TABLE attractions'
    '(id SERIAL NOT NULL,'
    'name varchar(255) NOT NULL,'
    'id_neighborhood int4 NOT NULL,'
    'PRIMARY KEY (id));',

    'CREATE TABLE boroughs'
    '(id SERIAL NOT NULL,'
    'name varchar(255),'
    'id_city int4 NOT NULL,'
    'PRIMARY KEY (id));',

    'CREATE TABLE cities'
    '(id SERIAL NOT NULL,'
    'name varchar(255) NOT NULL,'
    'id_country int4 NOT NULL,'
    'PRIMARY KEY (id));',

    'CREATE TABLE countries'
    '(id SERIAL NOT NULL,'
    'name varchar(255),'
    'PRIMARY KEY (id));']

path_file_1 = 'resources/Lisbon18-03-2015.csv'
default_values_1 = {
    'survey_id': 1,
    'country': 'Portugal',
    'city': 'Lisbon',
    'borough': 'Lisbon',
    'last_modified': '2015-03-18 00:00:00.000'
}

if __name__ == '__main__':
    connect = Connect('project', 'postgres', 'copito')
    connect.create_tables(drop_table_statements, create_table_statements)

    dataframe = DataFrame(path_file_1, default_values_1)
    dataframe.normalize()


