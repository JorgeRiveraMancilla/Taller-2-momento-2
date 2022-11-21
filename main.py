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
    '(id int4 NOT NULL,'
    'overall_satisfaction float4,'
    'reviews int4 NOT NULL,'
    'accommodates int4,'
    'bedrooms float4,'
    'bathrooms int4,'
    'price float4 NOT NULL,'
    'minstay int4,'
    'name varchar(255),'
    'last_modified timestamp NOT NULL,'
    'latitude int4,'
    'longitude float8,'
    'id_location int4 NOT NULL,'
    'id_host int4 NOT NULL,'
    'id_property_type int4,'
    'id_type int4 NOT NULL,'
    'id_survey int4 NOT NULL);',

    'CREATE TABLE surveys'
    '(id SERIAL NOT NULL PRIMARY KEY);',

    'CREATE TABLE hosts'
    '(id SERIAL NOT NULL PRIMARY KEY);',

    'CREATE TABLE property_types'
    '(id SERIAL NOT NULL PRIMARY KEY,'
    'name varchar(255) NOT NULL);',

    'CREATE TABLE types'
    '(id SERIAL NOT NULL PRIMARY KEY,'
    'name varchar(255) NOT NULL);',

    'CREATE TABLE locations'
    '(id SERIAL NOT NULL PRIMARY KEY,'
    'id_neighborhood int4 NOT NULL,'
    'id_borough int4 NOT NULL,'
    'id_city int4 NOT NULL,'
    'id_country int4 NOT NULL);',

    'CREATE TABLE neighborhoods'
    '(id SERIAL NOT NULL PRIMARY KEY,'
    'name varchar(255) NOT NULL,'
    'id_borough int4 NOT NULL);',

    'CREATE TABLE attractions'
    '(id SERIAL NOT NULL PRIMARY KEY,'
    'name varchar(255) NOT NULL,'
    'id_neighborhood int4 NOT NULL);',

    'CREATE TABLE boroughs'
    '(id SERIAL NOT NULL PRIMARY KEY,'
    'name varchar(255),'
    'id_city int4 NOT NULL);',

    'CREATE TABLE cities'
    '(id SERIAL NOT NULL PRIMARY KEY,'
    'name varchar(255) NOT NULL,'
    'id_country int4 NOT NULL);',

    'CREATE TABLE countries'
    '(id SERIAL NOT NULL PRIMARY KEY,'
    'name varchar(255));']

path_file_1 = 'resources/Lisbon18-03-2015.csv'
default_values_1 = {
    'survey_id': 1,
    'country': 'Portugal',
    'city': 'Lisbon',
    'borough': 'Lisbon',
    'last_modified': '2015-03-18 00:00:00.000'
}

path_file_2 = 'resources/Lisbon27-07-2017.csv'
default_values_2 = {
    'survey_id': 2,
    'country': 'Portugal',
    'city': 'Lisbon',
    'borough': 'Lisbon',
    'last_modified': '2017-07-27 00:00:00.000'
}

path_file_3 = 'resources/SaoPaulo01-07-2017.csv'
default_values_3 = {
    'survey_id': 3,
    'country': 'Brazil',
    'city': 'Sao Paulo',
    'borough': 'Sao Paulo',
    'last_modified': '2017-07-01 00:00:00.000'
}

if __name__ == '__main__':
    connect = Connect('project', 'postgres', 'copito')
    connect.create_tables(drop_table_statements, create_table_statements)

    dataframe_1 = DataFrame(path_file_1, default_values_1)
    dataframe_1.normalize()
    dataframe_1.insert(connect)
    dataframe_2 = DataFrame(path_file_2, default_values_2)
    dataframe_2.normalize()
    dataframe_2.insert(connect)
    dataframe_3 = DataFrame(path_file_3, default_values_3)
    dataframe_3.normalize()
    dataframe_3.insert(connect)
