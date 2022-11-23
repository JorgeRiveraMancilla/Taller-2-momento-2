from Connect import Connect
from Cube import Cube
from DataFrame import DataFrame
import configparser

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

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('resources/config.ini')
    create = config['database'].getboolean('create')
    paths = config['paths']

    connect = Connect('rdb')
    if create:
        connect.create_tables(drop_table_statements, create_table_statements)
    cube = Cube(connect, create)

    dataframe_1 = DataFrame(paths['path_file_lisbon_2015'],
                            config['default_values_lisbon_2015'])
    dataframe_1.normalize()

    dataframe_2 = DataFrame(paths['path_file_lisbon_2017'],
                            config['default_values_lisbon_2017'])
    dataframe_2.normalize()

    dataframe_3 = DataFrame(paths['path_file_sao_paulo_2017'],
                            config['default_values_sao_paulo_2017'])
    dataframe_3.normalize()

    if create:
        dataframe_1.insert(connect)
        cube.load_data(dataframe_1)
        dataframe_2.insert(connect)
        cube.load_data(dataframe_2)
        dataframe_3.insert(connect)
        cube.load_data(dataframe_3)

    cube.load_cube()
    cube.test_cube()