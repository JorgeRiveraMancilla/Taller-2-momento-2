from cubes import Workspace
from cubes.compat import ConfigParser
import pandas
from datetime import date


from Connect import Connect

cube_drop_table_statements = [
    'DROP TABLE IF EXISTS facts_table CASCADE;',
    'DROP TABLE IF EXISTS survey_dimension CASCADE;',
    'DROP TABLE IF EXISTS type_dimension CASCADE;',
    'DROP TABLE IF EXISTS neighborhood_dimension CASCADE;',
    'DROP TABLE IF EXISTS borough_dimension CASCADE;',
    'DROP TABLE IF EXISTS city_dimension CASCADE;',
    'DROP TABLE IF EXISTS country_dimension CASCADE;']

cube_create_table_statements = [
    'CREATE TABLE facts_table '
    '(price float8 NOT NULL, '
    'minstay int4, '
    'accommodates int4, '
    'bedrooms int4, '
    'bathrooms int4, '
    'overall_satisfaction float8, '
    'reviews int4 NOT NULL, '
    'location_id int4 NOT NULL, '
    'type_id int4 NOT NULL, '
    'survey_id int4 NOT NULL, '
    'room_id int4 NOT NULL);',

    'CREATE TABLE survey_dimension'
    '(id SERIAL NOT NULL PRIMARY KEY, '
    '"date" date NOT NULL,'
    'day int4 NOT NULL,'
    'month int4 NOT NULL,'
    'year int4 NOT NULL);',

    'CREATE TABLE type_dimension'
    '(id SERIAL NOT NULL PRIMARY KEY,'
    'name varchar(255) NOT NULL);',

    'CREATE TABLE neighborhood_dimension'
    '(id SERIAL NOT NULL PRIMARY KEY,'
    'neighborhood varchar(255) NOT NULL,'
    'borough_id int4 NOT NULL);',

    'CREATE TABLE borough_dimension'
    '(id SERIAL NOT NULL PRIMARY KEY,'
    'borough varchar(255),'
    'city_id int4 NOT NULL);',

    'CREATE TABLE city_dimension'
    '(id SERIAL NOT NULL PRIMARY KEY,'
    'city varchar(255) NOT NULL,'
    'country_id int4 NOT NULL);',

    'CREATE TABLE country_dimension'
    '(id SERIAL NOT NULL PRIMARY KEY,'
    'country varchar(255));']


class Cube:
    def __init__(self, relational_connect, pwd):
        self.workspace = None
        self.browser = None
        self.rdb = relational_connect
        self.is_connected = False
        connect = Connect('airbnb', 'postgres', pwd)
        connect.create_tables(cube_drop_table_statements, cube_create_table_statements)
        connect.close()

    def load_data(self, pwd, dataframe):
        connect = Connect('airbnb', 'postgres', pwd)

        for survey_id in dataframe.dataframe['survey_id'].unique():
            table = connect.select('SELECT * FROM survey_dimension WHERE survey_dimension.id = ' + str(survey_id) + ';')
            if not table:
                date_str = dataframe.dataframe['last_modified'].iloc[-1].split(' ')[0]
                date_iso = date.fromisoformat(date_str)
                day = date_iso.strftime('%d')
                month = date_iso.strftime('%m')
                year = date_iso.strftime('%Y')
                insert_statement = 'INSERT INTO survey_dimension(id, date, day, month, year) VALUES (' \
                                   + str(survey_id) + ',\'' + str(date_str) + \
                                   '\',' + str(day) + ',' + str(month) + ',' + str(year) + ');'
                connect.execute(insert_statement)

        for room_type in dataframe.dataframe['room_type'].unique():
            table = connect.select(
                'SELECT * FROM type_dimension WHERE type_dimension.name = \'' + str(room_type) + '\';')
            if not table:
                insert_statement = 'INSERT INTO type_dimension(name) VALUES (\'' + str(room_type) + '\');'
                connect.execute(insert_statement)

        for country in dataframe.dataframe['country'].unique():
            table = connect.select(
                'SELECT * FROM country_dimension WHERE country_dimension.country = \'' + str(country) + '\';')
            if not table:
                insert_statement = 'INSERT INTO country_dimension(country) VALUES (\'' + str(country) + '\');'
                connect.execute(insert_statement)

        dataframe_aux = dataframe.dataframe[['country', 'city', 'borough', 'neighborhood']]

        for keys in dataframe_aux.groupby(by=['country', 'city']).groups.keys():
            country = keys[0]
            city = keys[1]
            table = connect.select('SELECT * FROM country_dimension WHERE country_dimension.country = \''
                                   + str(country) + '\';')
            id_country = table[0][0]

            table = connect.select('SELECT * FROM city_dimension WHERE city_dimension.country_id = ' + str(id_country)
                                   + ' AND city_dimension.city = \'' + str(city) + '\';')
            if not table:
                insert_statement = 'INSERT INTO city_dimension(city, country_id) ' \
                                   'VALUES (\'' + str(city) + '\', ' + str(id_country) + ');'
                connect.execute(insert_statement)

        for keys in dataframe_aux.groupby(by=['city', 'borough']).groups.keys():
            city = keys[0]
            borough = keys[1]
            table = connect.select('SELECT * FROM city_dimension WHERE city_dimension.city = \'' + str(city) + '\';')
            id_city = table[0][0]
            table = connect.select('SELECT * FROM borough_dimension WHERE borough_dimension.city_id = ' + str(id_city)
                                   + ' AND borough_dimension.borough = \'' + str(borough) + '\';')
            if not table:
                insert_statement = 'INSERT INTO borough_dimension(borough, city_id) ' \
                                   'VALUES (\'' + str(borough) + '\', ' + str(id_city) + ');'
                connect.execute(insert_statement)

        for keys in dataframe_aux.groupby(by=['borough', 'neighborhood']).groups.keys():
            borough = keys[0]
            neighborhood = keys[1]
            table = connect.select('SELECT * FROM borough_dimension WHERE borough_dimension.borough = \''
                                   + str(borough) + '\';')
            id_borough = table[0][0]
            table = connect.select('SELECT * FROM neighborhood_dimension WHERE neighborhood_dimension.borough_id = '
                                   + str(id_borough)
                                   + ' AND neighborhood_dimension.neighborhood = \'' + str(neighborhood) + '\';')
            if not table:
                insert_statement = 'INSERT INTO neighborhood_dimension(neighborhood, borough_id) ' \
                                   'VALUES (\'' + str(neighborhood) + '\', ' + str(id_borough) + ');'
                connect.execute(insert_statement)

        columns = dataframe.dataframe.columns.tolist()
        for index, row in dataframe.dataframe.iterrows():
            country = row[columns.index('country')]
            city = row[columns.index('city')]
            borough = row[columns.index('borough')]
            neighborhood = row[columns.index('neighborhood')]

            table = connect.select('SELECT * FROM country_dimension WHERE country_dimension.country = \''
                                   + str(country) + '\';')
            id_country = table[0][0]

            table = connect.select('SELECT * FROM city_dimension WHERE city_dimension.country_id = ' + str(id_country)
                                   + ' AND city_dimension.city = \'' + str(city) + '\';')
            id_city = table[0][0]

            table = connect.select('SELECT * FROM borough_dimension WHERE borough_dimension.city_id = ' + str(id_city)
                                   + ' AND borough_dimension.borough = \'' + str(borough) + '\';')
            id_borough = table[0][0]

            table = connect.select('SELECT * FROM neighborhood_dimension WHERE neighborhood_dimension.borough_id = '
                                   + str(id_borough)
                                   + ' AND neighborhood_dimension.neighborhood = \'' + str(neighborhood) + '\';')
            id_neighborhood = table[0][0]

            id_survey = row[columns.index('survey_id')]
            room_id = row[columns.index('room_id')]
            overall_satisfaction = row[columns.index('overall_satisfaction')]
            reviews = row[columns.index('reviews')]
            accommodates = row[columns.index('accommodates')]
            bedrooms = row[columns.index('bedrooms')]
            bathrooms = row[columns.index('bathrooms')]
            price = row[columns.index('price')]
            minstay = row[columns.index('minstay')]
            room_type = row[columns.index('room_type')]

            table = connect.select(
                'SELECT * FROM type_dimension WHERE type_dimension.name = \'' + str(room_type) + '\'')
            id_type = table[0][0]

            insert_statement = 'INSERT INTO facts_table(price, minstay, accommodates, bedrooms, bathrooms,' \
                               ' overall_satisfaction, reviews, location_id, type_id, survey_id, room_id) ' \
                               'VALUES (' + str(price) + \
                               ', ' + str(minstay) + \
                               ', ' + str(accommodates) + \
                               ', ' + str(bedrooms) + \
                               ', ' + str(bathrooms) + \
                               ', ' + str(overall_satisfaction) + \
                               ', ' + str(reviews) + \
                               ', ' + str(id_neighborhood) + \
                               ', ' + str(id_type) + \
                               ', ' + str(id_survey) + \
                               ', ' + str(room_id) + ');'
            connect.execute(insert_statement)

        connect.close()

    def load_cube(self):
        settings = ConfigParser()
        settings.read("./resources/slicer.ini")
        self.workspace = Workspace(config=settings)
        self.workspace.import_model("resources/airbnb.json")
        self.browser = self.workspace.browser("facts_table")

    def test_cube(self):
        result = self.browser.aggregate()
        count = result.summary["record_count"]
        print(count)
