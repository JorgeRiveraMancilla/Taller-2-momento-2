from cubes import Workspace, PointCut, Cell
from cubes.compat import ConfigParser
import pandas
from datetime import date
from Connect import Connect


class Cube:
    def __init__(self, relational_connect, create):
        self.workspace = None
        self.browser = None
        self.rdb = relational_connect
        self.is_connected = False
        if create:
            connect = Connect('mdb')
            connect.create_tables('mdb')
            connect.close()

    def load_data(self, dataframe):
        connect = Connect('mdb')

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
        settings.read("./resources/config.ini")
        self.workspace = Workspace(config=settings)
        self.workspace.import_model("resources/airbnb.json")
        self.browser = self.workspace.browser("facts_table")

    def test_cube(self):
        result = self.browser.aggregate()
        count = result.summary["record_count"]
        print(count)
        cut = [
            PointCut("location", ["Brazil"], "country"),
            PointCut("survey", [2017], "year")
        ]
        cell = Cell(self.browser.cube, cut)
        result = self.browser.aggregate(cell)
        fields = ["overall_satisfaction", "reviews", "price"]
        facts = self.browser.facts(cell, fields)
        for fact in facts:
            print(fact)
        count = result.summary["record_count"]
        print(count)
        cut = [
            PointCut("location", ["Portugal"], "country"),
            PointCut("survey", [2017], "year")
        ]
        cell = Cell(self.browser.cube, cut)
        result = self.browser.aggregate(cell)
        count = result.summary["record_count"]
        print(count)
