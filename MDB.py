from datetime import date
from RDB import RDB


class MDB:
    def __init__(self, dataframe, connect):
        self.dataframe = dataframe
        self.connect = connect

    def insert_surveys(self):
        for survey_id in self.dataframe['survey_id'].unique():
            survey_id = str(survey_id)
            statement = RDB.select_statement('survey_dimension', ['id'], [survey_id])
            table = self.connect.select(statement)
            if not table:
                date_str = self.dataframe['last_modified'].iloc[-1].split(' ')[0]
                date_iso = date.fromisoformat(date_str)
                day = str(date_iso.strftime('%d'))
                month = str(date_iso.strftime('%m'))
                year = str(date_iso.strftime('%Y'))
                statement = RDB.insert_statement('survey_dimension',
                                                 ['id', 'date', 'day', 'month', 'year'],
                                                 [survey_id, '\'' + date_str + '\'', day, month, year])
                self.connect.execute(statement)

    def insert_types(self):
        for room_type in self.dataframe['room_type'].unique():
            room_type = '\'' + str(room_type) + '\''
            statement = RDB.select_statement('type_dimension', ['name'], [room_type])
            table = self.connect.select(statement)
            if not table:
                statement = RDB.insert_statement('type_dimension', ['name'], [room_type])
                self.connect.execute(statement)

    def insert_countries(self):
        for country in self.dataframe['country'].unique():
            country = '\'' + str(country) + '\''
            statement = RDB.select_statement('country_dimension', ['country'], [country])
            table = self.connect.select(statement)
            if not table:
                statement = RDB.insert_statement('country_dimension', ['country'], [country])
                self.connect.execute(statement)

    def insert_cities(self):
        for keys in self.dataframe.groupby(by=['country', 'city']).groups.keys():
            country = '\'' + str(keys[0]) + '\''
            city = '\'' + str(keys[1]) + '\''
            statement = RDB.select_statement('country_dimension', ['country'], [country])
            table = self.connect.select(statement)
            id_country = str(table[0][0])
            statement = RDB.select_statement('city_dimension', ['city', 'country_id'], [city, id_country])
            table = self.connect.select(statement)
            if not table:
                statement = RDB.insert_statement('city_dimension', ['city', 'country_id'], [city, id_country])
                self.connect.execute(statement)

    def insert_boroughs(self):
        for keys in self.dataframe.groupby(by=['city', 'borough']).groups.keys():
            city = '\'' + str(keys[0]) + '\''
            borough = '\'' + str(keys[1]) + '\''
            statement = RDB.select_statement('city_dimension', ['city'], [city])
            table = self.connect.select(statement)
            id_city = str(table[0][0])
            statement = RDB.select_statement('borough_dimension', ['borough', 'city_id'], [borough, id_city])
            table = self.connect.select(statement)
            if not table:
                statement = RDB.insert_statement('borough_dimension', ['borough', 'city_id'], [borough, id_city])
                self.connect.execute(statement)

    def insert_neighborhoods(self):
        for keys in self.dataframe.groupby(by=['borough', 'neighborhood']).groups.keys():
            borough = '\'' + str(keys[0]) + '\''
            neighborhood = '\'' + str(keys[1]) + '\''
            statement = RDB.select_statement('borough_dimension', ['borough'], [borough])
            table = self.connect.select(statement)
            id_borough = str(table[0][0])
            statement = RDB.select_statement('neighborhood_dimension',
                                             ['neighborhood', 'borough_id'],
                                             [neighborhood, id_borough])
            table = self.connect.select(statement)
            if not table:
                statement = RDB.insert_statement('neighborhood_dimension',
                                                 ['neighborhood', 'borough_id'],
                                                 [neighborhood, id_borough])
                self.connect.execute(statement)

    def insert_rooms(self):
        columns = self.dataframe.columns.tolist()
        for index, row in self.dataframe.iterrows():
            id_survey = str(row[columns.index('survey_id')])
            room_id = str(row[columns.index('room_id')])
            overall_satisfaction = str(row[columns.index('overall_satisfaction')])
            reviews = str(row[columns.index('reviews')])
            accommodates = str(row[columns.index('accommodates')])
            bedrooms = str(row[columns.index('bedrooms')])
            bathrooms = str(row[columns.index('bathrooms')])
            price = str(row[columns.index('price')])
            minstay = str(row[columns.index('minstay')])
            room_type = '\'' + str(row[columns.index('room_type')]) + '\''
            country = '\'' + str(row[columns.index('country')]) + '\''
            city = '\'' + str(row[columns.index('city')]) + '\''
            borough = '\'' + str(row[columns.index('borough')]) + '\''
            neighborhood = '\'' + str(row[columns.index('neighborhood')]) + '\''
            statement = RDB.select_statement('country_dimension', ['country'], [country])
            table = self.connect.select(statement)
            id_country = str(table[0][0])
            statement = RDB.select_statement('city_dimension', ['country_id', 'city'], [id_country, city])
            table = self.connect.select(statement)
            id_city = str(table[0][0])
            statement = RDB.select_statement('borough_dimension', ['city_id', 'borough'], [id_city, borough])
            table = self.connect.select(statement)
            id_borough = str(table[0][0])
            statement = RDB.select_statement('neighborhood_dimension',
                                             ['borough_id', 'neighborhood'],
                                             [id_borough, neighborhood])
            table = self.connect.select(statement)
            id_neighborhood = str(table[0][0])
            statement = RDB.select_statement('type_dimension', ['name'], [room_type])
            table = self.connect.select(statement)
            id_type = str(table[0][0])
            statement = RDB.insert_statement(
                'facts_table',
                ['price', 'minstay', 'accommodates', 'bedrooms', 'bathrooms', 'overall_satisfaction', 'reviews',
                 'location_id', 'type_id', 'survey_id', 'room_id'],
                [price, minstay, accommodates, bedrooms, bathrooms, overall_satisfaction, reviews, id_neighborhood,
                 id_type, id_survey, room_id])
            self.connect.execute(statement)

    def insert(self):
        self.insert_surveys()
        self.insert_types()
        self.insert_countries()
        self.insert_cities()
        self.insert_boroughs()
        self.insert_neighborhoods()
        self.insert_rooms()
