class RDB:
    def __init__(self, dataframe, connect):
        self.dataframe = dataframe
        self.connect = connect

    @staticmethod
    def select_statement(table, columns, values):
        statement = 'SELECT * FROM ' + table + ' WHERE '
        n = len(columns)
        for i in range(n - 1):
            statement += table + '.' + columns[i] + ' = ' + values[i] + ' AND '
        return statement + table + '.' + columns[n - 1] + ' = ' + values[n - 1]

    @staticmethod
    def insert_statement(table, columns, values):
        start = 'INSERT INTO ' + table + '('
        end = ') VALUES ('
        n = len(columns)
        for i in range(n - 1):
            start += columns[i] + ', '
            end += values[i] + ', '
        return start + columns[n - 1] + end + values[n - 1] + ')'

    def insert_surveys(self):
        for survey_id in self.dataframe['survey_id'].unique():
            survey_id = str(survey_id)
            statement = self.select_statement('surveys', ['id'], [survey_id])
            table = self.connect.select(statement)
            if not table:
                statement = self.insert_statement('surveys', ['id'], [survey_id])
                self.connect.execute(statement)

    def insert_hosts(self):
        for host_id in self.dataframe['host_id'].unique():
            host_id = str(host_id)
            statement = self.select_statement('hosts', ['id'], [host_id])
            table = self.connect.select(statement)
            if not table:
                statement = self.insert_statement('hosts', ['id'], [host_id])
                self.connect.execute(statement)

    def insert_types(self):
        for room_type in self.dataframe['room_type'].unique():
            room_type = '\'' + str(room_type) + '\''
            statement = self.select_statement('types', ['name'], [room_type])
            table = self.connect.select(statement)
            if not table:
                statement = self.insert_statement('types', ['name'], [room_type])
                self.connect.execute(statement)

    def insert_property_types(self):
        for property_type in self.dataframe['property_type'].dropna().unique():
            property_type = '\'' + str(property_type) + '\''
            if property_type == '\'NULL\'':
                continue
            statement = self.select_statement('property_types', ['name'], [property_type])
            table = self.connect.select(statement)
            if not table:
                statement = self.insert_statement('property_types', ['name'], [property_type])
                self.connect.execute(statement)

    def insert_countries(self):
        for country in self.dataframe['country'].unique():
            country = '\'' + str(country) + '\''
            statement = self.select_statement('countries', ['name'], [country])
            table = self.connect.select(statement)
            if not table:
                statement = self.insert_statement('countries', ['name'], [country])
                self.connect.execute(statement)

    def insert_cities(self):
        for keys in self.dataframe.groupby(by=['country', 'city']).groups.keys():
            country = '\'' + str(keys[0]) + '\''
            city = '\'' + str(keys[1]) + '\''
            statement = self.select_statement('countries', ['name'], [country])
            table = self.connect.select(statement)
            id_country = str(table[0][0])
            statement = self.select_statement('cities', ['name', 'id_country'], [city, id_country])
            table = self.connect.select(statement)
            if not table:
                statement = self.insert_statement('cities', ['name', 'id_country'], [city, id_country])
                self.connect.execute(statement)

    def insert_boroughs(self):
        for keys in self.dataframe.groupby(by=['city', 'borough']).groups.keys():
            city = '\'' + str(keys[0]) + '\''
            borough = '\'' + str(keys[1]) + '\''
            statement = self.select_statement('cities', ['name'], [city])
            table = self.connect.select(statement)
            id_city = str(table[0][0])
            statement = self.select_statement('boroughs', ['name', 'id_city'], [borough, id_city])
            table = self.connect.select(statement)
            if not table:
                statement = self.insert_statement('boroughs', ['name', 'id_city'], [borough, id_city])
                self.connect.execute(statement)

    def insert_neighborhoods(self):
        for keys in self.dataframe.groupby(by=['borough', 'neighborhood']).groups.keys():
            borough = '\'' + str(keys[0]) + '\''
            neighborhood = '\'' + str(keys[1]) + '\''
            statement = self.select_statement('boroughs', ['name'], [borough])
            table = self.connect.select(statement)
            id_borough = str(table[0][0])
            statement = self.select_statement('neighborhoods', ['name', 'id_borough'], [neighborhood, id_borough])
            table = self.connect.select(statement)
            if not table:
                statement = self.insert_statement('neighborhoods', ['name', 'id_borough'], [neighborhood, id_borough])
                self.connect.execute(statement)

    def insert_location(self, id_country, id_city, id_borough, id_neighborhood):
        statement = self.select_statement('locations',
                                          ['id_neighborhood', 'id_borough', 'id_city', 'id_country'],
                                          [id_neighborhood, id_borough, id_city, id_country])
        table = self.connect.select(statement)
        if not table:
            statement = self.insert_statement('locations',
                                              ['id_neighborhood', 'id_borough', 'id_city', 'id_country'],
                                              [id_neighborhood, id_borough, id_city, id_country])
            self.connect.execute(statement)

    def insert_rooms(self):
        columns = self.dataframe.columns.tolist()
        for index, row in self.dataframe.iterrows():
            room_id = str(row[columns.index('room_id')])
            id_survey = str(row[columns.index('survey_id')])
            id_host = str(row[columns.index('host_id')])
            room_type = str(row[columns.index('room_type')])
            country = '\'' + str(row[columns.index('country')]) + '\''
            city = '\'' + str(row[columns.index('city')]) + '\''
            borough = '\'' + str(row[columns.index('borough')]) + '\''
            neighborhood = '\'' + str(row[columns.index('neighborhood')]) + '\''
            reviews = str(row[columns.index('reviews')])
            overall_satisfaction = str(row[columns.index('overall_satisfaction')])
            accommodates = str(row[columns.index('accommodates')])
            bedrooms = str(row[columns.index('bedrooms')])
            bathrooms = str(row[columns.index('bathrooms')])
            price = str(row[columns.index('price')])
            minstay = str(row[columns.index('minstay')])
            name = str(row[columns.index('name')].replace('\'', ''))
            property_type = str(row[columns.index('property_type')])
            last_modified = '\'' + str(row[columns.index('last_modified')]) + '\''
            latitude = str(row[columns.index('latitude')])
            longitude = str(row[columns.index('longitude')])

            statement = self.select_statement('countries', ['name'], [country])
            table = self.connect.select(statement)
            id_country = str(table[0][0])

            statement = self.select_statement('cities', ['name'], [city])
            table = self.connect.select(statement)
            id_city = str(table[0][0])

            statement = self.select_statement('boroughs', ['name'], [borough])
            table = self.connect.select(statement)
            id_borough = str(table[0][0])

            statement = self.select_statement('neighborhoods', ['name'], [neighborhood])
            table = self.connect.select(statement)
            id_neighborhood = str(table[0][0])

            self.insert_location(id_country, id_city, id_borough, id_neighborhood)

            statement = self.select_statement('locations',
                                              ['id_neighborhood', 'id_borough', 'id_city', 'id_country'],
                                              [id_neighborhood, id_borough, id_city, id_country])
            table = self.connect.select(statement)
            id_location = str(table[0][0])

            if name != 'NULL':
                name = '\'' + name + '\''

            if property_type == 'NULL':
                id_property_type = 'NULL'
            else:
                property_type = '\'' + property_type + '\''
                statement = self.select_statement('property_types', ['name'], [property_type])
                table = self.connect.select(statement)
                id_property_type = str(table[0][0])

            room_type = '\'' + room_type + '\''
            statement = self.select_statement('types', ['name'], [room_type])
            table = self.connect.select(statement)
            id_type = str(table[0][0])

            statement = self.insert_statement(
                'rooms',
                ['id', 'overall_satisfaction', 'reviews', 'accommodates', 'bedrooms', 'bathrooms', 'price', 'minstay',
                 'name', 'last_modified', 'latitude', 'longitude', 'id_location', 'id_host', 'id_property_type',
                 'id_type', 'id_survey'],
                [room_id, overall_satisfaction, reviews, accommodates, bedrooms, bathrooms, price, minstay, name,
                 last_modified, latitude, longitude, id_location, id_host, id_property_type, id_type, id_survey])
            self.connect.execute(statement)

    def insert(self):
        self.insert_surveys()
        self.insert_hosts()
        self.insert_types()
        self.insert_property_types()
        self.insert_countries()
        self.insert_cities()
        self.insert_boroughs()
        self.insert_neighborhoods()
        self.insert_rooms()
