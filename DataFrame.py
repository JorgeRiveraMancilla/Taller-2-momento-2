import pandas


class DataFrame:
    dict = {
        'room_id': False,
        'survey_id': 'CONSTRUCTOR',
        'host_id': 'NULL',
        'room_type': 'NULL',
        'country': 'CONSTRUCTOR',
        'city': 'CONSTRUCTOR',
        'borough': 'CONSTRUCTOR',
        'neighborhood': False,
        'reviews': 'NULL',
        'overall_satisfaction': 'NULL',
        'accommodates': 'NULL',
        'bedrooms': 'NULL',
        'bathrooms': 'NULL',
        'price': 'NULL',
        'minstay': 'NULL',
        'name': 'NULL',
        'property_type': 'NULL',
        'last_modified': 'CONSTRUCTOR',
        'latitude': 'NULL',
        'longitude': 'NULL',
        'location': 'NULL'
    }

    def __init__(self, path_file, default_values):
        try:
            self.dataframe = pandas.read_csv(path_file)
            self.file_exists = True
            for key, value in default_values.items():
                self.dict[key] = value
        except FileNotFoundError:
            self.file_exists = False

    def append(self, df):
        self.dataframe = self.dataframe.append(df.dataframe)

    def normalize(self):
        if not self.file_exists:
            raise FileNotFoundError

        not_nan_columns = [key for key, value in self.dict.items() if not value]
        self.dataframe.dropna(axis=0, how='any', subset=not_nan_columns, inplace=True)

        for column, default_value in self.dict.items():
            if column in self.dataframe.columns:
                if not isinstance(default_value, bool):
                    self.dataframe.fillna(value={column: self.dict[column]}, inplace=True)
            else:
                self.dataframe = self.dataframe.assign(column=self.dict[column])
                self.dataframe.rename(columns={"column": column}, inplace=True)

    def insert(self, connect):
        for survey_id in self.dataframe['survey_id'].unique():
            table = connect.select('SELECT * FROM surveys WHERE surveys.id = ' + str(survey_id) + ';')
            if not table:
                insert_statement = 'INSERT INTO surveys(id) VALUES (' + str(survey_id) + ');'
                connect.execute(insert_statement)

        for host_id in self.dataframe['host_id'].unique():
            table = connect.select('SELECT * FROM hosts WHERE hosts.id = ' + str(host_id) + ';')
            if not table:
                insert_statement = 'INSERT INTO hosts(id) VALUES (' + str(host_id) + ');'
                connect.execute(insert_statement)

        for room_type in self.dataframe['room_type'].unique():
            table = connect.select('SELECT * FROM types WHERE types.name = \'' + str(room_type) + '\';')
            if not table:
                insert_statement = 'INSERT INTO types(name) VALUES (\'' + str(room_type) + '\');'
                connect.execute(insert_statement)

        for property_type in self.dataframe['property_type'].dropna().unique():
            if property_type == 'NULL':
                continue
            table = connect.select('SELECT * FROM property_types WHERE property_types.name = \'' +
                                   str(property_type) + '\';')
            if not table:
                insert_statement = 'INSERT INTO property_types(name) VALUES (\'' + str(property_type) + '\');'
                connect.execute(insert_statement)

        for country in self.dataframe['country'].unique():
            table = connect.select('SELECT * FROM countries WHERE countries.name = \'' + str(country) + '\';')
            if not table:
                insert_statement = 'INSERT INTO countries(name) VALUES (\'' + str(country) + '\');'
                connect.execute(insert_statement)

        dataframe_aux = self.dataframe[['country', 'city', 'borough', 'neighborhood']]

        for keys in dataframe_aux.groupby(by=['country', 'city']).groups.keys():
            country = keys[0]
            city = keys[1]
            table = connect.select('SELECT * FROM countries WHERE countries.name = \'' + str(country) + '\';')
            id_country = table[0][0]

            table = connect.select('SELECT * FROM cities WHERE cities.id_country = ' + str(id_country)
                                   + ' AND cities.name = \'' + str(city) + '\';')
            if not table:
                insert_statement = 'INSERT INTO cities(name, id_country) ' \
                                   'VALUES (\'' + str(city) + '\', ' + str(id_country) + ');'
                connect.execute(insert_statement)

        for keys in dataframe_aux.groupby(by=['city', 'borough']).groups.keys():
            city = keys[0]
            borough = keys[1]
            table = connect.select('SELECT * FROM cities WHERE cities.name = \'' + str(city) + '\';')
            id_city = table[0][0]
            table = connect.select('SELECT * FROM boroughs WHERE boroughs.id_city = ' + str(id_city)
                                   + ' AND boroughs.name = \'' + str(borough) + '\';')
            if not table:
                insert_statement = 'INSERT INTO boroughs(name, id_city) ' \
                                   'VALUES (\'' + str(borough) + '\', ' + str(id_city) + ');'
                connect.execute(insert_statement)

        for keys in dataframe_aux.groupby(by=['borough', 'neighborhood']).groups.keys():
            borough = keys[0]
            neighborhood = keys[1]
            table = connect.select('SELECT * FROM boroughs WHERE boroughs.name = \'' + str(borough) + '\';')
            id_borough = table[0][0]
            table = connect.select('SELECT * FROM neighborhoods WHERE neighborhoods.id_borough = ' + str(id_borough)
                                   + ' AND neighborhoods.name = \'' + str(neighborhood) + '\';')
            if not table:
                insert_statement = 'INSERT INTO neighborhoods(name, id_borough) ' \
                                   'VALUES (\'' + str(neighborhood) + '\', ' + str(id_borough) + ');'
                connect.execute(insert_statement)

        columns = self.dataframe.columns.tolist()
        for index, row in self.dataframe.iterrows():
            country = row[columns.index('country')]
            city = row[columns.index('city')]
            borough = row[columns.index('borough')]
            neighborhood = row[columns.index('neighborhood')]

            table = connect.select('SELECT * FROM countries WHERE countries.name = \'' + str(country) + '\';')
            id_country = table[0][0]

            table = connect.select('SELECT * FROM cities WHERE cities.name = \'' + str(city) + '\';')
            id_city = table[0][0]

            table = connect.select('SELECT * FROM boroughs WHERE boroughs.name = \'' + str(borough) + '\';')
            id_borough = table[0][0]

            table = connect.select('SELECT * FROM neighborhoods '
                                   'WHERE neighborhoods.name = \'' + str(neighborhood) + '\';')
            id_neighborhood = table[0][0]

            table = connect.select('SELECT * FROM locations WHERE locations.id_country = ' + str(id_country) +
                                   ' AND locations.id_city = ' + str(id_city) +
                                   ' AND locations.id_borough = ' + str(id_borough) +
                                   ' AND locations.id_neighborhood = ' + str(id_neighborhood) + ';')


            if not table:
                insert_statement = 'INSERT INTO locations(id_neighborhood, id_borough, id_city, id_country) ' \
                                   'VALUES (' + str(id_neighborhood) + ', ' + str(id_borough) + ', ' \
                                   + str(id_city) + ', ' + str(id_country) + ');'
                connect.execute(insert_statement)

                table = connect.select('SELECT * FROM locations ORDER BY locations.id DESC LIMIT 1')

            id_location = table[0][0]

            id_survey = row[columns.index('survey_id')]
            room_id = row[columns.index('room_id')]
            overall_satisfaction = row[columns.index('overall_satisfaction')]
            reviews = row[columns.index('reviews')]
            accommodates = row[columns.index('accommodates')]
            bedrooms = row[columns.index('bedrooms')]
            bathrooms = row[columns.index('bathrooms')]
            price = row[columns.index('price')]
            minstay = row[columns.index('minstay')]
            name = row[columns.index('name')].replace('\'', '')
            last_modified = row[columns.index('last_modified')]
            latitude = row[columns.index('latitude')]
            longitude = row[columns.index('longitude')]
            id_host = row[columns.index('host_id')]
            property_type = row[columns.index('property_type')]
            room_type = row[columns.index('room_type')]

            if name != 'NULL':
                name = '\'' + name + '\''

            if property_type == 'NULL':
                id_property_type = 'NULL'
            else:
                table = connect.select('SELECT * FROM property_types WHERE property_types.name = \'' +
                                       str(property_type) + '\'')
                id_property_type = table[0][0]

            table = connect.select('SELECT * FROM types WHERE types.name = \'' + str(room_type) + '\'')
            id_type = table[0][0]

            insert_statement = 'INSERT INTO rooms(id, overall_satisfaction, reviews, accommodates, bedrooms, ' \
                               'bathrooms, price, minstay, name, last_modified, latitude, longitude, id_location, ' \
                               'id_host, id_property_type, id_type, id_survey) ' \
                               'VALUES (' + str(room_id) + \
                               ', ' + str(overall_satisfaction) + \
                               ', ' + str(reviews) + \
                               ', ' + str(accommodates) + \
                               ', ' + str(bedrooms) + \
                               ', ' + str(bathrooms) + \
                               ', ' + str(price) + \
                               ', ' + str(minstay) + \
                               ', ' + str(name) + \
                               ', \'' + str(last_modified) + '\''\
                               ', ' + str(latitude) + \
                               ', ' + str(longitude) + \
                               ', ' + str(id_location) + \
                               ', ' + str(id_host) + \
                               ', ' + str(id_property_type) + \
                               ', ' + str(id_type) + \
                               ', ' + str(id_survey) + ');'
            connect.execute(insert_statement)