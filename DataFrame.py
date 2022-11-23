import pandas
import configparser

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
                try:
                    value = int(value)
                except ValueError:
                    pass
                self.dict[key] = value
        except FileNotFoundError:
            self.file_exists = False

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

    def insert_surveys(self, connect):
        for survey_id in self.dataframe['survey_id'].unique():
            table = connect.select('SELECT * FROM surveys WHERE surveys.id = ' + str(survey_id) + ';')
            if not table:
                insert_statement = 'INSERT INTO surveys(id) VALUES (' + str(survey_id) + ');'
                connect.execute(insert_statement)

    def insert_hosts(self, connect):
        for host_id in self.dataframe['host_id'].unique():
            table = connect.select('SELECT * FROM hosts WHERE hosts.id = ' + str(host_id) + ';')
            if not table:
                insert_statement = 'INSERT INTO hosts(id) VALUES (' + str(host_id) + ');'
                connect.execute(insert_statement)

    def insert_types(self, connect):
        for room_type in self.dataframe['room_type'].unique():
            table = connect.select('SELECT * FROM types WHERE types.name = \'' + str(room_type) + '\';')
            if not table:
                insert_statement = 'INSERT INTO types(name) VALUES (\'' + str(room_type) + '\');'
                connect.execute(insert_statement)

    def insert_property_types(self, connect):
        for property_type in self.dataframe['property_type'].dropna().unique():
            if property_type == 'NULL':
                continue
            table = connect.select('SELECT * FROM property_types WHERE property_types.name = \'' +
                                   str(property_type) + '\';')
            if not table:
                insert_statement = 'INSERT INTO property_types(name) VALUES (\'' + str(property_type) + '\');'
                connect.execute(insert_statement)

    def insert_countries(self, connect):
        for country in self.dataframe['country'].unique():
            table = connect.select('SELECT * FROM countries WHERE countries.name = \'' + str(country) + '\';')
            if not table:
                insert_statement = 'INSERT INTO countries(name) VALUES (\'' + str(country) + '\');'
                connect.execute(insert_statement)

    def insert_cities(self, connect):
        for keys in self.dataframe.groupby(by=['country', 'city']).groups.keys():
            country = keys[0]
            city = keys[1]
            table = connect.select('SELECT * FROM countries WHERE countries.name = \'' + str(country) + '\';')
            id_country = table[0][0]

            table = connect.select('SELECT * FROM cities WHERE cities.id_country = ' + str(id_country)
                                   + ' AND cities.name = \'' + str(city) + '\';')
            if not table:
                insert_statement = 'INSERT INTO cities(name, id_country) VALUES (\'' + str(city) + '\', ' + \
                                   str(id_country) + ');'
                connect.execute(insert_statement)

    def insert_boroughs(self, connect):
        for keys in self.dataframe.groupby(by=['city', 'borough']).groups.keys():
            city = keys[0]
            borough = keys[1]
            table = connect.select('SELECT * FROM cities WHERE cities.name = \'' + str(city) + '\';')
            id_city = table[0][0]
            table = connect.select('SELECT * FROM boroughs WHERE boroughs.id_city = ' + str(id_city)
                                   + ' AND boroughs.name = \'' + str(borough) + '\';')
            if not table:
                insert_statement = 'INSERT INTO boroughs(name, id_city) VALUES (\'' + str(borough) + '\', ' + \
                                   str(id_city) + ');'
                connect.execute(insert_statement)

    def insert_neighborhoods(self, connect):
        for keys in self.dataframe.groupby(by=['borough', 'neighborhood']).groups.keys():
            borough = keys[0]
            neighborhood = keys[1]
            table = connect.select('SELECT * FROM boroughs WHERE boroughs.name = \'' + str(borough) + '\';')
            id_borough = table[0][0]
            table = connect.select('SELECT * FROM neighborhoods WHERE neighborhoods.id_borough = ' + str(id_borough)
                                   + ' AND neighborhoods.name = \'' + str(neighborhood) + '\';')
            if not table:
                insert_statement = 'INSERT INTO neighborhoods(name, id_borough) VALUES (\'' + str(neighborhood) + \
                                   '\', ' + str(id_borough) + ');'
                connect.execute(insert_statement)

    def insert_locations(self, connect, id_country, id_city, id_borough, id_neighborhood):
        table = connect.select('SELECT * FROM locations WHERE locations.id_country = ' + str(id_country) +
                               ' AND locations.id_city = ' + str(id_city) +
                               ' AND locations.id_borough = ' + str(id_borough) +
                               ' AND locations.id_neighborhood = ' + str(id_neighborhood) + ';')
        if not table:
            insert_statement = 'INSERT INTO locations(id_neighborhood, id_borough, id_city, id_country) VALUES (' + \
                               str(id_neighborhood) + ', ' + str(id_borough) + ', ' \
                               + str(id_city) + ', ' + str(id_country) + ');'
            connect.execute(insert_statement)

    def get_id_location(self, connect, country, city, borough, neighborhood):
        table = connect.select('SELECT * FROM countries WHERE countries.name = \'' + country + '\';')
        id_country = table[0][0]
        table = connect.select('SELECT * FROM cities WHERE cities.name = \'' + city + '\';')
        id_city = table[0][0]
        table = connect.select('SELECT * FROM boroughs WHERE boroughs.name = \'' + borough + '\';')
        id_borough = table[0][0]
        table = connect.select('SELECT * FROM neighborhoods WHERE neighborhoods.name = \'' + neighborhood + '\';')
        id_neighborhood = table[0][0]
        self.insert_locations(connect, id_country, id_city, id_borough, id_neighborhood)
        table = connect.select('SELECT * FROM locations WHERE locations.id_country = ' + str(id_country) +
                               ' AND locations.id_city = ' + str(id_city) +
                               ' AND locations.id_borough = ' + str(id_borough) +
                               ' AND locations.id_neighborhood = ' + str(id_neighborhood) + ';')
        return table[0][0]

    def insert_rooms(self, connect):
        columns = self.dataframe.columns.tolist()
        for index, row in self.dataframe.iterrows():
            room_id = str(row[columns.index('room_id')])
            id_survey = str(row[columns.index('survey_id')])
            id_host = str(row[columns.index('host_id')])
            room_type = str(row[columns.index('room_type')])
            country = str(row[columns.index('country')])
            city = str(row[columns.index('city')])
            borough = str(row[columns.index('borough')])
            neighborhood = str(row[columns.index('neighborhood')])
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
            id_location = str(self.get_id_location(connect, country, city, borough, neighborhood))

            if name != 'NULL':
                name = '\'' + name + '\''

            if property_type == 'NULL':
                id_property_type = 'NULL'
            else:
                table = connect.select(
                    'SELECT * FROM property_types WHERE property_types.name = \'' + property_type + '\'')
                id_property_type = str(table[0][0])

            table = connect.select('SELECT * FROM types WHERE types.name = \'' + room_type + '\'')
            id_type = str(table[0][0])

            query = 'INSERT INTO rooms(id, overall_satisfaction, reviews, accommodates, bedrooms, bathrooms, price, ' \
                    'minstay, name, last_modified, latitude, longitude, id_location, id_host, id_property_type, ' \
                    'id_type, id_survey) VALUES (' + room_id + ', ' + overall_satisfaction + ', ' + reviews + ', ' + \
                    accommodates + ', ' + bedrooms + ', ' + bathrooms + ', ' + price + ', ' + minstay + ', ' + \
                    name + ', ' + last_modified + ', ' + latitude + ', ' + longitude + ', ' + id_location + ', ' + \
                    id_host + ', ' + id_property_type + ', ' + id_type + ', ' + id_survey + ');'
            connect.execute(query)

    def insert(self, connect):
        self.insert_surveys(connect)
        self.insert_hosts(connect)
        self.insert_types(connect)
        self.insert_property_types(connect)
        self.insert_countries(connect)
        self.insert_cities(connect)
        self.insert_boroughs(connect)
        self.insert_neighborhoods(connect)
        self.insert_rooms(connect)
