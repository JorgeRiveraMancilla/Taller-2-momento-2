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
