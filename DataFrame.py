import pandas


class DataFrame:
    def __init__(self, name_file):
        try:
            self.dataframe = pandas.read_csv(name_file)
            self.file_exists = True
        except FileNotFoundError:
            self.file_exists = False
