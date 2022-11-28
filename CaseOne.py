import matplotlib.pyplot as plt
from matplotlib import gridspec
from cubes import PointCut, Cell


class CaseOne:
    def __init__(self, browser, connect):
        self.browser = browser
        self.connect = connect
        self.neighborhoods = []
        self.number_rooms = []
        self.percentages = []

    @staticmethod
    def select_statement(select, table, columns, values):
        statement = 'SELECT ' + select + ' FROM ' + table + ' WHERE '
        n = len(columns)
        for i in range(n - 1):
            statement += columns[i] + ' = ' + values[i] + ' AND '
        return statement + columns[n - 1] + ' = ' + values[n - 1]

    def view(self):
        fig = plt.figure(figsize=(12, 8))
        gs0 = gridspec.GridSpec(1, 1, figure=fig)
        gs01 = gs0[0].subgridspec(5, 5)
        ax = fig.add_subplot(gs01[:, :-1])
        wedges, texts = ax.pie(self.number_rooms, textprops=dict(color="w"))
        ax.legend(wedges, self.neighborhoods, title='Vecindarios', loc='center right', bbox_to_anchor=(1.4, 0.5))
        plt.suptitle("Distribución de alojamientos en Lisboa")
        plt.show()

    def process(self, country, year):
        statement = self.select_statement('city_dimension.id, city_dimension.city',
                                          'country_dimension, city_dimension',
                                          ['country_dimension.country', 'city_dimension.country_id'],
                                          ['\'' + country + '\'', 'country_dimension.id'])
        table_cities = self.connect.select(statement)

        for city_info in table_cities:
            city_id = str(city_info[0])
            city = city_info[1]
            statement = self.select_statement('borough_dimension.id, borough_dimension.borough',
                                              'borough_dimension',
                                              ['borough_dimension.city_id'],
                                              [city_id])
            table_boroughs = self.connect.select(statement)

            for borough_info in table_boroughs:
                borough_id = str(borough_info[0])
                borough = borough_info[1]
                statement = self.select_statement('neighborhood_dimension.id, neighborhood_dimension.neighborhood',
                                                  'neighborhood_dimension',
                                                  ['neighborhood_dimension.borough_id'],
                                                  [borough_id])
                table_neighborhoods = self.connect.select(statement)

                for neighborhood_info in table_neighborhoods:
                    neighborhood = neighborhood_info[1]
                    cut = [PointCut('location', [country, city, borough, neighborhood], 'complete'),
                           PointCut('survey', [year], 'year')]
                    cell = Cell(self.browser.cube, cut)
                    result = self.browser.aggregate(cell)

                    self.neighborhoods.append(neighborhood)
                    self.number_rooms.append(result.summary['record_count'])

        total = sum(self.number_rooms)
        for i in range(len(self.neighborhoods)):
            percentage = str(round(self.number_rooms[i] * 100 / total, 2))
            if len(percentage) == 3:
                self.neighborhoods[i] = str(percentage) + '00% ' + self.neighborhoods[i]
            elif len(percentage) == 4:
                self.neighborhoods[i] = str(percentage) + '0% ' + self.neighborhoods[i]
            else:
                self.neighborhoods[i] = str(percentage) + '% ' + self.neighborhoods[i]
