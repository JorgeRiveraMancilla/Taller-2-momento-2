import matplotlib.pyplot as plt
from matplotlib import gridspec
import numpy as np
from cubes import PointCut, Cell
from CaseOne import CaseOne


class CaseTwo:
    def __init__(self, browser, connect):
        self.browser = browser
        self.connect = connect
        self.neighborhoods = []
        self.prices = []
        self.average_prices = []
        self.minimum_prices = []

    def view_violin(self):
        fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(10, 4))
        ax.violinplot(self.prices, showmeans=False, showmedians=True)
        ax.set_title('Distribución de precios')
        ax.yaxis.grid(True)
        ax.set_xticks([y + 1 for y in range(len(self.prices))], labels=self.neighborhoods)
        ax.set_xlabel('Vecindarios')
        ax.set_ylabel('Precio por noche')
        plt.show()

    def process(self, country, year):
        statement = CaseOne.select_statement('city_dimension.id, city_dimension.city',
                                             'country_dimension, city_dimension',
                                             ['country_dimension.country', 'city_dimension.country_id'],
                                             ['\'' + country + '\'', 'country_dimension.id'])
        table_cities = self.connect.select(statement)

        for city_info in table_cities:
            city_id = str(city_info[0])
            city = city_info[1]
            statement = CaseOne.select_statement('borough_dimension.id, borough_dimension.borough',
                                                 'borough_dimension',
                                                 ['borough_dimension.city_id'],
                                                 [city_id])
            table_boroughs = self.connect.select(statement)

            for borough_info in table_boroughs:
                borough_id = str(borough_info[0])
                borough = borough_info[1]
                statement = CaseOne.select_statement('neighborhood_dimension.id, neighborhood_dimension.neighborhood',
                                                     'neighborhood_dimension',
                                                     ['neighborhood_dimension.borough_id'],
                                                     [borough_id])
                table_neighborhoods = self.connect.select(statement)

                for neighborhood_info in table_neighborhoods:
                    neighborhood_prices = []
                    neighborhood = neighborhood_info[1]
                    cut = [PointCut('location', [country, city, borough, neighborhood], 'complete'),
                           PointCut('survey', [year], 'year')]
                    cell = Cell(self.browser.cube, cut)
                    facts = self.browser.facts(cell, ['price'])
                    for fact in facts:
                        neighborhood_prices.append(fact['price'])
                    self.neighborhoods.append(neighborhood[0:3])
                    self.prices.append(neighborhood_prices)
                    result = self.browser.aggregate(cell)
                    self.average_prices.append(result.summary['price_avg'])
                    self.minimum_prices.append(result.summary['price_min'])
