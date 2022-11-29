import matplotlib.pyplot as plt
from matplotlib import gridspec
import numpy as np
from cubes import PointCut, Cell
from CaseOne import CaseOne


class CaseTwo:
    def __init__(self, browser):
        self.browser = browser
        self.neighborhoods = []
        self.prices = []

    def view(self):
        fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(10, 4))
        ax.violinplot(self.prices, showmeans=False, showmedians=True)
        ax.set_title('Distribución de precios')
        ax.yaxis.grid(True)
        ax.set_xticks([y + 1 for y in range(len(self.prices))], labels=self.neighborhoods)
        ax.set_xlabel('Vecindarios')
        ax.set_ylabel('Precio por noche')
        plt.show()

    def process(self, country, year):
        cut = [PointCut('location', [country], 'complete'),
               PointCut('survey', [year], 'year')]
        cell = Cell(self.browser.cube, cut)
        result = self.browser.aggregate(cell, drilldown=[('location', 'complete', 'neighborhood')])
        for row in result:
            neighborhood_prices = []
            cut = [PointCut('location',
                            [row["location.country"], row["location.city"],
                             row["location.borough"], row["location.neighborhood"]],
                            'complete'),
                   PointCut('survey', [year], 'year')]
            cell = Cell(self.browser.cube, cut)
            facts = self.browser.facts(cell, ['price'])
            for fact in facts:
                neighborhood_prices.append(fact['price'])
            self.neighborhoods.append(row["location.neighborhood"][0:3])
            self.prices.append(neighborhood_prices)
