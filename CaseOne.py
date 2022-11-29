import matplotlib.pyplot as plt
from matplotlib import gridspec
from cubes import PointCut, Cell


class CaseOne:
    def __init__(self, browser):
        self.browser = browser
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
        cut = [PointCut('location', [country], 'complete'),
               PointCut('survey', [year], 'year')]
        cell = Cell(self.browser.cube, cut)
        result = self.browser.aggregate(cell, drilldown=[('location', 'complete', 'neighborhood')])
        for row in result:
            self.neighborhoods.append(row["location.neighborhood"])
            self.number_rooms.append(row['record_count'])

        total = sum(self.number_rooms)
        for i in range(len(self.neighborhoods)):
            percentage = str(round(self.number_rooms[i] * 100 / total, 2))
            zeros = ['0' for i in range(len(percentage), 5)]
            self.neighborhoods[i] = str(percentage) + ''.join(zeros) + '% ' + self.neighborhoods[i]