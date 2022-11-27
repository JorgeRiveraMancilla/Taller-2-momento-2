from cubes import Workspace, PointCut, Cell
from cubes.compat import ConfigParser


class Cube:
    def __init__(self):
        settings = ConfigParser()
        settings.read("./resources/config.ini")
        self.workspace = Workspace(config=settings)
        self.workspace.import_model("resources/airbnb.json")
        self.browser = self.workspace.browser("facts_table")

    def test_cube(self):
        result = self.browser.aggregate()
        count = result.summary["record_count"]
        print(count)
        cut = [
            PointCut("location", ["Brazil"], "country"),
            PointCut("survey", [2017], "year")
        ]
        cell = Cell(self.browser.cube, cut)
        result = self.browser.aggregate(cell)
        fields = ["overall_satisfaction", "reviews", "price"]
        facts = self.browser.facts(cell, fields)
        for fact in facts:
            print(fact)
        count = result.summary["record_count"]
        print(count)
        cut = [
            PointCut("location", ["Portugal"], "country"),
            PointCut("survey", [2017], "year")
        ]
        cell = Cell(self.browser.cube, cut)
        result = self.browser.aggregate(cell)
        count = result.summary["record_count"]
        print(count)
