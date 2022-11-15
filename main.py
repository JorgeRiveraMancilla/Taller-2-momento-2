from Connect import Connect
from DataFrame import DataFrame


if __name__ == '__main__':
    connect = Connect('project', 'postgres', 'copito')

    default_values = {
        'survey_id': 1,
        'country': 'Portugal',
        'city': 'Lisbon',
        'borough': 'Lisbon',
        'last_modified': '2015-03-18 00:00:00.000'
    }
    dataframe = DataFrame('resources/Lisbon18-03-2015.csv', default_values)

    dataframe.normalize()
