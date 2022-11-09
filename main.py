from Connect import Connect
from DataFrame import DataFrame

if __name__ == '__main__':
    connect = Connect('project', 'postgres', 'copito')
    dataframe = DataFrame('resources/sao_paulo_2017.csv')

    if connect.is_connected and dataframe.file_exists:
        pass