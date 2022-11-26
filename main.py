from Connect import Connect
from Cube import Cube
from DataFrame import DataFrame
import configparser

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('resources/config.ini')
    create = config['database'].getboolean('create')
    paths = config['paths']

    connect = Connect('rdb')
    if create:
        connect.create_tables('rdb')
    cube = Cube(connect, create)

    dataframe_1 = DataFrame(paths['path_file_lisbon_2015'],
                            config['default_values_lisbon_2015'])
    dataframe_1.normalize()

    dataframe_2 = DataFrame(paths['path_file_lisbon_2017'],
                            config['default_values_lisbon_2017'])
    dataframe_2.normalize()

    dataframe_3 = DataFrame(paths['path_file_sao_paulo_2017'],
                            config['default_values_sao_paulo_2017'])
    dataframe_3.normalize()

    if create:
        dataframe_1.insert(connect)
        cube.load_data(dataframe_1)
        dataframe_2.insert(connect)
        cube.load_data(dataframe_2)
        dataframe_3.insert(connect)
        cube.load_data(dataframe_3)

    cube.load_cube()
    cube.test_cube()