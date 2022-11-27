from Connect import Connect
from MDB import MDB
from RDB import RDB
from Cube import Cube
from DataFrame import DataFrame
import configparser


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('resources/config.ini')
    paths = config['paths']

    dataframe_1 = DataFrame(paths['path_file_lisbon_2015'], config['default_values_lisbon_2015'])
    dataframe_1.normalize()
    dataframe_2 = DataFrame(paths['path_file_lisbon_2017'], config['default_values_lisbon_2017'])
    dataframe_2.normalize()
    dataframe_3 = DataFrame(paths['path_file_sao_paulo_2017'], config['default_values_sao_paulo_2017'])
    dataframe_3.normalize()

    if config['database'].getboolean('populate_rdb'):
        connect_rdb = Connect('rdb')
        connect_rdb.create_tables()

        rdb = RDB(dataframe_1.dataframe, connect_rdb)
        rdb.insert()
        rdb = RDB(dataframe_2.dataframe, connect_rdb)
        rdb.insert()
        rdb = RDB(dataframe_3.dataframe, connect_rdb)
        rdb.insert()

        connect_rdb.close()

    if config['database'].getboolean('populate_mdb'):
        connect_mdb = Connect('mdb')
        connect_mdb.create_tables()

        mdb = MDB(dataframe_1.dataframe, connect_mdb)
        mdb.insert()
        mdb = MDB(dataframe_2.dataframe, connect_mdb)
        mdb.insert()
        mdb = MDB(dataframe_3.dataframe, connect_mdb)
        mdb.insert()

        connect_mdb.close()

    cube = Cube()
    cube.test_cube()
