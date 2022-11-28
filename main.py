import configparser
from DataFrame import DataFrame
from Connect import Connect
from RDB import RDB
from MDB import MDB
from Cube import Cube
from CaseOne import CaseOne
from CaseTwo import CaseTwo

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('resources/config.ini')
    paths = config['paths']

    if config['database'].getboolean('normalize'):
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
    connect_mdb = Connect('mdb')

    if config['database'].getboolean('case_one'):
        case_one = CaseOne(cube.browser, connect_mdb)
        case_one.process('Portugal', 2017)
        case_one.view()

    if config['database'].getboolean('case_two'):
        case_two = CaseTwo(cube.browser, connect_mdb)
        case_two.process('Portugal', 2017)
        case_two.view_violin()

    connect_mdb.close()
