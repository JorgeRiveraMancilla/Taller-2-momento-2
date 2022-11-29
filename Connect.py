import configparser
import psycopg2


class Connect:
    def __init__(self, db):
        try:
            config = configparser.ConfigParser()
            config.read('resources/config.ini')
            connection_info = config['database']
            self.db = connection_info[db]
            self.connect = psycopg2.connect(database=db,
                                            user=connection_info['user'],
                                            password=connection_info['password'],
                                            host=connection_info['server'],
                                            port=connection_info.getint('port'))
            self.is_connected = True
        except psycopg2.OperationalError:
            self.is_connected = False

    def execute(self, query):
        if not self.is_connected:
            raise psycopg2.OperationalError
        cursor = self.connect.cursor()
        cursor.execute(query)
        cursor.close()
        self.connect.commit()

    def select(self, query):
        if not self.is_connected:
            raise psycopg2.OperationalError
        cursor = self.connect.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result

    def create_tables(self):
        if not self.is_connected:
            raise psycopg2.OperationalError
        script = open('resources/' + self.db + '_schema.sql', mode='r').read()
        self.execute(script)

    def close(self):
        if not self.is_connected:
            raise psycopg2.OperationalError
        self.connect.close()
        del self.db
        self.is_connected = False
