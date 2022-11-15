import psycopg2


class Connect:
    def __init__(self, db_name, db_user, db_password):
        try:
            self.connect = psycopg2.connect(database=db_name, user=db_user, password=db_password)
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

    def create_tables(self, drop_statements, create_statements):
        if not self.is_connected:
            raise psycopg2.OperationalError

        for i in range(len(drop_statements)):
            self.execute(drop_statements[i])
            self.execute(create_statements[i])

    def close(self):
        if not self.is_connected:
            raise psycopg2.OperationalError

        self.connect.close()
        self.is_connected = False
