import psycopg2


class Connect:
    def __init__(self, db_name, db_user, db_password):
        try:
            self.connect = psycopg2.connect(database=db_name, user=db_user, password=db_password)
            self.is_connected = True
        except psycopg2.OperationalError:
            self.is_connected = False

    def execute(self, query):
        cursor = self.connect.cursor()
        cursor.execute(query)
        cursor.close()
        self.connect.commit()

    def close(self):
        self.connect.close()
