import psycopg2


class Conn:
    def __init__(self):
        self.config = config = {
            'host': 'localhost',
            'user': 'lester',
            'port': 5432,
            'password': 'password',
            'database': 'stocker'
        }
        self.conn = psycopg2.connect(**self.config)
        self.cursor = self.conn.cursor()