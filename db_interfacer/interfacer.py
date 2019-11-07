import psycopg2 as pg # kill me this name


class DBInterfacer:

    def __init__(self):
        self.connection, self.cursor = self._establish_postgres_connection()
        

    @staticmethod
    def _establish_postgres_connection():
        try:
            connection = pg.connect(user="postgres", port="5432", database=db)  # do we have to create this before? The db, I mean.
            return connection, connection.cursor()
        except (Exception, psycopg2.Error) as error:
            raise Exception("We were unsuccessful in connecting to postgres. Are you sure you set up your database as requested?")  # think this works? We shall see.







            
            
