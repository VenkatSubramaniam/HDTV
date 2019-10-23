import psycopg2

def insert_into_postgres_f(filer, db = self.database):
    """Connects to database as localhost and drops in all queries from previous file"""
    with open(filer,'r',encoding="utf-8",errors='ignore') as fillet:
        query_list = fillet.readlines()
        try:
            connection = psycopg2.connect(user="postgres",port="5432",database=db)
            cursor = connection.cursor()
            for query in query_list:
                cursor.execute(query)
            connection.commit()
        except (Exception, psycopg2.Error) as error:
            print(error)
        finally:
            if(connection):
                cursor.close()
                connection.close()
            fillet.close()
    return

def validate_pg_connection():
	pass