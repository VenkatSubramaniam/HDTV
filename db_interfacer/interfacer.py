from typing import Dict
import psycopg2 as pg # kill me this name


class DBInterfacer:

    def __init__(self) -> None:
        self.connection, self.cursor = self._establish_postgres_connection()

    @staticmethod
    def _establish_postgres_connection() -> None:
        try:
            connection = pg.connect(user="postgres", port="5432", database=db)  # do we have to create this before? The db, I mean.
            return connection, connection.cursor()
        except (Exception, psycopg2.Error) as error:
            raise Exception("We were unsuccessful in connecting to postgres. Are you sure you set up your database as requested?")  # think this works? We shall see.

    def commit(self) -> None:
        self.cursor.commit()
    
    def create_table(self, table: str, schema: Dict[str, str]) -> None:
        self.cursor.execute(f"create table {table} ({self._parse_schema(schema)});")
    
    @staticmethod
    def _parse_schema(schema: Dict[str, str], key: bool=None) -> str:
        if key:
            pass #TODO
        return ", ".join([" ".join(reversed(item)) for item in schema.items()])
    
    def insert_row(table:str, row: Dict[str, str]) -> None:
        keys = row.keys()
        self.cursor.execute(f"insert into {table} {keys} values ({[row[key] for key in keys]})")
