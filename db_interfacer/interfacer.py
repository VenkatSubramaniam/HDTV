#!/usr/bin/env python
# coding: utf-8

from typing import Dict
import psycopg2 as pg # kill me this name


class DBInterfacer:

    def __init__(self, uname: str, pword: str, db: str, port: str) -> None:
        self.connection, self.cursor = self._establish_postgres_connection(uname, pword, db, port)

    def _establish_postgres_connection(self, uname: str, pword: str, db: str, port: str) -> None:
        try:
            self._create_db(uname, pword, db, port)
            connection = pg.connect(user=uname, password=pword, port=port, database=db)  # do we have to create this before? The db, I mean.
            return connection, connection.cursor()
        except (Exception, psycopg2.Error) as error:
            raise Exception("We were unsuccessful in connecting to postgres. Are you sure you set up your database as requested?")  # think this works? We shall see.

    def commit(self) -> None:
        self.connection.commit()
    
    @staticmethod
    def _create_db(uname: str, pword: str, db: str, port: str) -> None:
        with pg.connect(user=uname, password=pword, port=port, database="postgres") as conn:
            conn.set_isolation_level(pg.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()

            cur.execute(f"select exists(SELECT datname FROM pg_catalog.pg_database WHERE lower(datname) = lower('{db}'))")
            if not cur.fetchone()[0]:
                cur.execute(f"create database {db};")

    def create_table(self, table: str, schema: Dict[str, str]) -> None:
        self.cursor.execute(f"create table {table} ({self._parse_schema(schema)});")
        self.commit()
    
    @staticmethod
    def _parse_schema(schema: Dict[str, str], key: bool=None) -> str:
        if key:
            pass #TODO
        return ", ".join([" ".join(item) for item in schema.items()])
    
    def insert_row(table:str, row: Dict[str, str]) -> None:
        keys = row.keys()
        self.cursor.execute(f"insert into {table} {keys} values ({[row[key] for key in keys]})")
