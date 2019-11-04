"""
Usage: python parse_dblp.py
Ensure you are using python3 and in the same directory as handler.py and data_models.py.
To learn how to use sax and psycopg2 I used the available online documentation for both, stack overflow, and 
http://www.mclassen.de/articles/bulk-loading-xml-encoded-data-into-postgresql.html, which is an online tutorial
for converting xml encoded data to sql tables. I drew inspiration from the last website but all work is my own. 
"""

import xml.sax as sax
from handler import DocHandler as handler, DBWriter as writer
import psycopg2 as sqltalker

DSN = "dbname=dblp user=dblpuser"
tables=["Inproceedings", "Article", "Authorship"]

def create_tables(cur) -> None:
    cur.execute(f"drop table {', '.join(tables)};")
    cur.execute(f"CREATE TABLE {tables[0]} (pubkey text PRIMARY KEY, title text, booktitle text, year int);")
    cur.execute(f"CREATE TABLE {tables[1]} (pubkey text PRIMARY KEY, title text, journal text, year int);")
    cur.execute(f"CREATE TABLE {tables[2]} (pubkey text, author text, PRIMARY KEY (pubkey, author));")

if __name__=='__main__':
    with sqltalker.connect(DSN) as conn:
        with conn.cursor() as cur:
            create_tables(cur)
            parser = sax.make_parser()
            writer = writer(cur, conn)
            parser.setContentHandler(handler(30000, writer))
            parser.parse(open("dblp-2019-09-05.xml", "r"))
            writer.flush()





