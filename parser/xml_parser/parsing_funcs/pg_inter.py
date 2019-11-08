#!/usr/bin/env python
# coding: utf-8

##Imports 
import psycopg2

# def pg_interfacer():
# 	"""Takes a block dictionary and inserts it into postgres"""

#     # for query in query_list:
#     #     cursor.execute(query)
#     ##Using this as the block writer format - PLACEHOLDER
#     with open(f'Script Executables/authorships_xml_to_sql{n}.sql', 'a', encoding='utf-8') as f:
#         for auth in x.findall('author')[:]:
#             try:
#                 at1 = str(auth.text)
#                 at = at1.translate(str.maketrans({"'":"-"})).replace("'","")
#             except:
#                 print('malformed author entry')
#                 print(f'could not parse at {auth}')
#             f.write(f"INSERT INTO Authorships (pubkey, author) VALUES (\'{a}\', \'{at}\');\n")
#         f.close()
#     return

def pg_streamer(query, cursor):
    """Streams input from the xml parser"""

    cursor.execute(query)
    connection.commit() #does removing the commit change the run-time?
    return

def pg_write_row(table, keys, row):
	self.cursor.execute(f"insert into {table} ({keys}) values {row}")


def pg_connector():
	"""Validates credentials for connecting to postgres"""
	
	try:
		connection = psycopg2.connect(user="postgres",port="5432",database=db)
		print("connection to postgres successful.")
		
	##Dumb exception catch - open this box later.
	except (Exception, psycopg2.Error) as error:
		print(error)
		return None
	return connection.cursor()

def pg_disconnector(connection):
	if(connection):
		cursor.close()
		connection.close()
		print("connection to postgres closed")
	return 