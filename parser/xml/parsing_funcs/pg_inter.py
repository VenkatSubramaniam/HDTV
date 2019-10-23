#!/usr/bin/env python
# coding: utf-8

##Imports 
import psycopg2

def pg_insert(filer, db = self.database):
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

def pg_interfacer():
	"""Takes a block dictionary and inserts it into postgres"""
    if x.tag=='article':
        with open(f'Script Executables/articles_xml_to_sql{n}.sql', 'a', encoding='utf-8') as f:
            f.write(f"INSERT INTO Articles (pubkey, title, journal, year) VALUES (\'{a}\', \'{b}\', \'{d}\', {c});\n")
            f.close()
    if x.tag=='inproceedings':
        with open(f'Script Executables/inproceedings_xml_to_sql{n}.sql', 'a', encoding='utf-8') as f:
            f.write(f"INSERT INTO Inproceedings (pubkey, title, booktitle, year) VALUES (\'{a}\', \'{b}\', \'{e}\', {c});\n")
            f.close()
    with open(f'Script Executables/authorships_xml_to_sql{n}.sql', 'a', encoding='utf-8') as f:
        for auth in x.findall('author')[:]:
            try:
                at1 = str(auth.text)
                at = at1.translate(str.maketrans({"'":"-"})).replace("'","")
            except:
                print('malformed author entry')
                print(f'could not parse at {auth}')
            f.write(f"INSERT INTO Authorships (pubkey, author) VALUES (\'{a}\', \'{at}\');\n")
        f.close()
    return

def pg_connector():
	pass