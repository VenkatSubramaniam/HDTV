#!/usr/bin/env python
# coding: utf-8

##Imports 
from lxml import etree
from parsing_funcs import pg_inter
import subprocess

def write_stream(x):
    """Stream writer for a stream of XML inputs"""

    ##Get the postgres connector:
    connection = pg_inter.pg_connector(x)
    counter = 0

    ##Currently assuming a stable and balanced schema.
    for event, element in x.tree:
        query = []
        for child in element:
            query.append(child.tag)
            try:
                query.append(child.text)
            except:
                query.append('')
        element.clear()
        try: 
            pg_inter.pg_streamer(\
                query = f"INSERT INTO table {tuple(query[i] for i in range(len(query)) if i%2==0)} VALUES {tuple(query[i] for i in range(len(query)) if i%2==1)}",
                connection = connection
                )
        except Exception as err:
            print(err)
        finally:
            counter+=1
    pg_inter.pg_disconnector(connection)
    print(f"Streamed through {counter} rows.")

def write_blocks(x):
    """Block writer for an XML tree - no dynamic sizing"""

    ##Naive implementation without schema inference
    #infer_schema()

    ##If we have the columns specified
    if x.columns:
        manager = {x.columns[i]:[] for i in range(len(x.columns))}
        for event, element in x.tree:
            for desired in x.columns:
                try:
                    manager[desired].append(element.find(desired).text)
                except:
                    manager[desired].append('')
            element.clear() 
            #test the pg insertion block size optimality here
            #blocksize=1000
            #if len(manager[desired])+1%blocksize==0:
            #for now - default behavior is to accumulate whole file into dictionary    
            pg_inter.writer_function()    
            
    ##Otherwise we take it all        
    else:
        # collector = {inferred_schema[i]:[] for i in range(len(inferred_schema))}
        collector = {}
        for event, element in x.tree:
            for child in element:
                if child.tag in collector:
                    try:
                        collector[child.tag].append(child.text)
                    except:
                        collector[child.tag].append('')
                else:
                    collector[child.tag] = []
            element.clear()     
            #test the pg insertion block size optimality here
            #blocksize=1000
            #if len(collector[child.tag])+1%blocksize==0:         
            #for now - default behavior is to accumulate whole file into dictionary
            pg_inter.writer_function()




