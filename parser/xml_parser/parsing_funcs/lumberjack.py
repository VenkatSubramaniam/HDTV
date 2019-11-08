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

    ##To be able to handle repeated attributes, take in array from learner:
    repeat_array = x.repeats #Learner passes a dict of COLNAME, Bool(Repeat_Elements)

    #How many passed?
    counter = 0 

    #If we have any repeated rows, create a serial uid and check for repeat cols
    if any(repeat_array.values())>0:
        uuid = 0
    
        for event, element in x.tree:
            query = {}
            query['uuid'] = uuid
            for child in element:
                if repeat_array[child.tag]==1:
                    #We write a separate table insert.
                    query_extra = {
                        'uuid': uuid
                    }
                    try:
                        query_extra[child.tag] = child.text
                    except:
                        query_extra[child.tag] = ''
                    x.db_inter.insert(table_name_+child.tag, query_extra) #fix table naming!!!
                else:
                    #We go with the regular.
                    try:
                        query[child.tag] = child.text
                    except:
                        query[child.tag] = ''
                    x.db_inter.insert(table_name, query)
            element.clear()
            uuid += 1
            counter += 1
            if counter == n:
                x.db_inter.commit()
                counter = 0
    else:
        #We proceed as normal - structured variety
        for event, element in x.tree:
            query = {}

            for child in element:
                try:
                    query[child.tag] = child.text
                except:
                    query[child.tag] = ''
            element.clear()
            x.db_inter.insert(table_name, query)
            counter += 1
            if counter == n:
                x.db_inter.commit()
                counter = 0

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


##TO-DO
#COPY TO DATABASE

