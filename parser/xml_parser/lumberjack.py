#!/usr/bin/env python
# coding: utf-8

##Imports 
from lxml import etree
import subprocess
from typing import Dict, List


class Lumberjack:
    """docstring for Lumberjack"""
    def __init__(self, fname: str, interface=None: object, cols=None: List, unit=None: List, validation_file=None: str, repeats=None: Dict[str,bool]) -> None:
        self.interface = interface #expects the interface object
        self.filename = fname #expects a path
        self.columns = cols #expects a list of names (str)
        self.unit = unit #expects a list of main units        
        self.validation_file = validation_file #expects some DTD
        self.repeats = repeats

    def get_tree(self) -> None:
            try:
                self.tree = etree.iterparse(self.filename, tag=self.unit, recover=True, huge_tree=True)
            except:
                print("Error forming iterparse tree")
                pass

    def write_stream(self) -> None:
        """Stream writer for a stream of XML inputs"""

        ##To be able to handle repeated attributes, take in array from learner:
        repeat_dict = self.repeats #Learner passes a dict of COLNAME, Bool(Repeat_Elements)

        #How many passed before commit?
        counter = 0 
        #Commit variable:
        n = 0

        #If we have any repeated rows, create a serial uid and check for repeat cols
        if any(repeat_dict.values())>0:
            uuid = 0
        
            for event, element in self.tree:
                query = {}
                query['uuid'] = uuid
                for child in element:
                    if repeat_dict[child.tag]==1:
                        #We write a separate table insert.
                        query_extra = {
                            'uuid': uuid
                        }
                        try:
                            query_extra[child.tag] = child.text
                        except:
                            query_extra[child.tag] = ''
                        x.db_inter.insert_row(table_name_+child.tag, query_extra) #fix table naming!!!
                    else:
                        #We go with the regular.
                        try:
                            query[child.tag] = child.text
                        except:
                            query[child.tag] = ''
                        x.db_inter.insert_row(table_name, query)
                element.clear()
                uuid += 1
                counter += 1
                if counter == n:
                    x.db_inter.commit()
                    counter = 0
        else:
            #We proceed as normal - structured variety
            for event, element in self.tree:
                query = {}

                for child in element:
                    try:
                        query[child.tag] = child.text
                    except:
                        query[child.tag] = ''
                element.clear()
                x.db_inter.insert_row(table_name, query)
                counter += 1
                if counter == n:
                    x.db_inter.commit()
                    counter = 0

    def write_blocks(self) -> None:
        """Block writer for an XML tree - no dynamic sizing"""

        ##Naive implementation without schema inference
        #infer_schema()

        ##If we have the columns specified
        if x.columns:
            manager = {x.columns[i]:[] for i in range(len(x.columns))}
            for event, element in self.tree:
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
            for event, element in self.tree:
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

