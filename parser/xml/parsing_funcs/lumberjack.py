#!/usr/bin/env python
# coding: utf-8

##Imports 
import subprocess
import pg_inter

def write_stream():
    """Stream writer for a stream of XML inputs"""
    ##Currently assuming a stable and balanced schema.
    
    pass

def write_blocks(tree):
    """Block writer for an XML tree - no dynamic sizing"""
    ##Naive implementation without schema inference
    #infer_schema()

    ##If we have the columns specified
    if x.columns:
        manager = {x.columns[i]:[] for i in range(len(x.columns))}
        for event, element in x.tree:
            for desired in x.columns:
                manager[desired].append(element.find(desired).text)
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
                    collector[child.tag].append(child.text)
                else:
                    collector[child.tag] = []
            element.clear()     
            #test the pg insertion block size optimality here
            #blocksize=1000
            #if len(collector[child.tag])+1%blocksize==0:         
            #for now - default behavior is to accumulate whole file into dictionary
            pg_inter.writer_function()




