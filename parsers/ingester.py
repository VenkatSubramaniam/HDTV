#!/usr/bin/env python
# coding: utf-8

##Imports - try to pull off more dependencies by the end:
import multiprocessing
import os
import operator
import pytest
import sys
import time
from typing import Dict

#Internals
#from json_parser import jparser
#from structured_parser import sparser

from parsers.xml_parser import lumberjack

## Main ingestion object:
class Ingester:
    """take file and column labels and insert into postgresql"""
    def __init__(self, fname: str=None, ftype: str="xml", interface: object=None, cols: str=None, unit: str=None, validation_file: str=None, repeats: Dict={"keys":False}):

        self.interface = interface #expects the interface object
        self.filename = fname #expects a path
        self.columns = cols #expects a list of names (str)
        self.validation_file = validation_file #expects some DTD
        self.repeats = repeats
        # self.tree = None
        self.filetype = ftype
        if self.filetype=="lxml":
            lumberjack = Lumberjack(fname=self.fname, interface=self.interface, cols=self.cols, validation_file=self.validation_file, repeats=self.repeats)
            lumberjack.get_tree()
            lumberjack.write_stream(self)
        #encoding specification from learner
        #host specification
        
    filename = property(operator.attrgetter('_filename'))
    columns = property(operator.attrgetter('_columns'))
    validation_file = property(operator.attrgetter('_validation_file')) #not passing test
    unit = property(operator.attrgetter('_unit'))
    
    @filename.setter
    def filename(self, f):
        if not os.path.isfile(os.path.join(os.getcwd(),f)):
            #default behavior is for the tool to be called in the directory of the file.
            raise Exception("No file at given path")
        else:
            self._filename = f       
        
    @columns.setter
    def columns(self, c):
        if not c:
            self._columns = False
        else:
            if type(c)==list:
                self._columns = c
            else:
                self._columns = list(c)
                assert len(self._columns)==1, "pass a list of names or single string"
   
    @validation_file.setter
    def validation_file(self, vf):
        if not vf:
            self._validation_file = False
        else:
            if not os.path.isfile(os.path.join(os.getcwd(),f)):
                #default behavior is for the tool to be called in the directory of the file.
                raise Exception("No file at given path")
            else:
                self._validation_file = vf
            
    @unit.setter
    def unit(self, unit):
        if unit:
            #infer this later (for each set(base element) in the tree, proceed)
            assert type(unit)==str, "primary unit must be string" 
        self._unit = unit
