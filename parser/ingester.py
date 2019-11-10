#!/usr/bin/env python
# coding: utf-8

##Imports - try to pull off more dependencies by the end:
import multiprocessing
import os
import operator
import pytest
import sys
import time
from typing import Dict, List

#Internals
#from json_parser import jparser
#from structured_parser import sparser
from parser.xml_parser import lumberjack

## Main ingestion object:
class Ingester:
    """take file and column labels and insert into postgresql"""
    def __init__(self, fname: str, ftype="xml": str, interface=None: object, cols=None: List, unit=None: List, validation_file=None: str, repeats={"keys":False}: Dict[str,bool]) -> None:
        self.interface = interface #expects the interface object
        self.repeats = repeats
        # self.tree = None
        self.filename = fname #expects a path
        self.columns = cols #expects a list of names (str)
        self.unit = unit #expects a list of main units
        self.validation_file = validation_file #expects some DTD
        ##        
        self.filetype = ftype
        if self.filetype=="lxml":
            Lumberjack = lumberjack(fname=self.fname, interface=self.interface, cols=self.cols, validation_file=self.validation_file, repeats=self.repeats)
            Lumberjack.get_tree()
            Lumberjack.write_stream(self)
        #encoding specification from learner
        #host specification
        
    filename = property(operator.attrgetter('_filename'))
    columns = property(operator.attrgetter('_columns'))
    validation_file = property(operator.attrgetter('_validation_file')) #not passing test
    unit = property(operator.attrgetter('_unit'))
    
    @filename.setter
    def filename(self, f: str) -> None:
        if not os.path.isfile(os.path.join(os.getcwd(),f)):
            #default behavior is for the tool to be called in the directory of the file.
            raise Exception("No file at given path")
        else:
            self._filename = f       
        
    @columns.setter
    def columns(self, c: List) -> None:
        if not c:
            self._columns = False
        else:
            if type(c)==list:
                self._columns = c
            else:
                self._columns = list(c)
                assert len(self._columns)==1, "pass a list of names or single string"
   
    @validation_file.setter
    def validation_file(self, vf: str) -> None:
        if not vf:
            self._validation_file = False
        else:
            if not os.path.isfile(os.path.join(os.getcwd(),f)):
                #default behavior is for the tool to be called in the directory of the file.
                raise Exception("No file at given path")
            else:
                self._validation_file = vf
            
    @unit.setter
    def unit(self, unit: List) -> None:
        if type(unit)!=list:
            if unit:
                assert type(unit)==str, "units must be string" 
                self.unit = list(unit)
        else:
            assert type(unit[0])==str, "units must be string" 
            self._unit = unit
