#!/usr/bin/env python
# coding: utf-8

##Imports - try to pull off more dependencies by the end:
import argparse
from lxml import etree
import multiprocessing
import os
import operator
import psycopg2
import pytest
import sys
import time
# from typing import

#Internals
#from json_parser import jparser
#from structured_parser import sparser
from parser.xml_parser.parsing_funcs import lumberjack

## Main ingestion object:
class ingester:
    """take file and column labels and insert into postgresql"""
    def __init__(self, fname, interface=None, cols=None, unit=None, validation_file=None):
        super(ingester, self).__init__()
        self.interface = interface #expects the interface object
        self.filename = fname #expects a path
        self.columns = cols #expects a list of names (str)
        self.validation_file = validation_file #expects some DTD
        self.tree = None
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

    @username.setter
    def username(self, u):
        assert type(u)==str, "username must be string"
        self._username = u

    @password.setter
    def password(self, p):
        assert type(p)==str, "password must be string"
        self._password = p
   
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
