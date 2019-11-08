#!/usr/bin/env python
# coding: utf-8

##Imports - try to pull off more dependencies by the end:
import argparse
import multiprocessing
import os
import operator
import pytest
import sys
import time

#Calling
from learner import student
from parser import ingester
from db_inter import dbi

class veranda:
    """Heart of the project. Calls the learner, the parser, and the db interface. UI possibly in future"""
    def __init__(self, fname, uname, pword, cols=None, validation_file=None, unit=None, port="5432", db=None):
        super(veranda, self).__init__()
        self.filename = fname #expects a path
        self.columns = cols #expects a list of names (str)
        self.username = uname #expects a string
        self.password = pword #expects a string
        self.validation_file = validation_file #expects some DTD
        self.unit = unit #expects a string        
        self.port = port #gives a default string
        self.database = db #default is to create a new one
        self.tree = None
        #encoding specification
        #host specification
        
    filename = property(operator.attrgetter('_filename'))
    columns = property(operator.attrgetter('_columns'))
    username = property(operator.attrgetter('_username'))
    password = property(operator.attrgetter('_password'))
    validation_file = property(operator.attrgetter('_validation_file')) #not passing test
    unit = property(operator.attrgetter('_unit'))
    port = property(operator.attrgetter('_port'))
    database = property(operator.attrgetter('_database'))
    
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

    @port.setter
    def port(self, pt):
        pt = str(pt) #if passed int
        self._port = pt

    @database.setter
    def database(self, db):
        if not db:
            self._database = False
        else:
            assert type(db)==str, "database name must be string"
            self._database = db







if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", type=str, dest="fname", help="Name of file to be parsed")
    parser.add_argument("-c", "--columns", type=list, dest='cols', default=False, help="List of columns to extract from the file")
    parser.add_argument("-U", "--username", type=str, dest="uname", default="postgres", help="Username to connect with postgres")
    parser.add_argument("-P", "--password", type=str, dest="pword", default="password", help="Password to connect with postgres")    
    parser.add_argument("-D", "--database", type=str, dest="db", default="postgres", help="Database to connect with postgres")
    parser.add_argument("-u", "--unit", type=str, dest="unit", default=None)
    args = vars(parser.parse_args())
    parser = ingester(fname=args['fname'], cols=args['cols'], uname=args['uname'], pword=args['pword'], unit=args['unit'], db=args['db'])
    #    parser.streaming()
    #    parser.blocked()