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
    def __init__(self, args):
        ##User interface - TODO
            #Request the atomic object by showing head
            #Request the desired columns by list
        ##Start each of the services:
        # pword=args['pword'],
        interface = dbi(uname=args['uname'], db=args['db'], port=args['p'])

        learner = student(interface=interface, fname=args['fname'], cols=args['cols'], unit=args['unit'])
        
        parser = ingester(interface=interface, fname=args['fname'], cols=args['cols'], unit=args['unit'], validation_file=args['vf'])
        for argname in kwargs:
            self.argname = kwargs['argname']





if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-f", "--file", type=str, dest="fname", help="Name of file to be parsed")
    argparser.add_argument("-c", "--columns", type=list, dest='cols', default=False, help="List of columns to be extracted from the file")
    argparser.add_argument("-u", "--unit", type=str, dest="unit", default=None, help="Base unit(s) to be extracted from the file")
    argparser.add_argument("-v", "--validation", type=str, dest="vf", help="Name of file to be used in validation")

    argparser.add_argument("-U", "--username", type=str, dest="uname", default="postgres", help="Username to connect with database")
    # argparser.add_argument("-P", "--password", type=str, dest="pword", default="password", help="Password to connect with database")    
    argparser.add_argument("-D", "--database", type=str, dest="db", default="postgres", help="Database to connect with database")
    argparser.add_argument("-p", "--port", type=str, dest="unit", default=None, help="Port to connect with database")

    args = vars(parser.parse_args())

    brain = veranda(args=args)