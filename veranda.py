#!/usr/bin/env python
# coding: utf-8

##Imports
import argparse
import multiprocessing
import os
import operator
import sys
import time

##Calling
from db_interfacer.interfacer import DBInterfacer as dbi
from learner.analyze_txt import TxtParser as tp
from learner.identifier.schema_inferer import Inferer as inf
from learner.sampler.slurper import Slurper as slurp
from parsers.ingester import Ingester as ing

class Veranda:
    """Head of the project. Calls the learner, the parser, and the db interface. UI possibly in future"""
    def __init__(self, args):
        ##User interface - TODO
            #Request the atomic object by showing head
            #Request the desired columns by listing
        ##Start each of the services 
        ##TO-DO as processes:
        ##Serial
        interface = dbi(uname=args['uname'], pword=args['pword'], db=args['db'], port=args['port'])
        delimit = tp(fname=args['fname'])
        delimiter, unstructured = delimit.get_delimiter()
        #pick the encoding somewhere here

        ##Get the samples
        sampler = slurp(filename=args['fname'], structured=not unstructured)
        if unstructured:
            if delimiter=="xml":
                rs0 = sampler.read_random_xml()
            else:
                #fix for the json sampler
                pass
        else:
            #Skim down to the faster one after testing
            rs1 = sampler.read_random_lines()
            rs2 = sampler.pythonic_reservoir()

        ##Pass them to the inferer
        #Time check but assuming the bytes win in speed
            schema = inf(rs1, delimiter, unstructured)

        # learner = student(interface=interface, fname=args['fname'], cols=args['cols'], unit=args['unit'])
        parser = ing(interface=interface, fname=args['fname'], ftype=delimiter, cols=args['cols'], unit=args['unit'], validation_file=args['vf'])


if __name__ == "__main__":
    
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-f", "--file", type=str, dest="fname", help="Name of file to be parsed", required=True)
    argparser.add_argument("-c", "--columns", type=str, nargs='+', dest='cols', default=False, help="List of columns to be extracted from the file")
    argparser.add_argument("-u", "--unit", type=str, dest="unit", default=None, help="Base unit(s) to be extracted from the file")
    argparser.add_argument("-v", "--validation", type=str, dest="vf", help="Name of file to be used in validation")

    argparser.add_argument("-U", "--username", type=str, dest="uname", default="postgres", help="Username to connect with database")
    argparser.add_argument("-P", "--password", type=str, dest="pword", default="password", help="Password to connect with database")    
    argparser.add_argument("-D", "--database", type=str, dest="db", default="postgres", help="Database to connect with database")
    argparser.add_argument("-p", "--port", type=str, dest="port", default=None, help="Port to connect with database")

    args = vars(argparser.parse_args())

    brain = Veranda(args=args)
