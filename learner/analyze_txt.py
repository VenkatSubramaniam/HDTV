#!/usr/bin/env python
# coding: utf-8

import operator
import os

class TxtParser:
    '''Object for analyzing a generic text file. Passes result back to head.'''
    def __init__(self, fname: str=None) -> None:
        self.distribution = {}
        self.filename = fname
        self.CSV_IDENTIFIER = "csv"
        self.JSON_IDENTIFIER = "json"
        self.TXT_IDENTIFIER = "txt"
        self.UNDEFINED = "undefined"
        self.XML_IDENTIFIER = "xml"        
        self.DELIMITERS = [
                    ",",
                    "\t",
                    "!",
                    " ",
                    ";",
                    "|",
                    "-",
                    ]

    #First time filename gets validated.
    filename = property(operator.attrgetter('_filename'))

    @filename.setter        
    def filename(self, f: str=None):
        if not os.path.isfile(os.path.join(os.getcwd(),f)):
            #default behavior is for the tool to be called in the directory of the file.
            raise Exception("No file at given path - please call tool in the directory of the file object")
        else:
            self._filename = f

    # @staticmethod
    def find_delimiter_distribution(self, filename: str):
        # Discrete counts as k,v pairs.
        for delim in self.DELIMITERS:
            self.distribution[delim] = 0
        # Remove one at a time until only one delimiter left.
        with open(filename, "r") as f:
            while len(self.distribution) > 1:
                l = f.readline()
                for delim in self.DELIMITERS[:]:
                    if delim not in l:
                        del self.distribution[delim]

    def get_delimiter(self) -> str:
        if "xml" in self.filename.split(".")[-1]:
                return self.XML_IDENTIFIER, True
        if "json" in self.filename.split(".")[-1]:
            return self.JSON_IDENTIFIER, True

        self.find_delimiter_distribution(filename=self.filename)
        if not self.distribution:
            print("Filetype is unstructured")
        return max(self.distribution.items(), key=operator.itemgetter(1))[0], False               

    def identify_filetype(self) -> str:
        extension = filename.split(".")[1]
        if extension not in (self.JSON_IDENTIFIER, self.CSV_IDENTIFIER, self.TXT_IDENTIFIER):
            return self.UNDEFINED
        if extension == self.JSON_IDENTIFIER:
            return self.JSON_IDENTIFIER
        if extension == self.CSV_IDENTIFIER:
            return self.CSV_IDENTIFIER
        return self.CSV_IDENTIFIER if self.get_delimiter(filename) == "," else TxtParse.TXT_IDENTIFIER

