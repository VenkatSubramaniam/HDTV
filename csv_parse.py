#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import csv
from collections import defaultdict

class csv_parser:
    
    def __init__(self, filename, delimiter, desired_cols, batchsize):
        self.file = filename
        self.column = desired_cols #columns user defined to keep
        self.batchsize = batchsize #number of rows we load into database at one time
        self.delimiter = delimiter #delimiter used in file
    
    # check if user defined some unexisted columns
    def validate_desired_cols(self, header):
        for col in self.column:
            if col in header:
                pass
            else:
                raise ValueError("'{column}' is not valid.", column = col)
    
    # helper function to read a batch of data
    def read_data(self, reader):
        table = defaultdict(list)
        count = 0
        for row in reader:
            for (k,v) in enumerate(row):
                if k in self.column:
                    table[k].append(v)
            count += 1
            if count == self.batchsize:
                load_to_db(table)
                table = defaultdict(list)
                count = 0  
        
    # funtion that will parse a file
    def parse(self):    
        with open(self.file, 'r') as csv_file:
            header = csv_file.readline().rstrip().split(',')
            validate_desired_cols(header) 
            reader = csv.DictReader(csv_file, delimiter = self.delimiter)
            read_data(reader)    
        csv_file.close()
          
    # load data into database
    def load_to_db(self, table):
        # TODO
        pass
        

