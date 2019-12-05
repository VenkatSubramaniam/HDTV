#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import csv


class csv_parser:
    
    def __init__(self, fname, interface, delimiter, desired_cols):
        self.file = fname 
        self.db_inter = interface 
        self.column = desired_cols 
        self.delimiter = delimiter 
    
    def read_data(self, reader):
        count = 0
        batchsize = 1000
        for row in reader:
            data = {}
            for (k,v) in enumerate(row):
                if k in self.column:
                    data[k] = v
            count += 1
            self.db_inter.insert_row(table_name, data)
            if count == batchsize:
                self.db_inter.commit()
                count = 0  
        
    
    def parse(self):    
        with open(self.file, 'r') as csv_file:
            reader = csv.DictReader(csv_file, delimiter = self.delimiter)
            read_data(reader) 
        self.db_inter.commit()
        csv_file.close()
          
    
 

