import csv

class Librarian:
    
    def __init__(self, fname, interface, delimiter, desired_cols, table_name):
        self.file = fname 
        self.db_inter = interface 
        self.column = desired_cols 
        self.delimiter = delimiter 
        self.table_name = table_name
    
    def read_data(self, reader):
        count = 0
        first_row = True
        batchsize = 1000
        first_row = True
        for row in reader:
            if first_row:
                first_row = False
                continue
            data = {}
            for k in row:
                if k:
                    data[k] = row[k]
            print(data)
            count += 1
            self.db_inter.insert_row(self.table_name, data)
            if count == batchsize:
                self.db_inter.commit()
                count = 0  
        
    
    def parse(self):    
        with open(self.file, 'r') as csv_file:
            if self.column:
                reader = csv.DictReader(csv_file, delimiter=self.delimiter, fieldnames=self.column)
            else:
                reader = csv.DictReader(csv_file, delimiter=self.delimiter)
            self.read_data(reader) 
        self.db_inter.commit()