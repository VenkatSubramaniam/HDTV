from itertools import islice
import math
import os
import random
import sys
import time
from typing import List

##TO DO:
#Only call this when the file is of more than ~1/3 total available memory size of the computer. (or approaching max of python 2)
#Ensure this works for unstructured file types - write this for xml and json
#Catch encoding errors inside the functions (get filesize)
#Connect to the API for the rest of the project

##TO DO:
#Only call this when the file is of more than ~1/3 total available memory size of the computer. (or approaching max of python 2)
#Ensure this works for unstructured file types - write this for json
#Connect to the API for the rest of the project

class Slurper(object):
    
    def __init__(self, filename: str, filetype: str = "txt",unit: str = None, encoding: str = 'UTF-8', structured: bool = False) -> None:
        self.filename = filename
        self.encoding = encoding
        self.structured = structured
        self.check_file_encodings()
        ##Call the filetype analyst
        self.filetype = filetype
        if self.structured == True:
            self.nlines = self.estimate_line_size()
            self.sample_size = self.get_sample_size()        
        else:
            self.unit = unit
    
    def check_file_encodings(self, file: str = None, encoding: str = None, cycle_bit: int = 0) -> str:
        """Ensure that the file can be decoded by cycling through common file types."""        
        if not file:
            file = self.filename
        if not encoding:
            encoding = self.encoding            
        
        if cycle_bit > 0:
            common_encodings = ["UTF-8","Latin-1", "UTF-16", "ascii", "cp037", "cp437", "UTF-32"]
            return common_encodings[cycle_bit]
            
        else:
            try:
                with open(file, encoding = encoding) as f:
                    f.seek(10000,0)
                    f.readline()
                    f.close()
                return encoding
            except:
                common_encodings = ["Latin-1", "UTF-16", "ascii", "cp037", "cp437", "UTF-32"]
            
                for codec in common_encodings:
                    try:
                        with open(file, encoding = codec) as f:
                            f.readline()
                            f.close()
                        return codec
                    except:
                        continue
                print("Your file is an unusual type - can you specify the encoding for us?")
    
    def estimate_line_size(self, file: str = None, encoding: str = None, structured: bool = False) -> int:
        """Estimate the number of lines in the file- requires exactly 62 line reads."""
        
        if not file:
            file = self.filename
        if not encoding:
            encoding = self.encoding
        if not structured:
            structured = self.structured
        assert structured==True, "Line size estimation only works for structured data."
        
        try:    
            with open(file, encoding = encoding) as f:
                #chop header
                f.readline()
                line = f.readline()
                line_length = len(line)
                f.seek(0,2)
                eof = f.tell()
                average_length = 0
                for sample in range(2,32):
                    f.seek(math.floor(eof/sample),0)
                    f.readline()
                    line = f.readline()
                    average_length += len(line)
                f.close()
        except Exception as e:
            print(e)
            for cycle in range(1,7):
                new_encoding = self.check_file_encodings(file = file, encoding = None, cycle_bit = cycle)
                try:
                    with open(file, encoding = new_encoding) as f:
                        #chop header
                        f.readline()
                        line = f.readline()
                        line_length = len(line)
                        f.seek(0,2)
                        eof = f.tell()
                        average_length = 0
                        for sample in range(2,32):
                            f.seek(math.floor(nlines/sample),0)
                            f.readline()
                            line = f.readline()
                            average_length += len(line)
                        f.close()
                    return math.floor(eof/(average_length/30))
                except:
                    continue
            print("Cannot estimate line size using the typical encodings. Is your data semi-structured?")
            
        return math.floor(eof/(average_length/30))
            
    def get_sample_size(self, nlines: int = None, confidence: float = 0.95, error: float = 0.05) -> int:
        """Returns number of samples needed for a population of nlines, when sampling is done with replacement"""
        if not nlines:
            nlines = self.nlines
        criticals = {
            0.90: 1.645,
            0.95: 1.96,
            0.99: 2.576,
            90: 1.645,
            95: 1.96,
            99: 2.576,            
        }    
        #Hard coding the std. dev. of bernoulli for now
        X = ((criticals[confidence]**2) * 0.5 * 0.5)/(error**2)
        return math.ceil(nlines*X / (X + nlines - 1)) #sample proportion correction
            
    
    def pythonic_reservoir(self, file: str = None, encoding: str = None, reservoir_size: int = None, structured: bool = False) -> List[str]:
        """Make a single pass through the file, replacing each value with some probability. This is Knuth's Reservoir Sampling."""
        if not file:
            file = self.filename
        if not reservoir_size:
            reservoir_size = self.sample_size
        if not encoding:
            encoding = self.encoding
        if not structured:
            structured = self.structured
        assert structured==True, "Line size estimation only works for structured data."
            
        reservoir = []
        #one extra for the header
        n = reservoir_size + 1 
        counter = 0
        try:
            with open(file, encoding = encoding) as f:
                for line in f:
                    if counter < n:
                        reservoir.append(line.strip())
                        counter += 1
                    else:
                        #skip the first to keep the header in
                        draw = random.randrange(1,n,1)
                        reservoir[draw] = line
                f.close()
        except:
            for cycle in range(1,7):
                new_encoding = self.check_file_encodings(file = file, encoding = None, cycle_bit = cycle)
                try:
                    with open(file, encoding = encoding) as f:
                        for line in f:
                            if counter < n:
                                reservoir.append(line.strip())
                                counter += 1
                            else:
                                #skip the first to keep the header in
                                draw = random.randrange(1,n,1)
                                reservoir[draw] = line
                        f.close()
                    return reservoir
                except:
                    continue
            print("Cannot run reservoir sampling using the typical encodings. Is your data semi-structured?")
        return reservoir
                    
                    
    def read_random_lines(self, file: str = None, sample_size: int = None, byte_bite: int = 20) -> List[bytes]:
        """Randomly point reader through the file, using the \n as a marker for a new record. Iterate until full line."""        
        if not file:
            file = self.filename
        if not sample_size:
            sample_size = self.sample_size
        
        random_sample = []
        with open(file, 'rb') as f:
            for sample in range(sample_size):
                f.seek(0, 2)
                size = f.tell()
                i = random.randrange(0, size)
                while True:
                    i -= byte_bite
                    ##Prevent an improper seek before (0,0).
                    if i < 0:
                        byte_bite += i
                        i = 0
                    f.seek(i, 0)
                    nxt = f.read(byte_bite)
                    eol_idx = nxt.rfind(b'\n')
                    ##If not clean, then jump to the next line by using the \n as a marker.
                    if eol_idx != -1:
                        i += eol_idx + 1
                        break
                    if i == 0:
                        break
                f.seek(i, 0)
                random_sample.append(f.readline().strip())
        return random_sample
    
    def read_random_xml(self, file: str = None, sample_size: int = 100, byte_bite: int = 50, unit: str = None, encoding: str = None) -> List[bytes]:
        """Randomly point reader through the file, using the bracketed unit as newline. Iterate until full line."""        
        if not file:
            file = self.filename
        if not sample_size:
            sample_size = self.sample_size
        if not unit:
            unit = self.unit              
        if not encoding:
            encoding = self.encoding            
        
        start_seq = bytearray("<"+unit, encoding = encoding)
        transform = bytearray("</"+unit+">", encoding = encoding)
        start_len = len(start_seq) - 1 
        tran_len = len(transform) - 1 
        assert byte_bite > 0, "Search step size must be positive."
        
        random_sample = []
        with open(file, "rb") as f:
            for sample in range(sample_size):
                f.seek(0, 2)
                size = f.tell()
                i = random.randrange(0, size)
                while True:
                    i -= byte_bite
                    #Prevent an improper seek before (0,0).
                    if i < 0:
                        byte_bite += i
                        i = 0
                    f.seek(i, 0)
                    nxt = f.read(byte_bite)
                    eol_idx = nxt.rfind(transform)
                    #If not clean, then jump to the next line by using the closing brackets as a marker.
                    if eol_idx != -1:
                        i += eol_idx + 1
                        break
                    if i == 0:
                        break

                #This marks the start of the xml data that we want.
                start_block = i + tran_len
                f.seek(start_block, 0)

                while True:
                    i += byte_bite
                    #Just read until eof if we rolled the very last.
                    if i > size:
                        i = f.seek(0,2)
                        break
                    f.seek(i, 0)
                    search = f.read(byte_bite)
                    start_idx = search.find(start_seq)
                    if start_idx != -1:
                        i += start_idx + 1
                        break
                    if i == 0:
                        break
                f.seek(start_block, 0)

                random_sample.append(f.read(i - start_block - 1))
            f.close()
        return random_sample