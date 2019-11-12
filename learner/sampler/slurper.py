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

class Slurper(object):
    def __init__(self, filename: str, encoding: str = 'UTF-8') -> None:
        self.file = filename
        self.encoding = check_file_encodings()
        self.nlines = estimate_line_size()
        self.sample_size = get_sample_size()
    
    def check_file_encodings(file: str = None, encoding: str = None) -> str:
        """Ensure that the file can be opened by cycling through common file types."""        
        if not file:
            file = self.filename
        if not encoding:
            encoding = self.encoding            

        try:
            with open(file, encoding = encoding) as f:
                f.seek(1000,0)
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
            print("Your file is of unusual type - can you specify the encoding for us?")
    
    def estimate_line_size(file: str = None) -> int:
        """Estimate the number of lines in the file- requires exactly 62 line reads."""
        if not file:
            file = self.filename
            
        with open(file) as f:
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
            
    def get_sample_size(nlines: int = None, confidence: float = 0.95, error: float = 0.05) -> int:
        """Returns number of samples needed for a population of nlines, when sampling is done with replacement"""
        if not nlines:
            nlines = self.nlines
            
        #Hard coding the critical and std. dev. for now
        X = (1.96 * 0.5 * 0.5)/(error**2)
        return math.floor(nlines*X / (X + N - 1))
            
    
    def pythonic_reservoir(file: str = None, reservoir_size: int = None) -> List[str]:
        """Make a single pass through the file, replacing each value with some probability. This is Knuth's Reservoir Sampling."""
        if not file:
            file = self.filename
        if not reservoir_size:
            reservoir_size = self.sample_size
            
        reservoir = []
        n = reservoir_size - 1
        counter = 0
        
        with open(file) as f:
            for line in f:
                if counter < reservoir_size:
                    reservoir.append(line)
                else:
                    n += 1
                    draw = random.randrange(0,n,1)
                    if draw < reservoir_size:
                        reservoir[draw] = line
            f.close()
        
        return reservoir
                    
                    
    def pythonic_read_random_lines(file: str = None, sample_size: int = None, byte_bite: int = 20) -> List[str]:
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
                random_sample.append(f.readline())
        return random_sample