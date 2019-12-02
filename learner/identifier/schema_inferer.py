#!/usr/bin/env python
# coding: utf-8

import csv
import re
from typing import Dict, List, Union

class Inferer:
	'''Object for extracting a schema from the random rows. Passes result back to head.'''
	def __init__(self, sample: List, delimiter: str, unstructured: bool, encoding: str="utf-8") -> None:
		self.sample = sample
		self.delimiter = delimiter
		self.unstructured = unstructured
		self.encoding = encoding
		self.type_dict = self.parse_into_dict(sample=self.sample)
		self.schema = self.infer_schema(type_dict=self.type_dict)
		

	def parse_into_dict(self, sample: List[Union[bytes,str]]) -> Dict[Union[str, int], List[str]]:
		#Two paths: structured and semi-structured:
		type_dict = {}
		if not self.unstructured:
			col_names = self.sample[0].decode(encoding=self.encoding).split(self.delimiter)
			#translation dictionary
			colname_to_idx = {}
			counter = 0
			for name in col_names:
				colname_to_idx[counter] = name
				type_dict[name] = []
				counter += 1

			#loop through each row and assign each row to a column
			for row in self.sample[1:]:
				## Tried to do it with the regex, but empty strings were removed.
				# print(re.split(r'(?:[^\s,"]|"(?:\\.|[^"])*")+', row.decode(encoding=self.encoding)))
				counter = 0 
				for parsed in csv.reader([row.decode(encoding=self.encoding)]):
					#Parsed should be of length one
					for val in parsed:
						type_dict[colname_to_idx[counter]].append(val)
						counter += 1				
		return type_dict




	def infer_schema(self, type_dict: Dict[Union[str, int], List[str]]) -> Dict[Union[str, int], str]:
			#call the checkers
			#terminate on first check that's positive, if a different positive is raised, go to varchar
			#append to the type dictionary
			#return type dictionary
			pass

	def check_bool(self, column: List[str]) -> bool:
		pass


	def check_double(self, column: List[str]) -> bool:
		pass


	def check_small_int(self, column: List[str]) -> bool:
		pass

	def check_int(self, column: List[str]) -> bool:
		pass