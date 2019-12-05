#!/usr/bin/env python
# coding: utf-8

import csv
import re
from typing import Dict, List, Union

class Inferer:
	'''Object for extracting a schema from the random rows. Passes result back to head.'''
	def __init__(self, sample: List, delimiter: str, unstructured: bool, encoding: str="utf-8") -> None:
		self.type_dict = self._infer_schema(sample, delimiter, unstructured, encoding)
		print(self.type_dict)

	def _parse_structured_dict(self, sample: List[Union[bytes,str]], delimiter: str, encoding: str) -> Dict[Union[str, int], List[str]]:
		#Two paths: structured and semi-structured:
		val_dict = {}
		col_names = sample[0].decode(encoding).split(delimiter)
		#translation dictionary
		colname_to_idx = {}
		counter = 0
		for name in col_names:
			colname_to_idx[counter] = name
			val_dict[name] = []
			counter += 1

		#loop through each row and assign each row to a column
		for row in sample[1:]:
			## Tried to do it with the regex, but empty strings were removed.
			# print(re.split(r'(?:[^\s,"]|"(?:\\.|[^"])*")+', row.decode(encoding=self.encoding)))
			counter = 0 
			for parsed in csv.reader([row.decode(encoding)]):
				#Parsed should be of length one
				for val in parsed:
					val_dict[colname_to_idx[counter]].append(val)
					counter += 1				
		return val_dict




	def _infer_schema(self, sample, delimiter: str, unstructured: bool, encoding: str) -> Dict[Union[str, int], str]:
		if unstructured:
			pass
		else:
			type_dict = self._parse_structured_dict(sample, delimiter, encoding)
		for key in type_dict:
			type_dict[key] = self._identify_dtype(type_dict[key])
		return type_dict

	def _identify_dtype(self, vals: List[str]) -> str:
		if self.check_nulls(vals):
			return "varchar"
		if self.check_bool(vals):
			return "bool"
		if self.check_int(vals):
			return "int"
		if self.check_double(vals):
			return "double"
		return "varchar"

	@staticmethod
	def check_nulls(column: List[str]) -> bool:
		return all([i=="" for i in column])

	@staticmethod
	def check_bool(column: List[str]) -> bool:
		return all([i=="0" or i=="1" or i=="" for i in column])

	@staticmethod
	def check_int(column: List[str]) -> bool:
		return all([i=="" or i.isdigit() or (i[0]=='-' and i[1:].isdigit()) for i in column])

	@staticmethod
	def check_double(column: List[str]) -> bool:
		for s in column:
		    try:
		        float(s)
		    except ValueError:
		        if s =="":
		        	continue
		        return False
		return True
