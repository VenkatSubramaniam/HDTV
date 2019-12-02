#!/usr/bin/env python
# coding: utf-8

from typing import Dict, List, Union

class Inferer:
	'''Object for extracting a schema from the random rows. Passes result back to head.'''
	def __init__(self, sample: List, delimiter: str, unstructured: bool) -> None:
		self.sample = sample
		self.unstructured = unstructured
		


	def infer_schema(type_dict: Dict[Union[str, int], List[str]]) -> Dict[Union[str, int], str]:
		#Two paths: structured and semi-structured:
		if not unstructured:
			pass
			#loop through each of the columns
			#call the checkers
			#terminate on first check that's positive
			#append to the type dictionary
			#return type dictionary

	def check_bool(column: List[str]) -> bool:
		pass


	def check_double(column: List[str]) -> bool:
		pass


	def check_small_int(column: List[str]) -> bool:
		pass

	def check_int(column: List[str]) -> bool:
		pass