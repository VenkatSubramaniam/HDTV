#!/usr/bin/env python
# coding: utf-8

from typing import Dict, List, Union

class Inferer:
	'''Object for analyzing a generic text file. Passes result back to head.'''
	def __init__(self, sample: List, unstructured: bool) -> None:
		self.sample = sample
		self.unstructured = unstructured

	def infer_schema(type_dict: Dict[Union[str, int], List[str]]) -> Dict[Union[str, int], str]:
		pass


	def check_bool(column: List[str]) -> bool:
		pass


	def check_double(column: List[str]) -> bool:
		pass


	def check_small_int(column: List[str]) -> bool:
		pass

	def check_int(column: List[str]) -> bool:
		pass