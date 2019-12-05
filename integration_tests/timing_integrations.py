#!/usr/bin/env python
# coding: utf-8

##Standard library imports
import os
import time
import timeit
from typing import Dict, List

##Import the functions to be tested
from learner.sampler.slurper import Slurper as slurp

class Timing_Tests:
	"""
	Stress tests all parts of the project and recording wall times for each under different file loads
	Should pass a dictionary of //'phase of project': time// to the graphing function
	"""
	def __init__(self):
		structured = True
		current_rows = 0
		phases = ["Sampler", "Estimate Lines", "Sample Lines - Structured",\
		"Sample Lines - XML", "Sample Lines - JSON", "Schema Inference", "Parse File",\
		"Parse Structured", "Parse XML", "Parse JSON"]

		self.nrows = [1e2,5e2,1e3,5e3,2e4,5e4,75e3,1e5,2e5,25e4,3e5,35e4,45e4,5e5,6e5,75e4,1e6,15e5,2e6,5e6,1e7]
		self.file_map = {key: f"{key}_test.csv" for key in nrows}
		self.timing_dictionary = {key: [] for key in phases}

		##Execute
		for i in range(nrows):
			self.record_times()
			self.increase_load(current_rows)
	
	#Phase -> n = 1, ..., n -> For each n, errs on n.

	def record_times(self, functional: str) -> Dict[str,List[float]]:
		##Identifies all parts of the project to be measures and establishes a baseline of 10k rows
		timeit.timeit(f1)
		pass

	def calculate_errors(self):
		pass

	def increase_load(self, current_rows: int) -> str:
		##Runs the same wall time tests with an increased number of rows
		#For now it just increments.
		rows_small = 9
		#Fetch a pre-chopped file with the same size 
		rows[current_rows+1]
		return fpath

	def validate_runtime(self):
		pass

	def push_to_grapher(self):
		return self.nrows, self.timing_dictionary