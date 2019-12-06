#!/usr/bin/env python
# coding: utf-8

##Standard library imports
import os
import pandas
import time
import timeit
from typing import Dict, List
import sys

sys.path.insert(0, os.path.abspath('..'))
##Import the functions to be tested
from db_interfacer.interfacer import DBInterfacer as dbi
from learner.analyze_txt import TxtParser as tp
from learner.identifier.schema_inferer import Inferer as inf
from learner.sampler.slurper import Slurper as slurp
from parsers.ingester import Ingester as ing

import plot_time as pt


class Timing_Tests:
	"""
	Stress tests all parts of the project and recording wall times for each under different file loads
	Should pass a dictionary of //'phase of project': time// to the graphing function
	"""
	def __init__(self):
		self.phases = ["Sampler", "Estimate Lines", "Sample Lines - Structured v1",] 
		# "Sample Lines - Structured v2",]
		# \
		# "Sample Lines - XML", "Sample Lines - JSON", "Schema Inference", "Parse File",\
		# "Parse Structured", "Parse XML", "Parse JSON", "Pandas"]
		self.nrows = [1e2,5e2,1e3,5e3,2e4,5e4,75e3,1e5,2e5,25e4,3e5,35e4,45e4,5e5,6e5,75e4,1e6,15e5,2e6,5e6,1e7]
		self.file_map = {key: f"{key}_test.csv" for key in self.nrows}
		self.timing_dictionary = {key: [] for key in self.phases}

		##Execute
		self.test_sampler()
		self.test_estimate_lines()
		self.test_sampling_lines_1()
		# self.test_sampling_lines_2()
		# self.test_schema_inferer()
		# self.test_database_insertion()
		# self.test_pandas_baseline()

	
	#Phase -> n = 1, ..., n -> For each n, errs on n.

	def test_sampler(self, current_phase: int = 0, structured: bool=True) -> None:

		##Identifies all parts of the project to be measures and establishes a baseline of 10k rows
		current_rows = 0
		for niter in range(len(self.nrows)):
			this_n = []
			file_new = self.increase_load(current_rows)
			current_rows += 1

			for test_iteration in range(30):
				t0 = timeit.default_timer()
				sampler = slurp(filename=f"../util/{file_new}", structured=True)
				sampler.read_random_lines()
				this_n.append(timeit.default_timer()-t0)

			self.timing_dictionary[self.phases[current_phase]].append(this_n)


	def test_estimate_lines(self, current_phase: int = 1, structured: bool=True) -> None:
		##Identifies all parts of the project to be measures and establishes a baseline of 10k rows
		current_rows = 0
		for niter in range(len(self.nrows)):
			this_n = []
			file_new = self.increase_load(current_rows)
			current_rows += 1

			for test_iteration in range(30):
				t0 = timeit.default_timer()
				sampler = slurp(filename=f"../util/{file_new}", structured=True)
				this_n.append(timeit.default_timer()-t0)

			self.timing_dictionary[self.phases[current_phase]].append(this_n)


	def test_sampling_lines_1(self, current_phase: int = 2, structured: bool=True) -> None:
		##Identifies all parts of the project to be measures and establishes a baseline of 10k rows
		current_rows = 0
		for niter in range(len(self.nrows)):
			this_n = []
			file_new = self.increase_load(current_rows)
			current_rows += 1
			sampler = slurp(filename=f"../util/{file_new}", structured=True)

			for test_iteration in range(30):
				t0 = timeit.default_timer()
				rs1 = sampler.read_random_lines()
				this_n.append(timeit.default_timer()-t0)

			self.timing_dictionary[self.phases[current_phase]].append(this_n)	

	def test_sampling_lines_2(self, current_phase: int = 3, structured: bool=True) -> None:
		##Identifies all parts of the project to be measures and establishes a baseline of 10k rows
		current_rows = 0
		for niter in range(len(self.nrows)):
			this_n = []
			file_new = self.increase_load(current_rows)
			current_rows += 1
			sampler = slurp(filename=f"../util/{file_new}", structured=True)

			for test_iteration in range(30):
				t0 = timeit.default_timer()
				rs2 = sampler.pythonic_reservoir()
				this_n.append(timeit.default_timer()-t0)

			self.timing_dictionary[self.phases[current_phase]].append(this_n)

	def test_schema_inferer(self, current_phase: int = 7, structured: bool=True) -> None:
		##Identifies all parts of the project to be measures and establishes a baseline of 10k rows
		current_rows = 0
		for niter in range(len(self.nrows)):
			this_n = []
			file_new = self.increase_load(current_rows)
			current_rows += 1
			sampler = slurp(filename=f"../util/{file_new}", structured=True)
			lines = sampler.read_random_lines()

			for test_iteration in range(30):
				t0 = timeit.default_timer()
				schema = inf(lines, ",", False).type_dict
				this_n.append(timeit.default_timer()-t0)

			self.timing_dictionary[self.phases[current_phase]].append(this_n)

	def test_pandas_baseline(self, current_phase: int = -1, structured: bool=True) -> None:
		##Identifies all parts of the project to be measures and establishes a baseline of 10k rows
		current_rows = 0
		for niter in range(len(self.nrows)):
			this_n = []
			file_new = self.increase_load(current_rows)
			current_rows += 1

			for test_iteration in range(30):
				t0 = timeit.default_timer()
				data = pandas.read_csv(f"../util/{file_new}")
				this_n.append(timeit.default_timer()-t0)

			self.timing_dictionary[self.phases[current_phase]].append(this_n)

	def increase_load(self, current_rows: int) -> str:
		##Runs the same wall time tests with an increased number of rows
		#For now it just increments.
		# rows_small = 9
		#Fetch a pre-chopped file with the same size 
		return self.file_map[self.nrows[current_rows]]

	def validate_runtime(self):
		pass

	def push_to_grapher(self):
		return self.nrows, self.timing_dictionary

if __name__ == "__main__":
	graphs = Timing_Tests()
	pt.plot_time(graphs.timing_dictionary, graphs.nrows)	