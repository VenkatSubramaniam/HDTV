#!/usr/bin/env python
# coding: utf-8

import os
import time
from typing import Dict, List

class Timing_Tests(object):
	"""
	Stress tests all parts of the project and recording wall times for each under different file loads
	Should pass a dictionary of //'phase of project': time// to the graphing function
	"""
	def __init__(self, arg):
		self.arg = arg
	

	def record_times(self) -> Dict[str,List[float]]:
		##Identifies all parts of the project to be measures and establishes a baseline of 10k rows
		pass

	def increase_load(self, current_rows: int) -> None:
		##Runs the same wall time tests with an increased number of rows
		pass