#!/usr/bin/env python
# coding: utf-8

##Imports - try to pull off more dependencies by the end:
import os
import operator
import pytest
import random
import sys
import time


def reservoir_sampling(filename, k):
	sample = []
	with open(filename) as f:
		for n, line in enumerate(f):
			if n < k:
				sample.append(line.rstrip())
			else:
				r = random.randint(0, n)
				if r < k:
					sample[r] = line.rstrip()
	return sample



def random_sampler(filename, k):
	sample = []
	with open(filename, 'rb') as f:
		f.seek(0, 2)
		filesize = f.tell()

		random_set = sorted(random.sample(xrange(filesize), k))

		for i in xrange(k):
			f.seek(random_set[i])
			# Skip current line (because we might be in the middle of a line) 
			f.readline()
			# Append the next line to the sample set 
			sample.append(f.readline().rstrip())

	return sample