from learner.sampler import slurper as s
import unittest
import pytest

## Sudeepa comes along and sees a new tool to do analysis on a file.

# She gets very excited and finds a large file that's not at the top of her priorities to try it on:
def test_initialize():
	slurp = s.Slurper(filename = "../../util/sample_xml.xml", unit = "row")

# She then tries to call the estimate lines to see how many rows she has in her XML
def test_fails_unstructured():
	slurp = s.Slurper(filename = "../../util/sample_xml.xml", unit = "row")
	with pytest.raises(Exception) as e:
		slurp.estimate_line_size()