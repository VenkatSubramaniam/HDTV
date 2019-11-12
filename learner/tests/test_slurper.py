from learner.sampler import slurper as s
import os
import pytest

## Sudeepa comes along and sees a new tool to do analysis on some files.

# She gets very excited and finds a large file that's not at the top of her priorities to try it on:
@pytest.fixture
def xml_slurper():
	'''Initialize the main object with an XML file.'''
	path_to_current_file = os.path.realpath(__file__)
	current_directory = os.path.split(path_to_current_file)[0]
	current_directory2 = os.path.split(current_directory)[0]
	current_directory3 = os.path.split(current_directory2)[0]
	path_to_file = current_directory3 + "/util/sample_xml.xml"
	return s.Slurper(filename = path_to_file, unit = "row")

# She then tries to call the estimate lines to see how many rows she has in her XML
def test_fails_unstructured_estimation(xml_slurper):
	with pytest.raises(Exception) as e:
		xml_slurper.estimate_line_size()

# Upset, she pays her underlings to count the number of lines by hand.

# Once she knows the filesize, the docs tell her she can calculate the sample size needed.
def test_nlines_to_sample_size(xml_slurper):
	sample = xml_slurper.get_sample_size(nlines=5000)
	assert type(sample) == int
	assert sample >= 1


# Sad, she wonders if she can still sample the file to learn about it.
def test_xml_fails_reservoir(xml_slurper):
	with pytest.raises(Exception) as e:
		xml_slurper.pythonic_reservoir()

# She's about to give up on the idea of using the package, but then realizes this should all be automated.

# The automated version takes the xml and directly passes it to the unstructured parser to get lines back:
def test_randomly_sample_xml(xml_slurper):
	sample = xml_slurper.read_random_xml()
	assert type(sample) == list
	assert len(sample) > 0
