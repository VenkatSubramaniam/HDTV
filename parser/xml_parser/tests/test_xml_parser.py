#!/usr/bin/env python
# coding: utf-8

##Imports
from parsing_funcs import xml_parser as xp
import os 
import pytest

##Read in some dummy data
def test_run_cmdline():
	base = os.path.dirname(__file__)
	script = os.path.abspath(os.path.join(base, "..", "xml_parser.py"))
	os.system(f"python {script} -f dummy.xml.test -U postgres -P password -d postgres")	

##Connecting to postgres:
def test_pg_connection():
	parser = xp.ingester(fname="dummy.xml.test", uname="postgres", pword="password", db="test")
	parser.validate_login()
	assert parser.connection.closed==0, "Connection not established"


##No columns specified
def test_xml_return():
	# parser = ingester(fname=data, cols=False, uname="username", pword="password", unit="book")
	# parser.streaming()
	pass

##Some columns specified
def test_partial_xml_return():
	pass
