#!/usr/bin/env python
# coding: utf-8

##Imports 
import pytest

##Connecting to postgres:
def test_pg_connection():
	parser = ingester(uname="username", pword="password", db="postgres")
	parser.validate_login()


##No columns specified
def test_xml_return(data):
	parser = ingester(fname=data, cols=False, uname="username", pword="password", unit="book")
    parser.streaming()

##Some columns specified
def test_partial_xml_return():
	pass
