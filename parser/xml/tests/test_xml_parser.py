import pytest

##Connecting to postgres:
def test_pg_connection:
	parser = ingester(uname="username", pword="password", db="postgres")
	parser.validate_login()


##No columns specified
def test_xml_return:
	parser = ingester(fname="dummy.xml", cols="yes", uname="username", pword="password", unit="book")
    parser.streaming()

##Some columns specified
def test_partial_xml_return:
	pass
