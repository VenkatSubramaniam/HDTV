import unittest
from parser.identifier.schema_inferer import infer_schema

class TestSchemaInference(unittest.TestCase):

	def test_bool(self):
		boolean_dict = {
			1: ["0", "1", "0", "0"]
		}
		self.assertDictEqual(infer_schema(boolean_dict), {1: "bool"})


	def test_double(self):
		float_dict = {
			"bar": ["1.2", "-3.4", "5.4"]
		}
		self.assertDictEqual(infer_schema(float_dict), {"bar": "double"})

	def test_smallint(self):
		smallint_dict = {
			1: ["-32768", "2", "4563", "-1231"]
		}
		self.assertDictEqual(infer_schema(smallint_dict), {1: "smallint"})

	def test_int(self):
		smallint_dict = {
			1: ["-32769", "2", "4563", "-1231"]
		}
		self.assertDictEqual(infer_schema(smallint_dict), {1: "int"})

	def test_bigint(self):
		smallint_dict = {
			1: ["-32769", "2147483648", "4563", "-1231"]
		}
		self.assertDictEqual(infer_schema(smallint_dict), {1: "bigint"})


	def test_varchar(self):
		# negative test
		weird_dict = {
		1: ["023", "1.0", "sdfdsf"],
		}
		self.assertDictEqual(infer_schema(boolean_dict), {1: "varchar"})


	def test_multicolumn(self):
		pass