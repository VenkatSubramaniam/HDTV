import json

class json_parser:
	def __init__(self, fname, interface = None, cols = None, batchsize, validation_file = None, repeats = None):
		self.filename = fname
		self.interface = interface
		self.cols = cols # expects a list of desired column names
		self.batchsize = batchsize # The size of passes in one commit.
		self.validation_file = validation_file # Do we need DTD for json?
		self.repeats = repeats # record whether there exist repeated columns.

	# To convert json file to a python dictionary for easy access.
	def get_dict(self):
		try:
			self.dict = json.loads(self.filename, indent = 2, sort_keys = False)
		except:
			print("Failed to convert json file to python dict!")
			pass

	# Get all the column names.
	def get_leaves(item, key = None):
		item = self.dict
	    if isinstance(item, dict):
	        leaves = {}
	        for i in item.keys():
				"""
				Having trouble dealing with nested json here.
				"""
	            leaves.update(get_leaves(item[i], i))
	        return leaves
	    elif isinstance(item, list):
	        leaves = {}
	        for i in item:
	            leaves.update(get_leaves(i, key))
	        return leaves
	    else:
	        return {key : item}

	def parse_file(self):
		with open(self.filename) as f:
		    json_data = json.loads(f.read())
		    #json_data = json.load(f_input)['LOG_28MAY']

		# First parse all entries to get the complete fieldname list
		fieldnames = set()

		for entry in json_data:
		    fieldnames.update(get_leaves(entry).keys())

		return fieldnames


	# Make sure all the columns requested by user are valid( exist in file).
	def validate_specified_cols(self, fieldnames):
		for col in self.cols:
			if col in fieldnames:
				pass
			else:
				raise ValueError(col)


	def write_stream(self):
		counter = 0
		with open(self.filename) as f:
		    json_data = json.loads(f.read())

		# If we have columns specified.
		if self.cols:
			for field in self.cols:
				query = {}
				query[field].qppend(json_data[field])
			self.db_inter.insert_row(table_name, query)
			counter += 1

			if counter == self.batchsize:
				self.db_inter.commit()
				counter = 0

		#Otherwise we take it all.
		else:
			for field in fieldnames:
				query = {}
				query[field].qppend(json_data[field])
			self.db_inter.insert_row(table_name, query)
			counter += 1

			if counter == self.batchsize:
				self.db_inter.commit()
				counter = 0
