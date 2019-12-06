import json

class json_parser:
	def __init__(self, fname, interface = None, cols = None, batchsize, table_name):
		self.filename = fname
		self.db_inter = interface
		self.cols = cols # expects a list of desired column names
		self.batchsize = batchsize # The size of passes in one commit.
		self.table_name = table_name

	# To convert json file to a python dictionary for easy access.
	def get_dict(self):
		try:
			with open(self.filename,  "r") as f:
				self.dict = json.loads(f.read())
		except:
			print("Failed to convert json file to python dict!")
			pass

	def get_struct(self):
		# Use a set to keep the attribute names.
		self.attr = set()
		# Use a list of tuples to store each tuple.
		self.val = []

		for i in self.dict:
		    for item in self.dict[i]:
		        for k in item.keys():
		            self.attr.add(k)
		        self.val.append(tuple(item.values()))

	# Make sure all the columns requested by user are valid( exist in file).
	def validate_specified_cols(self, self.attr):
		for col in self.cols:
			if col in attr:
				pass
			else:
				raise ValueError(col)


	def write_stream(self):
		counter = 0
		batchsize = 1000

		# If we have columns specified.
		if self.cols:
			for field in self.cols:
				query = {}
				query[field].append(self.dict[field])
			self.db_inter.insert_row(table_name, query)
			counter += 1

			if counter == self.batchsize:
				self.db_inter.commit()
				counter = 0

		#Otherwise we take it all.
		else:
			for field in fieldnames:
				query = {}
				query[field].append(self.dict[field])
			self.db_inter.insert_row(table_name, query)
			counter += 1

			if counter == self.batchsize:
				self.db_inter.commit()
				counter = 0
