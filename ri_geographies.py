from tables import *
import csv


class Geography(IsDescription):
	"""These match whats in ri_geographies.csv"""
	state = StringCol(5)
	logrecno = Int64Col()
	geo_id = StringCol(100)
	geography_name = StringCol(224)

		
def load(h5file):
	group = h5file.createGroup("/", 'geographies', 'ACS Geographies')
	#create the table using the LookupTable Description
	table = h5file.createTable(group, 'geographies_table', Geography, 'Geography Table')

	#create a row pointer 
	row = table.row

	# open the csv and star the process of reading
	reader = csv.reader(open("source/ri_geographies.csv"))
	reader.next() # skip the header
	for state, logrecno, geo_id, geography_name in reader:
		#create a row in our table
		row['state'] = state
		row['logrecno'] = val_to_int(logrecno)
		row['geo_id'] = geo_id
		row['geography_name'] = geography_name
		row.append()

	table.flush()

	#table.logrecno.createIndex()


def val_to_int(value, to = "int"):
	"""turns string to int if its empty returns 0 """
	val = 0

	if value is not "":
		# we can account for CELLS or CELL this way
		if to is "int":

			val = int(value)
		else:
			vale = float(value)
	return val