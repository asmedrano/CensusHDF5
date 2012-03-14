from tables import *

def get_sequence_num_from_table(table_id, row, h5file):
	"""This example returns the title of the table we are looking up"""

	table=h5file.getNode("/acs_seq_tables_lookup/lookup_table_row")
	query = """(table_id=='%s') & (line_number ==%s )""" % (table_id, row)
	results = [ x['sequence_number'] for x in table.where(query)]

	return results


def get_values_from_sequence(seq_num, logrecno, h5file):
	"""Seq_num is what sequence its in, logrecno is what logrecno to look in"""
	natural_node_name = "/sequences/Seq_%s/estimates/LOGRECNO_%s" % (seq_num, logrecno)
	row = h5file.getNode(natural_node_name)
	return row

def get_value_from_row(logrecno,table_id, row, h5file):
	"""pull as single value from the row
		expects row as 1,2,4,5 
	"""
	# figure out what sequence file this table is in.
	seq_num = get_sequence_num_from_table(table_id, row, h5file)[0]
	
	return get_values_from_sequence(seq_num, logrecno, h5file).read()[row]





def main():
	h5file = openFile("HDF5/census.h5", mode = "r", title = "Census Data")

	#seq_num = get_sequence_num_from_table('B07401', 1, h5file)[0]
	#node = h5file.getNode("/sequences/Seq_1/estimates/LOGRECNO_0000001").read()
	#print node
	#print get_values_from_sequence(seq_num, '0000001',h5file).read()
	print get_value_from_row('0000001', 'B07401', 1, h5file)


	h5file.close()



if __name__ == '__main__':
	main()