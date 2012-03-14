from tables import *

def get_sequence_num_from_table(table_id, row, h5file):
	"""This example returns the title of the table we are looking up"""

	table=h5file.getNode("/acs_seq_tables_lookup/lookup_table_row")
	query = """(table_id=='%s') & (line_number ==%s )""" % (table_id, row)
	results = [ x['sequence_number'] for x in table.where(query)]

	return results


def get_value_from_sequence(seq_num, logrecno, h5file):
	"""Seq_num is what sequence its in, logrecno is what logrecno to look in"""
	natural_node_name = "/sequences/Seq_%s/estimates/LOGRECNO_%s" % (seq_num, logrecno)
	row = h5file.getNode(natural_node_name)
	return row



def main():
	h5file = openFile("HDF5/census.h5", mode = "r", title = "Census Data")
	seq_num = get_sequence_num_from_table('B00001', 1, h5file)[0]

	print get_value_from_sequence(seq_num, '0000908',h5file).read()

	h5file.close()



if __name__ == '__main__':
	main()