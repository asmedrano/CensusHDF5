from tables import *

def get_sequence_num_from_table(table_id, row, h5file):
	"""This example returns the title of the table we are looking up"""

	table=h5file.getNode("/acs_seq_tables_lookup/lookup_table_row")
	query = """(table_id=='%s') & (line_number ==%s )""" % (table_id, row)

	sequence_nums = [ x['sequence_number'] for x in table.where(query)]

	start_poss_query = """(table_id=='%s')""" % (table_id)
	start_poss = [ x['start_position'] for x in table.where(start_poss_query)]

	return {'seq_num':sequence_nums[0], 'start_poss':start_poss[0]}


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
	table_info = get_sequence_num_from_table(table_id, row, h5file)
	
	#print seq_num, "<---- seq num"
	#print get_values_from_sequence(table_info['seq_num'], logrecno, h5file).read()
	# we need to caculate row_num so we can account for the fields that we removed in the normalization and for arrar(list) notation 0 
	row_num = (table_info['start_poss']-6)+(row-1)
	return get_values_from_sequence(table_info['seq_num'], logrecno, h5file)[row_num]





def main():
	h5file = openFile("HDF5/census.h5", mode = "r", title = "Census Data")

	#seq_num = get_sequence_num_from_table('B07401', 1, h5file)[0]
	#node = h5file.getNode("/sequences/Seq_1/estimates/LOGRECNO_0000001").read()
	#print node

	print get_value_from_row('0000040', 'B19113', 1, h5file)


	h5file.close()



if __name__ == '__main__':
	main()