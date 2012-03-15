from tables import *




def get_all_geographies(h5file):
	"""Returns an pytables.table """
	return h5file.getNode("/geographies/geographies_table")
	

def print_all_geographies(h5file):
	geography_table = get_all_geographies(h5file)
	for geography in geography_table.iterrows():
		print geography['geography_name'], "%07d" % geography['logrecno']


def get_all_table_ids(h5file):
	tables = h5file.getNode("/acs_seq_tables_lookup/lookup_table_row")

	for table in tables.iterrows():
		id = table['table_id']
		col = table['line_number']
		if col != 0.0:
			yield (id,int(col))


def run_seq_num_query(table, query):
	for x in table.where(query):
		if x['sequence_number']!= 0.0:
			yield x['sequence_number']
			break


def run_table_meta_query(table, query):
	for x in table.where(query):
		if x['table_title'] !='' and x['start_position']!= 0.0:
			yield (x['table_title'],x['start_position'])
			break

def get_sequence_num_from_table(table_id, row, h5file):
	"""Returns the Sequence Num, start_position, title and row for table_id we are looking up"""

	table=h5file.getNode("/acs_seq_tables_lookup/lookup_table_row")

	query = """(table_id=='%s') & (line_number ==%s )""" % (table_id, row)

	sequence_nums = [x for x in run_seq_num_query(table, query)]

	start_poss_query = """(table_id=='%s')""" % (table_id)
	meta = [x for x in run_table_meta_query(table, start_poss_query)]

	return {'seq_num':sequence_nums[0], 'start_poss':meta[0][1], 'table_title':meta[0][0], 'row':row}


def get_values_from_sequence(seq_num, logrecno, h5file):
	"""Seq_num is what sequence its in, logrecno is what logrecno to look in"""
	natural_node_name = "/sequences/Seq_%s/estimates/LOGRECNO_%s" % (seq_num, logrecno)
	row = h5file.getNode(natural_node_name)
	return row

def get_value_from_row(logrecno,table, h5file):
	"""pull as single value from the row
		expects row as 1,2,4,5 
	"""

	#print seq_num, "<---- seq num"
	#print get_values_from_sequence(table_info['seq_num'], logrecno, h5file).read()
	# we need to caculate row_num so we can account for the fields that we removed in the normalization and for arrar(list) notation 0 
	row_num = (table['start_poss']-6)+(table['row']-1)
	return get_values_from_sequence(table['seq_num'], logrecno, h5file)[row_num]


def dump_all_tables(h5file):
	for t in get_all_table_ids(h5file):
		table = get_sequence_num_from_table(t[0], t[1], h5file)
		# get results of this table
		yield table

def dump_all_tables_for_geo(logrecno, h5file):
	""" dump all tables for single geography
		Benchmark 1: 4min17
		TODO: fix crash at the end!
	"""
	for table in dump_all_tables(h5file):
		print table['table_title']
		print "Value: ", get_value_from_row(logrecno, table, h5file)

def dump_all_tables_all_geos(h5file):
	"""dumps all tables by geographies takes a while!
		Benchmark 1 : ~8hours
	"""
	#get all geographies
	geography_table = get_all_geographies(h5file)

	for table in dump_all_tables(h5file):
		print table['table_title']
		#run for all geos 
		for geo in geography_table.iterrows():
			logrecno = "%07d" % geo['logrecno']
		  	print logrecno, get_value_from_row(logrecno, table, h5file)


def main():
	# open h5 file
	h5file = openFile("HDF5/census.h5", mode = "r", title = "Census Data")

	dump_all_tables_for_geo('0000001', h5file)
	#------------------------------------------------------------
	# single table for all geographies
	# # get table info
	# table = get_sequence_num_from_table('B19113', 1, h5file)
	
	# #get all geographies
	# geography_table = get_all_geographies(h5file)
	
	# for rec in geography_table:
	#  	logrecno = "%07d" % rec['logrecno']
	#  	print logrecno, rec['geography_name']
	#  	print get_value_from_row(logrecno, table, h5file)

	#------------------------------------------------------------

	#------------------------------------------------------------
	#get all geographies
	#geography_table = get_all_geographies(h5file)

	# for t in get_all_table_ids(h5file):
	# 	# get table info
	# 	table = get_sequence_num_from_table(t[0], t[1], h5file)
	# 	print table['table_title'], table['row']
		

	#------------------------------------------------------------

	for i in dump_all(h5file):
		print i

	h5file.close()



if __name__ == '__main__':
	main()