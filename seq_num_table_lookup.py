from tables import *
import csv
import re


class LookupTable(IsDescription):
	"""These match whats in the Sequence_Number_and_Table_Number_Lookup.csv"""
	file_id = StringCol(16) #ACSSF
	table_id = StringCol(16) #BO7401
	sequence_number = Int64Col() #1
	line_number = Float32Col() #2 or sometimes even 0.5
	start_position = Int64Col()#7
	total_cells_in_table = Int64Col()# 80
	total_cells_in_sequence = Int64Col() #?
	table_title = StringCol(200) #Total living in area 1 year ago:
	subject_area = StringCol(200) #Residence Last Year - Migration

def load(h5file):
	#h5file = openFile("HDF5/census.h5", mode = "w", title = "Census Data")
	group = h5file.createGroup("/", 'acs_seq_tables_lookup', 'ACS Sequence Number Table Lookup')
	#create the table using the LookupTable Description
	table = h5file.createTable(group, 'lookup_table_row', LookupTable, 'Lookup Table')
	#create a row pointer 
	row = table.row


	# open the csv and star the process of reading
	reader = csv.reader(open("source/Sequence_Number_and_Table_Number_Lookup.csv"))
	reader.next() # skip the header
	for file_id, table_id, sequence_number, line_number, start_position, total_cells_in_sequence,total_cells_in_table, table_title, subject_area in reader:
		#create a row in our table
		row['file_id'] = file_id
		row['table_id'] = table_id
		row['sequence_number'] = val_to_int(sequence_number)
		row['line_number'] = val_to_int(line_number, "float")
		row['start_position'] = val_to_int(start_position)
		row['total_cells_in_table'] = val_to_int(total_cells_in_table)
		row['total_cells_in_sequence'] = val_to_int(total_cells_in_sequence)
		row['table_title'] = table_title
		row['subject_area'] = subject_area
		row.append()

	table.flush()
	table.cols.table_id.createIndex()
	



def val_to_int(value, to = "int"):
    """turns string to int if its empty returns 0 only save int--ables"""
    val = 0
    # match only numbers and stuff like 13.5 other wise return false
    p = re.compile('\d+(\.\d+)?')
    m = p.match(value)

    if m is not None:
        try:
            val = int(m.group())
        except ValueError:
            pass

    return val